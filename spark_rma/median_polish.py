#! /usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright 2018 Eli Lilly and Company
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Carry out median polish on grouping of probes, to return a value for each
sample in that grouping.
"""

from __future__ import print_function, unicode_literals
import argparse
import logging
import sys

from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
# pylint: disable=no-name-in-module
# not being found in path but are legitimate
from pyspark.sql.functions import log2, pandas_udf, PandasUDFType
# pylint: enable=no-name-in-module


def probe_summarization(data_frame):
    """
    Summarization step to be pickled by Spark as a UDF.
    Receives a groupings data in a pandas dataframe and performs median
    polish, calculates the expression values from the median polish matrix
    results, and return it to a new spark Dataframe.

    :param data_frame: spark dataframe input to pandas udf as pandas
     dataframe. Expect format of SAMPLE, TRANSCRIPT_CLUSTER/PROBESET, PROBE,
     VALUE This input should be for one target/group.

    :return: pandas dataframe with summarized values. SAMPLE, TARGET, VALUE.
    """

    # need to do all the imports here so they can be pickled and sent to
    # executors
    import pandas as pd
    import numpy as np

    def split_and_dedup(input_data):
        """
        Build a matrix of probes as columns and samples as rows. There
        cannot be duplicate probe IDs within a sample here to pivot, but
        typically there will be many when summarizing to transcript level
        due to multiple probesets with shared probes that map to the same
        transcript. Keep these duplicates, but rename the
        probes while keeping the matrix shape.

        :param input_data: a list given by spark that groups all the sample,
        probe, value tuples for a single group/target (probeset or transcript).
        :return: pivoted data frame, or matrix of probes as columns and
         samples as rows. The whole matrix represents one target (probeset or
         transcript cluster).
        """
        # we get our data, we need to unpack as sample, probe, value
        data = input_data[['SAMPLE', 'PROBE', 'VALUE']]
        # we may have duplicate probes, let's rename them but keep them matched
        # so we don't have duplicate indices in the pivoted data frame. sort
        #  values, group by sample and within groups replace probe with row
        # number.
        data['PROBE'] = data.sort_values(['SAMPLE', 'PROBE', 'VALUE']).groupby(
            'SAMPLE').cumcount()
        return data.pivot(index='SAMPLE', columns='PROBE', values='VALUE')

    def median_polish(matrix, max_iterations=10, eps=0.01):
        """
        Median polish of a pandas data frame, row first then column.
        This will stop at convergence or max iterations, whichever comes first.
        Convergence is defined as the sum of all absolute values of residuals
        not changing more than the eps between iterations.

        :param matrix: pandas data frame with all values in cells organized by
          row x columns, for gene expression rows are samples/arrays and
          columns are probes. The whole matrix should be all the probes
          mapping to the same transcript cluster and/or probeset region.
        :param max_iterations: The maximum iterations before stopping if
          convergence is not met. Must be 1 or greater.
        :param eps: The tolerance for convergence. Set to 0.0 to get full
          convergence, but this may be costly or never converge. Should be
          between 0 and 1.

        :return: a data frame of residuals, column effect, row effect,
          grand/overall effect, the status of convergence
        """
        # create row effect, initialize at 0.0 value for each sample in long
        #  format
        row_effect = pd.Series(0.0, index=matrix.index.values,
                               dtype=np.float64)  # pylint: disable=no-member
        # create column effect: initialize at 0.0 value for each probe in long
        # format
        column_effect = pd.Series(
            0.0,
            index=matrix.columns.values,
            dtype=np.float64  # pylint: disable=no-member
        )
        grand_effect = float(0.0)
        sum_of_absolute_residuals = 0
        converged = False

        if max_iterations < 1 or not isinstance(max_iterations, int):
            raise ValueError("max_iterations must be a positive, non-zero "
                             "integer value")
        for i in range(max_iterations):

            # columns
            col_median = matrix.median(0)

            matrix = matrix.subtract(col_median, axis=1)
            column_effect = column_effect + col_median
            diff = row_effect.median(0)
            row_effect -= diff
            grand_effect += diff

            # rows
            row_median = matrix.median(1)

            matrix = matrix.subtract(row_median, axis=0)
            row_effect = row_effect + row_median
            diff = column_effect.median()
            column_effect -= diff
            grand_effect += diff

            # Convergence check
            # check if the sum of all the absolute values of residuals has
            # changed less than the eps between iterations or is 0.
            new_sar = np.absolute(matrix).sum().sum()
            if abs(new_sar - sum_of_absolute_residuals) <= eps * new_sar or \
                    new_sar == 0:
                converged = True
                logging.debug(
                    "Convergence reached after %s iterations", i + 1)
                break
            sum_of_absolute_residuals = new_sar
        if not converged:
            logging.debug("No convergence, reached maximum iterations.")

        return matrix, column_effect, row_effect, grand_effect, converged

    def gene_expression(matrix):
        """
        Use median polish to get values and calculate gene expression values.

        :param matrix: sample, probe, value matrix to median polish
        :type matrix: pd.DataFrame()
        :return: expression values in pandas data frame
        :rtype: pd.DataFrame()
        """
        results = median_polish(matrix)
        row_effect = results[2]
        grand_effect = results[3]
        # expression is equal to the row effect + the overall effect
        expression = row_effect + grand_effect
        return expression

    def get_target(data):
        """
        Get the target for this grouping. This is a transcript or probeset.
        The intermediate steps are specific to a target, so remove this
        label for merging back later.
        :param data: input data frame with SAMPLE, TARGET, PROBE, VALUE
        :return: string value defining the target name
        """
        targets = data['TARGET'].unique()
        if len(targets) > 1:
            raise ValueError('More than one target found, possible poor '
                             'groupby')
        return targets[0]

    target = get_target(data_frame)
    data_frame = split_and_dedup(data_frame)
    result = gene_expression(data_frame)
    result = pd.DataFrame({
        'SAMPLE': result.index,
        'TARGET': target,
        'VALUE': result.values})
    return result


class Summary(object):
    """
    Summarize gene expression values via median polish.
    """

    def __init__(self, spark, input_data, num_samples, repartition_number,
                 **kwargs):
        self.spark = spark
        self.input_data = input_data
        self.num_samples = num_samples
        self.repartition_number = repartition_number
        self.group_key = kwargs.get('grouping')

    def udaf(self, data):
        """
        Apply median polish to groupBy keys and return value for each sample
        within that grouping.

        :returns: spark dataframe
        """
        # repartition by our grouping keys
        if self.group_key not in [
                'TRANSCRIPT_CLUSTER', 'PROBESET'
        ]:
            raise Exception("Invalid grouping keys.")
        data = data.withColumnRenamed('NORMALIZED_INTENSITY_VALUE', 'VALUE')
        data = data.repartition(self.repartition_number, self.group_key)

        # log 2 values
        data = data.withColumn('VALUE', log2(data['VALUE']).alias('VALUE'))

        # rename our group keys to TARGET to simplify UDF
        data = data.withColumnRenamed(self.group_key, 'TARGET')

        # register the pandas UDF for median polish
        medpol = pandas_udf(probe_summarization,
                            "SAMPLE string, TARGET string, VALUE double",
                            PandasUDFType.GROUPED_MAP)
        # group data by target so we can summarize probes across samples
        # for each target using a pandas UDF
        data = data.groupby('TARGET').apply(medpol)
        # rename target back to appropriate selection
        data = data.withColumnRenamed('TARGET', self.group_key)

        data = data.repartition(int(self.num_samples))
        return data

    def summarize(self):
        """
        Summarize results across samples with median polish within defined
        groups.
        """
        result = self.udaf(self.input_data)
        return result


class TranscriptSummary(Summary):
    """Summarize probes with transcript cluster groupings"""

    def __init__(self, spark, input_data, num_samples, repartition_number):
        super(TranscriptSummary, self).__init__(
            spark, input_data, num_samples, repartition_number,
            grouping='TRANSCRIPT_CLUSTER')


class ProbesetSummary(Summary):
    """Summarize probes with probeset region grouping"""

    def __init__(self, spark, input_data, num_samples, repartition_number):
        super(ProbesetSummary, self).__init__(
            spark, input_data, num_samples, repartition_number,
            grouping='PROBESET')


def infer_grouping_and_summarize(spark, input_file, output_file, num_samples,
                                 repartition_number):
    """
    Read the input file to infer grouping type and select appropriate
    summarization class.
    """
    logging.info("Reading file at: %s", input_file)
    data_frame = spark.read.parquet(input_file)
    headers = data_frame.columns

    def matched(columns_to_check):
        """Check columns against file."""
        return not bool(list(set(columns_to_check) - set(headers)))

    def inference():
        """Determine grouping and return appropriate class"""
        if matched(['TRANSCRIPT_CLUSTER']):
            return TranscriptSummary
        elif matched(['PROBESET']):
            return ProbesetSummary
        else:
            raise KeyError('Bad Input, no valid headers found.')

    summarization = inference()
    summarization_object = summarization(spark, data_frame, num_samples,
                                         repartition_number)
    summarized_data = summarization_object.summarize()
    logging.info("Writing to file: %s", output_file)
    summarized_data.write.parquet(output_file)


def command_line():
    """Collect and validate command line arguments."""

    class MyParser(argparse.ArgumentParser):
        """
        Override default behavior, print the whole help message for any CLI
        error.
        """

        def error(self, message):
            print('error: {}\n'.format(message), file=sys.stderr)
            self.print_help()
            sys.exit(2)

    parser = MyParser(description="""
    Summarization step of RMA using Median Polish.

    Median Polish summarizes probes within a group. That group can be probes 
    with the same transcript cluster and/or probeset region.

    The input to this should be the parquet output from quantile 
    normalization. The grouping is inferred from the input format as 
    generated by the annotation during the background correction step. 
    Specify the summarization type in the background correction step to 
    determine the grouping choice here.

    The input headers from quantile normalization:

    - SAMPLE
    - PROBE
    - PROBESET or TRANSCRIPT_CLUSTER
    - NORMALIZED_INTENSITY_VALUE

    The output format:

    - SAMPLE
    - PROBESET or TRANSCRIPT_CLUSTER
    - VALUE
    """,
                      formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('-v', '--verbose', help="Enable verbose logging",
                        action="store_const", dest="loglevel",
                        const=logging.DEBUG, default=logging.INFO)
    parser.add_argument('-i', '--input',
                        help="Input path", required=True)
    parser.add_argument('-o', '--output', help="Output filename",
                        default='expression.parquet')
    parser.add_argument('-ns', '--number_samples',
                        help="Number of samples. Explicitly stating it here "
                             "is much faster than making Spark count the "
                             "records. This helps set partitions.",
                        type=int, required=True)
    parser.add_argument('-rn', '--repartition_number',
                        help="Number of partitions to use when running "
                             "median polish. This determines how many "
                             "groupings get collected into each task, "
                             "which impacts processing time and memory "
                             "consumption. The default is the number of "
                             "samples if left unset.",
                        type=int)
    arguments = parser.parse_args()
    if not arguments.repartition_number:
        arguments.repartition_number = arguments.number_samples
    return arguments


def main():
    """
    Collect command-line arguments and start spark session when using
    spark-submit.
    """
    arguments = command_line()
    logging.basicConfig(level=arguments.loglevel,
                        format='%(asctime)s %(name)-12s %(levelname)-8s %('
                               'message)s')
    logging.info("Starting Spark...")
    spark_session = SparkSession.builder \
        .config(conf=SparkConf()
                .setAppName("Median Polish")).getOrCreate()

    infer_grouping_and_summarize(
        spark=spark_session,
        input_file=arguments.input,
        num_samples=arguments.number_samples,
        output_file=arguments.output,
        repartition_number=arguments.repartition_number
    )

    logging.info("Complete.")
    spark_session.stop()


if __name__ == "__main__":
    main()
