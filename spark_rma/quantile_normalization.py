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
Quantile normalize background corrected array samples.
"""

from __future__ import print_function, unicode_literals
import argparse
import logging
import sys

from pyspark.sql import SparkSession, Window
from pyspark.conf import SparkConf
# pylint: disable=no-name-in-module
# row_number is not being found in path but are legitimate
from pyspark.sql.functions import row_number
# pylint: enable=no-name-in-module


def quantile_normalize(data_frame, target):
    """
    Quantile normalize the data using spark window spec. This allows the
    ranking, average, and reassigning values with a join.

    :param data_frame: spark data frame
    :param target: summary target defined by annotation. This is the level
      to which we will summarize, either probeset or transcript_cluster.
    :return: quantile normalized dataframe with sample, probe, target,
      and normalized value.
    """

    # create window spec
    window = Window.partitionBy('SAMPLE').orderBy('INTENSITY_VALUE')

    # rank probes within each partition by value using 'first' ranking
    ranked = data_frame.select(
        data_frame['SAMPLE'],
        data_frame['PROBE'],
        data_frame[target],
        data_frame['INTENSITY_VALUE'],
        row_number().over(window).alias("row_number")
        )

    # get average by row number across partitions to assign ranks to
    # intensity values
    averaged = ranked.groupBy("row_number").avg("INTENSITY_VALUE")
    # rename so no conflict during join
    averaged_x = averaged.withColumnRenamed("row_number", "row_number_1")
    averaged_x = averaged_x.withColumnRenamed("avg(INTENSITY_VALUE)",
                                              "NORMALIZED_INTENSITY_VALUE")
    # substitute normalized value by rank to original data
    q_norm = ranked.join(averaged_x, ranked.row_number ==
                         averaged_x.row_number_1,
                         'left_outer')
    # drop the extra columns we no longer need after joining
    q_norm = q_norm.drop('row_number_1', 'INTENSITY_VALUE', 'row_number')
    return q_norm


def normalize(spark, input_path, output):
    """
    Read parquet file, normalize, and write results.

    :param spark: spark session object
    :param input_path: path to input parquet file with background corrected
      data
    :param output: path to write results
    """

    data_frame = spark.read.parquet(input_path)
    target = infer_target_level(data_frame)
    data_frame = quantile_normalize(data_frame, target)
    data_frame.write.parquet(output)


def infer_target_level(data_frame):
    """
    Read the input file to infer target type and select appropriate
    summarization class.
    """

    def inference(headers):
        """Determine target level based on input headers."""
        if 'TRANSCRIPT_CLUSTER' in headers:
            return 'TRANSCRIPT_CLUSTER'
        elif 'PROBESET' in headers:
            return 'PROBESET'
        else:
            raise KeyError('Bad Input, no valid headers found.')

    headers = [_.upper() for _ in data_frame.columns]
    target = inference(headers)
    return target


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
    Quantile Normalization of background corrected CEL files.
    
    The summarization target level is chosen in the previous step of 
    Background Correction and inferred here. Input is a parquet file or 
    directory of parquet files with background corrected samples. The 
    required input headers:
    
    - SAMPLE
    - PROBE
    - PROBESET or TRANSCRIPT_CLUSTER
    - INTENSITY_VALUE
    
    The output of this step is a parquet file of quantile normalized 
    samples with the headers:
    
    - SAMPLE
    - PROBE
    - PROBESET or TRANSCRIPT_CLUSTER
    - NORMALIZED_INTENSITY_VALUE
    """, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('-v', '--verbose', help="Enable Verbose logging",
                        action="store_const", dest="loglevel",
                        const=logging.DEBUG, default=logging.INFO)
    parser.add_argument('-i', '--input',
                        help="Input path to directory of background corrected "
                             "CEL files in parquet format.", required=True)
    parser.add_argument('-o', '--output', help="Output filename",
                        default='quantile_normalized.parquet')
    arguments = parser.parse_args()
    return arguments


def main():
    """
    Gather command-line arguments and create spark session if executed with
    spark-submit
    """
    args = command_line()
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(name)-12s '
                                                    '%(levelname)-8s %('
                                                    'message)s')
    logging.info("Starting Spark...")
    spark_session = SparkSession.builder \
        .config(conf=SparkConf()
                .setAppName("Quantile Normalization")).getOrCreate()

    normalize(spark_session, args.input, args.output)
    logging.info("Complete")
    spark_session.stop()


if __name__ == "__main__":
    main()
