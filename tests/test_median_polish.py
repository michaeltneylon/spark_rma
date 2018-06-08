#! /usr/bin/env python
# -*- coding: utf-8 -*-

"""
Test median polish.

Tukey, J. W. (1977). Exploratory Data Analysis, Reading Massachusetts: Addison-Wesley.

https://en.wikipedia.org/wiki/Median_polish

Order matters for median polish --> row then column or column first then rows.
Ours has been written to match results of preprocessCore
"""

from __future__ import print_function, unicode_literals

import logging
import random

import findspark
import pytest

findspark.init()
from pyspark import SparkConf
from pyspark.sql import SparkSession

from spark_rma import median_polish


def quiet_py4j():
    """
    Suppress spark logging for the test context.
    """
    logger = logging.getLogger('py4j')
    logger.setLevel(logging.WARN)


@pytest.fixture(scope="session")
def spark_session(request):
    """
    Fixture for pyspark sql: spark session (dataframe api)
    """
    conf = (SparkConf().setMaster("local[2]").setAppName(
        "pytest-pyspark-tests"))
    sc = SparkSession.builder \
        .config(conf=conf
                ).getOrCreate()
    request.addfinalizer(lambda: sc.stop())

    quiet_py4j()
    return sc


def generate_sample_data(spark_session):
    headers = ['SAMPLE', 'TRANSCRIPT_CLUSTER', 'PROBESET',
               'PROBE', 'VALUE']

    data = []
    # 6 samples
    for sample in range(6):
        for probe in range(10):
            data.append(
                ['sample_' + str(sample),
                 'tc_' + str(probe),
                 'psr_' + str(probe),
                 probe,
                 random.uniform(0, 100)]
            )
    rdd = spark_session.sparkContext.parallelize(data)
    df = rdd.toDF(headers)
    return df


def test_transcript_grouping(spark_session):
    """
    Test median polish grouping by transcript cluster only.
    """
    data = generate_sample_data(spark_session)

    testing_object = median_polish.TranscriptSummary(
        spark=spark_session,
        input_data=data,
        num_samples=6,
        repartition_number=6)
    df = testing_object.udaf(data)
    expected_headers = ['SAMPLE', 'TRANSCRIPT_CLUSTER', 'VALUE']
    assert expected_headers == df.columns
    assert len(df.collect()) > 0  # has results


def test_probeset_grouping(spark_session):
    """
    Test median polish grouping by probeset only.
    """
    data = generate_sample_data(spark_session)

    testing_object = median_polish.ProbesetSummary(
        spark=spark_session,
        input_data=data,
        num_samples=6,
        repartition_number=6)
    df = testing_object.udaf(data)
    expected_headers = ['SAMPLE', 'PROBESET', 'VALUE']
    assert expected_headers == df.columns
    assert len(df.collect()) > 0  # has results


def test_known_results():
    """
    Testing against known results from R's medpolish() example,
    using medpolish() and preprocessCore.
    https://stat.ethz.ch/R-manual/R-devel/library/stats/html/medpolish.html

    This example matches preprocessCore:::subColSummarizeMedianpolish() after
    adding row effects and overall effect. That uses a row first look,
    whereas preprocessCore:::subColSummarizeMedianpolishLog() uses column
    first. Oligo uses this latter method, so we test against that.

    Using medpolish():
    > deaths <-
        rbind(c(14,15,14),
              c( 7, 4, 7),
              c( 8, 2,10),
              c(15, 9,10),
              c( 0, 2, 0))
    > dimnames(deaths) <- list(c("1-24", "25-74", "75-199", "200++", "NA"),
                             paste(1973:1975))

    Using preprocessCore:
    preprocessCore's median polish returns the expression values as a
    result of row effects + overall effect:

    deaths <-
      rbind(c(14,7,8,15,0),
            c(15,4,2,9,2),
            c(14,7,10,10,0))
    colnames(deaths) <- c('1-24', '25-74', '75-199', '200++', 'NA')
    rownames(deaths) <- c('1974', '1974', '1975')
    medianDF <- preprocessCore:::subColSummarizeMedianpolishLog(deaths,
     c(rep(1, nrow(deaths))))
    colnames(medianDF) <- colnames(deaths)
     1-24   25-74   75-199    200++    NA
     3.807355    2.807355    3    3.906891    NaN

    The preprocessCore input data had to be transposed and we give a label
    input to the function using the same value (1 1 1 1 1) because they all
    belong to one summarization group in this example.
    """

    # data from R medpolish() example.
    data = [['1-24', 1973, 14],
            ['1-24', 1974, 15],
            ['1-24', 1975, 14],
            ['25-74', 1973, 7],
            ['25-74', 1974, 4],
            ['25-74', 1975, 7],
            ['75-199', 1973, 8],
            ['75-199', 1974, 2],
            ['75-199', 1975, 10],
            ['200++', 1973, 15],
            ['200++', 1974, 9],
            ['200++', 1975, 10],
            ['NA', 1973, 0],
            ['NA', 1974, 2],
            ['NA', 1975, 0],
            ]
    # log2 transform
    import numpy as np
    with np.errstate(divide='ignore'):
        data = [[_[0], _[1], np.log2(_[2])] for _ in data]
    import pandas as pd
    data = pd.DataFrame(data, columns=['SAMPLE', 'PROBE', 'VALUE'])
    data['TARGET'] = 'target'  # requires a target name.. we just have the
    # one group here though.
    result = median_polish.probe_summarization(data)
    result = result.drop('TARGET', axis=1).values.tolist()

    # expected results from R's preprocessCore subColSummarizeMedianpolishLog
    expected = [('1-24', 3.807355), ('25-74', 2.807355), ('75-199', 3),
                ('200++', 3.906891), ('NA', None)]
    # filter.. this example isn't great, log2(0) gives us -inf/NaN
    result = [(_[0], _[1]) if not np.isneginf(_[1]) else
              (_[0], None) for _ in result]
    # round to same place as R results.
    result = [(_[0], round(_[1], 6)) if _[1] else _ for _ in result]
    assert sorted(result) == sorted(expected)
