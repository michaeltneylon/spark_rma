#! /usr/bin/env python
# -*- coding: utf-8 -*-

"""
Test quantile normalization.
"""

from __future__ import print_function, unicode_literals

import logging
import random

import findspark
import pandas as pd
import pytest

findspark.init()
from pyspark import SparkConf
from pyspark.sql import SparkSession

from spark_rma import quantile_normalization


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


class Match:
    """
    Determine if results match to expected.
    """
    def __init__(self, observed, expected):
        self.observed = observed
        self.expected = expected
        self.joined, self.expected_values, self.observed_values = \
            self._match_up()

    def _match_up(self):
        df = self.expected.merge(self.observed, on=['sample', 'probe'],
                                 suffixes=['_expected', '_observed'])
        expected_values = df['normalized_intensity_value_expected'].tolist()
        observed_values = df['normalized_intensity_value_observed'].tolist()
        return df, expected_values, observed_values

    def exact_match(self):
        expected_values = [round(_, 2) for _ in self.expected_values]
        observed_values = [round(_, 2) for _ in self.observed_values]
        paired = zip(expected_values, observed_values)
        paired = [list(set(_)) for _ in paired]
        paired_lengths = [len(_) for _ in paired]
        if max(paired_lengths) == 1:
            return True
        else:
            return False


def generate_test_data():
    intensity_value = [random.uniform(0, 100) for _ in range(12)]
    data = {
        'sample':
            ['sample1', 'sample1', 'sample1', 'sample1',
             'sample2', 'sample2', 'sample2', 'sample2',
             'sample3', 'sample3', 'sample3', 'sample3'],
        'probe':
            [1, 2, 3, 4,
             1, 2, 3, 4,
             1, 2, 3, 4],
        'transcript_cluster':
            ['tc1', 'tc1', 'tc1', 'tc1',
             'tc1', 'tc1', 'tc1', 'tc1',
             'tc1', 'tc1', 'tc1', 'tc1'],
        'intensity_value':
            intensity_value
            }
    df = pd.DataFrame(data)
    return df


def wikipedia_test_set():
    # https://en.wikipedia.org/wiki/Quantile_normalization
    # wikipedia uses dense/min rank (unclear which with limited data)
    wiki = {
        'sample':
            ['I', 'I', 'I', 'I',
             'II', 'II', 'II', 'II',
             'III', 'III', 'III', 'III'
             ],
        'probe':
            [1, 2, 3, 4,
             1, 2, 3, 4,
             1, 2, 3, 4],
        'transcript_cluster':
            ['tc1', 'tc1', 'tc1', 'tc1',
             'tc1', 'tc1', 'tc1', 'tc1',
             'tc1', 'tc1', 'tc1', 'tc1'],
        'intensity_value':
            [5, 2, 3, 4,
             4, 1, 4, 2,
             3, 4, 6, 8
             ]
    }
    wiki = pd.DataFrame(wiki)

    expected = {
        'sample':
            ['I', 'I', 'I', 'I',
             'II', 'II', 'II', 'II',
             'III', 'III', 'III', 'III'
             ],
        'probe':
            [1, 2, 3, 4,
             1, 2, 3, 4,
             1, 2, 3, 4,
             ],
        'transcript_cluster':
            ['tc1', 'tc1', 'tc1', 'tc1',
             'tc1', 'tc1', 'tc1', 'tc1',
             'tc1', 'tc1', 'tc1', 'tc1'],
        'normalized_intensity_value':
            [5.67, 2.0, 3.0, 4.67,
             4.67, 2.0, 4.67, 3.0,
             2.0, 3.0, 4.67, 5.67
             ]
    }
    expected = pd.DataFrame(expected)
    return wiki, expected


def wikipedia_test_set_with_r_expect():
    """
    Using the example data with some ties from Wikipedia:
    # https://en.wikipedia.org/wiki/Quantile_normalization
    But instead of using its output, we use the output from running it in R's
    preprocessCore:::quantiles.normalize()
    The difference lies in how ties are handled. Dense vs Average ranking
    and reassigmnent with a second step of calculating average of row averages
    for averaged ranks.

    # ordered this way because normalize.quantiles() takes matrix of column
    per chip and row per probe and intensity values as cells.
    See page 3 on https://www.bioconductor.org/packages/3.7/bioc/manuals
    /preprocessCore/man/preprocessCore.pdf


    wiki_example <-
    rbind(
      c(5, 4, 3),
      c(2, 1, 4),
      c(3, 4, 6),
      c(4, 2, 8))

    quants <- preprocessCore:::normalize.quantiles(wiki_example)

    > quants
             [,1]     [,2]     [,3]
    [1,] 5.666667 5.166667 2.000000
    [2,] 2.000000 2.000000 3.000000
    [3,] 3.000000 5.166667 4.666667
    [4,] 4.666667 3.000000 5.666667

    # transpose to make it the same shape as our wiki example
    > aa <- t(quants)
    > aa
             [,1] [,2]     [,3]     [,4]
    [1,] 5.666667    2 3.000000 4.666667
    [2,] 5.166667    2 5.166667 3.000000
    [3,] 2.000000    3 4.666667 5.666667
    """
    wiki = {
        'sample':
            ['I', 'I', 'I', 'I',
             'II', 'II', 'II', 'II',
             'III', 'III', 'III', 'III'
             ],
        'probe':
            [1, 2, 3, 4,
             1, 2, 3, 4,
             1, 2, 3, 4],
        'transcript_cluster':
            ['tc1', 'tc1', 'tc1', 'tc1',
             'tc1', 'tc1', 'tc1', 'tc1',
             'tc1', 'tc1', 'tc1', 'tc1'],
        'intensity_value':
            [5, 2, 3, 4,
             4, 1, 4, 2,
             3, 4, 6, 8
             ]
    }
    wiki = pd.DataFrame(wiki)

    expected = {
        'sample':
            ['I', 'I', 'I', 'I',
             'II', 'II', 'II', 'II',
             'III', 'III', 'III', 'III'
             ],
        'probe':
            [1, 2, 3, 4,
             1, 2, 3, 4,
             1, 2, 3, 4,
             ],
        'transcript_cluster':
            ['tc1', 'tc1', 'tc1', 'tc1',
             'tc1', 'tc1', 'tc1', 'tc1',
             'tc1', 'tc1', 'tc1', 'tc1'],
        'normalized_intensity_value':
            [5.67, 2.0, 3.0, 4.67,
             5.17, 2.0, 5.17, 3.0,
             2.0, 3.0, 4.67, 5.67
             ]
    }
    expected = pd.DataFrame(expected)
    return wiki, expected


def irizarray_known_results():
    """
    Using 'first' ranking. Irizarray's preferred method.
    https://twitter.com/rafalab/status/545586012219772928/photo/1
    """
    rafael = {
        'sample':
            ['I', 'I', 'I', 'I', 'I',
             'II', 'II', 'II', 'II', 'II',
             'III', 'III', 'III', 'III', 'III',
             'IV', 'IV', 'IV', 'IV', 'IV'
             ],
        'transcript_cluster':
            ['tc1', 'tc1', 'tc1', 'tc1', 'tc1',
             'tc1', 'tc1', 'tc1', 'tc1', 'tc1',
             'tc1', 'tc1', 'tc1', 'tc1', 'tc1',
             'tc1', 'tc1', 'tc1', 'tc1', 'tc1'],
        'probe':
            [1, 2, 3, 4, 5,
             1, 2, 3, 4, 5,
             1, 2, 3, 4, 5,
             5, 2, 3, 4, 1],  # first sort order appearance matters.. for this
        # example, we re order these to match rafael's example on ties.
        'intensity_value':
            [2, 5, 4, 3, 3,
             4, 14, 8, 8, 9,
             4, 4, 6, 5, 3,
             5, 7, 9, 8, 5
             ]
    }
    rafael = pd.DataFrame(rafael)

    expected = {
        'sample':
            ['I', 'I', 'I', 'I', 'I',
             'II', 'II', 'II', 'II', 'II',
             'III', 'III', 'III', 'III', 'III',
             'IV', 'IV', 'IV', 'IV', 'IV'
             ],
        'probe':
            [1, 2, 3, 4, 5,
             1, 2, 3, 4, 5,
             1, 2, 3, 4, 5,
             1, 2, 3, 4, 5
             ],
        'transcript_cluster':
            ['tc1', 'tc1', 'tc1', 'tc1', 'tc1',
             'tc1', 'tc1', 'tc1', 'tc1', 'tc1',
             'tc1', 'tc1', 'tc1', 'tc1', 'tc1',
             'tc1', 'tc1', 'tc1', 'tc1', 'tc1'],
        'normalized_intensity_value':
            [3.5, 8.5, 6.5, 5.0, 5.5,
             3.5, 8.5, 5.0, 5.5, 6.5,
             5.0, 5.5, 8.5, 6.5, 3.5,
             5.0, 5.5, 8.5, 6.5, 3.5]
    }

    expected = pd.DataFrame(expected)
    return rafael, expected


def quantile_normalization_wiki_test_method(df):
    """
    Python pandas quantile normalization to compare against spark. This
    matches the wikpedia example, but it is not how it is done in RMA.
    """
    df['rank'] = df.groupby('sample')["intensity_value"].rank(method="dense")
    df['row_number'] = df.groupby('sample')['intensity_value'].rank(
        method='first')
    averaged = df.groupby("row_number")['intensity_value'].mean()
    averaged = averaged.to_frame()
    averaged = averaged.rename(columns={'intensity_value':
                                        'normalized_intensity_value'})
    normalized = df.merge(averaged, left_on='rank', right_on='row_number',
                          right_index=True)
    normalized = normalized[['sample', 'probe', 'normalized_intensity_value']]
    return normalized


def quantile_normalization_wiki_test_method_irizarry(df):
    """
    Python pandas quantile normalization to compare against spark. This
    using 'first' ranking to match Rafael's preferred method. There are
    three or four, dense or min, average, and first are all seen.
    https://twitter.com/rafalab/status/545586012219772928/photo/1
    """
    df['rank'] = df.groupby('sample')["intensity_value"].rank(method="first")
    df['row_number'] = df.groupby('sample')['intensity_value'].rank(
        method='first')
    averaged = df.groupby("row_number")['intensity_value'].mean()
    averaged = averaged.to_frame()
    averaged = averaged.rename(columns={'intensity_value':
                                        'normalized_intensity_value'})
    normalized = df.merge(averaged, left_on='rank', right_on='row_number',
                          right_index=True)
    normalized = normalized[['sample', 'probe', 'normalized_intensity_value']]
    return normalized


def quantile_normalization_preprocesscore_test_method(df):
    """
    Python pandas quantile normalization matching the R methods. How to
    handle tie breakers is done differently. In R preprocessCore, ranks
    are computed with averages in ties, and means are calculated on row
    numbers, first appear. So when it is joined back on to replace,
    if an average row rank exists, the value is computed as the average
    between the surrounding ranks' averages.
    """
    df['rank'] = df.groupby('sample')["intensity_value"].rank(method="average")
    df['row_number'] = df.groupby('sample')['intensity_value'].rank(
        method='first')
    averaged = df.groupby("row_number")['intensity_value'].mean()
    # get any averaged row values
    average_row_numbers = df['rank'].drop_duplicates().to_frame()
    averaged = averaged.to_frame()
    average_ranks = average_row_numbers.merge(averaged, left_on='rank',
                                              right_on='row_number',
                                              how='outer', right_index=True)
    averaged = averaged.reset_index()
    averaged = averaged.rename(columns={'row_number': 'rank'})
    # here we get the averaged ranks values as an average of the surrounding
    #  ranks.
    for index, row in average_ranks.iterrows():
        if pd.isnull(row['intensity_value']):
            upper = row['rank'] + 0.5
            lower = row['rank'] - 0.5
            upper_value = average_ranks[average_ranks['rank'] == upper]

            lower_value = average_ranks[average_ranks['rank'] == lower]
            getting_average = upper_value.append(lower_value,
                                                 ignore_index=True)
            average_rank_values = getting_average.mean().to_frame(
            ).T.reset_index(drop=True)
            averaged = averaged.append(average_rank_values, ignore_index=True)

    averaged = averaged.rename(columns={'intensity_value':
                                        'normalized_intensity_value'})
    normalized = df.merge(averaged, on='rank')
    normalized = normalized[['sample', 'probe', 'normalized_intensity_value']]
    return normalized


def test_first_rank_quantile_normalization():
    """
    Validate testing method that demonstrates first order ranking quantile
    normalization.
    """
    dataframe, expected = irizarray_known_results()
    compare = quantile_normalization_wiki_test_method_irizarry(dataframe)
    assert Match(compare, expected).exact_match()


def test_average_rank_quantile_normalization():
    """
    Validate testing method that demonstrates first order ranking quantile
    normalization.
    """
    dataframe, expected = wikipedia_test_set_with_r_expect()
    compare = quantile_normalization_preprocesscore_test_method(dataframe)
    assert Match(compare, expected).exact_match()


def test_dense_rank_quantile_normalization():
    """
    Validate testing method that demonstrates first order ranking quantile
    normalization.
    """
    dataframe, expected = wikipedia_test_set()
    compare = quantile_normalization_wiki_test_method(dataframe)
    assert Match(compare, expected).exact_match()


def test_quantile_normalization_random_data(spark_session):
    """
    Generating a test set of random values and feeding into the spark
    method and the pandas method and comparing.
    """
    dataframe = generate_test_data()
    df = spark_session.createDataFrame(dataframe)
    df = quantile_normalization.quantile_normalize(df, 'TRANSCRIPT_CLUSTER')
    df = df.toPandas()
    df = df[['SAMPLE', 'PROBE', 'TRANSCRIPT_CLUSTER',
             'NORMALIZED_INTENSITY_VALUE']]
    df.columns = [col.lower() for col in df.columns]
    compare = quantile_normalization_wiki_test_method_irizarry(dataframe)
    assert Match(df, compare).exact_match()


def test_quantile_normalization_preprocesscore(spark_session):
    """
    Seeding the spark method with the input data from the wikipedia example
    for quantile normalization and comparing the output to the known result
    in the article.
    """
    dataframe, expected = wikipedia_test_set_with_r_expect()
    dataframe = spark_session.createDataFrame(dataframe)
    df = quantile_normalization.quantile_normalize(dataframe,
                                                   'TRANSCRIPT_CLUSTER')
    df = df.toPandas()
    df = df[['SAMPLE', 'PROBE', 'TRANSCRIPT_CLUSTER',
             'NORMALIZED_INTENSITY_VALUE']]
    df.columns = [col.lower() for col in df.columns]
    expected = expected[['sample', 'probe', 'transcript_cluster',
                         'normalized_intensity_value']]
    assert not Match(df, expected).exact_match()


def test_quantile_normalization_rafael(spark_session):
    """
    Test SparkRMA quantile normalization against Rafael Irizarry's example
    for his preferred method of 'first' ranking.
    """
    dataframe, expected = irizarray_known_results()
    df = spark_session.createDataFrame(dataframe)
    df = quantile_normalization.quantile_normalize(df, 'TRANSCRIPT_CLUSTER')
    df = df.toPandas()
    df = df[['SAMPLE', 'PROBE', 'TRANSCRIPT_CLUSTER',
             'NORMALIZED_INTENSITY_VALUE']]
    df.columns = [col.lower() for col in df.columns]
    expected = expected[['sample', 'probe', 'transcript_cluster',
                         'normalized_intensity_value']]
    assert Match(df, expected).exact_match()


def test_quantile_normalization_wikipedia(spark_session):
    """
    Test SparkRMA quantile normalization against Wikipedia example
    that uses dense/min ranking.
    """
    dataframe, expected = wikipedia_test_set()
    df = spark_session.createDataFrame(dataframe)
    df = quantile_normalization.quantile_normalize(df, 'TRANSCRIPT_CLUSTER')
    df = df.toPandas()
    df = df[['SAMPLE', 'PROBE', 'TRANSCRIPT_CLUSTER',
             'NORMALIZED_INTENSITY_VALUE']]
    df.columns = [col.lower() for col in df.columns]
    expected = expected[['sample', 'probe',
                         'transcript_cluster', 'normalized_intensity_value']]
    assert not Match(df, expected).exact_match()
