# Getting Started with SparkRMA

SparkRMA: a scalable method for preprocessing oligonucleotide arrays
* Michael T. Neylon and Naveed Shaikh

----

## Background

Robust Multi-array Average (RMA) is a method for preprocessing oligonulceotide arrays.
Refer to the canonical publications on this topic:

- B M Bolstad et al. “A comparison of normalization methods for high density oligonucleotide array data based on variance and bias”. In: Bioinformatics 19.2 (Jan. 2003), pp. 185–93.
- Rafael A Irizarry et al. “Exploration, normalization, and summaries of high density oligonucleotide array probe level data”. In: Biostatistics 4.2 (Apr. 2003), pp. 249–64. doi: 10.1093/biostatistics/4.2.249.
- Rafael A Irizarry et al. “Summaries of Affymetrix GeneChip probe level data”. In: Nucleic acids research 31.4 (2003), e15–e15.


## Requirements

- Spark 2.0.0+

## Installation

The scripts are intended to be submitted directly to the Spark driver using `spark-submit`, so no installation of
SparkRMA is necessary. To run tests or build docs we have provided a setup.py script to install `spark_rma`.

## AWS

A fast way to start using SparkRMA on large data sets is to use Amazon's Elastic MapReduce. Select an EMR version
that meets the requirements listed above. There are dependencies that need to be installed and made available on each
worker node. Currently that includes:

- pandas

Write a bootstrap script and store it in S3 to install pandas on each node:

`sudo pip install pandas`

