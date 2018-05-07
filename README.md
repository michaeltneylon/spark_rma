# Robust Multi-array Average (RMA) using Spark

See the subdirectories for more detailed READMEs including usage of programs.

## contents
- spark_rma
- helper
- documentation

## spark_rma

This directory contains the code for running RMA analysis. This includes steps for:

- annotation and background correction
- quantile normalization
- median polish

### Annotation and Background Correction

Annotation maps perfect match (PM) probes on the array to their targets. Background correction removes artifacts and
preprocesses the raw CEL files for analysis.
This is not done in Spark, it is an embarrassingly parallel problem, done independently for each sample. It completed in R.

### Quantile Normalization

Quantile normalization removes array effects by normalizing each array against all others.

### Median Polish

Tukey's median polish is used to summarize the values of multiple probes mapping within the same transcript cluster
or probeset.

## Helper Scripts

### Parquet Converter

[convert_to_parquet.py](helper/convert_to_parquet.py)

This can convert flat files (csv and tsv currently supported) to parquet format using SNAPPY compression without using
JVM.

### HTA 2.0 Annotation

[hta_annotation.R](helper/hta_annotation.R)

Many examples have been shown for HTA 2.0. The annotation and background correction step requires an input specific
to the array type. In this script, this file is generated from Bioconductor for HTA 2.0.

## Documentation

View the documentation to follow a tutorial walking through an example. See the [docs directory](docs/) to build them.
