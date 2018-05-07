# Helper

Scripts here that are not main stages of the pipeline but are used in staging data, intermediate steps, etc.

## [convert_to_parquet.py](convert_to_parquet.py)

This can convert flat files (csv and tsv currently supported) to parquet format using SNAPPY compression without using
JVM. It supports either positional arguments for a single input and output file. It also supports
giving it a directory of files as an input and will run the conversion in parallel.

### Installation

Before installing the required modules, we need to meet a dependency for the compression library snappy.
This is a requirement for python-snappy, which has [instructions](https://github.com/andrix/python-snappy#dependencies)
 on how to install the dependencies. Then install the python packages:

`pip install -r requirements.txt`

Alternatively, you can [install fastparquet with conda](https://github.com/dask/fastparquet#installation)
to get all the appropriate dependencies, otherwise install snappy first and then the requirements file.

### Usage

```
usage: convert_to_parquet.py [-h] [-id INPUT_DIRECTORY]
                             [-od OUTPUT_DIRECTORY] [-tab | -comma]
                             [input] [output]

Convert to Parquet

positional arguments:
  input                 Input filename
  output                Output filename

optional arguments:
  -h, --help            show this help message and exit
  -id INPUT_DIRECTORY, --input_directory INPUT_DIRECTORY
                        Input directory of files to convert to parquet in
                        parallel. This overrides the positional arguments for
                        a single file.
  -od OUTPUT_DIRECTORY, --output_directory OUTPUT_DIRECTORY
                        Specify output directory for the parquet files,
                        otherwise they are written to pwd.
  -tab                  Set this argument if you are supplying a tab-separated
                        file. This is the default.
  -comma                set this argument if you are supplying a comma-
                        separated file.
```

## HTA Annotation

For many examples, we focus on the HTA 2.0 array. Affymetrix provides many associated
[annotation files](http://www.affymetrix.com/support/technical/byproduct.affx?product=human_transcriptome) that can be
used to build an appropriate annotation for RMA. This work, along with some effort toward cleaning the data,
 has already been done in bioconductor. We use the following R script
 to extract information from the pd.hta.2.0 package and create files relevant to this pipeline.

> https://bioconductor.org/packages/release/data/annotation/html/pd.hta.2.0.html

> MacDonald JW (2017). pd.hta.2.0: Platform Design Info for Affymetrix HTA-2_0. R package version 3.12.2.

Use:

```
./hta_annotation.R
```

This outputs one file:

- pm_probe_annotation.rda
  - an R data file with a list called `pm_list` containing two data.frames. Each data.frame contains the probe IDs for
  the type of array used that are perfect match (PM) probe and the targets (probeset or transcript_cluster) to which
  they belong. The two data.frames are for each target type, one named `transcript_annotation` and one named
  `probeset_annotation`.  Each contains two columns, probe and probeset or transcript_cluster.

The pm_probe_annotation.rda is an R data file to decrease loading time into our background correction R step.

