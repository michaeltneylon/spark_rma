# Overview

## Annotation and Background Correction

Annotation and background correction are not done in Spark, because they are done at an individual array level and not
dependent on other arrays. This means it is not a data-limiting step in RMA and makes it easy to parallelize.
Annotation is the process of mapping probes to their perfect match (PM) targets. That is typically a transcript cluster
or probeset.

R scripts are used for annotation and background correction, with the help of some Bioconductor packages.
We also convert the output of this step into parquet for consumption in the following Spark steps.

## Quantile Normalization

Quantile normalization removes array effects by normalizing each array against all others. This is a memory intensive
step that requires all the arrays be evaluated at the same time. This is done using Spark window functions.
The output of the values are log transformed (log2) at the start of the next step.

## Summarization via Median Polish

Summarization takes the normalized, log-transformed probe values and summarizes them to their target to acquire an
expression value for the target (a transcript cluster or probeset). This step groups all the same probes for a target
from all samples before assigning the resulting expression values to each sample for that same target. This makes it a highly iterative,
high memory step. We do this step using Spark UDF's to group the probes across samples and carry out the median polish
using pandas and NumPy.

