# Walkthrough an Example

HTA 2.0 are high-density microarrays that will be used for this example. This walk-through will also provide some
detailed instructions using Amazon Web Services to step through all the parts of SparkRMA. These examples will use a
small cluster of 4 workers of r4.8xlarge and 1 master node of r4.2xlarge. For all configuration options and arguments of
each script, see the READMEs in GitHub or the command-line interface help.

## Data

[908 HTA 2.0 Samples from the Gene Expression Omnibus (GEO) from a Lilly Clinical Trial on
Systemic Lupus Erythematosus (SLE).](https://www.ncbi.nlm.nih.gov/geo/query/acc.cgi?acc=GSE88885)

## CfnCluster

AWS CloudFormation Cluster is used to start a SunGrid Engine cluster for the annotation and background correction
steps. Array jobs are a suitable method for completing this step. If you have a grid engine cluster available, the same
directions could be used there.

Follow the instructions on setting up and using CfnCluster from
the [official docs.](http://cfncluster.readthedocs.io/en/latest/)

## EMR

AWS Elastic MapReduce is used for the quantile normalization and median polish steps. This provides a Hadoop/YARN
 cluster for deploying Spark. See the [getting started page](https://aws.amazon.com/emr/getting-started/) at Amazon.

----

Continue onto the next page to follow the tutorial