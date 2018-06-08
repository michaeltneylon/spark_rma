# Walkthrough: Quantile Normalization and Summarization via Median Polish

## Start EMR

An EMR cluster can be created in the console or through the aws cli.

First, create a bootstrap script in S3 for installing some dependencies:

Create a script at s3://<bucket_name>/bootstrap/install_dependencies.sh as:

```
#! /bin/bash

sudo pip install pandas PyArrow
```

### Console Setup

On the EMR console page, select 'Create Cluster', then select 'Go to advanced options'.

- Step 1: Software Configuration
  - Select EMR release 'emr-5.12.1' or up
  - For software select at least Hadoop and Spark
- Step 2: Hardware Configuration
  - Select 'uniform instance groups'
  - Adjust Root device EBS volume size to max '100 GiB'
  - For Master node, select r4.2xlarge with 200 GiB EBS Storage
  - For Core nodes, select r4.8xlarge with 500 GiB EBS Storage and adjust instance count to 4
- Step 3: General Cluster Settings
  - Cluster name: SparkRMA - GSE88885
  - Tags:
    - Key: Name; Value: SparkRMA - GSE88885
  - Additional Options
    - Bootstrap Actions
      - Add a bootstrap action:
        - select custom action
        - select configure and add
        - name it 'Install Pandas'
        - add the s3 path to the bootstrap script created earlier:
          - s3://<bucket_name>/bootstrap/install_dependencies.sh
- Step 4: Security Options
  - Select your EC2 Key Pair Name
  - select the default permissions
  - EC2 Security Groups
    - for Master, and optionally Core & Task, add an additional security group that has port 22 open for SSH
- Create Cluster

## Copy Data into HDFS

Once the cluster is ready in 'waiting' status, ssh into the master node. The console will output the master's Public DNS.

`ssh -i <ssh key file> hadoop@<master_public_DNS>`

Use `distcp` to copy the annotated, background corrected data in parquet format from S3 to HDFS:

```
hadoop distcp \
  -Dmapreduce.map.memory.mb=5000 \
  s3://<bucket_name>/gse88885/background_corrected.parquet \
  background_corrected.parquet
```

## Download SparkRMA

`git clone https://github.com/michaeltneylon/spark_rma.git`

## Quantile Normalization

```
spark-submit \
  --driver-memory=55G \
  --executor-memory=10G \
  --conf spark.yarn.executor.memoryOverhead=1500M \
  --conf spark.dynamicAllocation.maxExecutors=128 \
  --conf spark.sql.shuffle.partitions=908 \
  spark_rma/spark_rma/quantile_normalization.py \
  -i background_corrected.parquet \
  -o quantile_normalized.parquet
```

You can view the progress of the job through the Spark UI. Get the link through the Yarn ResourceManager UI.
See this [EMR page on web interface URIs](https://docs.aws.amazon.com/emr/latest/ManagementGuide/emr-web-interfaces.html)

## Median Polish

Using the same EMR cluster from the previous step of quantile normalization, submit the median polish job:

```
spark-submit \
  --driver-memory=55G \
  --executor-memory=8G \
  --conf spark.yarn.executor.memoryOverhead=1500M \
  --conf spark.dynamicAllocation.maxExecutors=128 \
  spark_rma/spark_rma/median_polish.py \
  -i quantile_normalized.parquet \
  -o tc_expression.parquet \
  -ns 908 -rn 30000
```

## Move Results to S3


```
hadoop distcp \
  -Dmapreduce.map.memory.mb=5000 \
  tc_expression.parquet \
  s3://<bucket_name>/gse88885/tc_expression.parquet
```

The EMR cluster can now be terminated.