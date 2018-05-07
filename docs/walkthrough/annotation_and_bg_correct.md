# Walkthrough: Annotation and Background Correction

### Configure and Start CfnCluster

After installing CfnCluster according to the official docs listed above, setup your cluster config. For this example,
use a 4 worker node cluster of r4.8xlarge and 1 master node of r4.2xlarge.
Setup a CfnCluster config (typically located at ~/.cfncluster/config) with the following parameters (fill in your own
values where there are asterisks):

```
[aws]
aws_region_name = ****
aws_access_key_id = *****
aws_secret_access_key = *****

[cluster default]
vpc_settings = public
key_name = *****
master_instance_type = r4.2xlarge
compute_instance_type = r4.8xlarge
initial_queue_size = 4
max_queue_size = 4
maintain_initial_size = true
ebs_settings = custom
master_root_volume_size = 50
compute_root_volume_size = 100

[vpc public]
master_subnet_id = *****
vpc_id = *****
vpc_security_group = *****

[global]
update_check = true
sanity_check = true
cluster_template = default

[ebs custom]
volume_size = 2000
```

Tip: To save money, you can set the `initial_queue_size` and `max_queue_size` to 0 initially while you install software
and download data, then change the values and update the cluster to start the workers.

- Start the cluster:
  - `cfncluster create sparkrma`
- SSH into the master. The Master IP or Public DNS should be output by CfnCluster when it's ready.
  - `ssh -i <PEM file> ec2-user@$PUBLIC_DNS`  # SSH Key File should match the key name specified in config above

### Setup Software on the Grid Engine Cluster

A few R/Bioconductor and Python packages need to be installed on the cluster.

This example shows how to use packrat and conda to make shared R and Python environments.

#### R

Install R and some dependencies:

`sudo yum -y install libcurl libcurl-devel openssl-devel R`

create packrat environment and install packages:

```
cd /shared/
mkdir renv
cd renv
R
> install.packages('devtools')
> devtools::install_github('rstudio/packrat')
> packrat::init()
> setRepositories()  # select 1 2 3
> install.packages('preprocessCore')
> install.packages("affyio")
> install.packages("docopt")
> install.packages('pd.hta.2.0')
```


#### Python

Install Miniconda:

```
wget https://repo.continuum.io/miniconda/Miniconda3-latest-Linux-x86_64.sh
sh Miniconda3-latest-Linux-x86_64.sh
```

During the setup:
- change install location to /shared/miniconda3/
- choose append to .bashrc

Then source it and install some packages that SparkRMA needs for converting the output to snappy-parquet format:

```
source ~/.bashrc
conda install fastparquet python-snappy
```

### Download Data and SparkRMA

Download GSE88885:

```
cd /shared/
mkdir gse88885
cd gse88885
wget ftp://ftp.ncbi.nlm.nih.gov/geo/series/GSE88nnn/GSE88885/suppl/GSE88885_RAW.tar
mkdir raw
tar xvf GSE88885_RAW.tar -C raw/
```

Download SparkRMA:

```
cd /shared/
git clone https://github.com/michaeltneylon/spark_rma.git
```

Edit /shared/spark_rma/spark_rma/backgroundCorrect.R and /shared/helper/hta_annotation.R to make the R packrat
environment available to it.
At top of these files, on a line after the shebang, add:

`source("/shared/renv/.Rprofile", chdir=TRUE)`


### Annotation, Background Correction, and Conversion to Parquet

First, generate the annotation file:

```
cd /shared/spark_rma/helper
./hta_annotation.R
```

Write a bash script to submit as an array job on the cluster, one job per sample. This is going to
do annotation, with target specified as `-a` (annotation_type) with options `ps` or `tc`. In this example, `tc` is selected
for transcript cluster annotation. Then the same script completes background correction and then converts the result to parquet.

Make our output directory before the job:
`mkdir -p /shared/gse88885/background_corrected`

Write a shell script, let's call this gse88885.sh:

```
#! /bin/bash
#$ -t 1-908
#$ -cwd

input_dir=/shared/gse88885/raw
output_dir=/shared/gse88885/background_corrected
scratch=/scratch
bg_correct_home=/shared/spark_rma/spark_rma/
convert_home=/shared/spark_rma/helper
probe_list=/shared/spark_rma/helper/pm_probe_annotation.rda

# setup environment
export PATH="/shared/miniconda3/bin:$PATH"

current_file=$(ls $input_dir/* | sed "${SGE_TASK_ID}q;d")
echo "processing $current_file"
output=${current_file##*/}
output=${output%.CEL.gz}
cd $bg_correct_home
rm -f $scratch/$output.background_corrected  # in case the file exists already from a previous run.
./backgroundCorrect.R -p $probe_list -i $current_file -o $scratch/$output.background_corrected -f text -a tc
sudo chmod 775 $scratch/$output.background_corrected
cd $convert_home
./convert_to_parquet.py $scratch/$output.background_corrected $output_dir/$output.background_corrected.parquet
sudo rm $scratch/$output.background_corrected
```

This script can be anywhere since we defined absolute paths in it, so create it at $HOME/gse88885.sh

Then submit the job:

`qsub gse88885.sh`

### Move to S3

Once the array job is finished, move the results into S3 using the aws cli. Using an existing bucket
(create one if one does not already exist), copy the results to S3:

```
aws s3 cp \
  --recursive \
  /shared/gse88885/background_corrected/ \
  s3://<bucket name>/gse88885/background_corrected.parquet
```

The cluster may now be deleted: `cfncluster delete sparkrma`