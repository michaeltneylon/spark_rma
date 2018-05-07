# Tests

Tests for spark-rma.

## Install

Install dependencies
`pip install -r requirements.txt`

also install `spark_rma` as a package so the modules can be imported into the tests.
`python setup.py install`

The tests look for your spark installation automatically. If it does not work, you may need to set SPARK_HOME.

## Run

Run pytest test using the `-m` module statement so the proper environment where spark_rma is installed will be used.

`cd tests`
`python -m pytest . --cov=spark_rma`
