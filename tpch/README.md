# TPC-H benchmark for DataFrame Systems

## Introduction

## Installation

Create an isolated environment using `conda` or `virtualenv`. In this case, we use `conda`. 

```bash
conda create -n tpch
conda activate tpch
conda install python=3.10
```

Install all the packages.

```bash
pip install -r requirements.txt
```

## Genernate TPC-H data

### Build tpch-dbgen

```bash
cd tpch-dbgen
make
cd ..
```

If you make `tpch-dbgen` before, execute `make clean` to clean the previous build.

### Generate TPC-H data

#### Usage

```bash
python generate_data_pq.py [-h] --folder FOLDER [--SF N] [--validate_dataset]

    -h, --help       Show this help message and exit
    folder FOLDER: output folder name (can be local folder or S3 bucket)
    SF N: data size number in GB (Default 1)
    validate_dataset: Validate each parquet dataset with pyarrow.parquet.ParquetDataset (Default True)
```

#### Example

Generate scale factor 1 data locally:

```bash
python generate_data_pq.py --SF 1 --folder SF1
```

Generate scale factor 1000 data and upload to S3 bucket:

```bash
python generate_data_pq.py --SF 1000 --folder s3://bucket-name/
```

NOTES:

This script assumes `tpch-dbgen` is in the same directory. If you downloaded it at another location, make sure to update tpch_dbgen_location in the script with the new location.

If using S3 bucket, install s3fs and add your AWS credentials.

## Run the queries

Go to the `queries` folder and execute the queries.

```bash
cd queries

# run a specific query
python pandas_query.py --path ../SF1 --queries 2

# run all queries and log the time metrics
# each query is executed twice, the first attempt is for data loading warmup
python pandas_query.py --path ../SF1 --log_timing --io_warmup
```
