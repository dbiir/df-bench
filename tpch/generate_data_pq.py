import os
import argparse
import shutil
import subprocess
from multiprocessing import Pool, set_start_method

import pyarrow.parquet as pq
import numpy as np
import pandas as pd

def load_lineitem(fpath):
    cols_names = ['L_ORDERKEY' , 'L_PARTKEY', 'L_SUPPKEY', 'L_LINENUMBER', 'L_QUANTITY',
            'L_EXTENDEDPRICE', 'L_DISCOUNT', 'L_TAX', 'L_RETURNFLAG', 'L_LINESTATUS', 'L_SHIPDATE',
            'L_COMMITDATE', 'L_RECEIPTDATE', 'L_SHIPINSTRUCT', 'L_SHIPMODE', 'L_COMMENT']
    cols = {'L_ORDERKEY' : np.int64, 'L_PARTKEY' : np.int64, 'L_SUPPKEY' : np.int64, 'L_LINENUMBER' : np.int64, 'L_QUANTITY' : np.float64,
            'L_EXTENDEDPRICE' : np.float64, 'L_DISCOUNT' : np.float64, 'L_TAX' : np.float64, 'L_RETURNFLAG' : str, 'L_LINESTATUS' : str, 'L_SHIPDATE' : str,
            'L_COMMITDATE' : str, 'L_RECEIPTDATE' : str, 'L_SHIPINSTRUCT' : str, 'L_SHIPMODE' : str, 'L_COMMENT' : str}
    rel = pd.read_csv(fpath, sep='|', header=None,
        names=cols_names,
        dtype=cols,
        parse_dates=[10, 11, 12]
        )
    return rel

def load_lineitem_with_date(fpath):
    cols_names = ['L_ORDERKEY' , 'L_PARTKEY', 'L_SUPPKEY', 'L_LINENUMBER', 'L_QUANTITY',
            'L_EXTENDEDPRICE', 'L_DISCOUNT', 'L_TAX', 'L_RETURNFLAG', 'L_LINESTATUS', 'L_SHIPDATE',
            'L_COMMITDATE', 'L_RECEIPTDATE', 'L_SHIPINSTRUCT', 'L_SHIPMODE', 'L_COMMENT']
    cols = {'L_ORDERKEY' : np.int64, 'L_PARTKEY' : np.int64, 'L_SUPPKEY' : np.int64, 'L_LINENUMBER' : np.int64, 'L_QUANTITY' : np.float64,
            'L_EXTENDEDPRICE' : np.float64, 'L_DISCOUNT' : np.float64, 'L_TAX' : np.float64, 'L_RETURNFLAG' : str, 'L_LINESTATUS' : str, 'L_SHIPDATE' : str,
            'L_COMMITDATE' : str, 'L_RECEIPTDATE' : str, 'L_SHIPINSTRUCT' : str, 'L_SHIPMODE' : str, 'L_COMMENT' : str}
    rel = pd.read_csv(fpath, sep='|', header=None,
        names=cols_names,
        dtype=cols,
        parse_dates=[10, 11, 12]
        )
    rel['L_SHIPDATE'] = [time.date() for time in rel['L_SHIPDATE']]
    rel['L_COMMITDATE'] = [time.date() for time in rel['L_COMMITDATE']]
    rel['L_RECEIPTDATE'] = [time.date() for time in rel['L_RECEIPTDATE']]
    return rel

def load_part(fpath):
    cols_names = ['P_PARTKEY', 'P_NAME', 'P_MFGR', 'P_BRAND',
            'P_TYPE', 'P_SIZE', 'P_CONTAINER',
            'P_RETAILPRICE', 'P_COMMENT']
    cols = {'P_PARTKEY' : np.int64, 'P_NAME' : str, 'P_MFGR' : str, 'P_BRAND' : str,
            'P_TYPE' : str, 'P_SIZE' : np.int64, 'P_CONTAINER' : str,
            'P_RETAILPRICE' : np.float64, 'P_COMMENT' : str}
    rel = pd.read_csv(fpath, sep='|', header=None,
        names=cols_names,
        dtype=cols
        )
    return rel

def load_orders(fpath):
    cols_names = ['O_ORDERKEY', 'O_CUSTKEY', 'O_ORDERSTATUS',
            'O_TOTALPRICE', 'O_ORDERDATE', 'O_ORDERPRIORITY',
            'O_CLERK', 'O_SHIPPRIORITY', 'O_COMMENT']
    cols = {'O_ORDERKEY' : np.int64, 'O_CUSTKEY' : np.int64, 'O_ORDERSTATUS' : str,
            'O_TOTALPRICE' : np.float64, 'O_ORDERDATE' : np.int64, 'O_ORDERPRIORITY' : str,
            'O_CLERK' : str, 'O_SHIPPRIORITY' : np.int64, 'O_COMMENT' : str}
    rel = pd.read_csv(fpath, sep='|', header=None,
        names=cols_names,
        dtype=cols,
        parse_dates=[4]
        )
    return rel

def load_orders_with_date(fpath):
    cols_names = ['O_ORDERKEY', 'O_CUSTKEY', 'O_ORDERSTATUS',
            'O_TOTALPRICE', 'O_ORDERDATE', 'O_ORDERPRIORITY',
            'O_CLERK', 'O_SHIPPRIORITY', 'O_COMMENT']
    cols = {'O_ORDERKEY' : np.int64, 'O_CUSTKEY' : np.int64, 'O_ORDERSTATUS' : str,
            'O_TOTALPRICE' : np.float64, 'O_ORDERDATE' : np.int64, 'O_ORDERPRIORITY' : str,
            'O_CLERK' : str, 'O_SHIPPRIORITY' : np.int64, 'O_COMMENT' : str}
    rel = pd.read_csv(fpath, sep='|', header=None,
        names=cols_names,
        dtype=cols,
        parse_dates=[4]
        )
    rel['O_ORDERDATE'] = [time.date() for time in rel['O_ORDERDATE']]
    return rel

def load_customer(fpath):
    cols_names = ['C_CUSTKEY', 'C_NAME',
            'C_ADDRESS', 'C_NATIONKEY',
            'C_PHONE', 'C_ACCTBAL',
            'C_MKTSEGMENT', 'C_COMMENT']
    cols = {'C_CUSTKEY' : np.int64, 'C_NAME' : str,
            'C_ADDRESS' : str, 'C_NATIONKEY' : np.int64,
            'C_PHONE' : str, 'C_ACCTBAL' : np.float64,
            'C_MKTSEGMENT' : str, 'C_COMMENT' : str}
    rel = pd.read_csv(fpath, sep='|', header=None,
        names=cols_names,
        dtype=cols
        )
    return rel

def load_nation(fpath):
    cols_names = ['N_NATIONKEY', 'N_NAME',
            'N_REGIONKEY', 'N_COMMENT']
    cols = {'N_NATIONKEY' : np.int64, 'N_NAME' : str,
            'N_REGIONKEY' : np.int64, 'N_COMMENT' : str}
    rel = pd.read_csv(fpath, sep='|', header=None,
        names=cols_names,
        dtype=cols
        )
    return rel

def load_region(fpath):
    cols_names = ['R_REGIONKEY', 'R_NAME', 'R_COMMENT']
    cols = {'R_REGIONKEY' : np.int64, 'R_NAME' : str, 'R_COMMENT' : str}
    rel = pd.read_csv(fpath, sep='|', header=None,
        names=cols_names,
        dtype=cols
        )
    return rel

def load_supplier(fpath):
    cols_names = ['S_SUPPKEY', 'S_NAME', 'S_ADDRESS',
            'S_NATIONKEY', 'S_PHONE', 'S_ACCTBAL',
            'S_COMMENT']
    cols = {'S_SUPPKEY' : np.int64, 'S_NAME' : str, 'S_ADDRESS' : str,
            'S_NATIONKEY' : np.int64, 'S_PHONE' : str, 'S_ACCTBAL' : np.float64,
            'S_COMMENT' : str}
    rel = pd.read_csv(fpath, sep='|', header=None,
        names=cols_names,
        dtype=cols
        )
    return rel

def load_partsupp(fpath):
    cols_names = ['PS_PARTKEY', 'PS_SUPPKEY', 'PS_AVAILQTY',
            'PS_SUPPLYCOST', 'PS_COMMENT']
    cols = {'PS_PARTKEY' : np.int64, 'PS_SUPPKEY' : np.int64, 'PS_AVAILQTY' : np.int64,
            'PS_SUPPLYCOST' : np.float64, 'PS_COMMENT' : str}
    rel = pd.read_csv(fpath, sep='|', header=None,
        names=cols_names,
        dtype=cols
        )
    return rel

# Change location of tpch-dbgen if not in same place as this script
tpch_dbgen_location = "./tpch-dbgen"

# First element is the table single character short-hand understood by dbgen
# Second element is the number of pieces we want the parquet dataset to have for that table
# Third element is the function that reads generated CSV to a pandas dataframe
def get_tables_info(num_pieces_base):
    tables = {}
    tables["customer"] = ("c", num_pieces_base, load_customer)
    tables["lineitem"] = ("L", num_pieces_base * 10, load_lineitem_with_date)
    # dbgen only produces one file for nation with SF1000
    tables["nation"] = ("n", 1, load_nation)
    tables["orders"] = ("O", num_pieces_base, load_orders_with_date)
    tables["part"] = ("P", num_pieces_base, load_part)
    tables["partsupp"] = ("S", num_pieces_base, load_partsupp)
    # dbgen only produces one file for region with SF1000
    tables["region"] = ("r", 1, load_region)
    tables["supplier"] = ("s", num_pieces_base // 100, load_supplier)
    return tables


def remove_file_if_exists(path):
    try:
        os.remove(path)
    except FileNotFoundError:
        pass


def to_parquet(args):
    (
        SCALE_FACTOR,
        table_name,
        table_short,
        load_func,
        piece,
        num_pieces,
        output_prefix,
    ) = args
    # generate `piece+1` of the table for the given scale factor with dbgen
    dbgen_fname = f"{tpch_dbgen_location}/{table_name}.tbl.{piece+1}"
    remove_file_if_exists(dbgen_fname)
    cmd = f"./dbgen -f -s {SCALE_FACTOR} -S {piece+1} -C {num_pieces} -T {table_short}"
    subprocess.run(cmd.split(), check=True, cwd=tpch_dbgen_location)
    # load csv file into pandas dataframe
    df = load_func(dbgen_fname)
    # csv file no longer needed, remove
    os.remove(dbgen_fname)
    # write dataframe to parquet
    zeros = "0" * (len(str(num_pieces)) - len(str(piece)))
    df.to_parquet(f"{output_prefix}/part-{zeros}{piece}.parquet")


def generate(
    tables, SCALE_FACTOR, folder, upload_to_s3, validate_dataset, num_processes
):

    if upload_to_s3:
        assert "AWS_ACCESS_KEY_ID" in os.environ, "AWS credentials not set"
    else:
        shutil.rmtree(f"{folder}", ignore_errors=True)
        os.mkdir(f"{folder}")

    if validate_dataset:
        fs = None
        if upload_to_s3:
            import s3fs

            fs = s3fs.S3FileSystem()

    for table_name, (table_short, num_pieces, load_func) in tables.items():

        if upload_to_s3:
            output_prefix = f"s3://{folder}/{table_name}.parquet"
        else:
            output_prefix = f"{folder}/{table_name}.parquet"
            if num_pieces > 1:
                os.mkdir(output_prefix)

        if num_pieces > 1:
            with Pool(num_processes) as pool:
                pool.map(
                    to_parquet,
                    [
                        (
                            SCALE_FACTOR,
                            table_name,
                            table_short,
                            load_func,
                            p,
                            num_pieces,
                            output_prefix,
                        )
                        for p in range(num_pieces)
                    ],
                )
        else:
            dbgen_fname = f"{tpch_dbgen_location}/{table_name}.tbl"
            # generate the whole table for the given scale factor with dbgen
            remove_file_if_exists(dbgen_fname)
            cmd = f"./dbgen -f -s {SCALE_FACTOR} -T {table_short}"
            subprocess.run(cmd.split(), check=True, cwd=tpch_dbgen_location)
            # load csv file into pandas dataframe
            df = load_func(dbgen_fname)
            # csv file no longer needed, remove
            os.remove(dbgen_fname)
            # write dataframe to parquet
            df.to_parquet(output_prefix)

        if validate_dataset:
            # make sure dataset is correct
            ds = pq.ParquetDataset(output_prefix, filesystem=fs)
            assert len(ds.pieces) == num_pieces


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="TPC-H Data Generation in Parquet Format"
    )
    parser.add_argument(
        "--SF",
        type=float,
        default=1,
        help="Data size number in GB",
    )
    parser.add_argument(
        "--folder",
        type=str,
        help="The folder containing output TPCH data",
    )
    parser.add_argument(
        "--validate_dataset",
        action="store_true",
        default=True,
        help="Validate each parquet dataset with pyarrow.parquet.ParquetDataset",
    )
    args = parser.parse_args()
    SCALE_FACTOR = args.SF
    folder = args.folder
    validate_dataset = args.validate_dataset
    num_processes = os.cpu_count() // 2
    upload_to_s3 = True if folder.startswith("s3://") else False
    # For SF1000 or more 1000
    if SCALE_FACTOR >= 1000:
        num_pieces_base = 1000
    else:
        # For smaller SFs
        num_pieces_base = 100
    tables = get_tables_info(num_pieces_base)
    set_start_method("spawn")
    generate(
        tables, SCALE_FACTOR, folder, upload_to_s3, validate_dataset, num_processes
    )
