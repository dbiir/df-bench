import argparse
import json
import time
import traceback
from datetime import datetime

import polars as pl

from utils import append_row

def load_lineitem(root: str):
    data_path = root + "/lineitem.parquet/*.parquet"
    df = pl.read_parquet(data_path).lazy()
    return df


def load_part(root: str):
    data_path = root + "/part.parquet/*.parquet"
    df = pl.read_parquet(data_path).lazy()
    return df


def load_orders(root: str):
    data_path = root + "/orders.parquet/*.parquet"
    df = pl.read_parquet(data_path).lazy()
    return df


def load_customer(root: str):
    data_path = root + "/customer.parquet/*.parquet"
    df = pl.read_parquet(data_path).lazy()
    return df


def load_nation(root: str):
    data_path = root + "/nation.parquet"
    df = pl.read_parquet(data_path).lazy()
    return df


def load_region(root: str):
    data_path = root + "/region.parquet"
    df = pl.read_parquet(data_path).lazy()
    return df


def load_supplier(root: str):
    data_path = root + "/supplier.parquet"
    df = pl.read_parquet(data_path).lazy()
    return df


def load_partsupp(root: str):
    data_path = root + "/partsupp.parquet/*.parquet"
    df = pl.read_parquet(data_path).lazy()
    return df


def q01(root: str):
    lineitem = load_lineitem(root)

    date = datetime(1998, 9, 2)

    q_final = (
        lineitem.filter(pl.col("L_SHIPDATE") <= date)
        .groupby(["L_RETURNFLAG", "L_LINESTATUS"])
        .agg(
            [
                pl.sum("L_QUANTITY").alias("SUM_QTY"),
                pl.sum("L_EXTENDEDPRICE").alias("SUM_BASE_PRICE"),
                (pl.col("L_EXTENDEDPRICE") * (1 - pl.col("L_DISCOUNT")))
                .sum()
                .alias("SUM_DISC_PRICE"),
                (
                    pl.col("L_EXTENDEDPRICE")
                    * (1.0 - pl.col("L_DISCOUNT"))
                    * (1.0 + pl.col("L_TAX"))
                )
                .sum()
                .alias("SUM_CHARGE"),
                pl.mean("L_QUANTITY").alias("AVG_QTY"),
                pl.mean("L_EXTENDEDPRICE").alias("AVG_PRICE"),
                pl.mean("L_DISCOUNT").alias("AVG_DISC"),
                pl.count().alias("COUNT_ORDER"),
            ],
        )
        .sort(["L_RETURNFLAG", "L_LINESTATUS"])
    )

    result = q_final.collect()
    print(result)


def q02(root: str): 
    region_ds = load_region(root)
    nation_ds = load_nation(root)
    supplier_ds = load_supplier(root)
    part_ds = load_part(root)
    part_supp_ds = load_partsupp(root)

    result_q1 = (
        part_ds.join(part_supp_ds, left_on="P_PARTKEY", right_on="PS_PARTKEY")
        .join(supplier_ds, left_on="PS_SUPPKEY", right_on="S_SUPPKEY")
        .join(nation_ds, left_on="S_NATIONKEY", right_on="N_NATIONKEY")
        .join(region_ds, left_on="N_REGIONKEY", right_on="R_REGIONKEY")
        .filter(pl.col("P_SIZE") == 15)
        .filter(pl.col("P_TYPE").str.ends_with("BRASS"))
        .filter(pl.col("R_NAME") == "EUROPE")
    ).cache()

    final_cols = [
        "S_ACCTBAL",
        "S_NAME",
        "N_NAME",
        "P_PARTKEY",
        "P_MFGR",
        "S_ADDRESS",
        "S_PHONE",
        "S_COMMENT",
    ]

    q_final = (
        result_q1.groupby("P_PARTKEY")
        .agg(pl.min("PS_SUPPLYCOST").alias("PS_SUPPLYCOST"))
        .join(
            result_q1,
            left_on=["P_PARTKEY", "PS_SUPPLYCOST"],
            right_on=["P_PARTKEY", "PS_SUPPLYCOST"],
        )
        .select(final_cols)
        .sort(
            by=["S_ACCTBAL", "N_NAME", "S_NAME", "P_PARTKEY"],
            descending=[True, False, False, False],
        )
        .with_columns(pl.col(pl.datatypes.Utf8).str.strip().keep_name())
    )

    result = q_final.collect()
    print(result)


def q03(root: str):
    var1 = var2 = datetime(1995, 3, 4)

    customer_ds = load_customer(root)
    line_item_ds = load_lineitem(root)
    orders_ds = load_orders(root)

    q_final = (
        customer_ds.filter(pl.col("C_MKTSEGMENT") == "HOUSEHOLD")
        .join(orders_ds, left_on="C_CUSTKEY", right_on="O_CUSTKEY")
        .join(line_item_ds, left_on="O_ORDERKEY", right_on="L_ORDERKEY")
        .filter(pl.col("O_ORDERDATE") < var2)
        .filter(pl.col("L_SHIPDATE") > var1)
        .with_columns(
            (pl.col("L_EXTENDEDPRICE") * (1 - pl.col("L_DISCOUNT"))).alias("REVENUE")
        )
        .groupby(["O_ORDERKEY", "O_ORDERDATE", "O_SHIPPRIORITY"])
        .agg([pl.sum("REVENUE")])
        .select(
            [
                pl.col("O_ORDERKEY").alias("L_ORDERKEY"),
                "REVENUE",
                "O_ORDERDATE",
                "O_SHIPPRIORITY",
            ]
        )
        .sort(by=["REVENUE", "O_ORDERDATE"], descending=[True, False])
    )
    result = q_final.collect()
    print(result)

    return result


def q04(root: str):
    date1 = datetime(1993, 11, 1)
    date2 = datetime(1993, 8, 1)

    line_item_ds = load_lineitem(root)
    orders_ds = load_orders(root)

    q_final = (
        line_item_ds.join(orders_ds, left_on="L_ORDERKEY", right_on="O_ORDERKEY")
        .filter(pl.col("O_ORDERDATE") >= date2)
        .filter(pl.col("O_ORDERDATE") < date1)
        .filter(pl.col("L_COMMITDATE") < pl.col("L_RECEIPTDATE"))
        .unique(subset=["O_ORDERPRIORITY", "L_ORDERKEY"])
        .groupby("O_ORDERPRIORITY")
        .agg(pl.count().alias("ORDER_COUNT"))
        .sort(by="O_ORDERPRIORITY")
        .with_columns(pl.col("ORDER_COUNT").cast(pl.datatypes.Int64))
    )
    result = q_final.collect()
    print(result)

    return result


def q05(root: str):
    var1 = "ASIA"
    var2 = datetime(1996, 1, 1)
    var3 = datetime(1997, 1, 1)

    region_ds = load_region(root)
    nation_ds = load_nation(root)
    customer_ds = load_customer(root)
    line_item_ds = load_lineitem(root)
    orders_ds = load_orders(root)
    supplier_ds = load_supplier(root)

    q_final = (
        region_ds.join(nation_ds, left_on="R_REGIONKEY", right_on="N_REGIONKEY")
        .join(customer_ds, left_on="N_NATIONKEY", right_on="C_NATIONKEY")
        .join(orders_ds, left_on="C_CUSTKEY", right_on="O_CUSTKEY")
        .join(line_item_ds, left_on="O_ORDERKEY", right_on="L_ORDERKEY")
        .join(
            supplier_ds,
            left_on=["L_SUPPKEY", "N_NATIONKEY"],
            right_on=["S_SUPPKEY", "S_NATIONKEY"],
        )
        .filter(pl.col("R_NAME") == var1)
        .filter(pl.col("O_ORDERDATE") >= var2)
        .filter(pl.col("O_ORDERDATE") < var3)
        .with_columns(
            (pl.col("L_EXTENDEDPRICE") * (1 - pl.col("L_DISCOUNT"))).alias("REVENUE")
        )
        .groupby("N_NAME")
        .agg([pl.sum("REVENUE")])
        .sort(by="REVENUE", descending=True)
    )

    result = q_final.collect()
    print(result)

    return result


def q06(root: str):
    var1 = datetime(1996, 1, 1)
    var2 = datetime(1997, 1, 1)
    var3 = 24

    line_item_ds = load_lineitem(root)

    q_final = (
        line_item_ds.filter(pl.col("L_SHIPDATE") >= var1)
        .filter(pl.col("L_SHIPDATE") < var2)
        .filter((pl.col("L_DISCOUNT") >= 0.08) & (pl.col("L_DISCOUNT") <= 0.1))
        .filter(pl.col("L_QUANTITY") < var3)
        .with_column(
            (pl.col("L_EXTENDEDPRICE") * pl.col("L_DISCOUNT")).alias("REVENUE")
        )
        .select(pl.sum("REVENUE").alias("REVENUE"))
    )
    result = q_final.collect()
    print(result)

    return result


def q07(root: str):
    nation_ds = load_nation(root)
    customer_ds = load_customer(root)
    line_item_ds = load_lineitem(root)
    orders_ds = load_orders(root)
    supplier_ds = load_supplier(root)

    n1 = nation_ds.filter(pl.col("N_NAME") == "FRANCE")
    n2 = nation_ds.filter(pl.col("N_NAME") == "GERMANY")

    df1 = (
        customer_ds.join(n1, left_on="C_NATIONKEY", right_on="N_NATIONKEY")
        .join(orders_ds, left_on="C_CUSTKEY", right_on="O_CUSTKEY")
        .rename({"N_NAME": "CUST_NATION"})
        .join(line_item_ds, left_on="O_ORDERKEY", right_on="L_ORDERKEY")
        .join(supplier_ds, left_on="L_SUPPKEY", right_on="S_SUPPKEY")
        .join(n2, left_on="S_NATIONKEY", right_on="N_NATIONKEY")
        .rename({"N_NAME": "SUPP_NATION"})
    )

    df2 = (
        customer_ds.join(n2, left_on="C_NATIONKEY", right_on="N_NATIONKEY")
        .join(orders_ds, left_on="C_CUSTKEY", right_on="O_CUSTKEY")
        .rename({"N_NAME": "CUST_NATION"})
        .join(line_item_ds, left_on="O_ORDERKEY", right_on="L_ORDERKEY")
        .join(supplier_ds, left_on="L_SUPPKEY", right_on="S_SUPPKEY")
        .join(n1, left_on="S_NATIONKEY", right_on="N_NATIONKEY")
        .rename({"N_NAME": "SUPP_NATION"})
    )

    q_final = (
        pl.concat([df1, df2])
        .filter(pl.col("L_SHIPDATE") >= datetime(1995, 1, 1))
        .filter(pl.col("L_SHIPDATE") < datetime(1997, 1, 1))
        .with_column(
            (pl.col("L_EXTENDEDPRICE") * (1 - pl.col("L_DISCOUNT"))).alias("VOLUME")
        )
        .with_column(pl.col("L_SHIPDATE").dt.year().alias("L_YEAR"))
        .groupby(["SUPP_NATION", "CUST_NATION", "L_YEAR"])
        .agg([pl.sum("VOLUME").alias("REVENUE")])
        .sort(by=["SUPP_NATION", "CUST_NATION", "L_YEAR"])
    )

    result = q_final.collect()
    print(result)

    return result


query_to_loaders = {
    1: [load_lineitem],
    2: [load_part, load_partsupp, load_supplier, load_nation, load_region],
    3: [load_lineitem, load_orders, load_customer],
    4: [load_lineitem, load_orders],
    5: [
        load_lineitem,
        load_orders,
        load_customer,
        load_nation,
        load_region,
        load_supplier,
    ],
    6: [load_lineitem],
    7: [load_lineitem, load_supplier, load_orders, load_customer, load_nation],
    # 8: [
    #     load_part,
    #     load_lineitem,
    #     load_supplier,
    #     load_orders,
    #     load_customer,
    #     load_nation,
    #     load_region,
    # ],
    # 9: [
    #     load_lineitem,
    #     load_orders,
    #     load_part,
    #     load_nation,
    #     load_partsupp,
    #     load_supplier,
    # ],
    # 10: [load_lineitem, load_orders, load_nation, load_customer],
    # 11: [load_partsupp, load_supplier, load_nation],
    # 12: [load_lineitem, load_orders],
    # 13: [load_customer, load_orders],
    # 14: [load_lineitem, load_part],
    # 15: [load_lineitem, load_supplier],
    # 16: [load_part, load_partsupp, load_supplier],
    # 17: [load_lineitem, load_part],
    # 18: [load_lineitem, load_orders, load_customer],
    # 19: [load_lineitem, load_part],
    # 20: [load_lineitem, load_part, load_nation, load_partsupp, load_supplier],
    # 21: [load_lineitem, load_orders, load_supplier, load_nation],
    # 22: [load_customer, load_orders],
}

query_to_runner = {
    1: q01,
    2: q02,
    3: q03,
    4: q04,
    5: q05,
    6: q06,
    7: q07,
    # 8: q08,
    # 9: q09,
    # 10: q10,
    # 11: q11,
    # 12: q12,
    # 13: q13,
    # 14: q14,
    # 15: q15,
    # 16: q16,
    # 17: q17,
    # 18: q18,
    # 19: q19,
    # 20: q20,
    # 21: q21,
    # 22: q22,
}

def run_queries(path, queries, log_timing = True, io_warmup = True):
    total_start = time.time()
    for query in queries:
        try:
            if io_warmup:
                query_to_runner[query](path)
            t1 = time.time()
            query_to_runner[query](path)
            dur = time.time() - t1
            success = True
        except Exception as e: 
            print(''.join(traceback.TracebackException.from_exception(e).format()))
            dur = 0.0
            success = False
        finally:
            if log_timing:
                append_row("polars", query, dur, pl.__version__, success)
    print(f"Total query execution time (s): {time.time() - total_start}")


def main():
    parser = argparse.ArgumentParser(description="polars TPC-H benchmark.")
    parser.add_argument(
        "--path", type=str, required=True, help="Path to the TPC-H dataset."
    )
    parser.add_argument(
        "--storage_options",
        type=str,
        required=False,
        help="Path to the storage options json file.",
    )
    parser.add_argument(
        "--queries",
        type=int,
        nargs="+",
        required=False,
        help="Comma separated TPC-H queries to run.",
    )
    parser.add_argument(
        "--log_timing",
        action="store_true",
        help="Log time metrics or not.",
    )
    parser.add_argument(
        "--io_warmup",
        action="store_true",
        help="Warm up IO data loading or not.",
    )
    args = parser.parse_args()
    log_timing = args.log_timing
    io_warmup = args.io_warmup

    # path to TPC-H data in parquet.
    path = args.path
    print(f"Path: {path}")

    # credentials to access the datasource.
    storage_options = {}
    if args.storage_options is not None:
        with open(args.storage_options, "r") as fp:
            storage_options = json.load(fp)
    print(f"Storage options: {storage_options}")

    queries = list(range(1, 23))
    if args.queries is not None:
        queries = args.queries
    print(f"Queries to run: {queries}")

    run_queries(path, queries, log_timing, io_warmup)


if __name__ == "__main__":
    main()