import os
import sys
import argparse
import json
import time
import traceback
from datetime import datetime

import polars as pl
from polars import testing as pl_test

current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)
from common_utils import append_row, ANSWERS_BASE_DIR

dataset_dict = {}

def _load_data(data_path: str, data_key: str, include_io: bool):
    if data_key not in dataset_dict:
        # load data using eagar mode with `collect()`
        # turn the data into lazy mode as queries are in lazy mode
        # the `dataset_dict` stores `LazyFrame`
        result = pl.scan_parquet(data_path).collect().rechunk().lazy()
        dataset_dict[data_key] = result
    else:
        # if we want to include_io, use the lazy mode
        if include_io:
            result = pl.scan_parquet(data_path)
            return result
        result = dataset_dict[data_key]
    return result

def load_lineitem(root: str, include_io: bool=False):
    data_path = root + "/lineitem.parquet/*.parquet"
    return _load_data(data_path, "lineitem", include_io)


def load_part(root: str, include_io: bool=False):
    data_path = root + "/part.parquet/*.parquet"
    return _load_data(data_path, "part", include_io)


def load_orders(root: str, include_io: bool=False):
    data_path = root + "/orders.parquet/*.parquet"
    return _load_data(data_path, "orders", include_io)


def load_customer(root: str, include_io: bool=False):
    data_path = root + "/customer.parquet/*.parquet"
    return _load_data(data_path, "customer", include_io)


def load_nation(root: str, include_io: bool=False):
    data_path = root + "/nation.parquet"
    return _load_data(data_path, "nation", include_io)


def load_region(root: str, include_io: bool=False):
    data_path = root + "/region.parquet"
    return _load_data(data_path, "region", include_io)


def load_supplier(root: str, include_io: bool=False):
    data_path = root + "/supplier.parquet"
    return _load_data(data_path, "supplier", include_io)


def load_partsupp(root: str, include_io: bool=False):
    data_path = root + "/partsupp.parquet/*.parquet"
    return _load_data(data_path, "partsupp", include_io)


def q01(root: str,
        include_io: bool=False):
    lineitem = load_lineitem(root, include_io)

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
    return result


def q02(root: str, include_io: bool=False): 
    region_ds = load_region(root, include_io)
    nation_ds = load_nation(root, include_io)
    supplier_ds = load_supplier(root, include_io)
    part_ds = load_part(root, include_io)
    part_supp_ds = load_partsupp(root, include_io)

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

    result = q_final.head(100).collect()
    return result


def q03(root: str, include_io: bool=False):
    var1 = var2 = datetime(1995, 3, 4)

    customer_ds = load_customer(root, include_io)
    line_item_ds = load_lineitem(root, include_io)
    orders_ds = load_orders(root, include_io)

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
    result = q_final.head(10).collect()
    return result


def q04(root: str, include_io: bool=False):
    date1 = datetime(1993, 11, 1)
    date2 = datetime(1993, 8, 1)

    line_item_ds = load_lineitem(root, include_io)
    orders_ds = load_orders(root, include_io)

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
    return result


def q05(root: str, include_io: bool=False):
    var1 = "ASIA"
    var2 = datetime(1996, 1, 1)
    var3 = datetime(1997, 1, 1)

    region_ds = load_region(root, include_io)
    nation_ds = load_nation(root, include_io)
    customer_ds = load_customer(root, include_io)
    line_item_ds = load_lineitem(root, include_io)
    orders_ds = load_orders(root, include_io)
    supplier_ds = load_supplier(root, include_io)

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
    return result


def q06(root: str, include_io: bool=False):
    var1 = datetime(1996, 1, 1)
    var2 = datetime(1997, 1, 1)
    var3 = 24

    line_item_ds = load_lineitem(root, include_io)

    q_final = (
        line_item_ds.filter(pl.col("L_SHIPDATE") >= var1)
        .filter(pl.col("L_SHIPDATE") < var2)
        .filter((pl.col("L_DISCOUNT") >= 0.08) & (pl.col("L_DISCOUNT") <= 0.1))
        .filter(pl.col("L_QUANTITY") < var3)
        .with_columns(
            (pl.col("L_EXTENDEDPRICE") * pl.col("L_DISCOUNT")).alias("REVENUE")
        )
        .select(pl.sum("REVENUE").alias("REVENUE"))
    )
    result = q_final.collect()
    return result


def q07(root: str, include_io: bool=False):
    nation_ds = load_nation(root, include_io)
    customer_ds = load_customer(root, include_io)
    line_item_ds = load_lineitem(root, include_io)
    orders_ds = load_orders(root, include_io)
    supplier_ds = load_supplier(root, include_io)

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
        .with_columns(
            (pl.col("L_EXTENDEDPRICE") * (1 - pl.col("L_DISCOUNT"))).alias("VOLUME")
        )
        .with_columns(pl.col("L_SHIPDATE").dt.year().alias("L_YEAR"))
        .groupby(["SUPP_NATION", "CUST_NATION", "L_YEAR"])
        .agg([pl.sum("VOLUME").alias("REVENUE")])
        .sort(by=["SUPP_NATION", "CUST_NATION", "L_YEAR"])
    )

    result = q_final.collect()
    return result

def q10(root: str, include_io: bool=False):
    var_1 = datetime(1993, 10, 1)
    var_2 = datetime(1994, 1, 1)

    customer_ds = load_customer(root, include_io)
    nation_ds = load_nation(root, include_io)
    line_item_ds = load_lineitem(root, include_io)
    orders_ds = load_orders(root, include_io)
    supplier_ds = load_supplier(root, include_io)

    q_final = (
        customer_ds.join(orders_ds, left_on="C_CUSTKEY", right_on="O_CUSTKEY")
        .join(line_item_ds, left_on="O_ORDERKEY", right_on="L_ORDERKEY")
        .join(nation_ds, left_on="C_NATIONKEY", right_on="N_NATIONKEY")
        .filter(pl.col("O_ORDERDATE").is_between(var_1, var_2, closed="left"))
        .filter(pl.col("L_RETURNFLAG") == "R")
        .groupby(
            [
                "C_CUSTKEY",
                "C_NAME",
                "C_ACCTBAL",
                "C_PHONE",
                "N_NAME",
                "C_ADDRESS",
                "C_COMMENT",
            ]
        )
        .agg(
            [
                (pl.col("L_EXTENDEDPRICE") * (1 - pl.col("L_DISCOUNT")))
                .sum()
                .round(2)
                .alias("REVENUE")
            ]
        )
        .with_columns(pl.col("C_ADDRESS").str.strip(), pl.col("C_COMMENT").str.strip())
        .select(
            [
                "C_CUSTKEY",
                "C_NAME",
                "REVENUE",
                "C_ACCTBAL",
                "N_NAME",
                "C_ADDRESS",
                "C_PHONE",
                "C_COMMENT",
            ]
        )
        .sort(by="REVENUE", descending=True)
        .limit(20)
    )

    result = q_final.collect()

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
    10: [load_lineitem, load_orders, load_nation, load_customer],
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
    10: q10,
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


def get_query_answer(query: int, base_dir: str = ANSWERS_BASE_DIR) -> pl.LazyFrame:
    answer_ldf = pl.scan_csv(
        os.path.join(base_dir, f"q{query}.out"),
        separator="|",
        has_header=True,
        try_parse_dates=True,
    )
    cols = answer_ldf.columns
    answer_ldf = answer_ldf.select(
        [pl.col(c).alias(c.strip()) for c in cols]
    ).with_columns([pl.col(pl.datatypes.Utf8).str.strip().keep_name()])

    return answer_ldf.collect()


def test_results(q_num: int, result_df: pl.DataFrame):
    answer = get_query_answer(q_num)

    for column_index in range(len(answer.columns)):
        answer_column_name = answer.columns[column_index]
        result_column_name = result_df.columns[column_index]

        s1 = result_df.get_column(result_column_name)
        s2 = answer.get_column(answer_column_name)
        
        pl_test.assert_series_equal(left=s1, right=s2, 
                check_dtype=False,
                check_names=False, 
                check_exact=False, 
                rtol=1e-2)

def run_queries(path, queries, 
            log_timing = True, 
            include_io = False, 
            test_answer = True, 
            print_result = False):
    print("Start data loading")
    total_start = time.time()
    for query in queries:
        loaders = query_to_loaders[query]
        for loader in loaders:
            loader(path, False)
    print(f"Data loading time (s): {time.time() - total_start}")
    total_start = time.time()
    for query in queries:
        try:
            t1 = time.time()
            result = query_to_runner[query](path, include_io)
            dur = time.time() - t1
            success = True
            if test_answer:
                test_results(query, result)
        except Exception as e: 
            print(''.join(traceback.TracebackException.from_exception(e).format()))
            dur = 0.0
            success = False
        finally:
            if log_timing:
                append_row("polars", query, dur, pl.__version__, success)
    print(f"Total query execution time (s): {time.time() - total_start}")


def main():
    parser = argparse.ArgumentParser(description="TPC-H benchmark.")
    parser.add_argument(
        "--path", type=str, required=True, help="path to the TPC-H dataset."
    )
    parser.add_argument(
        "--storage_options",
        type=str,
        required=False,
        help="storage options json file.",
    )
    parser.add_argument(
        "--queries",
        type=int,
        nargs="+",
        required=False,
        help="comma separated TPC-H queries to run.",
    )
    parser.add_argument(
        "--log_timing",
        action="store_true",
        help="log time metrics or not.",
    )
    parser.add_argument(
        "--include_io",
        action="store_true",
        help="include IO or not.",
    )
    parser.add_argument(
        "--test_answer",
        action="store_true",
        help="test results with official answers.",
    )
    parser.add_argument(
        "--print_result",
        action="store_true",
        help="print result.",
    )
    args = parser.parse_args()
    log_timing = args.log_timing
    include_io = args.include_io
    test_answer = args.test_answer
    print_result = args.print_result

    # path to TPC-H data in parquet.
    path = args.path
    print(f"Path: {path}")

    # credentials to access the datasource.
    storage_options = {}
    if args.storage_options is not None:
        with open(args.storage_options, "r") as fp:
            storage_options = json.load(fp)
    print(f"Storage options: {storage_options}")

    queries = list(range(1, 8))
    if args.queries is not None:
        queries = args.queries
    print(f"Queries to run: {queries}")

    run_queries(path, queries, log_timing, include_io, test_answer, print_result)


if __name__ == "__main__":
    main()