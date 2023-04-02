import argparse
import json
import time
import traceback
from datetime import datetime

import numpy as np
import vaex

from utils import append_row

def load_lineitem(root: str):
    data_path = root + "/lineitem.pq/*.parquet"
    df = vaex.open(data_path)

    df["L_SHIPDATE"] = df.L_SHIPDATE.astype('datetime64[ns]')
    df["L_RECEIPTDATE"] = df.L_RECEIPTDATE.astype('datetime64[ns]')
    df["L_COMMITDATE"] = df.L_COMMITDATE.astype('datetime64[ns]')
    return df


def load_part(root: str):
    data_path = root + "/part.pq/*.parquet"
    df = vaex.open(data_path)
    return df


def load_orders(root: str):
    data_path = root + "/orders.pq/*.parquet"
    df = vaex.open(data_path)
    df["O_ORDERDATE"] = df.O_ORDERDATE.astype('datetime64[ns]')
    return df


def load_customer(root: str):
    data_path = root + "/customer.pq/*.parquet"
    df = vaex.open(data_path)
    return df


def load_nation(root: str):
    data_path = root + "/nation.parquet"
    df = vaex.open(data_path)
    return df


def load_region(root: str):
    data_path = root + "/region.parquet"
    df = vaex.open(data_path)
    return df


def load_supplier(root: str):
    data_path = root + "/supplier.parquet"
    df = vaex.open(data_path)
    return df


def load_partsupp(root: str):
    data_path = root + "/partsupp.pq/*.parquet"
    df = vaex.open(data_path)
    return df


def q01(root: str):
    with vaex.cache.memory():
        lineitem = load_lineitem(root)
        date = np.datetime64("1998-09-02")

        lineitem_filtered = lineitem[
            [
                "L_QUANTITY",
                "L_EXTENDEDPRICE",
                "L_DISCOUNT",
                "L_TAX",
                "L_RETURNFLAG",
                "L_LINESTATUS",
                "L_SHIPDATE",
                "L_ORDERKEY",
            ]
        ]

        sel = lineitem_filtered.L_SHIPDATE <= date
        lineitem_filtered = lineitem_filtered[sel]
        lineitem_filtered["SUM_QTY"] = lineitem_filtered.L_QUANTITY
        lineitem_filtered["SUM_BASE_PRICE"] = lineitem_filtered.L_EXTENDEDPRICE
        lineitem_filtered["AVG_QTY"] = lineitem_filtered.L_QUANTITY
        lineitem_filtered["AVG_PRICE"] = lineitem_filtered.L_EXTENDEDPRICE
        lineitem_filtered["SUM_DISC_PRICE"] = lineitem_filtered.L_EXTENDEDPRICE * (
            1 - lineitem_filtered.L_DISCOUNT
        )
        lineitem_filtered["SUM_CHARGE"] = (
            lineitem_filtered.L_EXTENDEDPRICE
            * (1 - lineitem_filtered.L_DISCOUNT)
            * (1 + lineitem_filtered.L_TAX)
        )
        lineitem_filtered["AVG_DISC"] = lineitem_filtered.L_DISCOUNT
        lineitem_filtered["COUNT_ORDER"] = lineitem_filtered.L_ORDERKEY
        total = lineitem_filtered.groupby(
            ["L_RETURNFLAG", "L_LINESTATUS"],
            agg={
                "SUM_QTY": "sum",
                "SUM_BASE_PRICE": "sum",
                "SUM_DISC_PRICE": "sum",
                "SUM_CHARGE": "sum",
                "AVG_QTY": "mean",
                "AVG_PRICE": "mean",
                "AVG_DISC": "mean",
                "COUNT_ORDER": "count",
            },
        )

        result_df = total.sort(["L_RETURNFLAG", "L_LINESTATUS"])
        print(result_df)
        return result_df
    

def q02(root: str):
    var1 = 15
    var2 = "BRASS"
    var3 = "EUROPE"

    region_ds = load_region(root)
    nation_ds = load_nation(root)
    supplier_ds = load_supplier(root)
    part_ds = load_part(root)
    part_supp_ds = load_partsupp(root)

    nation_filtered = nation_ds[["N_NATIONKEY", "N_NAME", "N_REGIONKEY"]]
    region_filtered = region_ds[(region_ds["R_NAME"] == var3)]
    region_filtered = region_filtered[["R_REGIONKEY"]]
    r_n_merged = nation_filtered.join(
        region_filtered, left_on="N_REGIONKEY", right_on="R_REGIONKEY", how="inner"
    )
    r_n_merged = r_n_merged[["N_NATIONKEY", "N_NAME"]]

    supplier_filtered = supplier_ds[
        [
            "S_SUPPKEY",
            "S_NAME",
            "S_ADDRESS",
            "S_NATIONKEY",
            "S_PHONE",
            "S_ACCTBAL",
            "S_COMMENT",
        ]
    ]
    s_r_n_merged = r_n_merged.join(
        supplier_filtered,
        left_on="N_NATIONKEY",
        right_on="S_NATIONKEY",
        how="inner",
        allow_duplication=True,
    )
    s_r_n_merged = s_r_n_merged[
        [
            "N_NAME",
            "S_SUPPKEY",
            "S_NAME",
            "S_ADDRESS",
            "S_PHONE",
            "S_ACCTBAL",
            "S_COMMENT",
        ]
    ]
    partsupp_filtered = part_supp_ds[["PS_PARTKEY", "PS_SUPPKEY", "PS_SUPPLYCOST"]]
    ps_s_r_n_merged = s_r_n_merged.join(
        partsupp_filtered,
        left_on="S_SUPPKEY",
        right_on="PS_SUPPKEY",
        how="inner",
        allow_duplication=True,
    )
    ps_s_r_n_merged = ps_s_r_n_merged[
        [
            "N_NAME",
            "S_NAME",
            "S_ADDRESS",
            "S_PHONE",
            "S_ACCTBAL",
            "S_COMMENT",
            "PS_PARTKEY",
            "PS_SUPPLYCOST",
        ]
    ]
    part_filtered = part_ds[["P_PARTKEY", "P_MFGR", "P_SIZE", "P_TYPE"]]
    part_filtered = part_filtered[
        (part_filtered["P_SIZE"] == var1)
        & (part_filtered["P_TYPE"].str.endswith(var2))
    ]
    part_filtered = part_filtered[["P_PARTKEY", "P_MFGR"]]
    # see: https://github.com/vaexio/vaex/issues/1319
    part_filtered = part_filtered.sort("P_PARTKEY")

    merged_df = part_filtered.join(
        ps_s_r_n_merged,
        left_on="P_PARTKEY",
        right_on="PS_PARTKEY",
        how="inner",
        allow_duplication=True,
    )
    merged_df = merged_df[
        [
            "N_NAME",
            "S_NAME",
            "S_ADDRESS",
            "S_PHONE",
            "S_ACCTBAL",
            "S_COMMENT",
            "PS_SUPPLYCOST",
            "P_PARTKEY",
            "P_MFGR",
        ]
    ]
    min_values = ps_s_r_n_merged.groupby("PS_PARTKEY").agg({"PS_SUPPLYCOST": "min"})
    print(merged_df.dtypes)
    merged_df = merged_df.join(
        min_values,
        left_on=["P_PARTKEY", "PS_SUPPLYCOST"],
        right_on=["P_PARTKEY", "PS_SUPPLYCOST"],
        how="inner",
        allow_duplication=True,
    )
    result_df = merged_df[
        [
            "S_ACCTBAL",
            "S_NAME",
            "N_NAME",
            "P_PARTKEY",
            "P_MFGR",
            "S_ADDRESS",
            "S_PHONE",
            "S_COMMENT",
        ]
    ].sort(
        by=[
            "S_ACCTBAL",
            "N_NAME",
            "S_NAME",
            "P_PARTKEY",
        ],
        ascending=[
            False,
            True,
            True,
            True,
        ],
    )
    
    print(result_df)
    return result_df


def q03(root: str):
    var1 = var2 = np.datetime64("1995-03-15")
    var3 = "BUILDING"

    customer_ds = load_customer(root)
    line_item_ds = load_lineitem(root)
    orders_ds = load_orders(root)

    lineitem_filtered = line_item_ds[
        ["L_ORDERKEY", "L_EXTENDEDPRICE", "L_DISCOUNT", "L_SHIPDATE"]
    ]
    orders_filtered = orders_ds[
        ["O_ORDERKEY", "O_CUSTKEY", "O_ORDERDATE", "O_SHIPPRIORITY"]
    ]
    customer_filtered = customer_ds[["C_MKTSEGMENT", "C_CUSTKEY"]]
    lsel = lineitem_filtered.L_SHIPDATE > var1
    osel = orders_filtered.O_ORDERDATE < var2
    csel = customer_filtered.C_MKTSEGMENT == var3
    flineitem = lineitem_filtered[lsel]
    forders = orders_filtered[osel]
    forders = forders.sort("O_CUSTKEY")
    fcustomer = customer_filtered[csel]
    fcustomer = fcustomer.sort("C_CUSTKEY")

    jn1 = fcustomer.join(
        forders,
        left_on="C_CUSTKEY",
        right_on="O_CUSTKEY",
        how="inner",
        allow_duplication=True,
    )
    flineitem = flineitem.sort("L_ORDERKEY")
    jn1 = jn1.sort("O_ORDERKEY")
    jn2 = jn1.join(
        flineitem,
        left_on="O_ORDERKEY",
        right_on="L_ORDERKEY",
        how="inner",
        allow_duplication=True,
    )
    jn2["REVENUE"] = jn2.l_extendedprice * (1 - jn2.l_discount)

    total = (
        jn2.groupby(["L_ORDERKEY", "O_ORDERDATE", "O_SHIPPRIORITY"], as_index=False)
        .agg({"REVENUE": "sum"})
        .sort("VALUES", ascending=False)
    )
    result_df = total[
        ["L_ORDERKEY", "REVENUE", "O_ORDERDATE", "O_SHIPPRIORITY"]
    ]
    print(result_df.head(10))
    return result_df


def q05(root: str):
    date1 = np.datetime64("1994-01-01")
    date2 = np.datetime64("1995-01-01")

    region_ds = load_region(root)
    nation_ds = load_nation(root)
    customer_ds = load_customer(root)
    line_item_ds = load_lineitem(root)
    orders_ds = load_orders(root)
    supplier_ds = load_supplier(root)

    rsel = region_ds.R_NAME == "ASIA"
    osel = (orders_ds.O_ORDERDATE >= date1) & (orders_ds.O_ORDERDATE < date2)
    forders = orders_ds[osel]
    fregion = region_ds[rsel]
    # see: https://github.com/vaexio/vaex/issues/1319
    fregion = fregion.sort("R_REGIONKEY")

    jn1 = fregion.join(
        nation_ds,
        left_on="R_REGIONKEY",
        right_on="N_REGIONKEY",
        how="inner",
        allow_duplication=True,
    )
    jn2 = jn1.join(
        customer_ds,
        left_on="N_NATIONKEY",
        right_on="C_NATIONKEY",
        how="inner",
        allow_duplication=True,
    )
    jn3 = jn2.join(
        forders,
        left_on="C_CUSTKEY",
        right_on="O_CUSTKEY",
        how="inner",
        allow_duplication=True,
    )
    jn4 = jn3.join(
        line_item_ds,
        left_on="O_ORDERKEY",
        right_on="L_ORDERKEY",
        how="inner",
        allow_duplication=True,
    )
    jn5 = supplier_ds.join(
        jn4,
        left_on=["S_SUPPKEY", "S_NATIONKEY"],
        right_on=["L_SUPPKEY", "N_NATIONKEY"],
        how="inner",
        allow_duplication=True,
    )
    jn5["REVENUE"] = jn5.L_EXTENDEDPRICE * (1.0 - jn5.L_DISCOUNT)
    gb = jn5.groupby("N_NAME").agg({"REVENUE": "sum"})
    result_df = gb.sort("REVENUE", ascending=False)
    print(result_df)
    return result_df


def q06(root: str):
    date1 = np.datetime64("1996-01-01")
    date2 = np.datetime64("1997-01-01")
    var3 = 24

    line_item_ds = load_lineitem(root)

    lineitem_filtered = line_item_ds[
        ["L_QUANTITY", "L_EXTENDEDPRICE", "L_DISCOUNT", "L_SHIPDATE"]
    ]
    sel = (
        (lineitem_filtered.L_SHIPDATE >= date1)
        & (lineitem_filtered.L_SHIPDATE < date2)
        & (lineitem_filtered.L_DISCOUNT >= 0.08)
        & (lineitem_filtered.L_DISCOUNT <= 0.1)
        & (lineitem_filtered.L_QUANTITY < var3)
    )

    flineitem = lineitem_filtered[sel]
    result_value = (flineitem.L_EXTENDEDPRICE * flineitem.L_DISCOUNT).sum()
    print(result_value)
    # result_df = pd.DataFrame({"revenue": [float(result_value)]})
    return result_value



query_to_loaders = {
    1: [load_lineitem],
    2: [load_part, load_partsupp, load_supplier, load_nation, load_region],
    3: [load_lineitem, load_orders, load_customer],
    # 4: [load_lineitem, load_orders],
    5: [
        load_lineitem,
        load_orders,
        load_customer,
        load_nation,
        load_region,
        load_supplier,
    ],
    6: [load_lineitem],
    # 7: [load_lineitem, load_supplier, load_orders, load_customer, load_nation],
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
    # 4: q04,
    5: q05,
    6: q06,
    # 7: q07,
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
                append_row("vaex", query, dur, vaex.__version__["vaex"], success)
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