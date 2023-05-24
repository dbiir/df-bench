import argparse
import json
import time
import traceback
import datetime
import functools
from dateutil.relativedelta import relativedelta

from utils import append_row

import duckdb
import ibis


@functools.singledispatch
def add_date(datestr, dy=0, dm=0, dd=0):
    dt = datetime.date.fromisoformat(datestr)
    dt += relativedelta(years=dy, months=dm, days=dd)
    return dt.isoformat()


def load_lineitem(con, root: str):
    data_path = root + "/lineitem.parquet/*.parquet"
    con.read_parquet(data_path, table_name="lineitem")
    t = con.table("lineitem")
    return t


def load_part(con, root: str):
    data_path = root + "/part.parquet/*.parquet"
    con.read_parquet(data_path, table_name="part")
    t = con.table("part")
    return t


def load_orders(con, root: str):
    data_path = root + "/orders.parquet/*.parquet"
    con.read_parquet(data_path, table_name="orders")
    t = con.table("orders")
    return t


def load_customer(con, root: str):
    data_path = root + "/customer.parquet/*.parquet"
    con.read_parquet(data_path, table_name="customer")
    t = con.table("customer")
    return t


def load_nation(con, root: str):
    data_path = root + "/nation.parquet"
    con.read_parquet(data_path, table_name="nation")
    t = con.table("nation")
    return t


def load_region(con, root: str):
    data_path = root + "/region.parquet"
    con.read_parquet(data_path, table_name="region")
    t = con.table("region")
    return t


def load_supplier(con, root: str):
    data_path = root + "/supplier.parquet"
    con.read_parquet(data_path, table_name="supplier")
    t = con.table("supplier")
    return t


def load_partsupp(con, root: str):
    data_path = root + "/partsupp.parquet/*.parquet"
    con.read_parquet(data_path, table_name="partsupp")
    t = con.table("partsupp")
    return t


def q01(con, root, DELTA=0, DATE="1998-09-02"):
    t = load_lineitem(con, root)

    interval = add_date(DATE, dd=-1 * DELTA)
    q = t.filter(t.L_SHIPDATE <= interval)
    discount_price = t.L_EXTENDEDPRICE * (1 - t.L_DISCOUNT)
    charge = discount_price * (1 + t.L_TAX)
    q = q.group_by(["L_RETURNFLAG", "L_LINESTATUS"])
    q = q.aggregate(
        sum_qty=t.L_QUANTITY.sum(),
        sum_base_price=t.L_EXTENDEDPRICE.sum(),
        sum_disc_price=discount_price.sum(),
        sum_charge=charge.sum(),
        avg_qty=t.L_QUANTITY.mean(),
        avg_price=t.L_EXTENDEDPRICE.mean(),
        avg_disc=t.L_DISCOUNT.mean(),
        count_order=t.count(),
    )
    q = q.sort_by(["L_RETURNFLAG", "L_LINESTATUS"])
    result = q.execute()
    print(result)
    return result


def q02(con, root, REGION="EUROPE", SIZE=15, TYPE="BRASS"):
    part = load_part(con, root)
    supplier = load_supplier(con, root)
    partsupp = load_partsupp(con, root)
    nation = load_nation(con, root)
    region = load_region(con, root)

    expr = (
        part.join(partsupp, part.P_PARTKEY == partsupp.PS_PARTKEY)
        .join(supplier, supplier.S_SUPPKEY == partsupp.PS_SUPPKEY)
        .join(nation, supplier.S_NATIONKEY == nation.N_NATIONKEY)
        .join(region, nation.N_REGIONKEY == region.R_REGIONKEY)
    )

    subexpr = (
        partsupp.join(supplier, supplier.S_SUPPKEY == partsupp.PS_SUPPKEY)
        .join(nation, supplier.S_NATIONKEY == nation.N_NATIONKEY)
        .join(region, nation.N_REGIONKEY == region.R_REGIONKEY)
    )

    subexpr = subexpr[
        (subexpr.R_NAME == REGION) & (expr.P_PARTKEY == subexpr.PS_PARTKEY)
    ]

    filters = [
        expr.P_SIZE == SIZE,
        expr.P_TYPE.like("%" + TYPE),
        expr.R_NAME == REGION,
        expr.PS_SUPPLYCOST == subexpr.PS_SUPPLYCOST.min(),
    ]
    q = expr.filter(filters)

    q = q.select(
        [
            q.S_ACCTBAL,
            q.S_NAME,
            q.N_NAME,
            q.P_PARTKEY,
            q.P_MFGR,
            q.S_ADDRESS,
            q.S_PHONE,
            q.S_COMMENT,
        ]
    )

    q = q.sort_by(
        [
            ibis.desc(q.S_ACCTBAL),
            q.N_NAME,
            q.S_NAME,
            q.P_PARTKEY,
        ]
    )
    result = q.execute()
    print(result)

    return result

query_to_runner = {
    1: q01,
    2: q02,
    # 3: q03,
    # 4: q04,
    # 5: q05,
    # 6: q06,
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

def run_queries(con, path, queries, log_timing = True, io_warmup = True):

    total_start = time.time()
    for query in queries:
        try:
            if io_warmup:
                query_to_runner[query](con, path)
            t1 = time.time()
            query_to_runner[query](con, path)
            dur = time.time() - t1
            success = True
        except Exception as e: 
            print(''.join(traceback.TracebackException.from_exception(e).format()))
            dur = 0.0
            success = False
        finally:
            if log_timing:
                append_row("ibis[duckdb]", query, dur, ibis.__version__ + "[" + duckdb.__version__ + "]", success)
    print(f"Total query execution time (s): {time.time() - total_start}")


def main():
    parser = argparse.ArgumentParser(description="pandas TPC-H benchmark.")
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
    con = ibis.duckdb.connect(threads=8,)

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

    run_queries(con, path, queries, log_timing, io_warmup)


if __name__ == "__main__":
    main()
