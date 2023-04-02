import os

CWD = os.path.dirname(os.path.realpath(__file__))

TIMINGS_FILE = os.path.join(CWD, "time.csv")
# DATASET_BASE_DIR = os.path.join(CWD, f"tables_scale_{SCALE_FACTOR}")
# ANSWERS_BASE_DIR = os.path.join(CWD, "tpch-dbgen/answers")
DEFAULT_PLOTS_DIR = os.path.join(CWD, "plots")

WRITE_PLOT = bool(os.environ.get("WRITE_PLOT", False))


def append_row(solution: str, q: str, secs: float, version: str, success=True):
    with open(TIMINGS_FILE, "a") as f:
        if f.tell() == 0:
            f.write("solution,version,query_no,duration[s],success\n")
        f.write(f"{solution},{version},{q},{secs},{success}\n")
