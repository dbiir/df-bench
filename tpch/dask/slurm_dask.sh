#!/bin/bash

# ensure linear algebra libraries using 1 thread
# https://docs.dask.org/en/stable/array-best-practices.html#avoid-oversubscribing-threads

export OMP_NUM_THREADS=1
export MKL_NUM_THREADS=1
export OPENBLAS_NUM_THREADS=1

source activate /fs/fast/u20200002/envs/ds

python -u dask_query.py \
    --path /fs/fast/u20200002/df-bench/tpch/SF100 \
    --queries 11 12 13 14 15 16 17 18 19 20 21 22 \
    --log_timing \
    --include_io \
    --print_result \
