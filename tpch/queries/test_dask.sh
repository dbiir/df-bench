#!/bin/bash -l

# ensure linear algebra libraries using 1 thread
# https://docs.dask.org/en/stable/array-best-practices.html#avoid-oversubscribing-threads
export OMP_NUM_THREADS=1
export MKL_NUM_THREADS=1
export OPENBLAS_NUM_THREADS=1

source activate /fs/fast/u20200002/envs/ds

python dask_query.py --path /home/u20200002/hpc/test_xorbits/df-bench/tpch/SF10 --log_timing --io_warmup
