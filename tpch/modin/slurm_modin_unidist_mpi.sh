#!/bin/bash

#SBATCH --job-name=modin_unidist_mpi
#SBATCH --nodes=4
#SBATCH --cpus-per-task=24
#SBATCH --ntasks-per-node=1
#SBATCH --partition=cpu24c
#SBATCH --output=modin_unidist_mpi.log

ulimit -u unlimited

# activate environment
module load intel-oneapi-mpi/2021.9.0
source activate /fs/fast/u20200002/envs/ds

nodes=$(scontrol show hostnames "$SLURM_JOB_NODELIST")
nodes_array=($nodes)

delim=""
joined=""
for item in "${nodes_array[@]}"; do
  joined="$joined$delim$item"
  delim=","
done

export MODIN_ENGINE=unidist
export UNIDIST_BACKEND=mpi
# export UNIDIST_MPI_HOSTS=$joined
# echo $UNIDIST_MPI_HOSTS
# export UNIDIST_CPUS=24

# export UNIDIST_IS_MPI_SPAWN_WORKERS=False
export I_MPI_HYDRA_IFACE="ib0.8066"

# change the `--path` to your local location
mpiexec python -u modin_unidist_query.py \
    --path ../SF1 \
    --log_timing \
    --include_io \
    --print_result