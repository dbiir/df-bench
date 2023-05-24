#!/bin/bash

#SBATCH --job-name=modin
#SBATCH --nodes=4
#SBATCH --cpus-per-task=64
#SBATCH --ntasks-per-node=1
#SBATCH --partition=cpu64c
#SBATCH --output=modin-sf10.out

ulimit -a
ulimit -u unlimited
source activate /fs/fast/u20200002/envs/ds

### Use the debug mode to see if the shell commands are correct.
### If you do not want the shell command logs, delete the following line.
set -x

# Getting the node names
nodes=$(scontrol show hostnames "$SLURM_JOB_NODELIST")
nodes_array=($nodes)

head_node=${nodes_array[0]}
head_node_ip=$(srun --nodes=1 --ntasks=1 -w "$head_node" hostname --ip-address)

# if we detect a space character in the head node IP, we'll
# convert it to an ipv4 address. This step is optional.
if [[ "$head_node_ip" == *" "* ]]; then
    IFS=' ' read -ra ADDR <<<"$head_node_ip"
    if [[ ${#ADDR[0]} -gt 16 ]]; then
        head_node_ip=${ADDR[1]}
    else
        head_node_ip=${ADDR[0]}
    fi
    echo "IPV6 address detected. We split the IPV4 address as $head_node_ip"
fi

delim=""
joined=""
for item in "${nodes_array[@]}"; do
  joined="$joined$delim$item"
  delim=","
done
echo "$joined"

echo $joined

port=6379
ip_head=$head_node_ip:$port
export ip_head
echo "IP Head: $ip_head"

echo "Starting HEAD at $head_node"
srun --nodes=1 --ntasks=1 -w "$head_node" \
    ray start --head --node-ip-address="$head_node_ip" --port=$port \
    --num-cpus "${SLURM_CPUS_PER_TASK}" --block &
sleep 10

# number of nodes other than the head node
worker_num=$((SLURM_JOB_NUM_NODES - 1))

for ((i = 1; i <= worker_num; i++)); do
    node_i=${nodes_array[$i]}
    port_i=$((port + i))

    echo "Starting WORKER $i at $node_i"
    srun --nodes=1 --ntasks=1 -w "$node_i" \
        ray start --address "$ip_head" \
        --num-cpus "${SLURM_CPUS_PER_TASK}" --block &
done
sleep 10

address=ray://$head_node_ip:$port

export MODIN_ENGINE=unidist
export UNIDIST_BACKEND=mpi
export UNIDIST_MPI_HOSTS=$joined
. /opt/app/intel/2023/mpi/2021.9.0/env/vars.sh

cd /home/u20200002/hpc/test_xorbits/df-bench/tpch/queries
mpirun -np python -u modin_query.py \
    --path /home/u20200002/hpc/test_xorbits/df-bench/tpch/SF10 \
    --log_timing \
    --io_warmup