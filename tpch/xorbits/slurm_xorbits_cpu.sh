#!/bin/bash

#SBATCH --job-name=xorbits
#SBATCH --nodes=3
#SBATCH --cpus-per-task=32
#SBATCH --ntasks-per-node=1
#SBATCH --partition=cpu32c
#SBATCH --output=xorbits-cpu-sf10-q1.log

source activate /fs/fast/u20200002/envs/ds
# source activate tpch
# export LOGLEVEL=DEBUG

### Use the debug mode to see if the shell commands are correct.
### If you do not want the shell command logs, delete the following line.
# set -x

ulimit -u unlimited

# Getting the node names
nodes=$(scontrol show hostnames "$SLURM_JOB_NODELIST")
nodes_array=($nodes)

head_node=${nodes_array[0]}
port=17380
web_port=17379

echo "Starting HEAD at $head_node"
srun --nodes=1 --ntasks=1 -w "$head_node" \
    xorbits-supervisor -H "$head_node" -p "$port" -w "$web_port" &
sleep 10

# number of nodes other than the head node
# worker_num=$((SLURM_JOB_NUM_NODES - 1))
worker_num=$((SLURM_JOB_NUM_NODES))

for ((i = 0; i < worker_num; i++)); do
    node_i=${nodes_array[$i]}
    port_i=$((port + i + 1))
    
    echo "Starting WORKER $i at $node_i"
    srun --nodes=1 --ntasks=1 -w "$node_i" \
        xorbits-worker -H "$node_i"  -p "$port_i" -s "$head_node":"$port" &
done
sleep 5

address=http://"$head_node":"$web_port"
echo $address

python -u xorbits_query.py \
    --path /fs/fast/u20200002/df-bench/tpch/SF10 \
    --endpoint ${address} \
    --queries 1 \
    --log_timing \
    --include_io \
    --print_result \