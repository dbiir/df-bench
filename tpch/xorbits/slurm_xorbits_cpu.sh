#!/bin/bash

#SBATCH --job-name=xorbits
#SBATCH --nodes=4
#SBATCH --cpus-per-task=24
#SBATCH --ntasks-per-node=1
#SBATCH --partition=cpu24c
#SBATCH --output=xorbits-null-q22.log

source activate /fs/fast/u20200002/envs/ucx
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


port=17380
web_port=17379

echo "Starting HEAD at $head_node"
srun --nodes=1 --ntasks=1 -w "$head_node" \
    xorbits-supervisor -H "$head_node_ip" -p "$port" -w "$web_port" &
sleep 10

# number of nodes other than the head node
worker_num=$((SLURM_JOB_NUM_NODES - 1))

for ((i = 1; i <= worker_num; i++)); do
    node_i=${nodes_array[$i]}
    port_i=$((port + i))
    
    echo "Starting WORKER $i at $node_i"
    srun --nodes=1 --ntasks=1 -w "$node_i" \
        xorbits-worker -H "$node_i"  -p "$port_i" -s "$head_node_ip":"$port" --log-level=DEBUG &
    # srun --nodes=1 --ntasks=1 -w "$node_i" \
    #     xorbits-worker -H "$node_i"  -p "$port_i" -s "$head_node_ip":"$port" --log-level=DEBUG &
done
sleep 5

# address="$head_node_ip":"$port"
address=http://"$head_node_ip":"$web_port"
echo $address

python -u xorbits_query.py --path ../SF1 \
    --log_timing \
    --queries 22 \
    --endpoint "$address" \
    --print_result