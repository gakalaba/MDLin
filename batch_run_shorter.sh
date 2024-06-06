#!/bin/bash

sudo rm -rf /mnt/ex*/e*/2*
ssh client-0-1 'sudo rm -rf /mnt/ex*/e*/2*'
ssh client-0-2 'sudo rm -rf /mnt/ex*/e*/2*'
ssh client-1-0 'sudo rm -rf /mnt/ex*/e*/2*'
ssh client-1-1 'sudo rm -rf /mnt/ex*/e*/2*'
ssh client-1-2 'sudo rm -rf /mnt/ex*/e*/2*'
ssh client-2-0 'sudo rm -rf /mnt/ex*/e*/2*'
ssh client-2-1 'sudo rm -rf /mnt/ex*/e*/2*'
ssh client-2-2 'sudo rm -rf /mnt/ex*/e*/2*'
ssh client-3-0 'sudo rm -rf /mnt/ex*/e*/2*'
ssh client-3-1 'sudo rm -rf /mnt/ex*/e*/2*'
ssh client-3-2 'sudo rm -rf /mnt/ex*/e*/2*'
ssh client-4-0 'sudo rm -rf /mnt/ex*/e*/2*'
ssh client-4-1 'sudo rm -rf /mnt/ex*/e*/2*'
ssh client-4-2 'sudo rm -rf /mnt/ex*/e*/2*'
ssh client-5-0 'sudo rm -rf /mnt/ex*/e*/2*'
ssh client-5-1 'sudo rm -rf /mnt/ex*/e*/2*'
ssh client-5-2 'sudo rm -rf /mnt/ex*/e*/2*'
ssh client-6-0 'sudo rm -rf /mnt/ex*/e*/2*'
ssh client-6-1 'sudo rm -rf /mnt/ex*/e*/2*'
ssh us-east-1-0 'sudo rm -rf /mnt/ex*/e*/2*'
ssh us-east-1-1 'sudo rm -rf /mnt/ex*/e*/2*'
ssh us-east-1-2 'sudo rm -rf /mnt/ex*/e*/2*'
ssh us-west-1-0 'sudo rm -rf /mnt/ex*/e*/2*'
ssh us-west-1-1 'sudo rm -rf /mnt/ex*/e*/2*'
ssh us-west-1-2 'sudo rm -rf /mnt/ex*/e*/2*'

# batchsize starts at 2000
python3 ./scripts/run_multiple_experiments.py ./experiments/osdi24/test-1s-15clients.json
sudo rm -rf experiments/osdi24/results/tput-3/20*/2*/2*/out/cli*
sudo rm -rf experiments/osdi24/results/tput-3/20*/2*/2*/out/us-*
#sudo mv experiments/osdi24/results/tput-3/20* /proj/praxis-PG0/exp/mdl/SOSP24/batching/e_25us
sudo rm -rf /mnt/ex*/e*/2*
ssh client-0-1 'sudo rm -rf /mnt/ex*/e*/2*'
ssh client-0-2 'sudo rm -rf /mnt/ex*/e*/2*'
ssh client-1-0 'sudo rm -rf /mnt/ex*/e*/2*'
ssh client-1-1 'sudo rm -rf /mnt/ex*/e*/2*'
ssh client-1-2 'sudo rm -rf /mnt/ex*/e*/2*'
ssh client-2-0 'sudo rm -rf /mnt/ex*/e*/2*'
ssh client-2-1 'sudo rm -rf /mnt/ex*/e*/2*'
ssh client-2-2 'sudo rm -rf /mnt/ex*/e*/2*'
ssh client-3-0 'sudo rm -rf /mnt/ex*/e*/2*'
ssh client-3-1 'sudo rm -rf /mnt/ex*/e*/2*'
ssh client-3-2 'sudo rm -rf /mnt/ex*/e*/2*'
ssh client-4-0 'sudo rm -rf /mnt/ex*/e*/2*'
ssh client-4-1 'sudo rm -rf /mnt/ex*/e*/2*'
ssh client-4-2 'sudo rm -rf /mnt/ex*/e*/2*'
ssh client-5-0 'sudo rm -rf /mnt/ex*/e*/2*'
ssh client-5-1 'sudo rm -rf /mnt/ex*/e*/2*'
ssh client-5-2 'sudo rm -rf /mnt/ex*/e*/2*'
ssh client-6-0 'sudo rm -rf /mnt/ex*/e*/2*'
ssh client-6-1 'sudo rm -rf /mnt/ex*/e*/2*'
ssh us-east-1-0 'sudo rm -rf /mnt/ex*/e*/2*'
ssh us-east-1-1 'sudo rm -rf /mnt/ex*/e*/2*'
ssh us-east-1-2 'sudo rm -rf /mnt/ex*/e*/2*'
ssh us-west-1-0 'sudo rm -rf /mnt/ex*/e*/2*'
ssh us-west-1-1 'sudo rm -rf /mnt/ex*/e*/2*'
ssh us-west-1-2 'sudo rm -rf /mnt/ex*/e*/2*'
sed -i 's/"batchsize": 2000,/"batchsize": 9000,/g' experiments/osdi24/test-1s-15clients.json

python3 ./scripts/run_multiple_experiments.py ./experiments/osdi24/test-1s-15clients.json
sudo rm -rf experiments/osdi24/results/tput-3/20*/2*/2*/out/cli*
sudo rm -rf experiments/osdi24/results/tput-3/20*/2*/2*/out/us-*
#sudo mv experiments/osdi24/results/tput-3/20* /proj/praxis-PG0/exp/mdl/SOSP24/batching/e_25us
sudo rm -rf /mnt/ex*/e*/2*
ssh client-0-1 'sudo rm -rf /mnt/ex*/e*/2*'
ssh client-0-2 'sudo rm -rf /mnt/ex*/e*/2*'
ssh client-1-0 'sudo rm -rf /mnt/ex*/e*/2*'
ssh client-1-1 'sudo rm -rf /mnt/ex*/e*/2*'
ssh client-1-2 'sudo rm -rf /mnt/ex*/e*/2*'
ssh client-2-0 'sudo rm -rf /mnt/ex*/e*/2*'
ssh client-2-1 'sudo rm -rf /mnt/ex*/e*/2*'
ssh client-2-2 'sudo rm -rf /mnt/ex*/e*/2*'
ssh client-3-0 'sudo rm -rf /mnt/ex*/e*/2*'
ssh client-3-1 'sudo rm -rf /mnt/ex*/e*/2*'
ssh client-3-2 'sudo rm -rf /mnt/ex*/e*/2*'
ssh client-4-0 'sudo rm -rf /mnt/ex*/e*/2*'
ssh client-4-1 'sudo rm -rf /mnt/ex*/e*/2*'
ssh client-4-2 'sudo rm -rf /mnt/ex*/e*/2*'
ssh client-5-0 'sudo rm -rf /mnt/ex*/e*/2*'
ssh client-5-1 'sudo rm -rf /mnt/ex*/e*/2*'
ssh client-5-2 'sudo rm -rf /mnt/ex*/e*/2*'
ssh client-6-0 'sudo rm -rf /mnt/ex*/e*/2*'
ssh client-6-1 'sudo rm -rf /mnt/ex*/e*/2*'
ssh us-east-1-0 'sudo rm -rf /mnt/ex*/e*/2*'
ssh us-east-1-1 'sudo rm -rf /mnt/ex*/e*/2*'
ssh us-east-1-2 'sudo rm -rf /mnt/ex*/e*/2*'
ssh us-west-1-0 'sudo rm -rf /mnt/ex*/e*/2*'
ssh us-west-1-1 'sudo rm -rf /mnt/ex*/e*/2*'
ssh us-west-1-2 'sudo rm -rf /mnt/ex*/e*/2*'
sed -i 's/"batchsize": 9000,/"batchsize": 5000,/g' experiments/osdi24/test-1s-15clients.json

python3 ./scripts/run_multiple_experiments.py ./experiments/osdi24/test-1s-15clients.json
sudo rm -rf experiments/osdi24/results/tput-3/20*/2*/2*/out/cli*
sudo rm -rf experiments/osdi24/results/tput-3/20*/2*/2*/out/us-*
#sudo mv experiments/osdi24/results/tput-3/20* /proj/praxis-PG0/exp/mdl/SOSP24/batching/e_25us
sudo rm -rf /mnt/ex*/e*/2*
ssh client-0-1 'sudo rm -rf /mnt/ex*/e*/2*'
ssh client-0-2 'sudo rm -rf /mnt/ex*/e*/2*'
ssh client-1-0 'sudo rm -rf /mnt/ex*/e*/2*'
ssh client-1-1 'sudo rm -rf /mnt/ex*/e*/2*'
ssh client-1-2 'sudo rm -rf /mnt/ex*/e*/2*'
ssh client-2-0 'sudo rm -rf /mnt/ex*/e*/2*'
ssh client-2-1 'sudo rm -rf /mnt/ex*/e*/2*'
ssh client-2-2 'sudo rm -rf /mnt/ex*/e*/2*'
ssh client-3-0 'sudo rm -rf /mnt/ex*/e*/2*'
ssh client-3-1 'sudo rm -rf /mnt/ex*/e*/2*'
ssh client-3-2 'sudo rm -rf /mnt/ex*/e*/2*'
ssh client-4-0 'sudo rm -rf /mnt/ex*/e*/2*'
ssh client-4-1 'sudo rm -rf /mnt/ex*/e*/2*'
ssh client-4-2 'sudo rm -rf /mnt/ex*/e*/2*'
ssh client-5-0 'sudo rm -rf /mnt/ex*/e*/2*'
ssh client-5-1 'sudo rm -rf /mnt/ex*/e*/2*'
ssh client-5-2 'sudo rm -rf /mnt/ex*/e*/2*'
ssh client-6-0 'sudo rm -rf /mnt/ex*/e*/2*'
ssh client-6-1 'sudo rm -rf /mnt/ex*/e*/2*'
ssh us-east-1-0 'sudo rm -rf /mnt/ex*/e*/2*'
ssh us-east-1-1 'sudo rm -rf /mnt/ex*/e*/2*'
ssh us-east-1-2 'sudo rm -rf /mnt/ex*/e*/2*'
ssh us-west-1-0 'sudo rm -rf /mnt/ex*/e*/2*'
ssh us-west-1-1 'sudo rm -rf /mnt/ex*/e*/2*'
ssh us-west-1-2 'sudo rm -rf /mnt/ex*/e*/2*'
sed -i 's/"batchsize": 5000,/"batchsize": 500,/g' experiments/osdi24/test-1s-15clients.json

python3 ./scripts/run_multiple_experiments.py ./experiments/osdi24/test-1s-15clients.json
sudo rm -rf experiments/osdi24/results/tput-3/20*/2*/2*/out/cli*
sudo rm -rf experiments/osdi24/results/tput-3/20*/2*/2*/out/us-*
#sudo mv experiments/osdi24/results/tput-3/20* /proj/praxis-PG0/exp/mdl/SOSP24/batching/e_25us
sudo rm -rf /mnt/ex*/e*/2*
ssh client-0-1 'sudo rm -rf /mnt/ex*/e*/2*'
ssh client-0-2 'sudo rm -rf /mnt/ex*/e*/2*'
ssh client-1-0 'sudo rm -rf /mnt/ex*/e*/2*'
ssh client-1-1 'sudo rm -rf /mnt/ex*/e*/2*'
ssh client-1-2 'sudo rm -rf /mnt/ex*/e*/2*'
ssh client-2-0 'sudo rm -rf /mnt/ex*/e*/2*'
ssh client-2-1 'sudo rm -rf /mnt/ex*/e*/2*'
ssh client-2-2 'sudo rm -rf /mnt/ex*/e*/2*'
ssh client-3-0 'sudo rm -rf /mnt/ex*/e*/2*'
ssh client-3-1 'sudo rm -rf /mnt/ex*/e*/2*'
ssh client-3-2 'sudo rm -rf /mnt/ex*/e*/2*'
ssh client-4-0 'sudo rm -rf /mnt/ex*/e*/2*'
ssh client-4-1 'sudo rm -rf /mnt/ex*/e*/2*'
ssh client-4-2 'sudo rm -rf /mnt/ex*/e*/2*'
ssh client-5-0 'sudo rm -rf /mnt/ex*/e*/2*'
ssh client-5-1 'sudo rm -rf /mnt/ex*/e*/2*'
ssh client-5-2 'sudo rm -rf /mnt/ex*/e*/2*'
ssh client-6-0 'sudo rm -rf /mnt/ex*/e*/2*'
ssh client-6-1 'sudo rm -rf /mnt/ex*/e*/2*'
ssh us-east-1-0 'sudo rm -rf /mnt/ex*/e*/2*'
ssh us-east-1-1 'sudo rm -rf /mnt/ex*/e*/2*'
ssh us-east-1-2 'sudo rm -rf /mnt/ex*/e*/2*'
ssh us-west-1-0 'sudo rm -rf /mnt/ex*/e*/2*'
ssh us-west-1-1 'sudo rm -rf /mnt/ex*/e*/2*'
ssh us-west-1-2 'sudo rm -rf /mnt/ex*/e*/2*'

