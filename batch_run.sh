#!/bin/bash

#python3 ./scripts/run_multiple_experiments.py ./experiments/osdi24/bf4.json
#sudo rm -rf experiments/osdi24/results/tput-3/20*/2*/2*/out/cli*
#sudo mv experiments/osdi24/results/tput-3/20* experiments/osdi24/results/tput-3/3ms
#sudo rm -rf /mnt/ex*/e*/2*
#ssh client-3-0 'sudo rm -rf /mnt/ex*/e*/2*'
#ssh client-2-0 'sudo rm -rf /mnt/ex*/e*/2*'
sudo rm -rf /mnt/ex*/e*/2*
ssh client-1-0 'sudo rm -rf /mnt/ex*/e*/2*'
ssh client-3-0 'sudo rm -rf /mnt/ex*/e*/2*'
ssh client-2-0 'sudo rm -rf /mnt/ex*/e*/2*'
ssh client-4-0 'sudo rm -rf /mnt/ex*/e*/2*'
ssh client-5-0 'sudo rm -rf /mnt/ex*/e*/2*'
ssh client-6-0 'sudo rm -rf /mnt/ex*/e*/2*'
ssh client-7-0 'sudo rm -rf /mnt/ex*/e*/2*'
ssh client-8-0 'sudo rm -rf /mnt/ex*/e*/2*'
ssh us-east-1-0 'sudo rm -rf /mnt/ex*/e*/2*'
ssh us-east-1-1 'sudo rm -rf /mnt/ex*/e*/2*'
ssh us-east-1-2 'sudo rm -rf /mnt/ex*/e*/2*'
ssh us-west-1-0 'sudo rm -rf /mnt/ex*/e*/2*'
ssh us-west-1-1 'sudo rm -rf /mnt/ex*/e*/2*'
ssh us-west-1-2 'sudo rm -rf /mnt/ex*/e*/2*'

# batchsize starts at 12

#python3 ./scripts/run_multiple_experiments.py ./experiments/osdi24/test-9s-mp-15clients.json
#sudo rm -rf experiments/osdi24/results/tput-3/20*/2*/2*/out/cli*
#sudo rm -rf experiments/osdi24/results/tput-3/20*/2*/2*/out/us-*
##sudo mv experiments/osdi24/results/tput-3/20* /proj/praxis-PG0/exp/mdl/SOSP24/batching/e_25us
#sudo rm -rf /mnt/ex*/e*/2*
#ssh client-3-0 'sudo rm -rf /mnt/ex*/e*/2*'
#ssh client-2-0 'sudo rm -rf /mnt/ex*/e*/2*'
#ssh us-east-1-0 'sudo rm -rf /mnt/ex*/e*/2*'
#ssh us-east-1-1 'sudo rm -rf /mnt/ex*/e*/2*'
#ssh us-east-1-2 'sudo rm -rf /mnt/ex*/e*/2*'
#ssh us-west-1-0 'sudo rm -rf /mnt/ex*/e*/2*'
#ssh us-west-1-1 'sudo rm -rf /mnt/ex*/e*/2*'
#ssh us-west-1-2 'sudo rm -rf /mnt/ex*/e*/2*'
#ssh client-1-0 'sudo rm -rf /mnt/ex*/e*/2*'
#sed -i 's/"batchsize": 5,/"batchsize": 50,/g' experiments/osdi24/test-9s-mp-15clients.json

python3 ./scripts/run_multiple_experiments.py ./experiments/osdi24/test-9s-mp-15clients.json
sudo rm -rf experiments/osdi24/results/tput-3/20*/2*/2*/out/cli*
sudo rm -rf experiments/osdi24/results/tput-3/20*/2*/2*/out/us-*
#sudo mv experiments/osdi24/results/tput-3/20* /proj/praxis-PG0/exp/mdl/SOSP24/batching/e_25us
sudo rm -rf /mnt/ex*/e*/2*
ssh client-3-0 'sudo rm -rf /mnt/ex*/e*/2*'
ssh client-2-0 'sudo rm -rf /mnt/ex*/e*/2*'
ssh client-4-0 'sudo rm -rf /mnt/ex*/e*/2*'
ssh client-5-0 'sudo rm -rf /mnt/ex*/e*/2*'
ssh client-6-0 'sudo rm -rf /mnt/ex*/e*/2*'
ssh client-7-0 'sudo rm -rf /mnt/ex*/e*/2*'
ssh client-8-0 'sudo rm -rf /mnt/ex*/e*/2*'
ssh us-east-1-0 'sudo rm -rf /mnt/ex*/e*/2*'
ssh us-east-1-1 'sudo rm -rf /mnt/ex*/e*/2*'
ssh us-east-1-2 'sudo rm -rf /mnt/ex*/e*/2*'
ssh us-west-1-0 'sudo rm -rf /mnt/ex*/e*/2*'
ssh us-west-1-1 'sudo rm -rf /mnt/ex*/e*/2*'
ssh us-west-1-2 'sudo rm -rf /mnt/ex*/e*/2*'
ssh client-1-0 'sudo rm -rf /mnt/ex*/e*/2*'
sed -i 's/"batchsize": 12,/"batchsize": 6,/g' experiments/osdi24/test-9s-mp-15clients.json

python3 ./scripts/run_multiple_experiments.py ./experiments/osdi24/test-9s-mp-15clients.json
sudo rm -rf experiments/osdi24/results/tput-3/20*/2*/2*/out/cli*
sudo rm -rf experiments/osdi24/results/tput-3/20*/2*/2*/out/us-*
#sudo mv experiments/osdi24/results/tput-3/20* /proj/praxis-PG0/exp/mdl/SOSP24/batching/e_25us
sudo rm -rf /mnt/ex*/e*/2*
ssh client-3-0 'sudo rm -rf /mnt/ex*/e*/2*'
ssh client-2-0 'sudo rm -rf /mnt/ex*/e*/2*'
ssh client-4-0 'sudo rm -rf /mnt/ex*/e*/2*'
ssh client-5-0 'sudo rm -rf /mnt/ex*/e*/2*'
ssh client-6-0 'sudo rm -rf /mnt/ex*/e*/2*'
ssh client-7-0 'sudo rm -rf /mnt/ex*/e*/2*'
ssh client-8-0 'sudo rm -rf /mnt/ex*/e*/2*'
ssh us-east-1-0 'sudo rm -rf /mnt/ex*/e*/2*'
ssh us-east-1-1 'sudo rm -rf /mnt/ex*/e*/2*'
ssh us-east-1-2 'sudo rm -rf /mnt/ex*/e*/2*'
ssh us-west-1-0 'sudo rm -rf /mnt/ex*/e*/2*'
ssh us-west-1-1 'sudo rm -rf /mnt/ex*/e*/2*'
ssh us-west-1-2 'sudo rm -rf /mnt/ex*/e*/2*'
ssh client-1-0 'sudo rm -rf /mnt/ex*/e*/2*'
sed -i 's/"batchsize": 6,/"batchsize": 800,/g' experiments/osdi24/test-9s-mp-15clients.json

python3 ./scripts/run_multiple_experiments.py ./experiments/osdi24/test-9s-mp-15clients.json
sudo rm -rf experiments/osdi24/results/tput-3/20*/2*/2*/out/cli*
sudo rm -rf experiments/osdi24/results/tput-3/20*/2*/2*/out/us-*
#sudo mv experiments/osdi24/results/tput-3/20* /proj/praxis-PG0/exp/mdl/SOSP24/batching/e_25us
sudo rm -rf /mnt/ex*/e*/2*
ssh client-3-0 'sudo rm -rf /mnt/ex*/e*/2*'
ssh client-2-0 'sudo rm -rf /mnt/ex*/e*/2*'
ssh client-4-0 'sudo rm -rf /mnt/ex*/e*/2*'
ssh client-5-0 'sudo rm -rf /mnt/ex*/e*/2*'
ssh client-6-0 'sudo rm -rf /mnt/ex*/e*/2*'
ssh client-7-0 'sudo rm -rf /mnt/ex*/e*/2*'
ssh client-8-0 'sudo rm -rf /mnt/ex*/e*/2*'
ssh us-east-1-0 'sudo rm -rf /mnt/ex*/e*/2*'
ssh us-east-1-1 'sudo rm -rf /mnt/ex*/e*/2*'
ssh us-east-1-2 'sudo rm -rf /mnt/ex*/e*/2*'
ssh us-west-1-0 'sudo rm -rf /mnt/ex*/e*/2*'
ssh us-west-1-1 'sudo rm -rf /mnt/ex*/e*/2*'
ssh us-west-1-2 'sudo rm -rf /mnt/ex*/e*/2*'
ssh client-1-0 'sudo rm -rf /mnt/ex*/e*/2*'
sed -i 's/"batchsize": 800,/"batchsize": 1000,/g' experiments/osdi24/test-9s-mp-15clients.json

python3 ./scripts/run_multiple_experiments.py ./experiments/osdi24/test-9s-mp-15clients.json
sudo rm -rf experiments/osdi24/results/tput-3/20*/2*/2*/out/cli*
sudo rm -rf experiments/osdi24/results/tput-3/20*/2*/2*/out/us-*
#sudo mv experiments/osdi24/results/tput-3/20* /proj/praxis-PG0/exp/mdl/SOSP24/batching/e_25us
sudo rm -rf /mnt/ex*/e*/2*
ssh client-3-0 'sudo rm -rf /mnt/ex*/e*/2*'
ssh client-2-0 'sudo rm -rf /mnt/ex*/e*/2*'
ssh client-4-0 'sudo rm -rf /mnt/ex*/e*/2*'
ssh client-5-0 'sudo rm -rf /mnt/ex*/e*/2*'
ssh client-6-0 'sudo rm -rf /mnt/ex*/e*/2*'
ssh client-7-0 'sudo rm -rf /mnt/ex*/e*/2*'
ssh client-8-0 'sudo rm -rf /mnt/ex*/e*/2*'
ssh us-east-1-0 'sudo rm -rf /mnt/ex*/e*/2*'
ssh us-east-1-1 'sudo rm -rf /mnt/ex*/e*/2*'
ssh us-east-1-2 'sudo rm -rf /mnt/ex*/e*/2*'
ssh us-west-1-0 'sudo rm -rf /mnt/ex*/e*/2*'
ssh us-west-1-1 'sudo rm -rf /mnt/ex*/e*/2*'
ssh us-west-1-2 'sudo rm -rf /mnt/ex*/e*/2*'
ssh client-1-0 'sudo rm -rf /mnt/ex*/e*/2*'

