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
ssh us-east-1-0 'sudo rm -rf /mnt/ex*/e*/2*'
ssh us-east-1-1 'sudo rm -rf /mnt/ex*/e*/2*'
ssh us-east-1-2 'sudo rm -rf /mnt/ex*/e*/2*'
ssh us-west-1-0 'sudo rm -rf /mnt/ex*/e*/2*'
ssh us-west-1-1 'sudo rm -rf /mnt/ex*/e*/2*'
ssh us-west-1-2 'sudo rm -rf /mnt/ex*/e*/2*'

# batchsize starts at 5

#python3 ./scripts/run_multiple_experiments.py ./experiments/osdi24/test-9s-mp-10clients.json
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
#sed -i 's/"batchsize": 5,/"batchsize": 50,/g' experiments/osdi24/test-9s-mp-10clients.json

python3 ./scripts/run_multiple_experiments.py ./experiments/osdi24/test-9s-mp-10clients.json
sudo rm -rf experiments/osdi24/results/tput-3/20*/2*/2*/out/cli*
sudo rm -rf experiments/osdi24/results/tput-3/20*/2*/2*/out/us-*
#sudo mv experiments/osdi24/results/tput-3/20* /proj/praxis-PG0/exp/mdl/SOSP24/batching/e_25us
sudo rm -rf /mnt/ex*/e*/2*
ssh client-3-0 'sudo rm -rf /mnt/ex*/e*/2*'
ssh client-2-0 'sudo rm -rf /mnt/ex*/e*/2*'
ssh us-east-1-0 'sudo rm -rf /mnt/ex*/e*/2*'
ssh us-east-1-1 'sudo rm -rf /mnt/ex*/e*/2*'
ssh us-east-1-2 'sudo rm -rf /mnt/ex*/e*/2*'
ssh us-west-1-0 'sudo rm -rf /mnt/ex*/e*/2*'
ssh us-west-1-1 'sudo rm -rf /mnt/ex*/e*/2*'
ssh us-west-1-2 'sudo rm -rf /mnt/ex*/e*/2*'
ssh client-1-0 'sudo rm -rf /mnt/ex*/e*/2*'
sed -i 's/"batchsize": 50,/"batchsize": 100,/g' experiments/osdi24/test-9s-mp-10clients.json

python3 ./scripts/run_multiple_experiments.py ./experiments/osdi24/test-9s-mp-10clients.json
sudo rm -rf experiments/osdi24/results/tput-3/20*/2*/2*/out/cli*
sudo rm -rf experiments/osdi24/results/tput-3/20*/2*/2*/out/us-*
#sudo mv experiments/osdi24/results/tput-3/20* /proj/praxis-PG0/exp/mdl/SOSP24/batching/e_25us
sudo rm -rf /mnt/ex*/e*/2*
ssh client-3-0 'sudo rm -rf /mnt/ex*/e*/2*'
ssh client-2-0 'sudo rm -rf /mnt/ex*/e*/2*'
ssh us-east-1-0 'sudo rm -rf /mnt/ex*/e*/2*'
ssh us-east-1-1 'sudo rm -rf /mnt/ex*/e*/2*'
ssh us-east-1-2 'sudo rm -rf /mnt/ex*/e*/2*'
ssh us-west-1-0 'sudo rm -rf /mnt/ex*/e*/2*'
ssh us-west-1-1 'sudo rm -rf /mnt/ex*/e*/2*'
ssh us-west-1-2 'sudo rm -rf /mnt/ex*/e*/2*'
ssh client-1-0 'sudo rm -rf /mnt/ex*/e*/2*'
sed -i 's/"batchsize": 100,/"batchsize": 200,/g' experiments/osdi24/test-9s-mp-10clients.json

python3 ./scripts/run_multiple_experiments.py ./experiments/osdi24/test-9s-mp-10clients.json
sudo rm -rf experiments/osdi24/results/tput-3/20*/2*/2*/out/cli*
sudo rm -rf experiments/osdi24/results/tput-3/20*/2*/2*/out/us-*
#sudo mv experiments/osdi24/results/tput-3/20* /proj/praxis-PG0/exp/mdl/SOSP24/batching/e_25us
sudo rm -rf /mnt/ex*/e*/2*
ssh client-3-0 'sudo rm -rf /mnt/ex*/e*/2*'
ssh client-2-0 'sudo rm -rf /mnt/ex*/e*/2*'
ssh us-east-1-0 'sudo rm -rf /mnt/ex*/e*/2*'
ssh us-east-1-1 'sudo rm -rf /mnt/ex*/e*/2*'
ssh us-east-1-2 'sudo rm -rf /mnt/ex*/e*/2*'
ssh us-west-1-0 'sudo rm -rf /mnt/ex*/e*/2*'
ssh us-west-1-1 'sudo rm -rf /mnt/ex*/e*/2*'
ssh us-west-1-2 'sudo rm -rf /mnt/ex*/e*/2*'
ssh client-1-0 'sudo rm -rf /mnt/ex*/e*/2*'
sed -i 's/"batchsize": 200,/"batchsize": 400,/g' experiments/osdi24/test-9s-mp-10clients.json

