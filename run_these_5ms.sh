#!/bin/bash

#python3 ./scripts/run_multiple_experiments.py ./experiments/osdi24/bf4.json
#sudo rm -rf experiments/osdi24/results/tput-3/20*/2*/2*/out/cli*
#sudo mv experiments/osdi24/results/tput-3/20* experiments/osdi24/results/tput-3/3ms
#sudo rm -rf /mnt/ex*/e*/2*
#ssh client-3-0 'sudo rm -rf /mnt/ex*/e*/2*'
#ssh client-2-0 'sudo rm -rf /mnt/ex*/e*/2*'
sudo rm -rf /mnt/ex*/e*/2*
ssh client-3-0 'sudo rm -rf /mnt/ex*/e*/2*'
ssh client-2-0 'sudo rm -rf /mnt/ex*/e*/2*'

# batchsize starts at 100

python3 ./scripts/run_multiple_experiments.py ./experiments/osdi24/bf4.json
sudo rm -rf experiments/osdi24/results/tput-3/20*/2*/2*/out/cli*
sudo mv experiments/osdi24/results/tput-3/20* /proj/praxis-PG0/exp/mdl/SOSP24/batching/f16_100us
sudo rm -rf /mnt/ex*/e*/2*
ssh client-3-0 'sudo rm -rf /mnt/ex*/e*/2*'
ssh client-2-0 'sudo rm -rf /mnt/ex*/e*/2*'
sed -i 's/"batchsize": 100,/"batchsize": 150,/g' experiments/osdi24/bf4.json

python3 ./scripts/run_multiple_experiments.py ./experiments/osdi24/bf4.json
sudo rm -rf experiments/osdi24/results/tput-3/20*/2*/2*/out/cli*
sudo mv experiments/osdi24/results/tput-3/20* /proj/praxis-PG0/exp/mdl/SOSP24/batching/f16_150us
sudo rm -rf /mnt/ex*/e*/2*
ssh client-3-0 'sudo rm -rf /mnt/ex*/e*/2*'
ssh client-2-0 'sudo rm -rf /mnt/ex*/e*/2*'
sed -i 's/"batchsize": 150,/"batchsize": 200,/g' experiments/osdi24/bf4.json


python3 ./scripts/run_multiple_experiments.py ./experiments/osdi24/bf4.json
sudo rm -rf experiments/osdi24/results/tput-3/20*/2*/2*/out/cli*
sudo mv experiments/osdi24/results/tput-3/20* /proj/praxis-PG0/exp/mdl/SOSP24/batching/f16_200us
sudo rm -rf /mnt/ex*/e*/2*
ssh client-3-0 'sudo rm -rf /mnt/ex*/e*/2*'
ssh client-2-0 'sudo rm -rf /mnt/ex*/e*/2*'
sed -i 's/"batchsize": 200,/"batchsize": 250,/g' experiments/osdi24/bf4.json

python3 ./scripts/run_multiple_experiments.py ./experiments/osdi24/bf4.json
sudo rm -rf experiments/osdi24/results/tput-3/20*/2*/2*/out/cli*
sudo mv experiments/osdi24/results/tput-3/20* /proj/praxis-PG0/exp/mdl/SOSP24/batching/f16_250us
sudo rm -rf /mnt/ex*/e*/2*
ssh client-3-0 'sudo rm -rf /mnt/ex*/e*/2*'
ssh client-2-0 'sudo rm -rf /mnt/ex*/e*/2*'
sed -i 's/"batchsize": 250,/"batchsize": 300,/g' experiments/osdi24/bf4.json

python3 ./scripts/run_multiple_experiments.py ./experiments/osdi24/bf4.json
sudo rm -rf experiments/osdi24/results/tput-3/20*/2*/2*/out/cli*
sudo mv experiments/osdi24/results/tput-3/20* /proj/praxis-PG0/exp/mdl/SOSP24/batching/f16_300us
sudo rm -rf /mnt/ex*/e*/2*
ssh client-3-0 'sudo rm -rf /mnt/ex*/e*/2*'
ssh client-2-0 'sudo rm -rf /mnt/ex*/e*/2*'
sed -i 's/"batchsize": 300,/"batchsize": 350,/g' experiments/osdi24/bf4.json


python3 ./scripts/run_multiple_experiments.py ./experiments/osdi24/bf4.json
sudo rm -rf experiments/osdi24/results/tput-3/20*/2*/2*/out/cli*
sudo mv experiments/osdi24/results/tput-3/20* /proj/praxis-PG0/exp/mdl/SOSP24/batching/f16_350us
sudo rm -rf /mnt/ex*/e*/2*
ssh client-3-0 'sudo rm -rf /mnt/ex*/e*/2*'
ssh client-2-0 'sudo rm -rf /mnt/ex*/e*/2*'
sed -i 's/"batchsize": 350,/"batchsize": 400,/g' experiments/osdi24/bf4.json


python3 ./scripts/run_multiple_experiments.py ./experiments/osdi24/bf4.json
sudo rm -rf experiments/osdi24/results/tput-3/20*/2*/2*/out/cli*
sudo mv experiments/osdi24/results/tput-3/20* /proj/praxis-PG0/exp/mdl/SOSP24/batching/f16_400us
sudo rm -rf /mnt/ex*/e*/2*
ssh client-3-0 'sudo rm -rf /mnt/ex*/e*/2*'
ssh client-2-0 'sudo rm -rf /mnt/ex*/e*/2*'
sed -i 's/"batchsize": 400,/"batchsize": 450,/g' experiments/osdi24/bf4.json


python3 ./scripts/run_multiple_experiments.py ./experiments/osdi24/bf4.json
sudo rm -rf experiments/osdi24/results/tput-3/20*/2*/2*/out/cli*
sudo mv experiments/osdi24/results/tput-3/20* /proj/praxis-PG0/exp/mdl/SOSP24/batching/f16_450us
sudo rm -rf /mnt/ex*/e*/2*
ssh client-3-0 'sudo rm -rf /mnt/ex*/e*/2*'
ssh client-2-0 'sudo rm -rf /mnt/ex*/e*/2*'
sed -i 's/"batchsize": 450,/"batchsize": 500,/g' experiments/osdi24/bf4.json


python3 ./scripts/run_multiple_experiments.py ./experiments/osdi24/bf4.json
sudo rm -rf experiments/osdi24/results/tput-3/20*/2*/2*/out/cli*
sudo mv experiments/osdi24/results/tput-3/20* /proj/praxis-PG0/exp/mdl/SOSP24/batching/f16_500us
sudo rm -rf /mnt/ex*/e*/2*
ssh client-3-0 'sudo rm -rf /mnt/ex*/e*/2*'
ssh client-2-0 'sudo rm -rf /mnt/ex*/e*/2*'
sed -i 's/"batchsize": 500,/"batchsize": 550,/g' experiments/osdi24/bf4.json


python3 ./scripts/run_multiple_experiments.py ./experiments/osdi24/bf4.json
sudo rm -rf experiments/osdi24/results/tput-3/20*/2*/2*/out/cli*
sudo mv experiments/osdi24/results/tput-3/20* /proj/praxis-PG0/exp/mdl/SOSP24/batching/f16_550us
sudo rm -rf /mnt/ex*/e*/2*
ssh client-3-0 'sudo rm -rf /mnt/ex*/e*/2*'
ssh client-2-0 'sudo rm -rf /mnt/ex*/e*/2*'
sed -i 's/"batchsize": 550,/"batchsize": 600,/g' experiments/osdi24/bf4.json


python3 ./scripts/run_multiple_experiments.py ./experiments/osdi24/bf4.json
sudo rm -rf experiments/osdi24/results/tput-3/20*/2*/2*/out/cli*
sudo mv experiments/osdi24/results/tput-3/20* /proj/praxis-PG0/exp/mdl/SOSP24/batching/f16_600us
sudo rm -rf /mnt/ex*/e*/2*
ssh client-3-0 'sudo rm -rf /mnt/ex*/e*/2*'
ssh client-2-0 'sudo rm -rf /mnt/ex*/e*/2*'
sed -i 's/"batchsize": 600,/"batchsize": 700,/g' experiments/osdi24/bf4.json


python3 ./scripts/run_multiple_experiments.py ./experiments/osdi24/bf4.json
sudo rm -rf experiments/osdi24/results/tput-3/20*/2*/2*/out/cli*
sudo mv experiments/osdi24/results/tput-3/20* /proj/praxis-PG0/exp/mdl/SOSP24/batching/f16_700us
sudo rm -rf /mnt/ex*/e*/2*
ssh client-3-0 'sudo rm -rf /mnt/ex*/e*/2*'
ssh client-2-0 'sudo rm -rf /mnt/ex*/e*/2*'
sed -i 's/"batchsize": 700,/"batchsize": 800,/g' experiments/osdi24/bf4.json

python3 ./scripts/run_multiple_experiments.py ./experiments/osdi24/bf4.json
sudo rm -rf experiments/osdi24/results/tput-3/20*/2*/2*/out/cli*
sudo mv experiments/osdi24/results/tput-3/20* /proj/praxis-PG0/exp/mdl/SOSP24/batching/f16_800us
sudo rm -rf /mnt/ex*/e*/2*
ssh client-3-0 'sudo rm -rf /mnt/ex*/e*/2*'
ssh client-2-0 'sudo rm -rf /mnt/ex*/e*/2*'
sed -i 's/"batchsize": 800,/"batchsize": 1000,/g' experiments/osdi24/bf4.json

python3 ./scripts/run_multiple_experiments.py ./experiments/osdi24/bf4.json
sudo rm -rf experiments/osdi24/results/tput-3/20*/2*/2*/out/cli*
sudo mv experiments/osdi24/results/tput-3/20* /proj/praxis-PG0/exp/mdl/SOSP24/batching/f16_1ms
sudo rm -rf /mnt/ex*/e*/2*
ssh client-3-0 'sudo rm -rf /mnt/ex*/e*/2*'
ssh client-2-0 'sudo rm -rf /mnt/ex*/e*/2*'








