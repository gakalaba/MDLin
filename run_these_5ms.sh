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

# batchsize starts at 25

#python3 ./scripts/run_multiple_experiments.py ./experiments/osdi24/bf4-1.json
#sudo rm -rf experiments/osdi24/results/tput-3/20*/2*/2*/out/cli*
#sudo mv experiments/osdi24/results/tput-3/20* /proj/praxis-PG0/exp/mdl/SOSP24/batching/e_25us
#sudo rm -rf /mnt/ex*/e*/2*
#ssh client-3-0 'sudo rm -rf /mnt/ex*/e*/2*'
#ssh client-2-0 'sudo rm -rf /mnt/ex*/e*/2*'
#sed -i 's/"batchsize": 25,/"batchsize": 50,/g' experiments/osdi24/bf4-1.json
#
#python3 ./scripts/run_multiple_experiments.py ./experiments/osdi24/bf4-1.json
#sudo rm -rf experiments/osdi24/results/tput-3/20*/2*/2*/out/cli*
#sudo mv experiments/osdi24/results/tput-3/20* /proj/praxis-PG0/exp/mdl/SOSP24/batching/e_50us
#sudo rm -rf /mnt/ex*/e*/2*
#ssh client-3-0 'sudo rm -rf /mnt/ex*/e*/2*'
#ssh client-2-0 'sudo rm -rf /mnt/ex*/e*/2*'
#sed -i 's/"batchsize": 50,/"batchsize": 75,/g' experiments/osdi24/bf4-1.json
#
#
#python3 ./scripts/run_multiple_experiments.py ./experiments/osdi24/bf4-1.json
#sudo rm -rf experiments/osdi24/results/tput-3/20*/2*/2*/out/cli*
#sudo mv experiments/osdi24/results/tput-3/20* /proj/praxis-PG0/exp/mdl/SOSP24/batching/e_75us
#sudo rm -rf /mnt/ex*/e*/2*
#ssh client-3-0 'sudo rm -rf /mnt/ex*/e*/2*'
#ssh client-2-0 'sudo rm -rf /mnt/ex*/e*/2*'
#sed -i 's/"batchsize": 75,/"batchsize": 100,/g' experiments/osdi24/bf4-1.json
#
#python3 ./scripts/run_multiple_experiments.py ./experiments/osdi24/bf4-1.json
#sudo rm -rf experiments/osdi24/results/tput-3/20*/2*/2*/out/cli*
#sudo mv experiments/osdi24/results/tput-3/20* /proj/praxis-PG0/exp/mdl/SOSP24/batching/e_100us
#sudo rm -rf /mnt/ex*/e*/2*
#ssh client-3-0 'sudo rm -rf /mnt/ex*/e*/2*'
#ssh client-2-0 'sudo rm -rf /mnt/ex*/e*/2*'
#sed -i 's/"batchsize": 100,/"batchsize": 150,/g' experiments/osdi24/bf4-1.json
#
#python3 ./scripts/run_multiple_experiments.py ./experiments/osdi24/bf4-1.json
#sudo rm -rf experiments/osdi24/results/tput-3/20*/2*/2*/out/cli*
#sudo mv experiments/osdi24/results/tput-3/20* /proj/praxis-PG0/exp/mdl/SOSP24/batching/e_150us
#sudo rm -rf /mnt/ex*/e*/2*
#ssh client-3-0 'sudo rm -rf /mnt/ex*/e*/2*'
#ssh client-2-0 'sudo rm -rf /mnt/ex*/e*/2*'
#sed -i 's/"batchsize": 150,/"batchsize": 250,/g' experiments/osdi24/bf4-1.json
#
#
#python3 ./scripts/run_multiple_experiments.py ./experiments/osdi24/bf4-1.json
#sudo rm -rf experiments/osdi24/results/tput-3/20*/2*/2*/out/cli*
#sudo mv experiments/osdi24/results/tput-3/20* /proj/praxis-PG0/exp/mdl/SOSP24/batching/e_250us
#sudo rm -rf /mnt/ex*/e*/2*
#ssh client-3-0 'sudo rm -rf /mnt/ex*/e*/2*'
#ssh client-2-0 'sudo rm -rf /mnt/ex*/e*/2*'
#sed -i 's/"batchsize": 250,/"batchsize": 300,/g' experiments/osdi24/bf4-1.json
#
#
#python3 ./scripts/run_multiple_experiments.py ./experiments/osdi24/bf4-1.json
#sudo rm -rf experiments/osdi24/results/tput-3/20*/2*/2*/out/cli*
#sudo mv experiments/osdi24/results/tput-3/20* /proj/praxis-PG0/exp/mdl/SOSP24/batching/e_300us
#sudo rm -rf /mnt/ex*/e*/2*
#ssh client-3-0 'sudo rm -rf /mnt/ex*/e*/2*'
#ssh client-2-0 'sudo rm -rf /mnt/ex*/e*/2*'
#sed -i 's/"batchsize": 300,/"batchsize": 350,/g' experiments/osdi24/bf4-1.json
#
#
#python3 ./scripts/run_multiple_experiments.py ./experiments/osdi24/bf4-1.json
#sudo rm -rf experiments/osdi24/results/tput-3/20*/2*/2*/out/cli*
#sudo mv experiments/osdi24/results/tput-3/20* /proj/praxis-PG0/exp/mdl/SOSP24/batching/e_350us
#sudo rm -rf /mnt/ex*/e*/2*
#ssh client-3-0 'sudo rm -rf /mnt/ex*/e*/2*'
#ssh client-2-0 'sudo rm -rf /mnt/ex*/e*/2*'
#sed -i 's/"batchsize": 350,/"batchsize": 800,/g' experiments/osdi24/bf4-1.json
#
#
#python3 ./scripts/run_multiple_experiments.py ./experiments/osdi24/bf4-1.json
#sudo rm -rf experiments/osdi24/results/tput-3/20*/2*/2*/out/cli*
#sudo mv experiments/osdi24/results/tput-3/20* /proj/praxis-PG0/exp/mdl/SOSP24/batching/e_800us
#sudo rm -rf /mnt/ex*/e*/2*
#ssh client-3-0 'sudo rm -rf /mnt/ex*/e*/2*'
#ssh client-2-0 'sudo rm -rf /mnt/ex*/e*/2*'
#sed -i 's/"batchsize": 800,/"batchsize": 12,/g' experiments/osdi24/bf4-1.json
#
#
#python3 ./scripts/run_multiple_experiments.py ./experiments/osdi24/bf4-1.json
#sudo rm -rf experiments/osdi24/results/tput-3/20*/2*/2*/out/cli*
#sudo mv experiments/osdi24/results/tput-3/20* /proj/praxis-PG0/exp/mdl/SOSP24/batching/e_12us
#sudo rm -rf /mnt/ex*/e*/2*
#ssh client-3-0 'sudo rm -rf /mnt/ex*/e*/2*'
#ssh client-2-0 'sudo rm -rf /mnt/ex*/e*/2*'
#sed -i 's/"batchsize": 12,/"batchsize": 6,/g' experiments/osdi24/bf4-1.json
#
#
#python3 ./scripts/run_multiple_experiments.py ./experiments/osdi24/bf4-1.json
#sudo rm -rf experiments/osdi24/results/tput-3/20*/2*/2*/out/cli*
#sudo mv experiments/osdi24/results/tput-3/20* /proj/praxis-PG0/exp/mdl/SOSP24/batching/e_6us
#sudo rm -rf /mnt/ex*/e*/2*
#ssh client-3-0 'sudo rm -rf /mnt/ex*/e*/2*'
#ssh client-2-0 'sudo rm -rf /mnt/ex*/e*/2*'
#sed -i 's/"batchsize": 600,/"batchsize": 700,/g' experiments/osdi24/bf4.json


python3 ./scripts/run_multiple_experiments.py ./experiments/osdi24/bf4.json
sudo rm -rf experiments/osdi24/results/tput-3/20*/2*/2*/out/cli*
#sudo mv experiments/osdi24/results/tput-3/20* /proj/praxis-PG0/exp/mdl/SOSP24/batching/250us
sudo rm -rf /mnt/ex*/e*/2*
ssh client-3-0 'sudo rm -rf /mnt/ex*/e*/2*'
ssh client-2-0 'sudo rm -rf /mnt/ex*/e*/2*'
sed -i 's/"batchsize": 250,/"batchsize": 325,/g' experiments/osdi24/bf4.json

python3 ./scripts/run_multiple_experiments.py ./experiments/osdi24/bf4.json
sudo rm -rf experiments/osdi24/results/tput-3/20*/2*/2*/out/cli*
#sudo mv experiments/osdi24/results/tput-3/20* /proj/praxis-PG0/exp/mdl/SOSP24/batching/325us
sudo rm -rf /mnt/ex*/e*/2*
ssh client-3-0 'sudo rm -rf /mnt/ex*/e*/2*'
ssh client-2-0 'sudo rm -rf /mnt/ex*/e*/2*'
sed -i 's/"batchsize": 325,/"batchsize": 350,/g' experiments/osdi24/bf4.json

python3 ./scripts/run_multiple_experiments.py ./experiments/osdi24/bf4.json
sudo rm -rf experiments/osdi24/results/tput-3/20*/2*/2*/out/cli*
#sudo mv experiments/osdi24/results/tput-3/20* /proj/praxis-PG0/exp/mdl/SOSP24/batching/350us
sudo rm -rf /mnt/ex*/e*/2*
ssh client-3-0 'sudo rm -rf /mnt/ex*/e*/2*'
ssh client-2-0 'sudo rm -rf /mnt/ex*/e*/2*'
sed -i 's/"batchsize": 350,/"batchsize": 375,/g' experiments/osdi24/bf4.json

python3 ./scripts/run_multiple_experiments.py ./experiments/osdi24/bf4.json
sudo rm -rf experiments/osdi24/results/tput-3/20*/2*/2*/out/cli*
#sudo mv experiments/osdi24/results/tput-3/20* /proj/praxis-PG0/exp/mdl/SOSP24/batching/375us
sudo rm -rf /mnt/ex*/e*/2*
ssh client-3-0 'sudo rm -rf /mnt/ex*/e*/2*'
ssh client-2-0 'sudo rm -rf /mnt/ex*/e*/2*'
sed -i 's/"batchsize": 375,/"batchsize": 425,/g' experiments/osdi24/bf4.json

python3 ./scripts/run_multiple_experiments.py ./experiments/osdi24/bf4.json
sudo rm -rf experiments/osdi24/results/tput-3/20*/2*/2*/out/cli*
#sudo mv experiments/osdi24/results/tput-3/20* /proj/praxis-PG0/exp/mdl/SOSP24/batching/425us
sudo rm -rf /mnt/ex*/e*/2*
ssh client-3-0 'sudo rm -rf /mnt/ex*/e*/2*'
ssh client-2-0 'sudo rm -rf /mnt/ex*/e*/2*'
sed -i 's/"batchsize": 425,/"batchsize": 450,/g' experiments/osdi24/bf4.json

python3 ./scripts/run_multiple_experiments.py ./experiments/osdi24/bf4.json
sudo rm -rf experiments/osdi24/results/tput-3/20*/2*/2*/out/cli*
#sudo mv experiments/osdi24/results/tput-3/20* /proj/praxis-PG0/exp/mdl/SOSP24/batching/450us
sudo rm -rf /mnt/ex*/e*/2*
ssh client-3-0 'sudo rm -rf /mnt/ex*/e*/2*'
ssh client-2-0 'sudo rm -rf /mnt/ex*/e*/2*'
sed -i 's/"batchsize": 450,/"batchsize": 475,/g' experiments/osdi24/bf4.json

python3 ./scripts/run_multiple_experiments.py ./experiments/osdi24/bf4.json
sudo rm -rf experiments/osdi24/results/tput-3/20*/2*/2*/out/cli*
#sudo mv experiments/osdi24/results/tput-3/20* /proj/praxis-PG0/exp/mdl/SOSP24/batching/475us
sudo rm -rf /mnt/ex*/e*/2*
ssh client-3-0 'sudo rm -rf /mnt/ex*/e*/2*'
ssh client-2-0 'sudo rm -rf /mnt/ex*/e*/2*'
sed -i 's/"batchsize": 475,/"batchsize": 525,/g' experiments/osdi24/bf4.json

python3 ./scripts/run_multiple_experiments.py ./experiments/osdi24/bf4.json
sudo rm -rf experiments/osdi24/results/tput-3/20*/2*/2*/out/cli*
#sudo mv experiments/osdi24/results/tput-3/20* /proj/praxis-PG0/exp/mdl/SOSP24/batching/525us
sudo rm -rf /mnt/ex*/e*/2*
ssh client-3-0 'sudo rm -rf /mnt/ex*/e*/2*'
ssh client-2-0 'sudo rm -rf /mnt/ex*/e*/2*'
sed -i 's/"batchsize": 525,/"batchsize": 550,/g' experiments/osdi24/bf4.json

python3 ./scripts/run_multiple_experiments.py ./experiments/osdi24/bf4.json
sudo rm -rf experiments/osdi24/results/tput-3/20*/2*/2*/out/cli*
#sudo mv experiments/osdi24/results/tput-3/20* /proj/praxis-PG0/exp/mdl/SOSP24/batching/550us
sudo rm -rf /mnt/ex*/e*/2*
ssh client-3-0 'sudo rm -rf /mnt/ex*/e*/2*'
ssh client-2-0 'sudo rm -rf /mnt/ex*/e*/2*'
sed -i 's/"batchsize": 550,/"batchsize": 600,/g' experiments/osdi24/bf4.json

python3 ./scripts/run_multiple_experiments.py ./experiments/osdi24/bf4.json
sudo rm -rf experiments/osdi24/results/tput-3/20*/2*/2*/out/cli*
#sudo mv experiments/osdi24/results/tput-3/20* /proj/praxis-PG0/exp/mdl/SOSP24/batching/600us
sudo rm -rf /mnt/ex*/e*/2*
ssh client-3-0 'sudo rm -rf /mnt/ex*/e*/2*'
ssh client-2-0 'sudo rm -rf /mnt/ex*/e*/2*'
#sed -i 's/"batchsize": 325,/"batchsize": 350,/g' experiments/osdi24/bf4.json



