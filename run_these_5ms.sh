#!/bin/bash
sudo rm -rf /mnt/ex*/e*/2*

python3 ./scripts/run_multiple_experiments.py ./experiments/osdi24/test-1s-mp.json
sudo rm -rf experiments/osdi24/results/tput-3/20*/2*/2*/out/cli*
sudo mv experiments/osdi24/results/tput-3/20* /proj/praxis-PG0/exp/mdl/SOSP24/1s/f1
sudo rm -rf /mnt/ex*/e*/2*
sed -i 's/"client_fanout": 1,/"client_fanout": 2,/g' experiments/osdi24/test-1s-mp.json

#ssh client-1-0 'sudo rm -rf /mnt/ex*/e*/2*'
#ssh client-2-0 'sudo rm -rf /mnt/ex*/e*/2*'

python3 ./scripts/run_multiple_experiments.py ./experiments/osdi24/test-1s-mp.json
sudo rm -rf experiments/osdi24/results/tput-3/20*/2*/2*/out/cli*
sudo mv experiments/osdi24/results/tput-3/20* /proj/praxis-PG0/exp/mdl/SOSP24/1s/f2
sudo rm -rf /mnt/ex*/e*/2*
sed -i 's/"client_fanout": 2,/"client_fanout": 4,/g' experiments/osdi24/test-1s-mp.json

python3 ./scripts/run_multiple_experiments.py ./experiments/osdi24/test-1s-mp.json
sudo rm -rf experiments/osdi24/results/tput-3/20*/2*/2*/out/cli*
sudo mv experiments/osdi24/results/tput-3/20* /proj/praxis-PG0/exp/mdl/SOSP24/1s/f4
sudo rm -rf /mnt/ex*/e*/2*
sed -i 's/"client_fanout": 4,/"client_fanout": 8,/g' experiments/osdi24/test-1s-mp.json

python3 ./scripts/run_multiple_experiments.py ./experiments/osdi24/test-1s-mp.json
sudo rm -rf experiments/osdi24/results/tput-3/20*/2*/2*/out/cli*
sudo mv experiments/osdi24/results/tput-3/20* /proj/praxis-PG0/exp/mdl/SOSP24/1s/f8
sudo rm -rf /mnt/ex*/e*/2*
sed -i 's/"client_fanout": 8,/"client_fanout": 16,/g' experiments/osdi24/test-1s-mp.json

python3 ./scripts/run_multiple_experiments.py ./experiments/osdi24/test-1s-mp.json
sudo rm -rf experiments/osdi24/results/tput-3/20*/2*/2*/out/cli*
sudo mv experiments/osdi24/results/tput-3/20* /proj/praxis-PG0/exp/mdl/SOSP24/1s/f16
sudo rm -rf /mnt/ex*/e*/2*
sed -i 's/"client_fanout": 16,/"client_fanout": 32,/g' experiments/osdi24/test-1s-mp.json

python3 ./scripts/run_multiple_experiments.py ./experiments/osdi24/test-1s-mp.json
sudo rm -rf experiments/osdi24/results/tput-3/20*/2*/2*/out/cli*
sudo mv experiments/osdi24/results/tput-3/20* /proj/praxis-PG0/exp/mdl/SOSP24/1s/f32
sudo rm -rf /mnt/ex*/e*/2*
sed -i 's/"client_fanout": 32,/"client_fanout": 64,/g' experiments/osdi24/test-1s-mp.json


python3 ./scripts/run_multiple_experiments.py ./experiments/osdi24/test-1s-mp.json
sudo rm -rf experiments/osdi24/results/tput-3/20*/2*/2*/out/cli*
sudo mv experiments/osdi24/results/tput-3/20* /proj/praxis-PG0/exp/mdl/SOSP24/1s/f64
sudo rm -rf /mnt/ex*/e*/2*
sed -i 's/"client_fanout": 64,/"client_fanout": 128,/g' experiments/osdi24/test-1s-mp.json

python3 ./scripts/run_multiple_experiments.py ./experiments/osdi24/test-1s-mp.json
sudo rm -rf experiments/osdi24/results/tput-3/20*/2*/2*/out/cli*
sudo mv experiments/osdi24/results/tput-3/20* /proj/praxis-PG0/exp/mdl/SOSP24/1s/f128
sudo rm -rf /mnt/ex*/e*/2*
sed -i 's/"client_fanout": 128,/"client_fanout": 24,/g' experiments/osdi24/test-1s-mp.json

python3 ./scripts/run_multiple_experiments.py ./experiments/osdi24/test-1s-mp.json
sudo rm -rf experiments/osdi24/results/tput-3/20*/2*/2*/out/cli*
sudo mv experiments/osdi24/results/tput-3/20* /proj/praxis-PG0/exp/mdl/SOSP24/1s/f24
sudo rm -rf /mnt/ex*/e*/2*
sed -i 's/"client_fanout": 24,/"client_fanout": 48,/g' experiments/osdi24/test-1s-mp.json

python3 ./scripts/run_multiple_experiments.py ./experiments/osdi24/test-1s-mp.json
sudo rm -rf experiments/osdi24/results/tput-3/20*/2*/2*/out/cli*
sudo mv experiments/osdi24/results/tput-3/20* /proj/praxis-PG0/exp/mdl/SOSP24/1s/f48
sudo rm -rf /mnt/ex*/e*/2*
sed -i 's/"client_fanout": 48,/"client_fanout": 1,/g' experiments/osdi24/test-1s-mp.json

