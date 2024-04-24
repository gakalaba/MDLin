#!/bin/bash
sudo rm -rf /mnt/ex*/e*/2*
ssh client-0-0 'sudo rm -rf /mnt/ex*/e*/2*'

python3 ./scripts/run_multiple_experiments.py ./experiments/osdi24/mp-3-wide5.json
sudo rm -rf experiments/osdi24/results/tput-3/20*/2*/2*/out/cli*
sudo mv experiments/osdi24/results/tput-3/20* /proj/praxis-PG0/exp/mdl/SOSP24/wan/f1
sudo rm -rf /mnt/ex*/e*/2*
ssh client-0-0 'sudo rm -rf /mnt/ex*/e*/2*'
#ssh client-2-0 'sudo rm -rf /mnt/ex*/e*/2*'
sed -i 's/"client_fanout": 1,/"client_fanout": 2,/g' experiments/osdi24/mp-3-wide5.json

python3 ./scripts/run_multiple_experiments.py ./experiments/osdi24/mp-3-wide5.json
sudo rm -rf experiments/osdi24/results/tput-3/20*/2*/2*/out/cli*
sudo mv experiments/osdi24/results/tput-3/20* /proj/praxis-PG0/exp/mdl/SOSP24/wan/f2
sudo rm -rf /mnt/ex*/e*/2*
ssh client-0-0 'sudo rm -rf /mnt/ex*/e*/2*'
sed -i 's/"client_fanout": 2,/"client_fanout": 16,/g' experiments/osdi24/mp-3-wide5.json

python3 ./scripts/run_multiple_experiments.py ./experiments/osdi24/mp-3-wide5.json
sudo rm -rf experiments/osdi24/results/tput-3/20*/2*/2*/out/cli*
sudo mv experiments/osdi24/results/tput-3/20* /proj/praxis-PG0/exp/mdl/SOSP24/wan/f16
sudo rm -rf /mnt/ex*/e*/2*
ssh client-0-0 'sudo rm -rf /mnt/ex*/e*/2*'
sed -i 's/"client_fanout": 16,/"client_fanout": 32,/g' experiments/osdi24/mp-3-wide5.json

python3 ./scripts/run_multiple_experiments.py ./experiments/osdi24/mp-3-wide5.json
sudo rm -rf experiments/osdi24/results/tput-3/20*/2*/2*/out/cli*
sudo mv experiments/osdi24/results/tput-3/20* /proj/praxis-PG0/exp/mdl/SOSP24/wan/f32
sudo rm -rf /mnt/ex*/e*/2*
ssh client-0-0 'sudo rm -rf /mnt/ex*/e*/2*'

