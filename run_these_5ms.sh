#!/bin/bash
#python3 ./scripts/run_multiple_experiments.py ./experiments/sosp2023/tput-3.json
#sudo rm -rf experiments/sosp2023/results/tput-3/20*/2*/2*/out/cli*
#sudo mv experiments/sosp2023/results/tput-3/20* experiments/sosp2023/results/tput-3/50ms_0
#sudo rm -rf /mnt/ex*/e*/2*
#ssh client-1-0 'sudo rm -rf /mnt/ex*/e*/2*'
#ssh client-2-0 'sudo rm -rf /mnt/ex*/e*/2*'

#python3 ./scripts/run_multiple_experiments.py ./experiments/sosp2023/tput-3-1.json
#sudo rm -rf experiments/sosp2023/results/tput-3/20*/2*/2*/out/cli*
#sudo mv experiments/sosp2023/results/tput-3/20* experiments/sosp2023/results/tput-3/50ms_1
#sudo rm -rf /mnt/ex*/e*/2*
#ssh client-1-0 'sudo rm -rf /mnt/ex*/e*/2*'
#ssh client-2-0 'sudo rm -rf /mnt/ex*/e*/2*'

#python3 ./scripts/run_multiple_experiments.py ./experiments/sosp2023/tput-3-2.json
#sudo rm -rf experiments/sosp2023/results/tput-3/20*/2*/2*/out/cli*
#sudo mv experiments/sosp2023/results/tput-3/20* experiments/sosp2023/results/tput-3/50ms_2
#sudo rm -rf /mnt/ex*/e*/2*
#ssh client-1-0 'sudo rm -rf /mnt/ex*/e*/2*'
#ssh client-2-0 'sudo rm -rf /mnt/ex*/e*/2*'

python3 ./scripts/run_multiple_experiments.py ./experiments/sosp2023/tput-3-3.json
sudo rm -rf experiments/sosp2023/results/tput-3/20*/2*/2*/out/cli*
sudo mv experiments/sosp2023/results/tput-3/20* experiments/sosp2023/results/tput-3/50ms_3
sudo rm -rf /mnt/ex*/e*/2*
ssh client-1-0 'sudo rm -rf /mnt/ex*/e*/2*'
ssh client-2-0 'sudo rm -rf /mnt/ex*/e*/2*'

#python3 ./scripts/run_multiple_experiments.py ./experiments/sosp2023/tput-3-4.json
#sudo rm -rf experiments/sosp2023/results/tput-3/20*/2*/2*/out/cli*
#sudo mv experiments/sosp2023/results/tput-3/20* experiments/sosp2023/results/tput-3/15ms_4
#sudo rm -rf /mnt/ex*/e*/2*
#ssh client-1-0 'sudo rm -rf /mnt/ex*/e*/2*'
#ssh client-2-0 'sudo rm -rf /mnt/ex*/e*/2*'

