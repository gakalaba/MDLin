import utils
import sys

from utils.experiment_util import *

def main():
    if len(sys.argv) != 2:
        sys.stderr.write('Usage: python3 %s <config_file>\n' % sys.argv[0])
        sys.exit(1)

    with concurrent.futures.ThreadPoolExecutor(max_workers=16) as executor:
        run_varying_clients_experiment(sys.argv[1], executor)

if __name__ == "__main__":
    main()
