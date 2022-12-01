import utils
import sys

from utils.experiment_util import *

def main():
    if len(sys.argv) != 2:
        sys.stderr.write('Usage: python3 %s <config_file>\n' % sys.argv[0])
        sys.exit(1)

    run_multiple_protocols_experiment(sys.argv[1])

if __name__ == "__main__":
    main()
