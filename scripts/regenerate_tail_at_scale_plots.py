import utils
import sys
import concurrent.futures

from utils.experiment_util import *

def main():
    if len(sys.argv) != 3:
        sys.stderr.write('Usage: python3 %s <config_file> <exp_dir>\n' % sys.argv[0])
        sys.exit(1)

    regenerate_tail_at_scale_plots(sys.argv[1], sys.argv[2])


if __name__ == "__main__":
    main()
