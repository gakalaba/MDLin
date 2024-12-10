import abc
import os
import shutil
from utils.remote_util import *


class ExperimentCodebase(abc.ABC):

    @abc.abstractmethod
    def get_client_cmd(self, config, i, j, k, run, local_exp_directory,
                       remote_exp_directory):
        pass

    @abc.abstractmethod
    def get_replica_cmd(self, config, shard_idx, replica_idx, local_exp_directory,
                        remote_exp_directory):
        pass

    def prepare_local_exp_directory(self, config, config_file):
        exp_directory = get_timestamped_exp_dir(config)
        os.makedirs(exp_directory)
        shutil.copy(config_file, os.path.join(exp_directory,
                                              os.path.basename(config_file)))
        return exp_directory

    @abc.abstractmethod
    def prepare_remote_server_codebase(self, config, server_host, local_exp_directory, remote_out_directory):
        pass

    @abc.abstractmethod
    def setup_nodes(self, config):
        pass
