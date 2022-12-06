from scripts.lib.gryff_codebase import GryffCodebase
from scripts.lib.rdma_repl_codebase import RdmaReplCodebase
from scripts.lib.morty_codebase import MortyCodebase
from scripts.lib.mdl_codebase import MDLCodebase


__BUILDERS__ = {
    "gryff": GryffCodebase(),
    "rdma-repl": RdmaReplCodebase(),
    "morty": MortyCodebase(),
    "mdl": MDLCodebase()
}


def get_client_cmd(config, i, j, k, run, local_exp_directory,
                   remote_exp_directory):
    return __BUILDERS__[config['codebase_name']].get_client_cmd(config, i, j,
                                                                k, run, local_exp_directory, remote_exp_directory)


def get_replica_cmd(config, replica_id, run, local_exp_directory,
                    remote_exp_directory):
    return __BUILDERS__[config['codebase_name']].get_replica_cmd(config,
                                                                 replica_id, run, local_exp_directory, remote_exp_directory)


def prepare_local_exp_directory(config, config_file):
    return __BUILDERS__[config['codebase_name']].prepare_local_exp_directory(config, config_file)


def prepare_remote_server_codebase(config, server_host, local_exp_directory, remote_out_directory):
    return __BUILDERS__[config['codebase_name']].prepare_remote_server_codebase(config, server_host, local_exp_directory, remote_out_directory)


def setup_nodes(config):
    return __BUILDERS__[config['codebase_name']].setup_nodes(config)
