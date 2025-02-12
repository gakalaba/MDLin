from lib.gryff_codebase import GryffCodebase
from lib.rdma_repl_codebase import RdmaReplCodebase
from lib.morty_codebase import MortyCodebase
from lib.mdl_codebase import MDLCodebase
from lib.mdl_transformed_codebase import MDLTransformedCodebase


__BUILDERS__ = {
    "mdl": MDLCodebase(),
    "mdl_transformed": MDLTransformedCodebase()
}


def get_client_cmd(config, i, k, run, local_exp_directory,
                   remote_exp_directory):
    return __BUILDERS__[config['codebase_name']].get_client_cmd(config, i, k, run,
                                                                local_exp_directory,
                                                                remote_exp_directory)


def get_replica_cmd(config, shard_idx, replica_idx, run, local_exp_directory,
                    remote_exp_directory):
    return __BUILDERS__[config['codebase_name']].get_replica_cmd(config, shard_idx,
                                                                 replica_idx, run,
                                                                 local_exp_directory,
                                                                 remote_exp_directory)


def prepare_local_exp_directory(config, config_file):
    return __BUILDERS__[config['codebase_name']].prepare_local_exp_directory(config, config_file)


def prepare_remote_server_codebase(config, server_host, local_exp_directory, remote_out_directory):
    return __BUILDERS__[config['codebase_name']].prepare_remote_server_codebase(config, server_host, local_exp_directory, remote_out_directory)


def setup_nodes(config):
    return __BUILDERS__[config['codebase_name']].setup_nodes(config)
