import os
import sys

from lib.experiment_codebase import ExperimentCodebase

from utils.remote_util import get_coordinator_host
from utils.remote_util import get_coordinator_port
from utils.remote_util import get_master_host
from utils.remote_util import get_master_port
from utils.remote_util import get_replica_host
from utils.remote_util import get_replica_port
from utils.remote_util import get_replica_rpc_port
from utils.remote_util import is_exp_local
from utils.remote_util import is_using_tcsh
from utils.remote_util import tcsh_redirect_output_to_files


class MDLCodebase(ExperimentCodebase):

    def get_replication_protocol_arg_from_name(self, replication_protocol):
        return {
            'epaxos': ' -e',
            'gpaxos': ' -g',
            'mencius': ' -m',
            'multi_paxos': '',
            'abd': ' -a',
            'gryff': ' -t',
            'mdl': ' -mdl'
        }[replication_protocol]

    def get_client_cmd(self, config, i, k, run, local_exp_directory, remote_exp_directory):
        client = config["clients"][i]

        if is_exp_local(config):
            exp_directory = local_exp_directory
            path_to_client_bin = os.path.join(config['src_directory'],
                                              config['bin_directory_name'],
                                              config['client_bin_name'])
            stats_file = os.path.join(exp_directory,
                                      config['out_directory_name'], client,
                                      '%s-%d-stats-%d.json' % (client, k, run))
        else:
            exp_directory = remote_exp_directory
            path_to_client_bin = os.path.join(config['base_remote_bin_directory_nfs'],
                                              config['bin_directory_name'],
                                              config['client_bin_name'])

            stats_file = os.path.join(exp_directory,
                                      config['out_directory_name'],
                                      '%s-%d-stats-%d.json' % (client, k, run))

        coordinator_host = get_coordinator_host(config)
        coordinator_port = get_coordinator_port(config)

        client_id = i * config['client_processes_per_client_node'] + k

        client_command = ' '.join([str(x) for x in [
            path_to_client_bin,
            '-clientId', client_id,
            '-expLength', config['client_experiment_length'],
            '-caddr', coordinator_host,
            '-cport', coordinator_port,
            '-maxProcessors', config['client_max_processors'],
            '-numKeys', config['client_num_keys'],
            '-rampDown', config['client_ramp_down'],
            '-rampUp', config['client_ramp_up'],
            '-reads', config['client_read_percentage'],
            '-replProtocol', config['replication_protocol'],
            '-rmws', config['client_rmw_percentage'],
            '-statsFile', stats_file,
            '-writes', config['client_write_percentage'],
            '-fanout', config['client_fanout'],
        ]])
        if 'client_cpuprofile' in config and config['client_cpuprofile']:
            client_command += ' -cpuProfile %s' % os.path.join(exp_directory,
                                                               config['out_directory_name'],
                                                               '%s-%d-cpuprof-%d.log' % (client, k, run))
        if 'client_rand_sleep' in config:
            client_command += ' -randSleep %d' % config['client_rand_sleep']
        if config['client_conflict_percentage'] < 0:
            client_command += ' -zipfS %f' % config['client_zipfian_s']
            client_command += ' -zipfV %f' % config['client_zipfian_v']
        else:
            client_command += ' -conflicts %s' % config['client_conflict_percentage']
        # TODO need logic for how to determine leader for various protocols
        if config['replication_protocol'] == 'gpaxos':
            client_command += ' -fastPaxos'
        if config['client_random_coordinator']:
            client_command += ' -randomLeader'
        if config['client_debug_output']:
            client_command += ' -debug'
        if 'single_shard_aware' in config['replication_protocol_settings'] and config['replication_protocol_settings']['single_shard_aware']:
            client_command += ' -SSA'
        if 'server_epaxos_mode' in config['replication_protocol_settings'] and config['replication_protocol_settings']['server_epaxos_mode']:
            client_command += ' -epaxosMode'

        if 'server_emulate_wan' in config and not config['server_emulate_wan']:
            client_command += ' -defaultReplicaOrder'
            client_command += ' -forceLeader %d' % i

        if config['replication_protocol'] == 'abd':
            if config['replication_protocol_settings']['client_regular_consistency']:
                client_command += ' -regular'
        elif config['replication_protocol'] == 'gryff':
            if config['replication_protocol_settings']['client_regular_consistency']:
                client_command += ' -regular'
            if config['replication_protocol_settings']['client_sequential_consistency']:
                client_command += ' -sequential'
        if 'proxy_operations' in config['replication_protocol_settings'] and config['replication_protocol_settings']['proxy_operations']:
            client_command += ' -proxy'
        if 'server_thrifty' in config['replication_protocol_settings'] and config['replication_protocol_settings']['server_thrifty']:
            client_command += ' -thrifty'
        if 'client_tail_at_scale' in config and config['client_tail_at_scale'] > 0:
            client_command += ' -tailAtScale %d' % config['client_tail_at_scale']
        if 'client_gc_debug_trace' in config and config['client_gc_debug_trace']:
            if is_exp_local(config) or not is_using_tcsh(config):
                client_command = 'GODEBUG=\'gctrace=1\'; %s' % client_command
            else:
                client_command = 'setenv GODEBUG gctrace=1; %s' % client_command
        if 'client_disable_gc' in config and config['client_disable_gc']:
            if is_exp_local(config) or not is_using_tcsh(config):
                client_command = 'GOGC=off; %s' % client_command
            else:
                client_command = 'setenv GOGC off; %s' % client_command

        if is_exp_local(config):
            stdout_file = os.path.join(exp_directory,
                                       config['out_directory_name'],
                                       client,
                                       '%s-%d-stdout-%d.log' % (client, k, run))
            stderr_file = os.path.join(exp_directory,
                                       config['out_directory_name'],
                                       client,
                                       '%s-%d-stderr-%d.log' % (client, k, run))
            client_command = '%s 1> %s 2> %s' % (client_command, stdout_file,
                                                 stderr_file)
        else:
            stdout_file = os.path.join(exp_directory,
                                       config['out_directory_name'],
                                       '%s-%d-stdout-%d.log' % (client, k, run))
            stderr_file = os.path.join(exp_directory,
                                       config['out_directory_name'],
                                       '%s-%d-stderr-%d.log' % (client, k, run))
            if is_using_tcsh(config):
                client_command = tcsh_redirect_output_to_files(client_command,
                                                               stdout_file, stderr_file)
            else:
                client_command = '%s 1> %s 2> %s' % (client_command, stdout_file,
                                                     stderr_file)

        client_command = '(cd %s; %s) & ' % (exp_directory, client_command)
        client_command = 'cd /users/akalaba/redis-chat-transformed; cp /users/akalaba/test.json %s; source anjaredischat/bin/activate; python app_app_sync.py 1> %s 2> %s' % (stats_file, stdout_file, stderr_file)
        print(client_command)
        return client_command


    def get_replica_cmd(self, config, shard_idx, replica_idx, run, local_exp_directory,
                        remote_exp_directory):
        if is_exp_local(config):
            path_to_server_bin = os.path.join(config['src_directory'],
                                              config['bin_directory_name'],
                                              config['server_bin_name'])

            exp_directory = local_exp_directory
            stats_file = os.path.join(exp_directory,
                                      config['out_directory_name'],
                                      'server-%d' % shard_idx,
                                      'server-%d-%d-stats-%d.json' % (shard_idx, replica_idx, run))
        else:
            path_to_server_bin = os.path.join(config['base_remote_bin_directory_nfs'],
                                              config['bin_directory_name'],
                                              config['server_bin_name'])
            exp_directory = remote_exp_directory
            stats_file = os.path.join(exp_directory,
                                      config['out_directory_name'],
                                      'server-%d-%d-stats-%d.json' % (shard_idx, replica_idx, run))

        master_host = get_master_host(config, shard_idx)
        master_port = get_master_port(config, shard_idx)

        replica_host = get_replica_host(config, shard_idx, replica_idx, full=False)
        replica_port = get_replica_port(config, shard_idx, replica_idx)
        replica_rpc_port = get_replica_rpc_port(config, shard_idx, replica_idx)

        replica_command = ' '.join([str(x) for x in [
            path_to_server_bin,
            '-addr', replica_host,
            '-port', replica_port,
            '-rpcport', replica_rpc_port,
            '-maddr', master_host,
            '-mport', master_port,
            '-statsFile', stats_file,
            '-nshards', config['num_shards'],
            '-fanout', config['client_fanout'],
            '-epoch', config['server_epoch']
            ]])
        replica_command += self.get_replication_protocol_arg_from_name(config['replication_protocol'])
        if 'proxy_operations' in config['replication_protocol_settings'] and config['replication_protocol_settings']['proxy_operations']:
            replica_command += ' -proxy'
        if config['server_durable']:
            replica_command += ' -durable'
        if config['server_cpuprofile']:
            replica_command += ' -cpuprofile %s' % os.path.join(
                exp_directory, config['out_directory_name'], 'server-%d-%d-cpuprof-%d.prof' % (shard_idx, replica_idx, run))
        if 'server_blockprofile' in config and config['server_blockprofile']:
            replica_command += ' -blockprofile %s' % os.path.join(
                exp_directory, config['out_directory_name'], 'server-%d-%d-blockprof-%d.prof' % (shard_idx, replica_idx, run))
        if 'server_memprofile' in config and config['server_memprofile']:
            replica_command += ' -memProfile %s' % os.path.join(
                exp_directory, config['out_directory_name'], 'server-%d-%d-memprof-%d.prof' % (shard_idx, replica_idx, run))
        if config['server_debug_output']:
            replica_command += ' -debug'
        if 'server_emulate_wan' in config and config['server_emulate_wan']:
            replica_command += ' -beacon'
        if 'server_execute_commands' in config['replication_protocol_settings'] and config['replication_protocol_settings']['server_execute_commands']:
            replica_command += ' -exec'
        if 'server_delay_reply' in config['replication_protocol_settings'] and config['replication_protocol_settings']['server_delay_reply']:
            replica_command += ' -dreply'
        if 'server_thrifty' in config['replication_protocol_settings'] and config['replication_protocol_settings']['server_thrifty']:
            replica_command += ' -thrifty'
        if 'server_no_conflicts' in config['replication_protocol_settings'] and config['replication_protocol_settings']['server_no_conflicts'] and config['client_conflict_percentage'] == 0:
            replica_command += ' -noConflicts'
        if 'server_epaxos_mode' in config['replication_protocol_settings'] and config['replication_protocol_settings']['server_epaxos_mode']:
            replica_command += ' -epaxosMode'
        if 'client_regular_consistency' in config['replication_protocol_settings'] and config['replication_protocol_settings']['client_regular_consistency']:
            replica_command += ' -regular'
        if 'server_shortcircuit_timeout' in config['replication_protocol_settings']:
            replica_command += ' -shortcircuitTime %d' % config['replication_protocol_settings']['server_shortcircuit_timeout']
        if 'server_fast_overwrite_timeout' in config['replication_protocol_settings']:
            replica_command += ' -fastOverwriteTime %d' % config['replication_protocol_settings']['server_fast_overwrite_timeout']
        if 'server_force_write_period' in config['replication_protocol_settings']:
            replica_command += ' -forceWritePeriod %d' % config['replication_protocol_settings']['server_force_write_period']
        if 'first_round_batching' in config['replication_protocol_settings'] and config['replication_protocol_settings']['first_round_batching']:
            replica_command += ' -batching'
        if 'second_round_batching' in config['replication_protocol_settings'] and config['replication_protocol_settings']['second_round_batching']:
            replica_command += ' -epochBatching'
        if 'single_shard_aware' in config['replication_protocol_settings'] and config['replication_protocol_settings']['single_shard_aware']:
            replica_command += ' -singleShardAware'
            if config['num_shards'] != 1:
                sys.stderr.write('!!Can only run Single-Shard-Aware mode with 1-shard configuration!!\n')
                raise

        if 'server_gc_debug_trace' in config and config['server_gc_debug_trace']:
            if is_exp_local(config) or not is_using_tcsh(config):
                replica_command = 'GOGC=off; %s' % replica_command
            else:
                replica_command = 'setenv GODEBUG gctrace=1; %s' % replica_command
        if 'server_disable_gc' in config and config['server_disable_gc']:
            if is_exp_local(config) or not is_using_tcsh(config):
                replica_command = 'GOGC=off; %s' % replica_command
            else:
                replica_command = 'setenv GOGC off; %s' % replica_command

        if is_exp_local(config):
            stdout_file = os.path.join(exp_directory,
                                       config['out_directory_name'],
                                       'server-%d' % shard_idx,
                                       'server-%d-%d-stdout-%d.log' % (shard_idx, replica_idx, run))
            stderr_file = os.path.join(exp_directory,
                                       config['out_directory_name'],
                                       'server-%d' % shard_idx,
                                       'server-%d-%d-stderr-%d.log' % (shard_idx, replica_idx, run))
            replica_command = '%s 1> %s 2> %s' % (replica_command, stdout_file,
                                                  stderr_file)
        else:
            stdout_file = os.path.join(exp_directory,
                                       config['out_directory_name'],
                                       'server-%d-%d-stdout-%d.log' % (shard_idx, replica_idx, run))
            stderr_file = os.path.join(exp_directory,
                                       config['out_directory_name'],
                                       'server-%d-%d-stderr-%d.log' % (shard_idx, replica_idx, run))
            if is_using_tcsh(config):
                replica_command = tcsh_redirect_output_to_files(replica_command,
                                                                stdout_file,
                                                                stderr_file)
            else:
                replica_command = '%s 1> %s 2> %s' % (replica_command,
                                                      stdout_file,
                                                      stderr_file)

        replica_command = 'cd %s; %s' % (exp_directory, replica_command)
        return replica_command

    def prepare_local_exp_directory(self, config, config_file):
        return super().prepare_local_exp_directory(config, config_file)

    def prepare_remote_server_codebase(self, config, server_host, local_exp_directory, remote_out_directory):
        pass

    def setup_nodes(self, config):
        pass
