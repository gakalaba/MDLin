import json
import time
import concurrent.futures
import os
import sys
import threading

from utils.remote_util import *
from utils.git_util import *
from utils.eval_util import *
from lib.wrappers import *


def is_using_masters(config):
    return "use_masters" in config and config["use_masters"]


def collect_exp_data(config, remote_exp_directory, local_directory_base, executor):
    futures = []
    remote_directory = os.path.join(remote_exp_directory, config["out_directory_name"])

    if is_using_masters(config):
        coordinator_host = get_coordinator_host(config)
        futures.append(executor.submit(copy_remote_directory_to_local,
                                       os.path.join(local_directory_base, "coordinator"),
                                       config["emulab_user"], coordinator_host, remote_directory,
                                       file_filter="coordinator-*.*"))

        for i in range(config["num_shards"]):
            master_host = get_master_host(config, i)
            futures.append(executor.submit(copy_remote_directory_to_local,
                                           os.path.join(local_directory_base, "master-{}".format(i)),
                                           config["emulab_user"], master_host, remote_directory,
                                           file_filter="master-{}-*.*".format(i)))

    for shard_idx in range(len(config["shards"])):
        shard = config["shards"][shard_idx]
        for replica_idx in range(len(shard)):
            server_host = get_replica_host(config, shard_idx, replica_idx)

            futures.append(executor.submit(copy_remote_directory_to_local,
                                           os.path.join(local_directory_base, "server-{}".format(shard_idx)),
                                           config["emulab_user"], server_host, remote_directory,
                                           file_filter="server-{}-{}-*.*".format(shard_idx, replica_idx)))

    for client in config["clients"]:
        client_host = get_client_host(config, client)
        futures.append(executor.submit(copy_remote_directory_to_local,
                                       os.path.join(local_directory_base, client),
                                       config["emulab_user"], client_host, remote_directory))

    concurrent.futures.wait(futures)


def kill_servers(config, executor, kill_args='-9'):
    futures = []
    for i in range(len(config['server_names'])):
        server_host = get_server_host(config, i)
        if is_exp_remote(config):
            futures.append(executor.submit(kill_remote_process_by_name,
                                           config['server_bin_name'], config['emulab_user'],
                                           server_host, kill_args))
        else:
            futures.append(executor.submit(kill_process_by_name,
                                           config['server_bin_name'], kill_args))
    concurrent.futures.wait(futures)


def kill_clients_no_config(config, executor):
    futures = []
    for client in config["clients"]:
        client_host = get_client_host(config, client)
        if is_exp_remote(config):
            futures.append(executor.submit(kill_remote_process_by_name,
                                           config['client_bin_name'], config['emulab_user'],
                                           client_host, '-9'))
        else:
            futures.append(executor.submit(kill_process_by_name,
                                           config['client_bin_name'], '-9'))
    concurrent.futures.wait(futures)


def kill_clients(config, executor):
    kill_clients_no_config(config, executor)


def kill_masters(config, executor):
    n_shards = config["num_shards"]

    futures = []
    for i in range(n_shards):
        master_host = get_master_host(config, i)
        if is_exp_remote(config):
            futures.append(executor.submit(kill_remote_process_by_name,
                                           config["master_bin_name"],
                                           config["emulab_user"],
                                           master_host, "-9"))
        else:
            futures.append(executor.submit(kill_process_by_name,
                                           config["master_bin_name"], "-9"))

    if is_exp_remote(config):
        coordinator_host = get_coordinator_host(config)
        futures.append(executor.submit(kill_remote_process_by_name,
                                       config["coordinator_bin_name"],
                                       config["emulab_user"],
                                       coordinator_host, "-9"))
    else:
        futures.append(executor.submit(kill_process_by_name,
                                       config["coordinator_bin_name"], "-9"))

    concurrent.futures.wait(futures)


def terminate_clients_on_timeout(timeout, cond, client_ssh_threads):
    start = time.time()
    need_terminate = True
    while time.time() - start < timeout:
        cond.acquire()
        need_terminate = not cond.wait(timeout - (time.time() - start))
        cond.release()
    if need_terminate:
        print("Terminating clients")
        for c in client_ssh_threads:
            c.terminate()


def wait_for_clients_to_terminate(config, client_ssh_threads):
    cond = threading.Condition()
    timeout_thread = threading.Thread(
        target=terminate_clients_on_timeout,
        args=(config['client_experiment_length'] + 10,
              cond,
              client_ssh_threads))
    timeout_thread.daemon = True
    timeout_thread.start()
    for c in client_ssh_threads:
        c.wait()
    cond.acquire()
    cond.notify()
    cond.release()
    #time.sleep(config['client_experiment_length'] + 10)


def start_clients(config, local_exp_directory, remote_exp_directory, run):
    assert(config["client_total"] == (len(config["clients"])
                                      * config["client_processes_per_client_node"]))
    client_processes = []
    for i in range(len(config["clients"])):
        client = config["clients"][i]
        if is_exp_local(config):
            os.makedirs(os.path.join(local_exp_directory,
                                     config["out_directory_name"], client))

        client_host = get_client_host(config, client)

        appended_client_commands = ""
        for k in range(config["client_processes_per_client_node"]):
            appended_client_commands += get_client_cmd(
                config, i, k, run, local_exp_directory, remote_exp_directory)

            if k != 0 and k % 128 == 0:
                if appended_client_commands[-2:] == '& ':
                    appended_client_commands = appended_client_commands[:-2]
                if is_exp_remote(config):
                    client_processes.append(run_remote_command_async(
                        appended_client_commands + ' & wait',
                        config['emulab_user'],
                        client_host, False))
                else:
                    client_processes.append(run_local_command_async(
                                                appended_client_commands + ' & wait'))
                    print(appended_client_commands)
                appended_client_commands = ''

        if len(appended_client_commands) > 0:
            if appended_client_commands[-2:] == '& ':
                appended_client_commands = appended_client_commands[:-2]
            if is_exp_remote(config):
                client_processes.append(run_remote_command_async(
                    appended_client_commands + ' & wait',
                    config['emulab_user'],
                    client_host, False))
            else:
                client_processes.append(run_local_command_async(
                    appended_client_commands + ' & wait'))

    return client_processes


def start_servers(config, local_exp_directory, remote_exp_directory, run):
    server_threads = []

    server_names = config["server_names"]

    n_shards = len(config["shards"])
    assert(n_shards == config["num_shards"])

    start_commands = {}
    for shard_idx in range(n_shards):
        n_replicas = len(config["shards"][shard_idx])

        for replica_idx in range(n_replicas):
            replica_host = get_replica_host(config, shard_idx, replica_idx)

            if is_exp_local(config):
                out_dir = os.path.join(local_exp_directory,
                                       config['out_directory_name'],
                                       'server-%d' % (shard_idx))
                os.makedirs(out_dir, exist_ok=True)

            server_command = get_replica_cmd(config, shard_idx,
                                             replica_idx, run,
                                             local_exp_directory, remote_exp_directory)

            if not replica_host in start_commands:
                start_commands[replica_host] = '(%s)' % server_command
            else:
                start_commands[replica_host] += ' & (%s)' % server_command

    for replica_host, cmd in start_commands.items():
        if is_exp_remote(config):
            server_threads.append(run_remote_command_async(cmd,
                                                           config['emulab_user'],
                                                           replica_host, detach=False))
        else:
            server_threads.append(run_local_command_async(cmd))
        time.sleep(0.1)

    time.sleep(1)
    return server_threads


def start_coordinator(config, local_exp_directory, remote_exp_directory, run):
    if is_exp_remote(config):
        exp_directory = remote_exp_directory
        path_to_coordinator_bin = os.path.join(config["base_remote_bin_directory_nfs"],
                                               config["bin_directory_name"],
                                               config["coordinator_bin_name"])
    else:
        exp_directory = local_exp_directory
        path_to_coordinator_bin = os.path.join(config["src_directory"],
                                               config["bin_directory_name"],
                                               config["coordinator_bin_name"])
                                                           
        out_dir = os.path.join(local_exp_directory,
                               config["out_directory_name"],
                               "coordinator")
        os.makedirs(out_dir, exist_ok=True)

    n_shards = config["num_shards"]
    masters_hosts = []
    for i in range(n_shards):
        master_host = get_master_host(config, i)
        masters_hosts.append(master_host)

    coordinator_host = get_coordinator_host(config)
    coordinator_port = get_coordinator_port(config)

    coordinator_command = ' '.join([str(x) for x in [path_to_coordinator_bin,
                                                     "-port", coordinator_port,
                                                     "-N", n_shards,
                                                     "-ips", ','.join(masters_hosts)]])

    if is_exp_remote(config):
        stdout_file = os.path.join(exp_directory,
                                   config["out_directory_name"],
                                   "coordinator-stdout-{}.log".format(run))

        stderr_file = os.path.join(exp_directory,
                                   config["out_directory_name"],
                                   "coordinator-stderr-{}.log".format(run))
    else:
        stdout_file = os.path.join(exp_directory,
                                   config["out_directory_name"],
                                   "coordinator",
                                   "coordinator-stdout-{}.log".format(run))

        stderr_file = os.path.join(exp_directory,
                                   config["out_directory_name"],
                                   "coordinator",
                                   "coordinator-stderr-{}.log".format(run))


    if is_using_tcsh(config):
        coordinator_command = tcsh_redirect_output_to_files(coordinator_command,
                                                            stdout_file, stderr_file)
    else:
        coordinator_command = '%s 1> %s 2> %s' % (coordinator_command,
                                                  stdout_file, stderr_file)

    coordinator_command = 'cd %s; %s' % (exp_directory, coordinator_command)

    if is_exp_remote(config):
        coordinator_thread = run_remote_command_async(coordinator_command,
                                                      config["emulab_user"],
                                                      coordinator_host, detach=False)
    else:
        coordinator_thread = run_local_command_async(coordinator_command)

    return coordinator_thread


def start_masters(config, local_exp_directory, remote_exp_directory, run):
    master_threads = []

    master_threads.append(start_coordinator(config, local_exp_directory,
                                            remote_exp_directory, run))

    if is_exp_remote(config):
        exp_directory = remote_exp_directory
        path_to_master_bin = os.path.join(
            config['base_remote_bin_directory_nfs'],
            config['bin_directory_name'], config['master_bin_name'])
    else:
        exp_directory = local_exp_directory
        path_to_master_bin = os.path.join(
            config['src_directory'],
            config['bin_directory_name'], config['master_bin_name'])

    n_shards = config["num_shards"]
    shards = config["shards"]
    assert(len(shards) == n_shards)

    for i in range(n_shards):
        if is_exp_local(config):
            out_dir = os.path.join(local_exp_directory,
                config["out_directory_name"],
                "master-%d" % (i))
            os.makedirs(out_dir, exist_ok=True)

        coordinator_host = get_coordinator_host(config)
        coordinator_port = get_coordinator_port(config)
        master_host = get_master_host(config, i)
        master_port = get_master_port(config, i)

        n_replicas = len(shards[i])
        replica_hosts = []
        for j in range(n_replicas):
            replica_hosts.append(get_replica_host(config, i, j, full=False))

        master_command = ' '.join([str(x) for x in [path_to_master_bin,
                                                    '-addr', master_host,
                                                    '-port', master_port,
                                                    '-caddr', coordinator_host,
                                                    '-cport', coordinator_port,
                                                    '-N', n_replicas,
                                                    '-ips', ','.join(replica_hosts),
                                                    '-nshrds', n_shards,
                                                    '-shardId', i]])

        if is_exp_remote(config):
            stdout_file = os.path.join(exp_directory,
                                       config['out_directory_name'],
                                       'master-%d-stdout-%d.log' % (i, run))

            stderr_file = os.path.join(exp_directory,
                                       config['out_directory_name'],
                                       'master-%d-stderr-%d.log' % (i, run))
        else:
            stdout_file = os.path.join(exp_directory,
                                       config['out_directory_name'],
                                       'master-%d' % (i),
                                       'master-%d-stdout-%d.log' % (i, run))

            stderr_file = os.path.join(exp_directory,
                                       config['out_directory_name'],
                                       'master-%d' % (i),
                                       'master-%d-stderr-%d.log' % (i, run))

        if is_using_tcsh(config):
            master_command = tcsh_redirect_output_to_files(master_command,
                                                           stdout_file, stderr_file)
        else:
            master_command = '%s 1> %s 2> %s' % (master_command,
                                                 stdout_file, stderr_file)

        master_command = 'cd %s; %s' % (exp_directory, master_command)

        if is_exp_remote(config):
            master_threads.append(run_remote_command_async(master_command,
                                                           config['emulab_user'],
                                                           master_host, detach=False))
        else:
            master_threads.append(run_local_command_async(master_command))

    return master_threads


SERVERS_SETUP = {}


# TODO: Combine two "prepare" functions
def prepare_remote_server(config, server_host, local_exp_directory, remote_out_directory):
    if server_host not in SERVERS_SETUP:
        set_file_descriptor_limit(
            config['max_file_descriptors'], config['emulab_user'], server_host)
        change_mounted_fs_permissions(
            config['project_name'], config['emulab_user'], server_host, config['base_mounted_fs_path'])
        SERVERS_SETUP[server_host] = True
    change_mounted_fs_permissions(
        config['project_name'], config['emulab_user'], server_host, config['base_remote_exp_directory'])
    copy_path_to_remote_host(local_exp_directory, config['emulab_user'],
                             server_host, config['base_remote_exp_directory'])
    run_remote_command_sync(
        'mkdir -p %s' % remote_out_directory, config['emulab_user'], server_host)
    prepare_remote_server_codebase(
        config, server_host, local_exp_directory, remote_out_directory)


def prepare_remote_client(config, client_host, local_exp_directory, remote_out_directory):
    if client_host not in SERVERS_SETUP:
        set_file_descriptor_limit(
            config['max_file_descriptors'], config['emulab_user'], client_host)
        change_mounted_fs_permissions(
            config['project_name'], config['emulab_user'], client_host, config['base_mounted_fs_path'])
        SERVERS_SETUP[client_host] = True
    change_mounted_fs_permissions(
        config['project_name'], config['emulab_user'], client_host, config['base_remote_exp_directory'])
    copy_path_to_remote_host(local_exp_directory, config['emulab_user'],
                             client_host, config['base_remote_exp_directory'])
    run_remote_command_sync(
        'mkdir -p %s' % remote_out_directory, config['emulab_user'], client_host)
    prepare_remote_server_codebase(
        config, client_host, local_exp_directory, remote_out_directory)


def prepare_remote_exp_directories(config, local_exp_directory, executor):
    remote_directory = os.path.join(config['base_remote_exp_directory'],
                                    os.path.basename(local_exp_directory))
    remote_out_directory = os.path.join(remote_directory, config['out_directory_name'])

    futures = []

    for i in range(len(config['server_names'])):
        server_host = get_server_host(config, i)
        futures.append(executor.submit(prepare_remote_server, config, server_host,
                                       local_exp_directory, remote_out_directory))

    for client in config['clients']:
        client_host = get_client_host(config, client)
        futures.append(executor.submit(prepare_remote_client, config, client_host,
                                       local_exp_directory, remote_out_directory))

    concurrent.futures.wait(futures)
    return remote_directory


def collect_and_calculate(config, client_config_idx, remote_exp_directory, local_out_directory, executor):
    if is_exp_remote(config):
        collect_exp_data(config, remote_exp_directory, local_out_directory, executor)

    stats, op_latencies = calculate_statistics(config, local_out_directory)
    generate_cdf_plots(config, local_out_directory, stats, executor)
    #generate_lot_plots(config, local_out_directory, stats, op_latencies, executor)
    return local_out_directory


def get_arg_max():
    return int(subprocess.run(['getconf', 'ARG_MAX'], stdout=subprocess.PIPE,
                              universal_newlines=True).stdout)


def setup_delays(config, wan, executor):
    futures = []
    name_to_ip = get_name_to_ip_map(config, config['emulab_user'],
                                    get_server_host(config, 0))

    for i in range(len(config['server_names'])):
        server_host = get_server_host(config, i)
        server_ip_to_delay = get_ip_to_delay(
            config, name_to_ip, config['server_names'][i], True)
        if wan:
            futures.append(executor.submit(get_iface_add_delays,
                                           server_ip_to_delay, config['max_bandwidth'],
                                           config['emulab_user'], server_host))
        else:
            futures.append(executor.submit(remove_delays, config['emulab_user'],
                                           server_host))

    for client in config['clients']:
        client_host = get_client_host(config, client)
        client_ip_to_delay = get_ip_to_delay(config, name_to_ip, client)
        if wan:
            futures.append(executor.submit(get_iface_add_delays,
                                           client_ip_to_delay, config['max_bandwidth'],
                                           config['emulab_user'], client_host))
        else:
            futures.append(executor.submit(remove_delays,
                                           config['emulab_user'], client_host))

    concurrent.futures.wait(futures)


def get_local_path_to_bins(config):
    return os.path.join(config['src_directory'], config['bin_directory_name'])


def copy_binaries_to_nfs(config, executor):
    if 'remade_binaries' not in SERVERS_SETUP:
        remake_binaries(config)
        SERVERS_SETUP['remade_binaries'] = True
    nfs_enabled = not 'remote_bin_directory_nfs_enabled' in config or config[
        'remote_bin_directory_nfs_enabled']
    n = 1 if nfs_enabled else len(config['server_names'])
    futures = []
    for i in range(n):
        server_host = get_server_host(config, i)
        if server_host not in SERVERS_SETUP:
            futures.append(executor.submit(copy_path_to_remote_host,
                                           os.path.join(config['src_directory'],
                                                        config['bin_directory_name']),
                                           config['emulab_user'],
                                           server_host,
                                           config['base_remote_bin_directory_nfs']))
    if not nfs_enabled:
        for client in config['clients']:
            client_host = get_client_host(config, client)
            if client_host not in SERVERS_SETUP:
                futures.append(executor.submit(copy_path_to_remote_host,
                                               os.path.join(config['src_directory'],
                                                            config['bin_directory_name']),
                                               config['emulab_user'],
                                               client_host,
                                               config['base_remote_bin_directory_nfs']))

    concurrent.futures.wait(futures)


def run_experiment(config_file, client_config_idx, executor):
    with open(config_file) as f:
        config = json.load(f)
        if not 'server_regions' in config:
            config['server_regions'] = {}
            for server_name in config['server_names']:
                config['server_regions'][server_name] = [server_name]

        if not 'region_rtt_latencies' in config:
            config['region_rtt_latencies'] = config['server_ping_latencies']

        if not 'client_stats_blacklist' in config:
            config['client_stats_blacklist'] = []
        if not 'client_combine_stats_blacklist' in config:
            config['client_combine_stats_blacklist'] = []
        if not 'client_cdf_plot_blacklist' in config:
            config['client_cdf_plot_blacklist'] = []
        if not 'client_total' in config:
            config['client_total'] = config['client_nodes_per_server'] * \
                config['client_processes_per_client_node'] * \
                len(config['server_names'])

        if is_exp_remote(config):
            print('Setting up emulated WAN latencies.')
            wan = is_emulate_wan(config)
            setup_delays(config, wan, executor)

        kill_servers(config, executor)
        kill_clients(config, executor)
        kill_masters(config, executor)

        if is_exp_remote(config):
            copy_binaries_to_nfs(config, executor)

        setup_nodes(config)

        local_exp_directory = prepare_local_exp_directory(config, config_file)
        local_out_directory = os.path.join(local_exp_directory,
                                           config['out_directory_name'])
        if is_exp_local(config):
            os.makedirs(local_out_directory)
            remote_exp_directory = None
        else:
            remote_exp_directory = prepare_remote_exp_directories(config,
                                                                  local_exp_directory,
                                                                  executor)
        
        for i in range(config['num_experiment_runs']):
            kill_clients(config, executor)
            servers_alive = False
            retries = 0
            master_threads = None
            server_threads = None
            while not servers_alive and retries <= config['max_retries']:
                if is_using_masters(config):
                    kill_masters(config, executor)
                    master_threads = start_masters(config, local_exp_directory,
                                                   remote_exp_directory, i)
                
                kill_servers(config, executor)
                time.sleep(2)
                
                server_threads = start_servers(config, local_exp_directory,
                                               remote_exp_directory, i)
                all_alive = True
                for server_thread in server_threads:
                    if server_thread.poll() != None:
                        all_alive = False
                        break
                servers_alive = all_alive
                retries += 1
            if not servers_alive:
                sys.stderr.write('Failed to start all servers.\n')
                raise
            #print("Waiting {} seconds for servers to finish setup".format(5))
            #time.sleep(5)
            client_threads = start_clients(config, local_exp_directory,
                                           remote_exp_directory, i)
            wait_for_clients_to_terminate(config, client_threads)
            print("Waiting {} seconds for clients to finish".format(5))
            time.sleep(5)
            kill_clients(config, executor)
            time.sleep(1)
            for server_thread in server_threads:
                server_thread.terminate()
            kill_servers(config, executor, '-15')
            if is_using_masters(config):
                for master_thread in master_threads:
                    master_thread.terminate()
                kill_masters(config, executor)
        return executor.submit(collect_and_calculate, config,
                               client_config_idx, remote_exp_directory, local_out_directory,
                               executor)


def run_multiple_experiments(config_file, executor):
    start = time.time()
    exp_dir = None
    out_dirs = None
    with open(config_file) as f:
        config = json.load(f)

        # verify that we can run all of the experiments
        if len(config['experiment_independent_vars']) == 0:
            sys.stderr.write(
                'Need at least 1 independent variable to run multiple experiments.\n')
            sys.exit(1)
        if not 'experiment_independent_vars_unused' in config:
            config['experiment_independent_vars_unused'] = config['experiment_independent_vars']
        for i in range(len(config['experiment_independent_vars_unused'])):
            for j in range(len(config['experiment_independent_vars_unused'][i])):
                for k in range(j):
                    if len(config[config['experiment_independent_vars_unused'][i][j]]) != len(config[config['experiment_independent_vars_unused'][i][k]]):
                        sys.stderr.write('%s and %s arrays in config file must have same length.\n' % (
                            config['experiment_independent_vars_unused'][i][j],
                            config['experiment_independent_vars_unused'][i][k]))
                        sys.exit(1)

        config_name = os.path.splitext(os.path.basename(config_file))[0]

        exp_futs = []
        exp_futs_idxs = []
        config_files = []
        indep_vars_list = []

        exp_dir = get_timestamped_exp_dir(config)
        os.makedirs(exp_dir, exist_ok=True)

        out_dirs = []
        sub_out_dirs = []
        for i in range(len(config[config['experiment_independent_vars_unused'][0][0]])):
            config_new = config.copy()
            config_new['base_local_exp_directory'] = exp_dir
            config_new['experiment_independent_vars_unused'] = config['experiment_independent_vars_unused'][1:]

            for j in range(len(config['experiment_independent_vars_unused'][0])):
                config_new[config['experiment_independent_vars_unused'][0][j]
                           ] = config[config['experiment_independent_vars_unused'][0][j]][i]

            config_file_new = os.path.join(
                exp_dir, '%s-%d.json' % (config_name, i))
            with open(config_file_new, 'w+') as f_new:
                json.dump(config_new, f_new, indent=2, sort_keys=True)
            config_files.append(config_file_new)

            if len(config_new['experiment_independent_vars_unused']) == 0:
                exp_futs.append(run_experiment(config_file_new, i, executor))
                exp_futs_idxs.append(i)
            else:
                out_directory, sub_out_directories = run_multiple_experiments(
                    config_file_new, executor)
                out_dirs.append(out_directory)
                sub_out_dirs.append(sub_out_directories)

        retries = 0
        while len(out_dirs) < len(config[config['experiment_independent_vars_unused'][0][0]]) and retries <= config['max_retries']:
            retry_exp_futs = []
            for i in range(len(exp_futs)):
                try:
                    out_dir = exp_futs[i].result()
                    sub_out_dirs.insert(exp_futs_idxs[i], out_dir)
                except:
                    print('Unexpected error during %s %d: ' %
                          (config_files[exp_futs_idxs[i]], exp_futs_idxs[i]))
                    print(traceback.format_exc())
                    retry_exp_futs.append(exp_futs_idxs[i])
            if len(out_dirs) == len(config[config['experiment_independent_vars_unused'][0][0]]):
                break
            exp_futs = []
            exp_futs_idxs = []
            for j in retry_exp_futs:
                exp_futs.append(run_experiment(config_files[j], j, executor))
                exp_futs_idxs.append(j)
            retries += 1

        print("%s took %f seconds!" % (config_name, time.time() - start))

        print(exp_dir)
        out = [sub_out_dirs, out_dirs]
        generate_plots(config, exp_dir, out)
    return exp_dir, out


def run_varying_clients_experiment(config_file, executor):
    start = time.time()
    exp_dir = None
    out_dirs = None
    with open(config_file) as f:
        config = json.load(f)
        if len(config['client_nodes_per_server']) != len(config['client_processes_per_client_node']):
            sys.stderr.write('%s and %s arrays in config file must have same length.\n' % (
                'client_nodes_per_server', 'client_processes_per_node'))

        config_name = os.path.splitext(os.path.basename(config_file))[0]
        exp_futs = []
        exp_futs_idxs = []
        config_files = []
        exp_dir = get_timestamped_exp_dir(config)
        os.makedirs(exp_dir, exist_ok=True)
        for i in range(len(config['client_nodes_per_server'])):
            config_new = config.copy()
            config_new['base_local_exp_directory'] = exp_dir
            n = config['client_nodes_per_server'][i]
            m = config['client_processes_per_client_node'][i]
            if 'client_total' in config:
                config_new['client_total'] = config['client_total'][i]
            if 'client_threads_per_process' in config:
                config_new['client_threads_per_process'] = config['client_threads_per_process'][i]
            config_new['client_nodes_per_server'] = n
            config_new['client_processes_per_client_node'] = m
            config_file_new = os.path.join(exp_dir,
                                           '%s-cli-%d-%d.json' % (config_name, n, m))
            with open(config_file_new, 'w+') as f_new:
                json.dump(config_new, f_new, indent=2, sort_keys=True)
            config_files.append(config_file_new)
            exp_futs.append(run_experiment(config_file_new, i, executor))
            exp_futs_idxs.append(i)

        retries = 0
        out_dirs = {}
        while len(out_dirs) < len(config['client_nodes_per_server']) and retries < config['max_retries']:
            retry_exp_futs = []
            for i in range(len(exp_futs)):
                try:
                    out_dir = exp_futs[i].result()
                    out_dirs[exp_futs_idxs[i]] = out_dir
                except:
                    print('Unexpected error during %s %d: ' %
                          (config_files[exp_futs_idxs[i]], exp_futs_idxs[i]))
                    print(traceback.format_exc())
                    retry_exp_futs.append(exp_futs_idxs[i])
            if len(out_dirs) == len(config['client_nodes_per_server']):
                break
            exp_futs = []
            exp_futs_idxs = []
            for j in retry_exp_futs:
                exp_futs.append(run_experiment(config_files[j], j, executor))
                exp_futs_idxs.append(j)
            retries += 1

        generate_tput_lat_plots(config, exp_dir, out_dirs)
        print("%s took %f seconds!" % (config_name, time.time() - start))
    return exp_dir, out_dirs


def run_multiple_protocols_experiment(config_file, executor=None):
    start = time.time()
    exp_dir = None
    with open(config_file) as f:
        existing_exec = False
        if executor == None:
            executor = concurrent.futures.ThreadPoolExecutor(max_workers=16)
        else:
            existing_exec = True
        config = json.load(f)
        if not existing_exec == None:
            kill_clients_no_config(config, len(config['server_names']), max(
                config['client_nodes_per_server']), executor)
        config_name = os.path.splitext(os.path.basename(config_file))[0]
        out_directories = []
        sub_out_directories = []
        exp_dir = get_timestamped_exp_dir(config)
        os.makedirs(exp_dir, exist_ok=True)
        for i in range(len(config['replication_protocol'])):
            config_new = config.copy()
            config_new['base_local_exp_directory'] = exp_dir
            server_replication_protocol = config['replication_protocol'][i]
            config_new['replication_protocol'] = server_replication_protocol
            config_new['plot_cdf_series_title'] = config['plot_cdf_series_title'][i]
            config_new['plot_tput_lat_series_title'] = config['plot_tput_lat_series_title'][i]
            config_new['replication_protocol_settings'] = config['replication_protocol_settings'][i]
            config_file_new = os.path.join(exp_dir,
                                           '%s-%s-%d.json' % (config_name, server_replication_protocol.replace('_', '-'), i))
            with open(config_file_new, 'w+') as f_new:
                json.dump(config_new, f_new, indent=2, sort_keys=True)
            protocol_out_directory, protocol_sub_out_dirs = run_varying_clients_experiment(
                config_file_new, executor)
            out_directories.append(protocol_out_directory)
            sub_out_directories.append(protocol_sub_out_dirs)
        generate_agg_cdf_plots(config, exp_dir, sub_out_directories)
        generate_agg_tput_lat_plots(config, exp_dir, out_directories)
        print("%s took %f seconds!" % (config_name, time.time() - start))
        if not existing_exec:
            executor.shutdown()
        return exp_dir, out_directories, sub_out_directories


def run_multiple_tail_at_scale(config_file):
    start = time.time()
    exp_dir = None
    with open(config_file) as f:
        with concurrent.futures.ThreadPoolExecutor(max_workers=16) as executor:
            config = json.load(f)
            if True:
                kill_clients_no_config(config, len(config['server_names']), max(
                    config['client_nodes_per_server']), executor)
                config_name = os.path.splitext(
                    os.path.basename(config_file))[0]
                directories = []
                out_directories = []
                sub_out_directories = []
                exp_dir = get_timestamped_exp_dir(config)
                os.makedirs(exp_dir, exist_ok=True)
                for i in range(len(config['client_tail_at_scale'])):
                    config_new = config.copy()
                    config_new['base_local_exp_directory'] = exp_dir
                    config_new['client_tail_at_scale'] = config['client_tail_at_scale'][i]
                    config_file_new = os.path.join(
                        exp_dir, '%s-%d.json' % (config_name, i))
                    with open(config_file_new, 'w+') as f_new:
                        json.dump(config_new, f_new, indent=2, sort_keys=True)
                    directory, protocol_out_dirs, protocol_sub_out_dirs = run_multiple_protocols_experiment(
                        config_file_new, executor)
                    directories.append(directory)
                    out_directories.append(protocol_out_dirs)
                    sub_out_directories.append(protocol_sub_out_dirs)
                print("%s took %f seconds!" %
                      (config_name, time.time() - start))
            generate_tail_at_scale_plots(config, exp_dir, sub_out_directories)
            #generate_tail_at_scale_plots(config, 'experiments/emulab/2018-09-18-02-46-26', [[['experiments/emulab/2018-09-18-02-46-26/2018-09-18-02-46-31/2018-09-18-02-46-31/2018-09-18-02-47-03/out/']], [['experiments/emulab/2018-09-18-02-46-26/2018-09-18-02-48-14/2018-09-18-02-48-14/2018-09-18-02-48-32/out/']]])
