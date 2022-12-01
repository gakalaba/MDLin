import os
import json
import random
import numpy
import csv
import traceback
import subprocess
import concurrent
import collections

def convert_latency_nanos_to_millis(latencies):
    return list(map(lambda x: x / 1e6, latencies))

def get_num_regions(config):
    return len(config['server_names']) if not 'server_regions' in config else len(config['server_regions'])

def calculate_statistics(config, local_out_directory):
    runs = []
    op_latencies = {}
    op_latency_counts = {}
    for i in range(config['num_experiment_runs']):
        stats, run_op_latencies, run_op_latency_counts = calculate_statistics_for_run(config, local_out_directory, i)
        runs.append(stats)

        for k, v in run_op_latencies.items():
            for j in range(len(v)):
                if k in op_latencies:
                    if len(op_latencies[k]) <= j:
                        op_latencies[k].append(v[j])
                    else:
                        op_latencies[k][j] += v[j]
                else:
                    op_latencies[k] = [v[j]]
            if k in op_latency_counts:
                op_latency_counts[k] += run_op_latency_counts[k]
            else:
                op_latency_counts[k] = run_op_latency_counts[k]

    stats = {}
    stats['aggregate'] = {}
    calculate_all_op_statistics(config, stats['aggregate'], op_latencies, op_latency_counts)
    stats['runs'] = runs
    stats['run_stats'] = {}
    ignored = {'cdf': 1, 'cdf_log': 1, 'time': 1}
    for cat in runs[0]: # we assume there is at least one run
        if not ('region-' in cat) and type(runs[0][cat]) is dict:
            stats['run_stats'][cat] = {}
            for s in runs[0][cat]:
                if s in ignored:
                    continue
                data = []
                for run in runs:
                    data.append(run[cat][s])
                stats['run_stats'][cat][s] = calculate_statistics_for_data(data, cdf=False)
        if 'region-' in cat:
            stats['run_stats'][cat] = {}
            for cat2 in runs[0][cat]: # we assume there is at least one run
                if type(runs[0][cat][cat2]) is dict:
                    stats['run_stats'][cat][cat2] = {}
                    for s in runs[0][cat][cat2]:
                        if s in ignored:
                            continue
                        data = []
                        for run in runs:
                            data.append(run[cat][cat2][s])
                        stats['run_stats'][cat][cat2][s] = calculate_statistics_for_data(data, cdf=False)

    stats_file = STATS_FILE if 'stats_file_name' not in config else config['stats_file_name']
    with open(os.path.join(local_out_directory, stats_file), 'w') as f:
        json.dump(stats, f, indent=2, sort_keys=True)
    return stats, op_latencies

def calculate_statistics_for_run(config, local_out_directory, run):
    region_op_latencies = {}
    region_op_latency_counts = {}

    total = 0
    stats = {}
    num_regions = get_num_regions(config)
    for i in range(num_regions):
        op_latencies = {}
        op_latency_counts = {}
        for l in range(len(config['server_names']) // num_regions):
            server_idx = l + i * len(config['server_names']) // num_regions
            if not 'client_total' in config or total < config['client_total']:
                for j in range(config['client_nodes_per_server']):
                    client_dir = 'client-%d-%d' % (server_idx, j)
                    for k in range(config['client_processes_per_client_node']):
                        client_out_file = os.path.join(local_out_directory,
                                client_dir,
                                'client-%d-%d-%d-stdout-%d.log' % (server_idx,
                                    j, k, run))
                        with open(client_out_file) as f:
                            ops = f.readlines()
                            for op in ops:
                                opCols = op.strip().split(',')
                                for x in range(0, len(opCols), 2):
                                    if opCols[x].isdigit():
                                        break
                                    if not opCols[x] in config['client_stats_blacklist']:
                                        if 'input_latency_scale' in config:
                                            opLat = float(opCols[x+1]) / config['input_latency_scale']
                                        else:
                                            opLat = float(opCols[x+1]) / 1e9
                                        if 'output_latency_scale' in config:
                                            opLat = opLat * config['output_latency_scale']
                                        else:
                                            opLat = opLat * 1e3

                                        if opCols[x] in op_latencies:
                                            op_latencies[opCols[x]].append(opLat)
                                        else:
                                            op_latencies[opCols[x]] = [opLat]
                                        if not opCols[x] in config['client_combine_stats_blacklist']:
                                            if 'combined' in op_latencies:
                                                op_latencies['combined'].append(opLat)
                                            else:
                                                op_latencies['combined'] = [opLat]
                                            if 'combined' in op_latency_counts:
                                                op_latency_counts['combined'] += 1
                                            else:
                                                op_latency_counts['combined'] = 1
                                        if opCols[x] in op_latency_counts:
                                            op_latency_counts[opCols[x]] += 1
                                        else:
                                            op_latency_counts[opCols[x]] = 1

                        client_stats_file = os.path.join(local_out_directory,
                                client_dir,
                                'client-%d-%d-%d-stats-%d.json' % (server_idx, j,
                                    k, run))
                        try:
                            with open(client_stats_file) as f:
                                client_stats = json.load(f)
                                for k, v in client_stats.items():
                                    if not type(v) is dict:
                                        if k not in stats:
                                            stats[k] = v
                                        else:
                                            stats[k] += v
                        except FileNotFoundError:
                            print('No stats file %s.' % client_stats_file)
                        except json.decoder.JSONDecodeError:
                            print('Invalid JSON file %s.' % client_stats_file)
                        total += 1
                        if 'client_total' in config and total >= config['client_total']:
                            break
                    if 'client_total' in config and total >= config['client_total']:
                        break

            server_stats_file = os.path.join(local_out_directory, 'server-%d' % server_idx,
                    'server-%d-stats-%d.json' % (server_idx, run))
            try:
                with open(server_stats_file) as f:
                    print(server_stats_file)
                    server_stats = json.load(f)
                    for k, v in server_stats.items():
                        if not type(v) is dict:
                            if k not in stats:
                                stats[k] = v
                            else:
                                stats[k] += v
            except FileNotFoundError:
                print('No stats file %s.' % server_stats_file)
            except json.decoder.JSONDecodeError:
                print('Invalid JSON file %s.' % server_stats_file)

        # print('Region %d had op counts: w=%d, r=%d, rmw=%d.' % (i, writes, reads, rmws))
        # normalize by server region to account for latency differences
        for k, v in op_latencies.items():
            if k in region_op_latencies:
                region_op_latencies[k].append(v)
            else:
                region_op_latencies[k] = [v]
        for k, v in op_latency_counts.items():
            if k in region_op_latency_counts:
                region_op_latency_counts[k] = min(region_op_latency_counts[k], v)
            else:
                region_op_latency_counts[k] = v

    # TODO: remove this hack
    if 'fast_writes_0' in stats or 'slow_writes_0' in stats or 'fast_reads_0' in stats or 'slow_reads_0' in stats:
        fw0 = stats['fast_writes_0'] if 'fast_writes_0' in stats else 0
        sw0 = stats['slow_writes_0'] if 'slow_writes_0' in stats else 0
        if fw0 + sw0 > 0:
            stats['fast_write_ratio'] = fw0 / (fw0 + sw0)
            stats['slow_write_ratio'] = sw0 / (fw0 + sw0)
        fr0 = stats['fast_reads_0'] if 'fast_reads_0' in stats else 0
        sr0 = stats['slow_reads_0'] if 'slow_reads_0' in stats else 0
        if fr0 + sr0 > 0:
            stats['fast_read_ratio'] = fr0 / (fr0 + sr0)
            stats['slow_read_ratio'] = sr0 / (fr0 + sr0)

    total_committed = 0
    total_attempts = 0
    if 'new_order_committed' in stats:
        total_committed += stats['new_order_committed']
        total_attempts += stats['new_order_attempts']
        stats['new_order_commit_rate'] = stats['new_order_committed'] / stats['new_order_attempts']
        stats['new_order_abort_rate'] = 1 - stats['new_order_commit_rate'] 
    if 'payment_committed' in stats:
        total_committed += stats['payment_committed']
        total_attempts += stats['payment_attempts']
        stats['payment_commit_rate'] = stats['payment_committed'] / stats['payment_attempts']
        stats['payment_abort_rate'] = 1 - stats['payment_commit_rate'] 
    if 'order_status_committed' in stats:
        total_committed += stats['order_status_committed']
        total_attempts += stats['order_status_attempts']
        stats['order_status_commit_rate'] = stats['order_status_committed'] / stats['order_status_attempts']
        stats['order_status_abort_rate'] = 1 - stats['order_status_commit_rate'] 
    if 'stock_level_committed' in stats:
        total_committed += stats['stock_level_committed']
        total_attempts += stats['stock_level_attempts']
        stats['stock_level_commit_rate'] = stats['stock_level_committed'] / stats['stock_level_attempts']
        stats['stock_level_abort_rate'] = 1 - stats['stock_level_commit_rate'] 
    if 'delivery_committed' in stats:
        total_committed += stats['delivery_committed']
        total_attempts += stats['delivery_attempts']
        stats['delivery_commit_rate'] = stats['delivery_committed'] / stats['delivery_attempts']
        stats['delivery_abort_rate'] = 1 - stats['delivery_commit_rate'] 
    if total_attempts > 0:
        stats['committed'] = total_committed
        stats['attempts'] = total_attempts
        stats['commit_rate'] = total_committed / total_attempts
        stats['abort_rate'] = 1 - stats['commit_rate']

    calculate_all_op_statistics(config, stats, region_op_latencies, region_op_latency_counts)
    return stats, region_op_latencies, region_op_latency_counts 


def calculate_op_statistics(config, stats, total_recorded_time, op_type, latencies, norm_latencies):
    if len(latencies) > 0:
        stats[op_type] = calculate_statistics_for_data(latencies)
        stats[op_type]['ops'] = len(latencies)
        stats[op_type]['tput'] = len(latencies) / total_recorded_time
        if op_type == 'combined':
            stats['combined']['ops'] = len(latencies)
            stats['combined']['time'] = total_recorded_time
        if (not 'server_emulate_wan' in config or config['server_emulate_wan']) and len(norm_latencies) > 0:
            stats['%s_norm' % op_type] = calculate_statistics_for_data(norm_latencies)
            stats['%s_norm' % op_type]['samples'] = len(norm_latencies)

def calculate_all_op_statistics(config, stats, region_op_latencies, region_op_latency_counts):
    total_recorded_time = float(config['client_experiment_length'] - config['client_ramp_up'] - config['client_ramp_down'])

    for k, v in region_op_latencies.items():
        latencies = [lat for region_lats in v for lat in region_lats]
        
        norm_latencies = []
        for region_lats in v:
            norm_latencies += random.sample(region_lats, region_op_latency_counts[k])

        if not 'server_emulate_wan' in config or config['server_emulate_wan']:
            for i in range(len(v)):
                region_key = 'region-%d' % i
                if region_key not in stats:
                    stats[region_key] = {}
                calculate_op_statistics(config, stats[region_key], total_recorded_time, k, v[i], [])
        calculate_op_statistics(config, stats, total_recorded_time, k, latencies, norm_latencies)


def calculate_cdf_for_npdata(npdata):
    ptiles = []
    for i in range(1, 100): # compute percentiles [1, 100)
        ptiles.append([i, numpy.percentile(npdata, i, interpolation='higher')])
    return ptiles

def calculate_cdf_log_for_npdata(npdata, precision):
    ptiles = []
    base = 0
    scale = 1
    for i in range(0, precision):
        for j in range(0, 90):
            if i == 0 and j == 0:
                continue
            ptiles.append([base + j / scale, numpy.percentile(npdata, base + j / scale, interpolation='higher')])
        base += 90 / scale
        scale = scale * 10
    return ptiles

def calculate_statistics_for_data(data, cdf=True, cdf_log_precision=4):
    npdata = numpy.asarray(data)
    s = {
        'p50': numpy.percentile(npdata, 50).item(),
        'p75': numpy.percentile(npdata, 75).item(),
        'p90': numpy.percentile(npdata, 90).item(),
        'p95': numpy.percentile(npdata, 95).item(),
        'p99': numpy.percentile(npdata, 99).item(),
        'p99.9': numpy.percentile(npdata, 99.9).item(),
        'max': numpy.amax(npdata).item(),
        'min': numpy.amin(npdata).item(),
        'mean': numpy.mean(npdata).item(),
        'stddev': numpy.std(npdata).item(),
        'var': numpy.var(npdata).item(),
    }
    if cdf:
        s['cdf'] = calculate_cdf_for_npdata(npdata)
        s['cdf_log'] = calculate_cdf_log_for_npdata(npdata, cdf_log_precision)
    return s

def generate_gnuplot_script_cdf_log_agg_new(script_file, out_file, x_label,
        y_label, width, height, font, series, title):
    with open(script_file, 'w') as f:
        write_gpi_header(f)
        f.write("set title \"%s\"\n" % title)
        f.write("set key bottom right\n")
        f.write("set ytics (0,0.9,0.99,0.999,0.9999,1.0)\n")
        f.write("set xlabel '%s'\n" % x_label)
        f.write("set ylabel '%s'\n" % y_label)
        f.write("set terminal pngcairo size %d,%d enhanced dashed font '%s'\n" % (
            width, height, font))
        f.write('set output \'%s\'\n' % out_file)
        write_line_styles(f)
        f.write('plot ')
        for i in range(len(series)):
            if i == 0:
                labels = ':yticlabels(3)'
            else:
                labels = ''
            f.write("'%s' using 1:(-log10(1-$2))%s title \"%s\" ls %d with lines" % (
                series[i][1], labels, series[i][0].replace('_', '\\\\\\_'), i + 1))
            if i != len(series) - 1:
                f.write(', \\\n')

def generate_gnuplot_script_lot_plot_stacked(script_file, out_file, x_label, y_label, width, height, font, series, title):
    with open(script_file, 'w') as f:
        write_gpi_header(f)
        f.write("set title \"%s\"\n" % title)
        f.write("set key top left\n")
        f.write("set xlabel '%s'\n" % x_label)
        f.write("set ylabel '%s'\n" % y_label)
        f.write("set terminal pngcairo size %d,%d enhanced dashed font '%s'\n" %
            (width, height, font))
        f.write('set output \'%s\'\n' % out_file)
        write_line_styles(f)
        f.write('plot ')
        for i in range(len(series)):
            f.write("'%s' title \"%s\" ls %d with filledcurves x1" % (series[i][1],
                series[i][0].replace('_', '\\\\\\_'), i + 1))
            if i != len(series) - 1:
                f.write(', \\\n')

def generate_gnuplot_script_cdf_agg_new(script_file, out_file, x_label, y_label, width, height, font, series, title):
    with open(script_file, 'w') as f:
        write_gpi_header(f)
        f.write("set title \"%s\"\n" % title)
        f.write("set key bottom right\n")
        f.write("set xlabel '%s'\n" % x_label)
        f.write("set ylabel '%s'\n" % y_label)
        f.write("set terminal pngcairo size %d,%d enhanced dashed font '%s'\n" %
            (width, height, font))
        f.write('set output \'%s\'\n' % out_file)
        write_line_styles(f)
        f.write('plot ')
        for i in range(len(series)):
            f.write("'%s' title \"%s\" ls %d with lines" % (series[i][1],
                series[i][0].replace('_', '\\\\\\_'), i + 1))
            if i != len(series) - 1:
                f.write(', \\\n')

def generate_csv_for_plot(plot_csv_file, x_vars, y_vars):
    with open(plot_csv_file, 'w') as f:
        csvwriter = csv.writer(f)
        for i in range(len(x_vars)):
            csvwriter.writerow([x_vars[i], y_vars[i]])

def generate_gnuplot_script(plot, plot_script_file, plot_csv_file, plot_out_file):
    with open(plot_script_file, 'w') as f:
        write_gpi_header(f)
        f.write("set key top left\n")
        f.write("set xlabel '%s'\n" % plot['x_label'])
        f.write("set ylabel '%s'\n" % plot['y_label'])
        f.write("set terminal pngcairo size %d,%d enhanced font '%s'\n" %
            (plot['width'], plot['height'], plot['font']))
        f.write('set output \'%s\'\n' % plot_out_file)
        write_line_styles(f)
        f.write("plot '%s' title '%s' with linespoint\n" % (plot_csv_file, 'series-1'))

def generate_plot(plot, plots_directory, x_vars, y_vars):
    plot_csv_file = os.path.join(plots_directory, '%s.csv' % plot['name'])
    generate_csv_for_plot(plot_csv_file, x_vars, y_vars)
    plot_script_file = os.path.join(plots_directory, '%s.gpi' % plot['name'])
    plot_out_file = os.path.join(plots_directory, '%s.png' % plot['name'])
    generate_gnuplot_script(plot, plot_script_file, plot_csv_file, plot_out_file)
    subprocess.call(['gnuplot', plot_script_file])

def generate_gnuplot_script_agg(plot, plot_script_file, plot_out_file, series):
    with open(plot_script_file, 'w') as f:
        write_gpi_header(f)
        f.write("set key top left\n")
        f.write("set xlabel '%s'\n" % plot['x_label'])
        f.write("set ylabel '%s'\n" % plot['y_label'])
        f.write("set terminal pngcairo size %d,%d enhanced dashed font '%s'\n" %
            (plot['width'], plot['height'], plot['font']))
        f.write('set output \'%s\'\n' % plot_out_file)
        write_line_styles(f)
        f.write('plot ')
        for i in range(len(series)):
            f.write("'%s' title '%s' ls %d with linespoint" % (series[i], plot['series_titles'][i].replace('_', '\\_'), i + 1))
            if i != len(series) - 1:
                f.write(', \\\n')

def generate_plots(config, base_out_directory, out_dirs):
    plots_directory = os.path.join(base_out_directory, config['plot_directory_name'])
    os.makedirs(plots_directory, exist_ok=True)
    csv_classes = set()
    csv_files = []
    subprocesses = []
    
    ###
    # Generate aggregate cdf plots
    for i in range(len(out_dirs[0])):
        # for each series i
        csv_files.append({})
        collecting = collections.deque([out_dirs[0][i]])
        while len(collecting) != 0:
            # bfs flattening of independent vars
            if type(collecting[0]) is str:
                sub_plot_directory = os.path.join(collecting[0], config['plot_directory_name'])
                for f in os.listdir(sub_plot_directory):
                    if f.endswith('.csv') and (f.startswith('aggregate-') or f.startswith('lot-')):
                        csv_class = os.path.splitext(os.path.basename(f))[0]
                        csv_classes.add(csv_class)
                        if not csv_class in csv_files[i]:
                            csv_files[i][csv_class] = []
                        csv_files[i][csv_class].append(os.path.join(sub_plot_directory, f))
            else:
                for od in collecting[0]:
                    collecting.append(od)
            collecting.popleft()

    for csv_class in csv_classes:
        idx = -1
        for j in range(len(csv_files)):
            if csv_class in csv_files[j]:
                idx = j
                break
        if idx == -1:
            continue
        for j in range(len(csv_files[idx][csv_class])):
            title = ''
            for k in range(len(config['experiment_independent_vars']) - 1 - len(config['experiment_independent_vars_unused']), -1, -1):
                title += '%s=%s' % (config['experiment_independent_vars'][k][0].replace('_', '\\\\\\_'),
                        str(config[config['experiment_independent_vars'][k][0]]))
                if k > 0:
                    title += '\\n' 
            x = j
            for k in range(len(config['experiment_independent_vars_unused']) - 1, 0, -1):
                if k == len(config['experiment_independent_vars_unused']) - 1:
                    title += '\\n'
                n = len(config[config['experiment_independent_vars_unused'][k][0]])
                title += '%s=%s' % (config['experiment_independent_vars_unused'][k][0].replace('_', '\\\\\\_'),
                        str(config[config['experiment_independent_vars_unused'][k][0]][x % n]))
                x = x // n
                if k > 0:
                    title += '\\n' 
            plot_script_file = os.path.join(plots_directory, '%s-%d.gpi' % (csv_class, j))
            plot_out_file = os.path.join(plots_directory, '%s-%d.png' % (csv_class, j))
            series = []
            for i in range(len(csv_files)):
                if csv_class in csv_files[i] and len(csv_files[i][csv_class]) > j:
                    series.append(('%s=%s' % (config['experiment_independent_vars_unused'][0][0], 
                        config[config['experiment_independent_vars_unused'][0][0]][i]), csv_files[i][csv_class][j]))
            if 'lot-' in csv_class:
                if not 'lot_plots' in config:
                    config['lot_plots'] = {
                            'x_label': 'Time',
                            'y_label': 'Latency',
                            'width': config['cdf_plots']['width'],
                            'height': config['cdf_plots']['height'],
                            'font': config['cdf_plots']['font']
                        }
                generate_gnuplot_script_cdf_agg_new(plot_script_file,
                        plot_out_file, config['lot_plots']['x_label'],
                        config['lot_plots']['y_label'],
                        config['lot_plots']['width'],
                        config['lot_plots']['height'],
                        config['lot_plots']['font'], series, title)
            elif 'log' in csv_class:
                generate_gnuplot_script_cdf_log_agg_new(plot_script_file,
                        plot_out_file, config['cdf_plots']['x_label'],
                        config['cdf_plots']['y_label'],
                        config['cdf_plots']['width'],
                        config['cdf_plots']['height'],
                        config['cdf_plots']['font'], series, title)
            else:
                generate_gnuplot_script_cdf_agg_new(plot_script_file,
                        plot_out_file, config['cdf_plots']['x_label'],
                        config['cdf_plots']['y_label'],
                        config['cdf_plots']['width'],
                        config['cdf_plots']['height'],
                        config['cdf_plots']['font'], series, title)
            print(plot_script_file)
            #subprocesses.append(subprocess.Popen(['gnuplot', plot_script_file]))
            subprocess.call(['gnuplot', plot_script_file])
    # End generate all aggregate cdf plots
    ###

    ###
    # Generate specific plots
    #   for now we only support configurable plot generation with 1 indep var
    for plot in config['plots']:
        if len(config['experiment_independent_vars']) - len(config['experiment_independent_vars_unused']) == 1:
            # generate csvs and single series plots
            x_vars = []
            y_vars = []
            for i in range(len(out_dirs[0])):
                assert type(out_dirs[0][i]) is str
                # for each value of the independent variable
                stats_file = os.path.join(out_dirs[0][i], config['stats_file_name'])
                print(stats_file)
                with open(stats_file) as f:
                    stats = json.load(f)
                    if plot['x_var_is_config']:
                        x_var = config
                        for k in plot['x_var']:
                            x_var = x_var[k]
                        x_var = x_var[i]
                    else:
                        x_var = stats
                        for k in plot['x_var']:
                            if k in x_var:
                                x_var = x_var[k]
                            else:
                                x_var = 0
                                break
                    x_vars.append(x_var)

                    y_var = stats
                    for k in plot['y_var']:
                        if k in y_var or (isinstance(y_var, list) and isinstance(k, int) and k < len(y_var)):
                            y_var = y_var[k]
                        else:
                            y_var = 0
                            break
                    y_vars.append(y_var)
            print(plots_directory)
            generate_plot(plot, plots_directory, x_vars, y_vars)
        elif len(config['experiment_independent_vars']) == len(config['experiment_independent_vars_unused']):
            csv_files = []
            for i in range(len(out_dirs[-1])):
                # for series i
                sub_plot_directory = os.path.join(out_dirs[-1][i], config['plot_directory_name'])
                csv_files.append(os.path.join(sub_plot_directory, '%s.csv' % plot['name']))
            
            plot_script_file = os.path.join(plots_directory, '%s.gpi' % plot['name'])
            plot_out_file = os.path.join(plots_directory, '%s.png' % plot['name'])
            generate_gnuplot_script_agg(plot, plot_script_file, plot_out_file, csv_files)
            subprocess.call(['gnuplot', plot_script_file])
            #subprocesses.append(subprocess.Popen(['gnuplot', plot_script_file]))

    # End generate specific plots
    ###
    #for subprocess in subprocesses:
    #    subprocess.wait()

def run_gnuplot(data_files, out_file, script_file):
    print(script_file)
    args = ['gnuplot', '-e', "outfile='%s'" % out_file]
    for i in range(len(data_files)):
        args += ['-e', "datafile%d='%s'" % (i, data_files[i])]
    args.append(script_file)
    subprocess.call(args)

def generate_csv_for_cdf_plot(csv_file, cdf_data, log=False):
    with open(csv_file, 'w') as f:
        csvwriter = csv.writer(f)
        k = 1
        for i in range(len(cdf_data)):
            data = [cdf_data[i][1], cdf_data[i][0] / 100]
            if log and abs(cdf_data[i][0] / 100 - (1 - 10**-k)) < 0.000001:
                data.append(1 - 10**-k)
                k += 1
            csvwriter.writerow(data)

def generate_csv_for_lot_plot(csv_file, lot_data, use_idxs=False):
    with open(csv_file, 'w') as f:
        csvwriter = csv.writer(f)
        agg = 0.0
        for i in range(len(lot_data)):
            agg += lot_data[i]
            if use_idxs:
                data = [i, lot_data[i]]
            else:
                data = [agg, lot_data[i]]
            csvwriter.writerow(data)

def generate_cdf_plot(config, plots_directory, plot_name, cdf_data):
    plot_name = plot_name.replace('_', '-')
    plot_csv_file = os.path.join(plots_directory, '%s.csv' % plot_name)
    generate_csv_for_cdf_plot(plot_csv_file, cdf_data)
    plot_script_file = os.path.join(plots_directory, '%s.gpi' % plot_name)
    generate_gnuplot_script_cdf(config, plot_script_file)
    run_gnuplot([plot_csv_file], os.path.join(plots_directory, '%s.png' % plot_name),
        plot_script_file)

def generate_gnuplot_script_lot(config, script_file):
    with open(script_file, 'w') as f:
        f.write("set datafile separator ','\n")
        f.write("set key bottom right\n")
        if 'plot_lot_x_label' in config:
            f.write("set xlabel '%s'\n" % config['plot_lot_x_label'])
        if 'plot_lot_y_label' in config:
            f.write("set ylabel '%s'\n" % config['plot_lot_y_label'])
        f.write("set terminal pngcairo size %d,%d enhanced font '%s'\n" %
            (config['plot_cdf_png_width'], config['plot_cdf_png_height'],
                config['plot_cdf_png_font']))
        f.write('set output outfile\n')
        f.write("plot datafile0 title '%s' with lines\n" % config['plot_cdf_series_title'].replace('_', '\\_'))

def generate_lot_plot(config, plots_directory, plot_name, lot_data):
    plot_name = plot_name.replace('_', '-')
    plot_csv_file = os.path.join(plots_directory, '%s.csv' % plot_name)
    generate_csv_for_lot_plot(plot_csv_file, lot_data)
    plot_script_file = os.path.join(plots_directory, '%s.gpi' % plot_name)
    generate_gnuplot_script_lot(config, plot_script_file)
    run_gnuplot([plot_csv_file], os.path.join(plots_directory, '%s.png' % plot_name),
        plot_script_file)

def generate_cdf_log_plot(config, plots_directory, plot_name, cdf_data):
    plot_name = plot_name.replace('_', '-')
    plot_csv_file = os.path.join(plots_directory, '%s.csv' % plot_name)
    generate_csv_for_cdf_plot(plot_csv_file, cdf_data, log=True)
    plot_script_file = os.path.join(plots_directory, '%s.gpi' % plot_name)
    generate_gnuplot_script_cdf_log(config, plot_script_file)
    run_gnuplot([plot_csv_file], os.path.join(plots_directory, '%s.png' % plot_name),
        plot_script_file)

CDF_PLOTS = ['txn', 'w', 'r', 'rmw', 'max', 'maxr', 'maxw', 'maxrmw', 'combined', 'txn_norm', 'w_norm', 'r_norm', 'rmw_norm', 'combined_norm', 'max_norm', 'maxr_norm', 'maxw_norm', 'maxrmw_norm']
def generate_cdf_plots(config, local_out_directory, stats, executor):
    futures = []
    plots_directory = os.path.join(local_out_directory, config['plot_directory_name'])
    os.makedirs(plots_directory, exist_ok=True)
    for op_type in stats['aggregate']:
        if not op_type in config['client_cdf_plot_blacklist'] and not 'region-' in op_type:
            cdf_plot_name = 'aggregate-%s' % op_type
            futures.append(executor.submit(generate_cdf_plot, config, plots_directory, cdf_plot_name,
                stats['aggregate'][op_type]['cdf']))
            cdf_log_plot_name = 'aggregate-%s-log' % op_type
            futures.append(executor.submit(generate_cdf_log_plot, config, plots_directory, cdf_log_plot_name,
                stats['aggregate'][op_type]['cdf_log']))
        elif 'region-' in op_type:
            for op_type2 in stats['aggregate'][op_type]:
                if not op_type2 in config['client_cdf_plot_blacklist']:
                    cdf_plot_name = 'aggregate-%s-%s' % (op_type, op_type2)
                    futures.append(executor.submit(generate_cdf_plot, config, plots_directory, cdf_plot_name,
                        stats['aggregate'][op_type][op_type2]['cdf']))
                    cdf_log_plot_name = 'aggregate-%s-%s-log' % (op_type, op_type2)
                    futures.append(executor.submit(generate_cdf_log_plot, config, plots_directory, cdf_log_plot_name,
                        stats['aggregate'][op_type][op_type2]['cdf_log']))


    for i in range(len(stats['runs'])):
        for op_type in stats['runs'][i]:
            if not op_type in config['client_cdf_plot_blacklist'] and not 'region-' in op_type:
                if type(stats['runs'][i][op_type]) is dict:
                    cdf_plot_name = 'run-%d-%s' % (i, op_type)
                    futures.append(executor.submit(generate_cdf_plot, config, plots_directory, cdf_plot_name,
                        stats['runs'][i][op_type]['cdf']))
                    cdf_log_plot_name = 'run-%d-%s-log' % (i, op_type)
                    futures.append(executor.submit(generate_cdf_log_plot, config, plots_directory, cdf_log_plot_name,
                        stats['runs'][i][op_type]['cdf']))
            elif 'region-' in op_type:
                for op_type2 in stats['runs'][i][op_type]:
                    if not op_type2 in config['client_cdf_plot_blacklist']:
                        if type(stats['runs'][i][op_type]) is dict:
                            cdf_plot_name = 'run-%d-%s-%s' % (i, op_type, op_type2)
                            futures.append(executor.submit(generate_cdf_plot, config, plots_directory, cdf_plot_name,
                                stats['runs'][i][op_type][op_type2]['cdf']))
                            cdf_log_plot_name = 'run-%d-%s-%s-log' % (i, op_type, op_type2)
                            futures.append(executor.submit(generate_cdf_log_plot, config, plots_directory, cdf_log_plot_name,
                                stats['runs'][i][op_type][op_type2]['cdf']))
    concurrent.futures.wait(futures)

def generate_lot_plots(config, local_out_directory, stats, op_latencies, executor):
    futures = []
    plots_directory = os.path.join(local_out_directory, config['plot_directory_name'])
    os.makedirs(plots_directory, exist_ok=True)

    for i in range(len(stats['runs'])):
        ops = []
        for op_type in stats['runs'][i]:
            if config['client_total'] == 1 and op_type.startswith('op'):
                ops.append(op_type)
            if not op_type in config['client_cdf_plot_blacklist'] and not 'region-' in op_type:
                if type(stats['runs'][i][op_type]) is dict:
                    lot_plot_name = 'lot-run-%d-%s' % (i, op_type)
                    #futures.append(executor.submit(generate_lot_plot, config, plots_directory, lot_plot_name,
                    #    op_latencies[op_type][i]))
                    generate_lot_plot(config, plots_directory, lot_plot_name, op_latencies[op_type][i])
            elif 'region-' in op_type:
                for op_type2 in stats['runs'][i][op_type]:
                    if not op_type2 in config['client_cdf_plot_blacklist']:
                        if type(stats['runs'][i][op_type]) is dict:
                            lot_plot_name = 'lot-run-%d-%s-%s' % (i, op_type, op_type2)
                            futures.append(executor.submit(generate_lot_plot, config, plots_directory, lot_plot_name,
                                op_latencies[op_type2][i]))
        if len(ops) > 0:
            ops.sort()
            series = [convert_latency_nanos_to_millis(stats['runs'][i][ops[0]])]
            series_csvs = []
            plot_csv_file = os.path.join(plots_directory, '%s.csv' % ops[0])
            series_csvs.append((ops[0], plot_csv_file))
            generate_csv_for_lot_plot(plot_csv_file, series[0], True)
            for k in range(1, len(ops)):
                series.append(convert_latency_nanos_to_millis(stats['runs'][i][ops[k]]))
                for j in range(len(series[-2])):
                    series[-1][j] += series[-2][j]
                plot_csv_file = os.path.join(plots_directory, '%s.csv' % ops[k])
                series_csvs.append((ops[k], plot_csv_file))
                generate_csv_for_lot_plot(plot_csv_file, series[-1], True)
            
            series.append(convert_latency_nanos_to_millis(stats['runs'][i]['commit']))
            for j in range(len(series[-2])):
                series[-1][j] += series[-2][j]

            plot_csv_file = os.path.join(plots_directory, 'commit.csv')
            series_csvs.append(('commit', plot_csv_file))
            generate_csv_for_lot_plot(plot_csv_file, series[-1], True)

            plot_name = 'breakdown'
            plot_script_file = os.path.join(plots_directory, '%s.gpi' % plot_name)
            plot_out_file = os.path.join(plots_directory, '%s.png' % plot_name)
            series_csvs.reverse()
            generate_gnuplot_script_lot_plot_stacked(plot_script_file, plot_out_file,
                'Transaction #', config['lot_plots']['y_label'],
                1600, 600,
                config['lot_plots']['font'], series_csvs, 'Breakdown')
            subprocess.call(['gnuplot', plot_script_file])

    concurrent.futures.wait(futures)


def generate_csv_for_tput_lat_plot(plot_csv_file, tputs, lats):
    with open(plot_csv_file, 'w') as f:
        csvwriter = csv.writer(f)
        for i in range(len(tputs)):
            csvwriter.writerow([tputs[i], lats[i]])

def generate_tput_lat_plot(config, plots_directory, plot_name, tputs, lats):
    plot_csv_file = os.path.join(plots_directory, '%s.csv' % plot_name)
    generate_csv_for_tput_lat_plot(plot_csv_file, tputs, lats)
    plot_script_file = os.path.join(plots_directory, '%s.gpi' % plot_name)
    generate_gnuplot_script_tput_lat(config, plot_script_file)
    run_gnuplot([plot_csv_file], os.path.join(plots_directory,
        '%s.png' % plot_name), plot_script_file)

STATS_FILE = 'stats.json'

def generate_tput_lat_plots(config, base_out_directory, exp_out_directories):
    plots_directory = os.path.join(base_out_directory, config['plot_directory_name'])
    os.makedirs(plots_directory, exist_ok=True)
    tputs = []
    lats = {}
    for i in range(len(exp_out_directories)):
        stats_file = os.path.join(exp_out_directories[i], STATS_FILE)
        print(stats_file)
        with open(stats_file) as f:
            stats = json.load(f)
            if 'combined' in stats['run_stats']:
                combined_run_stats = stats['run_stats']['combined']
                tputs.append(combined_run_stats['tput']['p50'])
                ignore = {'stddev': 1, 'var' : 1, 'tput': 1, 'ops': 1}
                for lat_stat, lat in combined_run_stats.items():
                    if lat_stat in ignore:
                        continue
                    if lat_stat not in lats:
                        lats[lat_stat] = []
                    lats[lat_stat].append(lat['p50']) # median of runs
    for lat_stat, lat in lats.items():
        plot_name = 'tput-%s-lat' % lat_stat
        print(plots_directory)
        generate_tput_lat_plot(config, plots_directory, plot_name, tputs, lat)

def generate_agg_cdf_plots(config, base_out_directory, sub_out_directories):
    plots_directory = os.path.join(base_out_directory, config['plot_directory_name'])
    os.makedirs(plots_directory, exist_ok=True)
    csv_files = {}
    for i in range(len(sub_out_directories)):
        # for replication protocol i
        for j in range(len(sub_out_directories[i])):
            # for client configuration j
            sub_plot_directory = os.path.join(sub_out_directories[i][j], config['plot_directory_name'])
            for f in os.listdir(sub_plot_directory):
                if f.endswith('.csv') and f.startswith('aggregate'):
                    csv_class = os.path.splitext(os.path.basename(f))[0]
                    if csv_class not in csv_files:
                        csv_files[csv_class] = []
                    if len(csv_files[csv_class]) == j:
                        csv_files[csv_class].append([])
                    csv_files[csv_class][j].append(os.path.join(sub_plot_directory, f))
    
    for csv_class, file_lists in csv_files.items():
        for j in range(len(file_lists)):
            plot_script_file = os.path.join(plots_directory, '%s-%d.gpi' % (csv_class, j))
            if 'log' in csv_class:
                generate_gnuplot_script_cdf_log_agg(config, plot_script_file)
            else:
                generate_gnuplot_script_cdf_agg(config, plot_script_file)
            run_gnuplot(file_lists[j], os.path.join(plots_directory,
                '%s-%d.png' % (csv_class, j)), plot_script_file)

def generate_agg_tput_lat_plots(config, base_out_directory, out_directories):
    plots_directory = os.path.join(base_out_directory, config['plot_directory_name'])
    os.makedirs(plots_directory, exist_ok=True)
    csv_files = {}
    for i in range(len(out_directories)):
        # for replication protocol i
        sub_plot_directory = os.path.join(out_directories[i], config['plot_directory_name'])
        for f in os.listdir(sub_plot_directory):
            if f.endswith('.csv'):
                csv_class = os.path.splitext(os.path.basename(f))[0]
                if csv_class not in csv_files:
                    csv_files[csv_class] = []
                csv_files[csv_class].append(os.path.join(sub_plot_directory, f))
    
    for csv_class, files in csv_files.items():
        plot_script_file = os.path.join(plots_directory, '%s.gpi' % csv_class)
        generate_gnuplot_script_tput_lat_agg(config, plot_script_file)
        run_gnuplot(files, os.path.join(plots_directory, '%s.png' % csv_class), plot_script_file)

def generate_gnuplot_script_tail_at_scale(config, script_file):
    with open(script_file, 'w') as f:
        write_gpi_header(f)
        f.write("set key top left\n")
        f.write("set xlabel '# of subrequests'\n")
        f.write("set ylabel 'Median Latency (ms)'\n")
        f.write("set terminal pngcairo size %d,%d enhanced dashed font '%s'\n" %
            (config['plot_cdf_png_width'], config['plot_cdf_png_height'],
                config['plot_cdf_png_font']))
        f.write('set output outfile\n')
        write_line_styles(f)
        f.write('plot ')
        for i in range(len(config['replication_protocol'])):
            f.write("datafile%d title '%s' ls %d with lines" % (i, config['plot_cdf_series_title'][i].replace('_', '\\_'), i + 1))
            if i != len(config['replication_protocol']) - 1:
                f.write(', \\\n')

def generate_tail_at_scale_plots(config, base_out_directory, sub_out_directories):
    plots_directory = os.path.join(base_out_directory, config['plot_directory_name'])
    os.makedirs(plots_directory, exist_ok=True)
    csv_files = [[[] for i in range(len(config['server_names'])+1)] for j in range(len(config['client_nodes_per_server']))]
    for k in range(len(config['client_tail_at_scale'])):
        # for tail-at-scale request size k
        for i in range(len(sub_out_directories[k])):
            # for replication protocol i
            for j in range(len(sub_out_directories[k][i])):
                # for client configuration j
                with open(os.path.join(sub_out_directories[k][i][j], STATS_FILE)) as f:
                    stats = json.load(f)
                    csvfile = os.path.join(plots_directory, '%s-%d-%d-overall.csv' % (config['replication_protocol'][i], i, j))
                    csv_files[j][0].append(csvfile)
                    with open(csvfile, 'a') as csvf:
                        csvwriter = csv.writer(csvf)
                        csvwriter.writerow([config['client_tail_at_scale'][k], stats['aggregate']['max_norm']['p50']])
                        for r in range(len(config['server_names'])):
                            csvfilereg = os.path.join(plots_directory, '%s-%d-%d-region-%d.csv' % (config['replication_protocol'][i], i, j, r))
                            csv_files[j][r+1].append(csvfilereg)
                            with open(csvfilereg, 'a') as csvfreg:
                                csvwriterreg = csv.writer(csvfreg)
                                csvwriterreg.writerow([config['client_tail_at_scale'][k], stats['aggregate']['region-%d' % r]['max']['p50']])

    for j in range(len(config['client_nodes_per_server'])):
        plot_script_prefix = 'tail-at-scale-overall-%d' % j
        plot_script_file = os.path.join(plots_directory, '%s.gpi' % plot_script_prefix)
        generate_gnuplot_script_tail_at_scale(config, plot_script_file)
        run_gnuplot(csv_files[j][0], os.path.join(plots_directory, '%s.png' % plot_script_prefix), plot_script_file)
        print(csv_files)
        num_regions = get_num_regions(config) 
        for r in range(num_regions):
            plot_script_file = os.path.join(plots_directory, 'tail-at-scale-%d-region-%d.gpi' % (j, r))
            generate_gnuplot_script_tail_at_scale(config, plot_script_file)
            run_gnuplot(csv_files[j][r+1], os.path.join(plots_directory, 'tail-at-scale-%d-region-%d.png' % (j, r)), plot_script_file)

def generate_gnuplot_script_cdf(config, script_file):
    with open(script_file, 'w') as f:
        f.write("set datafile separator ','\n")
        f.write("set key bottom right\n")
        f.write("set xlabel '%s'\n" % config['plot_cdf_x_label'])
        f.write("set ylabel '%s'\n" % config['plot_cdf_y_label'])
        f.write("set terminal pngcairo size %d,%d enhanced font '%s'\n" %
            (config['plot_cdf_png_width'], config['plot_cdf_png_height'],
                config['plot_cdf_png_font']))
        f.write('set output outfile\n')
        f.write("plot datafile0 title '%s' with lines\n" % config['plot_cdf_series_title'].replace('_', '\\_'))

def generate_gnuplot_script_cdf_log(config, script_file):
    with open(script_file, 'w') as f:
        f.write("set datafile separator ','\n")
        f.write("set key bottom right\n")
        f.write("set xlabel '%s'\n" % config['plot_cdf_x_label'])
        f.write("set ylabel '%s'\n" % config['plot_cdf_y_label'])
        f.write("set terminal pngcairo size %d,%d enhanced font '%s'\n" %
            (config['plot_cdf_png_width'], config['plot_cdf_png_height'],
                config['plot_cdf_png_font']))
        f.write('set output outfile\n')
        f.write("plot datafile0 using 1:(-log10(1-$2)):yticlabels(3) title '%s' with lines\n" % config['plot_cdf_series_title'].replace('_', '\\_'))

def generate_gnuplot_script_tput_lat(config, plot_script_file):
    with open(plot_script_file, 'w') as f:
        f.write("set datafile separator ','\n")
        f.write("set key top left\n")
        f.write("set xlabel '%s'\n" % config['plot_tput_lat_x_label'])
        f.write("set ylabel '%s'\n" % config['plot_tput_lat_y_label'])
        f.write("set terminal pngcairo size %d,%d enhanced font '%s'\n" %
            (config['plot_tput_lat_png_width'],
                config['plot_tput_lat_png_height'],
                config['plot_tput_lat_png_font']))
        f.write('set output outfile\n')
        f.write("plot datafile0 title '%s' with linespoint\n" % config['plot_tput_lat_series_title'].replace('_', '\\_'))

def write_gpi_header(f):
    f.write("set datafile separator ','\n")

def write_line_styles(f):
    f.write('set style line 1 linetype 1 linewidth 2\n')
    f.write('set style line 2 linetype 1 linecolor "green" linewidth 2\n')
    f.write('set style line 3 linetype 1 linecolor "blue" linewidth 2\n')
    f.write('set style line 4 linetype 4 linewidth 2\n')
    f.write('set style line 5 linetype 5 linewidth 2\n')
    f.write('set style line 6 linetype 8 linewidth 2\n')

def generate_gnuplot_script_cdf_log_agg(config, script_file):
    with open(script_file, 'w') as f:
        write_gpi_header(f)
        f.write("set key bottom right\n")
        f.write("set ytics (0,0.9,0.99,0.999,0.9999,1.0)\n")
        f.write("set xlabel '%s'\n" % config['plot_cdf_x_label'])
        f.write("set ylabel '%s'\n" % config['plot_cdf_y_label'])
        f.write("set terminal pngcairo size %d,%d enhanced dashed font '%s'\n" %
            (config['plot_cdf_png_width'], config['plot_cdf_png_height'],
                config['plot_cdf_png_font']))
        f.write('set output outfile\n')
        write_line_styles(f)
        f.write('plot ')
        for i in range(len(config['replication_protocol'])):
            if i == 0:
                labels = ':yticlabels(3)'
            else:
                labels = ''
            f.write("datafile%d using 1:(-log10(1-$2))%s title '%s' ls %d with lines" % (i, labels, config['plot_cdf_series_title'][i].replace('_', '\\_'), i + 1))
            if i != len(config['replication_protocol']) - 1:
                f.write(', \\\n')

def generate_gnuplot_script_cdf_agg(config, script_file):
    with open(script_file, 'w') as f:
        write_gpi_header(f)
        f.write("set key bottom right\n")
        f.write("set xlabel '%s'\n" % config['plot_cdf_x_label'])
        f.write("set ylabel '%s'\n" % config['plot_cdf_y_label'])
        f.write("set terminal pngcairo size %d,%d enhanced dashed font '%s'\n" %
            (config['plot_cdf_png_width'], config['plot_cdf_png_height'],
                config['plot_cdf_png_font']))
        f.write('set output outfile\n')
        write_line_styles(f)
        f.write('plot ')
        for i in range(len(config['replication_protocol'])):
            f.write("datafile%d title '%s' ls %d with lines" % (i, config['plot_cdf_series_title'][i].replace('_', '\\_'), i + 1))
            if i != len(config['replication_protocol']) - 1:
                f.write(', \\\n')

def generate_gnuplot_script_tput_lat_agg(config, plot_script_file):
    with open(plot_script_file, 'w') as f:
        write_gpi_header(f)
        f.write("set key top left\n")
        f.write("set xlabel '%s'\n" % config['plot_tput_lat_x_label'])
        f.write("set ylabel '%s'\n" % config['plot_tput_lat_y_label'])
        f.write("set terminal pngcairo size %d,%d enhanced dashed font '%s'\n" %
            (config['plot_tput_lat_png_width'],
                config['plot_tput_lat_png_height'],
                config['plot_tput_lat_png_font']))
        f.write('set output outfile\n')
        write_line_styles(f)
        f.write('plot ')
        for i in range(len(config['replication_protocol'])):
            f.write("datafile%d title '%s' ls %d with linespoint" % (i, config['plot_tput_lat_series_title'][i].replace('_', '\\_'), i + 1))
            if i != len(config['replication_protocol']) - 1:
                f.write(', \\\n')

def regenerate_plots(config_file, exp_dir, executor, calc_stats=True):
    with open(config_file) as f:
        config = json.load(f)

        if not 'client_stats_blacklist' in config:
            config['client_stats_blacklist'] = []
        if not 'client_combine_stats_blacklist' in config:
            config['client_combine_stats_blacklist'] = []
        if not 'client_cdf_plot_blacklist' in config:
            config['client_cdf_plot_blacklist'] = []

        out_directories = sorted(next(os.walk(exp_dir))[1])
        if 'plots' in out_directories:
            out_directories.remove('plots')
        out_directories = [os.path.join(exp_dir, d) for d in out_directories]
        out_directories = out_directories[:len(config['replication_protocol'])]
        sub_out_directories = []
        for i in range(len(out_directories)):
            out_dir = out_directories[i]
            dirs = sorted(next(os.walk(out_dir))[1])
            if 'plots' in dirs:
                dirs.remove('plots')
            dirs = [os.path.join(out_dir, d, config['out_directory_name']) for d in dirs]
            config_new = config.copy()
            config_new['base_local_exp_directory'] = exp_dir
            server_replication_protocol = config['replication_protocol'][i]
            config_new['replication_protocol'] = server_replication_protocol
            config_new['plot_cdf_series_title'] = config['plot_cdf_series_title'][i]
            config_new['plot_tput_lat_series_title'] = config['plot_tput_lat_series_title'][i]
            config_new['replication_protocol_settings'] = config['replication_protocol_settings'][i]
            sub_out_directories.append(dirs)
            for j in range(len(dirs)):
                sub_out_dir = dirs[j]
                config_new_new = config_new.copy()
                config_new_new['base_local_exp_directory'] = exp_dir
                n = config_new['client_nodes_per_server'][j]
                m = config_new['client_processes_per_client_node'][j]
                config_new_new['client_nodes_per_server'] = n
                config_new_new['client_processes_per_client_node'] = m
                if calc_stats:
                    stats = calculate_statistics(config_new_new, sub_out_dir)
                else:
                    with open(os.path.join(sub_out_dir, STATS_FILE)) as f:
                        stats = json.load(f)
                generate_cdf_plots(config_new_new, sub_out_dir, stats, executor)
            generate_tput_lat_plots(config_new, out_dir, dirs)
        generate_agg_cdf_plots(config, exp_dir, sub_out_directories)
        generate_agg_tput_lat_plots(config, exp_dir, out_directories)

def generate_agg_write_percentage_csv(config, base_out_directory, sub_out_directories):
    plots_directory = os.path.join(base_out_directory, config['plot_directory_name'])
    os.makedirs(plots_directory, exist_ok=True)
    print(config['client_write_percentage'])
    write_percentage = config['client_write_percentage'] / (config['client_write_percentage'] + config['client_read_percentage'] + config['client_rmw_percentage'])
    for i in range(len(sub_out_directories)):
        # for replication protocol i
        for j in range(len(sub_out_directories[i])):
            # for client configuration j
            with open(os.path.join(sub_out_directories[i][j], STATS_FILE)) as f:
                stats = json.load(f)
                for p in ['p50', 'p75', 'p90', 'p95', 'p99']:
                    for t in ['w_norm', 'r_norm']:
                        with open(os.path.join(base_out_directory, '%s-%d-%d-%s-%s.csv' % (config['replication_protocol'][i], i, j, t, p)), 'w') as f:
                            csvwriter = csv.writer(f)
                            csvwriter.writerow([write_percentage, stats['aggregate'][t][p]])


def generate_varying_write_csvs(config_file, exp_dir, calc_stats=True):
    with open(config_file) as f:
        config = json.load(f)
        out_directories = sorted(next(os.walk(exp_dir))[1])
        if 'plots' in out_directories:
            out_directories.remove('plots')
        out_directories = [os.path.join(exp_dir, d) for d in out_directories]
        out_directories = out_directories[:len(config['replication_protocol'])]
        sub_out_directories = []
        for i in range(len(out_directories)):
            out_dir = out_directories[i]
            dirs = sorted(next(os.walk(out_dir))[1])
            if 'plots' in dirs:
                dirs.remove('plots')
            dirs = [os.path.join(out_dir, d, config['out_directory_name']) for d in dirs]
            config_new = config.copy()
            config_new['base_local_exp_directory'] = exp_dir
            server_replication_protocol = config['replication_protocol'][i]
            config_new['replication_protocol'] = server_replication_protocol
            config_new['plot_cdf_series_title'] = config['plot_cdf_series_title'][i]
            config_new['plot_tput_lat_series_title'] = config['plot_tput_lat_series_title'][i]
            config_new['replication_protocol_settings'] = config['replication_protocol_settings'][i]
            sub_out_directories.append(dirs)
            for j in range(len(dirs)):
                sub_out_dir = dirs[j]
                config_new_new = config_new.copy()
                config_new_new['base_local_exp_directory'] = exp_dir
                n = config_new['client_nodes_per_server'][j]
                m = config_new['client_processes_per_client_node'][j]
                config_new_new['client_nodes_per_server'] = n
                config_new_new['client_processes_per_client_node'] = m
        generate_agg_write_percentage_csv(config, exp_dir, sub_out_directories)

def regenerate_tail_at_scale_plots(config_file, exp_dir):
    with open(config_file) as f:
        config = json.load(f)
        directories = sorted(next(os.walk(exp_dir))[1])
        if 'plots' in directories:
            directories.remove('plots')
        directories = [os.path.join(exp_dir, d) for d in directories]
        directories = directories[:len(config['client_tail_at_scale'])]
        sub_sub_out_directories = []
        for k in range(len(directories)):
            out_directories = sorted(next(os.walk(directories[k]))[1])
            if 'plots' in out_directories:
                out_directories.remove('plots')
            out_directories = [os.path.join(directories[k], d) for d in out_directories]
            out_directories = out_directories[:len(config['replication_protocol'])]
            sub_out_directories = []
            for i in range(len(out_directories)):
                out_dir = out_directories[i]
                dirs = sorted(next(os.walk(out_dir))[1])
                if 'plots' in dirs:
                    dirs.remove('plots')
                dirs = [os.path.join(out_dir, d, config['out_directory_name']) for d in dirs]
                config_new = config.copy()
                config_new['client_tail_at_scale'] = config['client_tail_at_scale'][k]
                config_new['base_local_exp_directory'] = exp_dir
                server_replication_protocol = config['replication_protocol'][i]
                config_new['replication_protocol'] = server_replication_protocol
                config_new['plot_cdf_series_title'] = config['plot_cdf_series_title'][i]
                config_new['plot_tput_lat_series_title'] = config['plot_tput_lat_series_title'][i]
                config_new['replication_protocol_settings'] = config['replication_protocol_settings'][i]
                sub_out_directories.append(dirs)
            sub_sub_out_directories.append(sub_out_directories)
        generate_tail_at_scale_plots(config, exp_dir, sub_sub_out_directories)

