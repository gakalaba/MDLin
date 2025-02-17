import subprocess
import os
import shutil

def compile_make(config):
    e = os.environ.copy()
    if 'make_env' in config:
        for k, v in config['make_env'].items():
            e[k] = v
    if not 'make_clean' in config or config['make_clean']:
        subprocess.call(["make", "clean"], cwd=config['src_directory'])
    if 'make_args' in config:
        subprocess.call(["make", config['make_args']], cwd=config['src_directory'], env=e)
    else:
        subprocess.call(["make"], cwd=config['src_directory'], env=e)
    bin_path = os.path.join(config['src_directory'], 'bin')
    os.makedirs(bin_path, exist_ok=True)
    if 'make_collect_bins' in config:
        for f in config['make_collect_bins']:
            shutil.copy2(os.path.join(config['src_directory'], f),
                os.path.join(bin_path, os.path.basename(f)))
    return bin_path

def get_current_branch(src_directory):
    return subprocess.check_output(["git", "rev-parse", "--abbrev-ref",
        "HEAD"], cwd=src_directory).rstrip()

def checkout_commit(src_directory, src_commit_hash):
    subprocess.call(["git", "checkout", src_commit_hash],
            cwd=src_directory)

def stash_changes(src_directory):
    subprocess.call(["git", "stash"], cwd=src_directory)

def pop_stashed_changes(src_directory):
    subprocess.call(["git", "stash", "pop"], cwd=src_directory)

def clean_working_directory(src_directory):
    return subprocess.run(['git', 'status', '--untracked-files=no', '--porcelain'], cwd=src_directory).returncode == 0

def remake_binaries(config):
    clean = clean_working_directory(config['src_directory'])
    if not clean:
        stash_changes(config['src_directory'])
    current_branch = get_current_branch(config['src_directory'])
    print(current_branch)
    checkout_commit(config['src_directory'],
        config['src_commit_hash'])
    compile_make(config)
    checkout_commit(config['src_directory'], current_branch)
    if not clean:
        pop_stashed_changes(config['src_directory'])

