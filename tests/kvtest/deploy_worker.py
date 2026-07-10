#!/usr/bin/env python3
"""Batch start/stop datasystem workers in k8s Pods."""

import argparse
import base64
import glob
import json
import os
import subprocess
import sys
import tempfile
from concurrent.futures import ThreadPoolExecutor, as_completed


# Default timeout for all kubectl operations (seconds)
DEFAULT_TIMEOUT = 300


def get_pods(namespace, prefixes):
    """Get running pods matching any of the given name prefixes.

    OR semantics: a pod is selected if its name starts with any prefix.
    Dedup by name (defensive; pod names are unique within a namespace, so
    a pod matching multiple prefixes is still added once). The final list is
    sorted by name globally so instance_id assignment is deterministic
    regardless of the order prefixes were passed on the CLI. A WARNING is
    printed for each prefix that matched zero pods; callers decide whether
    an all-zero result is fatal.
    """
    try:
        out = subprocess.check_output(
            ['kubectl', 'get', 'pods', '-n', namespace, '-o', 'json',
             '--field-selector=status.phase=Running'],
            text=True, timeout=DEFAULT_TIMEOUT)
    except FileNotFoundError:
        print('ERROR: kubectl not found', file=sys.stderr)
        sys.exit(1)
    except subprocess.CalledProcessError as e:
        print(f'ERROR: kubectl failed: {e.stderr}', file=sys.stderr)
        sys.exit(1)

    prefixes = list(prefixes or [])
    pods = []
    seen = set()
    for item in json.loads(out).get('items', []):
        name = item['metadata']['name']
        if not any(name.startswith(p) for p in prefixes):
            continue
        pod_ip = item.get('status', {}).get('podIP', '')
        if not pod_ip:
            continue
        if name in seen:
            continue
        seen.add(name)
        pods.append({'name': name, 'ip': pod_ip})
    pods.sort(key=lambda p: p['name'])
    for p in prefixes:
        if not any(pod['name'].startswith(p) for pod in pods):
            print(f'WARNING: prefix "{p}" matched 0 pods', file=sys.stderr)
    return pods


def kubectl_exec(pod, namespace, cmd, check=True, timeout=DEFAULT_TIMEOUT):
    """Execute command in pod via kubectl."""
    return subprocess.run(
        ['kubectl', 'exec', pod, '-n', namespace, '--', 'sh', '-c', cmd],
        check=check, capture_output=True, text=True, timeout=timeout)


def kubectl_cp_to(pod, namespace, src, dst, timeout=DEFAULT_TIMEOUT):
    """Copy local file to pod."""
    subprocess.run(
        ['kubectl', 'cp', src, f'{namespace}/{pod}:{dst}'],
        check=True, capture_output=True, text=True, timeout=timeout)


def install_worker_whl(pod, namespace, whl_path, timeout=DEFAULT_TIMEOUT):
    """Install worker whl package in a single pod."""
    pod_name = pod['name']
    pod_ip = pod['ip']
    remote_whl = f'/tmp/{os.path.basename(whl_path)}'

    try:
        print(f'  {pod_name} ({pod_ip}) -> copying whl...')
        kubectl_cp_to(pod_name, namespace, whl_path, remote_whl, timeout=timeout)

        install_cmd = f'pip3 install --force-reinstall {remote_whl}'
        result = kubectl_exec(pod_name, namespace, install_cmd, timeout=timeout)
        print(f'  {pod_name} ({pod_ip}) -> whl installed successfully')
        return True

    except subprocess.TimeoutExpired:
        print(f'  {pod_name} ({pod_ip}) -> FAILED: timeout')
        return False
    except subprocess.CalledProcessError as e:
        print(f'  {pod_name} ({pod_ip}) -> FAILED: {e.stderr.strip() if e.stderr else "unknown error"}')
        return False
    except Exception as e:
        print(f'  {pod_name} ({pod_ip}) -> FAILED: {e}')
        return False
    finally:
        try:
            kubectl_exec(pod_name, namespace, f'rm -f {remote_whl}', check=False, timeout=10)
        except Exception:
            pass


def upload_procmon(pod, namespace, remote_dir='/tmp', timeout=DEFAULT_TIMEOUT):
    """Upload procmon.py to pod."""
    script_dir = os.path.dirname(os.path.abspath(__file__))
    procmon_src = os.path.join(script_dir, 'procmon.py')
    if not os.path.exists(procmon_src):
        procmon_src = os.path.join(script_dir, 'tools', 'procmon.py')
    if not os.path.exists(procmon_src):
        return False
    try:
        kubectl_exec(pod['name'], namespace, f'mkdir -p {remote_dir}', check=False, timeout=timeout)
        kubectl_cp_to(pod['name'], namespace, procmon_src, f'{remote_dir}/procmon.py', timeout=timeout)
        return True
    except Exception:
        return False


def start_procmon(pod, namespace, worker_pid, remote_dir='/tmp',
                  interval=1, timeout=30):
    """Start procmon monitoring for a worker process.

    Uses procmon.py --background for proper daemonization (os.fork +
    os.setsid). The parent prints the child PID to stdout and exits,
    so kubectl exec returns immediately. The child runs in a new session,
    fully detached from the kubectl exec session.
    """
    cmd = (f'cd {remote_dir} && '
           f'python3 procmon.py --pid {worker_pid} -i {interval} '
           f'--output resource_monitor.log --background')
    try:
        result = kubectl_exec(pod['name'], namespace, cmd, check=False, timeout=timeout)
        pid = result.stdout.strip()
        if pid and pid.isdigit():
            return pid
        return None
    except Exception:
        return None


def _resolve_procmon_dir(config_template, remote_config):
    """Resolve procmon output dir from worker config log_dir, fallback to remote_config dir.

    log_dir may be a {"value": ...} dict (dscli config style) or a plain string;
    empty/missing falls back to the directory holding remote_config.
    """
    log_dir_entry = config_template.get('log_dir', {})
    if isinstance(log_dir_entry, dict):
        procmon_dir = log_dir_entry.get('value', None)
    else:
        procmon_dir = log_dir_entry or None
    if not procmon_dir:
        procmon_dir = os.path.dirname(remote_config)
    return procmon_dir


def find_worker_pid(pod, namespace, port, timeout=DEFAULT_TIMEOUT):
    """Find worker PID by listening port."""
    result = kubectl_exec(pod['name'], namespace,
        f'ss -tlnp \'sport = :{port}\' 2>/dev/null | grep -oP \'pid=\\K[0-9]+\' | head -1',
        check=False, timeout=timeout)
    if result.returncode == 0 and result.stdout.strip():
        return result.stdout.strip().split('\n')[0]
    # Fallback: pgrep
    result = kubectl_exec(pod['name'], namespace,
        f'pgrep -f datasystem_worker | head -1', check=False, timeout=timeout)
    if result.returncode == 0 and result.stdout.strip():
        return result.stdout.strip().split('\n')[0]
    return None


def start_worker(pod, namespace, config_template, worker_port, remote_config,
                 enable_procmon=True, procmon_remote_dir='/tmp',
                 numactl_opts=None, timeout=DEFAULT_TIMEOUT):
    """Start worker in a single pod."""
    pod_name = pod['name']
    pod_ip = pod['ip']

    with tempfile.NamedTemporaryFile(
        mode='w', suffix='.json', prefix=f'worker_{pod_name}_',
        delete=False
    ) as tf:
        json.dump(config_template, tf, indent=2)
        tmp_path = tf.name

    try:
        kubectl_cp_to(pod_name, namespace, tmp_path, remote_config, timeout=timeout)
        cmd = f'dscli start -f {remote_config}'
        if numactl_opts:
            cmd += f' {numactl_opts}'
        result = kubectl_exec(pod_name, namespace, cmd, timeout=timeout)
        print(f'  {pod_name} ({pod_ip}) -> started')

        if enable_procmon:
            # Upload and start procmon
            if upload_procmon(pod, namespace, procmon_remote_dir, timeout):
                import time
                time.sleep(1)
                pid = find_worker_pid(pod, namespace, worker_port, timeout)
                if pid:
                    procmon_pid = start_procmon(pod, namespace, pid, procmon_remote_dir)
                    if procmon_pid:
                        print(f'  {pod_name} ({pod_ip}) -> procmon started (pid={procmon_pid}, monitoring worker pid={pid})')
                    else:
                        print(f'  {pod_name} ({pod_ip}) -> procmon start failed')
                else:
                    print(f'  {pod_name} ({pod_ip}) -> procmon skipped: worker pid not found')
            else:
                print(f'  {pod_name} ({pod_ip}) -> procmon skipped: upload failed')
        return True
    except subprocess.TimeoutExpired:
        print(f'  {pod_name} ({pod_ip}) -> FAILED: timeout')
        return False
    except subprocess.CalledProcessError as e:
        print(f'  {pod_name} ({pod_ip}) -> FAILED: {e.stderr.strip()}')
        return False
    finally:
        os.unlink(tmp_path)


def check_worker(pod, namespace, process_name='datasystem_worker', timeout=DEFAULT_TIMEOUT):
    """Check if worker process is alive in a single pod."""
    pod_name = pod['name']
    try:
        # Use ps + grep -v grep to exclude grep itself (handles path like
        # /usr/local/lib64/python3.11/site-packages/yr/datasystem/datasystem_worker)
        result = kubectl_exec(pod_name, namespace,
            f'ps aux | grep "{process_name}" | grep -v grep | wc -l', check=False, timeout=timeout)
    except subprocess.TimeoutExpired:
        return (pod, 'error', 'timeout')
    if result.returncode != 0:
        return (pod, 'error', result.stderr.strip())
    count = int(result.stdout.strip())
    return (pod, 'alive' if count > 0 else 'dead', count)


def kill_worker(pod, namespace, process_name='datasystem_worker', timeout=DEFAULT_TIMEOUT):
    """Force kill worker and procmon processes in a single pod."""
    pod_name = pod['name']
    pod_ip = pod['ip']
    try:
        kubectl_exec(pod_name, namespace,
            f'pgrep -f {process_name} | xargs -r kill -9; '
            f'pgrep -f procmon.py | xargs -r kill -9',
            check=False, timeout=timeout)
        print(f'  {pod_name} ({pod_ip}) -> killed')
        return True
    except subprocess.TimeoutExpired:
        print(f'  {pod_name} ({pod_ip}) -> FAILED: timeout')
        return False
    except Exception as e:
        print(f'  {pod_name} ({pod_ip}) -> FAILED: {e}')
        return False


def stop_worker(pod, namespace, remote_config='/tmp/worker.config', timeout=DEFAULT_TIMEOUT):
    """Stop worker gracefully using dscli."""
    pod_name = pod['name']
    pod_ip = pod['ip']
    try:
        kubectl_exec(pod_name, namespace, f'dscli stop -f {remote_config}', timeout=timeout)
        print(f'  {pod_name} ({pod_ip}) -> stopped')
        return True
    except subprocess.TimeoutExpired:
        print(f'  {pod_name} ({pod_ip}) -> FAILED: timeout')
        return False
    except subprocess.CalledProcessError as e:
        print(f'  {pod_name} ({pod_ip}) -> FAILED: {e.stderr.strip() if e.stderr else "unknown"}')
        return False
    except Exception as e:
        print(f'  {pod_name} ({pod_ip}) -> FAILED: {e}')
        return False


def find_default_whl():
    """Find the default whl file in ../../output directory."""
    output_dir = os.path.join(os.path.dirname(__file__), '..', '..', 'output')
    pattern = os.path.join(output_dir, 'openyuanrong_datasystem-*.whl')
    matches = glob.glob(pattern)
    if matches:
        return sorted(matches)[-1]
    return ''


def do_for_all_pods(pods, do_op, desc):
    """Execute operation for all pods in parallel."""
    print(f'\n{desc}...')
    results = []
    with ThreadPoolExecutor(max_workers=len(pods)) as pool:
        futures = {pool.submit(do_op, pod): pod for pod in pods}
        for future in as_completed(futures):
            results.append(future.result())

    ok = sum(1 for r in results if r)
    print(f'\nResult: {ok}/{len(results)} succeeded')
    return 0 if ok == len(results) else 1


def cmd_start(args, pods):
    """Start workers."""
    with open(args.config) as f:
        config_template = json.load(f)

    # Default procmon dir to log_dir from worker config, fallback to --remote-config dir
    if args.procmon_dir is None:
        args.procmon_dir = _resolve_procmon_dir(config_template, args.remote_config)

    # Apply config overrides from --set
    if args.set:
        print('\nApplying config overrides:')
        for override in args.set:
            if '=' not in override:
                print(f'  WARNING: Ignoring invalid override: {override}')
                continue
            key, value = override.split('=', 1)
            key = key.strip()
            value = value.strip()
            if value.lower() == 'true':
                parsed_value = True
            elif value.lower() == 'false':
                parsed_value = False
            elif value.lower() == 'null' or value.lower() == 'none':
                parsed_value = None
            else:
                try:
                    if '.' in value:
                        parsed_value = float(value)
                    else:
                        parsed_value = int(value)
                except ValueError:
                    parsed_value = value
            config_template[key] = {"value": parsed_value}
            print(f'  Set {key}.value = {parsed_value}')
    else:
        print('\nNo config overrides specified')

    def do_op(pod):
        cfg = json.loads(json.dumps(config_template))
        cfg['worker_address']['value'] = f'{pod["ip"]}:{args.port}'
        numactl_opts = None
        if args.numa_nodes:
            numactl_opts = f'-N {args.numa_nodes}'
        if args.cpu_bind:
            numactl_opts = f'-C {args.cpu_bind}'
        return start_worker(pod, args.namespace, cfg, args.port, args.remote_config,
                           enable_procmon=args.enable_procmon,
                           procmon_remote_dir=args.procmon_dir,
                           numactl_opts=numactl_opts,
                           timeout=args.timeout)

    return do_for_all_pods(pods, do_op, 'Starting workers')


def cmd_stop(args, pods):
    """Stop workers gracefully using dscli."""
    def do_op(pod):
        return stop_worker(pod, args.namespace, args.remote_config, args.timeout)

    return do_for_all_pods(pods, do_op, 'Stopping workers')


def cmd_kill(args, pods):
    """Force kill workers."""
    def do_op(pod):
        return kill_worker(pod, args.namespace, args.process, args.timeout)

    return do_for_all_pods(pods, do_op, 'Killing workers')


def cmd_restart(args, pods):
    """Restart workers in-place using each pod's existing remote config.

    Unlike ``start``, this does not re-push a config template: it reuses the
    remote_config already present in each pod, runs ``dscli start`` against it,
    then re-attaches procmon so resource_monitor.log keeps appending. Intended
    for kill/restart scaling tests where the pod stays Running and only the
    worker process was stopped/killed.
    """
    remote_config = args.remote_config
    port = args.port
    # All pods share the same log_dir; read it from the first pod's remote
    # config (same approach as cmd_clean) so procmon writes to the same dir
    # as the original start.
    procmon_dir = None
    if pods:
        try:
            result = kubectl_exec(pods[0]['name'], args.namespace,
                                  f'cat {remote_config}', check=True, timeout=args.timeout)
            config = json.loads(result.stdout)
            procmon_dir = _resolve_procmon_dir(config, remote_config)
        except Exception as e:
            print(f'WARNING: Failed to read remote config from pod: {e}', file=sys.stderr)
    if not procmon_dir:
        procmon_dir = os.path.dirname(remote_config)

    def do_op(pod):
        pod_name = pod['name']
        pod_ip = pod['ip']
        try:
            kubectl_exec(pod_name, args.namespace,
                         f'dscli start -f {remote_config}', timeout=args.timeout)
            pid = find_worker_pid(pod, args.namespace, port, timeout=args.timeout)
            if not pid:
                print(f'  {pod_name} ({pod_ip}) -> started, procmon skipped: worker pid not found')
                return True
            if upload_procmon(pod, args.namespace, procmon_dir, timeout=args.timeout):
                procmon_pid = start_procmon(pod, args.namespace, pid, procmon_dir, timeout=args.timeout)
                if procmon_pid:
                    print(f'  {pod_name} ({pod_ip}) -> restarted (worker pid={pid}, procmon pid={procmon_pid})')
                else:
                    print(f'  {pod_name} ({pod_ip}) -> started, procmon start failed')
            else:
                print(f'  {pod_name} ({pod_ip}) -> started, procmon upload failed')
            return True
        except subprocess.TimeoutExpired:
            print(f'  {pod_name} ({pod_ip}) -> FAILED: timeout')
            return False
        except subprocess.CalledProcessError as e:
            print(f'  {pod_name} ({pod_ip}) -> FAILED: {e.stderr.strip() if e.stderr else e}')
            return False
        except Exception as e:
            print(f'  {pod_name} ({pod_ip}) -> FAILED: {e}')
            return False

    return do_for_all_pods(pods, do_op, 'Restarting workers')


def clean_pod(pod, namespace, log_dir, remote_config_dir, timeout=DEFAULT_TIMEOUT):
    """Kill worker and clean logs in a single pod."""
    pod_name = pod['name']
    pod_ip = pod['ip']
    try:
        kill_worker(pod, namespace, timeout=timeout)

        # Remove log_dir and resource_monitor.log from remote_config_dir
        if log_dir:
            kubectl_exec(pod_name, namespace, f'rm -rf {log_dir}', check=False, timeout=timeout)
        kubectl_exec(pod_name, namespace, f'rm -f {remote_config_dir}/resource_monitor.log', check=False, timeout=timeout)

        print(f'  {pod_name} ({pod_ip}) -> OK')
        return True
    except subprocess.TimeoutExpired:
        print(f'  {pod_name} ({pod_ip}) -> FAILED: timeout')
        return False
    except Exception as e:
        print(f'  {pod_name} ({pod_ip}) -> FAILED: {e}')
        return False


def cmd_clean(args, pods):
    """Kill workers and clean log directories."""
    log_dir = None
    if pods:
        try:
            result = kubectl_exec(pods[0]['name'], args.namespace, f'cat {args.remote_config}', check=True, timeout=args.timeout)
            config = json.loads(result.stdout)
            log_dir_entry = config.get('log_dir', {})
            if isinstance(log_dir_entry, dict):
                log_dir = log_dir_entry.get('value', None)
            else:
                log_dir = log_dir_entry or None
        except Exception as e:
            print(f'WARNING: Failed to read remote config from pod: {e}', file=sys.stderr)

    remote_config_dir = os.path.dirname(args.remote_config)

    def do_op(pod):
        return clean_pod(pod, args.namespace, log_dir, remote_config_dir, timeout=args.timeout)

    return do_for_all_pods(pods, do_op, 'Cleaning worker logs')


def cmd_check(args, pods):
    """Check workers."""
    print(f'\nChecking worker processes ({args.process})...')
    results = []
    with ThreadPoolExecutor(max_workers=len(pods)) as pool:
        futures = {pool.submit(check_worker, pod, args.namespace, args.process, args.timeout): pod
                   for pod in pods}
        for future in as_completed(futures):
            results.append(future.result())

    alive = 0
    dead = 0
    errors = 0
    for pod, status, detail in results:
        if status == 'alive':
            alive += 1
            print(f'  {pod["name"]} ({pod["ip"]}) -> alive (count={detail})')
        elif status == 'dead':
            dead += 1
            print(f'  {pod["name"]} ({pod["ip"]}) -> dead')
        else:
            errors += 1
            print(f'  {pod["name"]} ({pod["ip"]}) -> error ({detail})')

    total = len(results)
    print(f'\nResult: {alive} alive / {dead} dead / {errors} error / {total} total')
    return 0


def cmd_install(args, pods):
    """Install whl."""
    if not os.path.exists(args.whl):
        print(f'ERROR: whl file not found: {args.whl}', file=sys.stderr)
        return 1

    def do_op(pod):
        return install_worker_whl(pod, args.namespace, args.whl, args.timeout)

    return do_for_all_pods(pods, do_op, 'Installing whl')


def cmd_exec(args, pods):
    """Execute command in pods."""
    cmd = args.cmd

    def do_op(pod):
        pod_name = pod['name']
        pod_ip = pod['ip']
        try:
            result = kubectl_exec(pod_name, args.namespace, cmd, check=False, timeout=args.timeout)
            success = result.returncode == 0
            print(f'  {pod_name} ({pod_ip}) -> {"OK" if success else "FAILED"}')
            if result.stdout.strip():
                for line in result.stdout.strip().split('\n')[:5]:
                    print(f'    stdout: {line}')
            if result.stderr.strip():
                for line in result.stderr.strip().split('\n')[:5]:
                    print(f'    stderr: {line}')
            return success
        except subprocess.TimeoutExpired:
            print(f'  {pod_name} ({pod_ip}) -> TIMEOUT')
            return False
        except Exception as e:
            print(f'  {pod_name} ({pod_ip}) -> ERROR: {e}')
            return False

    return do_for_all_pods(pods, do_op, f'Executing command: {cmd}')


def collect_logs_from_pod(pod, namespace, log_dir, local_dir,
                          remote_config_dir=None, timeout=DEFAULT_TIMEOUT):
    """Collect log files from a single pod."""
    pod_name = pod['name']
    pod_ip = pod['ip']
    local_pod_dir = os.path.join(local_dir, f'{pod_name}')
    os.makedirs(local_pod_dir, exist_ok=True)

    try:
        # Check if log directory exists
        ls_result = kubectl_exec(pod_name, namespace, f'ls -d {log_dir} 2>/dev/null', check=False, timeout=timeout)
        if ls_result.returncode != 0:
            print(f'  {pod_name} ({pod_ip}) -> log dir {log_dir} does not exist')
            return True

        # List log files
        ls_result = kubectl_exec(pod_name, namespace, f'ls {log_dir}/*.log {log_dir}/*.log.gz {log_dir}/*.txt 2>/dev/null', check=False, timeout=timeout)
        log_files = [f.strip() for f in (ls_result.stdout or '').splitlines() if f.strip()]

        if not log_files:
            print(f'  {pod_name} ({pod_ip}) -> no log files found')
            return True

        print(f'  {pod_name} ({pod_ip}) -> found {len(log_files)} log files')

        # Collect each log file using base64 to safely transfer binary/non-UTF-8 content.
        # kubectl_exec uses text=True which fails on non-UTF-8 bytes in log files.
        for remote_path in log_files:
            try:
                fname = os.path.basename(remote_path)
                local_path = os.path.join(local_pod_dir, fname)
                result = kubectl_exec(pod_name, namespace, f'base64 {remote_path}', check=True, timeout=timeout)
                content = base64.b64decode(result.stdout)
                with open(local_path, 'wb') as f:
                    f.write(content)
            except Exception as e:
                print(f'    {os.path.basename(remote_path)} -> FAILED: {e}')

        # Collect procmon resource_monitor.log from remote_config_dir (start's fallback)
        # and from log_dir (when config had log_dir from the start).
        # This covers both scenarios: log_dir injected via --set (procmon in
        # remote_config_dir) and log_dir in original config (procmon in log_dir).
        procmon_dirs = set()
        if remote_config_dir:
            procmon_dirs.add(remote_config_dir)
        if log_dir:
            procmon_dirs.add(log_dir)
        # Skip dirs already covered by the *.log glob above
        glob_dirs = {os.path.dirname(f) for f in log_files}
        for pdir in procmon_dirs:
            if pdir in glob_dirs:
                continue
            procmon_log = f'{pdir}/resource_monitor.log'
            try:
                result = kubectl_exec(pod_name, namespace, f'base64 {procmon_log}', check=True, timeout=timeout)
                content = base64.b64decode(result.stdout)
                local_path = os.path.join(local_pod_dir, 'resource_monitor.log')
                with open(local_path, 'wb') as f:
                    f.write(content)
            except Exception:
                pass

        return True
    except subprocess.TimeoutExpired:
        print(f'  {pod_name} ({pod_ip}) -> FAILED: timeout')
        return False
    except Exception as e:
        print(f'  {pod_name} ({pod_ip}) -> FAILED: {e}')
        return False


def cmd_collect(args, pods):
    """Collect worker logs from pods."""
    # First, try to get log_dir from one of the pods' config file
    log_dir = None
    if pods:
        try:
            # Read remote config from first pod
            result = kubectl_exec(pods[0]['name'], args.namespace, f'cat {args.remote_config}', check=True, timeout=args.timeout)
            config = json.loads(result.stdout)
            
            log_dir_entry = config.get('log_dir', {})
            if isinstance(log_dir_entry, dict):
                log_dir = log_dir_entry.get('value', None)
            else:
                log_dir = log_dir_entry or None
        except Exception as e:
            print(f'WARNING: Failed to read remote config from pod: {e}', file=sys.stderr)
    
    if not log_dir:
        print('ERROR: log_dir not found in remote config', file=sys.stderr)
        return 1

    # Procmon resource_monitor.log may be in remote_config_dir (start's fallback
    # when config had no log_dir) or in log_dir.  Try both to cover all scenarios.
    remote_config_dir = os.path.dirname(args.remote_config)

    print(f'Using log directory from remote config: {log_dir}')
    local_dir = args.output or 'collected_worker_logs'

    def do_op(pod):
        return collect_logs_from_pod(pod, args.namespace, log_dir, local_dir,
                                     remote_config_dir=remote_config_dir, timeout=args.timeout)

    return do_for_all_pods(pods, do_op, 'Collecting worker logs')


def main():
    default_whl = find_default_whl()

    # Main parser
    parser = argparse.ArgumentParser(
        description='Batch manage datasystem workers in k8s Pods',
        formatter_class=argparse.RawDescriptionHelpFormatter)
    subparsers = parser.add_subparsers(dest='action', help='Action to perform')

    # Common parent parser
    parent_parser = argparse.ArgumentParser(add_help=False)
    parent_parser.add_argument('-p', '--prefix', action='append', default=None,
                               dest='prefixes', metavar='PREFIX',
                               help='Pod name prefix to match (repeatable: '
                                    '-p worker-a -p worker-b). A pod is '
                                    'selected if it matches ANY prefix.')
    parent_parser.add_argument('-n', '--namespace', default='default',
                               help='k8s namespace (default: default)')
    parent_parser.add_argument('--timeout', type=int, default=DEFAULT_TIMEOUT,
                               help=f'Operation timeout in seconds (default: {DEFAULT_TIMEOUT})')

    # Start subcommand
    parser_start = subparsers.add_parser('start', parents=[parent_parser],
                                         help='Start workers from config')
    parser_start.add_argument('-c', '--config', required=True,
                              help='Path to worker.config template')
    parser_start.add_argument('--port', type=int, default=31501,
                              help='Worker port (default: 31501)')
    parser_start.add_argument('--remote-config', default='/tmp/worker.config',
                              help='Config path inside pod (default: /tmp/worker.config)')
    parser_start.add_argument('--set', '-s', action='append', default=[],
                              help='Add/override config values (format: key=value). '
                                   'Example: --set ttl_seconds=3600')
    parser_start.add_argument('--enable-procmon', action='store_true', default=True,
                              dest='enable_procmon',
                              help='Start procmon.py for worker monitoring (default: enabled)')
    parser_start.add_argument('--no-procmon', action='store_false',
                              dest='enable_procmon',
                              help='Disable procmon.py monitoring')
    parser_start.add_argument('--procmon-dir', default=None,
                              help='Remote directory for procmon files (default: same as --remote-config dir)')
    parser_start.add_argument('-N', '--numa-nodes', default=None,
                              help='NUMA node(s) to bind worker to, passed to dscli start -N (e.g. "0" or "0,1")')
    parser_start.add_argument('-C', '--cpu-bind', default=None,
                              help='CPU core(s) to bind worker to, passed to dscli start -C (e.g. "0-7" or "0,2,4,6")')

    # Stop subcommand (graceful stop using dscli)
    parser_stop = subparsers.add_parser('stop', parents=[parent_parser],
                                        help='Stop workers gracefully using dscli')
    parser_stop.add_argument('--remote-config', default='/tmp/worker.config',
                             help='Worker config file path (default: /tmp/worker.config)')

    # Kill subcommand (force kill using kill -9)
    parser_kill = subparsers.add_parser('kill', parents=[parent_parser],
                                        help='Force kill workers')
    parser_kill.add_argument('--process', default='datasystem_worker',
                             help='Process name to kill (default: datasystem_worker)')

    # Restart subcommand (in-place restart using existing remote config + procmon)
    parser_restart = subparsers.add_parser('restart', parents=[parent_parser],
                                           help='Restart workers in-place using existing remote config')
    parser_restart.add_argument('--port', type=int, default=31501,
                                help='Worker port to locate PID (default: 31501)')
    parser_restart.add_argument('--remote-config', default='/tmp/worker.config',
                                help='Config path inside pod (default: /tmp/worker.config)')

    # Check subcommand
    parser_check = subparsers.add_parser('check', parents=[parent_parser],
                                         help='Check worker status')
    parser_check.add_argument('--process', default='datasystem_worker',
                              help='Process name to check (default: datasystem_worker)')

    # Install subcommand
    parser_install = subparsers.add_parser('install', parents=[parent_parser],
                                           help='Install worker whl package')
    parser_install.add_argument('--whl', default=default_whl,
                                help='Path to worker whl package '
                                     '(default: auto-detect from ../../output)')

    # Exec subcommand
    parser_exec = subparsers.add_parser('exec', parents=[parent_parser],
                                        help='Execute command in pods')
    parser_exec.add_argument('--cmd', '-c', required=True,
                             help='Command to execute (required)')

    # Collect subcommand
    parser_collect = subparsers.add_parser('collect', parents=[parent_parser],
                                           help='Collect worker logs from pods')
    parser_collect.add_argument('--remote-config', default='/tmp/worker.config',
                                help='Config path inside pod (default: /tmp/worker.config)')
    parser_collect.add_argument('-o', '--output', default='collected_worker_logs',
                                help='Local output directory (default: collected_worker_logs)')

    # Clean subcommand
    parser_clean = subparsers.add_parser('clean', parents=[parent_parser],
                                         help='Kill workers and clean log directories')
    parser_clean.add_argument('--remote-config', default='/tmp/worker.config',
                              help='Config path inside pod (default: /tmp/worker.config)')

    args = parser.parse_args()

    if not args.action:
        parser.print_help()
        return 1

    # argparse with action='append' default=None won't enforce presence, so
    # validate explicitly here with a clear message.
    if not args.prefixes:
        print('ERROR: at least one --prefix is required '
              '(e.g. -p worker-a [-p worker-b])', file=sys.stderr)
        return 1

    # Get pods
    pods = get_pods(args.namespace, args.prefixes)
    if not pods:
        print(f'No running pods found matching prefixes {args.prefixes} '
              f'in namespace "{args.namespace}"')
        return 1

    print(f'Found {len(pods)} pods:')
    for p in pods:
        print(f'  {p["name"]} ({p["ip"]})')

    # Dispatch
    if args.action == 'start':
        return cmd_start(args, pods)
    elif args.action == 'stop':
        return cmd_stop(args, pods)
    elif args.action == 'kill':
        return cmd_kill(args, pods)
    elif args.action == 'restart':
        return cmd_restart(args, pods)
    elif args.action == 'check':
        return cmd_check(args, pods)
    elif args.action == 'install':
        return cmd_install(args, pods)
    elif args.action == 'exec':
        return cmd_exec(args, pods)
    elif args.action == 'collect':
        return cmd_collect(args, pods)
    elif args.action == 'clean':
        return cmd_clean(args, pods)

    return 0


if __name__ == '__main__':
    sys.exit(main())