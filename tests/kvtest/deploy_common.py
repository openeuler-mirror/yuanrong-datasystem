#!/usr/bin/env python3
"""Shared helpers for batch-managing datasystem worker / coordinator pods.

deploy_worker.py and deploy_coordinator.py build on top of this module. The
helpers here are role-agnostic: kubectl transport primitives, procmon
orchestration, per-pod lifecycle ops (start/stop/kill/check/collect/clean),
and shared subcommand implementations that differ between roles only by a
process name, an address key, or a label.

Role-specific concerns (which config key carries the listening address, NUMA
binding, default ports/paths, process names) live in the role files and are
passed into the shared helpers as parameters.
"""

import base64
import glob
import json
import os
import subprocess
import sys
import tempfile
from concurrent.futures import ThreadPoolExecutor, as_completed


# Default timeout for all kubectl operations (seconds).
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


def discover_nodes(timeout=DEFAULT_TIMEOUT):
    """Discover cluster nodes via ``kubectl get nodes``.

    Returns a list of ``{'ip', 'name'}`` for every node that exposes an
    InternalIP address, sorted by node name so any caller that spreads work
    across nodes (round-robin instance distribution, percentage buckets) gets
    a deterministic, reproducible assignment across runs -- the k8s API does
    not guarantee item order. Returns an empty list on any kubectl failure
    (kubectl missing, non-zero exit, timeout) so callers can decide whether
    to abort; a multi-instance deploy that must spread pods treats an empty
    list as a hard error. This is the single canonical node-discovery helper
    shared by deploy_pods and deploy_coordinator.
    """
    try:
        out = subprocess.check_output(
            ['kubectl', 'get', 'nodes', '-o', 'json'],
            text=True, timeout=timeout)
    except FileNotFoundError:
        print('ERROR: kubectl not found', file=sys.stderr)
        return []
    except (subprocess.CalledProcessError, subprocess.TimeoutExpired) as e:
        print(f'ERROR: kubectl get nodes failed: {e}', file=sys.stderr)
        return []

    nodes = []
    for item in json.loads(out).get('items', []):
        for addr in item.get('status', {}).get('addresses', []):
            if addr.get('type') == 'InternalIP':
                nodes.append({
                    'ip': addr.get('address', ''),
                    'name': item.get('metadata', {}).get('name', ''),
                })
                break
    nodes.sort(key=lambda n: n['name'])
    return nodes


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


def upload_procmon(pod, namespace, remote_dir='/tmp', timeout=DEFAULT_TIMEOUT):
    """Upload procmon.py to pod."""
    script_dir = os.path.dirname(os.path.abspath(__file__))
    procmon_src = os.path.join(script_dir, 'procmon.py')
    if not os.path.exists(procmon_src):
        procmon_src = os.path.join(script_dir, 'tools', 'procmon.py')
    if not os.path.exists(procmon_src):
        return False
    try:
        kubectl_exec(pod['name'], namespace, f'mkdir -p {remote_dir}',
                     check=False, timeout=timeout)
        kubectl_cp_to(pod['name'], namespace, procmon_src,
                      f'{remote_dir}/procmon.py', timeout=timeout)
        return True
    except Exception:
        return False


def start_procmon(pod, namespace, target_pid, remote_dir='/tmp',
                  interval=1, timeout=30):
    """Start procmon monitoring for a service process.

    Uses procmon.py --background for proper daemonization (os.fork +
    os.setsid). The parent prints the child PID to stdout and exits,
    so kubectl exec returns immediately. The child runs in a new session,
    fully detached from the kubectl exec session.
    """
    cmd = (f'cd {remote_dir} && '
           f'python3 procmon.py --pid {target_pid} -i {interval} '
           f'--output resource_monitor.log --background')
    try:
        result = kubectl_exec(pod['name'], namespace, cmd,
                               check=False, timeout=timeout)
        pid = result.stdout.strip()
        if pid and pid.isdigit():
            return pid
        return None
    except Exception:
        return None


def resolve_procmon_dir(config_template, remote_config):
    """Resolve procmon output dir from config log_dir, fallback to remote_config dir.

    log_dir may be a {"value": ...} dict (dscli config style) or a plain
    string; empty/missing falls back to the directory holding remote_config.
    """
    log_dir_entry = config_template.get('log_dir', {})
    if isinstance(log_dir_entry, dict):
        procmon_dir = log_dir_entry.get('value', None)
    else:
        procmon_dir = log_dir_entry or None
    if not procmon_dir:
        procmon_dir = os.path.dirname(remote_config)
    return procmon_dir


def find_pid_by_port(pod, namespace, port, process_name, timeout=DEFAULT_TIMEOUT):
    """Find service PID by listening port, falling back to pgrep on process name."""
    result = kubectl_exec(pod['name'], namespace,
        f'ss -tlnp \'sport = :{port}\' 2>/dev/null | grep -oP \'pid=\\K[0-9]+\' | head -1',
        check=False, timeout=timeout)
    if result.returncode == 0 and result.stdout.strip():
        return result.stdout.strip().split('\n')[0]
    result = kubectl_exec(pod['name'], namespace,
        f'pgrep -f {process_name} | head -1', check=False, timeout=timeout)
    if result.returncode == 0 and result.stdout.strip():
        return result.stdout.strip().split('\n')[0]
    return None


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


def check_process(pod, namespace, process_name, timeout=DEFAULT_TIMEOUT):
    """Check if a service process is alive in a single pod.

    Returns (pod, status, detail) where status is 'alive'/'dead'/'error'.
    Uses ps + grep -v grep to exclude grep itself (handles paths like
    /usr/local/lib64/python3.11/site-packages/yr/datasystem/datasystem_worker).
    """
    pod_name = pod['name']
    try:
        result = kubectl_exec(pod_name, namespace,
            f'ps aux | grep "{process_name}" | grep -v grep | wc -l',
            check=False, timeout=timeout)
    except subprocess.TimeoutExpired:
        return (pod, 'error', 'timeout')
    if result.returncode != 0:
        return (pod, 'error', result.stderr.strip())
    count = int(result.stdout.strip())
    return (pod, 'alive' if count > 0 else 'dead', count)


def kill_process(pod, namespace, process_name, timeout=DEFAULT_TIMEOUT):
    """Force kill a service process and procmon processes in a single pod."""
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


def stop_service(pod, namespace, remote_config, timeout=DEFAULT_TIMEOUT,
                 service_type='worker'):
    """Stop a service gracefully using dscli stop.

    Uses -W (worker_config_path) or -C (coordinator_config_path) depending on
    service_type, matching the dscli stop argument changes in 42d8950d.
    """
    pod_name = pod['name']
    pod_ip = pod['ip']
    flag = '-C' if service_type == 'coordinator' else '-W'
    try:
        kubectl_exec(pod_name, namespace, f'dscli stop {flag} {remote_config} -t {timeout}',
                      timeout=timeout)
        print(f'  {pod_name} ({pod_ip}) -> stopped')
        return True
    except subprocess.TimeoutExpired:
        print(f'  {pod_name} ({pod_ip}) -> FAILED: timeout')
        return False
    except subprocess.CalledProcessError as e:
        print(f'  {pod_name} ({pod_ip}) -> FAILED: '
              f'{e.stderr.strip() if e.stderr else "unknown"}')
        return False
    except Exception as e:
        print(f'  {pod_name} ({pod_ip}) -> FAILED: {e}')
        return False


def find_default_whl():
    """Find the default whl file in ../../output directory.

    The whl installs both datasystem_worker and datasystem_coordinator
    (setup.py SERVICE_BINARIES), so this helper is role-agnostic.
    """
    output_dir = os.path.join(os.path.dirname(__file__), '..', '..', 'output')
    pattern = os.path.join(output_dir, 'openyuanrong_datasystem-*.whl')
    matches = glob.glob(pattern)
    if matches:
        return sorted(matches)[-1]
    return ''


def install_whl(pod, namespace, whl_path, timeout=DEFAULT_TIMEOUT):
    """Install the datasystem whl package in a single pod.

    Copies the whl into /tmp inside the pod, runs
    ``pip3 install --force-reinstall``, and removes the remote whl in a
    finally block so a failure does not leave stale files behind.
    """
    pod_name = pod['name']
    pod_ip = pod['ip']
    remote_whl = f'/tmp/{os.path.basename(whl_path)}'

    try:
        print(f'  {pod_name} ({pod_ip}) -> copying whl...')
        kubectl_cp_to(pod_name, namespace, whl_path, remote_whl, timeout=timeout)

        install_cmd = f'pip3 install --force-reinstall {remote_whl}'
        kubectl_exec(pod_name, namespace, install_cmd, timeout=timeout)
        print(f'  {pod_name} ({pod_ip}) -> whl installed successfully')
        return True

    except subprocess.TimeoutExpired:
        print(f'  {pod_name} ({pod_ip}) -> FAILED: timeout')
        return False
    except subprocess.CalledProcessError as e:
        print(f'  {pod_name} ({pod_ip}) -> FAILED: '
              f'{e.stderr.strip() if e.stderr else "unknown error"}')
        return False
    except Exception as e:
        print(f'  {pod_name} ({pod_ip}) -> FAILED: {e}')
        return False
    finally:
        try:
            kubectl_exec(pod_name, namespace, f'rm -f {remote_whl}',
                         check=False, timeout=10)
        except Exception:
            pass


def start_service(pod, namespace, config, remote_config, port, process_name,
                  enable_procmon=True, procmon_remote_dir='/tmp',
                  numactl_opts=None, timeout=DEFAULT_TIMEOUT):
    """Start a datasystem service in a single pod.

    The caller is responsible for injecting the per-pod listening address
    into ``config`` (worker_address for workers, coordinator_address for
    coordinators) before calling this function.

    Writes the config to a temp file, copies it into the pod, runs
    ``dscli start -f <remote_config>`` for workers or
    ``dscli start -C <remote_config>`` for coordinators (optionally with
    numactl options appended for the worker), then attaches procmon to the
    started process. The role is selected from ``process_name``: dscli's
    ``-f`` flag binds to ``worker_config_path`` and ``-C`` binds to
    ``coordinator_config_path``, so a coordinator must not be started with
    ``-f`` (dscli would treat it as a worker config).
    """
    pod_name = pod['name']
    pod_ip = pod['ip']

    with tempfile.NamedTemporaryFile(
        mode='w', suffix='.json', prefix=f'service_{pod_name}_',
        delete=False
    ) as tf:
        json.dump(config, tf, indent=2)
        tmp_path = tf.name

    try:
        kubectl_cp_to(pod_name, namespace, tmp_path, remote_config, timeout=timeout)
        is_coordinator = process_name == 'datasystem_coordinator'
        config_flag = '-C' if is_coordinator else '-f'
        cmd = f'dscli start {config_flag} {remote_config}'
        if numactl_opts and not is_coordinator:
            cmd += f' {numactl_opts}'
        kubectl_exec(pod_name, namespace, cmd, timeout=timeout)
        print(f'  {pod_name} ({pod_ip}) -> started')

        if enable_procmon:
            if upload_procmon(pod, namespace, procmon_remote_dir, timeout):
                import time
                time.sleep(1)
                pid = find_pid_by_port(pod, namespace, port, process_name, timeout)
                if pid:
                    procmon_pid = start_procmon(pod, namespace, pid,
                                                 procmon_remote_dir)
                    if procmon_pid:
                        print(f'  {pod_name} ({pod_ip}) -> procmon started '
                              f'(pid={procmon_pid}, monitoring {process_name} '
                              f'pid={pid})')
                    else:
                        print(f'  {pod_name} ({pod_ip}) -> procmon start failed')
                else:
                    print(f'  {pod_name} ({pod_ip}) -> procmon skipped: '
                          f'{process_name} pid not found')
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


def collect_logs_from_pod(pod, namespace, log_dir, local_dir,
                          remote_config_dir=None, timeout=DEFAULT_TIMEOUT):
    """Collect log files from a single pod."""
    pod_name = pod['name']
    pod_ip = pod['ip']
    local_pod_dir = os.path.join(local_dir, f'{pod_name}')
    os.makedirs(local_pod_dir, exist_ok=True)

    try:
        ls_result = kubectl_exec(pod_name, namespace,
            f'ls -d {log_dir} 2>/dev/null', check=False, timeout=timeout)
        if ls_result.returncode != 0:
            print(f'  {pod_name} ({pod_ip}) -> log dir {log_dir} does not exist')
            return True

        ls_result = kubectl_exec(pod_name, namespace,
            f'ls {log_dir}/*.log {log_dir}/*.log.gz {log_dir}/*.txt 2>/dev/null',
            check=False, timeout=timeout)
        log_files = [f.strip() for f in (ls_result.stdout or '').splitlines()
                     if f.strip()]

        if not log_files:
            print(f'  {pod_name} ({pod_ip}) -> no log files found')
            return True

        print(f'  {pod_name} ({pod_ip}) -> found {len(log_files)} log files')

        # Collect each log file using base64 to safely transfer
        # binary/non-UTF-8 content. kubectl_exec uses text=True which fails
        # on non-UTF-8 bytes in log files.
        for remote_path in log_files:
            try:
                fname = os.path.basename(remote_path)
                local_path = os.path.join(local_pod_dir, fname)
                result = kubectl_exec(pod_name, namespace,
                    f'base64 {remote_path}', check=True, timeout=timeout)
                content = base64.b64decode(result.stdout)
                with open(local_path, 'wb') as f:
                    f.write(content)
            except Exception as e:
                print(f'    {os.path.basename(remote_path)} -> FAILED: {e}')

        # Collect procmon resource_monitor.log from remote_config_dir
        # (start's fallback) and from log_dir (when config had log_dir from
        # the start). This covers both scenarios: log_dir injected via --set
        # (procmon in remote_config_dir) and log_dir in original config
        # (procmon in log_dir).
        procmon_dirs = set()
        if remote_config_dir:
            procmon_dirs.add(remote_config_dir)
        if log_dir:
            procmon_dirs.add(log_dir)
        glob_dirs = {os.path.dirname(f) for f in log_files}
        for pdir in procmon_dirs:
            if pdir in glob_dirs:
                continue
            procmon_log = f'{pdir}/resource_monitor.log'
            try:
                result = kubectl_exec(pod_name, namespace,
                    f'base64 {procmon_log}', check=True, timeout=timeout)
                content = base64.b64decode(result.stdout)
                local_path = os.path.join(local_pod_dir,
                                           'resource_monitor.log')
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


def clean_pod(pod, namespace, log_dir, remote_config_dir, process_name,
              timeout=DEFAULT_TIMEOUT):
    """Kill the service process and clean logs in a single pod."""
    pod_name = pod['name']
    pod_ip = pod['ip']
    try:
        kill_process(pod, namespace, process_name, timeout=timeout)

        if log_dir:
            kubectl_exec(pod_name, namespace, f'rm -rf {log_dir}',
                         check=False, timeout=timeout)
        kubectl_exec(pod_name, namespace,
            f'rm -f {remote_config_dir}/resource_monitor.log',
            check=False, timeout=timeout)

        print(f'  {pod_name} ({pod_ip}) -> OK')
        return True
    except subprocess.TimeoutExpired:
        print(f'  {pod_name} ({pod_ip}) -> FAILED: timeout')
        return False
    except Exception as e:
        print(f'  {pod_name} ({pod_ip}) -> FAILED: {e}')
        return False


def read_remote_log_dir(namespace, pods, remote_config, timeout=DEFAULT_TIMEOUT):
    """Read log_dir from the first pod's remote config.

    Returns (log_dir_or_None, config_dict_or_empty). log_dir may be a
    {"value": ...} dict (dscli config style) or a plain string. On any
    failure (no pods, cat fails, JSON parse fails), returns (None, {}).
    """
    if not pods:
        return None, {}
    try:
        result = kubectl_exec(pods[0]['name'], namespace,
            f'cat {remote_config}', check=True, timeout=timeout)
        config = json.loads(result.stdout)
    except Exception as e:
        print(f'WARNING: Failed to read remote config from pod: {e}',
              file=sys.stderr)
        return None, {}

    log_dir_entry = config.get('log_dir', {})
    if isinstance(log_dir_entry, dict):
        log_dir = log_dir_entry.get('value', None)
    else:
        log_dir = log_dir_entry or None
    return log_dir, config


def parse_config_override(value):
    """Parse a --set key=value override's value into a typed Python value.

    Recognized: true/false -> bool, null/none -> None, integers, floats
    (presence of '.'), otherwise the raw string. Values containing '=' are
    kept as raw strings (the part after the first '=' is the value).
    """
    value = value.strip()
    if value.lower() == 'true':
        return True
    if value.lower() == 'false':
        return False
    if value.lower() in ('null', 'none'):
        return None
    try:
        if '.' in value:
            return float(value)
        return int(value)
    except ValueError:
        return value


def apply_config_overrides(config_template, overrides):
    """Apply --set key=value overrides onto a config template in place.

    Each override becomes config_template[key] = {"value": parsed}. Invalid
    overrides (no '=') are warned and skipped. The dscli config style wraps
    values in {"value": ...}; that wrapper is what dscli start/stop expect.
    """
    print('\nApplying config overrides:')
    for override in overrides or []:
        if '=' not in override:
            print(f'  WARNING: Ignoring invalid override: {override}')
            continue
        key, value = override.split('=', 1)
        key = key.strip()
        value = value.strip()
        parsed_value = parse_config_override(value)
        config_template[key] = {"value": parsed_value}
        print(f'  Set {key}.value = {parsed_value}')


# --- Shared subcommand implementations ---
# These wrap the per-pod primitives with a thread pool and a result summary.
# Role files supply role-specific labels / process names / defaults and call
# these so the orchestration logic is not duplicated.


def cmd_exec_impl(pods, namespace, cmd, timeout=DEFAULT_TIMEOUT):
    """Execute a shell command in every pod in parallel."""
    def do_op(pod):
        pod_name = pod['name']
        pod_ip = pod['ip']
        try:
            result = kubectl_exec(pod_name, namespace, cmd,
                                   check=False, timeout=timeout)
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


def cmd_check_impl(pods, namespace, process_name, label, timeout=DEFAULT_TIMEOUT):
    """Check service processes across all pods and print a summary.

    Always returns 0; check is non-fatal. Tally: alive / dead / error.
    """
    print(f'\nChecking {label} ({process_name})...')
    results = []
    with ThreadPoolExecutor(max_workers=len(pods)) as pool:
        futures = {pool.submit(check_process, pod, namespace, process_name,
                               timeout): pod for pod in pods}
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


def cmd_stop_impl(pods, namespace, remote_config, label, timeout=DEFAULT_TIMEOUT,
                  service_type='worker'):
    """Stop services gracefully using dscli across all pods."""
    def do_op(pod):
        return stop_service(pod, namespace, remote_config, timeout, service_type)
    return do_for_all_pods(pods, do_op, f'Stopping {label}')


def cmd_kill_impl(pods, namespace, process_name, label, timeout=DEFAULT_TIMEOUT):
    """Force kill service processes across all pods."""
    def do_op(pod):
        return kill_process(pod, namespace, process_name, timeout)
    return do_for_all_pods(pods, do_op, f'Killing {label}')


def cmd_collect_impl(pods, namespace, remote_config, output_dir, label,
                     timeout=DEFAULT_TIMEOUT):
    """Collect service logs from all pods."""
    log_dir, _ = read_remote_log_dir(namespace, pods, remote_config, timeout)
    if not log_dir:
        print('ERROR: log_dir not found in remote config', file=sys.stderr)
        return 1

    remote_config_dir = os.path.dirname(remote_config)
    print(f'Using log directory from remote config: {log_dir}')
    local_dir = output_dir

    def do_op(pod):
        return collect_logs_from_pod(pod, namespace, log_dir, local_dir,
                                     remote_config_dir=remote_config_dir,
                                     timeout=timeout)
    return do_for_all_pods(pods, do_op, f'Collecting {label}')


def cmd_clean_impl(pods, namespace, remote_config, process_name, label,
                    timeout=DEFAULT_TIMEOUT):
    """Kill service processes and clean log directories across all pods."""
    log_dir, _ = read_remote_log_dir(namespace, pods, remote_config, timeout)
    remote_config_dir = os.path.dirname(remote_config)

    def do_op(pod):
        return clean_pod(pod, namespace, log_dir, remote_config_dir,
                         process_name, timeout=timeout)
    return do_for_all_pods(pods, do_op, f'Cleaning {label}')


def cmd_install_impl(pods, namespace, whl, timeout=DEFAULT_TIMEOUT):
    """Install the datasystem whl package across all pods.

    Validates the local whl path exists before dispatching per-pod installs.
    """
    if not os.path.exists(whl):
        print(f'ERROR: whl file not found: {whl}', file=sys.stderr)
        return 1

    def do_op(pod):
        return install_whl(pod, namespace, whl, timeout)
    return do_for_all_pods(pods, do_op, 'Installing whl')


# ============================================================================
# Standalone mode helpers (coordinator_test / worker_test binary)
# ============================================================================

def kubectl_exec_raw(pod, namespace, cmd, timeout=DEFAULT_TIMEOUT):
    """Execute a command in a pod, return stdout string."""
    full = ['kubectl', 'exec', '-n', namespace, pod['name'], '--', 'bash', '-c', cmd]
    try:
        r = subprocess.run(full, capture_output=True, text=True, timeout=timeout)
        return r.stdout if r.returncode == 0 else ''
    except Exception:
        return ''


def install_binary(pod, namespace, local_binary, remote_dir,
                    timeout=DEFAULT_TIMEOUT):
    """Copy a standalone binary to a pod (no lib directory needed -- the whl
    package provides all .so files via pip3 install).

    Caller must ensure whl is already installed (via install_whl) before
    calling this; the standalone binary's LD_LIBRARY_PATH is set to the
    whl-installed lib directory at start time.
    """
    if not os.path.exists(local_binary):
        print(f'ERROR: binary not found: {local_binary}', file=sys.stderr)
        return False
    name = pod['name']
    subprocess.run(['kubectl', 'exec', '-n', namespace, name, '--', 'mkdir', '-p', remote_dir],
                    timeout=timeout)
    subprocess.run(['kubectl', 'cp', '-n', namespace, local_binary, f'{name}:{remote_dir}/'],
                    timeout=timeout)
    subprocess.run(['kubectl', 'exec', '-n', namespace, name, '--', 'chmod', '+x',
                     f'{remote_dir}/{os.path.basename(local_binary)}'], timeout=timeout)
    return True


def find_whl_lib_dir(pod, namespace, timeout=DEFAULT_TIMEOUT):
    """Find the lib directory inside the pip-installed datasystem package.

    The whl installs .so files under yr/datasystem/lib/ inside the Python
    site-packages directory. Returns the path (e.g.
    /usr/local/lib64/python3.11/site-packages/yr/datasystem/lib) or '' if not
    found.
    """
    # Primary: use Python introspection to locate the package directory
    cmd = ('python3 -c "import yr.datasystem, os; '
           'print(os.path.join(os.path.dirname(yr.datasystem.__file__), \'lib\'))" 2>/dev/null')
    result = kubectl_exec_raw(pod, namespace, cmd, timeout=timeout)
    if result and result.strip() and os.path.basename(result.strip()) == 'lib':
        return result.strip()
    # Fallback: find via pip show -- works even if yr.datasystem can't be
    # imported (e.g. broken .pyc, missing __init__.py, wrong python version)
    cmd2 = 'pip3 show -f openyuanrong-datasystem 2>/dev/null | grep "yr/datasystem/lib$" | head -1'
    rel_path = kubectl_exec_raw(pod, namespace, cmd2, timeout=timeout)
    if rel_path and rel_path.strip():
        cmd3 = 'python3 -c "import site; print(site.getsitepackages()[0])" 2>/dev/null'
        sp = kubectl_exec_raw(pod, namespace, cmd3, timeout=timeout)
        if sp and sp.strip():
            return f'{sp.strip()}/{rel_path.strip()}'
    return ''


def start_service_standalone(pod, namespace, binary_name, remote_dir, config_path,
                              jf_addr, service_name, extra_args='',
                              config=None,
                              enable_procmon=True, procmon_remote_dir='/tmp',
                              port=None, process_name=None,
                              timeout=DEFAULT_TIMEOUT):
    """Start a standalone test binary in a pod.

    ``config`` is the per-pod config dict built by the caller. It is written
    to ``config_path`` inside the pod via ``kubectl cp`` before launch.
    ``port`` and ``process_name`` are used for procmon PID lookup (same logic
    as ``start_service`` dscli path).
    """
    name = pod['name']
    pod_ip = pod['ip']
    # Find the whl-installed lib directory for LD_LIBRARY_PATH
    whl_lib_dir = find_whl_lib_dir(pod, namespace, timeout)
    if config is not None:
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json',
                                         prefix=f'standalone_{name}_',
                                         delete=False) as tf:
            json.dump(config, tf, indent=2)
            tmp_cfg = tf.name
        try:
            kubectl_cp_to(name, namespace, tmp_cfg, config_path, timeout=timeout)
        finally:
            os.unlink(tmp_cfg)
    # nohup + </dev/null detaches stdin; echo $! makes shell print PID and exit.
    # LD_LIBRARY_PATH points at the whl-installed lib dir so the standalone
    # binary finds libdatasystem_worker.so / libdatasystem_coordinator.so etc.
    ld_path = whl_lib_dir or f'{remote_dir}/lib'
    cmd = (f'cd {remote_dir} && '
           f'LD_LIBRARY_PATH={ld_path}:$LD_LIBRARY_PATH nohup ./{binary_name} '
           f'--config {config_path} --jf {jf_addr} --service {service_name} {extra_args} '
           f'> {remote_dir}/stdout.log 2>&1 </dev/null & '
           f'echo $!')
    try:
        subprocess.run(
            ['kubectl', 'exec', '-n', namespace, name, '--', 'sh', '-c', cmd],
            capture_output=True, text=True, timeout=10)
    except subprocess.TimeoutExpired:
        pass
    # Verify process started (mirrors deploy_client.py pgrep check)
    import time
    time.sleep(1)
    pid = None
    if port and process_name:
        pid = find_pid_by_port(pod, namespace, port, process_name, timeout)
    else:
        verify = kubectl_exec_raw({'name': name}, namespace,
                                  f'pgrep -f {binary_name}', timeout=10)
        if verify and verify.strip():
            pid = verify.strip().split('\n')[0]
    if not pid:
        print(f'  {name} ({pod_ip}) -> FAILED: process not found')
        return False
    print(f'  {name} ({pod_ip}) -> started (pid={pid})')
    # Attach procmon (same logic as start_service dscli path)
    if enable_procmon:
        if upload_procmon(pod, namespace, procmon_remote_dir, timeout):
            procmon_pid = start_procmon(pod, namespace, pid, procmon_remote_dir)
            if procmon_pid:
                print(f'  {name} ({pod_ip}) -> procmon started '
                      f'(pid={procmon_pid}, monitoring pid={pid})')
            else:
                print(f'  {name} ({pod_ip}) -> procmon start failed')
        else:
            print(f'  {name} ({pod_ip}) -> procmon skipped: upload failed')
    return True


def stop_service_standalone(pod, namespace, process_name, timeout=DEFAULT_TIMEOUT):
    """Stop a standalone test binary via SIGTERM."""
    name = pod['name']
    r = subprocess.run(['kubectl', 'exec', '-n', namespace, name, '--', 'bash', '-c',
                        f'pkill -TERM -f {process_name} 2>/dev/null || true'],
                       capture_output=True, text=True, timeout=timeout)
    import time
    time.sleep(3)
    return True


# ============================================================================
# Shared command implementations (used by deploy_coordinator + deploy_worker)
# ============================================================================

def _print_timings(action, timings):
    """Print per-pod duration stats for a start/stop action.

    ``timings`` is a list of ``(pod_name, elapsed_seconds, succeeded)``
    tuples populated from worker threads (list.append is GIL-atomic in
    CPython, so concurrent appends from the thread pool are safe).
    """
    if not timings:
        return
    print(f'\n{action} per-pod timings:')
    for pod_name, elapsed, ok in sorted(timings, key=lambda x: x[0]):
        print(f'  {pod_name:<40} {elapsed:7.2f}s  {"OK" if ok else "FAIL"}')
    elapsed_all = [t for _, t, _ in timings]
    ok_count = sum(1 for _, _, ok in timings if ok)
    fail_count = len(timings) - ok_count
    print(f'  min={min(elapsed_all):.2f}s  max={max(elapsed_all):.2f}s  '
          f'avg={sum(elapsed_all) / len(elapsed_all):.2f}s  '
          f'total={sum(elapsed_all):.2f}s  '
          f'(succeeded={ok_count}, failed={fail_count})')


def cmd_exec_shared(args, pods, timeout=DEFAULT_TIMEOUT):
    """Execute command in pods."""
    return cmd_exec_impl(pods, args.namespace, args.cmd, timeout)


def cmd_collect_shared(args, pods, label, timeout=DEFAULT_TIMEOUT):
    """Collect service logs from pods."""
    return cmd_collect_impl(pods, args.namespace, args.remote_config,
                            args.output, label, timeout)


def cmd_clean_shared(args, pods, process_name, label, timeout=DEFAULT_TIMEOUT):
    """Kill service processes and clean log directories."""
    return cmd_clean_impl(pods, args.namespace, args.remote_config,
                           process_name, label, timeout)


def cmd_kill_shared(args, pods, process_name_standalone, label,
                    timeout=DEFAULT_TIMEOUT):
    """Force kill service processes across all pods."""
    proc = (process_name_standalone if getattr(args, 'standalone', False)
            else args.process)
    return cmd_kill_impl(pods, args.namespace, proc, label, timeout)


def cmd_install_shared(args, pods, process_name_standalone, label,
                        script_dir, timeout=DEFAULT_TIMEOUT):
    """Install whl first (both modes need .so deps), then optionally copy
    standalone binary on top."""
    whl_rc = cmd_install_impl(pods, args.namespace, args.whl, timeout)
    if whl_rc != 0:
        return whl_rc
    if getattr(args, 'standalone', False):
        binary = args.binary or os.path.join(
            script_dir, 'output', process_name_standalone)
        def do_op(pod):
            return install_binary(pod, args.namespace, binary,
                                  args.remote_dir, timeout)
        return do_for_all_pods(pods, do_op, f'Installing {label} binary (standalone)')
    return 0


def cmd_stop_shared(args, pods, process_name_standalone, label,
                    service_type='worker', with_timings=False,
                    timeout=DEFAULT_TIMEOUT):
    """Stop service gracefully. Standalone mode uses SIGTERM + do_for_all_pods;
    non-standalone uses dscli stop."""
    if getattr(args, 'standalone', False):
        timings = []
        def do_op(pod):
            import time as _time
            t0 = _time.monotonic()
            ok = False
            try:
                ok = stop_service_standalone(pod, args.namespace,
                                             process_name_standalone, timeout)
                return ok
            finally:
                elapsed = _time.monotonic() - t0
                timings.append((pod['name'], elapsed, bool(ok)))
        rc = do_for_all_pods(pods, do_op, f'Stopping {label} (standalone)')
        if with_timings:
            _print_timings('stop', timings)
        return rc

    if with_timings:
        import time as _time
        timings = []
        def do_op(pod):
            t0 = _time.monotonic()
            ok = False
            try:
                ok = stop_service(pod, args.namespace, args.remote_config,
                                  timeout=timeout)
                return ok
            finally:
                elapsed = _time.monotonic() - t0
                timings.append((pod['name'], elapsed, bool(ok)))
        rc = do_for_all_pods(pods, do_op, f'Stopping {label}')
        _print_timings('stop', timings)
        return rc

    return cmd_stop_impl(pods, args.namespace, args.remote_config,
                         label, timeout, service_type=service_type)
