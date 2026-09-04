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
import logging
import os
import subprocess
import sys
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor, as_completed


# ---------------------------------------------------------------------------
# Logging helpers
# ---------------------------------------------------------------------------
# Two-stream design preserves print()'s stdout/stderr split: log_info goes to
# stdout (visible in normal pipe captures), log_info/log_error go to
# stderr (visible even when stdout is redirected to /dev/null). Format is the
# raw message only — no timestamp, no level prefix — so existing callers and
# CI greps that parsed print() output keep working unchanged. setup_logging()
# in each deploy_*.main() flips to DEBUG when --verbose is set.

_stdout_logger = logging.getLogger('deploy.stdout')
_stderr_logger = logging.getLogger('deploy.stderr')
for _lg in (_stdout_logger, _stderr_logger):
    _lg.handlers = []
    _lg.propagate = False

_stdout_handler = logging.StreamHandler(sys.stdout)
_stdout_handler.setFormatter(logging.Formatter('%(message)s'))
_stdout_logger.addHandler(_stdout_handler)
_stdout_logger.setLevel(logging.INFO)

_stderr_handler = logging.StreamHandler(sys.stderr)
_stderr_handler.setFormatter(logging.Formatter('%(message)s'))
_stderr_logger.addHandler(_stderr_handler)
_stderr_logger.setLevel(logging.WARNING)


def setup_logging(verbose: bool = False) -> None:
    """Configure deploy loggers. Idempotent; safe to call multiple times.

    Optional; if a main() never calls this, log_info/warning/error still work
    using the default INFO/WARNING levels installed at import time. Pass
    verbose=True to lower both thresholds to DEBUG.
    """
    _stdout_logger.setLevel(logging.DEBUG if verbose else logging.INFO)
    _stderr_logger.setLevel(logging.DEBUG if verbose else logging.WARNING)


def log_info(msg, *args):
    """Info-level message to stdout. Drop-in for ``print(msg)``."""
    if args:
        _stdout_logger.info(msg, *args)
    else:
        _stdout_logger.info(msg)


def log_error(msg, *args):
    """Error-level message to stderr. Drop-in for ``print(msg, file=sys.stderr)``."""
    if args:
        _stderr_logger.error(msg, *args)
    else:
        _stderr_logger.error(msg)


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
        log_error('ERROR: kubectl not found')
        sys.exit(1)
    except subprocess.CalledProcessError as e:
        log_error(f'ERROR: kubectl failed: {e.stderr}')
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
            log_error(f'WARNING: prefix "{p}" matched 0 pods')
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
        log_error('ERROR: kubectl not found')
        return []
    except (subprocess.CalledProcessError, subprocess.TimeoutExpired) as e:
        log_error(f'ERROR: kubectl get nodes failed: {e}')
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
    """Copy local file to pod. Raises RuntimeError on failure so callers can
    catch a single exception type (CalledProcessError/TimeoutExpired both
    bubble up raw otherwise and crash the whole batch)."""
    r = subprocess.run(
        ['kubectl', 'cp', '-n', namespace, src, f'{pod}:{dst}'],
        capture_output=True, text=True, timeout=timeout)
    if r.returncode != 0:
        raise RuntimeError(
            f'kubectl cp to {pod} failed: {(r.stderr or r.stdout).strip()}')


def upload_procmon(pod, namespace, remote_dir='/tmp', timeout=DEFAULT_TIMEOUT):
    """Upload procmon.py to pod."""
    script_dir = os.path.dirname(os.path.abspath(__file__))
    procmon_src = os.path.join(script_dir, 'procmon.py')
    if not os.path.exists(procmon_src):
        procmon_src = os.path.join(script_dir, 'tools', 'procmon.py')
    if not os.path.exists(procmon_src):
        return False
    # Retried like upload_launcher: a transient cp failure under heavy
    # concurrency would silently skip procmon monitoring for this pod.
    for attempt in range(3):
        try:
            kubectl_exec(pod['name'], namespace, f'mkdir -p {remote_dir}',
                         check=False, timeout=timeout)
            kubectl_cp_to(pod['name'], namespace, procmon_src,
                          f'{remote_dir}/procmon.py', timeout=timeout)
            return True
        except Exception:
            if attempt == 2:
                return False
            time.sleep(5 * (attempt + 1))
    return False


def upload_launcher(pod, namespace, remote_dir='/tmp', timeout=DEFAULT_TIMEOUT):
    """Upload standalone_launcher.py to pod.

    Returns the remote path on success (so callers know where to invoke it),
    or ``None`` on failure. Mirrors ``upload_procmon``: same script
    discovery, same upload mechanism. Kept separate from procmon because
    the launcher's responsibility (start a binary detached + readiness poll)
    is distinct from procmon's (resource monitoring). The upload is retried
    twice with backoff: under 500-way concurrency a transient cp failure
    (Rancher impersonation InternalError / timeout) would otherwise silently
    downgrade this pod to the slow nohup launch path, losing the readiness
    wait.
    """
    script_dir = os.path.dirname(os.path.abspath(__file__))
    launcher_src = os.path.join(script_dir, 'standalone_launcher.py')
    if not os.path.exists(launcher_src):
        launcher_src = os.path.join(script_dir, 'tools',
                                    'standalone_launcher.py')
    if not os.path.exists(launcher_src):
        return None
    remote_path = f'{remote_dir}/standalone_launcher.py'
    for attempt in range(3):
        try:
            kubectl_exec(pod['name'], namespace, f'mkdir -p {remote_dir}',
                         check=False, timeout=timeout)
            kubectl_cp_to(pod['name'], namespace, launcher_src,
                          remote_path, timeout=timeout)
            return remote_path
        except Exception:
            if attempt == 2:
                return None
            time.sleep(5 * (attempt + 1))
    return None


def start_procmon(pod, namespace, target_pid, remote_dir='/tmp',
                  interval=1, timeout=30, port=None):
    """Start procmon monitoring for a service process.

    Uses procmon.py --background for proper daemonization (os.fork +
    os.setsid). The parent prints the child PID to stdout and exits,
    so kubectl exec returns immediately. The child runs in a new session,
    fully detached from the kubectl exec session.

    When ``port`` is set, procmon additionally monitors inbound/outbound
    byte throughput on the service's listening port via NETLINK_INET_DIAG
    (queries kernel tcp_info directly — no external binary like ss needed,
    works in slim containers). Aggregates bytes_sent/bytes_received across
    all ESTABLISHED sockets on the port, diffs per sample → BytesIn/s +
    BytesOut/s). Useful for coordinator monitoring where traffic on the
    listening port shows worker connection activity.
    """
    cmd = (f'cd {remote_dir} && '
           f'python3 procmon.py --pid {target_pid} -i {interval} '
           f'--output resource_monitor.log --background')
    if port:
        cmd += f' --port {port}'
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


def do_for_all_pods(pods, do_op, desc, max_workers=None):
    """Execute operation for all pods in parallel."""
    log_info(f'\n{desc}...')
    results = []
    workers = max_workers or len(pods)
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {pool.submit(do_op, pod): pod for pod in pods}
        for future in as_completed(futures):
            results.append(future.result())

    ok = sum(1 for r in results if r)
    log_info(f'\nResult: {ok}/{len(results)} succeeded')
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
                     f'pkill -9 -f {process_name}; '
                     f'pkill -9 -f procmon.py',
                     check=False, timeout=timeout)
        log_info(f'  {pod_name} ({pod_ip}) -> killed')
        return True
    except subprocess.TimeoutExpired:
        log_info(f'  {pod_name} ({pod_ip}) -> FAILED: timeout')
        return False
    except Exception as e:
        log_info(f'  {pod_name} ({pod_ip}) -> FAILED: {e}')
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
        log_info(f'  {pod_name} ({pod_ip}) -> stopped')
        return True
    except subprocess.TimeoutExpired:
        log_info(f'  {pod_name} ({pod_ip}) -> FAILED: timeout')
        return False
    except subprocess.CalledProcessError as e:
        log_info(f'  {pod_name} ({pod_ip}) -> FAILED: '
                 f'{e.stderr.strip() if e.stderr else "unknown"}')
        return False
    except Exception as e:
        log_info(f'  {pod_name} ({pod_ip}) -> FAILED: {e}')
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
        log_info(f'  {pod_name} ({pod_ip}) -> copying whl...')
        kubectl_cp_to(pod_name, namespace, whl_path, remote_whl, timeout=timeout)

        install_cmd = f'pip3 install --force-reinstall {remote_whl}'
        kubectl_exec(pod_name, namespace, install_cmd, timeout=timeout)
        log_info(f'  {pod_name} ({pod_ip}) -> whl installed successfully')
        return True

    except subprocess.TimeoutExpired:
        log_info(f'  {pod_name} ({pod_ip}) -> FAILED: timeout')
        return False
    except subprocess.CalledProcessError as e:
        log_info(f'  {pod_name} ({pod_ip}) -> FAILED: '
                 f'{e.stderr.strip() if e.stderr else "unknown error"}')
        return False
    except Exception as e:
        log_info(f'  {pod_name} ({pod_ip}) -> FAILED: {e}')
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
        # Time only the actual launch (dscli start). Config upload, pid
        # verify, and procmon attach are excluded — caller reads
        # pod['_start_elapsed'] to record the start stopwatch.
        import time
        t_start = time.monotonic()
        try:
            kubectl_exec(pod_name, namespace, cmd, timeout=timeout)
        finally:
            pod['_start_elapsed'] = time.monotonic() - t_start
        log_info(f'  {pod_name} ({pod_ip}) -> started')

        if enable_procmon:
            if upload_procmon(pod, namespace, procmon_remote_dir, timeout):
                time.sleep(1)
                pid = find_pid_by_port(pod, namespace, port, process_name, timeout)
                if pid:
                    procmon_pid = start_procmon(pod, namespace, pid,
                                                procmon_remote_dir,
                                                port=port)
                    if procmon_pid:
                        log_info(f'  {pod_name} ({pod_ip}) -> procmon started '
                                 f'(pid={procmon_pid}, monitoring {process_name} '
                                 f'pid={pid})')
                    else:
                        log_info(f'  {pod_name} ({pod_ip}) -> procmon start failed')
                else:
                    log_info(f'  {pod_name} ({pod_ip}) -> procmon skipped: '
                             f'{process_name} pid not found')
            else:
                log_info(f'  {pod_name} ({pod_ip}) -> procmon skipped: upload failed')
        return True
    except subprocess.TimeoutExpired:
        log_info(f'  {pod_name} ({pod_ip}) -> FAILED: timeout')
        return False
    except subprocess.CalledProcessError as e:
        log_info(f'  {pod_name} ({pod_ip}) -> FAILED: {e.stderr.strip()}')
        return False
    finally:
        os.unlink(tmp_path)


def collect_logs_from_pod(pod, namespace, log_dir, local_dir,
                          remote_config_dir=None, remote_dir=None,
                          timeout=DEFAULT_TIMEOUT):
    """Collect log files from a single pod.

    Also collects stdout.log from ``remote_dir`` when that directory exists
    (standalone mode writes the binary's combined stdout+stderr to
    ``{remote_dir}/stdout.log``; ``start_service_standalone`` sets that
    path explicitly, so ``remote_dir`` -- not ``remote_config_dir`` -- is
    where the file actually lives). The directory-existence gate means
    dscli-mode collects (which never create ``remote_dir``) skip stdout.log
    silently without needing a ``--standalone`` flag: if the dir is absent
    there is nothing to collect.
    """
    pod_name = pod['name']
    pod_ip = pod['ip']
    local_pod_dir = os.path.join(local_dir, f'{pod_name}')
    os.makedirs(local_pod_dir, exist_ok=True)

    try:
        ls_result = kubectl_exec(pod_name, namespace,
                                 f'ls -d {log_dir} 2>/dev/null', check=False, timeout=timeout)
        if ls_result.returncode != 0:
            log_info(f'  {pod_name} ({pod_ip}) -> log dir {log_dir} does not exist')
            return True

        ls_result = kubectl_exec(pod_name, namespace,
                                 f'ls {log_dir}/*.log {log_dir}/*.log.gz {log_dir}/*.txt 2>/dev/null',
                                 check=False, timeout=timeout)
        log_files = [f.strip() for f in (ls_result.stdout or '').splitlines()
                     if f.strip()]

        if not log_files:
            log_info(f'  {pod_name} ({pod_ip}) -> no log files found')
            return True

        log_info(f'  {pod_name} ({pod_ip}) -> found {len(log_files)} log files')

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
                log_info(f'    {os.path.basename(remote_path)} -> FAILED: {e}')

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

        # Collect stdout.log from remote_dir (standalone mode writes the
        # binary's combined stdout+stderr to {remote_dir}/stdout.log, NOT
        # {remote_config_dir}/stdout.log -- start_service_standalone sets
        # log_path = f'{remote_dir}/stdout.log' explicitly). Gate on
        # `ls -d {remote_dir}` so a dscli-mode pod (which never creates
        # remote_dir) skips silently instead of erroring; this is what
        # lets the same `collect` subcommand serve both modes with no
        # --standalone flag. If the dir exists but stdout.log is absent
        # (binary hasn't written yet, or crashed before redirect), the
        # base64 call fails and we skip silently too.
        if remote_dir:
            ls_remote = kubectl_exec(pod_name, namespace,
                                     f'ls -d {remote_dir} 2>/dev/null',
                                     check=False, timeout=timeout)
            if ls_remote.returncode == 0:
                stdout_path = f'{remote_dir}/stdout.log'
                try:
                    result = kubectl_exec(pod_name, namespace,
                                          f'base64 {stdout_path}', check=True,
                                          timeout=timeout)
                    content = base64.b64decode(result.stdout)
                    local_path = os.path.join(local_pod_dir, 'stdout.log')
                    with open(local_path, 'wb') as f:
                        f.write(content)
                except Exception:
                    pass

        return True
    except subprocess.TimeoutExpired:
        log_info(f'  {pod_name} ({pod_ip}) -> FAILED: timeout')
        return False
    except Exception as e:
        log_info(f'  {pod_name} ({pod_ip}) -> FAILED: {e}')
        return False


def clean_pod(pod, namespace, log_dir, remote_config_dir, process_name,
              remote_dir=None, timeout=DEFAULT_TIMEOUT):
    """Kill the service process and clean logs in a single pod.

    ``remote_dir`` (standalone mode only) holds the standalone binary,
    ``lib/`` .so deps, and ``stdout.log``; when set it is removed entirely
    so a subsequent deploy starts from a clean state instead of stacking
    stale binaries, leftover .so variants, and appended stdout logs. When
    ``None`` (dscli mode), only ``log_dir`` and ``resource_monitor.log`` are
    touched -- the dscli install path installs into the package prefix, not
    ``remote_dir``, so there is nothing of the deploy's own to remove.
    """
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
        if remote_dir:
            kubectl_exec(pod_name, namespace, f'rm -rf {remote_dir}',
                         check=False, timeout=timeout)

        log_info(f'  {pod_name} ({pod_ip}) -> OK')
        return True
    except subprocess.TimeoutExpired:
        log_info(f'  {pod_name} ({pod_ip}) -> FAILED: timeout')
        return False
    except Exception as e:
        log_info(f'  {pod_name} ({pod_ip}) -> FAILED: {e}')
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
        log_error(f'WARNING: Failed to read remote config from pod: {e}')
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
    log_info('\nApplying config overrides:')
    for override in overrides or []:
        if '=' not in override:
            log_info(f'  WARNING: Ignoring invalid override: {override}')
            continue
        key, value = override.split('=', 1)
        key = key.strip()
        value = value.strip()
        parsed_value = parse_config_override(value)
        config_template[key] = {"value": parsed_value}
        log_info(f'  Set {key}.value = {parsed_value}')


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
            log_info(f'  {pod_name} ({pod_ip}) -> {"OK" if success else "FAILED"}')
            if result.stdout.strip():
                for line in result.stdout.strip().split('\n')[:5]:
                    log_info(f'    stdout: {line}')
            if result.stderr.strip():
                for line in result.stderr.strip().split('\n')[:5]:
                    log_info(f'    stderr: {line}')
            return success
        except subprocess.TimeoutExpired:
            log_info(f'  {pod_name} ({pod_ip}) -> TIMEOUT')
            return False
        except Exception as e:
            log_info(f'  {pod_name} ({pod_ip}) -> ERROR: {e}')
            return False

    return do_for_all_pods(pods, do_op, f'Executing command: {cmd}')


def cmd_check_impl(pods, namespace, process_name, label, timeout=DEFAULT_TIMEOUT):
    """Check service processes across all pods and print a summary.

    Always returns 0; check is non-fatal. Tally: alive / dead / error.
    """
    log_info(f'\nChecking {label} ({process_name})...')
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
            log_info(f'  {pod["name"]} ({pod["ip"]}) -> alive (count={detail})')
        elif status == 'dead':
            dead += 1
            log_info(f'  {pod["name"]} ({pod["ip"]}) -> dead')
        else:
            errors += 1
            log_info(f'  {pod["name"]} ({pod["ip"]}) -> error ({detail})')

    total = len(results)
    log_info(f'\nResult: {alive} alive / {dead} dead / {errors} error / {total} total')
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
                     remote_dir=None, timeout=DEFAULT_TIMEOUT):
    """Collect service logs from all pods.

    ``remote_dir`` (standalone mode) is where the binary's ``stdout.log``
    lives; ``collect_logs_from_pod`` gates on its existence so a None value
    (dscli mode, no ``--remote-dir`` passed) simply skips stdout.log
    collection.
    """
    log_dir, _ = read_remote_log_dir(namespace, pods, remote_config, timeout)
    if not log_dir:
        log_error('ERROR: log_dir not found in remote config')
        return 1

    remote_config_dir = os.path.dirname(remote_config)
    log_info(f'Using log directory from remote config: {log_dir}')
    local_dir = output_dir

    def do_op(pod):
        return collect_logs_from_pod(pod, namespace, log_dir, local_dir,
                                     remote_config_dir=remote_config_dir,
                                     remote_dir=remote_dir, timeout=timeout)
    return do_for_all_pods(pods, do_op, f'Collecting {label}')


def cmd_clean_impl(pods, namespace, remote_config, process_name, label,
                   remote_dir=None, timeout=DEFAULT_TIMEOUT):
    """Kill service processes and clean log directories across all pods.

    ``remote_dir`` (standalone mode) is removed entirely per pod to drop the
    standalone binary, ``lib/`` .so deps, and ``stdout.log``. ``None`` keeps
    the legacy dscli-mode behavior (clean only ``log_dir`` + resource_monitor.log).
    """
    log_dir, _ = read_remote_log_dir(namespace, pods, remote_config, timeout)
    remote_config_dir = os.path.dirname(remote_config)

    def do_op(pod):
        return clean_pod(pod, namespace, log_dir, remote_config_dir,
                         process_name, remote_dir=remote_dir, timeout=timeout)
    return do_for_all_pods(pods, do_op, f'Cleaning {label}')


def cmd_install_impl(pods, namespace, whl, timeout=DEFAULT_TIMEOUT):
    """Install the datasystem whl package across all pods.

    Validates the local whl path exists before dispatching per-pod installs.
    """
    if not os.path.exists(whl):
        log_error(f'ERROR: whl file not found: {whl}')
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


import tarfile  # noqa: E402  (kept here to match the original deferred import)


def build_install_bundle(local_binary, local_lib_dir):
    """Pack binary + .so deps into ONE shared tar, so each pod needs a single
    ``kubectl cp`` + a single ``kubectl exec`` instead of 5 round trips (500
    pods x 5 = 2500 kubectl processes, each a fresh TLS conn + Rancher
    impersonation account -- the dominant cost of a large install).

    Layout inside the tar (must match what service start expects):
      {binary_name}          <- extracted to {remote_dir}/
      lib/<so>               <- extracted to {remote_dir}/lib/
    Symlinks are resolved to real files via realpath. The tar is gzipped:
    a 500-pod install fires 500 concurrent ``kubectl cp`` of this archive,
    and the wire size (not the process count) is the bottleneck -- gzip
    cuts it ~3x so the transfers fit inside the cp timeout. Returns the
    tar path, or None when the binary is missing and no .so files exist.
    """
    has_binary = local_binary and os.path.isfile(local_binary)
    so_files = []
    if local_lib_dir and os.path.isdir(local_lib_dir):
        so_files = glob.glob(os.path.join(local_lib_dir, '*.so*'))
    if not has_binary and not so_files:
        return None
    fd, tar_path = tempfile.mkstemp(suffix='.tar.gz', prefix='ds_bundle_')
    os.close(fd)
    with tarfile.open(tar_path, 'w:gz') as tar:
        if has_binary:
            tar.add(os.path.realpath(local_binary),
                    arcname=os.path.basename(local_binary))
        for so in so_files:
            tar.add(os.path.realpath(so),
                    arcname=f'lib/{os.path.basename(so)}')
    return tar_path


_cp_stagger_window = [0.0]


def set_cp_stagger_window(seconds):
    """Set the random pre-cp delay window for the current batch install.

    A 500-pod install fires 500 concurrent ``kubectl cp`` of the same
    multi-MB bundle; when they all start at once, each transfer's fair
    share of the local uplink pushes every one past the cp timeout
    (all-or-nothing failure). A random delay per pod, drawn from a window
    sized to the batch, spreads the transfer wave into overlapping groups
    without reducing per-pod parallelism.
    """
    _cp_stagger_window[0] = max(0.0, float(seconds))


def _cp_stagger_delay():
    import random
    return random.uniform(0, _cp_stagger_window[0])


def install_binary(pod, namespace, local_binary, local_lib_dir, remote_dir,
                   timeout=DEFAULT_TIMEOUT, bundle_tar_path=None):
    """Copy a standalone binary + .so deps to a pod.

    One bundle tar (binary + lib/*.so, from build_install_bundle) is copied
    with a single ``kubectl cp`` into ``/tmp`` and unpacked with a single
    ``kubectl exec`` that also mkdir's ``remote_dir``, recovers a leftover
    FILE at that path (kubectl cp to a missing parent silently creates
    remote_dir as a FILE), chmod's the binary, and removes the tar --
    2 kubectl round trips per pod instead of 5. Pass ``bundle_tar_path``
    when installing to many pods so the archive is built once and shared.
    """
    if not os.path.exists(local_binary):
        log_error(f'ERROR: binary not found: {local_binary}')
        return False
    name = pod['name']

    def run_checked(cmd, retries=2):
        # Transient kubectl failures (impersonation InternalError, cp timeouts
        # under heavy concurrency) are common on Rancher-fronted clusters;
        # retry with backoff before declaring the pod failed.
        for attempt in range(retries + 1):
            try:
                r = subprocess.run(cmd, capture_output=True, text=True,
                                   timeout=timeout)
            except subprocess.TimeoutExpired:
                if attempt == retries:
                    log_error(f'ERROR: {name}: {" ".join(cmd[:4])}... timed out')
                    return False
                time.sleep(5 * (attempt + 1))
                continue
            if r.returncode == 0:
                return True
            err = (r.stderr or r.stdout).strip()
            if attempt == retries:
                log_error(f'ERROR: {name}: {" ".join(cmd[:4])}... failed: {err}')
                return False
            time.sleep(5 * (attempt + 1))
        return False

    owns_tar = False
    tar_path = bundle_tar_path
    if tar_path is None:
        tar_path = build_install_bundle(local_binary, local_lib_dir)
        owns_tar = True
    try:
        if tar_path is None:
            # No .so deps: fall back to the 3-step cp sequence (mkdir + cp
            # binary + chmod) -- still checked, still fail-fast.
            if not run_checked(['kubectl', 'exec', '-n', namespace, name, '--',
                                'mkdir', '-p', remote_dir]):
                if not run_checked(['kubectl', 'exec', '-n', namespace, name, '--',
                                    'sh', '-c',
                                    f'[ -d {remote_dir} ] || rm -f {remote_dir}']):
                    return False
                if not run_checked(['kubectl', 'exec', '-n', namespace, name, '--',
                                    'mkdir', '-p', remote_dir]):
                    return False
            if not run_checked(['kubectl', 'cp', '-n', namespace, local_binary,
                                f'{name}:{remote_dir}/']):
                return False
            if not run_checked(['kubectl', 'exec', '-n', namespace, name, '--',
                                'chmod', '+x',
                                f'{remote_dir}/{os.path.basename(local_binary)}']):
                return False
            return True

        # Bundle path: single cp + single exec that mkdir's remote_dir
        # (removing a leftover FILE at that path -- kubectl cp to a missing
        # parent silently creates remote_dir as a FILE), unpacks binary +
        # lib/*.so, chmod's the binary, and removes the tar.
        remote_tar = f'/tmp/ds_bundle_{name}.tar.gz'
        binary_base = os.path.basename(local_binary)
        unpack = (f'[ -d {remote_dir} ] || rm -f {remote_dir}; '
                  f'mkdir -p {remote_dir} && '
                  f'tar xzf {remote_tar} -C {remote_dir} && '
                  f'chmod +x {remote_dir}/{binary_base} && '
                  f'rm -f {remote_tar}')
        # Stagger the cp start across pods: a 500-pod install fires 500
        # concurrent `kubectl cp` of the same multi-MB archive, and when they
        # all start at once each transfer's fair share of bandwidth pushes
        # every one past the timeout (all-or-nothing failure). A small random
        # delay per pod spreads the transfer wave; per-pod parallelism is
        # unchanged.
        time.sleep(_cp_stagger_delay())
        for attempt in range(3):
            try:
                kubectl_cp_to(name, namespace, tar_path, remote_tar,
                              timeout=timeout)
                break
            except Exception as e:
                if attempt == 2:
                    log_error(f'ERROR: {name}: bundle cp failed: {e}')
                    return False
                # Longer backoff for timeout-shaped failures: the whole
                # batch's cp wave is still draining, retrying into it just
                # re-competes with the same congestion.
                time.sleep(15 * (attempt + 1))
        if not run_checked(['kubectl', 'exec', '-n', namespace, name, '--',
                            'sh', '-c', unpack]):
            return False
        return True
    finally:
        if owns_tar and tar_path is not None:
            os.unlink(tar_path)


def start_service_standalone(pod, namespace, binary_name, remote_dir, config_path,
                             jf_addr, service_name, extra_args='',
                             config=None,
                             enable_procmon=True, procmon_remote_dir='/tmp',
                             port=None, process_name=None,
                             timeout=DEFAULT_TIMEOUT):
    """Start a standalone test binary in a pod.

    Uses ``standalone_launcher.py`` (uploaded alongside procmon) to fork +
    ``setsid`` the binary in a new session and poll for readiness before
    returning. This mirrors ``dscli start`` (``cli/start.py``): the launcher
    parent prints the PID and exits when the binary is ready, so
    ``kubectl exec`` returns promptly instead of hanging on the SPDY pipe
    held by a ``nohup``-backgrounded binary.

    Readiness check priority (matches dscli split):
      * ``ready_check_path`` in config -> poll for that file's existence
        (worker; authoritative — written after WaitForServiceReady +
        WaitForTopologyReady in worker_oc_server.cpp:2911-2933)
      * else ``port`` given          -> poll TCP connect on ``pod_ip:port``
        (coordinator; mirrors dscli's start_coordinator is_tcp_ready)
      * else                         -> launcher prints PID immediately;
        caller does its own pgrep verify (client-style binaries)

    ``pod['_start_elapsed']`` records the actual launch + readiness wait
    (same semantics as ``start_service``'s dscli path). The post-launch
    pgrep fallback is excluded from the timing.

    If launcher upload fails, falls back to the legacy ``nohup ... &`` path
    (slow but works without the launcher script).
    """
    name = pod['name']
    pod_ip = pod['ip']
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

    binary_path = f'{remote_dir}/{binary_name}'
    log_path = f'{remote_dir}/stdout.log'
    lib_path = f'{remote_dir}/lib'
    import shlex
    binary_argv = (['--config', config_path,
                    '--jf', jf_addr,
                    '--service', service_name]
                   + (shlex.split(extra_args) if extra_args else []))

    # Extract ready_check_path from the worker config (if set). The worker
    # binary writes this file only after WaitForServiceReady() +
    # WaitForTopologyReady() complete (worker_oc_server.cpp:2911-2933), so
    # it's a strictly stronger readiness signal than TCP port listening.
    # Coordinators have no ready_check_path; they fall through to --port.
    ready_file = _extract_ready_check_path(config)

    launcher_remote = upload_launcher({'name': name}, namespace,
                                      procmon_remote_dir, timeout=timeout)
    import time
    t_start = time.monotonic()
    try:
        if launcher_remote:
            pid = _launch_via_launcher(
                name, namespace, launcher_remote,
                binary_path, remote_dir, log_path, lib_path, binary_argv,
                port=port, host=pod_ip,
                ready_file=ready_file,
                ready_timeout=min(timeout, 60),
                subprocess_timeout=timeout)
        else:
            log_error(f'  {name} ({pod_ip}) -> launcher upload failed, '
                      f'falling back to nohup path')
            pid = _launch_via_nohup(
                name, namespace, binary_name, remote_dir, log_path,
                lib_path, config_path, jf_addr, service_name, extra_args,
                pod, port, process_name, timeout)
    finally:
        pod['_start_elapsed'] = time.monotonic() - t_start

    # Launcher / nohup path returned no PID (timeout, error, or fallback).
    # Fall back to pgrep / find_pid_by_port as a sanity check before
    # declaring failure. Excluded from _start_elapsed to match dscli timing
    # semantics (launch + readiness only).
    if not pid:
        time.sleep(1)
        if port and process_name:
            pid = find_pid_by_port(pod, namespace, port, process_name, timeout)
        else:
            verify = kubectl_exec_raw({'name': name}, namespace,
                                      f'pgrep -f {binary_name}', timeout=10)
            if verify and verify.strip():
                pid = verify.strip().split('\n')[0]
    if not pid:
        log_info(f'  {name} ({pod_ip}) -> FAILED: process not found')
        return False
    log_info(f'  {name} ({pod_ip}) -> started (pid={pid})')
    # Attach procmon (same logic as start_service dscli path)
    if enable_procmon:
        if upload_procmon(pod, namespace, procmon_remote_dir, timeout):
            procmon_pid = start_procmon(pod, namespace, pid, procmon_remote_dir,
                                        port=port)
            if procmon_pid:
                log_info(f'  {name} ({pod_ip}) -> procmon started '
                         f'(pid={procmon_pid}, monitoring pid={pid})')
            else:
                log_info(f'  {name} ({pod_ip}) -> procmon start failed')
        else:
            log_info(f'  {name} ({pod_ip}) -> procmon skipped: upload failed')
    return True


def _launch_via_launcher(name, namespace, launcher_remote, binary_path,
                         cwd, log_path, lib_path, binary_argv,
                         port=None, host='127.0.0.1',
                         ready_file=None,
                         ready_timeout=30, subprocess_timeout=DEFAULT_TIMEOUT):
    """Invoke standalone_launcher.py via kubectl exec; return parsed PID.

    Returns the PID string if the launcher printed one, or ``None`` if the
    launcher timed out, exited non-zero, or did not print a parseable PID.

    Readiness signal priority (matches dscli): ``ready_file`` (authoritative,
    e.g. worker ``ready_check_path``) > ``port`` (TCP connect, e.g.
    coordinator) > none (print PID immediately).
    """
    cmd = ['kubectl', 'exec', '-n', namespace, name, '--',
           'python3', launcher_remote,
           '--binary', binary_path,
           '--cwd', cwd,
           '--log', log_path,
           '--lib-path', lib_path,
           '--ready-timeout', str(ready_timeout)]
    if ready_file:
        cmd.extend(['--ready-file', ready_file])
    if port:
        cmd.extend(['--port', str(port), '--host', host])
    cmd.append('--')
    cmd.extend(binary_argv)
    try:
        result = subprocess.run(cmd, capture_output=True, text=True,
                                timeout=subprocess_timeout)
    except subprocess.TimeoutExpired:
        return None
    if result.returncode != 0:
        stderr = (result.stderr or '').strip()
        if stderr:
            log_error(stderr)
        return None
    out = (result.stdout or '').strip()
    if not out:
        return None
    # Launcher prints only the PID to stdout; pick the last line in case
    # kubectl adds any prefix noise.
    pid_line = out.splitlines()[-1].strip()
    return pid_line if pid_line.isdigit() else None


def _extract_ready_check_path(config):
    """Extract ready_check_path from a dscli-style worker config.

    The config field may be a ``{"value": "/path"}`` dict (dscli config
    style, as emitted by deploy_worker / helm_chart/worker.config) or a
    plain string. Returns ``None`` if not set or empty.

    Source: src/datasystem/worker/worker_oc_server.cpp:133 defines
    ``FLAGS_ready_check_path``; ``ReadinessProbe()`` writes the file only
    after ``WaitForServiceReady()`` + ``WaitForTopologyReady()`` complete
    (worker_oc_server.cpp:2911-2933), so file existence is a strictly
    stronger readiness signal than TCP port listening.
    """
    if not config:
        return None
    rcp = config.get('ready_check_path')
    if rcp is None:
        return None
    if isinstance(rcp, dict):
        path = rcp.get('value')
    elif isinstance(rcp, str):
        path = rcp
    else:
        return None
    return path if path else None


def _launch_via_nohup(name, namespace, binary_name, remote_dir, log_path,
                      lib_path, config_path, jf_addr, service_name,
                      extra_args, pod, port, process_name, timeout):
    """Legacy nohup-and-timeout launch path (fallback when launcher upload fails).

    Mirrors the pre-launcher implementation: kubectl exec with the
    nohup+echo-$! shell pattern, swallowing the 10s subprocess timeout
    (kubectl hangs because the binary holds the SPDY pipe). Slow but works
    without the launcher script.
    """
    cmd = (f'cd {remote_dir} && '
           f'LD_LIBRARY_PATH={lib_path}:$LD_LIBRARY_PATH nohup ./{binary_name} '
           f'--config {config_path} --jf {jf_addr} --service {service_name} {extra_args} '
           f'> {log_path} 2>&1 </dev/null & '
           f'echo $!')
    try:
        subprocess.run(
            ['kubectl', 'exec', '-n', namespace, name, '--', 'sh', '-c', cmd],
            capture_output=True, text=True, timeout=10)
    except subprocess.TimeoutExpired:
        pass
    return None


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
    log_info(f'\n{action} per-pod timings:')
    for pod_name, elapsed, ok in sorted(timings, key=lambda x: x[0]):
        log_info(f'  {pod_name:<40} {elapsed:7.2f}s  {"OK" if ok else "FAIL"}')
    elapsed_all = [t for _, t, _ in timings]
    ok_count = sum(1 for _, _, ok in timings if ok)
    fail_count = len(timings) - ok_count
    log_info(f'  min={min(elapsed_all):.2f}s  max={max(elapsed_all):.2f}s  '
             f'avg={sum(elapsed_all) / len(elapsed_all):.2f}s  '
             f'total={sum(elapsed_all):.2f}s  '
             f'(succeeded={ok_count}, failed={fail_count})')


def cmd_exec_shared(args, pods, timeout=DEFAULT_TIMEOUT):
    """Execute command in pods."""
    return cmd_exec_impl(pods, args.namespace, args.cmd, timeout)


def cmd_collect_shared(args, pods, label, timeout=DEFAULT_TIMEOUT):
    """Collect service logs from pods.

    Forwards ``args.remote_dir`` (when present) so standalone-mode collects
    pick up ``stdout.log`` from the binary's install dir; the role CLIs
    default ``--remote-dir`` to the same value ``install`` / ``deploy``
    use, so a collect after a default deploy needs no extra flags. Falls
    back to ``None`` if the attr is missing (older callers, test stubs).
    """
    remote_dir = getattr(args, 'remote_dir', None)
    return cmd_collect_impl(pods, args.namespace, args.remote_config,
                            args.output, label, remote_dir=remote_dir,
                            timeout=timeout)


def cmd_clean_shared(args, pods, process_name, process_name_standalone, label,
                    timeout=DEFAULT_TIMEOUT):
    """Kill service processes and clean log directories.

    Standalone mode (``--standalone``): kill ``process_name_standalone``
    (e.g. ``worker_test`` / ``coordinator_test``) and remove
    ``args.remote_dir`` so a re-deploy does not stack a new binary on top of
    a running stale one. Non-standalone: kill ``process_name`` (e.g.
    ``datasystem_worker``) and clean only ``log_dir`` + resource_monitor.log
    (dscli installs into the package prefix, not ``remote_dir``).
    """
    if getattr(args, 'standalone', False):
        proc = process_name_standalone
        remote_dir = getattr(args, 'remote_dir', None)
    else:
        proc = process_name
        remote_dir = None
    return cmd_clean_impl(pods, args.namespace, args.remote_config,
                          proc, label, remote_dir=remote_dir, timeout=timeout)


def cmd_kill_shared(args, pods, process_name_standalone, label,
                    timeout=DEFAULT_TIMEOUT):
    """Force kill service processes across all pods."""
    proc = (process_name_standalone if getattr(args, 'standalone', False)
            else args.process)
    return cmd_kill_impl(pods, args.namespace, proc, label, timeout)


def cmd_install_shared(args, pods, process_name_standalone, label,
                       script_dir, timeout=DEFAULT_TIMEOUT):
    """Install: standalone mode copies binary + .so (no whl);
    non-standalone mode installs whl only."""
    if getattr(args, 'standalone', False):
        binary = args.binary or os.path.join(
            script_dir, 'output', process_name_standalone)
        lib_dir = getattr(args, 'lib_dir', None) or os.path.join(
            script_dir, 'output', 'lib')
        # Build the install bundle (binary + lib/*.so) once and share it
        # across all pod threads: one archive, and each pod needs only 2
        # kubectl round trips (cp + unpack) instead of 5. Stagger the cp
        # wave for large batches (see set_cp_stagger_window).
        shared_tar = build_install_bundle(binary, lib_dir)
        set_cp_stagger_window(min(30.0, len(pods) * 0.06))
        try:
            def do_op(pod):
                return install_binary(pod, args.namespace, binary, lib_dir,
                                      args.remote_dir, timeout,
                                      bundle_tar_path=shared_tar)
            return do_for_all_pods(pods, do_op, f'Installing {label} (standalone)')
        finally:
            if shared_tar is not None:
                os.unlink(shared_tar)
    else:
        return cmd_install_impl(pods, args.namespace, args.whl, timeout)


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


# ============================================================================
# Pod creation helpers (shared by deploy_jf, deploy_coordinator, deploy_worker)
# ============================================================================

# Deferred imports to avoid circular dependency: deploy_pods imports from
# deploy_common at its top level, so these must run after deploy_common's own
# symbols are defined.
from types import SimpleNamespace  # noqa: E402
import deploy_pods  # noqa: E402


def distribute_instances(num_instances, nodes):
    """Spread N instances across M nodes evenly.

    Returns {ip: count}. First N % M nodes get one extra.
    """
    if num_instances <= 0:
        raise ValueError(f'instances must be a positive integer, got {num_instances}')
    if not nodes:
        raise ValueError('no cluster nodes discovered; cannot spread instances')
    m = len(nodes)
    base = num_instances // m
    remainder = num_instances % m
    distribution = {}
    for i, node in enumerate(nodes):
        count = base + (1 if i < remainder else 0)
        if count > 0:
            distribution[node['ip']] = count
    return distribution


def create_pods(prefix, namespace, image, instances,
                yaml='config/pod_config.yaml.example',
                cpu='8', memory='16Gi',
                requests_cpu=None, requests_memory=None,
                force=False, dry_run=False, timeout=DEFAULT_TIMEOUT):
    """Create pods via deploy_pods.py. Returns pod list or None on failure."""
    nodes = discover_nodes(timeout=timeout)
    try:
        distribution = distribute_instances(instances, nodes)
    except ValueError as e:
        log_error(f'ERROR: {e}')
        return None
    replicas_str = ','.join(f'{ip}:{count}' for ip, count in distribution.items())

    pod_count = sum(distribution.values())
    log_info(f'Creating {pod_count} pod(s) across {len(distribution)} node(s):')
    for ip, count in distribution.items():
        log_info(f'  {ip}: {count}')

    deploy_args = SimpleNamespace(
        namespace=namespace,
        prefix=prefix,
        image=image,
        cpu=cpu,
        memory=memory,
        requests_cpu=requests_cpu or cpu,
        requests_memory=requests_memory or memory,
        replicas=replicas_str,
        pods_per_node=None,
        yaml=yaml,
        dry_run=dry_run,
        force=force,
        wait=True,
        timeout=timeout,
    )
    rc = deploy_pods.cmd_deploy(deploy_args)
    if rc != 0:
        log_error('ERROR: deploy_pods failed')
        return None
    if dry_run:
        return []
    return get_pods(namespace, [prefix])
