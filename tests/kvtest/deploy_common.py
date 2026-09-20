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
import posixpath
import shlex
import subprocess
import sys
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor, as_completed


def normalize_jemalloc_prof_conf(conf, log_dir, instance_id, profile_name='kvtest'):
    if not isinstance(conf, str) or not conf.strip():
        raise ValueError('jemalloc_prof_conf cannot be empty')
    values = {}
    for position, item in enumerate(conf.split(','), start=1):
        if ':' not in item:
            raise ValueError(f'Invalid jemalloc_prof_conf option {position}: expected key:value')
        key, value = (part.strip() for part in item.split(':', 1))
        if not key or not value or '\0' in item or '\n' in item or '\r' in item:
            raise ValueError(f'Invalid jemalloc_prof_conf option {position}')
        if key in values:
            raise ValueError(f'Duplicate jemalloc_prof_conf option: {key}')
        values[key] = value
    for key in ('prof', 'prof_active', 'prof_thread_active_init'):
        if values.get(key, 'true').lower() != 'true':
            raise ValueError(f'jemalloc_prof_conf requires {key}:true')
    for key in ('prof', 'prof_active', 'prof_thread_active_init', 'prof_final', 'prof_gdump',
                'prof_accum', 'prof_leak', 'prof_log', 'prof_leak_error', 'prof_sys_thread_name'):
        if key in values:
            if values[key].lower() not in ('true', 'false'):
                raise ValueError(f'{key} must be true or false')
            values[key] = values[key].lower()
    for key, minimum in (('lg_prof_sample', 0), ('lg_prof_interval', -1)):
        if key in values:
            value = values[key]
            if not value.lstrip('-').isascii() or not value.lstrip('-').isdigit():
                raise ValueError(f'{key} must be an integer')
            if not minimum <= int(value) <= 63:
                raise ValueError(f'{key} must be between {minimum} and 63')
    values['prof'] = 'true'
    if 'prof_prefix' not in values:
        if not log_dir:
            raise ValueError('log directory is required when prof_prefix is absent')
        values['prof_prefix'] = posixpath.join(log_dir, 'jemalloc', f'{profile_name}_{instance_id}')
    if any(char in values['prof_prefix'] for char in ',\0\n\r'):
        raise ValueError('prof_prefix cannot contain commas or control characters')
    parent = posixpath.dirname(values['prof_prefix'])
    if not parent:
        raise ValueError('prof_prefix must include a directory, for example /path/to/heap/kvtest')
    return ','.join(f'{key}:{value}' for key, value in values.items()), parent


def validate_jemalloc_prof_conf(conf):
    normalize_jemalloc_prof_conf(conf, 'logs', 0)
    return conf


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


def get_pods(namespace, prefixes, pod_names=None):
    """Get running pods matching any name prefix or exact pod name.

    OR semantics: a pod is selected if its name starts with any prefix OR its
    name exactly equals any entry in ``pod_names``. ``pod_names`` entries may
    themselves be space-separated (so a single ``--pod-names "a b c"`` value and a
    repeated ``--pod-names a --pod-names b`` both work); they are split on whitespace.
    Dedup by name (defensive; pod names are unique within a namespace, so a
    pod matching multiple selectors is still added once). The final list is
    sorted by name globally so instance_id assignment is deterministic
    regardless of the order selectors were passed on the CLI. A WARNING is
    printed for each prefix/exact name that matched zero pods; callers decide
    whether an all-zero result is fatal.

    Returns ``[{'name', 'ip', 'node', 'host_ip'}, ...]``. ``node`` is
    ``spec.nodeName`` (the k8s node hostname) and ``host_ip`` is
    ``status.hostIP`` (the k8s node InternalIP, set by the kubelet).
    ``deploy_client`` needs both to build per-pod HOST_IP env and to spread
    writer/reader instances across physical nodes; the worker/coordinator/jf
    callers only read ``name``/``ip`` and ignore the extra fields.
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
    # Flatten space-separated values so callers can pass either
    # --pod-names "a b" (one value) or --pod-names a --pod-names b (many values).
    exact = set()
    for v in (pod_names or []):
        exact.update(str(v).split())
    pods = []
    seen = set()
    for item in json.loads(out).get('items', []):
        name = item['metadata']['name']
        if name not in exact and not any(name.startswith(p) for p in prefixes):
            continue
        pod_ip = item.get('status', {}).get('podIP', '')
        if not pod_ip:
            continue
        if name in seen:
            continue
        seen.add(name)
        pods.append({
            'name': name,
            'ip': pod_ip,
            'node': item.get('spec', {}).get('nodeName', ''),
            'host_ip': item.get('status', {}).get('hostIP', ''),
        })
    pods.sort(key=lambda p: p['name'])
    for p in prefixes:
        if not any(pod['name'].startswith(p) for pod in pods):
            log_error(f'WARNING: prefix "{p}" matched 0 pods')
    for n in sorted(exact):
        if not any(pod['name'] == n for pod in pods):
            log_error(f'WARNING: pod "{n}" matched 0 pods')
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
                  interval=1, timeout=30, port=None, brpc_bvar_port=None,
                  brpc_bvar_host=None):
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
           f'--output resource_monitor.csv --background')
    if port:
        cmd += f' --port {port}'
    if brpc_bvar_port:
        cmd += f' --brpc-bvar-port {brpc_bvar_port}'
    if brpc_bvar_host:
        cmd += f' --brpc-bvar-host {shlex.quote(brpc_bvar_host)}'
    try:
        result = kubectl_exec(pod['name'], namespace, cmd,
                              check=False, timeout=timeout)
        pid = result.stdout.strip()
        if pid and pid.isdigit():
            return pid
        return None
    except Exception:
        return None


def _config_enabled(config, key):
    value = config.get(key, False)
    if isinstance(value, dict):
        value = value.get('value', False)
    if isinstance(value, str):
        return value.lower() in ('1', 'true', 'on', 'yes')
    return bool(value)


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
                  enable_procmon=True,
                  numactl_opts=None, jemalloc_prof_conf=None,
                  env=None,
                  timeout=DEFAULT_TIMEOUT):
    """Start a datasystem service in a single pod.

    The caller is responsible for injecting the per-pod listening address
    into ``config`` (worker_address for workers, coordinator_address for
    coordinators) before calling this function.

    Writes the config to a temp file, copies it into the pod, runs
    ``dscli start -f <remote_config>`` for workers or
    ``dscli start -C <remote_config>`` for coordinators (optionally with
    numactl and jemalloc profiling options appended for the worker), then
    attaches procmon to the started process. The role is selected from
    ``process_name``: dscli's
    ``-f`` flag binds to ``worker_config_path`` and ``-C`` binds to
    ``coordinator_config_path``, so a coordinator must not be started with
    ``-f`` (dscli would treat it as a worker config).

    ``env`` is a ``{name: value}`` mapping emitted as leading shell
    assignments. dscli forwards its own environment to the service it
    forks, so this is how callers reach the service process with settings
    dscli has no flag for (ASAN_OPTIONS, MALLOC_CONF, ...). Names must be
    shell identifier words — the assignment prefix is only recognized by
    the shell when the name is unquoted.
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
        env_prefix = ''.join(
            f'{name}={shlex.quote(value)} ' for name, value in (env or {}).items())
        cmd = f'{env_prefix}dscli start {config_flag} {remote_config}'
        if numactl_opts and not is_coordinator:
            cmd += f' {numactl_opts}'
        if jemalloc_prof_conf is not None and not is_coordinator:
            cmd += f' --jemalloc_prof_conf {shlex.quote(jemalloc_prof_conf)}'
        # Time only the actual launch (dscli start). Config upload, pid
        # verify, and procmon attach are excluded — caller reads
        # pod['_start_elapsed'] to record the start stopwatch.
        t_start = time.monotonic()
        try:
            kubectl_exec(pod_name, namespace, cmd, timeout=timeout)
        finally:
            pod['_start_elapsed'] = time.monotonic() - t_start
        # Post-launch verify: dscli start returns 0 when the binary signals
        # readiness, but the process may crash immediately after (segfault,
        # bad config, missing .so). check_process confirms it is still alive
        # before declaring success; without this, "started" is printed for a
        # process that is already gone, and the operator sees a green deploy
        # that is actually red.
        _, status, _ = check_process(pod, namespace, process_name, timeout=timeout)
        if status != 'alive':
            log_info(f'  {pod_name} ({pod_ip}) -> FAILED: process exited '
                     f'immediately after dscli start (status={status})')
            return False
        log_info(f'  {pod_name} ({pod_ip}) -> started')

        if enable_procmon:
            # procmon.py lives next to the config file (same directory); its
            # --output csv is written relative to its CWD, which start_procmon
            # sets to this dir. For dscli mode there is no remote_dir (no
            # binary upload), so the config dir is the natural location.
            procmon_dir = os.path.dirname(remote_config)
            if upload_procmon(pod, namespace, procmon_dir, timeout):
                time.sleep(1)
                pid = find_pid_by_port(pod, namespace, port, process_name, timeout)
                if pid:
                    procmon_pid = start_procmon(pod, namespace, pid,
                                                procmon_dir,
                                                port=port,
                                                brpc_bvar_port=(
                                                    port if _config_enabled(
                                                        config,
                                                        'brpc_enable_builtin_services')
                                                    else None),
                                                brpc_bvar_host=(
                                                    pod_ip if _config_enabled(
                                                        config,
                                                        'brpc_enable_builtin_services')
                                                    else None))
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
                          timeout=DEFAULT_TIMEOUT, include_pod_info=False, archive_options=None):
    """Collect log files from a single pod.

    Primary path: stream all log files + resource_monitor.csv + stdout.log
    in a single ``tar cf - {files} | kubectl exec`` round-trip, extracted
    locally. This replaces the prior per-file ``base64 {file}`` loop which
    fired ``N_files + 5`` kubectl exec calls per pod -- on 500+ pods that
    was 7500+ API server round-trips (each a fresh TLS + impersonation
    connection). The tar stream is binary-safe (no base64 33% inflation)
    and gzip-compresses text logs 3-5x.

    Fallback: if the tar stream fails (minimal image without ``tar``, or
    kubectl stdout pipe broken), degrades to the legacy per-file base64
    path so collect still works on any image. Also collects
    ``stdout.log`` from ``remote_dir`` when that directory exists
    (standalone mode writes the binary's combined stdout+stderr to
    ``{remote_dir}/stdout.log``); the directory-existence gate means
    dscli-mode collects (which never create ``remote_dir``) skip
    stdout.log silently without needing a ``--standalone`` flag.
    """
    pod_name = pod['name']
    pod_ip = pod['ip']
    from log_collect import pod_directory
    directory = pod_directory(pod_name, pod_ip, pod.get('host_ip')) if include_pod_info else pod_name
    local_pod_dir = os.path.join(local_dir, directory)
    os.makedirs(local_pod_dir, exist_ok=True)

    try:
        ls_result = kubectl_exec(pod_name, namespace,
                                 f'ls -d -- {shlex.quote(log_dir)} 2>/dev/null', check=False, timeout=timeout)
        if ls_result.returncode != 0:
            log_info(f'  {pod_name} ({pod_ip}) -> log dir {log_dir} does not exist')
            return True

        ls_result = kubectl_exec(pod_name, namespace,
                                 f'ls -- {shlex.quote(log_dir)}/*.log {shlex.quote(log_dir)}/*.log.gz '
                                 f'{shlex.quote(log_dir)}/*.txt 2>/dev/null',
                                 check=False, timeout=timeout)
        log_files = [f.strip() for f in (ls_result.stdout or '').splitlines()
                     if f.strip()]

        if not log_files:
            log_info(f'  {pod_name} ({pod_ip}) -> no log files found')
            return True

        log_info(f'  {pod_name} ({pod_ip}) -> found {len(log_files)} log files')

        # Build the list of files to collect: log files + resource_monitor.csv
        # from procmon dirs. Determine which procmon dirs to include so we
        # can add resource_monitor.csv to the tar set.
        procmon_dirs = set()
        if remote_config_dir:
            procmon_dirs.add(remote_config_dir)
        if log_dir:
            procmon_dirs.add(log_dir)
        glob_dirs = {os.path.dirname(f) for f in log_files}
        extra_csvs = []
        for pdir in procmon_dirs:
            if pdir in glob_dirs:
                continue
            extra_csvs.append(f'{pdir}/resource_monitor.csv')

        # Determine stdout.log path (standalone mode).
        stdout_remote = None
        if remote_dir:
            ls_remote = kubectl_exec(pod_name, namespace,
                                     f'ls -d {remote_dir} 2>/dev/null',
                                     check=False, timeout=timeout)
            if ls_remote.returncode == 0:
                stdout_remote = f'{remote_dir}/stdout.log'

        # Existence check for the optional extras (legacy procmon csv path +
        # stdout.log). tar aborts with rc!=0 when ANY listed file is missing,
        # which used to fail the whole stream on pods where /tmp/
        # resource_monitor.csv (legacy dual-path compat) or stdout.log
        # (binary crashed before redirect) did not exist. One merged `ls -d`
        # filters the tar list to files that actually exist; log_files came
        # from a glob ls so they already exist.
        candidates = list(extra_csvs)
        if stdout_remote:
            candidates.append(stdout_remote)
        if candidates:
            ls_opt = kubectl_exec(pod_name, namespace,
                                  'ls -d ' + ' '.join(candidates) + ' 2>/dev/null',
                                  check=False, timeout=timeout)
            existing = {f.strip() for f in (ls_opt.stdout or '').splitlines()
                        if f.strip()}
            extra_csvs = [f for f in extra_csvs if f in existing]
            if stdout_remote and stdout_remote not in existing:
                stdout_remote = None

        # --- Primary: tar stream ---
        # One kubectl exec pipes tar stdout to local tar extraction.
        # Binary-safe, gzip-compressed, 1 round-trip for all files.
        all_files = log_files + extra_csvs
        if stdout_remote:
            all_files.append(stdout_remote)
        tar_file_list = ' '.join(shlex.quote(f) for f in all_files)
        if archive_options is not None:
            tar_file_list = ' '.join(shlex.quote(f) for f in dict.fromkeys(all_files))
            from log_collect import receive_archive
            flags = 'czf' if archive_options['compress'] else 'cf'
            command = ['kubectl', 'exec', '-n', namespace, pod_name, '--', 'sh', '-c',
                       f'tar {flags} - {tar_file_list}']
            receive_archive(command, local_pod_dir, timeout, archive_options=archive_options, flatten=True)
            return True
        collected = _collect_via_tar_stream(
            pod_name, namespace, tar_file_list, local_pod_dir, timeout)
        if collected:
            return True

        # --- Fallback: per-file base64 (minimal image without tar) ---
        log_info(f'  {pod_name} ({pod_ip}) -> tar stream failed, '
                 f'falling back to per-file base64')
        for remote_path in log_files:
            try:
                fname = os.path.basename(remote_path)
                local_path = os.path.join(local_pod_dir, fname)
                result = kubectl_exec(pod_name, namespace,
                                      f'base64 -- {shlex.quote(remote_path)}', check=True, timeout=timeout)
                content = base64.b64decode(result.stdout)
                with open(local_path, 'wb') as f:
                    f.write(content)
            except Exception as e:
                log_info(f'    {os.path.basename(remote_path)} -> FAILED: {e}')

        for csv_path in extra_csvs:
            try:
                result = kubectl_exec(pod_name, namespace,
                                      f'base64 {csv_path}', check=True, timeout=timeout)
                content = base64.b64decode(result.stdout)
                local_path = os.path.join(local_pod_dir,
                                          'resource_monitor.csv')
                with open(local_path, 'wb') as f:
                    f.write(content)
            except Exception:
                pass

        if stdout_remote:
            try:
                result = kubectl_exec(pod_name, namespace,
                                      f'base64 {stdout_remote}', check=True,
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


def _collect_via_tar_stream(pod_name, namespace, tar_file_list, local_dir,
                            timeout=DEFAULT_TIMEOUT):
    """Stream ``tar cf - {files}`` from a pod to local extraction.

    One ``kubectl exec`` pipes the remote ``tar`` stdout through a local
    ``tarfile`` reader without gzip compression.
    A nonzero tar rc with a non-empty stream (e.g. a file vanished
    between the existence check and tar) still attempts extraction of the
    bytes received -- partial data beats a full fallback. Returns True on
    success, False on any failure (caller falls back to per-file base64).

    Files are extracted by basename (no directory prefix) so the local
    layout matches the base64 fallback path which uses os.path.basename --
    ``{local_dir}/worker.log`` not ``{local_dir}/home/kvcache/logs/worker.log``.
    """
    cmd_str = f'tar cf - {tar_file_list} 2>/dev/null'
    try:
        r = subprocess.run(
            ['kubectl', 'exec', '-n', namespace, pod_name, '--', 'sh', '-c', cmd_str],
            capture_output=True, timeout=timeout)
        if not r.stdout:
            return False
        import io
        with tarfile.open(fileobj=io.BytesIO(r.stdout), mode='r:') as tar:
            for member in tar.getmembers():
                if not member.isfile():
                    continue
                # Strip directory prefix: extract by basename into local_dir,
                # matching the base64 fallback's os.path.basename layout.
                basename = os.path.basename(member.name)
                if not basename:
                    continue
                local_path = os.path.join(local_dir, basename)
                with tar.extractfile(member) as src, open(local_path, 'wb') as dst:
                    import shutil
                    shutil.copyfileobj(src, dst)
        if r.returncode != 0:
            log_info(f'    {pod_name} -> tar rc={r.returncode}, '
                     f'extracted partial stream')
        return True
    except Exception:
        return False


def clean_pod(pod, namespace, log_dir, remote_config_dir, process_name,
              remote_dir=None, timeout=DEFAULT_TIMEOUT, keep_binary=False):
    """Kill the service process and clean logs in a single pod.

    ``remote_dir`` (standalone mode only) holds the standalone binary,
    ``lib/`` .so deps, and ``stdout.log``. By default (``keep_binary=False``)
    it is removed entirely so a subsequent deploy starts from a clean state
    instead of stacking stale binaries, leftover .so variants, and appended
    stdout logs. When ``keep_binary=True`` (clean-logs), only
    ``{remote_dir}/stdout.log`` is removed -- the binary and ``lib/`` .so
    are preserved so a re-deploy skips the 100M+ upload. stdout.log must
    still be deleted explicitly because the standalone binary appends to it
    across runs. When ``remote_dir`` is ``None`` (dscli mode), only
    ``log_dir`` and ``resource_monitor.csv`` are touched -- the dscli install
    path installs into the package prefix, not ``remote_dir``, so there is
    nothing of the deploy's own to remove and ``keep_binary`` is a no-op.
    """
    pod_name = pod['name']
    pod_ip = pod['ip']
    try:
        kill_process(pod, namespace, process_name, timeout=timeout)

        if log_dir:
            kubectl_exec(pod_name, namespace, f'rm -rf {log_dir}',
                         check=False, timeout=timeout)
        kubectl_exec(pod_name, namespace,
                     f'rm -f {remote_config_dir}/resource_monitor.csv',
                     check=False, timeout=timeout)
        if remote_dir:
            if keep_binary:
                kubectl_exec(pod_name, namespace,
                             f'rm -f {remote_dir}/stdout.log',
                             check=False, timeout=timeout)
            else:
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
                     remote_dir=None, timeout=DEFAULT_TIMEOUT,
                     max_workers=None, include_pod_info=False, archive_options=None, log_dir=None):
    """Collect service logs from all pods.

    ``remote_dir`` (standalone mode) is where the binary's ``stdout.log``
    lives; ``collect_logs_from_pod`` gates on its existence so a None value
    (dscli mode, no ``--remote-dir`` passed) simply skips stdout.log
    collection. ``max_workers`` bounds the ThreadPoolExecutor; on large
    clusters (500-2000 pods) an unbounded pool overloads the API server
    with concurrent TLS+impersonation connections, so callers should pass
    ``--max-workers`` (defaults to ``len(pods)`` for backward compat).
    """
    if log_dir is None:
        log_dir, _ = read_remote_log_dir(namespace, pods, remote_config, timeout)
    if not log_dir:
        log_error('ERROR: log_dir not found in remote config')
        return 1

    remote_config_dir = os.path.dirname(remote_config)
    log_info(f'Using log directory: {log_dir}')
    local_dir = output_dir

    # Size the collect stagger window to the batch: a 500-pod collect fires
    # 500 concurrent downloads; a random delay per pod spreads the wave.
    # Window scales with batch size (same heuristic as install's stagger).
    set_collect_stagger_window(min(30.0, len(pods) * 0.06))

    def do_op(pod):
        kwargs = {'include_pod_info': True} if include_pod_info else {}
        if archive_options is not None:
            kwargs['archive_options'] = archive_options
        delay = _collect_stagger_delay()
        if delay > 0:
            time.sleep(delay)
        return collect_logs_from_pod(pod, namespace, log_dir, local_dir,
                                     remote_config_dir=remote_config_dir,
                                     remote_dir=remote_dir, timeout=timeout, **kwargs)
    return do_for_all_pods(pods, do_op, f'Collecting {label}',
                           max_workers=max_workers)


def cmd_clean_impl(pods, namespace, remote_config, process_name, label,
                   remote_dir=None, timeout=DEFAULT_TIMEOUT,
                   keep_binary=False):
    """Kill service processes and clean log directories across all pods.

    ``remote_dir`` (standalone mode) is removed entirely per pod to drop the
    standalone binary, ``lib/`` .so deps, and ``stdout.log``. ``None`` keeps
    the legacy dscli-mode behavior (clean only ``log_dir`` + resource_monitor.csv).
    ``keep_binary=True`` (clean-logs) removes only ``{remote_dir}/stdout.log``
    and preserves the binary + lib/ so a re-deploy skips the 100M+ upload;
    a no-op when ``remote_dir`` is ``None`` (dscli mode never touched it).
    """
    log_dir, _ = read_remote_log_dir(namespace, pods, remote_config, timeout)
    remote_config_dir = os.path.dirname(remote_config)

    def do_op(pod):
        return clean_pod(pod, namespace, log_dir, remote_config_dir,
                         process_name, remote_dir=remote_dir, timeout=timeout,
                         keep_binary=keep_binary)
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


_collect_stagger_window = [0.0]


def set_collect_stagger_window(seconds):
    """Set the random pre-collect delay window for the current batch.

    Mirrors ``set_cp_stagger_window`` but for the download direction: a
    500-pod collect fires 500 concurrent ``kubectl exec tar`` downloads;
    when they all start at once, each transfer's fair share of the local
    downlink pushes every one past the kubectl timeout (all-or-nothing
    failure). A random delay per pod, drawn from a window sized to the
    batch, spreads the download wave into overlapping groups without
    reducing per-pod parallelism.
    """
    _collect_stagger_window[0] = max(0.0, float(seconds))


def _collect_stagger_delay():
    import random
    return random.uniform(0, _collect_stagger_window[0])


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
                             enable_procmon=True,
                             port=None, process_name=None,
                             timeout=DEFAULT_TIMEOUT, jemalloc_prof_conf=None,
                             start_timeout=90):
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
    (same semantics as ``start_service``'s dscli path). When the launcher
    path is used, the elapsed is measured inside the launcher (Popen →
    ready), excluding ``kubectl exec`` connection setup and ``python3``
    interpreter startup — this matters on large clusters where
    ``kubectl exec`` latency can dominate. The post-launch pgrep fallback
    is also excluded from the timing.

    If launcher upload fails, falls back to the legacy ``nohup ... &`` path
    (slow but works without the launcher script).
    """
    name = pod['name']
    pod_ip = pod['ip']
    env = {}
    if jemalloc_prof_conf is not None:
        try:
            log_dir = (config or {}).get('log_dir')
            if isinstance(log_dir, dict):
                log_dir = log_dir.get('value')
            conf, parent = normalize_jemalloc_prof_conf(
                jemalloc_prof_conf, log_dir, port or name, profile_name=binary_name)
            _, status, _ = check_process(pod, namespace, binary_name, timeout=timeout)
            if status == 'alive':
                raise RuntimeError('profiling configuration requires stopping and restarting the existing process')
            if status != 'dead':
                raise RuntimeError('cannot determine whether the service is already running')
            prepare_standalone_jemalloc_prof(
                pod, namespace, binary_name, remote_dir, parent, timeout)
            env['MALLOC_CONF'] = conf
        except (ValueError, RuntimeError, subprocess.SubprocessError) as error:
            log_error(f'{name} ({pod_ip}) -> profiling preflight failed: {error}')
            return False
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

    # Upload the launcher to the same directory as the binary (remote_dir), so
    # start and stop both find it at {remote_dir}/standalone_launcher.py and
    # the pidfile at {remote_dir}/{binary_name}.pid.
    launcher_remote = upload_launcher({'name': name}, namespace,
                                      remote_dir, timeout=timeout)
    import time
    t_start = time.monotonic()
    pid = None
    launch_elapsed = None
    try:
        if launcher_remote:
            pid, launch_elapsed = _launch_via_launcher(
                name, namespace, launcher_remote,
                binary_path, remote_dir, log_path, lib_path, binary_argv,
                port=port, host=pod_ip,
                ready_file=ready_file,
                ready_timeout=start_timeout,
                subprocess_timeout=start_timeout + 60, env=env)
        else:
            log_error(f'  {name} ({pod_ip}) -> launcher upload failed, '
                      f'falling back to nohup path')
            pid = _launch_via_nohup(
                name, namespace, binary_name, remote_dir, log_path,
                lib_path, config_path, jf_addr, service_name, extra_args,
                pod, port, process_name, timeout, env=env)
    finally:
        # Use the launcher-reported elapsed (Popen → ready, excludes
        # kubectl exec / python3 startup overhead) when available. Fall
        # back to the outer measurement for the nohup path or when the
        # launcher didn't report a timing.
        if launch_elapsed is not None:
            pod['_start_elapsed'] = launch_elapsed
        else:
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
    # Post-launch verify: the launcher / nohup path reported a PID, but the
    # process may have crashed immediately after the readiness signal
    # (segfault, bad config, missing .so). A quick pgrep confirms the PID
    # is still alive before declaring success; without this, "started" is
    # printed for a process that is already gone, and the operator sees a
    # green deploy that is actually red.
    verify = kubectl_exec_raw({'name': name}, namespace,
                              f'pgrep -f {binary_name}', timeout=10)
    if not verify or not verify.strip():
        log_info(f'  {name} ({pod_ip}) -> FAILED: process exited immediately '
                 f'after launch (pid={pid} no longer found)')
        return False
    log_info(f'  {name} ({pod_ip}) -> started (pid={pid})')
    # Attach procmon (same logic as start_service dscli path). procmon.py is
    # uploaded to remote_dir (same as the binary), and its --output csv is
    # written relative to the CWD set by start_procmon.
    if enable_procmon:
        if upload_procmon(pod, namespace, remote_dir, timeout):
            procmon_pid = start_procmon(
                pod, namespace, pid, remote_dir, port=port,
                brpc_bvar_port=(
                    port if config and _config_enabled(
                        config, 'brpc_enable_builtin_services') else None),
                brpc_bvar_host=(
                    pod_ip if config and _config_enabled(
                        config, 'brpc_enable_builtin_services') else None))
            if procmon_pid:
                log_info(f'  {name} ({pod_ip}) -> procmon started '
                         f'(pid={procmon_pid}, monitoring pid={pid})')
            else:
                log_info(f'  {name} ({pod_ip}) -> procmon start failed')
        else:
            log_info(f'  {name} ({pod_ip}) -> procmon skipped: upload failed')
    return True


def prepare_standalone_jemalloc_prof(pod, namespace, binary_name, remote_dir, parent, timeout):
    cwd = f'cd {shlex.quote(remote_dir)} && '
    runtime_env = 'MALLOC_CONF= LD_LIBRARY_PATH=./lib:"${LD_LIBRARY_PATH:-}" '
    result = kubectl_exec(
        pod['name'], namespace,
        cwd + runtime_env + shlex.quote(f'./{binary_name}') + ' --version',
        check=False, timeout=timeout)
    if result.returncode != 0 or 'jemalloc_prof_supported=true' not in result.stdout.splitlines():
        raise RuntimeError(
            f'{binary_name} cannot load a profiling allocator; rebuild with -b bazel -x on '
            'and install the matching lib/ directory')
    directory = shlex.quote(parent)
    kubectl_exec(pod['name'], namespace,
                 cwd + f'mkdir -p -- {directory} && test -d {directory} && '
                 f'test -w {directory} && test -x {directory}', timeout=timeout)


def _launch_via_launcher(name, namespace, launcher_remote, binary_path,
                         cwd, log_path, lib_path, binary_argv,
                         port=None, host='127.0.0.1',
                         ready_file=None,
                         ready_timeout=30, subprocess_timeout=DEFAULT_TIMEOUT, env=None):
    """Invoke standalone_launcher.py via kubectl exec; return (pid, elapsed).

    Returns a ``(pid_str, elapsed_float)`` tuple if the launcher printed a
    PID, or ``(None, None)`` if the launcher timed out, exited non-zero,
    or did not print a parseable PID.

    The ``elapsed`` is measured inside the launcher (Popen → ready signal),
    excluding ``kubectl exec`` connection setup and ``python3`` interpreter
    startup overhead. The caller should use this value for
    ``pod['_start_elapsed']`` instead of the outer ``time.monotonic()``
    diff so that large-cluster ``kubectl exec`` latency does not inflate
    the reported startup time.

    Readiness signal priority (matches dscli): ``ready_file`` (authoritative,
    e.g. worker ``ready_check_path``) > ``port`` (TCP connect, e.g.
    coordinator) > none (grace-poll for early exits).
    """
    cmd = ['kubectl', 'exec', '-n', namespace, name, '--']
    if env:
        cmd.extend(['env'] + [f'{key}={value}' for key, value in env.items()])
    cmd.extend(['python3', launcher_remote,
           '--binary', binary_path,
           '--cwd', cwd,
           '--log', log_path,
           '--lib-path', lib_path,
           '--pidfile', f'{binary_path}.pid',
           '--ready-timeout', str(ready_timeout)])
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
        return None, None
    stderr = (result.stderr or '').strip()
    if result.returncode != 0:
        if stderr:
            log_error(stderr)
        return None, None
    # Surface launcher warnings (e.g. not-ready-timeout with PID) even on
    # success so the caller knows readiness was not confirmed.
    if stderr:
        log_info(stderr)
    out = (result.stdout or '').strip()
    if not out:
        return None, None
    # Launcher prints "{pid} {elapsed}" to stdout; pick the last line in
    # case kubectl adds any prefix noise.
    pid_line = out.splitlines()[-1].strip()
    parts = pid_line.split()
    pid = parts[0] if parts and parts[0].isdigit() else None
    if not pid:
        return None, None
    elapsed = None
    if len(parts) > 1:
        try:
            elapsed = float(parts[1])
        except ValueError:
            pass
    return pid, elapsed


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
                      extra_args, pod, port, process_name, timeout, env=None):
    """Legacy nohup-and-timeout launch path (fallback when launcher upload fails).

    Mirrors the pre-launcher implementation: kubectl exec with the
    nohup+echo-$! shell pattern, swallowing the 10s subprocess timeout
    (kubectl hangs because the binary holds the SPDY pipe). Slow but works
    without the launcher script.
    """
    env_prefix = ''.join(f'{key}={shlex.quote(value)} ' for key, value in (env or {}).items())
    cmd = (f'cd {shlex.quote(remote_dir)} && {env_prefix}'
           f'LD_LIBRARY_PATH={shlex.quote(lib_path)}:"${{LD_LIBRARY_PATH:-}}" '
           f'nohup {shlex.quote("./" + binary_name)} '
           f'--config {shlex.quote(config_path)} --jf {shlex.quote(jf_addr)} '
           f'--service {shlex.quote(service_name)} {extra_args} '
           f'> {shlex.quote(log_path)} 2>&1 </dev/null & '
           f'echo $!')
    try:
        subprocess.run(
            ['kubectl', 'exec', '-n', namespace, name, '--', 'sh', '-c', cmd],
            capture_output=True, text=True, timeout=10)
    except subprocess.TimeoutExpired:
        pass
    return None


def stop_service_standalone(pod, namespace, process_name, remote_dir=None,
                            grace=180, timeout=DEFAULT_TIMEOUT):
    """Stop a standalone test binary via the launcher's stop subcommand.

    Primary path: one ``kubectl exec`` runs
    ``standalone_launcher.py stop --pidfile {remote_dir}/{process_name}.pid``
    which sends SIGTERM, polls for exit inside the pod (0.2s interval, same
    /proc semantics as dscli's ``wait_exit``), escalates to SIGKILL after
    ``grace`` seconds, and waits another 10s. The whole wait/escalate loop
    runs in-pod so the deploy side issues a single round-trip regardless of
    how long the graceful shutdown takes.

    Exit-code contract (from the launcher): 0 = exited (including "was
    already gone" -- idempotent), 1 = alive after SIGKILL (FAILED),
    3 = pidfile missing (process was started by an older deploy or via the
    nohup path) -> fall back to the legacy pkill + pgrep-poll path below.

    Returns True if the process is gone, False only if it refused to die
    even after SIGKILL.
    """
    name = pod['name']
    pod_ip = pod.get('ip', '')
    if remote_dir:
        # upload_launcher uploads to {remote_dir}/standalone_launcher.py (same
        # directory as the binary), so stop looks for it there too. pidfile
        # is also under {remote_dir}.
        launcher_remote = f'{remote_dir}/standalone_launcher.py'
        pidfile = f'{remote_dir}/{process_name}.pid'
        cmd = (f'python3 {shlex.quote(launcher_remote)} stop '
               f'--pidfile {shlex.quote(pidfile)} '
               f'--binary {shlex.quote(process_name)} --grace {grace}')
        try:
            r = kubectl_exec(name, namespace, cmd, check=False,
                             timeout=grace + 10 + 60)
        except subprocess.TimeoutExpired:
            log_info(f'  {name} ({pod_ip}) -> FAILED: launcher stop timed out')
            return False
        except Exception as e:
            log_info(f'  {name} ({pod_ip}) -> launcher stop error: {e}; '
                     f'falling back to pkill')
            r = None
        if r is not None:
            if r.returncode == 0:
                elapsed = _launcher_elapsed(r.stdout)
                detail = f' (elapsed={elapsed:.2f}s)' if elapsed is not None else ''
                log_info(f'  {name} ({pod_ip}) -> stopped{detail}')
                return True
            if r.returncode == 1:
                elapsed = _launcher_elapsed(r.stdout)
                detail = f' (elapsed={elapsed:.2f}s)' if elapsed is not None else ''
                log_info(f'  {name} ({pod_ip}) -> FAILED: still running after '
                         f'TERM+KILL{detail}')
                return False
            # rc == 3 (no pidfile) or any unexpected code: legacy fallback.
        # fall through
    return _stop_service_standalone_legacy(pod, namespace, process_name, grace)


def _launcher_elapsed(stdout):
    """Parse ``stopped {elapsed}`` / ``alive {elapsed}`` from launcher stop
    stdout. Returns the float or None."""
    if not stdout:
        return None
    parts = stdout.strip().splitlines()[-1].split()
    if len(parts) >= 2:
        try:
            return float(parts[1])
        except ValueError:
            return None
    return None


def _pgrep_alive(name, namespace, process_name, timeout=10):
    """True if pgrep -f finds the process in the pod."""
    out = kubectl_exec_raw({'name': name}, namespace,
                           f'pgrep -f {process_name}', timeout=timeout)
    return bool(out and out.strip())


def _stop_service_standalone_legacy(pod, namespace, process_name, grace):
    """Pre-pidfile stop path: pkill + deploy-side pgrep polling with the
    same TERM -> KILL escalation contract as the launcher stop. Kept for
    processes started before pidfiles existed (or nohup starts)."""
    name = pod['name']
    pod_ip = pod.get('ip', '')
    try:
        subprocess.run(['kubectl', 'exec', '-n', namespace, name, '--', 'bash', '-c',
                        f'pkill -TERM -f {process_name} 2>/dev/null'],
                       capture_output=True, text=True, timeout=30)
    except subprocess.TimeoutExpired:
        pass
    # Poll for exit every 2s (kubectl exec per poll is the floor cost; the
    # launcher path avoids this entirely). Same grace as the launcher path.
    deadline = time.monotonic() + grace
    while time.monotonic() < deadline:
        if not _pgrep_alive(name, namespace, process_name):
            log_info(f'  {name} ({pod_ip}) -> stopped')
            return True
        time.sleep(2)
    subprocess.run(['kubectl', 'exec', '-n', namespace, name, '--', 'bash', '-c',
                    f'pkill -9 -f {process_name} 2>/dev/null'],
                   capture_output=True, text=True, timeout=30)
    time.sleep(2)
    if not _pgrep_alive(name, namespace, process_name):
        log_info(f'  {name} ({pod_ip}) -> stopped (after SIGKILL)')
        return True
    log_info(f'  {name} ({pod_ip}) -> FAILED: still running after TERM+KILL')
    return False


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
    Forwards ``args.max_workers`` (when present) to bound the pool on
    large clusters; ``None`` keeps the legacy unbounded behavior.
    """
    remote_dir = getattr(args, 'remote_dir', None)
    max_workers = getattr(args, 'max_workers', None)
    kwargs = {'include_pod_info': True} if getattr(args, 'pod_info', False) else {}
    from log_collect import archive_options_from_args
    archive_options = archive_options_from_args(args)
    if archive_options is not None:
        kwargs['archive_options'] = archive_options
    if getattr(args, 'log_dir', None) is not None:
        kwargs['log_dir'] = args.log_dir
    return cmd_collect_impl(pods, args.namespace, args.remote_config,
                            args.output, label, remote_dir=remote_dir,
                            timeout=timeout, max_workers=max_workers, **kwargs)


def cmd_clean_shared(args, pods, process_name, process_name_standalone, label,
                     timeout=DEFAULT_TIMEOUT):
    """Kill service processes and clean log directories.

    Standalone mode (``--standalone``): kill ``process_name_standalone``
    (e.g. ``worker_test`` / ``coordinator_test``) and remove
    ``args.remote_dir`` so a re-deploy does not stack a new binary on top of
    a running stale one. Non-standalone: kill ``process_name`` (e.g.
    ``datasystem_worker``) and clean only ``log_dir`` + resource_monitor.csv
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


def cmd_clean_logs_shared(args, pods, process_name, process_name_standalone,
                          label, timeout=DEFAULT_TIMEOUT):
    """Kill service processes and clean log directories but keep the binary.

    Mirrors ``cmd_clean_shared`` except the standalone binary, ``lib/`` .so
    deps, and the ``remote_dir`` itself are preserved -- only
    ``{remote_dir}/stdout.log`` is removed. Lets a re-deploy skip the
    100M+ binary+lib upload on large clusters. Process selection and
    ``--standalone``/``--remote-dir`` semantics match ``clean`` exactly;
    in dscli mode (non-standalone) behavior is identical to ``clean``
    since clean never touched the package-prefix install path either.
    """
    if getattr(args, 'standalone', False):
        proc = process_name_standalone
        remote_dir = getattr(args, 'remote_dir', None)
    else:
        proc = process_name
        remote_dir = None
    return cmd_clean_impl(pods, args.namespace, args.remote_config,
                          proc, label, remote_dir=remote_dir, timeout=timeout,
                          keep_binary=True)


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
    """Stop service gracefully. Standalone mode delegates to the launcher's
    stop subcommand (pidfile + in-pod TERM/wait/KILL escalation, grace from
    --stop-timeout, default 180s); non-standalone uses dscli stop."""
    if getattr(args, 'standalone', False):
        remote_dir = getattr(args, 'remote_dir', None)
        grace = getattr(args, 'stop_timeout', 180)
        timings = []

        def do_op(pod):
            import time as _time
            t0 = _time.monotonic()
            ok = False
            try:
                ok = stop_service_standalone(pod, args.namespace,
                                             process_name_standalone,
                                             remote_dir=remote_dir,
                                             grace=grace, timeout=timeout)
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
