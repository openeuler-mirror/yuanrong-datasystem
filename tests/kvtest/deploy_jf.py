#!/usr/bin/env python3
"""Quick deploy/stop for the JF mock service discovery server.

Manages mock_jf_server.py lifecycle in a K8s pod: deploy, start, stop, check, clean.
Reuses deploy_common.py kubectl transport primitives.

Usage:
    python3 deploy_jf.py deploy -p jf [--image xxx] --port 9999 --ttl-default 30
    python3 deploy_jf.py start -p jf --port 9999 --ttl-default 30
    python3 deploy_jf.py stop
    python3 deploy_jf.py check
    python3 deploy_jf.py clean
"""

from deploy_common import DEFAULT_TIMEOUT, get_pods, create_pods, log_error, log_info, setup_logging
import argparse
import base64
import os
import subprocess
import sys
import time

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

PROCESS_NAME = 'mock_jf_server.py'
MOCK_SCRIPT = 'mock_jf_server.py'
DEFAULT_PORT = 9999


def _kubectl_exec(namespace, pod_name, cmd, timeout=DEFAULT_TIMEOUT):
    """Execute a command in a pod via kubectl exec."""
    full = ['kubectl', 'exec', '-n', namespace, pod_name, '--', 'bash', '-c', cmd]
    try:
        return subprocess.run(full, capture_output=True, text=True, timeout=timeout)
    except subprocess.TimeoutExpired:
        return None


def _wait_port_ready(namespace, pod_name, port, timeout=30):
    """Poll until the mock server port is connectable inside the pod."""
    for _ in range(timeout):
        r = _kubectl_exec(namespace, pod_name,
                          f'python3 -c "import socket; s=socket.socket(); s.settimeout(1); s.connect((\\\"0.0.0.0\\\",{port})); s.close()" 2>/dev/null')
        if r and r.returncode == 0:
            return True
        time.sleep(1)
    return False


def _start_jf_mock(pod, ns, port, ttl_default, remote_dir, timeout):
    """Copy script + start mock server on a single pod.

    Uses ``mock_jf_server.py --background``: the script forks internally
    (``os.fork + os.setsid``, same pattern as ``tools/procmon.py``), parent
    prints child PID to stdout and exits. The script binds the port BEFORE
    forking, so when ``kubectl exec`` returns (parent exited) the port is
    already listening — no separate ``_wait_port_ready`` poll needed.

    This avoids both the ``nohup`` hang (kubectl exec waits on the SPDY
    pipe held by the backgrounded binary) and the need for an external
    ``standalone_launcher.py`` (the script is itself Python and can
    daemonize itself).
    """
    name = pod['name']
    _kubectl_exec(ns, name, f'mkdir -p {remote_dir}')

    local_script = os.path.join(SCRIPT_DIR, 'src', MOCK_SCRIPT)
    if not os.path.exists(local_script):
        local_script = os.path.join(SCRIPT_DIR, MOCK_SCRIPT)
    if not os.path.exists(local_script):
        log_error(f'ERROR: {MOCK_SCRIPT} not found')
        return False

    subprocess.run(['kubectl', 'cp', '-n', ns, local_script, f'{name}:{remote_dir}/{MOCK_SCRIPT}'],
                   timeout=DEFAULT_TIMEOUT)
    _kubectl_exec(ns, name, f'chmod +x {remote_dir}/{MOCK_SCRIPT}')

    log_path = f'{remote_dir}/jf_mock.log'
    script_path = f'{remote_dir}/{MOCK_SCRIPT}'
    # --background: script binds port, forks, parent prints PID + exits.
    # kubectl exec returns immediately; port is already listening.
    cmd = (f'cd {remote_dir} && python3 {script_path} '
           f'--port {port} --ttl-default {ttl_default} '
           f'--background --log {log_path}')
    result = _kubectl_exec(ns, name, cmd, timeout=timeout)
    if not result or result.returncode != 0:
        stderr = (result.stderr if result else '').strip()
        log_error(f'ERROR: JF mock failed to start: {stderr}')
        _kubectl_exec(ns, name, f'cat {log_path}')
        return False

    # Script printed PID after port bind → port is ready. Verify the PID
    # was actually printed (defensive; if the script's --background path
    # has a regression, fall back to the explicit port poll).
    stdout = (result.stdout or '').strip()
    if stdout and stdout.splitlines()[-1].isdigit():
        jf_addr = f"{pod['ip']}:{port}"
        log_info(f'JF mock ready: {jf_addr}')
        return True

    # Fallback: no PID on stdout (older script without --background, or
    # fork failed before printing). Poll the port explicitly.
    log_info(f'JF mock: no PID on stdout, falling back to port poll')
    if _wait_port_ready(ns, name, port, timeout=timeout):
        jf_addr = f"{pod['ip']}:{port}"
        log_info(f'JF mock ready: {jf_addr}')
        return True
    log_error(f'ERROR: JF mock did not become ready within {timeout}s')
    _kubectl_exec(ns, name, f'cat {log_path}')
    return False


def cmd_deploy(args):
    """Deploy: [optional create pod] + copy + start JF mock."""
    prefix = args.prefixes[0]
    ns = args.namespace

    if args.image:
        pods = create_pods(prefix=prefix, namespace=ns, image=args.image, instances=1,
                           yaml=args.yaml, cpu=args.cpu, memory=args.memory,
                           requests_cpu=args.requests_cpu, requests_memory=args.requests_memory,
                           force=args.force, dry_run=getattr(args, 'dry_run', False),
                           timeout=args.timeout)
        if pods is None:
            return 1
        if getattr(args, 'dry_run', False):
            log_info('Dry run: skipped copy and start')
            return 0
    else:
        pods = get_pods(ns, args.prefixes)
        if not pods:
            log_error(f'No pods found matching {args.prefixes} in {ns}')
            return 1

    pod = pods[0]
    if _start_jf_mock(pod, ns, args.port, args.ttl_default, args.remote_dir, args.timeout):
        return 0
    return 1


def cmd_start(args, pods):
    if not pods:
        log_error(f'No pods matching prefix {args.prefixes}')
        return 1
    pod = pods[0]
    if _start_jf_mock(pod, args.namespace, args.port, args.ttl_default, args.remote_dir, args.timeout):
        return 0
    return 1


def cmd_stop(args, pods):
    if not pods:
        return 0
    for pod in pods:
        _kubectl_exec(args.namespace, pod['name'], f'pkill -f {PROCESS_NAME} 2>/dev/null || true')
    time.sleep(2)
    return 0


def cmd_check(args, pods):
    if not pods:
        log_info('No pods found')
        return 1
    ok = True
    for pod in pods:
        r = _kubectl_exec(args.namespace, pod['name'], f'pgrep -f {PROCESS_NAME} 2>/dev/null')
        alive = r and r.returncode == 0
        log_info(f"  {pod['name']}: {'ALIVE' if alive else 'NOT RUNNING'}")
        if not alive:
            ok = False
    return 0 if ok else 1


def cmd_clean(args, pods):
    cmd_stop(args, pods)
    for pod in pods:
        _kubectl_exec(args.namespace, pod['name'], f'rm -rf {args.remote_dir}')
    return 0


def cmd_collect(args, pods):
    """Collect jf_mock.log (and any other .log/.txt, including stdout.log
    if present) from each pod's ``--remote-dir``.

    Gates on ``ls -d {remote_dir}`` so a never-deployed or already-cleaned
    pod (no dir) is skipped silently rather than erroring -- mirrors
    ``deploy_common.collect_logs_from_pod``'s existence gate. Uses base64
    transfer so non-UTF-8 bytes in the log don't break the kubectl exec
    text path. ``jf_mock.log`` is the ``--log`` redirect target written by
    ``_daemonize`` (every register/heartbeat/unregister/discover/expire
    line lands there); ``stdout.log`` is collected opportunistically for
    parity with the worker/coordinator collect path, in case a future
    launch mode writes one.
    """
    if not pods:
        log_error(f'No pods matching prefix {args.prefixes}')
        return 1
    os.makedirs(args.output, exist_ok=True)
    for pod in pods:
        name = pod['name']
        local_pod_dir = os.path.join(args.output, name)
        os.makedirs(local_pod_dir, exist_ok=True)
        r = _kubectl_exec(args.namespace, name,
                          f'ls -d {args.remote_dir} 2>/dev/null')
        if r is None or r.returncode != 0:
            log_info(f'  {name}: {args.remote_dir} does not exist')
            continue
        r = _kubectl_exec(args.namespace, name,
                          f'ls {args.remote_dir}/*.log {args.remote_dir}/*.txt 2>/dev/null')
        files = [f.strip() for f in (r.stdout or '').splitlines()
                 if f.strip()] if r else []
        if not files:
            log_info(f'  {name}: no log files in {args.remote_dir}')
            continue
        log_info(f'  {name}: found {len(files)} log files')
        for remote_path in files:
            fname = os.path.basename(remote_path)
            local_path = os.path.join(local_pod_dir, fname)
            try:
                result = _kubectl_exec(args.namespace, name,
                                      f'base64 {remote_path}')
                if result is None or result.returncode != 0:
                    rc = result.returncode if result else 'timeout'
                    log_info(f'    {fname} -> FAILED: base64 rc={rc}')
                    continue
                content = base64.b64decode(result.stdout)
                with open(local_path, 'wb') as f:
                    f.write(content)
                log_info(f'    {fname} -> collected')
            except Exception as e:
                log_info(f'    {fname} -> FAILED: {e}')
    return 0


def main():
    parser = argparse.ArgumentParser(description='Manage JF mock server in K8s pods')
    sub = parser.add_subparsers(dest='action')

    parent = argparse.ArgumentParser(add_help=False)
    parent.add_argument('-p', '--prefix', action='append', dest='prefixes', required=True,
                        metavar='PREFIX', help='Pod name prefix')
    parent.add_argument('-n', '--namespace', default='default')
    parent.add_argument('--timeout', type=int, default=30)
    parent.add_argument('--remote-dir', default='/tmp/jf_mock')

    # deploy: optional pod creation + copy + start
    p_deploy = sub.add_parser('deploy', parents=[parent],
                              help='[optional create pod] + copy + start JF mock')
    p_deploy.add_argument('--image', default=None,
                          help='Container image (if set, create pod first)')
    p_deploy.add_argument('--yaml', default='config/pod_config.yaml.example')
    p_deploy.add_argument('--cpu', default='1')
    p_deploy.add_argument('--memory', default='1Gi')
    p_deploy.add_argument('--requests-cpu', default=None)
    p_deploy.add_argument('--requests-memory', default=None)
    p_deploy.add_argument('--force', '-f', action='store_true', default=False)
    p_deploy.add_argument('--dry-run', action='store_true', default=False)
    p_deploy.add_argument('--port', type=int, default=DEFAULT_PORT)
    p_deploy.add_argument('--ttl-default', type=int, default=30)

    p_start = sub.add_parser('start', parents=[parent])
    p_start.add_argument('--port', type=int, default=DEFAULT_PORT)
    p_start.add_argument('--ttl-default', type=int, default=30)

    sub.add_parser('stop', parents=[parent])
    sub.add_parser('check', parents=[parent])
    sub.add_parser('clean', parents=[parent])

    p_collect = sub.add_parser('collect', parents=[parent],
                               help='Collect jf_mock.log + stdout.log from pods')
    p_collect.add_argument('-o', '--output', default='collected_jf_logs',
                           help='Local output directory (default: collected_jf_logs)')

    args = parser.parse_args()
    if not args.action:
        parser.print_help()
        return 1
    setup_logging()

    if args.action == 'deploy':
        return cmd_deploy(args)

    pods = get_pods(args.namespace, args.prefixes)
    if not pods and args.action != 'stop':
        log_error(f'No pods found matching {args.prefixes} in {args.namespace}')
        return 1

    handlers = {'start': cmd_start, 'stop': cmd_stop, 'check': cmd_check,
                'clean': cmd_clean, 'collect': cmd_collect}
    return handlers[args.action](args, pods)


if __name__ == '__main__':
    sys.exit(main())
