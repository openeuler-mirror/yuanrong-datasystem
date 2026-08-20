#!/usr/bin/env python3
"""Quick deploy/stop for the JF mock service discovery server.

Manages mock_jf_server.py lifecycle in a K8s pod: start, stop, check, clean.
Reuses deploy_common.py kubectl transport primitives.

Usage:
    python3 deploy_jf.py start --port 9999 --ttl-default 30
    python3 deploy_jf.py stop
    python3 deploy_jf.py check
    python3 deploy_jf.py clean
"""

import argparse
import os
import subprocess
import sys
import time

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
from deploy_common import DEFAULT_TIMEOUT, get_pods

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


def cmd_start(args, pods):
    if not pods:
        print(f'No pods matching prefix {args.prefixes}', file=sys.stderr)
        return 1
    pod = pods[0]
    name = pod['name']
    ns = args.namespace

    remote_dir = args.remote_dir
    _kubectl_exec(ns, name, f'mkdir -p {remote_dir}')

    local_script = os.path.join(SCRIPT_DIR, 'src', MOCK_SCRIPT)
    if not os.path.exists(local_script):
        local_script = os.path.join(SCRIPT_DIR, MOCK_SCRIPT)
    if not os.path.exists(local_script):
        print(f'ERROR: {MOCK_SCRIPT} not found', file=sys.stderr)
        return 1

    subprocess.run(['kubectl', 'cp', '-n', ns, local_script, f'{name}:{remote_dir}/{MOCK_SCRIPT}'],
                    timeout=DEFAULT_TIMEOUT)
    _kubectl_exec(ns, name, f'chmod +x {remote_dir}/{MOCK_SCRIPT}')

    # nohup + </dev/null detaches stdin; echo $! makes shell print PID and exit.
    # kubectl exec may still hang on PTY cleanup, so use short timeout +
    # allow_timeout (mirrors deploy_client.py pattern): fire-and-forget, then
    # verify via port check below.
    cmd = (f'cd {remote_dir} && nohup python3 {MOCK_SCRIPT} '
           f'--port {args.port} --ttl-default {args.ttl_default} '
           f'> {remote_dir}/jf_mock.log 2>&1 </dev/null & '
           f'echo $!')
    try:
        _kubectl_exec(ns, name, cmd, timeout=10)
    except Exception:
        pass  # fire-and-forget; verify via port check below

    if _wait_port_ready(ns, name, args.port, timeout=args.timeout):
        jf_addr = f"{pod['ip']}:{args.port}"
        print(f'JF mock ready: {jf_addr}')
        return 0
    else:
        print(f'ERROR: JF mock did not become ready within {args.timeout}s', file=sys.stderr)
        _kubectl_exec(ns, name, f'cat {remote_dir}/jf_mock.log')
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
        print('No pods found')
        return 1
    ok = True
    for pod in pods:
        r = _kubectl_exec(args.namespace, pod['name'], f'pgrep -f {PROCESS_NAME} 2>/dev/null')
        alive = r and r.returncode == 0
        print(f"  {pod['name']}: {'ALIVE' if alive else 'NOT RUNNING'}")
        if not alive:
            ok = False
    return 0 if ok else 1


def cmd_clean(args, pods):
    cmd_stop(args, pods)
    for pod in pods:
        _kubectl_exec(args.namespace, pod['name'], f'rm -rf {args.remote_dir}')
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

    p_start = sub.add_parser('start', parents=[parent])
    p_start.add_argument('--port', type=int, default=DEFAULT_PORT)
    p_start.add_argument('--ttl-default', type=int, default=30)

    sub.add_parser('stop', parents=[parent])
    sub.add_parser('check', parents=[parent])
    sub.add_parser('clean', parents=[parent])

    args = parser.parse_args()
    if not args.action:
        parser.print_help()
        return 1

    pods = get_pods(args.namespace, args.prefixes)
    if not pods and args.action != 'stop':
        print(f'No pods found matching {args.prefixes} in {args.namespace}', file=sys.stderr)
        return 1

    handlers = {'start': cmd_start, 'stop': cmd_stop, 'check': cmd_check, 'clean': cmd_clean}
    return handlers[args.action](args, pods)


if __name__ == '__main__':
    sys.exit(main())
