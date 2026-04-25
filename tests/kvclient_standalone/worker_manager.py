#!/usr/bin/env python3
"""Batch start/stop datasystem workers in k8s Pods."""

import argparse
import json
import os
import subprocess
import sys
import tempfile
from concurrent.futures import ThreadPoolExecutor, as_completed


def get_pods(namespace, prefix):
    """Get running pods matching name prefix."""
    try:
        out = subprocess.check_output(
            ['kubectl', 'get', 'pods', '-n', namespace, '-o', 'json',
             '--field-selector=status.phase=Running'],
            text=True, timeout=30)
    except FileNotFoundError:
        print('ERROR: kubectl not found', file=sys.stderr)
        sys.exit(1)
    except subprocess.CalledProcessError as e:
        print(f'ERROR: kubectl failed: {e.stderr}', file=sys.stderr)
        sys.exit(1)

    pods = []
    for item in json.loads(out).get('items', []):
        name = item['metadata']['name']
        if not name.startswith(prefix):
            continue
        pod_ip = item.get('status', {}).get('podIP', '')
        if not pod_ip:
            continue
        pods.append({'name': name, 'ip': pod_ip})
    pods.sort(key=lambda p: p['name'])
    return pods


def kubectl_exec(pod, namespace, cmd, check=True, timeout=30):
    """Execute command in pod via kubectl."""
    return subprocess.run(
        ['kubectl', 'exec', pod, '-n', namespace, '--', 'sh', '-c', cmd],
        check=check, capture_output=True, text=True, timeout=timeout)


def kubectl_cp_to(pod, namespace, src, dst):
    """Copy local file to pod."""
    subprocess.run(
        ['kubectl', 'cp', src, f'{namespace}/{pod}:{dst}'],
        check=True, capture_output=True, text=True, timeout=120)


def start_worker(pod, namespace, config_template, worker_port):
    """Start worker in a single pod."""
    pod_name = pod['name']
    pod_ip = pod['ip']
    worker_address = f'{pod_ip}:{worker_port}'

    with tempfile.NamedTemporaryFile(
        mode='w', suffix='.json', prefix=f'worker_{pod_name}_',
        delete=False
    ) as tf:
        json.dump(config_template, tf, indent=2)
        tmp_path = tf.name

    remote_config = '/tmp/worker.config'
    try:
        kubectl_cp_to(pod_name, namespace, tmp_path, remote_config)
        result = kubectl_exec(pod_name, namespace, f'dscli start -f {remote_config}')
        print(f'  {pod_name} ({pod_ip}) -> started')
        return True
    except subprocess.TimeoutExpired:
        print(f'  {pod_name} ({pod_ip}) -> FAILED: timeout')
        return False
    except subprocess.CalledProcessError as e:
        print(f'  {pod_name} ({pod_ip}) -> FAILED: {e.stderr.strip()}')
        return False
    finally:
        os.unlink(tmp_path)


def check_worker(pod, namespace, process_name='datasystem_worker'):
    """Check if worker process is alive in a single pod."""
    pod_name = pod['name']
    try:
        result = kubectl_exec(pod_name, namespace,
            f'pgrep -x {process_name} | wc -l', check=False)
    except subprocess.TimeoutExpired:
        return (pod, 'error', 'timeout')
    if result.returncode != 0:
        return (pod, 'error', result.stderr.strip())
    count = int(result.stdout.strip())
    return (pod, 'alive' if count > 0 else 'dead', count)


def stop_worker(pod, namespace, process_name='datasystem_worker'):
    """Force kill worker process in a single pod."""
    pod_name = pod['name']
    try:
        # Find and kill worker process
        kubectl_exec(pod_name, namespace,
            f'pgrep -x {process_name} | xargs -r kill -9', check=False)
        print(f'  {pod_name} -> killed')
        return True
    except subprocess.TimeoutExpired:
        print(f'  {pod_name} -> FAILED: timeout')
        return False
    except Exception as e:
        print(f'  {pod_name} -> FAILED: {e}')
        return False


def main():
    parser = argparse.ArgumentParser(
        description='Batch start/stop datasystem workers in k8s Pods')
    parser.add_argument('action', choices=['start', 'stop', 'check'],
                        help='Action to perform')
    parser.add_argument('-p', '--prefix', required=True,
                        help='Pod name prefix to match')
    parser.add_argument('-n', '--namespace', default='default',
                        help='k8s namespace (default: default)')
    parser.add_argument('-c', '--config', required=True,
                        help='Path to worker.config template')
    parser.add_argument('--port', type=int, default=31501,
                        help='Worker port (default: 31501)')
    parser.add_argument('--remote-config', default='/tmp/worker.config',
                        help='Config path inside pod (default: /tmp/worker.config)')
    parser.add_argument('--process', default='datasystem_worker',
                        help='Process name to check (default: datasystem_worker)')
    args = parser.parse_args()

    pods = get_pods(args.namespace, args.prefix)
    if not pods:
        print(f'No running pods found with prefix "{args.prefix}" '
              f'in namespace "{args.namespace}"')
        sys.exit(1)

    print(f'Found {len(pods)} pods:')
    for p in pods:
        print(f'  {p["name"]} ({p["ip"]})')

    if args.action == 'check':
        print(f'\nChecking worker processes ({args.process})...')
        results = []
        with ThreadPoolExecutor(max_workers=len(pods)) as pool:
            futures = {pool.submit(check_worker, pod, args.namespace, args.process): pod
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
        return

    if args.action == 'start':
        with open(args.config) as f:
            config_template = json.load(f)

        # Replace worker_address with placeholder — per-pod IP injected later
        if 'worker_address' in config_template:
            config_template['worker_address']['value'] = '{POD_IP}:' + str(args.port)

        def do_op(pod):
            cfg = json.loads(json.dumps(config_template))
            cfg['worker_address']['value'] = f'{pod["ip"]}:{args.port}'
            return start_worker(pod, args.namespace, cfg, args.port)

        print(f'\nStarting workers...')
    else:
        def do_op(pod):
            return stop_worker(pod, args.namespace, args.process)

        print(f'\nStopping workers...')

    results = []
    with ThreadPoolExecutor(max_workers=len(pods)) as pool:
        futures = {pool.submit(do_op, pod): pod for pod in pods}
        for future in as_completed(futures):
            results.append(future.result())

    ok = sum(1 for r in results if r)
    print(f'\nResult: {ok}/{len(results)} succeeded')


if __name__ == '__main__':
    main()
