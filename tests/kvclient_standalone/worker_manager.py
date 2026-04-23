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
            text=True)
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


def kubectl_exec(pod, namespace, cmd, check=True):
    """Execute command in pod via kubectl."""
    return subprocess.run(
        ['kubectl', 'exec', pod, '-n', namespace, '--', 'sh', '-c', cmd],
        check=check, capture_output=True, text=True)


def kubectl_cp_to(pod, namespace, src, dst):
    """Copy local file to pod."""
    subprocess.run(
        ['kubectl', 'cp', src, f'{namespace}/{pod}:{dst}'],
        check=True, capture_output=True, text=True)


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
    except subprocess.CalledProcessError as e:
        print(f'  {pod_name} ({pod_ip}) -> FAILED: {e.stderr.strip()}')
        return False
    finally:
        os.unlink(tmp_path)


def stop_worker(pod, namespace, remote_config):
    """Stop worker in a single pod."""
    pod_name = pod['name']
    try:
        kubectl_exec(pod_name, namespace, f'dscli stop -f {remote_config}', check=False)
        print(f'  {pod_name} -> stopped')
        return True
    except Exception as e:
        print(f'  {pod_name} -> FAILED: {e}')
        return False


def main():
    parser = argparse.ArgumentParser(
        description='Batch start/stop datasystem workers in k8s Pods')
    parser.add_argument('action', choices=['start', 'stop'],
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
    args = parser.parse_args()

    pods = get_pods(args.namespace, args.prefix)
    if not pods:
        print(f'No running pods found with prefix "{args.prefix}" '
              f'in namespace "{args.namespace}"')
        sys.exit(1)

    print(f'Found {len(pods)} pods:')
    for p in pods:
        print(f'  {p["name"]} ({p["ip"]})')

    if args.action == 'start':
        with open(args.config) as f:
            config_template = json.load(f)

        # Replace worker_address with placeholder — per-pod IP injected later
        if 'worker_address' in config_template:
            config_template['worker_address']['value'] = '{POD_IP}:' + str(args.port)

        def do_start(pod):
            cfg = json.loads(json.dumps(config_template))
            cfg['worker_address']['value'] = f'{pod["ip"]}:{args.port}'
            return start_worker(pod, args.namespace, cfg, args.port)

        print(f'\nStarting workers...')
        workers = len(pods)
    else:
        def do_start(pod):
            return stop_worker(pod, args.namespace, args.remote_config)

        print(f'\nStopping workers...')
        workers = len(pods)

    results = []
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {pool.submit(do_start, pod): pod for pod in pods}
        for future in as_completed(futures):
            results.append(future.result())

    ok = sum(1 for r in results if r)
    print(f'\nResult: {ok}/{len(results)} succeeded')


if __name__ == '__main__':
    main()
