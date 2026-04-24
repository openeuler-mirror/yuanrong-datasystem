#!/usr/bin/env python3
"""Generate deploy.json by querying k8s Pods with a name prefix."""

import argparse
import json
import os
import shutil
import subprocess
import sys


def get_pods(namespace, prefix):
    """Get running pods matching name prefix via kubectl."""
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

    data = json.loads(out)
    pods = []
    for item in data.get('items', []):
        name = item['metadata']['name']
        if not name.startswith(prefix):
            continue
        pod_ip = item.get('status', {}).get('podIP', '')
        if not pod_ip:
            continue
        node_name = item.get('spec', {}).get('nodeName', '')
        pods.append({'name': name, 'ip': pod_ip, 'node': node_name})

    pods.sort(key=lambda p: p['name'])
    return pods


def main():
    parser = argparse.ArgumentParser(
        description='Generate deploy.json from k8s Pod names')
    parser.add_argument('-p', '--prefix', required=True,
                        help='Pod name prefix to match')
    parser.add_argument('-n', '--namespace', default='default',
                        help='k8s namespace (default: default)')
    parser.add_argument('-r', '--remote-work-dir',
                        default='/home/user/kvclient_test',
                        help='Remote work directory')
    parser.add_argument('-o', '--output-dir', default='config',
                        help='Output directory (default: config)')
    parser.add_argument('-e', '--etcd-address',
                        help='Override etcd_address in generated config.json '
                             '(e.g. "10.0.0.5:2379")')
    parser.add_argument('-c', '--cluster-name',
                        help='Set cluster_name in generated config.json')
    parser.add_argument('--remote-sdk-dir',
                        help='SDK lib path inside containers (skip copying SDK '
                             'from master if set, e.g. "/usr/local/datasystem/lib")')
    parser.add_argument('-w', '--writer-count', type=int, default=1,
                        help='Number of writer instances (default: 1). '
                             'First N pods become writers, rest become readers.')
    args = parser.parse_args()

    pods = get_pods(args.namespace, args.prefix)
    if not pods:
        print(f'No running pods found with prefix "{args.prefix}" '
              f'in namespace "{args.namespace}"')
        sys.exit(1)

    if args.writer_count < 0 or args.writer_count > len(pods):
        print(f'ERROR: --writer-count ({args.writer_count}) must be 0..{len(pods)}',
              file=sys.stderr)
        sys.exit(1)

    # Distribute writers evenly across nodes (round-robin per node)
    # Build node -> [pod_index] mapping
    node_pods = {}
    for i, pod in enumerate(pods):
        node_pods.setdefault(pod['node'], []).append(i)

    writer_indices = set()
    # Sort nodes for deterministic assignment
    sorted_nodes = sorted(node_pods.keys())
    # Round-robin: pick one pod from each node in turn
    pod_queues = {n: list(node_pods[n]) for n in sorted_nodes}
    assigned = 0
    while assigned < args.writer_count:
        for node in sorted_nodes:
            if assigned >= args.writer_count:
                break
            if pod_queues[node]:
                writer_indices.add(pod_queues[node].pop(0))
                assigned += 1

    nodes = []
    for i, pod in enumerate(pods):
        is_writer = i in writer_indices
        node = {
            'pod_name': pod['name'],
            'pod_ip': pod['ip'],
            'namespace': args.namespace,
            'instance_id': i,
            'role': 'writer' if is_writer else 'reader',
            'pipeline': ['setStringView'] if is_writer else [],
            'notify_pipeline': ['getBuffer'],
        }
        nodes.append(node)

    deploy = {
        'remote_work_dir': args.remote_work_dir,
        'transport': 'kubectl',
        'enable_procmon': True,
        'nodes': nodes,
    }

    if args.remote_sdk_dir:
        deploy['remote_sdk_dir'] = args.remote_sdk_dir

    os.makedirs(args.output_dir, exist_ok=True)

    deploy_path = os.path.join(args.output_dir, 'deploy.json')
    with open(deploy_path, 'w') as f:
        json.dump(deploy, f, indent=2)
        f.write('\n')
    print(f'Generated {deploy_path} ({len(nodes)} pods)')

    # Copy config template
    script_dir = os.path.dirname(os.path.abspath(__file__))
    src = os.path.join(script_dir, 'config', 'config.json.example')
    dst = os.path.join(args.output_dir, 'config.json')
    if os.path.exists(src):
        shutil.copy2(src, dst)
        overrides = {}
        if args.etcd_address:
            overrides['etcd_address'] = args.etcd_address
        if args.cluster_name:
            overrides['cluster_name'] = args.cluster_name
        if overrides:
            with open(dst) as f:
                cfg = json.load(f)
            cfg.update(overrides)
            with open(dst, 'w') as f:
                json.dump(cfg, f, indent=2)
                f.write('\n')
            for k, v in overrides.items():
                print(f'  {k} set to {v}')
        print(f'Copied {src} -> {dst}')
    else:
        print(f'WARNING: {src} not found, skipping config.json')

    for node in nodes:
        print(f'  {node["pod_name"]} -> {node["pod_ip"]} '
              f'(instance_id={node["instance_id"]}, role={node["role"]})')


if __name__ == '__main__':
    main()
