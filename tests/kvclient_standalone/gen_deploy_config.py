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
            text=True)
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
        pods.append({'name': name, 'ip': pod_ip})

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
    args = parser.parse_args()

    pods = get_pods(args.namespace, args.prefix)
    if not pods:
        print(f'No running pods found with prefix "{args.prefix}" '
              f'in namespace "{args.namespace}"')
        sys.exit(1)

    nodes = []
    for i, pod in enumerate(pods):
        nodes.append({
            'pod_name': pod['name'],
            'pod_ip': pod['ip'],
            'namespace': args.namespace,
            'instance_id': i,
        })

    deploy = {
        'remote_work_dir': args.remote_work_dir,
        'transport': 'kubectl',
        'enable_procmon': True,
        'nodes': nodes,
    }

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
        print(f'Copied {src} -> {dst}')
    else:
        print(f'WARNING: {src} not found, skipping config.json')

    for node in nodes:
        print(f'  {node["pod_name"]} -> {node["pod_ip"]} (instance_id={node["instance_id"]})')


if __name__ == '__main__':
    main()
