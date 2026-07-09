#!/usr/bin/env python3
"""Deploy k8s Pods from YAML template.

Usage:
    python3 deploy_pods.py deploy --image xxx --prefix xxx [--yaml template.yaml]
    python3 deploy_pods.py delete --prefix xxx
    python3 deploy_pods.py status --prefix xxx
"""

import argparse
import json
import os
import subprocess
import sys
import tempfile

MAX_PARALLEL_KUBECTL = 32


def run_kubectl(args, check=True, timeout=60):
    """Run kubectl command.

    Raises subprocess.TimeoutExpired on timeout so callers cannot silently
    ignore a hung kubectl. CalledProcessError is still controlled by 'check'.
    """
    cmd = ['kubectl'] + args
    try:
        result = subprocess.run(
            cmd, check=check, capture_output=True, text=True, timeout=timeout)
        return result
    except subprocess.TimeoutExpired:
        # Re-raise so callers cannot silently swallow a hung kubectl. A
        # timeout is an unexpected operational failure, not a normal
        # kubectl non-zero exit that 'check=False' intends to tolerate.
        print(f'ERROR: kubectl timeout for: {" ".join(args)}', file=sys.stderr)
        raise
    except subprocess.CalledProcessError as e:
        print(f'ERROR: kubectl failed for: {" ".join(args)}', file=sys.stderr)
        if e.stderr:
            print(f'  stderr: {e.stderr}', file=sys.stderr)
        return None


def delete_pods_parallel(pod_names, namespace, timeout=60):
    """Delete multiple pods in parallel.

    Raises RuntimeError if any deletion failed (kubectl timeout or non-zero
    return code), so callers cannot silently proceed after a failed cleanup.
    """
    if not pod_names:
        return
    from concurrent.futures import ThreadPoolExecutor, as_completed

    def delete_one(pod):
        try:
            result = run_kubectl(['delete', 'pod', pod, '-n', namespace],
                                 check=False, timeout=timeout)
        except (subprocess.TimeoutExpired, OSError) as e:
            print(f'ERROR: delete {pod} failed: {e}', file=sys.stderr)
            return pod, False
        ok = result is not None and result.returncode == 0
        return pod, ok

    failed = []
    workers = min(len(pod_names), MAX_PARALLEL_KUBECTL)
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {pool.submit(delete_one, pod): pod for pod in pod_names}
        for future in as_completed(futures):
            pod, ok = future.result()
            if not ok:
                failed.append(pod)
    print(f'  Deleted {len(pod_names) - len(failed)}/{len(pod_names)} pod(s)')
    if failed:
        raise RuntimeError(
            f'Failed to delete {len(failed)} pod(s): {", ".join(failed)}')


def get_pods_by_prefix(namespace, prefix):
    """Get running pods matching prefix."""
    result = run_kubectl([
        'get', 'pods', '-n', namespace, '-o', 'json'], check=False)
    if not result or result.returncode != 0:
        return []

    pods = []
    for item in json.loads(result.stdout).get('items', []):
        name = item['metadata']['name']
        if name.startswith(prefix):
            pod_ip = item.get('status', {}).get('podIP', '')
            node_name = item.get('spec', {}).get('nodeName', '')
            pods.append({'name': name, 'ip': pod_ip, 'node': node_name})
    return pods


def parse_replicas(replicas_str):
    """Parse replica spec string to dict.

    Format: "ip1:count1,ip2:count2,..."
    Returns: {ip1: count1, ip2: count2, ...}

    Raises SystemExit on malformed count so the user gets a clear message
    instead of a ValueError traceback.
    """
    if not replicas_str:
        return {}

    result = {}
    for item in replicas_str.split(','):
        item = item.strip()
        if not item:
            continue
        if ':' in item:
            ip, count = item.rsplit(':', 1)
            ip = ip.strip()
            count = count.strip()
            try:
                count_int = int(count)
            except ValueError:
                print(f'ERROR: invalid replica count "{count}" for ip "{ip}" '
                      f'in --replicas (expected an integer)', file=sys.stderr)
                sys.exit(1)
            if count_int < 0:
                print(f'ERROR: negative replica count {count_int} for ip "{ip}" '
                      f'in --replicas', file=sys.stderr)
                sys.exit(1)
            result[ip] = count_int
        else:
            result[item.strip()] = 1
    return result


def apply_yaml(yaml_content, namespace='default', timeout=60):
    """Apply YAML manifest to cluster.

    The temp file is always cleaned up in finally; a cleanup failure is
    logged (not silently swallowed) but does not mask the apply result,
    since a leftover temp file is non-fatal compared to a failed apply.
    """
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
        f.write(yaml_content)
        tmp_path = f.name

    try:
        result = run_kubectl(['apply', '-f', tmp_path, '-n', namespace],
                             timeout=timeout)
        return result is not None
    finally:
        try:
            os.unlink(tmp_path)
        except OSError as e:
            print(f'WARNING: failed to remove temp manifest {tmp_path}: {e}',
                  file=sys.stderr)


def generate_pod_manifest(config, template_content):
    """Generate Pod YAML manifests from template.

    Args:
        config: dict with:
            - image (str): container image
            - name_prefix (str): pod name prefix
            - namespace (str): namespace
            - cpu (str): CPU limit
            - memory (str): memory limit
            - requests_cpu (str): CPU request
            - requests_memory (str): memory request
            - pod_replicas (dict): {node_ip: count} (explicit spec)
            - pods_per_node (int): number of pods per node (if pod_replicas not set)
        template_content: YAML template string
    """
    import copy
    import yaml

    image = config.get('image')
    name_prefix = config.get('name_prefix')
    namespace = config.get('namespace', 'default')
    cpu_limit = config.get('cpu', '8')
    memory_limit = config.get('memory', '16Gi')
    requests_cpu = config.get('requests_cpu', cpu_limit)
    requests_memory = config.get('requests_memory', memory_limit)
    pod_replicas = config.get('pod_replicas') or {}
    pods_per_node = config.get('pods_per_node') or 0

    if not image:
        raise ValueError('image is required')
    if not name_prefix:
        raise ValueError('name_prefix is required')

    # Get all node IPs and names
    result = run_kubectl([
        'get', 'nodes', '-o', 'json',
    ], check=False)
    all_nodes = []
    if result and result.returncode == 0:
        for item in json.loads(result.stdout).get('items', []):
            for addr in item.get('status', {}).get('addresses', []):
                if addr.get('type') == 'InternalIP':
                    all_nodes.append({
                        'ip': addr.get('address', ''),
                        'name': item.get('metadata', {}).get('name', ''),
                    })
                    break

    # Determine target nodes and replica counts
    if pod_replicas:
        # Explicit per-node spec: {ip: count, ...}
        target_replicas = pod_replicas
    elif pods_per_node > 0:
        # Simple mode: N pods per node
        target_replicas = {node['ip']: pods_per_node for node in all_nodes}
    else:
        # Default: 1 pod per node
        target_replicas = {node['ip']: 1 for node in all_nodes}

    # Build IP to node name mapping
    ip_to_node = {node['ip']: node['name'] for node in all_nodes}

    # Validate user-specified IPs against discovered nodes. A typo'd or
    # stale IP must not silently fall back to using the raw IP string as
    # nodeName -- that would make Kubernetes fail to schedule the Pod with
    # a cryptic "node not found" event. Only skip this check when we could
    # not discover any node at all (kubectl get nodes failed), so we don't
    # block the user in a degraded cluster where discovery itself failed.
    if all_nodes and pod_replicas:
        unknown = [ip for ip in pod_replicas if ip not in ip_to_node]
        if unknown:
            raise ValueError(
                'Unknown node IP(s) in --replicas not found in cluster: '
                f'{", ".join(unknown)}. '
                f'Known node IPs: {", ".join(sorted(ip_to_node))}')

    # Parse the YAML template once; each pod gets its own deep copy so the
    # original is not mutated across iterations.
    template_spec = yaml.safe_load(template_content)

    manifest_parts = []
    container_index = 0  # Global counter for container naming

    for node_ip, replica_count in target_replicas.items():
        if replica_count <= 0:
            continue

        node_name = ip_to_node.get(node_ip, node_ip)
        # Sanitize node_name for use in pod name (k8s naming rules)
        safe_node_name = node_name.replace('.', '-').replace('_', '-')

        for replica in range(replica_count):
            # Pod name: prefix-replica-nodeName (unique across nodes)
            pod_name = f'{name_prefix}-{replica}-{safe_node_name}'

            # Container name: prefix-globalIndex (unique across all pods)
            container_name = f'{name_prefix}-{container_index}'
            container_index += 1

            # Deep-copy the parsed template so each pod has an independent spec
            pod_spec = copy.deepcopy(template_spec)

            # Update metadata
            pod_spec.setdefault('metadata', {})
            pod_spec['metadata']['name'] = pod_name
            if 'namespace' in config:
                pod_spec['metadata']['namespace'] = namespace

            # Update nodeName
            pod_spec.setdefault('spec', {})
            pod_spec['spec']['nodeName'] = node_name

            # Update container resources and name
            containers = pod_spec.get('spec', {}).get('containers', [])
            for container in containers:
                container['name'] = container_name
                container.setdefault('resources', {})
                # Update limits
                container['resources'].setdefault('limits', {})
                container['resources']['limits']['cpu'] = cpu_limit
                container['resources']['limits']['memory'] = memory_limit
                # Update requests
                container['resources'].setdefault('requests', {})
                container['resources']['requests']['cpu'] = requests_cpu
                container['resources']['requests']['memory'] = requests_memory
                # Update image
                if 'image' in container:
                    container['image'] = image

            # Dump to YAML
            manifest_parts.append(yaml.dump(pod_spec, default_flow_style=False, sort_keys=False))

    return '---\n'.join(manifest_parts)


def wait_for_pods(name_prefix, namespace, timeout=300):
    """Wait for pods to be ready.

    A kubectl timeout during a single poll is treated as a transient polling
    failure: the error is logged and the loop continues (rather than crashing
    with an uncaught TimeoutExpired), because the API server may briefly be
    slow or unreachable. Only an overall wall-clock timeout or per-failed-pod
    condition ends the wait with a non-success result.
    """
    import time
    start_time = time.time()
    seen_any = False

    while time.time() - start_time < timeout:
        try:
            result = run_kubectl([
                'get', 'pods', '-n', namespace,
                '-o', 'jsonpath={range .items[*]}{.metadata.name}{"\\t"}{.status.phase}{"\\n"}{end}'
            ], check=False, timeout=timeout)
        except subprocess.TimeoutExpired:
            elapsed = int(time.time() - start_time)
            print(f'  [{elapsed}s] kubectl get pods timed out, retrying...',
                  file=sys.stderr)
            time.sleep(5)
            continue

        pending = 0
        running = 0
        failed = 0

        if result and result.returncode == 0:
            for line in result.stdout.strip().split('\n'):
                if not line:
                    continue
                parts = line.split('\t')
                if len(parts) >= 2:
                    pod_name, phase = parts[0], parts[1]
                    if pod_name.startswith(name_prefix):
                        seen_any = True
                        if phase == 'Pending':
                            pending += 1
                        elif phase == 'Running':
                            running += 1
                        elif phase in ('Failed', 'Error'):
                            failed += 1

        elapsed = int(time.time() - start_time)
        print(f'  [{elapsed}s] Running: {running}, Pending: {pending}, Failed: {failed}')

        # Only declare success once we have actually observed matching pods;
        # an empty first poll (pods not yet scheduled) must not short-circuit
        # to a spurious success.
        if seen_any and pending == 0 and failed == 0:
            print(f'\nAll {running} pods are running!')
            return True

        if failed > 0:
            print(f'\nERROR: {failed} pods failed')
            return False

        time.sleep(5)

    print(f'\nTIMEOUT after {timeout}s: {running} running, {pending} pending')
    return False


def cmd_deploy(args):
    """Deploy pods."""
    namespace = args.namespace or 'default'
    name_prefix = args.prefix
    image = args.image
    cpu = args.cpu or '8'
    memory = args.memory or '16Gi'
    requests_cpu = args.requests_cpu or cpu
    requests_memory = args.requests_memory or memory
    replicas_str = args.replicas
    pods_per_node = args.pods_per_node or 0
    yaml_path = args.yaml

    # Parse replicas (explicit per-node spec takes precedence)
    pod_replicas = parse_replicas(replicas_str) if replicas_str else {}

    print('Deployment config:')
    print(f'  name_prefix: {name_prefix}')
    print(f'  image: {image}')
    print(f'  cpu: {cpu} (limits)')
    print(f'  memory: {memory} (limits)')
    print(f'  requests_cpu: {requests_cpu}')
    print(f'  requests_memory: {requests_memory}')
    if pod_replicas:
        print(f'  replicas: {pod_replicas}')
    elif pods_per_node:
        print(f'  pods_per_node: {pods_per_node}')
    else:
        print('  replicas: (default 1 per node)')

    # Load YAML template
    if not os.path.exists(yaml_path):
        print(f'ERROR: YAML template not found: {yaml_path}', file=sys.stderr)
        return 1

    with open(yaml_path) as f:
        template_content = f.read()

    print(f'\nLoaded template from: {yaml_path}')

    # Build config
    config = {
        'image': image,
        'name_prefix': name_prefix,
        'namespace': namespace,
        'cpu': cpu,
        'memory': memory,
        'requests_cpu': requests_cpu,
        'requests_memory': requests_memory,
        'pod_replicas': pod_replicas,
        'pods_per_node': pods_per_node,
    }

    # Generate manifest
    try:
        manifest = generate_pod_manifest(config, template_content)
    except subprocess.TimeoutExpired:
        print('ERROR: timed out while discovering cluster nodes '
              '(kubectl get nodes)', file=sys.stderr)
        return 1
    except ValueError as e:
        print(f'ERROR: {e}', file=sys.stderr)
        return 1

    if not manifest.strip():
        print('No pods to deploy (all replica counts are 0)')
        return 0

    # Count pods
    pod_count = len([d for d in manifest.split('---\n') if d.strip()])
    print(f'\nGenerated manifest for {pod_count} pod(s)')

    if args.dry_run:
        print('\n--- Dry run, manifest not applied ---')
        print(manifest)
        return 0

    # Clean up existing pods with same prefix before applying
    if args.force:
        existing_pods = get_pods_by_prefix(namespace, name_prefix)
        if existing_pods:
            existing_names = [p['name'] for p in existing_pods]
            print(f'\nCleaning up {len(existing_names)} existing pod(s)...')
            try:
                delete_pods_parallel(existing_names, namespace, args.timeout)
            except (RuntimeError, subprocess.TimeoutExpired) as e:
                print(f'ERROR: Failed to clean up existing pods: {e}', file=sys.stderr)
                return 1
            import time
            time.sleep(2)  # Wait for cleanup to take effect

    # Apply manifest
    print('\nApplying manifest...')
    try:
        applied = apply_yaml(manifest, namespace, args.timeout)
    except subprocess.TimeoutExpired:
        print('ERROR: timed out applying manifest (kubectl apply)',
              file=sys.stderr)
        return 1
    if not applied:
        print('ERROR: Failed to apply manifest', file=sys.stderr)
        return 1

    print('Manifest applied successfully')

    # Wait for pods
    if args.wait:
        print(f'\nWaiting for pods (timeout: {args.timeout}s)...')
        try:
            ok = wait_for_pods(name_prefix, namespace, args.timeout)
        except subprocess.TimeoutExpired:
            print('ERROR: timed out while waiting for pods (kubectl get pods)',
                  file=sys.stderr)
            return 1
        if not ok:
            return 1

    return 0


def cmd_delete(args):
    """Delete pods."""
    namespace = args.namespace or 'default'
    name_prefix = args.prefix

    print(f'Deleting pods with prefix "{name_prefix}" in namespace "{namespace}"...')

    result = run_kubectl([
        'get', 'pods', '-n', namespace,
        '-o', 'jsonpath={range .items[*]}{.metadata.name}{"\\n"}{end}'
    ], check=False, timeout=args.timeout)

    if not result or result.returncode != 0:
        print('ERROR: Failed to get pods', file=sys.stderr)
        return 1

    pods = [p for p in result.stdout.strip().split('\n') if p.startswith(name_prefix)]

    if not pods:
        print('No matching pods found')
        return 0

    print(f'Found {len(pods)} pods:')
    for pod in pods[:10]:
        print(f'  - {pod}')
    if len(pods) > 10:
        print(f'  ... and {len(pods) - 10} more')

    if args.dry_run:
        print('\n--- Dry run, pods not deleted ---')
        return 0

    if not args.force:
        confirm = input('\nConfirm deletion (y/N): ')
        if confirm.lower() != 'y':
            print('Cancelled')
            return 0

    try:
        delete_pods_parallel(pods, namespace, args.timeout)
    except (RuntimeError, subprocess.TimeoutExpired) as e:
        print(f'ERROR: {e}', file=sys.stderr)
        return 1

    print(f'Deleted {len(pods)} pods')
    return 0


def cmd_status(args):
    """Show pod status."""
    namespace = args.namespace or 'default'
    name_prefix = args.prefix

    result = run_kubectl([
        'get', 'pods', '-n', namespace,
        '-o', 'wide', '--no-headers'
    ], check=False)

    if not result or result.returncode != 0:
        print('ERROR: Failed to get pods', file=sys.stderr)
        return 1

    print(f'Pods with prefix "{name_prefix}" in namespace "{namespace}":')
    print(f'{"NAME":<45} {"STATUS":<12} {"IP":<18} {"NODE"}')
    print('-' * 100)

    count = 0
    for line in result.stdout.strip().split('\n'):
        if not line:
            continue
        parts = line.split()
        if parts and parts[0].startswith(name_prefix):
            count += 1
            name = parts[0]
            status = parts[2] if len(parts) > 2 else '-'
            ip = parts[5] if len(parts) > 5 else '-'
            node = parts[6] if len(parts) > 6 else '-'
            print(f'{name:<45} {status:<12} {ip:<18} {node}')

    print(f'\nTotal: {count} pods')
    return 0


def main():
    parser = argparse.ArgumentParser(
        description='Deploy k8s Pods from YAML template',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''
Examples:
  # Deploy 2 pods on node 10.0.0.1, 1 pod on 10.0.0.2, skip 10.0.0.3
  python3 deploy_pods.py deploy \\
    --image my-registry.com/worker:latest \\
    --prefix ds-worker \\
    --yaml config/pod_config.yaml.example \\
    --replicas "10.0.0.1:2,10.0.0.2:1,10.0.0.3:0"

  # Deploy with custom resources
  python3 deploy_pods.py deploy \\
    --image my-registry.com/worker:latest \\
    --prefix ds-worker \\
    --yaml config/pod_config.yaml.example \\
    --cpu 16 --memory 32Gi \\
    --requests-cpu 8 --requests-memory 16Gi

  # Deploy and wait for ready
  python3 deploy_pods.py deploy \\
    --image my-registry.com/worker:latest \\
    --prefix ds-worker \\
    --yaml config/pod_config.yaml.example \\
    --wait --timeout 300

  # Delete pods
  python3 deploy_pods.py delete --prefix ds-worker

  # Show status
  python3 deploy_pods.py status --prefix ds-worker
''')
    subparsers = parser.add_subparsers(dest='command', help='Command')

    # Common args
    common_parser = argparse.ArgumentParser(add_help=False)
    common_parser.add_argument('--namespace', '-n', default='default',
                               help='k8s namespace (default: default)')
    common_parser.add_argument('--prefix', '-p', required=True,
                               help='Pod name prefix (required)')

    # Deploy
    deploy_parser = subparsers.add_parser('deploy', parents=[common_parser],
                                          help='Deploy pods')
    deploy_parser.add_argument('--image', '-i', required=True,
                               help='Container image (required)')
    deploy_parser.add_argument('--yaml', '-y',
                               default='config/pod_config.yaml.example',
                               help='YAML template (default: config/pod_config.yaml.example)')
    deploy_parser.add_argument('--cpu', default='8',
                               help='CPU limits (default: 8)')
    deploy_parser.add_argument('--memory', '-m', default='16Gi',
                               help='Memory limits (default: 16Gi)')
    deploy_parser.add_argument('--requests-cpu',
                               help='CPU requests (default: same as --cpu)')
    deploy_parser.add_argument('--requests-memory',
                               help='Memory requests (default: same as --memory)')
    deploy_parser.add_argument('--replicas', '-r',
                               help='Replica spec: "ip1:count1,ip2:count2,..." '
                                    '(0 = skip node)')
    deploy_parser.add_argument('--pods-per-node', type=int,
                               help='Number of pods per node (simpler than --replicas). '
                                    'Example: --pods-per-node 3 deploys 3 pods on each node')
    deploy_parser.add_argument('--dry-run', action='store_true',
                               help='Show manifest without applying')
    deploy_parser.add_argument('--force', '-f', action='store_true',
                               help='Delete existing pods with same prefix before deploying')
    deploy_parser.add_argument('--wait', action='store_true',
                               help='Wait for pods to be ready')
    deploy_parser.add_argument('--timeout', type=int, default=300,
                               help='kubectl timeout in seconds, also used as '
                                    'wait timeout (default: 300s)')

    # Delete
    delete_parser = subparsers.add_parser('delete', parents=[common_parser],
                                          help='Delete pods')
    delete_parser.add_argument('--force', '-f', action='store_true',
                               help='Force delete without confirmation')
    delete_parser.add_argument('--dry-run', action='store_true',
                               help='Show pods without deleting')
    delete_parser.add_argument('--timeout', type=int, default=300,
                               help='kubectl timeout in seconds (default: 300s)')

    # Status
    status_parser = subparsers.add_parser('status', parents=[common_parser],
                                          help='Show pod status')

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        return 1

    if args.command == 'deploy':
        return cmd_deploy(args)
    elif args.command == 'delete':
        return cmd_delete(args)
    elif args.command == 'status':
        return cmd_status(args)

    return 0


if __name__ == '__main__':
    sys.exit(main())
