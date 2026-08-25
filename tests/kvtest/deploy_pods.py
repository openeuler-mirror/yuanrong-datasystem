#!/usr/bin/env python3
"""Deploy k8s Pods from YAML template.

Usage:
    python3 deploy_pods.py deploy --image xxx --prefix xxx [--yaml template.yaml]
    python3 deploy_pods.py delete --prefix xxx
    python3 deploy_pods.py status --prefix xxx

Node discovery reuses deploy_common.discover_nodes (the single canonical
kubectl-get-nodes helper, sorted by node name for deterministic distribution)
rather than a local copy, so deploy_pods depends on deploy_common for that
one helper; everything else (kubectl transport, manifest apply/wait/delete)
stays self-contained here.
"""

import argparse
import json
import os
import subprocess
import sys
import tempfile

from deploy_common import discover_nodes, log_error, log_info, setup_logging

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
        log_error(f'ERROR: kubectl timeout for: {" ".join(args)}')
        raise
    except subprocess.CalledProcessError as e:
        log_error(f'ERROR: kubectl failed for: {" ".join(args)}')
        if e.stderr:
            log_error(f'  stderr: {e.stderr}')
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
            log_error(f'ERROR: delete {pod} failed: {e}')
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
    log_info(f'  Deleted {len(pod_names) - len(failed)}/{len(pod_names)} pod(s)')
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
                log_error(f'ERROR: invalid replica count "{count}" for ip "{ip}" '
                          f'in --replicas (expected an integer)')
                sys.exit(1)
            if count_int < 0:
                log_error(f'ERROR: negative replica count {count_int} for ip "{ip}" '
                          f'in --replicas')
                sys.exit(1)
            result[ip] = count_int
        else:
            result[item.strip()] = 1
    return result


def parse_replicas_pct(spec):
    """Parse a percentage-based replica spec: ``"PCT:COUNT,PCT:COUNT,..."``.

    Each entry means "PCT percent of discovered nodes each get COUNT pods".
    PCT may be a float (e.g. ``"33.3:1,66.7:2"``); COUNT must be a
    non-negative integer. Returns a list of ``(pct_float, count_int)`` tuples
    in input order. Raises SystemExit on a malformed entry so the user gets
    a clear message instead of a ValueError traceback.

    Example: ``"30:0,60:1,10:2"`` -> ``[(30.0, 0), (60.0, 1), (10.0, 2)]``.
    """
    if not spec:
        return []
    buckets = []
    for item in spec.split(','):
        item = item.strip()
        if not item:
            continue
        if ':' not in item:
            log_error(f'ERROR: invalid --replicas-pct entry "{item}": expected '
                      f'"PCT:COUNT" (e.g. "30:0")')
            sys.exit(1)
        pct_str, count_str = item.rsplit(':', 1)
        try:
            pct = float(pct_str.strip())
        except ValueError:
            log_error(f'ERROR: invalid percentage "{pct_str}" in --replicas-pct '
                      f'entry "{item}"')
            sys.exit(1)
        try:
            count = int(count_str.strip())
        except ValueError:
            log_error(f'ERROR: invalid count "{count_str}" in --replicas-pct '
                      f'entry "{item}"')
            sys.exit(1)
        if pct < 0:
            log_error(f'ERROR: negative percentage {pct} in --replicas-pct '
                      f'entry "{item}"')
            sys.exit(1)
        if count < 0:
            log_error(f'ERROR: negative count {count} in --replicas-pct entry '
                      f'"{item}"')
            sys.exit(1)
        if pct == 0:
            log_error(f'WARNING: --replicas-pct entry "{item}" has 0%, which '
                      f'has no effect')
        buckets.append((pct, count))
    return buckets


def distribute_nodes_by_percentage(nodes, pct_spec):
    """Distribute nodes across percentage buckets via Largest Remainder Method.

    Args:
        nodes: list of ``{'ip', 'name'}`` (already discovered; the caller
            should have sorted them by name for deterministic assignment).
        pct_spec: list of ``(pct, count)`` tuples from ``parse_replicas_pct``.

    Returns ``(target_replicas, bucket_summary)``:
        target_replicas: ``{node_ip: count}`` covering EVERY node (count may
            be 0 for nodes that land in a 0-pod bucket).
        bucket_summary: list of ``(pct, count, assigned_node_count)`` in spec
            order, for plan printing and assertions.

    Largest Remainder Method: floor each bucket's raw node count, then hand
    the leftover nodes (``n - sum(floors)``) to the buckets with the largest
    fractional remainders, breaking ties by original spec order (stable) so
    the result is deterministic and debuggable. This guarantees
    ``sum(assigned_node_count) == len(nodes)`` exactly.

    Raises ValueError when the percentages do not sum to 100 (within 0.01
    float tolerance), there are no nodes to distribute over, or the spec is
    empty.
    """
    if not pct_spec:
        raise ValueError('empty --replicas-pct spec')
    total_pct = sum(pct for pct, _ in pct_spec)
    if abs(total_pct - 100.0) > 0.01:
        raise ValueError(
            f'--replicas-pct percentages sum to {total_pct:g}, expected 100')
    n = len(nodes)
    if n == 0:
        raise ValueError('no cluster nodes discovered; cannot distribute')

    # raw[i] = pct[i]/100 * n; floor loses the fractional part, summed loss
    # is < len(spec), so leftover < len(spec) and the index below is safe.
    raw = [pct / 100.0 * n for pct, _ in pct_spec]
    floors = [int(r) for r in raw]  # int() == floor() for non-negative r
    remainders = [(raw[i] - floors[i], i) for i in range(len(pct_spec))]
    leftover = n - sum(floors)
    # Sort by remainder desc; stable on original index for deterministic ties.
    remainders.sort(key=lambda x: (-x[0], x[1]))
    for k in range(leftover):
        floors[remainders[k][1]] += 1

    # Assign nodes contiguously to each bucket in spec order so the plan is
    # readable: "first N0 nodes get count0, next N1 get count1, ...".
    target = {}
    pos = 0
    for i, (pct, count) in enumerate(pct_spec):
        for _ in range(floors[i]):
            target[nodes[pos]['ip']] = count
            pos += 1
    bucket_summary = [(pct_spec[i][0], pct_spec[i][1], floors[i])
                      for i in range(len(pct_spec))]
    return target, bucket_summary


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
            log_error(f'WARNING: failed to remove temp manifest {tmp_path}: {e}')


def generate_pod_manifest(config, template_content, target_replicas,
                          ip_to_node):
    """Generate Pod YAML manifests from a pre-computed per-node replica plan.

    Distribution (node discovery, spec parsing, percentage rounding) lives
    in ``cmd_deploy``; this function only renders one deep-copied pod spec
    per (node_ip, count) entry so it stays free of kubectl and is purely
    about manifest construction.

    Args:
        config: dict with image, name_prefix, namespace, cpu, memory,
            requests_cpu, requests_memory.
        template_content: YAML template string.
        target_replicas: ``{node_ip: count}`` as computed by the caller
            (explicit ``--replicas``, percentage distribution, uniform
            ``--pods-per-node``, or the default 1-per-node). Entries with
            count <= 0 are skipped.
        ip_to_node: ``{node_ip: node_name}`` mapping for ``spec.nodeName``;
            a missing entry falls back to the raw IP string (the degraded
            -cluster path where discovery itself failed).
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

    if not image:
        raise ValueError('image is required')
    if not name_prefix:
        raise ValueError('name_prefix is required')

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
            log_error(f'  [{elapsed}s] kubectl get pods timed out, retrying...')
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
        log_info(f'  [{elapsed}s] Running: {running}, Pending: {pending}, Failed: {failed}')

        # Only declare success once we have actually observed matching pods;
        # an empty first poll (pods not yet scheduled) must not short-circuit
        # to a spurious success.
        if seen_any and pending == 0 and failed == 0:
            log_info(f'\nAll {running} pods are running!')
            return True

        if failed > 0:
            log_info(f'\nERROR: {failed} pods failed')
            return False

        time.sleep(5)

    log_info(f'\nTIMEOUT after {timeout}s: {running} running, {pending} pending')
    return False


def cmd_deploy(args):
    """Deploy pods.

    Distribution is selected by exactly one of the mutually-exclusive flags:
    ``--replicas`` (explicit per-node ``ip:count``), ``--replicas-pct``
    (percentage-of-nodes buckets), or ``--pods-per-node`` (uniform); the
    default is 1 pod per discovered node. Node discovery, spec parsing,
    percentage rounding, and IP validation all happen here so manifest
    generation stays free of kubectl and purely about rendering pod specs.
    """
    namespace = args.namespace or 'default'
    name_prefix = args.prefix
    image = args.image
    cpu = args.cpu or '8'
    memory = args.memory or '16Gi'
    requests_cpu = args.requests_cpu or cpu
    requests_memory = args.requests_memory or memory
    replicas_str = args.replicas
    # getattr: deploy_coordinator._build_deploy_pods_args builds a hand-rolled
    # SimpleNamespace without this field; tolerate its absence so the existing
    # coordinator caller keeps working without importing percentage support.
    replicas_pct_str = getattr(args, 'replicas_pct', None)
    pods_per_node = args.pods_per_node or 0
    yaml_path = args.yaml

    # Discover nodes once; every distribution mode needs them. discover_nodes
    # (shared with deploy_common) swallows kubectl failure/timeout and returns
    # [] so callers handle "no nodes" per path (percentage -> hard error,
    # explicit --replicas -> degraded-cluster passthrough, uniform/default ->
    # no pods to deploy).
    nodes = discover_nodes(timeout=args.timeout)
    ip_to_node = {node['ip']: node['name'] for node in nodes}

    # Parse specs. The mutually-exclusive CLI group guarantees only one is
    # set when invoked via argparse; the precedence below also handles the
    # hand-rolled Namespace from deploy_coordinator (which sets replicas and
    # pods_per_node=None, no replicas_pct).
    pod_replicas = parse_replicas(replicas_str) if replicas_str else {}
    pct_spec = parse_replicas_pct(replicas_pct_str) if replicas_pct_str else []

    if pod_replicas:
        # Explicit per-node spec: {ip: count, ...}. Validate against
        # discovered nodes so a typo'd or stale IP surfaces before apply --
        # otherwise Kubernetes would fail to schedule with a cryptic
        # "node not found" event. Skip only when discovery itself failed,
        # to preserve the degraded-cluster path.
        if nodes:
            unknown = [ip for ip in pod_replicas if ip not in ip_to_node]
            if unknown:
                log_error('ERROR: Unknown node IP(s) in --replicas not found in '
                          f'cluster: {", ".join(unknown)}. Known node IPs: '
                          f'{", ".join(sorted(ip_to_node))}')
                return 1
        target_replicas = pod_replicas
        dist_label = f'--replicas (explicit): {pod_replicas}'
    elif pct_spec:
        try:
            target_replicas, bucket_summary = distribute_nodes_by_percentage(
                nodes, pct_spec)
        except ValueError as e:
            log_error(f'ERROR: {e}')
            return 1
        dist_label = (f'--replicas-pct "{replicas_pct_str}": '
                      + ', '.join(f'{pct:g}% x{count} -> {assigned} node(s)'
                                  for pct, count, assigned in bucket_summary))
    elif pods_per_node > 0:
        target_replicas = {node['ip']: pods_per_node for node in nodes}
        dist_label = f'--pods-per-node {pods_per_node}'
    else:
        target_replicas = {node['ip']: 1 for node in nodes}
        dist_label = 'default 1 per node'

    total_pods = sum(c for c in target_replicas.values() if c > 0)

    log_info('Deployment config:')
    log_info(f'  name_prefix: {name_prefix}')
    log_info(f'  image: {image}')
    log_info(f'  cpu: {cpu} (limits)')
    log_info(f'  memory: {memory} (limits)')
    log_info(f'  requests_cpu: {requests_cpu}')
    log_info(f'  requests_memory: {requests_memory}')
    log_info(f'  distribution: {dist_label}')
    log_info(f'  discovered nodes: {len(nodes)}')
    if nodes:
        for node in nodes:
            c = target_replicas.get(node['ip'], 0)
            log_info(f'    {node["name"]} ({node["ip"]}): {c} pod(s)')
    log_info(f'  total pods to deploy: {total_pods}')

    # Load YAML template
    if not os.path.exists(yaml_path):
        log_error(f'ERROR: YAML template not found: {yaml_path}')
        return 1

    with open(yaml_path) as f:
        template_content = f.read()

    log_info(f'\nLoaded template from: {yaml_path}')

    # Build config (distribution is already resolved into target_replicas;
    # generate_pod_manifest only renders).
    config = {
        'image': image,
        'name_prefix': name_prefix,
        'namespace': namespace,
        'cpu': cpu,
        'memory': memory,
        'requests_cpu': requests_cpu,
        'requests_memory': requests_memory,
    }

    # Generate manifest
    try:
        manifest = generate_pod_manifest(config, template_content,
                                         target_replicas, ip_to_node)
    except ValueError as e:
        log_error(f'ERROR: {e}')
        return 1

    if not manifest.strip():
        log_info('No pods to deploy (all replica counts are 0)')
        return 0

    # Count pods
    pod_count = len([d for d in manifest.split('---\n') if d.strip()])
    log_info(f'\nGenerated manifest for {pod_count} pod(s)')

    if args.dry_run:
        log_info('\n--- Dry run, manifest not applied ---')
        log_info(manifest)
        return 0

    # Clean up existing pods with same prefix before applying
    if args.force:
        existing_pods = get_pods_by_prefix(namespace, name_prefix)
        if existing_pods:
            existing_names = [p['name'] for p in existing_pods]
            log_info(f'\nCleaning up {len(existing_names)} existing pod(s)...')
            try:
                delete_pods_parallel(existing_names, namespace, args.timeout)
            except (RuntimeError, subprocess.TimeoutExpired) as e:
                log_error(f'ERROR: Failed to clean up existing pods: {e}')
                return 1
            import time
            time.sleep(2)  # Wait for cleanup to take effect

    # Apply manifest
    log_info('\nApplying manifest...')
    try:
        applied = apply_yaml(manifest, namespace, args.timeout)
    except subprocess.TimeoutExpired:
        log_error('ERROR: timed out applying manifest (kubectl apply)')
        return 1
    if not applied:
        log_error('ERROR: Failed to apply manifest')
        return 1

    log_info('Manifest applied successfully')

    # Wait for pods
    if args.wait:
        log_info(f'\nWaiting for pods (timeout: {args.timeout}s)...')
        try:
            ok = wait_for_pods(name_prefix, namespace, args.timeout)
        except subprocess.TimeoutExpired:
            log_error('ERROR: timed out while waiting for pods (kubectl get pods)')
            return 1
        if not ok:
            return 1

    return 0


def cmd_delete(args):
    """Delete pods."""
    namespace = args.namespace or 'default'
    name_prefix = args.prefix

    log_info(f'Deleting pods with prefix "{name_prefix}" in namespace "{namespace}"...')

    result = run_kubectl([
        'get', 'pods', '-n', namespace,
        '-o', 'jsonpath={range .items[*]}{.metadata.name}{"\\n"}{end}'
    ], check=False, timeout=args.timeout)

    if not result or result.returncode != 0:
        log_error('ERROR: Failed to get pods')
        return 1

    pods = [p for p in result.stdout.strip().split('\n') if p.startswith(name_prefix)]

    if not pods:
        log_info('No matching pods found')
        return 0

    log_info(f'Found {len(pods)} pods:')
    for pod in pods[:10]:
        log_info(f'  - {pod}')
    if len(pods) > 10:
        log_info(f'  ... and {len(pods) - 10} more')

    if args.dry_run:
        log_info('\n--- Dry run, pods not deleted ---')
        return 0

    if not args.force:
        confirm = input('\nConfirm deletion (y/N): ')
        if confirm.lower() != 'y':
            log_info('Cancelled')
            return 0

    try:
        delete_pods_parallel(pods, namespace, args.timeout)
    except (RuntimeError, subprocess.TimeoutExpired) as e:
        log_error(f'ERROR: {e}')
        return 1

    log_info(f'Deleted {len(pods)} pods')
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
        log_error('ERROR: Failed to get pods')
        return 1

    log_info(f'Pods with prefix "{name_prefix}" in namespace "{namespace}":')
    log_info(f'{"NAME":<45} {"STATUS":<12} {"IP":<18} {"NODE"}')
    log_info('-' * 100)

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
            log_info(f'{name:<45} {status:<12} {ip:<18} {node}')

    log_info(f'\nTotal: {count} pods')
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

  # Deploy by percentage of nodes: 30% get 0 pods, 60% get 1, 10% get 2
  # (nodes are sorted by name then assigned contiguously to each bucket;
  #  rounding uses the Largest Remainder Method so the totals match exactly)
  python3 deploy_pods.py deploy \\
    --image my-registry.com/worker:latest \\
    --prefix ds-worker \\
    --yaml config/pod_config.yaml.example \\
    --replicas-pct "30:0,60:1,10:2" --dry-run

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
    # Distribution modes are mutually exclusive: only one of explicit
    # per-node, percentage-of-nodes, or uniform may drive a single deploy.
    # (Previously --replicas silently won over --pods-per-node; making them
    # exclusive surfaces a redundant-flag mistake instead of ignoring it.)
    dist_group = deploy_parser.add_mutually_exclusive_group()
    dist_group.add_argument('--replicas', '-r',
                            help='Replica spec: "ip1:count1,ip2:count2,..." '
                                 '(0 = skip node)')
    dist_group.add_argument('--pods-per-node', type=int,
                            help='Number of pods per node (uniform). '
                                 'Example: --pods-per-node 3 deploys 3 pods on each node')
    dist_group.add_argument('--replicas-pct',
                            help='Percentage-based distribution: '
                                 '"PCT:COUNT,PCT:COUNT,..." where PCT%% of '
                                 'discovered nodes each get COUNT pods. '
                                 'Example: "30:0,60:1,10:2" means 30%% of '
                                 'nodes get 0 pods, 60%% get 1, 10%% get 2. '
                                 'Percentages must sum to 100; nodes are '
                                 'sorted by name then assigned contiguously '
                                 'to each bucket; rounding uses the Largest '
                                 'Remainder Method so the assigned node '
                                 'count matches exactly.')
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
    setup_logging()

    if args.command == 'deploy':
        return cmd_deploy(args)
    elif args.command == 'delete':
        return cmd_delete(args)
    elif args.command == 'status':
        return cmd_status(args)

    return 0


if __name__ == '__main__':
    sys.exit(main())
