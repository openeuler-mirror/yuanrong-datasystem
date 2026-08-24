#!/usr/bin/env python3
"""Batch start/stop datasystem coordinators in k8s Pods.

Role-specific layer over deploy_common.py: knows that coordinators listen on
``coordinator_address``, are backed by the ``datasystem_coordinator`` binary,
do not support NUMA binding (dscli's numactl path applies to workers only),
and default to port 31511 / /tmp/coordinator.config.

Coordinator topology is etcd-like: a cluster runs 1, 3, or 5 coordinators.
This script is built on the same parallel batch framework as the worker
deployer so multi-replica deployments work without code changes, but the
typical case is a single coordinator pod.

Shared kubectl transport, procmon orchestration, and the stop/kill/check/
exec/collect/clean orchestration live in deploy_common.py.
"""

import argparse
import json
import os
import sys
from types import SimpleNamespace

import deploy_pods
from deploy_common import (
    DEFAULT_TIMEOUT,
    apply_config_overrides,
    check_process,
    cmd_check_impl,
    cmd_clean_shared,
    cmd_collect_shared,
    cmd_exec_shared,
    cmd_install_impl,
    cmd_install_shared,
    cmd_kill_shared,
    cmd_stop_shared,
    discover_nodes,
    do_for_all_pods,
    find_default_whl,
    get_pods,
    kubectl_exec_raw,
    read_remote_log_dir,
    resolve_procmon_dir,
    start_service,
    start_service_standalone,
)


PROCESS_NAME = 'datasystem_coordinator'
PROCESS_NAME_STANDALONE = 'coordinator_test'
ADDRESS_KEY = 'coordinator_address'


def start_coordinator(pod, namespace, config, coordinator_port, remote_config,
                      enable_procmon=True, procmon_remote_dir='/tmp',
                      timeout=DEFAULT_TIMEOUT):
    """Start a coordinator in a single pod.

    Delegates to deploy_common.start_service with the coordinator role's
    binding (datasystem_coordinator binary). The caller must have injected
    ``coordinator_address`` (and, for a multi-instance cluster, the shared
    ``coordinator_raft_initial_peers`` list -- see ``_inject_raft_initial_peers``)
    into ``config`` already (see cmd_start / cmd_deploy).
    """
    return start_service(pod, namespace, config, remote_config,
                         coordinator_port, PROCESS_NAME, enable_procmon,
                         procmon_remote_dir, numactl_opts=None, timeout=timeout)


def _inject_raft_initial_peers(cfg, pods, port):
    """Inject ``coordinator_raft_initial_peers`` from the full pod member list.

    Static-peers Raft requires every node to carry the same full member list
    (including self). With 2+ pods we inject the comma-joined ``ip:port``
    list so the coordinators run static-peers election; with a single pod we
    leave the field untouched so that coordinator runs in single-node
    no-election mode. The caller passes the *full* pod set (all pods that will
    form or rejoin the cluster), not just the one being started, so a restart
    of one member of an existing cluster still gets the full membership.
    """
    if len(pods) >= 2:
        peers = ','.join(f'{p["ip"]}:{port}' for p in pods)
        cfg['coordinator_raft_initial_peers'] = {'value': peers}


def cmd_start(args, pods):
    """Start coordinators from a config template.

    In standalone mode (--standalone), uses coordinator_test binary instead
    of dscli. The binary and .so deps must be pre-installed via
    ``install --standalone`` or the deploy command.
    """
    if getattr(args, 'standalone', False):
        return cmd_start_standalone(args, pods)

    with open(args.config) as f:
        config_template = json.load(f)

    # Default procmon dir to log_dir from coordinator config, fallback to
    # --remote-config dir.
    if args.procmon_dir is None:
        args.procmon_dir = resolve_procmon_dir(config_template, args.remote_config)

    if args.set:
        apply_config_overrides(config_template, args.set)
    else:
        print('\nNo config overrides specified')

    def do_op(pod):
        _, status, _ = check_process(pod, args.namespace, PROCESS_NAME,
                                    timeout=args.timeout)
        if status == 'alive':
            print(f'  {pod["name"]} ({pod["ip"]}) -> already running, skip')
            return True
        cfg = json.loads(json.dumps(config_template))
        cfg[ADDRESS_KEY]['value'] = f'{pod["ip"]}:{args.port}'
        _inject_raft_initial_peers(cfg, pods, args.port)
        return start_coordinator(pod, args.namespace, cfg, args.port,
                                 args.remote_config,
                                 enable_procmon=args.enable_procmon,
                                 procmon_remote_dir=args.procmon_dir,
                                 timeout=args.timeout)

    return do_for_all_pods(pods, do_op, 'Starting coordinators')


def cmd_start_standalone(args, pods):
    """Start coordinator_test binary in standalone mode."""
    if not getattr(args, 'jf', None):
        print('ERROR: --jf is required in standalone mode', file=sys.stderr)
        return 1
    with open(args.config) as f:
        config_template = json.load(f)

    if args.set:
        apply_config_overrides(config_template, args.set)

    remote_dir = getattr(args, 'remote_dir', None) or os.path.dirname(args.remote_config) or '/tmp/ds_coordinator'
    binary_name = PROCESS_NAME_STANDALONE

    extra = f'--coordinator {{pod_ip}}:{args.port} --hooks --ttl {args.ttl} --expected-member-count {args.expected_member_count}'

    def do_op(pod):
        _, status, _ = check_process(pod, args.namespace, binary_name,
                                     timeout=args.timeout)
        if status == 'alive':
            print(f'  {pod["name"]} ({pod["ip"]}) -> already running, skip')
            return True
        cfg = json.loads(json.dumps(config_template))
        cfg[ADDRESS_KEY] = {'value': f'{pod["ip"]}:{args.port}'}
        pod_extra = extra.replace('{pod_ip}', pod['ip'])
        return start_service_standalone(
            pod, args.namespace, binary_name, remote_dir, args.remote_config,
            args.jf, args.service, pod_extra,
            config=cfg,
            enable_procmon=args.enable_procmon,
            procmon_remote_dir=args.procmon_dir or '/tmp',
            port=args.port,
            process_name=binary_name,
            timeout=args.timeout)

    return do_for_all_pods(pods, do_op, 'Starting coordinators (standalone)')


def _distribute_instances_across_nodes(num_instances, nodes):
    """Spread N instances across M nodes as evenly as possible.

    Returns a ``{node_ip: count}`` dict with only non-zero counts. The first
    ``N % M`` nodes receive one extra instance so the spread is balanced
    (e.g. 5 instances across 3 nodes -> {ip0:2, ip1:2, ip2:1}).

    Raises ValueError when there is nothing to distribute or no nodes to
    spread across; the caller surfaces this as a hard error since a
    multi-instance deploy cannot place pods without nodes.
    """
    if num_instances <= 0:
        raise ValueError(
            f'--instances must be a positive integer, got {num_instances}')
    if not nodes:
        raise ValueError(
            'no cluster nodes discovered via kubectl get nodes; '
            'cannot spread instances')
    m = len(nodes)
    base = num_instances // m
    remainder = num_instances % m
    distribution = {}
    for i, node in enumerate(nodes):
        count = base + (1 if i < remainder else 0)
        if count > 0:
            distribution[node['ip']] = count
    return distribution


def _build_deploy_pods_args(args, prefix, replicas_str, dry_run):
    """Construct the Namespace ``deploy_pods.cmd_deploy`` expects.

    deploy_pods.cmd_deploy reads ``namespace/prefix/image/cpu/memory/
    requests_cpu/requests_memory/replicas/pods_per_node/yaml/dry_run/
    force/wait/timeout``. The pod-bringup knobs are forwarded from the
    user's args, and the computed ``replicas_str`` (built from
    ``--instances`` spread) is the authoritative spec; ``pods_per_node``
    stays None so deploy_pods uses the replicas string.

    ``wait`` is forced True (not exposed on the CLI) because the whl
    install and coordinator start steps require Running pods; deploy_pods
    skips its wait path under dry_run, so forcing True is safe there too.
    """
    return SimpleNamespace(
        namespace=args.namespace,
        prefix=prefix,
        image=args.image,
        cpu=args.cpu,
        memory=args.memory,
        requests_cpu=args.requests_cpu or args.cpu,
        requests_memory=args.requests_memory or args.memory,
        replicas=replicas_str,
        pods_per_node=None,
        yaml=args.yaml,
        dry_run=dry_run,
        force=args.force,
        wait=True,
        timeout=args.timeout,
    )


def cmd_deploy(args, pods=None):
    """Full lifecycle deploy: [optional create pods] + install + start.

    --standalone: install binary + .so, start coordinator_test.
    non-standalone: install whl, start via dscli.
    --image: create pods first. Without --image: use existing pods.
    """
    if not args.prefixes or len(args.prefixes) != 1:
        print('ERROR: deploy requires exactly one --prefix '
              '(e.g. -p coordinator-a)', file=sys.stderr)
        return 1
    prefix = args.prefixes[0]

    # Validate: --image requires --instances
    if args.image and not args.instances:
        print('ERROR: --instances is required when --image is set', file=sys.stderr)
        return 1
    # Validate: standalone requires --jf
    if getattr(args, 'standalone', False) and not getattr(args, 'jf', None):
        print('ERROR: --jf is required in standalone mode', file=sys.stderr)
        return 1

    # Step 1: create pods if --image is set
    if args.image:
        from deploy_common import create_pods
        pods = create_pods(prefix=prefix, namespace=args.namespace, image=args.image,
                           instances=args.instances, yaml=args.yaml,
                           cpu=args.cpu, memory=args.memory,
                           requests_cpu=args.requests_cpu, requests_memory=args.requests_memory,
                           force=args.force, dry_run=args.dry_run, timeout=args.timeout)
        if pods is None:
            return 1
        if args.dry_run:
            print('Dry run: skipped install and start')
            return 0
        if not pods:
            print('ERROR: no running pods found after bringup', file=sys.stderr)
            return 1
    else:
        if pods is None:
            pods = get_pods(args.namespace, args.prefixes)
        if not pods:
            print('ERROR: no running pods found matching prefix', file=sys.stderr)
            return 1
    print(f'\nFound {len(pods)} pod(s):')
    for p in pods:
        print(f'  {p["name"]} ({p["ip"]})')

    # Step 2 + 3: install + start
    if getattr(args, 'standalone', False):
        print('\n--- Step 2/2: installing binary + .so (standalone) ---')
        install_rc = cmd_install_shared(args, pods, PROCESS_NAME_STANDALONE, 'coordinator',
                                       os.path.dirname(os.path.abspath(__file__)),
                                       args.timeout)
        if install_rc != 0:
            print('ERROR: install failed', file=sys.stderr)
            return install_rc
        print('\n--- Step 2/2: starting coordinators (standalone) ---')
        return cmd_start_standalone(args, pods)
    else:
        print('\n--- Step 2/3: installing whl ---')
        if cmd_install_impl(pods, args.namespace, args.whl,
                            timeout=args.timeout) != 0:
            print('ERROR: whl install failed; leaving pods running '
                  'for inspection', file=sys.stderr)
            return 1

        print('\n--- Step 3/3: starting coordinators ---')
        with open(args.config) as f:
            config_template = json.load(f)
        if args.procmon_dir is None:
            args.procmon_dir = resolve_procmon_dir(config_template,
                                                   args.remote_config)
        if args.set:
            apply_config_overrides(config_template, args.set)
        else:
            print('\nNo config overrides specified')

        def do_op(pod):
            cfg = json.loads(json.dumps(config_template))
            cfg[ADDRESS_KEY]['value'] = f'{pod["ip"]}:{args.port}'
            _inject_raft_initial_peers(cfg, pods, args.port)
            return start_coordinator(pod, args.namespace, cfg, args.port,
                                     args.remote_config,
                                     enable_procmon=args.enable_procmon,
                                     procmon_remote_dir=args.procmon_dir,
                                     timeout=args.timeout)

        rc = do_for_all_pods(pods, do_op, 'Starting coordinators')
        if rc != 0:
            print('ERROR: some coordinators failed to start; pods left '
                  'running for inspection', file=sys.stderr)
        return rc


def cmd_stop(args, pods):
    """Stop coordinators gracefully."""
    return cmd_stop_shared(args, pods, PROCESS_NAME_STANDALONE, 'coordinators',
                           service_type='coordinator', timeout=args.timeout)


def cmd_kill(args, pods):
    """Force kill coordinators."""
    return cmd_kill_shared(args, pods, PROCESS_NAME_STANDALONE,
                           'coordinators', timeout=args.timeout)


def cmd_check(args, pods):
    """Check coordinators.

    With --standalone --ready, after checking process is alive, also polls:
    1. TCP port connectable on each coordinator
    2. For multi-replica: at least one coordinator log shows
       CONFIGURATION_COMMITTED (leader elected, cluster ready to serve)
    """
    import time
    proc = PROCESS_NAME_STANDALONE if getattr(args, 'standalone', False) else args.process

    # Step 1: Basic process check
    rc = cmd_check_impl(pods, args.namespace, proc,
                        'coordinator processes', args.timeout)
    if rc != 0:
        return rc

    # Step 2: Ready check (--ready, both standalone and dscli modes)
    if not getattr(args, 'ready', False):
        return 0

    port = getattr(args, 'port', 31511)

    # 2a: TCP port check on each pod
    print('\nChecking coordinator port readiness...')
    all_port_ok = True
    for pod in pods:
        pod_ip = pod['ip']
        r = kubectl_exec_raw(pod, namespace=args.namespace,
                             cmd=f'python3 -c "import socket;s=socket.socket();s.settimeout(2);'
                                 f's.connect((\\\"{pod_ip}\\\",{port}));s.close()" 2>/dev/null '
                                 f'&& echo OK || echo FAIL', timeout=10)
        status = r.strip() if r else 'FAIL'
        print(f'  {pod["name"]} ({pod_ip}:{port}) -> {status}')
        if status != 'OK':
            all_port_ok = False
    if not all_port_ok:
        print('Result: some coordinator ports not ready')
        return 1

    # 2b: Leader elected check (for multi-replica)
    expected_members = getattr(args, 'expected_member_count', 1)
    if expected_members > 1:
        print(f'\nChecking leader election (expected {expected_members} members)...')
        # Read log_dir from the remote config (same as collect command)
        remote_config = getattr(args, 'remote_config', '/tmp/coordinator.config')
        log_dir, _ = read_remote_log_dir(args.namespace, pods, remote_config, args.timeout)
        if not log_dir:
            print(f'  WARNING: log_dir not found in remote config, '
                  f'cannot check leader election')
            return 0
        leader_found = False
        check_pod = pods[0]
        for i in range(60):
            r = kubectl_exec_raw(
                check_pod, namespace=args.namespace,
                cmd=f'grep -rl "CONFIGURATION_COMMITTED\\|LEADER_ELECTED" '
                    f'{log_dir}/ 2>/dev/null | head -1',
                timeout=10)
            if r and r.strip():
                leader_found = True
                print(f'  Leader elected (detected in {check_pod["name"]})')
                break
            if i % 5 == 0:
                print(f'  Waiting for leader election... ({i}s)')
            time.sleep(1)
        if not leader_found:
            print('  Leader not elected after 60s')
            return 1

    print('\nAll coordinators ready')
    return 0


def cmd_exec(args, pods):
    """Execute command in pods."""
    return cmd_exec_shared(args, pods, args.timeout)


def cmd_collect(args, pods):
    """Collect coordinator logs from pods."""
    return cmd_collect_shared(args, pods, 'coordinator logs', args.timeout)


def cmd_clean(args, pods):
    """Kill coordinators and clean log directories."""
    return cmd_clean_shared(args, pods, PROCESS_NAME, 'coordinator logs', args.timeout)


def cmd_install(args, pods):
    """Install coordinator: always install whl first, then optionally copy
    standalone binary (standalone mode adds the binary on top of the whl)."""
    return cmd_install_shared(args, pods, PROCESS_NAME_STANDALONE, 'coordinator',
                              os.path.dirname(os.path.abspath(__file__)),
                              args.timeout)


def main():
    parser = argparse.ArgumentParser(
        description='Batch manage datasystem coordinators in k8s Pods',
        formatter_class=argparse.RawDescriptionHelpFormatter)
    subparsers = parser.add_subparsers(dest='action', help='Action to perform')

    # Common parent parser
    parent_parser = argparse.ArgumentParser(add_help=False)
    parent_parser.add_argument('-p', '--prefix', action='append', default=None,
                               dest='prefixes', metavar='PREFIX',
                               help='Pod name prefix to match (repeatable: '
                                    '-p coordinator-a -p coordinator-b). A pod '
                                    'is selected if it matches ANY prefix.')
    parent_parser.add_argument('-n', '--namespace', default='default',
                               help='k8s namespace (default: default)')
    parent_parser.add_argument('--timeout', type=int, default=DEFAULT_TIMEOUT,
                               help=f'Operation timeout in seconds (default: {DEFAULT_TIMEOUT})')

    # Start subcommand
    parser_start = subparsers.add_parser('start', parents=[parent_parser],
                                         help='Start coordinators from config')
    parser_start.add_argument('-c', '--config', required=True,
                              help='Path to coordinator.config template')
    parser_start.add_argument('--port', type=int, default=31511,
                              help='Coordinator port (default: 31511)')
    parser_start.add_argument('--remote-config', default='/tmp/coordinator.config',
                              help='Config path inside pod (default: /tmp/coordinator.config)')
    parser_start.add_argument('--set', '-s', action='append', default=[],
                              help='Add/override config values (format: key=value). '
                                   'Common coordinator keys: log_dir, log_filename, '
                                   'minloglevel, rpc_thread_num, '
                                   'watch_event_dispatch_thread, '
                                   'coordinator_rpc_stub_cache_size, '
                                   'max_log_size, max_log_file_num, '
                                   'log_retention_day. Example: --set rpc_thread_num=128')
    parser_start.add_argument('--enable-procmon', action='store_true', default=False,
                              dest='enable_procmon',
                              help='Start procmon.py for coordinator monitoring (default: disabled)')
    parser_start.add_argument('--no-procmon', action='store_false',
                              dest='enable_procmon',
                              help='Disable procmon.py monitoring (default)')
    parser_start.add_argument('--procmon-dir', default=None,
                               help='Remote directory for procmon files (default: same as --remote-config dir)')
    # Standalone mode (coordinator_test binary instead of dscli)
    parser_start.add_argument('-S', '--standalone', action='store_true', default=False,
                               help='Use coordinator_test binary instead of dscli')
    parser_start.add_argument('--jf', default=None,
                               help='JF mock address for service discovery (standalone mode only)')
    parser_start.add_argument('--service', default='kvcache_coordinator',
                               help='JF service name (standalone mode only, default: kvcache_coordinator)')
    parser_start.add_argument('--ttl', type=int, default=30,
                               help='Heartbeat TTL seconds (standalone mode only, default: 30)')
    parser_start.add_argument('--expected-member-count', type=int, default=1,
                               help='Raft member count for multi-replica (standalone mode only, default: 1)')
    parser_start.add_argument('--remote-dir', default='/tmp/ds_coordinator',
                               help='Remote directory with standalone binary (must match install --remote-dir)')

    # Stop subcommand (graceful stop using dscli)
    parser_stop = subparsers.add_parser('stop', parents=[parent_parser],
                                        help='Stop coordinators gracefully')
    parser_stop.add_argument('--remote-config', default='/tmp/coordinator.config',
                             help='Config file path (default: /tmp/coordinator.config)')
    parser_stop.add_argument('-S', '--standalone', action='store_true', default=False)

    # Kill subcommand (force kill using kill -9)
    parser_kill = subparsers.add_parser('kill', parents=[parent_parser],
                                        help='Force kill coordinators')
    parser_kill.add_argument('--process', default=PROCESS_NAME,
                             help=f'Process name to kill (default: {PROCESS_NAME})')
    parser_kill.add_argument('-S', '--standalone', action='store_true', default=False)

    # Check subcommand
    parser_check = subparsers.add_parser('check', parents=[parent_parser],
                                         help='Check coordinator status')
    parser_check.add_argument('--process', default=PROCESS_NAME,
                              help=f'Process name to check (default: {PROCESS_NAME})')
    parser_check.add_argument('-S', '--standalone', action='store_true', default=False)
    parser_check.add_argument('--ready', action='store_true', default=False,
                               help='Wait for coordinator readiness: port connectable, '
                                    'leader elected (standalone only)')
    parser_check.add_argument('--port', type=int, default=31511,
                               help='Coordinator port for TCP check (default: 31511)')
    parser_check.add_argument('--expected-member-count', type=int, default=1,
                               help='Expected member count for leader election check (default: 1)')
    parser_check.add_argument('--remote-config', default='/tmp/coordinator.config',
                               help='Remote config path (used to read log_dir for leader check)')

    # Exec subcommand
    parser_exec = subparsers.add_parser('exec', parents=[parent_parser],
                                        help='Execute command in pods')
    parser_exec.add_argument('--cmd', '-c', required=True,
                             help='Command to execute (required)')

    # Collect subcommand
    parser_collect = subparsers.add_parser('collect', parents=[parent_parser],
                                           help='Collect coordinator logs from pods')
    parser_collect.add_argument('--remote-config', default='/tmp/coordinator.config',
                                help='Config path inside pod (default: /tmp/coordinator.config)')
    parser_collect.add_argument('-o', '--output', default='collected_coordinator_logs',
                                help='Local output directory (default: collected_coordinator_logs)')

    # Clean subcommand
    parser_clean = subparsers.add_parser('clean', parents=[parent_parser],
                                         help='Kill coordinators and clean log directories')
    parser_clean.add_argument('--remote-config', default='/tmp/coordinator.config',
                              help='Config path inside pod (default: /tmp/coordinator.config)')
    parser_clean.add_argument('-S', '--standalone', action='store_true', default=False)

    # Install subcommand
    parser_install = subparsers.add_parser('install', parents=[parent_parser],
                                           help='Install coordinator binary or whl')
    parser_install.add_argument('-S', '--standalone', action='store_true', default=False,
                                help='Install standalone binary + .so (no whl)')
    parser_install.add_argument('--whl', default=find_default_whl(),
                                help='Path to datasystem whl package (non-standalone mode)')
    parser_install.add_argument('--binary', default=None,
                                help='Local path to coordinator_test binary (standalone mode)')
    parser_install.add_argument('--lib-dir', default=None,
                                help='Local directory with .so files (default: output/lib/)')
    parser_install.add_argument('--remote-dir', default='/tmp/ds_coordinator',
                                help='Remote directory for standalone binary (default: /tmp/ds_coordinator)')

    # Deploy subcommand (full lifecycle: bring up pods + install whl +
    # start multi-instance coordinators with Raft peers)
    parser_deploy = subparsers.add_parser(
        'deploy', parents=[parent_parser],
        help='Bring up N pods, install whl, and start N coordinators '
             'with Raft peers (multi-instance)')
    parser_deploy.add_argument('-c', '--config', required=True,
                               help='Path to coordinator.config template')
    parser_deploy.add_argument('--port', type=int, default=31511,
                               help='Coordinator port (default: 31511)')
    parser_deploy.add_argument('--remote-config', default='/tmp/coordinator.config',
                               help='Config path inside pod (default: /tmp/coordinator.config)')
    parser_deploy.add_argument('--set', '-s', action='append', default=[],
                               help='Add/override config values (format: key=value). '
                                    'Common coordinator keys: log_dir, log_filename, '
                                    'minloglevel, rpc_thread_num, '
                                    'watch_event_dispatch_thread, '
                                    'coordinator_rpc_stub_cache_size, '
                                    'max_log_size, max_log_file_num, '
                                    'log_retention_day.')
    parser_deploy.add_argument('--enable-procmon', action='store_true', default=False,
                               dest='enable_procmon',
                               help='Start procmon.py for coordinator monitoring (default: disabled)')
    parser_deploy.add_argument('--no-procmon', action='store_false',
                               dest='enable_procmon',
                               help='Disable procmon.py monitoring (default)')
    parser_deploy.add_argument('--procmon-dir', default=None,
                               help='Remote directory for procmon files (default: same as --remote-config dir)')
    parser_deploy.add_argument('--whl', default=find_default_whl(),
                                help='Path to datasystem whl package (non-standalone mode)')
    parser_deploy.add_argument('-S', '--standalone', action='store_true', default=False,
                                help='Standalone mode: install binary + .so, start coordinator_test')
    parser_deploy.add_argument('--image', '-i', required=False, default=None,
                                help='Container image (if set, create pods first)')
    parser_deploy.add_argument('--yaml', '-y',
                                default='config/pod_config.yaml.example',
                                help='Pod YAML template (default: config/pod_config.yaml.example)')
    parser_deploy.add_argument('--cpu', default='8',
                                help='Pod CPU limit (default: 8)')
    parser_deploy.add_argument('--memory', '-m', default='16Gi',
                                help='Pod memory limit (default: 16Gi)')
    parser_deploy.add_argument('--requests-cpu', default=None,
                                help='Pod CPU request (default: same as --cpu)')
    parser_deploy.add_argument('--requests-memory', default=None,
                               help='Pod memory request (default: same as --memory)')
    parser_deploy.add_argument('--instances', type=int, required=False, default=None,
                                help='Number of coordinator instances (required when --image is set)')
    parser_deploy.add_argument('--force', '-f', action='store_true', default=False,
                                help='Delete existing pods with same prefix before deploying')
    parser_deploy.add_argument('--dry-run', action='store_true', default=False,
                                help='Preview pod manifest only; skip install and start')
    # Standalone mode params
    parser_deploy.add_argument('--jf', default=None,
                                help='JF mock address (standalone mode)')
    parser_deploy.add_argument('--service', default='kvcache_coordinator',
                                help='JF service name (standalone mode)')
    parser_deploy.add_argument('--ttl', type=int, default=30,
                                help='Heartbeat TTL (standalone mode)')
    parser_deploy.add_argument('--expected-member-count', type=int, default=1,
                                help='Raft member count (standalone mode)')
    parser_deploy.add_argument('--binary', default=None,
                                help='Path to coordinator_test binary (standalone mode)')
    parser_deploy.add_argument('--lib-dir', default=None,
                                help='Local .so directory (standalone mode)')
    parser_deploy.add_argument('--remote-dir', default='/tmp/ds_coordinator',
                                help='Remote directory for standalone binary')

    args = parser.parse_args()

    if not args.action:
        parser.print_help()
        return 1

    # argparse with action='append' default=None won't enforce presence, so
    # validate explicitly here with a clear message.
    if not args.prefixes:
        print('ERROR: at least one --prefix is required '
              '(e.g. -p coordinator-a [-p coordinator-b])', file=sys.stderr)
        return 1

    # deploy brings up its own pods; skip the pre-fetch used by the
    # operate-on-existing-pods subcommands.
    if args.action == 'deploy':
        pods = None
    else:
        pods = get_pods(args.namespace, args.prefixes)
        if not pods:
            print(f'No running pods found matching prefixes {args.prefixes} '
                  f'in namespace "{args.namespace}"')
            return 1

        print(f'Found {len(pods)} pods:')
        for p in pods:
            print(f'  {p["name"]} ({p["ip"]})')

    # Dispatch
    if args.action == 'start':
        return cmd_start(args, pods)
    elif args.action == 'stop':
        return cmd_stop(args, pods)
    elif args.action == 'kill':
        return cmd_kill(args, pods)
    elif args.action == 'check':
        return cmd_check(args, pods)
    elif args.action == 'exec':
        return cmd_exec(args, pods)
    elif args.action == 'collect':
        return cmd_collect(args, pods)
    elif args.action == 'clean':
        return cmd_clean(args, pods)
    elif args.action == 'install':
        return cmd_install(args, pods)
    elif args.action == 'deploy':
        return cmd_deploy(args, pods)

    return 0


if __name__ == '__main__':
    sys.exit(main())
