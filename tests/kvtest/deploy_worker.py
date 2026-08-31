#!/usr/bin/env python3
"""Batch start/stop datasystem workers in k8s Pods.

Role-specific layer over deploy_common.py: knows that workers listen on
``worker_address``, are backed by the ``datasystem_worker`` binary, support
NUMA binding via dscli, and default to port 31501 / /tmp/worker.config.

Shared kubectl transport, procmon orchestration, and the stop/kill/check/
exec/collect/clean orchestration live in deploy_common.py.
"""

import argparse
import json
import os
import re
import sys

from deploy_common import (
    DEFAULT_TIMEOUT,
    apply_config_overrides,
    cmd_check_impl,
    cmd_clean_shared,
    cmd_collect_shared,
    cmd_exec_shared,
    cmd_install_impl,
    cmd_install_shared,
    cmd_kill_shared,
    cmd_stop_shared,
    do_for_all_pods,
    find_default_whl,
    get_pods,
    kubectl_exec,
    log_error,
    log_info,
    resolve_procmon_dir,
    setup_logging,
    start_service,
    start_service_standalone,
    stop_service,
    _print_timings,
)


PROCESS_NAME = 'datasystem_worker'
PROCESS_NAME_STANDALONE = 'worker_test'
ADDRESS_KEY = 'worker_address'


def start_worker(pod, namespace, config, worker_port, remote_config,
                 enable_procmon=True, procmon_remote_dir='/tmp',
                 numactl_opts=None, timeout=DEFAULT_TIMEOUT):
    """Start a worker in a single pod.

    Delegates to deploy_common.start_service with the worker role's binding
    (datasystem_worker binary). The caller must have injected ``worker_address``
    into ``config`` already (see cmd_start).
    """
    return start_service(pod, namespace, config, remote_config, worker_port,
                         PROCESS_NAME, enable_procmon, procmon_remote_dir,
                         numactl_opts=numactl_opts, timeout=timeout)


def cmd_start(args, pods):
    """Start workers from a config template."""
    if getattr(args, 'standalone', False):
        return cmd_start_standalone(args, pods)

    with open(args.config) as f:
        config_template = json.load(f)

    # Default procmon dir to log_dir from worker config, fallback to
    # --remote-config dir.
    if args.procmon_dir is None:
        args.procmon_dir = resolve_procmon_dir(config_template, args.remote_config)

    if args.set:
        apply_config_overrides(config_template, args.set)
    else:
        log_info('\nNo config overrides specified')

    timings = []

    def do_op(pod):
        cfg = json.loads(json.dumps(config_template))
        cfg[ADDRESS_KEY]['value'] = f'{pod["ip"]}:{args.port}'
        numactl_opts = None
        if args.numa_nodes:
            numactl_opts = f'-N {args.numa_nodes}'
        if args.cpu_bind:
            numactl_opts = f'--physcpubind {args.cpu_bind}'
        ok = False
        try:
            ok = start_worker(pod, args.namespace, cfg, args.port,
                              args.remote_config,
                              enable_procmon=args.enable_procmon,
                              procmon_remote_dir=args.procmon_dir,
                              numactl_opts=numactl_opts,
                              timeout=args.timeout)
            return ok
        finally:
            # start_worker records only the actual launch duration on the
            # pod dict (config upload / pid verify / procmon excluded).
            start_elapsed = pod.pop('_start_elapsed', 0.0)
            timings.append((pod['name'], start_elapsed, bool(ok)))

    rc = do_for_all_pods(pods, do_op, 'Starting workers')
    _print_timings('start', timings)
    return rc


def cmd_deploy(args, pods):
    """Deploy: install + start workers in one command."""
    if getattr(args, 'standalone', False):
        if not getattr(args, 'jf', None):
            log_error('ERROR: --jf is required in standalone mode')
            return 1
        # Standalone: install binary + .so, then start
        log_info('\n--- Step 1/2: installing binary + .so (standalone) ---')
        install_rc = cmd_install_shared(args, pods, PROCESS_NAME_STANDALONE, 'worker',
                                        os.path.dirname(os.path.abspath(__file__)),
                                        args.timeout)
        if install_rc != 0:
            log_error('ERROR: install failed')
            return install_rc
        log_info('\n--- Step 2/2: starting workers (standalone) ---')
        return cmd_start_standalone(args, pods)
    else:
        # Non-standalone: install whl, then dscli start
        log_info('\n--- Step 1/2: installing whl ---')
        whl_rc = cmd_install_impl(pods, args.namespace, args.whl, args.timeout)
        if whl_rc != 0:
            log_error('ERROR: whl install failed')
            return whl_rc
        log_info('\n--- Step 2/2: starting workers ---')
        return cmd_start(args, pods)


def cmd_start_standalone(args, pods):
    """Start worker_test binary in standalone mode."""
    if not getattr(args, 'jf', None):
        log_error('ERROR: --jf is required in standalone mode')
        return 1
    with open(args.config) as f:
        config_template = json.load(f)

    if args.set:
        apply_config_overrides(config_template, args.set)

    remote_dir = getattr(args, 'remote_dir', None) or os.path.dirname(args.remote_config) or '/tmp/ds_worker'
    timings = []

    def do_op(pod):
        cfg = json.loads(json.dumps(config_template))
        cfg[ADDRESS_KEY] = {'value': f'{pod["ip"]}:{args.port}'}
        ok = False
        try:
            ok = start_service_standalone(
                pod, args.namespace, PROCESS_NAME_STANDALONE, remote_dir,
                args.remote_config, args.jf, args.service, '',
                config=cfg,
                enable_procmon=args.enable_procmon,
                procmon_remote_dir=args.procmon_dir or '/tmp',
                port=args.port,
                process_name=PROCESS_NAME_STANDALONE,
                timeout=args.timeout)
            return ok
        finally:
            # start_service_standalone records only the actual launch
            # duration on the pod dict (config upload / pid verify /
            # procmon excluded).
            start_elapsed = pod.pop('_start_elapsed', 0.0)
            timings.append((pod['name'], start_elapsed, bool(ok)))

    rc = do_for_all_pods(pods, do_op, 'Starting workers (standalone)')
    _print_timings('start', timings)
    return rc


def cmd_stop(args, pods):
    """Stop workers gracefully."""
    return cmd_stop_shared(args, pods, PROCESS_NAME_STANDALONE, 'workers',
                           service_type='worker', with_timings=True,
                           timeout=args.timeout)


def cmd_kill(args, pods):
    """Force kill workers."""
    return cmd_kill_shared(args, pods, PROCESS_NAME_STANDALONE,
                           'workers', timeout=args.timeout)


def cmd_check(args, pods):
    """Check workers."""
    proc = PROCESS_NAME_STANDALONE if getattr(args, 'standalone', False) else args.process
    return cmd_check_impl(pods, args.namespace, proc,
                          'worker processes', args.timeout)


def cmd_exec(args, pods):
    """Execute command in pods."""
    return cmd_exec_shared(args, pods, args.timeout)


def cmd_check_commit(args, pods):
    """Check dscli commit hash across all pods against an expected value.

    Runs ``dscli --version`` in each pod, parses the commit hash from stdout,
    and lists pod IPs whose commit differs from the expected one. Returns 0
    regardless of mismatches -- the mismatched IP list is the actionable
    output, not the exit code.
    """
    cmd = 'dscli --version'
    expected = args.expected_commit.strip().lower()
    mismatches = []
    matches = []
    failures = []

    def do_op(pod):
        pod_name = pod['name']
        pod_ip = pod['ip']
        try:
            result = kubectl_exec(pod_name, namespace=args.namespace,
                                  cmd=cmd, check=False, timeout=args.timeout)
        except Exception as e:
            log_info(f'  {pod_name} ({pod_ip}) -> ERROR: {e}')
            failures.append((pod_name, pod_ip, str(e)))
            return False
        if result.returncode != 0:
            log_info(f'  {pod_name} ({pod_ip}) -> FAILED rc={result.returncode}')
            failures.append((pod_name, pod_ip,
                             result.stderr.strip() or 'rc!=0'))
            return False
        m = re.search(r'commit:\s*([0-9a-f]+)', result.stdout)
        if not m:
            log_info(f'  {pod_name} ({pod_ip}) -> commit not found in stdout')
            failures.append((pod_name, pod_ip, 'commit not found'))
            return False
        actual = m.group(1).lower()
        ok = actual == expected or actual.startswith(expected) or expected.startswith(actual)
        if ok:
            matches.append((pod_name, pod_ip, actual))
            log_info(f'  {pod_name} ({pod_ip}) -> OK commit={actual[:12]}')
        else:
            mismatches.append((pod_name, pod_ip, actual))
            log_info(f'  {pod_name} ({pod_ip}) -> MISMATCH expected={expected[:12]} actual={actual[:12]}')
        return True

    rc = do_for_all_pods(pods, do_op, f'Checking dscli version (expected: {expected[:12]})')

    log_info(f'\nVersion check summary:')
    log_info(f'  matched: {len(matches)}')
    log_info(f'  mismatched: {len(mismatches)}')
    log_info(f'  failed: {len(failures)}')
    if mismatches:
        log_info(f'\nPods with mismatched commit (expected {expected[:12]}):')
        for name, ip, actual in mismatches:
            log_info(f'  {name} ({ip}) actual={actual[:12]}')
    if failures:
        log_info(f'\nPods where dscli --version failed:')
        for name, ip, err in failures:
            log_info(f'  {name} ({ip}) {err}')
    return 0


def cmd_collect(args, pods):
    """Collect worker logs from pods."""
    return cmd_collect_shared(args, pods, 'worker logs', args.timeout)


def cmd_clean(args, pods):
    """Kill workers and clean log directories.

    Standalone mode: kill worker_test and rm -rf the remote_dir holding the
    standalone binary + .so + stdout.log so a re-deploy starts clean instead
    of stacking a new binary on a running stale one.
    """
    return cmd_clean_shared(args, pods, PROCESS_NAME, PROCESS_NAME_STANDALONE,
                            'worker logs', args.timeout)


def cmd_install(args, pods):
    """Install worker: always install whl first, then optionally copy
    standalone binary (standalone mode adds the binary on top of the whl)."""
    return cmd_install_shared(args, pods, PROCESS_NAME_STANDALONE, 'worker',
                              os.path.dirname(os.path.abspath(__file__)),
                              args.timeout)


def main():
    default_whl = find_default_whl()

    parser = argparse.ArgumentParser(
        description='Batch manage datasystem workers in k8s Pods',
        formatter_class=argparse.RawDescriptionHelpFormatter)
    subparsers = parser.add_subparsers(dest='action', help='Action to perform')

    # Common parent parser
    parent_parser = argparse.ArgumentParser(add_help=False)
    parent_parser.add_argument('-p', '--prefix', action='append', default=None,
                               dest='prefixes', metavar='PREFIX',
                               help='Pod name prefix to match (repeatable: '
                                    '-p worker-a -p worker-b). A pod is '
                                    'selected if it matches ANY prefix.')
    parent_parser.add_argument('-n', '--namespace', default='default',
                               help='k8s namespace (default: default)')
    parent_parser.add_argument('--timeout', type=int, default=DEFAULT_TIMEOUT,
                               help=f'Operation timeout in seconds (default: {DEFAULT_TIMEOUT})')
    parent_parser.add_argument('--count', type=int, default=None,
                               help='Limit operation to N matching pods starting '
                                    'at --offset (pods are sorted by name, so the '
                                    'subset is deterministic across runs). Useful '
                                    'for scale-in/out tests at specific sizes '
                                    '(e.g. --count 10 / 100 / 500).')
    parent_parser.add_argument('--offset', type=int, default=0,
                               help='Skip the first N matching pods before applying '
                                    '--count (default: 0). Pair with --count to '
                                    'scale out on top of an already-deployed base: '
                                    'e.g. deploy 1900 workers, then --offset 1900 '
                                    '--count 1 (or 10 / 50) to add more on top.')

    # Start subcommand
    parser_start = subparsers.add_parser('start', parents=[parent_parser],
                                         help='Start workers from config')
    parser_start.add_argument('-c', '--config', required=True,
                              help='Path to worker.config template')
    parser_start.add_argument('--port', type=int, default=31501,
                              help='Worker port (default: 31501)')
    parser_start.add_argument('--remote-config', default='/tmp/worker.config',
                              help='Config path inside pod (default: /tmp/worker.config)')
    parser_start.add_argument('--set', '-s', action='append', default=[],
                              help='Add/override config values (format: key=value). '
                                   'Example: --set ttl_seconds=3600')
    parser_start.add_argument('--enable-procmon', action='store_true', default=False,
                              dest='enable_procmon',
                              help='Start procmon.py for worker monitoring (default: disabled)')
    parser_start.add_argument('--no-procmon', action='store_false',
                              dest='enable_procmon',
                              help='Disable procmon.py monitoring (default)')
    parser_start.add_argument('--procmon-dir', default=None,
                              help='Remote directory for procmon files (default: same as --remote-config dir)')
    parser_start.add_argument('-N', '--numa-nodes', default=None,
                              help='NUMA node(s) to bind worker to, passed to dscli start -N (e.g. "0" or "0,1")')
    parser_start.add_argument('-C', '--cpu-bind', default=None,
                              help='CPU core(s) to bind worker to (dscli mode only)')
    # Standalone mode
    parser_start.add_argument('-S', '--standalone', action='store_true', default=False,
                              help='Use worker_test binary instead of dscli')
    parser_start.add_argument('--jf', default=None,
                              help='JF mock address for service discovery (standalone mode)')
    parser_start.add_argument('--service', default='kvcache_coordinator',
                              help='JF service name (standalone mode, default: kvcache_coordinator)')
    parser_start.add_argument('--remote-dir', default='/tmp/ds_worker',
                              help='Remote directory with standalone binary (must match install --remote-dir)')

    # Stop subcommand
    parser_stop = subparsers.add_parser('stop', parents=[parent_parser],
                                        help='Stop workers gracefully')
    parser_stop.add_argument('--remote-config', default='/tmp/worker.config',
                             help='Worker config file path (default: /tmp/worker.config)')
    parser_stop.add_argument('-S', '--standalone', action='store_true', default=False)

    # Kill subcommand (force kill using kill -9)
    parser_kill = subparsers.add_parser('kill', parents=[parent_parser],
                                        help='Force kill workers')
    parser_kill.add_argument('--process', default=PROCESS_NAME,
                             help=f'Process name to kill (default: {PROCESS_NAME})')
    parser_kill.add_argument('-S', '--standalone', action='store_true', default=False)

    # Check subcommand
    parser_check = subparsers.add_parser('check', parents=[parent_parser],
                                         help='Check worker status')
    parser_check.add_argument('--process', default=PROCESS_NAME,
                              help=f'Process name to check (default: {PROCESS_NAME})')
    parser_check.add_argument('-S', '--standalone', action='store_true', default=False)

    # Exec subcommand
    parser_exec = subparsers.add_parser('exec', parents=[parent_parser],
                                        help='Execute command in pods')
    parser_exec.add_argument('--cmd', '-c', required=True,
                             help='Command to execute (required)')

    # Check-commit subcommand
    parser_check_commit = subparsers.add_parser('check-commit', parents=[parent_parser],
                                                help='Check dscli commit hash across pods')
    parser_check_commit.add_argument('--expected-commit', required=True,
                                     help='Expected commit hash (full or prefix, '
                                          'first 12+ chars). Runs dscli --version '
                                          'in each pod and lists mismatched IPs.')

    # Collect subcommand
    parser_collect = subparsers.add_parser('collect', parents=[parent_parser],
                                           help='Collect worker logs from pods')
    parser_collect.add_argument('--remote-config', default='/tmp/worker.config',
                                help='Config path inside pod (default: /tmp/worker.config)')
    parser_collect.add_argument('-o', '--output', default='collected_worker_logs',
                                help='Local output directory (default: collected_worker_logs)')
    parser_collect.add_argument('--remote-dir', default='/tmp/ds_worker',
                                help='Remote standalone-binary dir; stdout.log is '
                                     'collected from here when the dir exists '
                                     '(default: /tmp/ds_worker, must match install)')

    # Clean subcommand
    parser_clean = subparsers.add_parser('clean', parents=[parent_parser],
                                         help='Kill workers and clean log directories')
    parser_clean.add_argument('--remote-config', default='/tmp/worker.config',
                              help='Config path inside pod (default: /tmp/worker.config)')
    parser_clean.add_argument('-S', '--standalone', action='store_true', default=False,
                              help='Kill worker_test and remove --remote-dir '
                                   '(standalone mode; must match install --remote-dir)')
    parser_clean.add_argument('--remote-dir', default='/tmp/ds_worker',
                              help='Remote directory holding the standalone binary '
                                   '(default: /tmp/ds_worker, must match install)')

    # Install subcommand
    parser_install = subparsers.add_parser('install', parents=[parent_parser],
                                           help='Install worker binary or whl')
    parser_install.add_argument('-S', '--standalone', action='store_true', default=False,
                                help='Install standalone binary + .so (no whl)')
    parser_install.add_argument('--whl', default=default_whl,
                                help='Path to worker whl package (non-standalone mode)')
    parser_install.add_argument('--binary', default=None,
                                help='Local path to worker_test binary (standalone mode)')
    parser_install.add_argument('--lib-dir', default=None,
                                help='Local directory with .so files (default: output/lib/)')
    parser_install.add_argument('--remote-dir', default='/tmp/ds_worker',
                                help='Remote directory for standalone binary (default: /tmp/ds_worker)')

    # Deploy subcommand: install + start (both standalone and non-standalone)
    parser_deploy = subparsers.add_parser('deploy', parents=[parent_parser],
                                          help='Install + start workers in one command')
    parser_deploy.add_argument('-c', '--config', required=True,
                               help='Path to worker.config template')
    parser_deploy.add_argument('--port', type=int, default=31501,
                               help='Worker port (default: 31501)')
    parser_deploy.add_argument('--remote-config', default='/tmp/worker.config',
                               help='Config path inside pod (default: /tmp/worker.config)')
    parser_deploy.add_argument('--remote-dir', default='/tmp/ds_worker',
                               help='Remote directory for standalone binary (default: /tmp/ds_worker)')
    # Standalone mode
    parser_deploy.add_argument('-S', '--standalone', action='store_true', default=False,
                               help='Standalone mode: install binary + .so, start worker_test')
    parser_deploy.add_argument('--jf', default=None,
                               help='JF mock address (standalone mode)')
    parser_deploy.add_argument('--service', default='kvcache_coordinator',
                               help='JF service name (standalone mode)')
    parser_deploy.add_argument('--binary', default=None,
                               help='Local path to worker_test binary (standalone mode)')
    parser_deploy.add_argument('--lib-dir', default=None,
                               help='Local directory with .so files (default: output/lib/)')
    # Non-standalone mode
    parser_deploy.add_argument('--whl', default=default_whl,
                               help='Path to worker whl package (non-standalone mode)')
    parser_deploy.add_argument('--set', '-s', action='append', default=[],
                               help='Add/override config values (format: key=value)')
    parser_deploy.add_argument('-N', '--numa-nodes', default=None,
                               help='NUMA node(s) to bind worker to (non-standalone mode)')
    parser_deploy.add_argument('-C', '--cpu-bind', default=None,
                               help='CPU core(s) to bind worker to (non-standalone mode)')
    # Common
    parser_deploy.add_argument('--enable-procmon', action='store_true', default=False,
                               dest='enable_procmon',
                               help='Start procmon.py for worker monitoring (default: disabled)')
    parser_deploy.add_argument('--no-procmon', action='store_false',
                               dest='enable_procmon',
                               help='Disable procmon.py monitoring (default)')
    parser_deploy.add_argument('--procmon-dir', default=None,
                               help='Remote directory for procmon files')

    args = parser.parse_args()

    if not args.action:
        parser.print_help()
        return 1
    setup_logging()

    # argparse with action='append' default=None won't enforce presence, so
    # validate explicitly here with a clear message.
    if not args.prefixes:
        log_error('ERROR: at least one --prefix is required '
                  '(e.g. -p worker-a [-p worker-b])')
        return 1

    # Get pods
    pods = get_pods(args.namespace, args.prefixes)
    if not pods:
        log_info(f'No running pods found matching prefixes {args.prefixes} '
                 f'in namespace "{args.namespace}"')
        return 1

    if args.count is not None:
        if args.count <= 0:
            log_error(f'ERROR: --count must be a positive integer, got {args.count}')
            return 1
        if args.offset < 0:
            log_error(f'ERROR: --offset must be >= 0, got {args.offset}')
            return 1
        if args.offset >= len(pods):
            log_error(f'ERROR: --offset {args.offset} reaches end of the '
                      f'{len(pods)} pods matching prefixes {args.prefixes}')
            return 1
        if args.offset + args.count > len(pods):
            log_error(f'ERROR: --offset {args.offset} + --count {args.count} '
                      f'exceeds the {len(pods)} pods matching prefixes '
                      f'{args.prefixes}')
            return 1
        pods = pods[args.offset:args.offset + args.count]

    log_info(f'Found {len(pods)} pods:')
    for p in pods:
        log_info(f'  {p["name"]} ({p["ip"]})')

    # Dispatch
    if args.action == 'deploy':
        return cmd_deploy(args, pods)
    elif args.action == 'start':
        return cmd_start(args, pods)
    elif args.action == 'stop':
        return cmd_stop(args, pods)
    elif args.action == 'kill':
        return cmd_kill(args, pods)
    elif args.action == 'check':
        return cmd_check(args, pods)
    elif args.action == 'exec':
        return cmd_exec(args, pods)
    elif args.action == 'check-commit':
        return cmd_check_commit(args, pods)
    elif args.action == 'collect':
        return cmd_collect(args, pods)
    elif args.action == 'clean':
        return cmd_clean(args, pods)
    elif args.action == 'install':
        return cmd_install(args, pods)

    return 0


if __name__ == '__main__':
    sys.exit(main())
