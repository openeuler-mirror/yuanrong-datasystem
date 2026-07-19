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
import sys
import time

from deploy_common import (
    DEFAULT_TIMEOUT,
    apply_config_overrides,
    cmd_check_impl,
    cmd_clean_impl,
    cmd_collect_impl,
    cmd_exec_impl,
    cmd_install_impl,
    cmd_kill_impl,
    do_for_all_pods,
    find_default_whl,
    get_pods,
    resolve_procmon_dir,
    start_service,
    stop_service,
)


PROCESS_NAME = 'datasystem_worker'
ADDRESS_KEY = 'worker_address'


def _print_timings(action, timings):
    """Print per-pod duration stats for a start/stop action.

    ``timings`` is a list of ``(pod_name, elapsed_seconds, succeeded)``
    tuples populated from worker threads (list.append is GIL-atomic in
    CPython, so concurrent appends from the thread pool are safe).
    """
    if not timings:
        return
    print(f'\n{action} per-pod timings:')
    for pod_name, elapsed, ok in sorted(timings, key=lambda x: x[0]):
        print(f'  {pod_name:<40} {elapsed:7.2f}s  {"OK" if ok else "FAIL"}')
    elapsed_all = [t for _, t, _ in timings]
    ok_count = sum(1 for _, _, ok in timings if ok)
    fail_count = len(timings) - ok_count
    print(f'  min={min(elapsed_all):.2f}s  max={max(elapsed_all):.2f}s  '
          f'avg={sum(elapsed_all) / len(elapsed_all):.2f}s  '
          f'total={sum(elapsed_all):.2f}s  '
          f'(succeeded={ok_count}, failed={fail_count})')


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
    with open(args.config) as f:
        config_template = json.load(f)

    # Default procmon dir to log_dir from worker config, fallback to
    # --remote-config dir.
    if args.procmon_dir is None:
        args.procmon_dir = resolve_procmon_dir(config_template, args.remote_config)

    if args.set:
        apply_config_overrides(config_template, args.set)
    else:
        print('\nNo config overrides specified')

    timings = []

    def do_op(pod):
        cfg = json.loads(json.dumps(config_template))
        cfg[ADDRESS_KEY]['value'] = f'{pod["ip"]}:{args.port}'
        numactl_opts = None
        if args.numa_nodes:
            numactl_opts = f'-N {args.numa_nodes}'
        if args.cpu_bind:
            numactl_opts = f'-C {args.cpu_bind}'
        t0 = time.monotonic()
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
            elapsed = time.monotonic() - t0
            timings.append((pod['name'], elapsed, bool(ok)))

    rc = do_for_all_pods(pods, do_op, 'Starting workers')
    _print_timings('start', timings)
    return rc


def cmd_stop(args, pods):
    """Stop workers gracefully using dscli."""
    timings = []

    def do_op(pod):
        t0 = time.monotonic()
        ok = False
        try:
            ok = stop_service(pod, args.namespace, args.remote_config,
                              timeout=args.timeout)
            return ok
        finally:
            elapsed = time.monotonic() - t0
            timings.append((pod['name'], elapsed, bool(ok)))

    rc = do_for_all_pods(pods, do_op, 'Stopping workers')
    _print_timings('stop', timings)
    return rc


def cmd_kill(args, pods):
    """Force kill workers."""
    return cmd_kill_impl(pods, args.namespace, args.process,
                         'workers', args.timeout)


def cmd_check(args, pods):
    """Check workers."""
    return cmd_check_impl(pods, args.namespace, args.process,
                          'worker processes', args.timeout)


def cmd_exec(args, pods):
    """Execute command in pods."""
    return cmd_exec_impl(pods, args.namespace, args.cmd, args.timeout)


def cmd_collect(args, pods):
    """Collect worker logs from pods."""
    return cmd_collect_impl(pods, args.namespace, args.remote_config,
                            args.output, 'worker logs', args.timeout)


def cmd_clean(args, pods):
    """Kill workers and clean log directories."""
    return cmd_clean_impl(pods, args.namespace, args.remote_config,
                          PROCESS_NAME, 'worker logs', args.timeout)


def cmd_install(args, pods):
    """Install worker whl package."""
    return cmd_install_impl(pods, args.namespace, args.whl, args.timeout)


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
    parser_start.add_argument('--enable-procmon', action='store_true', default=True,
                              dest='enable_procmon',
                              help='Start procmon.py for worker monitoring (default: enabled)')
    parser_start.add_argument('--no-procmon', action='store_false',
                              dest='enable_procmon',
                              help='Disable procmon.py monitoring')
    parser_start.add_argument('--procmon-dir', default=None,
                              help='Remote directory for procmon files (default: same as --remote-config dir)')
    parser_start.add_argument('-N', '--numa-nodes', default=None,
                              help='NUMA node(s) to bind worker to, passed to dscli start -N (e.g. "0" or "0,1")')
    parser_start.add_argument('-C', '--cpu-bind', default=None,
                              help='CPU core(s) to bind worker to, passed to dscli start -C (e.g. "0-7" or "0,2,4,6")')

    # Stop subcommand (graceful stop using dscli)
    parser_stop = subparsers.add_parser('stop', parents=[parent_parser],
                                        help='Stop workers gracefully using dscli')
    parser_stop.add_argument('--remote-config', default='/tmp/worker.config',
                             help='Worker config file path (default: /tmp/worker.config)')

    # Kill subcommand (force kill using kill -9)
    parser_kill = subparsers.add_parser('kill', parents=[parent_parser],
                                        help='Force kill workers')
    parser_kill.add_argument('--process', default=PROCESS_NAME,
                             help=f'Process name to kill (default: {PROCESS_NAME})')

    # Check subcommand
    parser_check = subparsers.add_parser('check', parents=[parent_parser],
                                         help='Check worker status')
    parser_check.add_argument('--process', default=PROCESS_NAME,
                              help=f'Process name to check (default: {PROCESS_NAME})')

    # Exec subcommand
    parser_exec = subparsers.add_parser('exec', parents=[parent_parser],
                                        help='Execute command in pods')
    parser_exec.add_argument('--cmd', '-c', required=True,
                             help='Command to execute (required)')

    # Collect subcommand
    parser_collect = subparsers.add_parser('collect', parents=[parent_parser],
                                           help='Collect worker logs from pods')
    parser_collect.add_argument('--remote-config', default='/tmp/worker.config',
                                help='Config path inside pod (default: /tmp/worker.config)')
    parser_collect.add_argument('-o', '--output', default='collected_worker_logs',
                                help='Local output directory (default: collected_worker_logs)')

    # Clean subcommand
    parser_clean = subparsers.add_parser('clean', parents=[parent_parser],
                                         help='Kill workers and clean log directories')
    parser_clean.add_argument('--remote-config', default='/tmp/worker.config',
                              help='Config path inside pod (default: /tmp/worker.config)')

    # Install subcommand
    parser_install = subparsers.add_parser('install', parents=[parent_parser],
                                           help='Install worker whl package')
    parser_install.add_argument('--whl', default=default_whl,
                                help='Path to worker whl package '
                                     '(default: auto-detect from ../../output)')

    args = parser.parse_args()

    if not args.action:
        parser.print_help()
        return 1

    # argparse with action='append' default=None won't enforce presence, so
    # validate explicitly here with a clear message.
    if not args.prefixes:
        print('ERROR: at least one --prefix is required '
              '(e.g. -p worker-a [-p worker-b])', file=sys.stderr)
        return 1

    # Get pods
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

    return 0


if __name__ == '__main__':
    sys.exit(main())
