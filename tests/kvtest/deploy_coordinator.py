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

from deploy_common import (
    DEFAULT_TIMEOUT,
    apply_config_overrides,
    cmd_check_impl,
    cmd_clean_impl,
    cmd_collect_impl,
    cmd_exec_impl,
    cmd_kill_impl,
    cmd_stop_impl,
    do_for_all_pods,
    get_pods,
    resolve_procmon_dir,
    start_service,
)


PROCESS_NAME = 'datasystem_coordinator'
ADDRESS_KEY = 'coordinator_address'


def start_coordinator(pod, namespace, config, coordinator_port, remote_config,
                      enable_procmon=True, procmon_remote_dir='/tmp',
                      timeout=DEFAULT_TIMEOUT):
    """Start a coordinator in a single pod.

    Delegates to deploy_common.start_service with the coordinator role's
    binding (datasystem_coordinator binary). The caller must have injected
    ``coordinator_address`` into ``config`` already (see cmd_start).
    """
    return start_service(pod, namespace, config, remote_config,
                         coordinator_port, PROCESS_NAME, enable_procmon,
                         procmon_remote_dir, numactl_opts=None, timeout=timeout)


def cmd_start(args, pods):
    """Start coordinators from a config template."""
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
        cfg = json.loads(json.dumps(config_template))
        cfg[ADDRESS_KEY]['value'] = f'{pod["ip"]}:{args.port}'
        return start_coordinator(pod, args.namespace, cfg, args.port,
                                 args.remote_config,
                                 enable_procmon=args.enable_procmon,
                                 procmon_remote_dir=args.procmon_dir,
                                 timeout=args.timeout)

    return do_for_all_pods(pods, do_op, 'Starting coordinators')


def cmd_stop(args, pods):
    """Stop coordinators gracefully using dscli."""
    return cmd_stop_impl(pods, args.namespace, args.remote_config,
                         'coordinators', args.timeout)


def cmd_kill(args, pods):
    """Force kill coordinators."""
    return cmd_kill_impl(pods, args.namespace, args.process,
                         'coordinators', args.timeout)


def cmd_check(args, pods):
    """Check coordinators."""
    return cmd_check_impl(pods, args.namespace, args.process,
                          'coordinator processes', args.timeout)


def cmd_exec(args, pods):
    """Execute command in pods."""
    return cmd_exec_impl(pods, args.namespace, args.cmd, args.timeout)


def cmd_collect(args, pods):
    """Collect coordinator logs from pods."""
    return cmd_collect_impl(pods, args.namespace, args.remote_config,
                            args.output, 'coordinator logs', args.timeout)


def cmd_clean(args, pods):
    """Kill coordinators and clean log directories."""
    return cmd_clean_impl(pods, args.namespace, args.remote_config,
                          PROCESS_NAME, 'coordinator logs', args.timeout)


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
    parser_start.add_argument('--enable-procmon', action='store_true', default=True,
                              dest='enable_procmon',
                              help='Start procmon.py for coordinator monitoring (default: enabled)')
    parser_start.add_argument('--no-procmon', action='store_false',
                              dest='enable_procmon',
                              help='Disable procmon.py monitoring')
    parser_start.add_argument('--procmon-dir', default=None,
                              help='Remote directory for procmon files (default: same as --remote-config dir)')

    # Stop subcommand (graceful stop using dscli)
    parser_stop = subparsers.add_parser('stop', parents=[parent_parser],
                                        help='Stop coordinators gracefully using dscli')
    parser_stop.add_argument('--remote-config', default='/tmp/coordinator.config',
                             help='Coordinator config file path (default: /tmp/coordinator.config)')

    # Kill subcommand (force kill using kill -9)
    parser_kill = subparsers.add_parser('kill', parents=[parent_parser],
                                        help='Force kill coordinators')
    parser_kill.add_argument('--process', default=PROCESS_NAME,
                             help=f'Process name to kill (default: {PROCESS_NAME})')

    # Check subcommand
    parser_check = subparsers.add_parser('check', parents=[parent_parser],
                                         help='Check coordinator status')
    parser_check.add_argument('--process', default=PROCESS_NAME,
                              help=f'Process name to check (default: {PROCESS_NAME})')

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

    return 0


if __name__ == '__main__':
    sys.exit(main())
