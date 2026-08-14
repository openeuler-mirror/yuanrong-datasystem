#!/usr/bin/env python3
"""Tests for deploy_coordinator.py role-specific layer.

Mirrors test_deploy_worker.py but for the coordinator role: start_coordinator
delegates to deploy_common.start_service with the datasystem_coordinator
binding and NO numactl (dscli's numactl path applies to workers only),
cmd_start injects coordinator_address per pod, and the stop/kill/clean
wiring uses the coordinator process name and labels.
"""

import inspect
import json
import os
import sys
import tempfile
import unittest
from types import SimpleNamespace
from unittest.mock import patch

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from deploy_coordinator import (
    ADDRESS_KEY,
    PROCESS_NAME,
    cmd_clean,
    cmd_deploy,
    cmd_kill,
    cmd_start,
    cmd_stop,
    start_coordinator,
)


# Mock.call_args / call_args_list[i] are (args, kwargs) tuples in all
# supported Pythons; the .args / .kwargs attributes are 3.8+. Use index
# access (call[0] / call[1]) so the tests run on Python 3.7 too.
def _pos(call):
    return call[0]


def _kw(call):
    return call[1]


def _write_config(cfg):
    """Write a JSON config to a temp file and return its path."""
    tf = tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False)
    json.dump(cfg, tf)
    tf.close()
    return tf.name


class TestStartCoordinator(unittest.TestCase):
    """start_coordinator delegates to deploy_common.start_service with the
    coordinator binding (datasystem_coordinator) and numactl_opts=None always.
    """

    @patch('deploy_coordinator.start_service', return_value=True)
    def test_delegates_with_coordinator_binding_and_no_numactl(self, mock_start):
        pod = {'name': 'p1', 'ip': '10.0.0.1'}
        cfg = {'coordinator_address': {'value': 'old'}}
        ok = start_coordinator(pod, 'default', cfg, 31511,
                               '/tmp/coordinator.config',
                               enable_procmon=True, procmon_remote_dir='/tmp',
                               timeout=10)
        self.assertTrue(ok)
        mock_start.assert_called_once_with(
            pod, 'default', cfg, '/tmp/coordinator.config', 31511,
            PROCESS_NAME, True, '/tmp',
            numactl_opts=None, timeout=10)

    @patch('deploy_coordinator.start_service', return_value=True)
    def test_uses_coordinator_process_name(self, mock_start):
        pod = {'name': 'p1', 'ip': '10.0.0.1'}
        start_coordinator(pod, 'default', {}, 31511,
                          '/tmp/coordinator.config', timeout=10)
        # positional arg index 5 is process_name
        self.assertEqual(_pos(mock_start.call_args)[5],
                         'datasystem_coordinator')

    def test_start_coordinator_has_no_numactl_param(self):
        """NUMA is worker-only; start_coordinator must not expose numactl_opts.

        This guards against accidentally copying the worker signature and
        silently accepting (then ignoring) a NUMA binding.
        """
        sig = inspect.signature(start_coordinator)
        self.assertNotIn('numactl_opts', sig.parameters)


class TestCmdStart(unittest.TestCase):
    """cmd_start: per-pod coordinator_address injection, procmon dir
    resolution, --set overrides, and skip-alive (pods with a running
    coordinator are skipped so a multi-instance cluster can be partially
    restarted without --set). No NUMA option construction (coordinator
    does not support NUMA)."""

    def setUp(self):
        # cmd_start probes each pod with check_process before starting and
        # skips pods that already have a running coordinator. Default to
        # 'dead' so the start path is exercised by every test in this class;
        # the skip-alive tests override per-pod via side_effect.
        patcher = patch('deploy_coordinator.check_process',
                        return_value=(None, 'dead', 0))
        patcher.start()
        self.addCleanup(patcher.stop)

    def _args(self, **overrides):
        defaults = dict(namespace='default', port=31511,
                        remote_config='/tmp/coordinator.config',
                        set=[], enable_procmon=True, procmon_dir=None,
                        timeout=10, config=None)
        defaults.update(overrides)
        return SimpleNamespace(**defaults)

    @patch('deploy_coordinator.start_coordinator', return_value=True)
    def test_injects_coordinator_address_per_pod(self, mock_start):
        cfg_path = _write_config({
            'coordinator_address': {'value': '0.0.0.0:0'},
            'log_dir': {'value': '/var/log/ds'},
        })
        try:
            args = self._args(config=cfg_path)
            pods = [{'name': 'p1', 'ip': '10.0.0.1'},
                    {'name': 'p2', 'ip': '10.0.0.2'}]
            rc = cmd_start(args, pods)
            self.assertEqual(rc, 0)
            self.assertEqual(mock_start.call_count, 2)
            c0 = mock_start.call_args_list[0]
            c1 = mock_start.call_args_list[1]
            # positional: pod, namespace, cfg, port, remote_config
            self.assertEqual(_pos(c0)[2][ADDRESS_KEY]['value'],
                             '10.0.0.1:31511')
            self.assertEqual(_pos(c1)[2][ADDRESS_KEY]['value'],
                             '10.0.0.2:31511')
            # procmon_dir resolved from log_dir in the config
            self.assertEqual(_kw(c0)['procmon_remote_dir'], '/var/log/ds')
            # each pod gets a distinct deep-copied config
            self.assertIsNot(_pos(c0)[2], _pos(c1)[2])
        finally:
            os.unlink(cfg_path)

    @patch('deploy_coordinator.start_coordinator', return_value=True)
    def test_set_overrides_applied(self, mock_start):
        cfg_path = _write_config({
            'coordinator_address': {'value': '0.0.0.0:0'},
        })
        try:
            args = self._args(config=cfg_path, set=['rpc_thread_num=128'])
            cmd_start(args, [{'name': 'p1', 'ip': '10.0.0.1'}])
            cfg = _pos(mock_start.call_args)[2]
            self.assertEqual(cfg['rpc_thread_num']['value'], 128)
        finally:
            os.unlink(cfg_path)

    @patch('deploy_coordinator.start_coordinator', return_value=True)
    def test_procmon_dir_falls_back_to_remote_config_dir(self, mock_start):
        # No log_dir in config -> procmon_dir defaults to the remote-config
        # directory (dirname of /tmp/coordinator.config == /tmp).
        cfg_path = _write_config({
            'coordinator_address': {'value': '0.0.0.0:0'},
        })
        try:
            args = self._args(config=cfg_path)
            cmd_start(args, [{'name': 'p1', 'ip': '10.0.0.1'}])
            self.assertEqual(_kw(mock_start.call_args)['procmon_remote_dir'],
                             '/tmp')
        finally:
            os.unlink(cfg_path)

    @patch('deploy_coordinator.start_coordinator', return_value=True)
    def test_default_port_is_31511(self, mock_start):
        cfg_path = _write_config({
            'coordinator_address': {'value': '0.0.0.0:0'},
        })
        try:
            # Port not overridden -> cmd_start uses args.port which we leave
            # at the role default (31511) to mirror argparse default.
            args = self._args(config=cfg_path)
            cmd_start(args, [{'name': 'p1', 'ip': '10.0.0.1'}])
            # positional arg index 3 is port
            self.assertEqual(_pos(mock_start.call_args)[3], 31511)
            # and the injected address uses that port
            self.assertEqual(_pos(mock_start.call_args)[2][ADDRESS_KEY]['value'],
                             '10.0.0.1:31511')
        finally:
            os.unlink(cfg_path)

    @patch('deploy_coordinator.start_coordinator', return_value=True)
    def test_multi_instance_injects_raft_peers_with_full_member_list(self,
                                                                     mock_start):
        # 2+ pods -> every cfg carries the same coordinator_raft_initial_peers
        # built from ALL matched pods (including self), so the cluster can run
        # static-peers Raft election.
        cfg_path = _write_config({
            'coordinator_address': {'value': '0.0.0.0:0'},
        })
        try:
            args = self._args(config=cfg_path)
            pods = [{'name': 'p1', 'ip': '10.0.0.1'},
                    {'name': 'p2', 'ip': '10.0.0.2'},
                    {'name': 'p3', 'ip': '10.0.0.3'}]
            rc = cmd_start(args, pods)
            self.assertEqual(rc, 0)
            self.assertEqual(mock_start.call_count, 3)
            expected_peers = '10.0.0.1:31511,10.0.0.2:31511,10.0.0.3:31511'
            for call in mock_start.call_args_list:
                cfg = _pos(call)[2]
                self.assertEqual(cfg['coordinator_raft_initial_peers']['value'],
                                 expected_peers)
        finally:
            os.unlink(cfg_path)

    @patch('deploy_coordinator.start_coordinator', return_value=True)
    def test_single_pod_leaves_raft_peers_untouched(self, mock_start):
        # 1 pod -> single-node no-election mode; peers must NOT be injected
        # (matches cmd_deploy's single-instance rule).
        cfg_path = _write_config({
            'coordinator_address': {'value': '0.0.0.0:0'},
        })
        try:
            args = self._args(config=cfg_path)
            cmd_start(args, [{'name': 'p1', 'ip': '10.0.0.1'}])
            cfg = _pos(mock_start.call_args)[2]
            self.assertNotIn('coordinator_raft_initial_peers', cfg)
        finally:
            os.unlink(cfg_path)

    @patch('deploy_coordinator.start_coordinator', return_value=True)
    def test_peers_built_from_full_pod_set_not_just_started_pod(self,
                                                                mock_start):
        # Restart invariant: even when only one pod of an existing cluster is
        # being (re)started, the peers list must come from the FULL matched
        # pod set so the restarting node can rejoin. cmd_start discovers pods
        # by prefix; passing all cluster prefixes yields the full membership.
        cfg_path = _write_config({
            'coordinator_address': {'value': '0.0.0.0:0'},
        })
        try:
            args = self._args(config=cfg_path)
            pods = [{'name': 'p1', 'ip': '10.0.0.1'},
                    {'name': 'p2', 'ip': '10.0.0.2'},
                    {'name': 'p3', 'ip': '10.0.0.3'}]
            cmd_start(args, pods)
            # The cfg handed to each pod's start_coordinator carries all three
            # members in peers, including the pod itself.
            for i, call in enumerate(mock_start.call_args_list):
                pod = _pos(call)[0]
                cfg = _pos(call)[2]
                peers = cfg['coordinator_raft_initial_peers']['value']
                self.assertIn(f'{pod["ip"]}:31511', peers)
                for other in pods:
                    self.assertIn(f'{other["ip"]}:31511', peers)
        finally:
            os.unlink(cfg_path)

    @patch('deploy_coordinator.start_coordinator', return_value=True)
    def test_skips_pods_with_running_coordinator(self, mock_start):
        # Pods that already have a live coordinator process are skipped (no
        # start_coordinator call); only dead pods are started. This lets the
        # operator restart a subset of a cluster by passing all prefixes.
        cfg_path = _write_config({
            'coordinator_address': {'value': '0.0.0.0:0'},
        })
        try:
            args = self._args(config=cfg_path)
            pods = [{'name': 'p1', 'ip': '10.0.0.1'},
                    {'name': 'p2', 'ip': '10.0.0.2'}]
            with patch('deploy_coordinator.check_process',
                       side_effect=lambda pod, namespace, process_name,
                                     timeout=10:
                       (pod, 'alive' if pod['name'] == 'p1' else 'dead', 0)):
                rc = cmd_start(args, pods)
            self.assertEqual(rc, 0)
            self.assertEqual(mock_start.call_count, 1)
            started_pod = _pos(mock_start.call_args)[0]
            self.assertEqual(started_pod['name'], 'p2')
        finally:
            os.unlink(cfg_path)

    @patch('deploy_coordinator.start_coordinator', return_value=True)
    def test_restarted_pod_carries_full_peer_list_including_skipped_members(
            self, mock_start):
        # 2 of 3 pods alive (skipped), 1 dead (started). The single started
        # pod's cfg must carry the full 3-member peers list so it can rejoin
        # the cluster via persisted Raft recovery -- this is the restart-1
        # scenario without --set.
        cfg_path = _write_config({
            'coordinator_address': {'value': '0.0.0.0:0'},
        })
        try:
            args = self._args(config=cfg_path)
            pods = [{'name': 'p1', 'ip': '10.0.0.1'},
                    {'name': 'p2', 'ip': '10.0.0.2'},
                    {'name': 'p3', 'ip': '10.0.0.3'}]
            with patch('deploy_coordinator.check_process',
                       side_effect=lambda pod, namespace, process_name,
                                     timeout=10:
                       (pod, 'dead' if pod['name'] == 'p3' else 'alive', 0)):
                rc = cmd_start(args, pods)
            self.assertEqual(rc, 0)
            self.assertEqual(mock_start.call_count, 1)
            cfg = _pos(mock_start.call_args)[2]
            expected_peers = '10.0.0.1:31511,10.0.0.2:31511,10.0.0.3:31511'
            self.assertEqual(cfg['coordinator_raft_initial_peers']['value'],
                             expected_peers)
        finally:
            os.unlink(cfg_path)

    @patch('deploy_coordinator.start_coordinator', return_value=True)
    def test_liveness_probe_error_is_treated_as_dead(self, mock_start):
        # A check_process 'error' (e.g. transient kubectl timeout) must not
        # silently skip the pod; it is started so a real failure surfaces.
        cfg_path = _write_config({
            'coordinator_address': {'value': '0.0.0.0:0'},
        })
        try:
            args = self._args(config=cfg_path)
            with patch('deploy_coordinator.check_process',
                       return_value=(None, 'error', 'timeout')):
                rc = cmd_start(args, [{'name': 'p1', 'ip': '10.0.0.1'}])
            self.assertEqual(rc, 0)
            self.assertEqual(mock_start.call_count, 1)
        finally:
            os.unlink(cfg_path)


class TestCmdWiring(unittest.TestCase):
    """Verify stop/kill/clean pass the coordinator process name and labels."""

    def _pod(self):
        return {'name': 'p1', 'ip': '10.0.0.1'}

    @patch('deploy_coordinator.cmd_stop_impl', return_value=0)
    def test_cmd_stop(self, mock_impl):
        args = SimpleNamespace(namespace='default',
                               remote_config='/tmp/coordinator.config',
                               timeout=10)
        rc = cmd_stop(args, [self._pod()])
        self.assertEqual(rc, 0)
        mock_impl.assert_called_once_with(
            [self._pod()], 'default', '/tmp/coordinator.config',
            'coordinators', 10)

    @patch('deploy_coordinator.cmd_kill_impl', return_value=0)
    def test_cmd_kill_passes_process_name(self, mock_impl):
        args = SimpleNamespace(namespace='default',
                               process=PROCESS_NAME, timeout=10)
        rc = cmd_kill(args, [self._pod()])
        self.assertEqual(rc, 0)
        mock_impl.assert_called_once_with(
            [self._pod()], 'default', PROCESS_NAME,
            'coordinators', 10)

    @patch('deploy_coordinator.cmd_clean_impl', return_value=0)
    def test_cmd_clean_uses_coordinator_process(self, mock_impl):
        # clean has no --process flag; it is hardcoded to the role's
        # PROCESS_NAME so a coordinator clean never kills datasystem_worker
        # by mistake.
        args = SimpleNamespace(namespace='default',
                               remote_config='/tmp/coordinator.config',
                               timeout=10)
        rc = cmd_clean(args, [self._pod()])
        self.assertEqual(rc, 0)
        mock_impl.assert_called_once_with(
            [self._pod()], 'default', '/tmp/coordinator.config',
            PROCESS_NAME, 'coordinator logs', 10)


class TestCmdDeploy(unittest.TestCase):
    """cmd_deploy: full lifecycle (pods -> whl -> coordinators with peers).

    Each step is mocked so the orchestration logic is tested without a real
    cluster. The role-specific concern under test is that multi-instance
    coordinators get coordinator_raft_initial_peers injected with the full
    member list (including self), while a single instance stays in
    single-node no-election mode (peers left untouched).
    """

    def _pod(self, name, ip):
        return {'name': name, 'ip': ip}

    def _nodes(self, n):
        return [{'ip': f'10.0.0.{i + 1}', 'name': f'node-{i}'}
                for i in range(n)]

    def _args(self, **overrides):
        defaults = dict(
            prefixes=['coordinator-a'],
            namespace='default',
            timeout=10,
            config=None,
            port=31511,
            remote_config='/tmp/coordinator.config',
            set=[],
            enable_procmon=True,
            procmon_dir=None,
            whl='/path/to/datasystem.whl',
            image='registry/coordinator:latest',
            yaml='config/pod_config.yaml.example',
            cpu='8',
            memory='16Gi',
            requests_cpu=None,
            requests_memory=None,
            instances=2,
            force=False,
            dry_run=False,
        )
        defaults.update(overrides)
        return SimpleNamespace(**defaults)

    @patch('deploy_coordinator.start_coordinator', return_value=True)
    @patch('deploy_coordinator.cmd_install_impl', return_value=0)
    @patch('deploy_coordinator.get_pods')
    @patch('deploy_coordinator.discover_nodes')
    @patch('deploy_pods.cmd_deploy', return_value=0)
    def test_full_lifecycle_installs_whl_and_starts_with_peers(
            self, mock_deploy_pods, mock_discover, mock_get_pods,
            mock_install, mock_start):
        pods = [self._pod('coordinator-a-0', '10.0.0.1'),
                self._pod('coordinator-a-1', '10.0.0.2')]
        mock_get_pods.return_value = pods
        mock_discover.return_value = self._nodes(2)
        cfg_path = _write_config({
            'coordinator_address': {'value': '0.0.0.0:0'},
        })
        try:
            args = self._args(config=cfg_path, instances=2)
            rc = cmd_deploy(args)
            self.assertEqual(rc, 0)

            # Step 1: deploy_pods got a replicas string spread across nodes.
            deploy_ns = mock_deploy_pods.call_args[0][0]
            self.assertEqual(deploy_ns.replicas, '10.0.0.1:1,10.0.0.2:1')
            self.assertFalse(deploy_ns.dry_run)

            # Step 2: whl install ran once for the pod batch.
            mock_install.assert_called_once()

            # Step 3: one start_coordinator per pod, each cfg carries the
            # per-pod address and the full member list (including self).
            self.assertEqual(mock_start.call_count, 2)
            expected_peers = '10.0.0.1:31511,10.0.0.2:31511'
            seen_peers = set()
            for call in mock_start.call_args_list:
                pod = _pos(call)[0]
                cfg = _pos(call)[2]
                self.assertEqual(cfg[ADDRESS_KEY]['value'],
                                 f'{pod["ip"]}:31511')
                self.assertEqual(cfg['coordinator_raft_initial_peers']['value'],
                                 expected_peers)
                seen_peers.add(cfg['coordinator_raft_initial_peers']['value'])
            # Every coordinator got the same full member list.
            self.assertEqual(len(seen_peers), 1)
        finally:
            os.unlink(cfg_path)

    @patch('deploy_coordinator.start_coordinator', return_value=True)
    @patch('deploy_coordinator.cmd_install_impl', return_value=0)
    @patch('deploy_coordinator.get_pods')
    @patch('deploy_coordinator.discover_nodes')
    @patch('deploy_pods.cmd_deploy', return_value=0)
    def test_single_instance_leaves_peers_untouched(
            self, mock_deploy_pods, mock_discover, mock_get_pods,
            mock_install, mock_start):
        mock_get_pods.return_value = [self._pod('coordinator-a-0', '10.0.0.1')]
        mock_discover.return_value = self._nodes(1)
        cfg_path = _write_config({
            'coordinator_address': {'value': '0.0.0.0:0'},
        })
        try:
            args = self._args(config=cfg_path, instances=1)
            rc = cmd_deploy(args)
            self.assertEqual(rc, 0)
            self.assertEqual(mock_start.call_count, 1)
            cfg = _pos(mock_start.call_args)[2]
            self.assertEqual(cfg[ADDRESS_KEY]['value'], '10.0.0.1:31511')
            # N == 1 -> single-node no-election mode; peers not injected.
            self.assertNotIn('coordinator_raft_initial_peers', cfg)
        finally:
            os.unlink(cfg_path)

    @patch('deploy_coordinator.discover_nodes')
    @patch('deploy_pods.cmd_deploy', return_value=0)
    def test_instances_spread_round_robin_across_nodes(
            self, mock_deploy_pods, mock_discover):
        # 3 nodes, 5 instances -> 2,2,1 balanced spread.
        mock_discover.return_value = self._nodes(3)
        args = self._args(instances=5, dry_run=True)
        rc = cmd_deploy(args)
        self.assertEqual(rc, 0)
        deploy_ns = mock_deploy_pods.call_args[0][0]
        self.assertEqual(deploy_ns.replicas,
                         '10.0.0.1:2,10.0.0.2:2,10.0.0.3:1')

    @patch('deploy_coordinator.discover_nodes')
    @patch('deploy_pods.cmd_deploy', return_value=0)
    def test_more_instances_than_nodes_spreads_evenly(
            self, mock_deploy_pods, mock_discover):
        # 2 nodes, 7 instances -> 4,3.
        mock_discover.return_value = self._nodes(2)
        args = self._args(instances=7, dry_run=True)
        rc = cmd_deploy(args)
        self.assertEqual(rc, 0)
        deploy_ns = mock_deploy_pods.call_args[0][0]
        self.assertEqual(deploy_ns.replicas, '10.0.0.1:4,10.0.0.2:3')

    @patch('deploy_coordinator.discover_nodes', return_value=[])
    @patch('deploy_pods.cmd_deploy')
    def test_no_nodes_discovered_errors(self, mock_deploy_pods, mock_discover):
        args = self._args(instances=3)
        rc = cmd_deploy(args)
        self.assertEqual(rc, 1)
        mock_deploy_pods.assert_not_called()

    @patch('deploy_coordinator.cmd_install_impl')
    @patch('deploy_coordinator.get_pods', return_value=[])
    @patch('deploy_coordinator.discover_nodes')
    @patch('deploy_pods.cmd_deploy', return_value=0)
    def test_no_pods_after_bringup_aborts_before_whl(
            self, mock_deploy_pods, mock_discover, mock_get_pods,
            mock_install):
        mock_discover.return_value = self._nodes(1)
        args = self._args(instances=1)
        rc = cmd_deploy(args)
        self.assertEqual(rc, 1)
        mock_install.assert_not_called()

    @patch('deploy_coordinator.start_coordinator')
    @patch('deploy_coordinator.cmd_install_impl')
    @patch('deploy_coordinator.get_pods')
    @patch('deploy_coordinator.discover_nodes')
    @patch('deploy_pods.cmd_deploy', return_value=1)
    def test_deploy_pods_failure_aborts_before_whl_and_start(
            self, mock_deploy_pods, mock_discover, mock_get_pods,
            mock_install, mock_start):
        mock_discover.return_value = self._nodes(1)
        args = self._args(instances=1)
        rc = cmd_deploy(args)
        self.assertEqual(rc, 1)
        mock_install.assert_not_called()
        mock_start.assert_not_called()

    @patch('deploy_coordinator.start_coordinator')
    @patch('deploy_coordinator.cmd_install_impl', return_value=1)
    @patch('deploy_coordinator.get_pods')
    @patch('deploy_coordinator.discover_nodes')
    @patch('deploy_pods.cmd_deploy', return_value=0)
    def test_whl_install_failure_aborts_before_start(
            self, mock_deploy_pods, mock_discover, mock_get_pods,
            mock_install, mock_start):
        mock_get_pods.return_value = [self._pod('coordinator-a-0', '10.0.0.1')]
        mock_discover.return_value = self._nodes(1)
        cfg_path = _write_config({
            'coordinator_address': {'value': '0.0.0.0:0'},
        })
        try:
            args = self._args(config=cfg_path, instances=1)
            rc = cmd_deploy(args)
            self.assertEqual(rc, 1)
            mock_start.assert_not_called()
        finally:
            os.unlink(cfg_path)

    @patch('deploy_coordinator.discover_nodes')
    @patch('deploy_pods.cmd_deploy')
    def test_requires_exactly_one_prefix(self, mock_deploy_pods, mock_discover):
        mock_discover.return_value = self._nodes(1)
        args = self._args(prefixes=['coordinator-a', 'coordinator-b'],
                          instances=1)
        rc = cmd_deploy(args)
        self.assertEqual(rc, 1)
        mock_deploy_pods.assert_not_called()

    @patch('deploy_coordinator.discover_nodes')
    @patch('deploy_pods.cmd_deploy')
    def test_zero_prefixes_errors(self, mock_deploy_pods, mock_discover):
        mock_discover.return_value = self._nodes(1)
        args = self._args(prefixes=[], instances=1)
        rc = cmd_deploy(args)
        self.assertEqual(rc, 1)
        mock_deploy_pods.assert_not_called()

    @patch('deploy_coordinator.discover_nodes')
    @patch('deploy_pods.cmd_deploy', return_value=0)
    def test_dry_run_skips_whl_and_start(self, mock_deploy_pods, mock_discover):
        mock_discover.return_value = self._nodes(1)
        args = self._args(instances=1, dry_run=True)
        rc = cmd_deploy(args)
        self.assertEqual(rc, 0)
        deploy_ns = mock_deploy_pods.call_args[0][0]
        self.assertTrue(deploy_ns.dry_run)

    @patch('deploy_coordinator.discover_nodes')
    @patch('deploy_pods.cmd_deploy', return_value=0)
    def test_force_forwarded_and_wait_always_on(
            self, mock_deploy_pods, mock_discover):
        # --wait is not exposed: deploy always waits for pods to be Running
        # before whl install / coordinator start, since those steps require
        # Running pods. --force is forwarded verbatim.
        mock_discover.return_value = self._nodes(1)
        args = self._args(instances=1, force=True, dry_run=True)
        rc = cmd_deploy(args)
        self.assertEqual(rc, 0)
        deploy_ns = mock_deploy_pods.call_args[0][0]
        self.assertTrue(deploy_ns.force)
        self.assertTrue(deploy_ns.wait)

    @patch('deploy_coordinator.discover_nodes')
    @patch('deploy_pods.cmd_deploy', return_value=0)
    def test_resource_overrides_forwarded_to_deploy_pods(
            self, mock_deploy_pods, mock_discover):
        mock_discover.return_value = self._nodes(1)
        args = self._args(instances=1, cpu='16', memory='32Gi',
                          requests_cpu='8', requests_memory='16Gi',
                          dry_run=True)
        rc = cmd_deploy(args)
        self.assertEqual(rc, 0)
        deploy_ns = mock_deploy_pods.call_args[0][0]
        self.assertEqual(deploy_ns.cpu, '16')
        self.assertEqual(deploy_ns.memory, '32Gi')
        self.assertEqual(deploy_ns.requests_cpu, '8')
        self.assertEqual(deploy_ns.requests_memory, '16Gi')

    @patch('deploy_coordinator.discover_nodes')
    @patch('deploy_pods.cmd_deploy', return_value=0)
    def test_requests_default_to_limits_when_unset(
            self, mock_deploy_pods, mock_discover):
        mock_discover.return_value = self._nodes(1)
        args = self._args(instances=1, cpu='16', memory='32Gi',
                          requests_cpu=None, requests_memory=None,
                          dry_run=True)
        rc = cmd_deploy(args)
        self.assertEqual(rc, 0)
        deploy_ns = mock_deploy_pods.call_args[0][0]
        self.assertEqual(deploy_ns.requests_cpu, '16')
        self.assertEqual(deploy_ns.requests_memory, '32Gi')


if __name__ == '__main__':
    unittest.main()
