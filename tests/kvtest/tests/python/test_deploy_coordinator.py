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
    resolution, --set overrides. No NUMA option construction (coordinator
    does not support NUMA)."""

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


if __name__ == '__main__':
    unittest.main()
