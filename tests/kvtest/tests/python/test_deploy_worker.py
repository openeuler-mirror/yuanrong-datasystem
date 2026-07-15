#!/usr/bin/env python3
"""Tests for deploy_worker.py role-specific layer.

The shared kubectl / procmon / orchestration primitives are tested in
test_deploy_common.py. This file focuses on what is unique to the worker
role: start_worker delegation to deploy_common.start_service with the
datasystem_worker binding, and cmd_start's per-pod worker_address
injection, NUMA option construction, --set override application, and
procmon dir resolution from the worker config.
"""

import json
import os
import sys
import tempfile
import unittest
from types import SimpleNamespace
from unittest.mock import patch

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from deploy_worker import (
    ADDRESS_KEY,
    PROCESS_NAME,
    cmd_install,
    cmd_start,
    start_worker,
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


class TestStartWorker(unittest.TestCase):
    """start_worker should delegate to deploy_common.start_service with the
    worker role's binding (datasystem_worker binary) and forward numactl_opts.
    """

    @patch('deploy_worker.start_service', return_value=True)
    def test_delegates_with_worker_binding_and_numactl(self, mock_start):
        pod = {'name': 'p1', 'ip': '10.0.0.1'}
        cfg = {'worker_address': {'value': 'old'}}
        ok = start_worker(pod, 'default', cfg, 31501, '/tmp/worker.config',
                          enable_procmon=True, procmon_remote_dir='/tmp',
                          numactl_opts='-N 0', timeout=10)
        self.assertTrue(ok)
        mock_start.assert_called_once_with(
            pod, 'default', cfg, '/tmp/worker.config', 31501,
            PROCESS_NAME, True, '/tmp',
            numactl_opts='-N 0', timeout=10)

    @patch('deploy_worker.start_service', return_value=True)
    def test_no_numactl_by_default(self, mock_start):
        pod = {'name': 'p1', 'ip': '10.0.0.1'}
        start_worker(pod, 'default', {}, 31501, '/tmp/worker.config',
                     enable_procmon=False, procmon_remote_dir='/tmp',
                     numactl_opts=None, timeout=10)
        self.assertIsNone(_kw(mock_start.call_args)['numactl_opts'])

    @patch('deploy_worker.start_service', return_value=True)
    def test_uses_worker_process_name(self, mock_start):
        pod = {'name': 'p1', 'ip': '10.0.0.1'}
        start_worker(pod, 'default', {}, 31501, '/tmp/worker.config',
                     timeout=10)
        # positional arg index 5 is process_name
        self.assertEqual(_pos(mock_start.call_args)[5], 'datasystem_worker')


class TestCmdStart(unittest.TestCase):
    """cmd_start: per-pod worker_address injection, NUMA opts, overrides,
    procmon dir resolution."""

    def _args(self, **overrides):
        defaults = dict(namespace='default', port=31501,
                        remote_config='/tmp/worker.config',
                        set=[], enable_procmon=True, procmon_dir=None,
                        numa_nodes=None, cpu_bind=None, timeout=10,
                        config=None)
        defaults.update(overrides)
        return SimpleNamespace(**defaults)

    @patch('deploy_worker.start_worker', return_value=True)
    def test_injects_worker_address_per_pod(self, mock_start):
        cfg_path = _write_config({
            'worker_address': {'value': '0.0.0.0:0'},
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
                             '10.0.0.1:31501')
            self.assertEqual(_pos(c1)[2][ADDRESS_KEY]['value'],
                             '10.0.0.2:31501')
            # procmon_dir resolved from log_dir in the config
            self.assertEqual(_kw(c0)['procmon_remote_dir'], '/var/log/ds')
            # each pod gets a distinct deep-copied config
            self.assertIsNot(_pos(c0)[2], _pos(c1)[2])
        finally:
            os.unlink(cfg_path)

    @patch('deploy_worker.start_worker', return_value=True)
    def test_numactl_opts_from_cpu_bind(self, mock_start):
        cfg_path = _write_config({'worker_address': {'value': '0.0.0.0:0'}})
        try:
            args = self._args(config=cfg_path, cpu_bind='0-7')
            cmd_start(args, [{'name': 'p1', 'ip': '10.0.0.1'}])
            self.assertEqual(_kw(mock_start.call_args)['numactl_opts'],
                             '-C 0-7')
        finally:
            os.unlink(cfg_path)

    @patch('deploy_worker.start_worker', return_value=True)
    def test_numactl_opts_from_numa_nodes(self, mock_start):
        cfg_path = _write_config({'worker_address': {'value': '0.0.0.0:0'}})
        try:
            args = self._args(config=cfg_path, numa_nodes='0,1')
            cmd_start(args, [{'name': 'p1', 'ip': '10.0.0.1'}])
            self.assertEqual(_kw(mock_start.call_args)['numactl_opts'],
                             '-N 0,1')
        finally:
            os.unlink(cfg_path)

    @patch('deploy_worker.start_worker', return_value=True)
    def test_cpu_bind_wins_over_numa_nodes(self, mock_start):
        # Matches original precedence: cpu_bind is assigned after numa_nodes,
        # so it overrides when both are set.
        cfg_path = _write_config({'worker_address': {'value': '0.0.0.0:0'}})
        try:
            args = self._args(config=cfg_path, numa_nodes='0', cpu_bind='0-3')
            cmd_start(args, [{'name': 'p1', 'ip': '10.0.0.1'}])
            self.assertEqual(_kw(mock_start.call_args)['numactl_opts'],
                             '-C 0-3')
        finally:
            os.unlink(cfg_path)

    @patch('deploy_worker.start_worker', return_value=True)
    def test_set_overrides_applied(self, mock_start):
        cfg_path = _write_config({'worker_address': {'value': '0.0.0.0:0'}})
        try:
            args = self._args(config=cfg_path, set=['rpc_thread_num=128'])
            cmd_start(args, [{'name': 'p1', 'ip': '10.0.0.1'}])
            cfg = _pos(mock_start.call_args)[2]
            self.assertEqual(cfg['rpc_thread_num']['value'], 128)
        finally:
            os.unlink(cfg_path)

    @patch('deploy_worker.start_worker', return_value=True)
    def test_procmon_dir_falls_back_to_remote_config_dir(self, mock_start):
        # No log_dir in config -> procmon_dir defaults to the remote-config
        # directory (dirname of /tmp/worker.config == /tmp).
        cfg_path = _write_config({'worker_address': {'value': '0.0.0.0:0'}})
        try:
            args = self._args(config=cfg_path)
            cmd_start(args, [{'name': 'p1', 'ip': '10.0.0.1'}])
            self.assertEqual(_kw(mock_start.call_args)['procmon_remote_dir'],
                             '/tmp')
        finally:
            os.unlink(cfg_path)


class TestCmdInstall(unittest.TestCase):
    """cmd_install delegates to deploy_common.cmd_install_impl with the
    whl path and timeout from args."""

    @patch('deploy_worker.cmd_install_impl', return_value=0)
    def test_delegates_with_whl(self, mock_impl):
        args = SimpleNamespace(namespace='default',
                               whl='/path/to/pkg.whl', timeout=10)
        pods = [{'name': 'p1', 'ip': '10.0.0.1'}]
        rc = cmd_install(args, pods)
        self.assertEqual(rc, 0)
        mock_impl.assert_called_once_with(
            pods, 'default', '/path/to/pkg.whl', 10)


if __name__ == '__main__':
    unittest.main()
