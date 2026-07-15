#!/usr/bin/env python3
"""Tests for deploy_common.py shared primitives.

Covers the role-agnostic layer shared by deploy_worker.py and
deploy_coordinator.py: config override parsing, procmon dir resolution,
parallel pod orchestration, procmon upload, process check/kill, pid
lookup by port, remote log_dir reading, and pod discovery.
"""

import json
import os
import subprocess
import sys
import unittest
from unittest.mock import patch, MagicMock

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from deploy_common import (
    check_process,
    cmd_install_impl,
    do_for_all_pods,
    find_default_whl,
    find_pid_by_port,
    get_pods,
    kill_process,
    parse_config_override,
    read_remote_log_dir,
    resolve_procmon_dir,
    upload_procmon,
)


class TestParseConfigOverride(unittest.TestCase):
    """Test parse_config_override typing."""

    def test_string(self):
        self.assertEqual(parse_config_override('hello'), 'hello')

    def test_bool_true(self):
        self.assertTrue(parse_config_override('true'))
        self.assertTrue(parse_config_override('True'))

    def test_bool_false(self):
        self.assertFalse(parse_config_override('false'))
        self.assertFalse(parse_config_override('False'))

    def test_int(self):
        self.assertEqual(parse_config_override('42'), 42)

    def test_float(self):
        self.assertEqual(parse_config_override('3.14'), 3.14)

    def test_null(self):
        self.assertIsNone(parse_config_override('null'))
        self.assertIsNone(parse_config_override('none'))

    def test_negative_int(self):
        self.assertEqual(parse_config_override('-1'), -1)

    def test_string_with_equals(self):
        # Value after first '=' is parsed as string (no '=' recognition here;
        # the splitter lives in apply_config_overrides, not parse).
        self.assertEqual(parse_config_override('key=value'), 'key=value')

    def test_whitespace_is_stripped(self):
        self.assertEqual(parse_config_override('  hello  '), 'hello')
        self.assertEqual(parse_config_override('  42  '), 42)


class TestResolveProcmonDir(unittest.TestCase):
    """Test resolve_procmon_dir: log_dir dict/string, fallback to config dir."""

    def test_from_log_dir_dict(self):
        cfg = {'log_dir': {'value': '/var/log/datasystem'}}
        self.assertEqual(resolve_procmon_dir(cfg, '/tmp/worker.config'),
                         '/var/log/datasystem')

    def test_from_log_dir_string(self):
        cfg = {'log_dir': '/data/logs'}
        self.assertEqual(resolve_procmon_dir(cfg, '/tmp/worker.config'),
                         '/data/logs')

    def test_fallback_to_remote_config_dir(self):
        cfg = {}
        self.assertEqual(resolve_procmon_dir(cfg, '/data/workers/worker.config'),
                         '/data/workers')

    def test_empty_log_dir_falls_back(self):
        cfg = {'log_dir': ''}
        self.assertEqual(resolve_procmon_dir(cfg, '/opt/worker.config'),
                         '/opt')

    def test_log_dir_dict_empty_value(self):
        cfg = {'log_dir': {'value': ''}}
        self.assertEqual(resolve_procmon_dir(cfg, '/tmp/worker.config'), '/tmp')


class TestDoForAllPods(unittest.TestCase):
    def test_all_succeed(self):
        pods = [{'name': 'p1'}, {'name': 'p2'}]
        result = do_for_all_pods(pods, lambda pod: True, 'test')
        self.assertEqual(result, 0)

    def test_partial_failure(self):
        pods = [{'name': 'p1'}, {'name': 'p2'}]
        call_count = [0]

        def op(pod):
            call_count[0] += 1
            return call_count[0] == 1

        result = do_for_all_pods(pods, op, 'test')
        self.assertEqual(result, 1)

    def test_all_fail(self):
        pods = [{'name': 'p1'}]
        result = do_for_all_pods(pods, lambda pod: False, 'test')
        self.assertEqual(result, 1)


class TestUploadProcmon(unittest.TestCase):
    @patch('deploy_common.kubectl_cp_to')
    @patch('deploy_common.kubectl_exec')
    @patch('os.path.exists', return_value=True)
    def test_success(self, mock_exists, mock_exec, mock_cp):
        mock_exec.return_value = MagicMock(returncode=0)
        mock_cp.return_value = None
        pod = {'name': 'test-pod', 'ip': '10.0.0.1'}
        result = upload_procmon(pod, 'default', '/tmp')
        self.assertTrue(result)

    @patch('os.path.exists', return_value=False)
    def test_no_procmon_file(self, mock_exists):
        pod = {'name': 'test-pod', 'ip': '10.0.0.1'}
        result = upload_procmon(pod, 'default', '/tmp')
        self.assertFalse(result)


class TestCheckProcess(unittest.TestCase):
    def _pod(self):
        return {'name': 'p1', 'ip': '10.0.0.1'}

    @patch('deploy_common.kubectl_exec')
    def test_alive(self, mock_exec):
        mock_exec.return_value = MagicMock(returncode=0, stdout='2\n')
        pod, status, detail = check_process(self._pod(), 'default',
                                            'datasystem_worker')
        self.assertEqual(status, 'alive')
        self.assertEqual(detail, 2)

    @patch('deploy_common.kubectl_exec')
    def test_dead(self, mock_exec):
        mock_exec.return_value = MagicMock(returncode=0, stdout='0\n')
        pod, status, detail = check_process(self._pod(), 'default',
                                            'datasystem_worker')
        self.assertEqual(status, 'dead')
        self.assertEqual(detail, 0)

    @patch('deploy_common.kubectl_exec')
    def test_error_on_nonzero_return(self, mock_exec):
        mock_exec.return_value = MagicMock(returncode=1, stderr='boom')
        pod, status, detail = check_process(self._pod(), 'default',
                                            'datasystem_worker')
        self.assertEqual(status, 'error')
        self.assertEqual(detail, 'boom')

    @patch('deploy_common.kubectl_exec')
    def test_timeout(self, mock_exec):
        mock_exec.side_effect = subprocess.TimeoutExpired(cmd=['kubectl'],
                                                          timeout=300)
        pod, status, detail = check_process(self._pod(), 'default',
                                            'datasystem_worker')
        self.assertEqual(status, 'error')
        self.assertEqual(detail, 'timeout')


class TestKillProcess(unittest.TestCase):
    @patch('deploy_common.kubectl_exec')
    def test_calls_kill_with_process_name(self, mock_exec):
        mock_exec.return_value = MagicMock(returncode=0)
        pod = {'name': 'p1', 'ip': '10.0.0.1'}
        ok = kill_process(pod, 'default', 'datasystem_coordinator')
        self.assertTrue(ok)
        self.assertEqual(mock_exec.call_count, 1)
        # call_args is a (args, kwargs) tuple on Py3.7; use index access.
        cmd = mock_exec.call_args[0][2]
        self.assertIn('datasystem_coordinator', cmd)
        self.assertIn('procmon.py', cmd)

    @patch('deploy_common.kubectl_exec')
    def test_timeout_returns_false(self, mock_exec):
        mock_exec.side_effect = subprocess.TimeoutExpired(cmd=['kubectl'],
                                                          timeout=300)
        pod = {'name': 'p1', 'ip': '10.0.0.1'}
        ok = kill_process(pod, 'default', 'datasystem_worker')
        self.assertFalse(ok)


class TestFindPidByPort(unittest.TestCase):
    def _pod(self):
        return {'name': 'p1', 'ip': '10.0.0.1'}

    @patch('deploy_common.kubectl_exec')
    def test_found_by_port(self, mock_exec):
        mock_exec.return_value = MagicMock(returncode=0, stdout='1234\n')
        pid = find_pid_by_port(self._pod(), 'default', 31511,
                               'datasystem_coordinator')
        self.assertEqual(pid, '1234')

    @patch('deploy_common.kubectl_exec')
    def test_falls_back_to_pgrep(self, mock_exec):
        # First call (ss) returns empty; second (pgrep) returns a pid.
        mock_exec.side_effect = [
            MagicMock(returncode=0, stdout=''),
            MagicMock(returncode=0, stdout='5678\n'),
        ]
        pid = find_pid_by_port(self._pod(), 'default', 31501,
                               'datasystem_worker')
        self.assertEqual(pid, '5678')
        # Second call must use pgrep with the process name.
        # call_args_list[i] is a (args, kwargs) tuple on Py3.7.
        second_cmd = mock_exec.call_args_list[1][0][2]
        self.assertIn('pgrep -f datasystem_worker', second_cmd)

    @patch('deploy_common.kubectl_exec')
    def test_returns_none_when_both_miss(self, mock_exec):
        mock_exec.return_value = MagicMock(returncode=0, stdout='')
        pid = find_pid_by_port(self._pod(), 'default', 31511,
                               'datasystem_coordinator')
        self.assertIsNone(pid)


class TestReadRemoteLogDir(unittest.TestCase):
    def _pods(self):
        return [{'name': 'p1', 'ip': '10.0.0.1'}]

    @patch('deploy_common.kubectl_exec')
    def test_log_dir_dict(self, mock_exec):
        cfg = {'log_dir': {'value': '/var/log/ds'}}
        mock_exec.return_value = MagicMock(stdout=json.dumps(cfg),
                                           returncode=0)
        log_dir, parsed = read_remote_log_dir('default', self._pods(),
                                              '/tmp/coordinator.config')
        self.assertEqual(log_dir, '/var/log/ds')
        self.assertEqual(parsed, cfg)

    @patch('deploy_common.kubectl_exec')
    def test_log_dir_string(self, mock_exec):
        cfg = {'log_dir': '/data/logs'}
        mock_exec.return_value = MagicMock(stdout=json.dumps(cfg),
                                           returncode=0)
        log_dir, _ = read_remote_log_dir('default', self._pods(),
                                         '/tmp/worker.config')
        self.assertEqual(log_dir, '/data/logs')

    @patch('deploy_common.kubectl_exec')
    def test_missing_log_dir_returns_none(self, mock_exec):
        mock_exec.return_value = MagicMock(stdout=json.dumps({}),
                                           returncode=0)
        log_dir, _ = read_remote_log_dir('default', self._pods(),
                                        '/tmp/worker.config')
        self.assertIsNone(log_dir)

    @patch('deploy_common.kubectl_exec')
    def test_cat_failure_returns_none_and_empty(self, mock_exec):
        mock_exec.side_effect = subprocess.CalledProcessError(1, 'cat')
        log_dir, parsed = read_remote_log_dir('default', self._pods(),
                                              '/tmp/coordinator.config')
        self.assertIsNone(log_dir)
        self.assertEqual(parsed, {})

    def test_no_pods_returns_none(self):
        log_dir, parsed = read_remote_log_dir('default', [],
                                              '/tmp/coordinator.config')
        self.assertIsNone(log_dir)
        self.assertEqual(parsed, {})


class TestGetPods(unittest.TestCase):
    @patch('deploy_common.subprocess.check_output')
    def test_filter_sort_and_dedup(self, mock_co):
        mock_co.return_value = json.dumps({
            'items': [
                {'metadata': {'name': 'worker-b'}, 'status': {'podIP': '10.0.0.2'}},
                {'metadata': {'name': 'worker-a'}, 'status': {'podIP': '10.0.0.1'}},
                {'metadata': {'name': 'other-x'}, 'status': {'podIP': '10.0.0.3'}},
                # Duplicate of worker-a (dedup defensive).
                {'metadata': {'name': 'worker-a'}, 'status': {'podIP': '10.0.0.1'}},
                # No podIP -> skipped.
                {'metadata': {'name': 'worker-c'}, 'status': {}},
            ]
        })
        pods = get_pods('default', ['worker-'])
        names = [p['name'] for p in pods]
        self.assertEqual(names, ['worker-a', 'worker-b'])
        self.assertEqual(pods[0]['ip'], '10.0.0.1')

    @patch('deploy_common.subprocess.check_output')
    def test_or_semantics_multiple_prefixes(self, mock_co):
        mock_co.return_value = json.dumps({
            'items': [
                {'metadata': {'name': 'worker-a'}, 'status': {'podIP': '10.0.0.1'}},
                {'metadata': {'name': 'coordinator-a'}, 'status': {'podIP': '10.0.0.9'}},
            ]
        })
        pods = get_pods('default', ['worker-', 'coordinator-'])
        self.assertEqual([p['name'] for p in pods],
                         ['coordinator-a', 'worker-a'])


class TestFindDefaultWhl(unittest.TestCase):
    @patch('deploy_common.glob.glob')
    def test_found(self, mock_glob):
        mock_glob.return_value = [
            '/some/path/openyuanrong_datasystem-0.8.2-cp311.whl']
        result = find_default_whl()
        self.assertIn('0.8.2', result)

    @patch('deploy_common.glob.glob')
    def test_not_found(self, mock_glob):
        mock_glob.return_value = []
        result = find_default_whl()
        self.assertEqual(result, '')


class TestCmdInstallImpl(unittest.TestCase):
    """Guard: missing local whl fails fast; valid whl dispatches per pod."""

    @patch('deploy_common.install_whl')
    def test_missing_whl_returns_1_without_install(self, mock_install):
        rc = cmd_install_impl([{'name': 'p1', 'ip': '10.0.0.1'}], 'default',
                              '/no/such/file.whl', timeout=10)
        self.assertEqual(rc, 1)
        mock_install.assert_not_called()

    @patch('deploy_common.install_whl', return_value=True)
    @patch('os.path.exists', return_value=True)
    def test_valid_whl_dispatches_per_pod(self, mock_exists, mock_install):
        pods = [{'name': 'p1', 'ip': '10.0.0.1'},
                {'name': 'p2', 'ip': '10.0.0.2'}]
        rc = cmd_install_impl(pods, 'default', '/path/pkg.whl', timeout=10)
        self.assertEqual(rc, 0)
        self.assertEqual(mock_install.call_count, 2)


if __name__ == '__main__':
    unittest.main()
