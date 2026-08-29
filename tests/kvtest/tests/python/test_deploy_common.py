#!/usr/bin/env python3
"""Tests for deploy_common.py shared primitives.

Covers the role-agnostic layer shared by deploy_worker.py and
deploy_coordinator.py: config override parsing, procmon dir resolution,
parallel pod orchestration, procmon upload, process check/kill, pid
lookup by port, remote log_dir reading, and pod discovery.
"""

import base64
import json
import os
import subprocess
import sys
import tempfile
import unittest
from types import SimpleNamespace
from unittest.mock import patch, MagicMock

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from deploy_common import (
    check_process,
    clean_pod,
    cmd_clean_shared,
    cmd_collect_impl,
    cmd_collect_shared,
    cmd_install_impl,
    collect_logs_from_pod,
    discover_nodes,
    do_for_all_pods,
    find_default_whl,
    find_pid_by_port,
    get_pods,
    kill_process,
    parse_config_override,
    read_remote_log_dir,
    resolve_procmon_dir,
    start_service,
    start_service_standalone,
    upload_launcher,
    upload_procmon,
)
from deploy_common import _extract_ready_check_path


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


class TestUploadLauncher(unittest.TestCase):
    """upload_launcher mirrors upload_procmon but returns the remote path
    (so callers know where to invoke python3) or None on failure."""

    @patch('deploy_common.kubectl_cp_to')
    @patch('deploy_common.kubectl_exec')
    @patch('os.path.exists', return_value=True)
    def test_success_returns_remote_path(self, mock_exists, mock_exec,
                                          mock_cp):
        mock_exec.return_value = MagicMock(returncode=0)
        mock_cp.return_value = None
        pod = {'name': 'test-pod', 'ip': '10.0.0.1'}
        result = upload_launcher(pod, 'default', '/tmp')
        self.assertEqual(result, '/tmp/standalone_launcher.py')
        # Must create the target dir before copying.
        mock_exec.assert_called_once_with('test-pod', 'default',
                                          'mkdir -p /tmp', check=False,
                                          timeout=300)

    @patch('os.path.exists', return_value=False)
    def test_no_launcher_file_returns_none(self, mock_exists):
        pod = {'name': 'test-pod', 'ip': '10.0.0.1'}
        result = upload_launcher(pod, 'default', '/tmp')
        self.assertIsNone(result)

    @patch('deploy_common.kubectl_cp_to', side_effect=Exception('cp boom'))
    @patch('deploy_common.kubectl_exec')
    @patch('os.path.exists', return_value=True)
    def test_cp_failure_returns_none(self, mock_exists, mock_exec, mock_cp):
        mock_exec.return_value = MagicMock(returncode=0)
        pod = {'name': 'test-pod', 'ip': '10.0.0.1'}
        result = upload_launcher(pod, 'default', '/tmp')
        self.assertIsNone(result)


class TestStartServiceStandaloneTiming(unittest.TestCase):
    """start_service_standalone must set pod['_start_elapsed'] covering the
    actual launch + readiness wait (mirrors dscli semantics), regardless of
    whether the launcher path or the legacy nohup fallback was used."""

    def _pod(self):
        return {'name': 'p1', 'ip': '10.0.0.1'}

    @patch('deploy_common.upload_launcher')
    @patch('deploy_common.subprocess.run')
    def test_launcher_path_records_elapsed_and_invokes_python(self, mock_run,
                                                               mock_upload):
        # upload_launcher succeeds; launcher prints PID 1234 on stdout.
        mock_upload.return_value = '/tmp/standalone_launcher.py'
        mock_run.return_value = MagicMock(returncode=0, stdout='1234\n',
                                           stderr='')
        pod = self._pod()
        ok = start_service_standalone(
            pod, 'default', 'worker_test', '/tmp/ds', '/tmp/cfg.json',
            'jf:9999', 'predictor', '', config=None,
            enable_procmon=False, port=31501, process_name='worker_test',
            timeout=10)
        self.assertTrue(ok)
        # Timing recorded as a non-negative float (mocked subprocess returns
        # instantly so the value may be 0.0; we only assert the field is set
        # and is numeric).
        self.assertIn('_start_elapsed', pod)
        self.assertIsInstance(pod['_start_elapsed'], (int, float))
        self.assertGreaterEqual(pod['_start_elapsed'], 0)
        # Launcher invoked via subprocess.run; command must include python3
        # and the launcher script path. NOT the nohup sh -c path.
        self.assertGreaterEqual(mock_run.call_count, 1)
        called_cmd = mock_run.call_args[0][0]
        self.assertIn('python3', called_cmd)
        self.assertIn('/tmp/standalone_launcher.py', called_cmd)
        self.assertNotIn('nohup', ' '.join(called_cmd))

    @patch('deploy_common.upload_launcher')
    @patch('deploy_common.subprocess.run')
    def test_launcher_passes_ready_file_when_config_has_ready_check_path(
            self, mock_run, mock_upload):
        # When the worker config has ready_check_path, the launcher command
        # must include --ready-file <path> so the launcher polls for the
        # authoritative readiness file (worker_oc_server.cpp:2911-2933)
        # instead of falling back to TCP port polling.
        mock_upload.return_value = '/tmp/standalone_launcher.py'
        mock_run.return_value = MagicMock(returncode=0, stdout='1234\n',
                                           stderr='')
        pod = self._pod()
        config = {
            'worker_address': {'value': '10.0.0.1:31501'},
            'ready_check_path': {'value': '/tmp/ds/probe/ready'},
        }
        ok = start_service_standalone(
            pod, 'default', 'worker_test', '/tmp/ds', '/tmp/cfg.json',
            'jf:9999', 'predictor', '', config=config,
            enable_procmon=False, port=31501, process_name='worker_test',
            timeout=10)
        self.assertTrue(ok)
        called_cmd = mock_run.call_args[0][0]
        self.assertIn('--ready-file', called_cmd)
        self.assertIn('/tmp/ds/probe/ready', called_cmd)

    @patch('deploy_common.upload_launcher')
    @patch('deploy_common.subprocess.run')
    def test_launcher_omits_ready_file_when_config_has_no_ready_check_path(
            self, mock_run, mock_upload):
        # When the worker config has no ready_check_path (coordinator or
        # worker with the flag unset), the launcher command must NOT
        # include --ready-file; it falls back to --port polling.
        mock_upload.return_value = '/tmp/standalone_launcher.py'
        mock_run.return_value = MagicMock(returncode=0, stdout='1234\n',
                                           stderr='')
        pod = self._pod()
        config = {'coordinator_address': {'value': '10.0.0.1:31511'}}
        ok = start_service_standalone(
            pod, 'default', 'coordinator_test', '/tmp/ds', '/tmp/cfg.json',
            'jf:9999', 'kvcache_coordinator', '', config=config,
            enable_procmon=False, port=31511,
            process_name='coordinator_test', timeout=10)
        self.assertTrue(ok)
        called_cmd = mock_run.call_args[0][0]
        self.assertNotIn('--ready-file', called_cmd)
        # Still uses --port for coordinator readiness.
        self.assertIn('--port', called_cmd)

    @patch('deploy_common.upload_launcher')
    @patch('deploy_common.subprocess.run')
    @patch('deploy_common.kubectl_exec_raw')
    def test_fallback_path_when_upload_fails_uses_nohup(self, mock_raw,
                                                         mock_run,
                                                         mock_upload):
        # Launcher upload fails -> fall back to nohup path. subprocess.run
        # is the kubectl exec sh -c (no PID returned). kubectl_exec_raw is
        # the pgrep fallback that returns a PID.
        mock_upload.return_value = None
        mock_run.return_value = MagicMock(returncode=0, stdout='',
                                           stderr='')
        mock_raw.return_value = '1234\n'
        pod = self._pod()
        ok = start_service_standalone(
            pod, 'default', 'worker_test', '/tmp/ds', '/tmp/cfg.json',
            'jf:9999', 'predictor', '', config=None,
            enable_procmon=False, port=None, process_name='worker_test',
            timeout=10)
        self.assertTrue(ok)
        # Timing recorded as a non-negative float (mocked subprocess returns
        # instantly so the value may be 0.0; we only assert the field is set).
        self.assertIn('_start_elapsed', pod)
        self.assertIsInstance(pod['_start_elapsed'], (int, float))
        self.assertGreaterEqual(pod['_start_elapsed'], 0)
        # Nohup path used: sh -c with nohup appears in the run call.
        self.assertGreaterEqual(mock_run.call_count, 1)
        called_cmd = ' '.join(mock_run.call_args[0][0])
        self.assertIn('nohup', called_cmd)
        self.assertIn('worker_test', called_cmd)


class TestExtractReadyCheckPath(unittest.TestCase):
    """_extract_ready_check_path must pull ready_check_path out of a
    dscli-style worker config ({"value": "/path"} dict) or plain string,
    and return None when absent / empty / not a config."""

    def test_dict_value(self):
        cfg = {'ready_check_path': {'value': '/tmp/probe/ready',
                                     'description': '...'}}
        self.assertEqual(_extract_ready_check_path(cfg), '/tmp/probe/ready')

    def test_plain_string_value(self):
        cfg = {'ready_check_path': '/tmp/probe/ready'}
        self.assertEqual(_extract_ready_check_path(cfg), '/tmp/probe/ready')

    def test_relative_path_preserved(self):
        # Relative paths are kept as-is; the launcher resolves them against
        # the binary's --cwd (which mirrors the binary's FLAGS_ready_check_path
        # resolution semantics).
        cfg = {'ready_check_path': {'value': './probe/ready'}}
        self.assertEqual(_extract_ready_check_path(cfg), './probe/ready')

    def test_empty_dict_value_returns_none(self):
        cfg = {'ready_check_path': {'value': ''}}
        self.assertIsNone(_extract_ready_check_path(cfg))

    def test_missing_key_returns_none(self):
        cfg = {'worker_address': {'value': '10.0.0.1:31501'}}
        self.assertIsNone(_extract_ready_check_path(cfg))

    def test_none_config_returns_none(self):
        self.assertIsNone(_extract_ready_check_path(None))

    def test_empty_config_returns_none(self):
        self.assertIsNone(_extract_ready_check_path({}))

    def test_non_string_non_dict_value_returns_none(self):
        cfg = {'ready_check_path': 12345}
        self.assertIsNone(_extract_ready_check_path(cfg))


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


class TestStartService(unittest.TestCase):
    """start_service must pick the dscli config flag from process_name.

    dscli start -f <cfg> binds to worker_config_path (starts a worker);
    -C <cfg> binds to coordinator_config_path (starts a coordinator).
    A coordinator config has no worker_address/etcd_address, so starting it
    with -f would make dscli run start_worker and fail backend validation.
    """
    def _pod(self):
        return {'name': 'p1', 'ip': '10.0.0.1'}

    @patch('deploy_common.kubectl_exec')
    @patch('deploy_common.kubectl_cp_to')
    def test_worker_uses_f_flag(self, mock_cp, mock_exec):
        mock_exec.return_value = MagicMock(returncode=0)
        ok = start_service(self._pod(), 'default',
                           {'worker_address': {'value': '10.0.0.1:31501'}},
                           '/tmp/worker.config', 31501, 'datasystem_worker',
                           enable_procmon=False, timeout=10)
        self.assertTrue(ok)
        self.assertEqual(mock_exec.call_count, 1)
        self.assertEqual(mock_exec.call_args[0][2],
                         'dscli start -f /tmp/worker.config')

    @patch('deploy_common.kubectl_exec')
    @patch('deploy_common.kubectl_cp_to')
    def test_coordinator_uses_C_flag(self, mock_cp, mock_exec):
        mock_exec.return_value = MagicMock(returncode=0)
        ok = start_service(self._pod(), 'default',
                           {'coordinator_address': {'value': '10.0.0.1:31511'}},
                           '/tmp/coordinator.config', 31511,
                           'datasystem_coordinator',
                           enable_procmon=False, timeout=10)
        self.assertTrue(ok)
        self.assertEqual(mock_exec.call_count, 1)
        self.assertEqual(mock_exec.call_args[0][2],
                         'dscli start -C /tmp/coordinator.config')

    @patch('deploy_common.kubectl_exec')
    @patch('deploy_common.kubectl_cp_to')
    def test_numactl_opts_appended_for_worker(self, mock_cp, mock_exec):
        mock_exec.return_value = MagicMock(returncode=0)
        start_service(self._pod(), 'default',
                      {'worker_address': {'value': '10.0.0.1:31501'}},
                      '/tmp/worker.config', 31501, 'datasystem_worker',
                      enable_procmon=False, numactl_opts='-N 0', timeout=10)
        self.assertEqual(mock_exec.call_args[0][2],
                         'dscli start -f /tmp/worker.config -N 0')

    @patch('deploy_common.kubectl_exec')
    @patch('deploy_common.kubectl_cp_to')
    def test_coordinator_ignores_numactl_opts(self, mock_cp, mock_exec):
        # numactl is worker-only; coordinator path passes numactl_opts=None,
        # so even if a caller mistakenly passed opts they must not be
        # appended to a coordinator's dscli start -C command.
        mock_exec.return_value = MagicMock(returncode=0)
        start_service(self._pod(), 'default',
                      {'coordinator_address': {'value': '10.0.0.1:31511'}},
                      '/tmp/coordinator.config', 31511,
                      'datasystem_coordinator',
                      enable_procmon=False, numactl_opts='-N 0', timeout=10)
        self.assertEqual(mock_exec.call_args[0][2],
                         'dscli start -C /tmp/coordinator.config')


class TestDiscoverNodes(unittest.TestCase):
    """discover_nodes: parses kubectl get nodes JSON, sorts by name for
    deterministic cross-run distribution (shared by deploy_pods and
    deploy_coordinator), and returns [] on any kubectl failure."""

    @staticmethod
    def _nodes_json(node_specs):
        # node_specs: list of (name, ip); build the kubectl get nodes -o json
        # items list and return its JSON string (check_output returns stdout).
        items = []
        for name, ip in node_specs:
            items.append({
                'metadata': {'name': name},
                'status': {'addresses': [{'type': 'InternalIP', 'address': ip}]},
            })
        return json.dumps({'items': items})

    @patch('deploy_common.subprocess.check_output')
    def test_parses_internal_ip_and_sorts_by_name(self, mock_co):
        # Intentionally unsorted in the API response to prove discover_nodes
        # sorts by name so percentage / round-robin assignment is deterministic
        # across runs (the k8s API does not guarantee item order).
        mock_co.return_value = self._nodes_json(
            [('node-c', '10.0.0.3'),
             ('node-a', '10.0.0.1'),
             ('node-b', '10.0.0.2')])
        nodes = discover_nodes()
        self.assertEqual([n['name'] for n in nodes],
                         ['node-a', 'node-b', 'node-c'])
        self.assertEqual(nodes[0]['ip'], '10.0.0.1')

    @patch('deploy_common.subprocess.check_output')
    def test_returns_empty_on_kubectl_nonzero_exit(self, mock_co):
        # check_output raises CalledProcessError on non-zero exit; discover_nodes
        # swallows it and returns [] so callers can decide whether to abort.
        mock_co.side_effect = subprocess.CalledProcessError(1, ['kubectl'])
        self.assertEqual(discover_nodes(), [])

    @patch('deploy_common.subprocess.check_output')
    def test_returns_empty_when_kubectl_missing(self, mock_co):
        mock_co.side_effect = FileNotFoundError()
        self.assertEqual(discover_nodes(), [])

    @patch('deploy_common.subprocess.check_output')
    def test_returns_empty_on_timeout(self, mock_co):
        mock_co.side_effect = subprocess.TimeoutExpired(cmd=['kubectl'],
                                                       timeout=60)
        self.assertEqual(discover_nodes(), [])

    @patch('deploy_common.subprocess.check_output')
    def test_skips_non_internal_ip_addresses(self, mock_co):
        # A Hostname-type address before InternalIP must not shadow the
        # InternalIP; discover_nodes picks InternalIP and breaks per-node.
        mock_co.return_value = json.dumps({'items': [{
            'metadata': {'name': 'node-a'},
            'status': {'addresses': [
                {'type': 'Hostname', 'address': 'node-a'},
                {'type': 'InternalIP', 'address': '10.0.0.1'},
            ]},
        }]})
        nodes = discover_nodes()
        self.assertEqual(len(nodes), 1)
        self.assertEqual(nodes[0]['ip'], '10.0.0.1')


def _kubectl_exec_responder(log_dir_files, remote_dir_exists,
                            stdout_log_content=None,
                            procmon_log_content=None):
    """Build a side_effect for kubectl_exec that responds by command text.

    Responds to the collect_logs_from_pod call sequence:
      * `ls -d {log_dir}`           -> returncode 0 (log_dir always exists
                                       in these tests)
      * `ls {log_dir}/*.log ...`    -> returncode 0, stdout = log_dir_files
      * `base64 <path>`             -> returncode 0, stdout = base64 bytes
                                       of the matching file's content; raises
                                       CalledProcessError if path unknown
      * `ls -d {remote_dir}`        -> returncode 0 if remote_dir_exists else 1
    """
    file_map = {}
    if stdout_log_content is not None:
        file_map['__stdout__'] = stdout_log_content
    if procmon_log_content is not None:
        file_map['__procmon__'] = procmon_log_content

    def _resp(*args, **kwargs):
        cmd = args[2]
        if cmd.startswith('ls -d '):
            target = cmd.split('ls -d ', 1)[1].split(' 2>/dev/null', 1)[0]
            if target in ('/var/log/ds', '/tmp/ds_worker', '/tmp/ds_coordinator'):
                if target == '/var/log/ds':
                    return MagicMock(returncode=0)
                return MagicMock(returncode=0 if remote_dir_exists else 1)
            return MagicMock(returncode=0)
        if cmd.startswith('ls '):
            return MagicMock(returncode=0, stdout='\n'.join(log_dir_files))
        if cmd.startswith('base64 '):
            path = cmd.split('base64 ', 1)[1]
            if path == '/tmp/ds_worker/stdout.log' or path == '/tmp/ds_coordinator/stdout.log':
                if stdout_log_content is None:
                    raise subprocess.CalledProcessError(1, cmd, b'')
                return MagicMock(returncode=0, stdout=base64.b64encode(
                    stdout_log_content).decode())
            if 'resource_monitor.log' in path:
                if procmon_log_content is None:
                    raise subprocess.CalledProcessError(1, cmd, b'')
                return MagicMock(returncode=0, stdout=base64.b64encode(
                    procmon_log_content).decode())
            return MagicMock(returncode=0, stdout=base64.b64encode(b'').decode())
        return MagicMock(returncode=0, stdout='')
    return _resp


class TestCollectLogsFromPod(unittest.TestCase):
    """collect_logs_from_pod: stdout.log is collected from remote_dir (where
    start_service_standalone actually writes it), gated on the dir's
    existence so dscli-mode collects skip silently without a --standalone
    flag. Covers the regression where the old code looked in
    remote_config_dir/stdout.log and never matched the real path."""

    def _pod(self):
        return {'name': 'p1', 'ip': '10.0.0.1'}

    def _run_collect(self, remote_dir, remote_dir_exists,
                     stdout_log_content=None):
        tmp = tempfile.mkdtemp()
        try:
            responder = _kubectl_exec_responder(
                log_dir_files=['/var/log/ds/worker.log'],
                remote_dir_exists=remote_dir_exists,
                stdout_log_content=stdout_log_content)
            with patch('deploy_common.kubectl_exec', side_effect=responder):
                ok = collect_logs_from_pod(
                    self._pod(), 'default', '/var/log/ds', tmp,
                    remote_config_dir='/tmp',
                    remote_dir=remote_dir, timeout=10)
            return ok, tmp
        except Exception:
            import shutil
            shutil.rmtree(tmp, ignore_errors=True)
            raise

    def _read_local(self, tmp, fname):
        path = os.path.join(tmp, self._pod()['name'], fname)
        if not os.path.exists(path):
            return None
        with open(path, 'rb') as f:
            return f.read()

    def test_standalone_collects_stdout_log_from_remote_dir(self):
        # remote_dir exists + stdout.log present -> file is collected.
        # This is the regression guard: the old code looked in
        # /tmp/stdout.log (remote_config_dir) and never found the file.
        ok, tmp = self._run_collect(
            remote_dir='/tmp/ds_worker', remote_dir_exists=True,
            stdout_log_content=b'worker_test stdout output\n')
        try:
            self.assertTrue(ok)
            self.assertEqual(self._read_local(tmp, 'stdout.log'),
                             b'worker_test stdout output\n')
        finally:
            import shutil
            shutil.rmtree(tmp, ignore_errors=True)

    def test_standalone_skips_stdout_log_when_remote_dir_missing(self):
        # dscli-mode pod: remote_dir was never created (dscli installs into
        # the package prefix). The `ls -d` gate fails silently and no
        # base64 call is issued for stdout.log. This is the "no -S flag
        # needed" path -- the same collect invocation serves both modes.
        ok, tmp = self._run_collect(
            remote_dir='/tmp/ds_worker', remote_dir_exists=False,
            stdout_log_content=b'should-not-be-collected\n')
        try:
            self.assertTrue(ok)
            self.assertIsNone(self._read_local(tmp, 'stdout.log'))
        finally:
            import shutil
            shutil.rmtree(tmp, ignore_errors=True)

    def test_standalone_skips_stdout_log_when_file_missing(self):
        # remote_dir exists but stdout.log absent (binary hasn't written
        # yet, or crashed before redirect opened). The base64 call raises
        # CalledProcessError and collect silently skips -- no exception
        # propagates, no partial file is left on disk.
        ok, tmp = self._run_collect(
            remote_dir='/tmp/ds_worker', remote_dir_exists=True,
            stdout_log_content=None)
        try:
            self.assertTrue(ok)
            self.assertIsNone(self._read_local(tmp, 'stdout.log'))
        finally:
            import shutil
            shutil.rmtree(tmp, ignore_errors=True)

    def test_no_remote_dir_skips_stdout_collection_entirely(self):
        # remote_dir=None (older caller, no --remote-dir passed): no `ls -d`
        # or base64 call is issued at all. Pins that the existence gate is
        # gated on `remote_dir is not None` first, so None never reaches
        # kubectl.
        captured_cmds = []
        tmp = tempfile.mkdtemp()
        try:
            def _capture(*args, **kwargs):
                captured_cmds.append(args[2])
                return MagicMock(returncode=0, stdout='')
            with patch('deploy_common.kubectl_exec', side_effect=_capture):
                collect_logs_from_pod(self._pod(), 'default', '/var/log/ds',
                                      tmp, remote_config_dir='/tmp',
                                      remote_dir=None, timeout=10)
            self.assertFalse(
                any('stdout.log' in c for c in captured_cmds),
                'stdout.log must not be queried when remote_dir is None')
        finally:
            import shutil
            shutil.rmtree(tmp, ignore_errors=True)


class TestCmdCollectShared(unittest.TestCase):
    """cmd_collect_shared forwards args.remote_dir to cmd_collect_impl so
    standalone-mode collects pick up stdout.log; missing attr defaults to
    None (older callers / test stubs)."""

    @patch('deploy_common.cmd_collect_impl', return_value=0)
    def test_forwards_remote_dir_from_args(self, mock_impl):
        args = SimpleNamespace(namespace='default',
                               remote_config='/tmp/worker.config',
                               output='out',
                               remote_dir='/tmp/ds_worker')
        pods = [{'name': 'p1', 'ip': '10.0.0.1'}]
        rc = cmd_collect_shared(args, pods, 'worker logs', timeout=10)
        self.assertEqual(rc, 0)
        pos = mock_impl.call_args[0]
        kwargs = mock_impl.call_args[1]
        self.assertEqual(pos[0], pods)
        self.assertEqual(pos[1], 'default')
        self.assertEqual(pos[2], '/tmp/worker.config')
        self.assertEqual(pos[3], 'out')
        self.assertEqual(pos[4], 'worker logs')
        self.assertEqual(kwargs['remote_dir'], '/tmp/ds_worker')
        self.assertEqual(kwargs['timeout'], 10)

    @patch('deploy_common.cmd_collect_impl', return_value=0)
    def test_missing_remote_dir_attr_defaults_to_none(self, mock_impl):
        # Older callers (or test stubs) may not set remote_dir; getattr
        # must fall back to None rather than AttributeError, so the same
        # shared helper keeps working after the new param is added.
        args = SimpleNamespace(namespace='default',
                               remote_config='/tmp/worker.config',
                               output='out')
        pods = [{'name': 'p1', 'ip': '10.0.0.1'}]
        rc = cmd_collect_shared(args, pods, 'worker logs', timeout=10)
        self.assertEqual(rc, 0)
        kwargs = mock_impl.call_args[1]
        self.assertIsNone(kwargs['remote_dir'])


class TestCleanPod(unittest.TestCase):
    """clean_pod: kill + log_dir + resource_monitor.log removal, plus the
    standalone-only rm -rf remote_dir (binary + .so + stdout.log). Order is
    kill -> log_dir -> resource_monitor.log -> remote_dir so a still-running
    binary does not race the rm -rf on its own files."""

    def _pod(self):
        return {'name': 'p1', 'ip': '10.0.0.1'}

    @patch('deploy_common.kill_process')
    @patch('deploy_common.kubectl_exec')
    def test_non_standalone_skips_remote_dir(self, mock_exec, mock_kill):
        # remote_dir=None (dscli mode): only log_dir + resource_monitor.log
        # are touched. rm -rf {remote_dir} must NOT be issued.
        clean_pod(self._pod(), 'default', '/var/log/ds', '/tmp',
                  'datasystem_coordinator', remote_dir=None, timeout=10)
        cmds = [c[0][2] for c in mock_exec.call_args_list]
        self.assertIn('rm -rf /var/log/ds', cmds)
        self.assertIn('rm -f /tmp/resource_monitor.log', cmds)
        self.assertFalse(any(c.startswith('rm -rf /tmp/ds') for c in cmds))
        # kill_process was the first call (before any rm)
        mock_kill.assert_called_once_with(
            self._pod(), 'default', 'datasystem_coordinator', timeout=10)

    @patch('deploy_common.kill_process')
    @patch('deploy_common.kubectl_exec')
    def test_standalone_removes_remote_dir_after_logs(self, mock_exec, mock_kill):
        # remote_dir set (standalone mode): rm -rf remote_dir is issued, and
        # it comes AFTER the log_dir + resource_monitor.log cleanups so a
        # still-running binary does not see its files disappear mid-shutdown.
        clean_pod(self._pod(), 'default', '/var/log/ds', '/tmp',
                  'coordinator_test', remote_dir='/tmp/ds_coordinator',
                  timeout=10)
        cmds = [c[0][2] for c in mock_exec.call_args_list]
        self.assertIn('rm -rf /var/log/ds', cmds)
        self.assertIn('rm -f /tmp/resource_monitor.log', cmds)
        self.assertIn('rm -rf /tmp/ds_coordinator', cmds)
        self.assertLess(cmds.index('rm -rf /var/log/ds'),
                        cmds.index('rm -rf /tmp/ds_coordinator'))
        self.assertLess(cmds.index('rm -f /tmp/resource_monitor.log'),
                        cmds.index('rm -rf /tmp/ds_coordinator'))
        # kill target switches to the standalone binary name
        mock_kill.assert_called_once_with(
            self._pod(), 'default', 'coordinator_test', timeout=10)

    @patch('deploy_common.kill_process')
    @patch('deploy_common.kubectl_exec')
    def test_standalone_without_log_dir_still_cleans_remote_dir(self, mock_exec, mock_kill):
        # A config without log_dir must still remove remote_dir in standalone
        # mode (stdout.log lives there and would otherwise accumulate across
        # deploys). log_dir is skipped via the `if log_dir:` guard.
        clean_pod(self._pod(), 'default', None, '/tmp',
                  'worker_test', remote_dir='/tmp/ds_worker', timeout=10)
        cmds = [c[0][2] for c in mock_exec.call_args_list]
        self.assertNotIn('rm -rf None', cmds)
        self.assertIn('rm -f /tmp/resource_monitor.log', cmds)
        self.assertIn('rm -rf /tmp/ds_worker', cmds)


class TestCmdCleanShared(unittest.TestCase):
    """cmd_clean_shared resolves the kill target + remote_dir from
    args.standalone, mirroring cmd_kill_shared's pattern but with both process
    names supplied by the role file (clean has no --process flag)."""

    def _args(self, **overrides):
        defaults = dict(namespace='default',
                        remote_config='/tmp/coordinator.config',
                        standalone=False,
                        remote_dir='/tmp/ds_coordinator')
        defaults.update(overrides)
        return SimpleNamespace(**defaults)

    @patch('deploy_common.cmd_clean_impl', return_value=0)
    def test_non_standalone_uses_dscli_process_and_skips_remote_dir(self, mock_impl):
        # Non-standalone: kill datasystem_coordinator, do NOT remove remote_dir
        # (dscli installs into the package prefix, not --remote-dir).
        args = self._args(standalone=False)
        pods = [{'name': 'p1', 'ip': '10.0.0.1'}]
        rc = cmd_clean_shared(args, pods, 'datasystem_coordinator',
                              'coordinator_test', 'coordinator logs', timeout=10)
        self.assertEqual(rc, 0)
        pos = mock_impl.call_args[0]
        kwargs = mock_impl.call_args[1]
        # positional: pods, namespace, remote_config, process_name, label
        self.assertEqual(pos[0], pods)
        self.assertEqual(pos[1], 'default')
        self.assertEqual(pos[2], '/tmp/coordinator.config')
        self.assertEqual(pos[3], 'datasystem_coordinator')
        self.assertEqual(pos[4], 'coordinator logs')
        # remote_dir=None signals the impl to skip the rm -rf step
        self.assertIsNone(kwargs['remote_dir'])
        self.assertEqual(kwargs['timeout'], 10)

    @patch('deploy_common.cmd_clean_impl', return_value=0)
    def test_standalone_uses_test_binary_process_and_removes_remote_dir(self, mock_impl):
        # Standalone: kill coordinator_test, rm -rf remote_dir so a re-deploy
        # starts clean (no stale binary / .so / appended stdout.log).
        args = self._args(standalone=True)
        pods = [{'name': 'p1', 'ip': '10.0.0.1'}]
        rc = cmd_clean_shared(args, pods, 'datasystem_coordinator',
                              'coordinator_test', 'coordinator logs', timeout=10)
        self.assertEqual(rc, 0)
        pos = mock_impl.call_args[0]
        kwargs = mock_impl.call_args[1]
        self.assertEqual(pos[3], 'coordinator_test')
        self.assertEqual(kwargs['remote_dir'], '/tmp/ds_coordinator')

    @patch('deploy_common.cmd_clean_impl', return_value=0)
    def test_standalone_without_remote_dir_attr_skips_rm_rf(self, mock_impl):
        # If a caller forgets --remote-dir in standalone mode, getattr falls
        # back to None and clean skips the rm -rf instead of issuing rm -rf
        # (the role CLIs always set a default, but the shared helper itself
        # must tolerate a missing attr rather than AttributeError).
        args = SimpleNamespace(namespace='default',
                               remote_config='/tmp/coordinator.config',
                               standalone=True)
        pods = [{'name': 'p1', 'ip': '10.0.0.1'}]
        rc = cmd_clean_shared(args, pods, 'datasystem_coordinator',
                              'coordinator_test', 'coordinator logs', timeout=10)
        self.assertEqual(rc, 0)
        kwargs = mock_impl.call_args[1]
        self.assertIsNone(kwargs['remote_dir'])


if __name__ == '__main__':
    unittest.main()
