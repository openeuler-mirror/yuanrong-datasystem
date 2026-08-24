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


if __name__ == '__main__':
    unittest.main()
