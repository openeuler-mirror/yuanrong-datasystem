#!/usr/bin/env python3
"""Tests for deploy_worker.py pure/logic functions."""

import json
import os
import sys
import tempfile
import unittest
from unittest.mock import patch, MagicMock, PropertyMock
from types import SimpleNamespace

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from deploy_worker import (
    find_default_whl,
    do_for_all_pods,
    check_worker,
    kill_worker,
    upload_procmon,
)


class TestConfigOverrideParsing(unittest.TestCase):
    """Test config override parsing logic from cmd_start."""

    def _parse_override(self, value):
        """Replicate the override parsing logic from cmd_start."""
        value = value.strip()
        if value.lower() == 'true':
            return True
        elif value.lower() == 'false':
            return False
        elif value.lower() in ('null', 'none'):
            return None
        else:
            try:
                if '.' in value:
                    return float(value)
                else:
                    return int(value)
            except ValueError:
                return value

    def test_string(self):
        self.assertEqual(self._parse_override('hello'), 'hello')

    def test_bool_true(self):
        self.assertTrue(self._parse_override('true'))
        self.assertTrue(self._parse_override('True'))

    def test_bool_false(self):
        self.assertFalse(self._parse_override('false'))
        self.assertFalse(self._parse_override('False'))

    def test_int(self):
        self.assertEqual(self._parse_override('42'), 42)

    def test_float(self):
        self.assertEqual(self._parse_override('3.14'), 3.14)

    def test_null(self):
        self.assertIsNone(self._parse_override('null'))
        self.assertIsNone(self._parse_override('none'))

    def test_negative_int(self):
        self.assertEqual(self._parse_override('-1'), -1)

    def test_string_with_equals(self):
        # Value after first '=' should be parsed as string
        self.assertEqual(self._parse_override('key=value'), 'key=value')


class TestProcmonDirDefault(unittest.TestCase):
    """Test procmon_dir default logic from cmd_start."""

    def _resolve_procmon_dir(self, config_template, remote_config):
        """Replicate the procmon_dir resolution from cmd_start."""
        procmon_dir = None
        log_dir_entry = config_template.get('log_dir', {})
        if isinstance(log_dir_entry, dict):
            procmon_dir = log_dir_entry.get('value', None)
        else:
            procmon_dir = log_dir_entry or None
        if not procmon_dir:
            procmon_dir = os.path.dirname(remote_config)
        return procmon_dir

    def test_from_log_dir_dict(self):
        cfg = {'log_dir': {'value': '/var/log/datasystem'}}
        self.assertEqual(self._resolve_procmon_dir(cfg, '/tmp/worker.config'), '/var/log/datasystem')

    def test_from_log_dir_string(self):
        cfg = {'log_dir': '/data/logs'}
        self.assertEqual(self._resolve_procmon_dir(cfg, '/tmp/worker.config'), '/data/logs')

    def test_fallback_to_remote_config_dir(self):
        cfg = {}
        self.assertEqual(self._resolve_procmon_dir(cfg, '/data/workers/worker.config'), '/data/workers')

    def test_empty_log_dir_falls_back(self):
        cfg = {'log_dir': ''}
        self.assertEqual(self._resolve_procmon_dir(cfg, '/opt/worker.config'), '/opt')

    def test_log_dir_dict_empty_value(self):
        cfg = {'log_dir': {'value': ''}}
        self.assertEqual(self._resolve_procmon_dir(cfg, '/tmp/worker.config'), '/tmp')


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


class TestFindDefaultWhl(unittest.TestCase):
    @patch('deploy_worker.glob.glob')
    def test_found(self, mock_glob):
        mock_glob.return_value = ['/some/path/openyuanrong_datasystem-0.8.2-cp311.whl']
        result = find_default_whl()
        self.assertIn('0.8.2', result)

    @patch('deploy_worker.glob.glob')
    def test_not_found(self, mock_glob):
        mock_glob.return_value = []
        result = find_default_whl()
        self.assertEqual(result, '')


class TestUploadProcmon(unittest.TestCase):
    @patch('deploy_worker.kubectl_cp_to')
    @patch('deploy_worker.kubectl_exec')
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


if __name__ == '__main__':
    unittest.main()
