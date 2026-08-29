#!/usr/bin/env python3
"""Tests for deploy_jf.py -- JF mock pod lifecycle CLI.

Focuses on ``cmd_collect`` (the new log-shipping subcommand): verifies it
gates on ``remote_dir`` existence (skip silently if absent, like
``deploy_common.collect_logs_from_pod``), lists ``*.log`` / ``*.txt``
files, and base64-decodes each into the local output dir. Other
subcommands (deploy/start/stop/check/clean) are thin shells over the
local ``_kubectl_exec`` helper and are not exhaustively tested here.
"""

import base64
import os
import sys
import tempfile
import unittest
from unittest.mock import patch, MagicMock

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from deploy_jf import cmd_collect


def _pos(call):
    return call[0]


def _kw(call):
    return call[1]


class TestCmdCollect(unittest.TestCase):
    """cmd_collect: existence gate on remote_dir, file listing, base64
    decode into per-pod local dir."""

    def _pod(self):
        return {'name': 'jf-pod-0', 'ip': '10.0.0.1'}

    def _args(self, **overrides):
        defaults = dict(namespace='default',
                         prefixes=['jf'],
                         remote_dir='/tmp/jf_mock',
                         output=None,
                         timeout=30)
        defaults.update(overrides)
        return _SimpleNS(**defaults)

    @patch('deploy_jf._kubectl_exec')
    def test_collects_log_files_when_remote_dir_exists(self, mock_exec):
        # remote_dir exists, 2 log files (jf_mock.log + stdout.log).
        # Both must be base64-decoded into the per-pod local dir.
        jf_content = b'[2026-08-28T10:00:00] register 200 from=10.0.0.1\n'
        stdout_content = b'[2026-08-28T10:00:01] extra stdout\n'

        def _resp(ns, pod, cmd, timeout=30):
            if cmd.startswith('ls -d '):
                return MagicMock(returncode=0)
            if cmd.startswith('ls '):
                return MagicMock(returncode=0,
                                 stdout='/tmp/jf_mock/jf_mock.log\n'
                                        '/tmp/jf_mock/stdout.log\n')
            if cmd.startswith('base64 '):
                path = cmd.split('base64 ', 1)[1]
                if path.endswith('jf_mock.log'):
                    return MagicMock(returncode=0,
                                     stdout=base64.b64encode(jf_content).decode())
                if path.endswith('stdout.log'):
                    return MagicMock(returncode=0,
                                     stdout=base64.b64encode(stdout_content).decode())
                return MagicMock(returncode=1)
            return MagicMock(returncode=0, stdout='')
        mock_exec.side_effect = _resp

        with tempfile.TemporaryDirectory() as out:
            args = self._args(output=out)
            rc = cmd_collect(args, [self._pod()])
            self.assertEqual(rc, 0)

            pod_dir = os.path.join(out, 'jf-pod-0')
            self.assertTrue(os.path.isfile(os.path.join(pod_dir, 'jf_mock.log')))
            self.assertTrue(os.path.isfile(os.path.join(pod_dir, 'stdout.log')))
            with open(os.path.join(pod_dir, 'jf_mock.log'), 'rb') as f:
                self.assertEqual(f.read(), jf_content)
            with open(os.path.join(pod_dir, 'stdout.log'), 'rb') as f:
                self.assertEqual(f.read(), stdout_content)

    @patch('deploy_jf._kubectl_exec')
    def test_skips_silently_when_remote_dir_missing(self, mock_exec):
        # remote_dir does not exist (server was never deployed, or was
        # cleaned). The ls -d gate fails; no file-listing or base64 call
        # is issued; the pod is skipped, not errored. This is the
        # "no --standalone flag needed" path -- the same collect works
        # whether or not the server was ever started.
        mock_exec.return_value = MagicMock(returncode=1)

        with tempfile.TemporaryDirectory() as out:
            args = self._args(output=out)
            rc = cmd_collect(args, [self._pod()])
            self.assertEqual(rc, 0)
            # Only the ls -d call was made; no ls *.log, no base64.
            self.assertEqual(mock_exec.call_count, 1)
            self.assertIn('ls -d', _pos(mock_exec.call_args)[2])
            pod_dir = os.path.join(out, 'jf-pod-0')
            self.assertTrue(os.path.isdir(pod_dir))
            self.assertEqual(os.listdir(pod_dir), [])

    @patch('deploy_jf._kubectl_exec')
    def test_skips_when_dir_exists_but_no_log_files(self, mock_exec):
        # remote_dir exists but has no *.log / *.txt (server started but
        # hasn't written yet, or logs were rotated away). Skip silently
        # without erroring.
        def _resp(ns, pod, cmd, timeout=30):
            if cmd.startswith('ls -d '):
                return MagicMock(returncode=0)
            if cmd.startswith('ls '):
                return MagicMock(returncode=0, stdout='')
            return MagicMock(returncode=0, stdout='')
        mock_exec.side_effect = _resp

        with tempfile.TemporaryDirectory() as out:
            args = self._args(output=out)
            rc = cmd_collect(args, [self._pod()])
            self.assertEqual(rc, 0)
            # ls -d (gate) + ls *.log (listing) = 2 calls; no base64.
            self.assertEqual(mock_exec.call_count, 2)

    @patch('deploy_jf._kubectl_exec')
    def test_base64_failure_skips_file_keeps_going(self, mock_exec):
        # One file's base64 returns non-zero (file deleted mid-collect,
        # or permission denied). cmd_collect must log + skip that file
        # and continue with the rest, not abort the whole pod.
        good_content = b'line 1\n'

        def _resp(ns, pod, cmd, timeout=30):
            if cmd.startswith('ls -d '):
                return MagicMock(returncode=0)
            if cmd.startswith('ls '):
                return MagicMock(returncode=0,
                                 stdout='/tmp/jf_mock/jf_mock.log\n'
                                        '/tmp/jf_mock/gone.log\n')
            if cmd.startswith('base64 '):
                path = cmd.split('base64 ', 1)[1]
                if path.endswith('jf_mock.log'):
                    return MagicMock(returncode=0,
                                     stdout=base64.b64encode(good_content).decode())
                # gone.log: simulate a failed base64 (file vanished).
                return MagicMock(returncode=1)
            return MagicMock(returncode=0, stdout='')
        mock_exec.side_effect = _resp

        with tempfile.TemporaryDirectory() as out:
            args = self._args(output=out)
            rc = cmd_collect(args, [self._pod()])
            self.assertEqual(rc, 0)
            pod_dir = os.path.join(out, 'jf-pod-0')
            # jf_mock.log collected; gone.log skipped.
            self.assertTrue(os.path.isfile(os.path.join(pod_dir, 'jf_mock.log')))
            self.assertFalse(os.path.exists(os.path.join(pod_dir, 'gone.log')))

    @patch('deploy_jf._kubectl_exec')
    def test_no_pods_returns_1(self, mock_exec):
        # No matching pods: cmd_collect must not dispatch any kubectl
        # calls and must return 1 (mirrors cmd_start / cmd_check's no-pods
        # contract).
        with tempfile.TemporaryDirectory() as out:
            args = self._args(output=out)
            rc = cmd_collect(args, [])
            self.assertEqual(rc, 1)
            mock_exec.assert_not_called()


class _SimpleNS:
    """Tiny SimpleNamespace substitute -- Python 3.7-safe and avoids
    importing ``types.SimpleNamespace`` (kept local so the test file has
    no non-mock deps beyond stdlib + deploy_jf)."""

    def __init__(self, **kw):
        self.__dict__.update(kw)


if __name__ == '__main__':
    unittest.main()
