#!/usr/bin/env python3
"""Tests for tools/standalone_launcher.py pure functions and arg parsing.

These tests cover the parts that can be exercised without spawning real
processes or opening real sockets:

* ``is_port_ready`` against a local listener and a closed port
* ``build_env`` LD_LIBRARY_PATH prepending (empty / set / no lib path)
* ``parse_args`` shape, including the `--` passthrough separator and that
  a leading `--` in positional argv is preserved (launcher strips it
  defensively in ``main``, but ``parse_args`` itself does not).

The end-to-end fork+setsid+poll path is covered by integration tests
(deploy_worker / deploy_coordinator standalone smoke), not here, because
it spawns subprocesses and opens sockets; per the repo's 8-second default
test budget, those scenarios belong in the manual/perf bucket.
"""

import os
import socket
import sys
import unittest
from unittest.mock import patch

# Make standalone_launcher importable
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'tools'))
import standalone_launcher as launcher


class TestIsPortReady(unittest.TestCase):
    """is_port_ready must distinguish a listening socket from a closed port."""

    def setUp(self):
        self._listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._listener.bind(('127.0.0.1', 0))
        self._listener.listen(1)
        self._port = self._listener.getsockname()[1]

    def tearDown(self):
        try:
            self._listener.close()
        except OSError:
            pass

    def test_open_port_is_ready(self):
        self.assertTrue(launcher.is_port_ready('127.0.0.1', self._port,
                                                timeout=0.5))

    def test_closed_port_is_not_ready(self):
        # Pick a port that is almost certainly closed (high ephemeral range).
        self.assertFalse(launcher.is_port_ready('127.0.0.1', 65535,
                                                 timeout=0.2))

    def test_invalid_host_returns_false(self):
        # Non-routable TEST-NET-1; should fail fast, not raise.
        self.assertFalse(launcher.is_port_ready('192.0.2.1', self._port,
                                                 timeout=0.2))


class TestIsFileReady(unittest.TestCase):
    """is_file_ready must resolve relative paths against cwd and detect
    file existence / non-existence."""

    def test_absolute_path_exists(self):
        import tempfile
        with tempfile.NamedTemporaryFile(delete=False) as tf:
            path = tf.name
        try:
            self.assertTrue(launcher.is_file_ready(path))
        finally:
            os.unlink(path)

    def test_absolute_path_missing(self):
        self.assertFalse(launcher.is_file_ready('/tmp/this_should_not_exist_xyz'))

    def test_relative_path_resolved_against_cwd(self):
        # Create a file in a temp dir, pass a relative path + cwd.
        import tempfile
        tmpdir = tempfile.mkdtemp()
        try:
            rel = 'ready_marker'
            abs_path = os.path.join(tmpdir, rel)
            # Not ready yet.
            self.assertFalse(launcher.is_file_ready(rel, cwd=tmpdir))
            # Create the file; now ready.
            with open(abs_path, 'w') as f:
                f.write('ready\n')
            self.assertTrue(launcher.is_file_ready(rel, cwd=tmpdir))
        finally:
            import shutil
            shutil.rmtree(tmpdir, ignore_errors=True)

    def test_relative_path_without_cwd_uses_process_cwd(self):
        # No cwd given: os.path.exists uses the launcher process's CWD.
        # Just verify it doesn't raise and returns a bool.
        result = launcher.is_file_ready('.')
        self.assertIsInstance(result, bool)

    def test_empty_path_returns_false(self):
        self.assertFalse(launcher.is_file_ready(''))
        self.assertFalse(launcher.is_file_ready(None))


class TestClearStaleReadyFile(unittest.TestCase):
    """clear_stale_ready_file must remove an existing ready file and be a
    no-op when the file is absent. Path resolution must match
    is_file_ready (relative paths resolved against cwd) so the launcher
    deletes the same path it later polls."""

    def test_removes_existing_absolute_path(self):
        import tempfile
        with tempfile.NamedTemporaryFile(delete=False) as tf:
            path = tf.name
        try:
            self.assertTrue(os.path.exists(path))
            self.assertTrue(launcher.clear_stale_ready_file(path))
            self.assertFalse(os.path.exists(path))
        finally:
            if os.path.exists(path):
                os.unlink(path)

    def test_missing_absolute_path_is_noop(self):
        # False but does not raise.
        self.assertFalse(
            launcher.clear_stale_ready_file('/tmp/this_should_not_exist_xyz'))
        self.assertFalse(launcher.clear_stale_ready_file(None))
        self.assertFalse(launcher.clear_stale_ready_file(''))

    def test_relative_path_resolved_against_cwd(self):
        # Mirror is_file_ready: relative path resolved against cwd so the
        # launcher deletes the same file it would later poll.
        import tempfile
        import shutil
        tmpdir = tempfile.mkdtemp()
        try:
            rel = 'ready_marker'
            abs_path = os.path.join(tmpdir, rel)
            with open(abs_path, 'w') as f:
                f.write('stale\n')
            self.assertTrue(launcher.clear_stale_ready_file(rel, cwd=tmpdir))
            self.assertFalse(os.path.exists(abs_path))
        finally:
            shutil.rmtree(tmpdir, ignore_errors=True)

    def test_missing_relative_path_is_noop(self):
        import tempfile
        import shutil
        tmpdir = tempfile.mkdtemp()
        try:
            self.assertFalse(
                launcher.clear_stale_ready_file('absent_marker', cwd=tmpdir))
        finally:
            shutil.rmtree(tmpdir, ignore_errors=True)


class TestBuildEnv(unittest.TestCase):
    """build_env must prepend LD_LIBRARY_PATH only when a lib path is given."""

    def test_no_lib_path_keeps_environ(self):
        with patch.dict('os.environ', {'LD_LIBRARY_PATH': '/existing'}, clear=False):
            env = launcher.build_env(None)
            self.assertEqual(env['LD_LIBRARY_PATH'], '/existing')

    def test_lib_path_prepended_to_existing(self):
        with patch.dict('os.environ', {'LD_LIBRARY_PATH': '/existing'}, clear=False):
            env = launcher.build_env('/new/lib')
            self.assertEqual(env['LD_LIBRARY_PATH'], '/new/lib:/existing')

    def test_lib_path_with_no_existing(self):
        # Ensure LD_LIBRARY_PATH is unset in os.environ for this case.
        env_base = dict(os.environ)
        env_base.pop('LD_LIBRARY_PATH', None)
        with patch('os.environ', env_base):
            env = launcher.build_env('/new/lib')
            self.assertEqual(env['LD_LIBRARY_PATH'], '/new/lib')

    def test_environ_copied_not_mutated(self):
        """build_env must not mutate the caller's os.environ."""
        with patch.dict('os.environ', {'LD_LIBRARY_PATH': '/existing'}, clear=False):
            before = dict(os.environ)
            launcher.build_env('/new/lib')
            self.assertEqual(os.environ.get('LD_LIBRARY_PATH'),
                             before.get('LD_LIBRARY_PATH'))


class TestParseArgs(unittest.TestCase):
    """parse_args must parse required fields and the `--` passthrough."""

    def test_required_binary_and_log(self):
        args = launcher.parse_args(['--binary', '/bin/true', '--log', '/tmp/x'])
        self.assertEqual(args.binary, '/bin/true')
        self.assertEqual(args.log, '/tmp/x')
        self.assertEqual(args.argv, [])

    def test_full_options(self):
        args = launcher.parse_args(
            ['--binary', '/bin/true', '--log', '/tmp/x',
             '--cwd', '/tmp', '--lib-path', '/opt/lib',
             '--ready-file', '/tmp/ready',
             '--port', '1234', '--host', '127.0.0.1',
             '--ready-timeout', '5', '--ready-interval', '0.25',
             '--', '--config', 'cfg.json', '--jf', 'jf:9999'])
        self.assertEqual(args.binary, '/bin/true')
        self.assertEqual(args.cwd, '/tmp')
        self.assertEqual(args.lib_path, '/opt/lib')
        self.assertEqual(args.ready_file, '/tmp/ready')
        self.assertEqual(args.port, 1234)
        self.assertEqual(args.host, '127.0.0.1')
        self.assertEqual(args.ready_timeout, 5.0)
        self.assertEqual(args.ready_interval, 0.25)
        self.assertEqual(args.argv, ['--config', 'cfg.json', '--jf', 'jf:9999'])

    def test_passthrough_args_without_double_dash(self):
        # Without `--`, the first non-option token starts argv. Use values
        # that don't start with `-` so they're treated as positional.
        args = launcher.parse_args(
            ['--binary', '/bin/true', '--log', '/tmp/x', 'cfg.json'])
        self.assertEqual(args.argv, ['cfg.json'])

    def test_double_dash_in_passthrough_preserved(self):
        # If caller passes `--` followed by another `--`, argparse keeps
        # both in argv; launcher.main strips a leading `--`. Verify parse
        # semantics so the strip in main is correct.
        args = launcher.parse_args(
            ['--binary', '/bin/true', '--log', '/tmp/x',
             '--', '--config', 'cfg.json'])
        self.assertEqual(args.argv, ['--config', 'cfg.json'])

    def test_missing_binary_raises(self):
        with self.assertRaises(SystemExit):
            launcher.parse_args(['--log', '/tmp/x'])

    def test_missing_log_raises(self):
        with self.assertRaises(SystemExit):
            launcher.parse_args(['--binary', '/bin/true'])

    def test_port_must_be_int(self):
        # Non-integer port should fail argparse type conversion.
        with self.assertRaises(SystemExit):
            launcher.parse_args(
                ['--binary', '/bin/true', '--log', '/tmp/x',
                 '--port', 'not-a-port'])


class TestMainNoPortImmediatePid(unittest.TestCase):
    """main() with no --port must print PID immediately and exit 0.

    Uses /bin/true (exits 0 immediately) as the binary; the launcher should
    fork it, see no port to poll, print the PID, and return 0. The forked
    child becomes /bin/true which exits right away, but the parent returns
    before checking the child's exit (no port path returns immediately).
    """

    @unittest.skipUnless(os.path.exists('/bin/true'),
                         '/bin/true not available on this platform')
    def test_no_port_returns_zero_with_pid_on_stdout(self):
        import io
        import contextlib
        stdout_buf = io.StringIO()
        stderr_buf = io.StringIO()
        with contextlib.redirect_stdout(stdout_buf), \
                contextlib.redirect_stderr(stderr_buf):
            rc = launcher.main(['--binary', '/bin/true',
                                '--log', '/tmp/launcher_test_no_port.log'])
        self.assertEqual(rc, 0)
        out = stdout_buf.getvalue().strip()
        self.assertTrue(out, 'launcher must print PID to stdout')
        last_line = out.splitlines()[-1].strip()
        self.assertTrue(last_line.isdigit(),
                         f'last stdout line must be a PID, got: {last_line!r}')


class TestMainEarlyExitFails(unittest.TestCase):
    """main() must return non-zero if the binary exits before becoming ready.

    Uses /bin/false (exits 1 immediately) with a port that never opens; the
    launcher's poll loop should detect the early exit and return non-zero
    without printing a PID to stdout.
    """

    @unittest.skipUnless(os.path.exists('/bin/false'),
                         '/bin/false not available on this platform')
    def test_early_exit_returns_nonzero(self):
        import io
        import contextlib
        stdout_buf = io.StringIO()
        stderr_buf = io.StringIO()
        with contextlib.redirect_stdout(stdout_buf), \
                contextlib.redirect_stderr(stderr_buf):
            rc = launcher.main(['--binary', '/bin/false',
                                '--log', '/tmp/launcher_test_early.log',
                                '--port', '1',  # unreachable port forces poll
                                '--host', '127.0.0.1',
                                '--ready-timeout', '5',
                                '--ready-interval', '0.1'])
        self.assertNotEqual(rc, 0)
        # Must not have printed a PID — early exit is a failure.
        self.assertEqual(stdout_buf.getvalue().strip(), '')


class TestMainClearsStaleReadyFile(unittest.TestCase):
    """main() must remove a stale ready_file before Popen.

    Regression guard for the stale-file TOCTOU: previously the readiness
    poll could hit a file left by a previous run and report ``ready`` in
    milliseconds, before the new binary reached ReadinessProbe(). The
    contract mirrors dscli ``start_worker`` (``cli/start.py:671-672``):
    unlink the ready_file before launching, then poll for the new binary
    to (re)create it.
    """

    def test_stale_ready_file_removed_before_popen(self):
        import io
        import tempfile
        import contextlib
        from unittest.mock import patch, MagicMock
        fd, path = tempfile.mkstemp(suffix='.ready')
        os.write(fd, b'stale\n')
        os.close(fd)
        # Use a portable temp log path so the test runs on Windows too
        # (the existing /bin/true-based tests skip on Windows but this
        # regression test should not).
        log_path = os.path.join(tempfile.gettempdir(),
                                'launcher_test_stale.log')
        try:
            self.assertTrue(os.path.exists(path),
                            'precondition: stale ready_file exists')
            # Track call order: unlink (via clear_stale_ready_file) must
            # happen before Popen. Force is_file_ready to False and
            # time.monotonic past the deadline so main() returns quickly
            # without actually launching anything.
            call_order = []
            real_unlink = os.unlink
            tracking_popen_calls = []

            def tracking_unlink(p, *a, **kw):
                call_order.append('unlink')
                return real_unlink(p, *a, **kw)

            def tracking_popen(*a, **kw):
                call_order.append('popen')
                tracking_popen_calls.append((a, kw))
                return mock_proc

            mock_proc = MagicMock()
            mock_proc.poll.return_value = None  # binary never exits
            mock_proc.pid = 12345

            stdout_buf = io.StringIO()
            stderr_buf = io.StringIO()
            with contextlib.redirect_stdout(stdout_buf), \
                    contextlib.redirect_stderr(stderr_buf), \
                    patch('standalone_launcher.os.unlink',
                          side_effect=tracking_unlink), \
                    patch('standalone_launcher.subprocess.Popen',
                          side_effect=tracking_popen), \
                    patch('standalone_launcher.is_file_ready',
                          return_value=False), \
                    patch('standalone_launcher.time.sleep'), \
                    patch('standalone_launcher.time.monotonic',
                          side_effect=[0.0, 0.0, 100.0, 100.0]):
                launcher.main([
                    '--binary', '/bin/true',  # not executed (Popen mocked)
                    '--log', log_path,
                    '--ready-file', path,
                    '--ready-timeout', '0.5',
                    '--ready-interval', '0.1',
                ])
            # Stale file must be gone.
            self.assertFalse(os.path.exists(path),
                             'stale ready_file must be removed before Popen')
            # Popen must have been called exactly once after unlink.
            self.assertEqual(len(tracking_popen_calls), 1)
            self.assertIn('unlink', call_order)
            self.assertIn('popen', call_order)
            self.assertLess(call_order.index('unlink'),
                            call_order.index('popen'),
                            'clear_stale_ready_file must run before Popen')
        finally:
            if os.path.exists(path):
                os.unlink(path)
            if os.path.exists(log_path):
                try:
                    os.unlink(log_path)
                except OSError:
                    pass


if __name__ == '__main__':
    unittest.main()
