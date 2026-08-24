#!/usr/bin/env python3
"""Tests for src/mock_jf_server.py --background daemonization.

Verifies the procmon.py-style daemonize pattern used by mock_jf_server:

1. Parent binds the port BEFORE forking (so bind failure is visible to
   ``kubectl exec`` — non-zero exit + stderr, no PID printed).
2. Parent prints the child PID to stdout and exits (``os._exit(0)``) so
   ``kubectl exec`` returns immediately instead of hanging on the SPDY
   pipe held by a ``nohup``-backgrounded process.
3. The printed PID implies port-ready (bind happened in the parent before
   fork; the caller does not need a separate port-ready poll).
4. Child runs in a new session (``os.setsid``) and serves /health.

These tests spawn real subprocesses and open real sockets; they are fast
(< 1s each) but require ``os.fork``, so they are skipped on platforms
without fork (e.g., Windows).
"""

import os
import signal
import socket
import subprocess
import sys
import time
import unittest
import urllib.request


SCRIPT_PATH = os.path.join(os.path.dirname(__file__), '..', '..',
                            'src', 'mock_jf_server.py')

_HAS_FORK = hasattr(os, 'fork')


def _find_free_port():
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s.bind(('127.0.0.1', 0))
    port = s.getsockname()[1]
    s.close()
    return port


def _is_port_ready(host, port, timeout=2.0):
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


def _health_ok(host, port, timeout=2.0):
    try:
        with urllib.request.urlopen(
            f'http://{host}:{port}/health', timeout=timeout) as r:
            return r.status == 200
    except Exception:
        return False


def _is_process_alive(pid):
    try:
        os.kill(pid, 0)
        return True
    except ProcessLookupError:
        return False
    except PermissionError:
        return True


def _kill(pid):
    try:
        os.kill(pid, signal.SIGTERM)
    except ProcessLookupError:
        return
    for _ in range(20):
        try:
            os.kill(pid, 0)
        except ProcessLookupError:
            return
        time.sleep(0.1)
    try:
        os.kill(pid, signal.SIGKILL)
    except ProcessLookupError:
        pass


def _read_log(path):
    try:
        with open(path) as f:
            return f.read()
    except OSError:
        return '(log not readable)'


@unittest.skipUnless(_HAS_FORK, 'os.fork required (Unix only)')
class TestDaemonizeBackground(unittest.TestCase):
    """End-to-end: --background starts the server, prints PID, port ready."""

    def test_background_starts_and_serves_health(self):
        port = _find_free_port()
        log_path = f'/tmp/test_jf_mock_{port}.log'

        proc = subprocess.run(
            [sys.executable, SCRIPT_PATH,
             '--port', str(port),
             '--ttl-default', '5',
             '--background', '--log', log_path],
            capture_output=True, text=True, timeout=10)

        # Parent must exit 0 (os._exit(0) in _daemonize).
        self.assertEqual(proc.returncode, 0,
                         f'stderr: {proc.stderr}\nlog: '
                         f'{_read_log(log_path)}')

        # stdout must contain the child PID (printed by parent before exit).
        stdout = proc.stdout.strip()
        self.assertTrue(stdout, f'parent printed nothing; log: '
                                 f'{_read_log(log_path)}')
        pid_line = stdout.splitlines()[-1].strip()
        self.assertTrue(pid_line.isdigit(),
                         f'expected PID on stdout, got: {stdout!r}')
        pid = int(pid_line)

        try:
            # Port must be listening (bind happened in parent before fork).
            self.assertTrue(_is_port_ready('127.0.0.1', port),
                            f'port {port} not ready after parent exit; '
                            f'log: {_read_log(log_path)}')

            # /health endpoint must respond (child is serving).
            self.assertTrue(_health_ok('127.0.0.1', port),
                            f'/health not responding; log: '
                            f'{_read_log(log_path)}')

            # Child process must be alive.
            self.assertTrue(_is_process_alive(pid),
                            f'child PID {pid} not alive; log: '
                            f'{_read_log(log_path)}')
        finally:
            _kill(pid)
            if os.path.exists(log_path):
                os.unlink(log_path)


@unittest.skipUnless(_HAS_FORK, 'os.fork required (Unix only)')
class TestDaemonizePortBindFailure(unittest.TestCase):
    """When the port is already in use, the script must exit non-zero
    BEFORE forking (bind failure is visible to kubectl exec, not hidden
    inside the daemonized child)."""

    def test_bind_failure_exits_nonzero_before_fork(self):
        listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        listener.bind(('127.0.0.1', 0))
        listener.listen(1)
        port = listener.getsockname()[1]

        try:
            proc = subprocess.run(
                [sys.executable, SCRIPT_PATH,
                 '--port', str(port),
                 '--background', '--log', '/dev/null'],
                capture_output=True, text=True, timeout=10)
        finally:
            listener.close()

        # Must exit non-zero (bind failure).
        self.assertNotEqual(proc.returncode, 0,
                            f'expected non-zero rc for bind failure; '
                            f'stdout: {proc.stdout!r}')
        # Must NOT have printed a PID (fork never happened).
        stdout = proc.stdout.strip()
        self.assertFalse(stdout and stdout.isdigit(),
                         f'expected no PID on stdout for bind failure, '
                         f'got: {stdout!r}')
        # Must have printed an error to stderr.
        self.assertTrue(proc.stderr.strip(),
                        'expected error message on stderr for bind failure')


if __name__ == '__main__':
    unittest.main()
