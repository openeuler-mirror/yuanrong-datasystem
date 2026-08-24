#!/usr/bin/env python3
"""Standalone binary launcher for kubectl-exec-safe background start.

Mirrors the pattern in ``cli/start.py`` (``subprocess.Popen`` with
``start_new_session=True``): launch the binary in a new session so it does
not hold the caller's stdout/stderr pipe open, poll for readiness, then
print the PID and exit. The caller (``kubectl exec``) returns immediately
because the binary is detached and the launcher parent has exited.

This avoids two problems with the previous ``nohup ./binary ... & echo $!``
shell pattern:

1. ``kubectl exec`` hangs for the full subprocess timeout because the
   background binary inherits the shell's SPDY pipe; the launcher's parent
   does not have that pipe after ``start_new_session=True`` + redirects,
   so ``kubectl exec`` returns as soon as the launcher parent exits.
2. The ``setsid`` binary is missing on some minimal images; the launcher
   uses Python's ``subprocess.Popen(start_new_session=True)`` which calls
   ``setsid(2)`` directly, no external binary required.

Readiness polling matches ``dscli start`` (``cli/start.py``):

* If ``--port`` is given, poll TCP connect to ``--host:--port`` until it
  succeeds (or ``--ready-timeout`` elapses). Used by worker / coordinator
  standalones that listen on a known port.
* If no ``--port`` is given, print the PID immediately after launch. Used
  by client-style binaries that do not listen; the caller does its own
  ``pgrep`` verify.

If the binary exits before becoming ready, the launcher exits with the
binary's exit code so the caller can detect the failure.
"""

import argparse
import os
import socket
import subprocess
import sys
import time


def is_port_ready(host, port, timeout=0.5):
    """Return True if a TCP connect to host:port succeeds."""
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


def is_file_ready(path, cwd=None):
    """Return True if a readiness file exists.

    ``path`` may be absolute or relative. Relative paths are resolved
    against ``cwd`` (the binary's working directory), matching the worker
    binary's own file-write semantics — the binary writes
    ``FLAGS_ready_check_path`` relative to its CWD, so the launcher must
    check the same resolved path.
    """
    if not path:
        return False
    if not os.path.isabs(path) and cwd:
        path = os.path.join(cwd, path)
    return os.path.exists(path)


def build_env(extra_lib_path=None):
    """Return a copy of os.environ with LD_LIBRARY_PATH prepended if given."""
    env = os.environ.copy()
    if extra_lib_path:
        existing = env.get('LD_LIBRARY_PATH', '')
        env['LD_LIBRARY_PATH'] = (
            f'{extra_lib_path}:{existing}' if existing else extra_lib_path)
    return env


def parse_args(argv=None):
    parser = argparse.ArgumentParser(
        description='Launch a binary detached from the caller session.')
    parser.add_argument('--binary', required=True,
                        help='Path to the binary to launch')
    parser.add_argument('--cwd',
                        help='Working directory for the binary')
    parser.add_argument('--log', required=True,
                        help='File to redirect binary stdout+stderr (append)')
    parser.add_argument('--lib-path',
                        help='Path to prepend to LD_LIBRARY_PATH for the binary')
    parser.add_argument('--ready-file',
                        help='If set, poll for this file\'s existence until '
                             'ready (authoritative readiness signal, e.g. '
                             'worker ready_check_path). Relative paths are '
                             'resolved against --cwd.')
    parser.add_argument('--port', type=int,
                        help='If set, poll TCP connect on host:port until ready')
    parser.add_argument('--host', default='127.0.0.1',
                        help='Host for port readiness poll (default: 127.0.0.1)')
    parser.add_argument('--ready-timeout', type=float, default=30.0,
                        help='Max seconds to wait for readiness (default: 30)')
    parser.add_argument('--ready-interval', type=float, default=0.5,
                        help='Polling interval in seconds (default: 0.5)')
    parser.add_argument('argv', nargs='*',
                        help='Arguments for the binary (separate with --)')
    return parser.parse_args(argv)


def main(argv=None):
    args = parse_args(argv)

    binary_argv = list(args.argv)
    # Defensive: strip a leading `--` if argparse left it in positional argv.
    if binary_argv and binary_argv[0] == '--':
        binary_argv = binary_argv[1:]

    env = build_env(args.lib_path)

    # Open log in parent; pass fd to child as stdout/stderr. Parent closes
    # its copy after Popen so the file is only held by the child.
    log_fd = os.open(args.log,
                     os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o644)

    try:
        proc = subprocess.Popen(
            [args.binary] + binary_argv,
            cwd=args.cwd,
            env=env,
            stdout=log_fd,
            stderr=subprocess.STDOUT,
            stdin=subprocess.DEVNULL,
            start_new_session=True,
            close_fds=True,
        )
    except OSError as e:
        os.close(log_fd)
        print(f'standalone_launcher: failed to start {args.binary}: {e}',
              file=sys.stderr, flush=True)
        return 1
    finally:
        try:
            os.close(log_fd)
        except OSError:
            pass

    # Readiness polling priority: --ready-file (authoritative, e.g. worker
    # ready_check_path) > --port (TCP connect, e.g. coordinator) > none
    # (print PID immediately, caller verifies via pgrep). This mirrors
    # dscli's split: start_worker waits on ready_check_path, start_coordinator
    # waits on is_tcp_ready.
    deadline = time.monotonic() + args.ready_timeout
    while time.monotonic() < deadline:
        rc = proc.poll()
        if rc is not None:
            print(f'standalone_launcher: binary exited early with code {rc}',
                  file=sys.stderr, flush=True)
            return rc if isinstance(rc, int) else 1
        if args.ready_file:
            if is_file_ready(args.ready_file, args.cwd):
                print(proc.pid, flush=True)
                return 0
        elif args.port is not None:
            if is_port_ready(args.host, args.port,
                             timeout=args.ready_interval):
                print(proc.pid, flush=True)
                return 0
        else:
            # No readiness signal given: print PID immediately and let the
            # caller verify (pgrep etc). Matches procmon.py --background.
            print(proc.pid, flush=True)
            return 0
        time.sleep(args.ready_interval)

    # Timeout: print PID anyway so caller can pgrep/kill, but warn on stderr.
    print(proc.pid, flush=True)
    waited_on = (f'file={args.ready_file}' if args.ready_file
                 else f'port={args.host}:{args.port}' if args.port is not None
                 else 'no-signal')
    print(f'standalone_launcher: WARNING: not ready within '
          f'{args.ready_timeout}s ({waited_on}, pid={proc.pid}); '
          f'printed PID for caller verify',
          file=sys.stderr, flush=True)
    return 0


if __name__ == '__main__':
    sys.exit(main())
