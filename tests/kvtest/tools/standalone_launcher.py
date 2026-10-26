#!/usr/bin/env python3
"""Standalone binary launcher for kubectl-exec-safe background start + stop.

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

Subcommands:

* ``start`` (default; also the behavior when invoked with no subcommand,
  for backward compatibility with callers that predate the split): launch
  the binary detached, poll for readiness, print ``{pid} {elapsed}``, and
  record the PID in ``--pidfile`` so ``stop`` can address the exact process.

* ``stop --pidfile P --binary B [--grace S] [--kill-wait S]``: graceful
  stop of the recorded process. SIGTERM first, poll for exit (``/proc``
  state, same semantics as ``dscli stop``'s ``wait_exit``), SIGKILL after
  ``--grace`` seconds, wait ``--kill-wait`` more. Keeps the whole
  signal/wait/escalate loop inside the pod so the deploy script issues a
  single ``kubectl exec`` instead of polling from outside.

Readiness polling matches ``dscli start`` (``cli/start.py``):

* If ``--worker-probe-file`` is given, poll for that file's existence until it
  appears (or ``--ready-timeout`` elapses). Used by worker standalones;
  the worker binary writes ``FLAGS_health_check_path`` only after
  ``WaitForServiceReady()`` + ``WaitForTopologyReady()`` complete, so this
  is the authoritative readiness signal.
* If ``--port`` is given (and no ``--worker-probe-file``), poll TCP connect to
  ``--host:--port`` until it succeeds. Used by coordinator standalones
  and kvtest clients (HTTP control port).
* If neither is given, poll ``proc.poll()`` for ``--no-signal-grace``
  seconds (default 2) to catch early exits (bad gflags, missing .so,
  config errors), then print the PID.

Failure detection (start):

* If the binary exits before becoming ready (or before the no-signal grace
  period elapses), the launcher prints an error to stderr and returns 1
  — no PID is printed to stdout, so the caller can distinguish success
  from failure by checking stdout.
* If the readiness deadline elapses without the binary becoming ready and
  without the binary exiting, the launcher does a final ``proc.poll()``
  check. If the binary has exited (race with the last sleep), it returns
  1. Otherwise it prints the PID + elapsed and returns 0 with a WARNING
  on stderr, so the caller can verify via ``pgrep``.

Output format:

* ``start`` success: prints ``{pid} {elapsed}`` to stdout, where
  ``elapsed`` is the actual binary startup time measured inside the
  launcher (Popen → ready signal), excluding ``kubectl exec`` /
  ``python3`` startup overhead.
* ``start`` failure: prints nothing to stdout; error details on stderr.
* ``stop`` success: prints ``stopped {elapsed}`` to stdout, where elapsed
  is the total wall-clock from SIGTERM (including the SIGKILL wait when
  escalation was needed). Failure: ``alive {elapsed}``.
* ``stop`` exit codes: 0 = process exited (including "was already gone"
  -- stopping a stopped process is idempotent); 1 = still alive after
  SIGKILL + kill-wait (caller reports FAILED); 3 = pidfile missing or
  unreadable (caller falls back to its legacy pgrep path).

PID identity: before signaling, ``stop`` validates ``/proc/{pid}/cmdline``
against ``--binary`` (basename match). A stale pidfile whose PID was
recycled by an unrelated process is treated as "binary already gone"
(idempotent success, pidfile removed) instead of signaling an innocent
process.
"""

import argparse
import os
import socket
import subprocess
import sys
import time


_POLL_INTERVAL = 0.2


def is_port_ready(host, port, timeout=0.5):
    """Return True if a TCP connect to host:port succeeds."""
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


def is_file_ready(path, cwd=None):
    """Return True if a worker probe file exists.

    ``path`` may be absolute or relative. Relative paths are resolved
    against ``cwd`` (the binary's working directory), matching the worker
    binary's own file-write semantics — the binary writes
    ``FLAGS_health_check_path`` relative to its CWD, so the launcher must
    check the same resolved path.
    """
    if not path:
        return False
    if not os.path.isabs(path) and cwd:
        path = os.path.join(cwd, path)
    return os.path.exists(path)


def clear_stale_worker_probe_file(path, cwd=None):
    """Remove a stale worker probe file before launching a new binary.

    Mirrors dscli ``start_worker``: the worker also deletes
    ``FLAGS_health_check_path`` during early initialization and publishes it
    only after startup gates pass. Launcher-side cleanup closes the interval
    between process creation and that initialization step.

    Relative paths are resolved against ``cwd`` to match ``is_file_ready``.
    Returns True when a file was removed, False when it was already absent
    or could not be removed (treated as best-effort: launch proceeds and
    the readiness poll still bounds the wait).
    """
    if not path:
        return False
    if not os.path.isabs(path) and cwd:
        path = os.path.join(cwd, path)
    try:
        os.unlink(path)
        return True
    except FileNotFoundError:
        return False
    except OSError:
        return False


def build_env(extra_lib_path=None):
    """Return a copy of os.environ with LD_LIBRARY_PATH prepended if given."""
    env = os.environ.copy()
    if extra_lib_path:
        existing = env.get('LD_LIBRARY_PATH', '')
        env['LD_LIBRARY_PATH'] = (
            f'{extra_lib_path}:{existing}' if existing else extra_lib_path)
    return env


# --- pidfile helpers -------------------------------------------------------


def read_pid(pidfile):
    """Return the PID recorded in pidfile, or None if missing/invalid."""
    try:
        with open(pidfile) as f:
            content = f.read().strip()
    except OSError:
        return None
    if not content.isdigit():
        return None
    return int(content)


def write_pid(pidfile, pid):
    """Record pid in pidfile. Best effort: a write failure only disables
    the pidfile-based stop path (caller falls back to pgrep); start itself
    must not fail because of it."""
    try:
        with open(pidfile, 'w') as f:
            f.write(f'{pid}\n')
        return True
    except OSError as e:
        print(f'standalone_launcher: WARNING: cannot write pidfile '
              f'{pidfile}: {e}', file=sys.stderr, flush=True)
        return False


def remove_pidfile(pidfile):
    """Best-effort pidfile removal."""
    try:
        os.unlink(pidfile)
        return True
    except OSError:
        return False


def parse_stat_state(content):
    """Extract the process state letter from /proc/{pid}/stat content.

    The stat format is ``pid (comm) state ...`` where comm may contain
    spaces and parentheses, so state is the token after the LAST ')'.
    """
    idx = content.rfind(')')
    if idx < 0 or idx + 2 > len(content):
        return ''
    return content[idx + 2:].split(' ', 1)[0] if content[idx + 1:] else ''


def pid_state(pid):
    """Return the process state letter, or None if /proc entry is gone."""
    try:
        with open(f'/proc/{pid}/stat') as f:
            return parse_stat_state(f.read())
    except (OSError, ValueError):
        return None


def pid_alive(pid):
    """True if the process exists and is not a zombie (Z = exited, awaiting
    reaping -- treated as exited, matching dscli's wait_exit semantics)."""
    state = pid_state(pid)
    return state is not None and state != 'Z'


def cmdline_matches(raw_cmdline, binary):
    """True if any argv entry's basename equals binary's basename.

    raw_cmdline is the NUL-separated /proc/{pid}/cmdline content.
    """
    args = [a.decode('utf-8', errors='ignore')
            for a in raw_cmdline.split(b'\x00') if a]
    if not args:
        return False
    base = os.path.basename(binary)
    return any(os.path.basename(a) == base for a in args)


def pid_matches_binary(pid, binary):
    """Validate that pid still refers to the expected binary (guards against
    a stale pidfile whose PID the kernel recycled for another process)."""
    try:
        with open(f'/proc/{pid}/cmdline', 'rb') as f:
            return cmdline_matches(f.read(), binary)
    except OSError:
        return False


def wait_exit(pid, timeout, interval=_POLL_INTERVAL):
    """Poll until the process exits. Returns True if gone within timeout."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if not pid_alive(pid):
            return True
        time.sleep(interval)
    return not pid_alive(pid)


# --- start subcommand -------------------------------------------------------


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
    parser.add_argument('--pidfile',
                        help='File to record the launched PID so the stop '
                             'subcommand can address the exact process')
    parser.add_argument('--worker-probe-file',
                        help="If set, poll for this file's existence until "
                             'ready (authoritative readiness signal, e.g. '
                             'worker health_check_path). Relative paths are '
                             'resolved against --cwd.')
    parser.add_argument('--port', type=int,
                        help='If set, poll TCP connect on host:port until ready')
    parser.add_argument('--host', default='127.0.0.1',
                        help='Host for port readiness poll (default: 127.0.0.1)')
    parser.add_argument('--ready-timeout', type=float, default=30.0,
                        help='Max seconds to wait for readiness (default: 30)')
    parser.add_argument('--ready-interval', type=float, default=0.5,
                        help='Polling interval in seconds (default: 0.5)')
    parser.add_argument('--no-signal-grace', type=float, default=2.0,
                        help='Max seconds to poll proc.poll() for early '
                             'exits when no readiness signal (--worker-probe-file '
                             'or --port) is given (default: 2). Catches '
                             'gflag/config failures before reporting '
                             'success. Ignored when --worker-probe-file or --port '
                             'is set.')
    parser.add_argument('argv', nargs='*',
                        help='Arguments for the binary (separate with --)')
    return parser.parse_args(argv)


def main_start(argv=None):
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

    # Clear any stale worker_probe_file before Popen, mirroring dscli start_worker
    # (cli/start.py:671-672). Without this, the first readiness poll could
    # hit a file left by a previous run and report ready before the new
    # binary reaches ReadinessProbe(). See clear_stale_worker_probe_file for the
    # full rationale.
    if args.worker_probe_file:
        clear_stale_worker_probe_file(args.worker_probe_file, args.cwd)

    # t_launch anchors the actual binary startup timing (Popen → ready),
    # excluding kubectl exec / python3 startup overhead that the caller's
    # outer time.monotonic() diff would include.
    t_launch = time.monotonic()
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

    # Record the PID for the stop subcommand. Written before readiness so
    # stop can address the process even if readiness is slow; removed again
    # on the early-exit failure path below.
    if args.pidfile:
        write_pid(args.pidfile, proc.pid)

    # Readiness polling priority: --worker-probe-file (authoritative, e.g. worker
    # health_check_path) > --port (TCP connect, e.g. coordinator) > none
    # (grace-poll proc.poll() to catch early exits). This mirrors dscli's
    # split: start_worker waits on health_check_path, start_coordinator waits
    # on is_tcp_ready.
    #
    # For the no-signal path, use a shorter grace deadline instead of the
    # full ready_timeout: the goal is to catch gflag/config failures (which
    # happen within the first few hundred ms), not to wait for indefinite
    # readiness. The binary is expected to stay running; if it survives the
    # grace period, print PID + elapsed and return 0.
    has_readiness_signal = bool(args.worker_probe_file) or args.port is not None
    if has_readiness_signal:
        deadline = t_launch + args.ready_timeout
    else:
        deadline = t_launch + min(args.ready_timeout, args.no_signal_grace)

    while time.monotonic() < deadline:
        rc = proc.poll()
        if rc is not None:
            elapsed = time.monotonic() - t_launch
            if args.pidfile:
                remove_pidfile(args.pidfile)
            print(f'standalone_launcher: binary exited early with code {rc} '
                  f'after {elapsed:.3f}s',
                  file=sys.stderr, flush=True)
            return 1
        if args.worker_probe_file:
            if is_file_ready(args.worker_probe_file, args.cwd):
                elapsed = time.monotonic() - t_launch
                print(f'{proc.pid} {elapsed:.3f}', flush=True)
                return 0
        elif args.port is not None:
            if is_port_ready(args.host, args.port,
                             timeout=args.ready_interval):
                elapsed = time.monotonic() - t_launch
                print(f'{proc.pid} {elapsed:.3f}', flush=True)
                return 0
        # No readiness signal: keep polling proc.poll() until the grace
        # deadline to catch early exits (bad gflags, missing .so, etc).
        time.sleep(args.ready_interval)

    # Deadline expired. Final poll: if the binary exited during the last
    # sleep interval (race window), report failure instead of printing a
    # dead PID.
    rc = proc.poll()
    if rc is not None:
        elapsed = time.monotonic() - t_launch
        if args.pidfile:
            remove_pidfile(args.pidfile)
        print(f'standalone_launcher: binary exited with code {rc} '
              f'after {elapsed:.3f}s',
              file=sys.stderr, flush=True)
        return 1

    # Binary is still running but not ready within the deadline. Print
    # PID + elapsed so the caller can pgrep/kill, with a warning on stderr.
    elapsed = time.monotonic() - t_launch
    print(f'{proc.pid} {elapsed:.3f}', flush=True)
    timeout_label = (args.ready_timeout if has_readiness_signal
                     else min(args.ready_timeout, args.no_signal_grace))
    waited_on = (f'file={args.worker_probe_file}' if args.worker_probe_file
                 else f'port={args.host}:{args.port}' if args.port is not None
                 else 'no-signal')
    print(f'standalone_launcher: WARNING: not ready within '
          f'{timeout_label}s ({waited_on}, pid={proc.pid}); '
          f'printed PID for caller verify',
          file=sys.stderr, flush=True)
    return 0


# --- stop subcommand -------------------------------------------------------


def parse_stop_args(argv=None):
    parser = argparse.ArgumentParser(
        description='Gracefully stop a process recorded by the start '
                    'subcommand (SIGTERM -> wait -> SIGKILL escalation).')
    parser.add_argument('--pidfile', required=True,
                        help='Pidfile written by the start subcommand')
    parser.add_argument('--binary', required=True,
                        help='Binary basename used to validate the recorded '
                             'PID still refers to the expected process')
    parser.add_argument('--grace', type=float, default=180.0,
                        help='Max seconds to wait after SIGTERM before '
                             'escalating to SIGKILL (default: 180)')
    parser.add_argument('--kill-wait', type=float, default=10.0,
                        help='Max seconds to wait after SIGKILL (default: 10)')
    parser.add_argument('--interval', type=float, default=_POLL_INTERVAL,
                        help='Exit-polling interval in seconds '
                             '(default: 0.2)')
    return parser.parse_args(argv)


def main_stop(argv=None):
    args = parse_stop_args(argv)

    pid = read_pid(args.pidfile)
    if pid is None:
        print('no-pidfile', file=sys.stderr, flush=True)
        return 3

    t0 = time.monotonic()

    # Stale/recycled PID: if the recorded process is gone, or exists but is
    # not our binary, the launched process is already gone -- idempotent
    # success (never signal an unrelated process).
    if not pid_alive(pid) or not pid_matches_binary(pid, args.binary):
        remove_pidfile(args.pidfile)
        print('stopped 0.00', flush=True)
        return 0

    try:
        os.kill(pid, 15)  # SIGTERM
    except ProcessLookupError:
        remove_pidfile(args.pidfile)
        print('stopped 0.00', flush=True)
        return 0

    if wait_exit(pid, args.grace, args.interval):
        remove_pidfile(args.pidfile)
        print(f'stopped {time.monotonic() - t0:.2f}', flush=True)
        return 0

    print(f'standalone_launcher: process {pid} still alive after SIGTERM '
          f'grace {args.grace}s, escalating to SIGKILL',
          file=sys.stderr, flush=True)
    try:
        os.kill(pid, 9)  # SIGKILL
    except ProcessLookupError:
        remove_pidfile(args.pidfile)
        print(f'stopped {time.monotonic() - t0:.2f}', flush=True)
        return 0

    if wait_exit(pid, args.kill_wait, args.interval):
        remove_pidfile(args.pidfile)
        print(f'stopped {time.monotonic() - t0:.2f}', flush=True)
        return 0

    print(f'alive {time.monotonic() - t0:.2f}', flush=True)
    return 1


def main(argv=None):
    """Dispatch: 'stop' -> stop subcommand, everything else -> start.

    The start path accepts an optional leading 'start' word (and tolerates
    its absence so callers that predate the subcommand split keep working).
    """
    argv = list(sys.argv[1:] if argv is None else argv)
    if argv and argv[0] == 'stop':
        return main_stop(argv[1:])
    if argv and argv[0] == 'start':
        argv = argv[1:]
    return main_start(argv)


if __name__ == '__main__':
    sys.exit(main())
