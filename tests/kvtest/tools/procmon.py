#!/usr/bin/env python3
"""Monitor CPU, memory, file-descriptor, and TCP retransmission of a process by name."""

import argparse
import atexit
import os
import signal
import subprocess
import sys
import time


def find_pid(name):
    """Find PID by full command line (first match). Uses pgrep -f to match
    process names longer than Linux's 15-char comm field limit."""
    try:
        out = subprocess.check_output(
            ["pgrep", "-f", name], text=True).strip()
        if not out:
            return None
        return int(out.splitlines()[0])
    except (subprocess.CalledProcessError, ValueError):
        return None


def read_proc_stat(pid):
    """Read CPU times (utime + stime) in ticks from /proc/<pid>/stat."""
    try:
        with open(f"/proc/{pid}/stat") as f:
            fields = f.read().split()
        # Fields: pid comm state utime(14) stime(15) in clock ticks
        utime = int(fields[13])
        stime = int(fields[14])
        return utime + stime
    except (FileNotFoundError, ValueError, IndexError):
        return None


def read_proc_statm(pid):
    """Read RSS in pages from /proc/<pid>/statm."""
    try:
        with open(f"/proc/{pid}/statm") as f:
            fields = f.read().split()
        rss_pages = int(fields[1])
        page_size = os.sysconf("SC_PAGE_SIZE")
        return rss_pages * page_size
    except (FileNotFoundError, ValueError, IndexError):
        return None


def read_proc_fd_count(pid):
    """Count open file descriptors from /proc/<pid>/fd.

    Each directory entry is a symlink named after an FD number, so the entry
    count equals the open-FD count. Returns None when the directory cannot be
    read (process exited, or caller lacks permission; common when monitoring
    a process owned by another user); callers should treat None as 'FD
    unavailable for this sample' rather than 0.
    """
    try:
        return len(os.listdir(f"/proc/{pid}/fd"))
    except (FileNotFoundError, PermissionError, OSError):
        return None


def read_tcp_retrans_stats():
    """Read cumulative TCP retransmit and OutSegs counters from /proc/net/snmp.

    Returns (retrans, outsegs) or (None, None) on read/parse failure.
    /proc/net/snmp is network-namespace scoped: inside a container it reports
    that container's TCP totals rather than a single process's. When the
    monitored process is the dominant TCP user in the container (typical for
    KV test workloads), container-level stats are an effective per-process
    proxy. Counters are cumulative since the namespace's boot, so callers must
    diff successive samples to get a rate.
    """
    try:
        header = None
        vals = None
        with open("/proc/net/snmp") as f:
            for line in f:
                if not line.startswith("Tcp:"):
                    continue
                fields = line.split()
                if header is None:
                    header = fields
                else:
                    vals = fields
                    break
        if header is None or vals is None:
            return None, None
        idx_retrans = header.index("RetransSegs")
        idx_outsegs = header.index("OutSegs")
        return int(vals[idx_retrans]), int(vals[idx_outsegs])
    except (FileNotFoundError, ValueError, IndexError):
        return None, None


def format_mb(bytes_val):
    return f"{bytes_val / (1024 * 1024):.1f}"


def _daemonize():
    """Fork into background. Parent prints child PID to stdout and exits.

    Child creates a new session (os.setsid) and redirects stdin/stdout/stderr
    to /dev/null so it does not hold the caller's pipe open. This lets
    kubectl exec / ssh return immediately instead of hanging until timeout.

    Use --output to capture monitoring data to a file. To debug procmon
    startup errors, run without --background so stderr stays visible.
    """
    devnull = os.open('/dev/null', os.O_RDWR)
    os.dup2(devnull, 0)

    try:
        pid = os.fork()
    except OSError as e:
        # fork may fail with EAGAIN (RLIMIT_NPROC) or ENOMEM. Do not continue:
        # stdout is still the caller's pipe, so falling through would mix
        # monitoring output into the caller's stream.
        os.close(devnull)
        print(f'procmon: fork failed: {e}', file=sys.stderr, flush=True)
        sys.exit(1)

    if pid > 0:
        print(pid, flush=True)
        os._exit(0)

    os.setsid()
    os.dup2(devnull, 1)
    os.dup2(devnull, 2)
    os.close(devnull)


def main():
    parser = argparse.ArgumentParser(description="Monitor process CPU, memory, file-descriptor, and TCP retransmission")
    parser.add_argument("-p", "--process", help="Process name to find and monitor")
    parser.add_argument("--pid", type=int, help="Monitor specific PID directly")
    parser.add_argument("-i", "--interval", type=float, default=1, help="Sample interval in seconds (default: 1)")
    parser.add_argument("-d", "--duration", type=float, default=0, help="Monitor duration in seconds, 0=until exit/Ctrl+C (default: 0)")
    parser.add_argument("-o", "--output", help="Write output to this file in real-time (default: stdout)")
    parser.add_argument("--background", action="store_true",
                        help="Daemonize: fork into background, print child PID to "
                             "stdout, parent exits. Requires --output. Lets the "
                             "caller (kubectl exec, ssh) return immediately instead "
                             "of waiting for timeout.")
    args = parser.parse_args()

    if not args.process and not args.pid:
        parser.error("Either --process or --pid is required")

    if args.background and not args.output:
        parser.error("--output is required when --background is used")

    if args.background:
        _daemonize()

    clock_ticks = os.sysconf("SC_CLK_TCK")

    if args.output:
        script_dir = os.path.dirname(os.path.abspath(__file__))
        if not os.path.isabs(args.output):
            output_path = os.path.join(script_dir, args.output)
        else:
            output_path = args.output
        outfile = open(output_path, "a", buffering=1)
        atexit.register(outfile.close)
        out = outfile
    else:
        out = sys.stdout

    def emit(msg=""):
        print(msg, file=out, flush=True)

    if args.pid:
        pid = args.pid
    else:
        pid = find_pid(args.process)
        if pid is None:
            print(f"Process '{args.process}' not found", file=sys.stderr)
            sys.exit(1)

    emit(f"Monitoring PID={pid}, interval={args.interval}s"
         + (f", duration={args.duration}s" if args.duration > 0 else ""))

    samples_cpu = []
    samples_mem = []
    samples_fd = []
    samples_retrans = []
    total_retrans = 0
    prev_cpu = read_proc_stat(pid)
    prev_time = time.monotonic()
    prev_retrans, prev_outsegs = read_tcp_retrans_stats()
    start_time = prev_time

    running = True

    def on_signal(_, __):
        nonlocal running
        running = False

    signal.signal(signal.SIGINT, on_signal)
    signal.signal(signal.SIGTERM, on_signal)

    while running:
        time.sleep(args.interval)

        if 0 < args.duration <= (time.monotonic() - start_time):
            break

        now = time.monotonic()
        cpu_ticks = read_proc_stat(pid)
        rss_bytes = read_proc_statm(pid)
        fd_count = read_proc_fd_count(pid)

        if cpu_ticks is None or rss_bytes is None:
            emit(f"[{time.strftime('%Y-%m-%dT%H:%M:%S')}] Process exited")
            break

        dt = now - prev_time
        if dt > 0 and prev_cpu is not None:
            cpu_pct = (cpu_ticks - prev_cpu) / clock_ticks / dt * 100
        else:
            cpu_pct = 0.0

        mem_mb = rss_bytes / (1024 * 1024)

        samples_cpu.append(cpu_pct)
        samples_mem.append(mem_mb)
        if fd_count is not None:
            samples_fd.append(fd_count)

        retrans, outsegs = read_tcp_retrans_stats()
        tcp_str = ""
        if retrans is not None and outsegs is not None:
            if prev_retrans is not None and prev_outsegs is not None:
                delta_retrans = max(0, retrans - prev_retrans)
                delta_outsegs = max(0, outsegs - prev_outsegs)
            else:
                delta_retrans = 0
                delta_outsegs = 0
            retrans_per_sec = delta_retrans / dt if dt > 0 else 0.0
            retrans_rate = (delta_retrans / delta_outsegs * 100) if delta_outsegs > 0 else 0.0
            samples_retrans.append(retrans_per_sec)
            total_retrans += delta_retrans
            tcp_str = (f" Retrans/s={retrans_per_sec:.2f}"
                       f" Rate={retrans_rate:.2f}%")
            prev_retrans = retrans
            prev_outsegs = outsegs

        ts = time.strftime("%Y-%m-%dT%H:%M:%S")
        fd_str = f" FD={fd_count}" if fd_count is not None else ""
        emit(f"[{ts}] PID={pid} CPU={cpu_pct:.1f}% MEM={format_mb(rss_bytes)}MB{fd_str}{tcp_str}")

        prev_cpu = cpu_ticks
        prev_time = now

    elapsed = time.monotonic() - start_time
    emit()
    emit("=== Summary ===")
    emit(f"Samples: {len(samples_cpu)}, Duration: {elapsed:.0f}s")
    if samples_cpu:
        emit(f"CPU  avg={sum(samples_cpu)/len(samples_cpu):.1f}%  peak={max(samples_cpu):.1f}%")
        avg_mem = sum(samples_mem) / len(samples_mem)
        peak_mem = max(samples_mem)
        emit(f"MEM  avg={avg_mem:.1f}MB peak={peak_mem:.1f}MB")
    if samples_fd:
        avg_fd = sum(samples_fd) / len(samples_fd)
        peak_fd = max(samples_fd)
        emit(f"FD   avg={avg_fd:.0f} peak={peak_fd}")
    if samples_retrans:
        avg_retrans = sum(samples_retrans) / len(samples_retrans)
        peak_retrans = max(samples_retrans)
        emit(f"TCP  retrans total={total_retrans} avg={avg_retrans:.2f}/s peak={peak_retrans:.2f}/s")


if __name__ == "__main__":
    main()
