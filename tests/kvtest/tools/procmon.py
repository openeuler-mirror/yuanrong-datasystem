#!/usr/bin/env python3
"""Monitor CPU, memory, file-descriptor, and TCP connection-attempt failures of a process by name."""

import argparse
import atexit
import os
import re
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


def read_proc_mem_breakdown(pid):
    """Read resident memory breakdown (RSS, true anon, non-anon) in bytes.

    Returns (rss_bytes, anon_bytes, shared_bytes) where:
      - rss_bytes: total resident set size. All resident pages mapped by
        this process: private anon + shmem + file-backed pages.
      - anon_bytes: private anonymous resident memory, taken from the
        `Anonymous` field of /proc/<pid>/smaps_rollup. Counts only
        PageAnon pages: heap, stack, and private MAP_ANONYMOUS mmap.
        Crucially, this EXCLUDES shmem and file-backed pages even when
        this process is the only one currently mapping them. The earlier
        Private_Clean + Private_Dirty formula was mapcount-based and
        mistook single-attacher shmem (e.g. a worker that pins a shmem
        segment only it attaches) for anon; the `Anonymous` field is
        backed by page->mapping and does not have that blind spot.
      - shared_bytes: derived as Rss - Anonymous. Includes shmem (tmpfs,
        MAP_ANONYMOUS|MAP_SHARED, shm_open) and file-backed pages (shared
        library text, file mmap) regardless of current mapcount. A worker
        that pins a shmem segment only it attaches will see that segment
        fully accounted under shared_bytes, not anon_bytes.

    anon + shared equals RSS exactly except for hugetlb pages (see the
    Shared_Hugetlb / Private_Hugetlb fields in smaps_rollup), which are
    counted in Rss but excluded from `Anonymous`. Most KV-test workloads
    do not use hugepages, so the sum holds; if hugetlb is in play the gap
    appears as Rss > Anon + Shared and the missing bytes are in
    Shared_Hugetlb + Private_Hugetlb.

    Primary source is /proc/<pid>/smaps_rollup. Falls back to
    /proc/<pid>/statm when smaps_rollup is unavailable (restricted
    container, pre-4.4 kernels): statm's `shared` field counts shmem +
    file-backed pages, so anon = Rss - shared matches the new
    smaps_rollup semantics. The fallback is coarser-grained but the
    anon/shared split stays usable everywhere.

    Returns (None, None, None) if neither source can be read.
    """
    # Primary: smaps_rollup (values in kB).
    try:
        stats = {}
        with open(f"/proc/{pid}/smaps_rollup") as f:
            for line in f:
                parts = line.split()
                if len(parts) >= 2 and parts[0].endswith(":"):
                    try:
                        stats[parts[0][:-1]] = int(parts[1])
                    except ValueError:
                        continue
        if "Rss" in stats:
            rss_kb = stats["Rss"]
            # `Anonymous` excludes single-attacher shmem; see docstring.
            if "Anonymous" in stats:
                anon_kb = stats["Anonymous"]
                shared_kb = max(0, rss_kb - anon_kb)
            else:
                # Pre-4.4 kernel: no Anonymous field, fall back to the
                # mapcount-based split (less accurate for shmem).
                anon_kb = stats.get("Private_Clean", 0) + stats.get("Private_Dirty", 0)
                shared_kb = stats.get("Shared_Clean", 0) + stats.get("Shared_Dirty", 0)
            return rss_kb * 1024, anon_kb * 1024, shared_kb * 1024
    except (FileNotFoundError, PermissionError, OSError):
        pass

    # Fallback: statm (values in pages).
    try:
        with open(f"/proc/{pid}/statm") as f:
            fields = f.read().split()
        page_size = os.sysconf("SC_PAGE_SIZE")
        rss_bytes = int(fields[1]) * page_size
        shared_bytes = int(fields[2]) * page_size
        anon_bytes = rss_bytes - shared_bytes
        return rss_bytes, anon_bytes, shared_bytes
    except (FileNotFoundError, ValueError, IndexError, PermissionError, OSError):
        return None, None, None


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


def read_tcp_attempt_fails_stats():
    """Read cumulative TCP AttemptFails and ActiveOpens from /proc/net/snmp.

    Returns (attempt_fails, active_opens) or (None, None) on read/parse
    failure.

    - AttemptFails: number of failed active TCP connection attempts (SYN
      sent but never reached ESTABLISHED - peer RST in response to SYN,
      peer unreachable, connection timeout, etc.). Sustained growth
      means the process is repeatedly trying to talk to nodes that do
      not exist, are not listening, or are unreachable - exactly the
      signal we want for catching stale membership / misconfigured
      targets.
    - ActiveOpens: total active TCP connections initiated in the same
      interval, used as denominator for fail rate
      (AttemptFails / ActiveOpens).

    /proc/net/snmp is network-namespace scoped: inside a container it
    reports that container's TCP totals rather than a single process's.
    When the monitored process is the dominant TCP user in the container
    (typical for KV test workloads), container-level stats are an
    effective per-process proxy. Counters are cumulative since the
    namespace's boot, so callers must diff successive samples to get a
    rate.
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
        idx_fails = header.index("AttemptFails")
        idx_opens = header.index("ActiveOpens")
        return int(vals[idx_fails]), int(vals[idx_opens])
    except (FileNotFoundError, ValueError, IndexError):
        return None, None


def read_port_traffic(port, timeout=5):
    """Read cumulative bytes_sent + bytes_received across all TCP sockets
    on the listening ``port`` via ``ss -tin``.

    Returns ``(total_bytes_sent, total_bytes_received)`` — the SUM across
    all matching sockets. ESTABLISHED sockets carry ``bytes_sent`` /
    ``bytes_received`` counters (kernel >= 4.4, iproute2 >= 4.4); the
    LISTEN socket does not, so it is naturally excluded. Counters are
    cumulative since each socket's creation; callers diff successive
    samples to get a rate:

      BytesOut/s = delta(total_bytes_sent) / dt   (server-sent = outbound)
      BytesIn/s  = delta(total_bytes_received) / dt (server-recv = inbound)

    Sockets that close between samples drop their byte counters from the
    aggregate, so the diff undercounts short-lived connections; for
    long-lived coordinator<->worker connections this is accurate.

    Returns ``(None, None)`` when ``ss`` is missing, the kernel/iproute2
    is too old to emit byte counters, the port filter matches zero
    sockets, or the subprocess times out — callers treat None as "no
    traffic data this sample" and skip the rate computation.
    """
    try:
        result = subprocess.run(
            ['ss', '-tin', f'sport = :{port}'],
            capture_output=True, text=True, timeout=timeout)
    except (FileNotFoundError, subprocess.TimeoutExpired):
        return None, None
    if result.returncode != 0:
        return None, None
    out = result.stdout or ''
    # Real `ss -ti` output uses `key:value` colon-separated fields on the
    # indented TCP info lines (e.g. ``bytes_sent:16703 bytes_received:1449``).
    # Older/some iproute2 builds may use whitespace instead; ``[:\s]+`` matches
    # both so the parser does not silently no-op on the dominant colon format.
    sent_values = re.findall(r'bytes_sent[:\s]+(\d+)', out)
    recv_values = re.findall(r'bytes_received[:\s]+(\d+)', out)
    if not sent_values and not recv_values:
        return None, None
    return (sum(int(v) for v in sent_values),
            sum(int(v) for v in recv_values))


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
    parser = argparse.ArgumentParser(description="Monitor process CPU, memory, file-descriptor, and TCP connection-attempt failures")
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
    parser.add_argument("--port", type=int, default=None,
                        help="Monitor inbound/outbound byte throughput on this TCP "
                             "listening port via `ss -tin` (aggregates bytes_sent / "
                             "bytes_received across all ESTABLISHED sockets on the "
                             "port). When omitted, no traffic monitoring is done.")
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
         + (f", duration={args.duration}s" if args.duration > 0 else "")
         + (f", port={args.port}" if args.port else ""))

    samples_cpu = []
    samples_mem = []
    samples_mem_anon = []
    samples_mem_shared = []
    samples_fd = []
    samples_fails = []
    samples_bytes_in = []
    samples_bytes_out = []
    total_fails = 0
    total_bytes_in = 0
    total_bytes_out = 0
    prev_cpu = read_proc_stat(pid)
    prev_time = time.monotonic()
    prev_fails, prev_opens = read_tcp_attempt_fails_stats()
    prev_sent, prev_recv = (read_port_traffic(args.port)
                            if args.port else (None, None))
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
        rss_bytes, anon_bytes, shared_bytes = read_proc_mem_breakdown(pid)
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
        anon_mb = anon_bytes / (1024 * 1024)
        shared_mb = shared_bytes / (1024 * 1024)

        samples_cpu.append(cpu_pct)
        samples_mem.append(mem_mb)
        samples_mem_anon.append(anon_mb)
        samples_mem_shared.append(shared_mb)
        if fd_count is not None:
            samples_fd.append(fd_count)

        fails, opens = read_tcp_attempt_fails_stats()
        tcp_str = ""
        if fails is not None and opens is not None:
            if prev_fails is not None and prev_opens is not None:
                delta_fails = max(0, fails - prev_fails)
                delta_opens = max(0, opens - prev_opens)
            else:
                delta_fails = 0
                delta_opens = 0
            fails_per_sec = delta_fails / dt if dt > 0 else 0.0
            fail_rate = (delta_fails / delta_opens * 100) if delta_opens > 0 else 0.0
            samples_fails.append(fails_per_sec)
            total_fails += delta_fails
            tcp_str = (f" Fails/s={fails_per_sec:.2f}"
                       f" FailRate={fail_rate:.2f}%")
            prev_fails = fails
            prev_opens = opens

        traffic_str = ""
        if args.port:
            sent, recv = read_port_traffic(args.port)
            if sent is not None and recv is not None:
                if prev_sent is not None and prev_recv is not None:
                    delta_sent = max(0, sent - prev_sent)
                    delta_recv = max(0, recv - prev_recv)
                else:
                    delta_sent = 0
                    delta_recv = 0
                bytes_out_per_sec = delta_sent / dt if dt > 0 else 0.0
                bytes_in_per_sec = delta_recv / dt if dt > 0 else 0.0
                samples_bytes_in.append(bytes_in_per_sec)
                samples_bytes_out.append(bytes_out_per_sec)
                total_bytes_in += delta_recv
                total_bytes_out += delta_sent
                traffic_str = (f" BytesIn/s={format_mb(bytes_in_per_sec)}MB"
                               f" BytesOut/s={format_mb(bytes_out_per_sec)}MB")
                prev_sent = sent
                prev_recv = recv

        ts = time.strftime("%Y-%m-%dT%H:%M:%S")
        fd_str = f" FD={fd_count}" if fd_count is not None else ""
        emit(f"[{ts}] PID={pid} CPU={cpu_pct:.1f}% MEM={format_mb(rss_bytes)}MB"
             f" Anon={format_mb(anon_bytes)}MB Shared={format_mb(shared_bytes)}MB"
             f"{fd_str}{tcp_str}{traffic_str}")

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
        emit(f"MEM  RSS     avg={avg_mem:.1f}MB peak={peak_mem:.1f}MB")
        avg_anon = sum(samples_mem_anon) / len(samples_mem_anon)
        peak_anon = max(samples_mem_anon)
        emit(f"MEM  Anon    avg={avg_anon:.1f}MB peak={peak_anon:.1f}MB")
        avg_shared = sum(samples_mem_shared) / len(samples_mem_shared)
        peak_shared = max(samples_mem_shared)
        emit(f"MEM  Shared  avg={avg_shared:.1f}MB peak={peak_shared:.1f}MB")
    if samples_fd:
        avg_fd = sum(samples_fd) / len(samples_fd)
        peak_fd = max(samples_fd)
        emit(f"FD   avg={avg_fd:.0f} peak={peak_fd}")
    if samples_fails:
        avg_fails = sum(samples_fails) / len(samples_fails)
        peak_fails = max(samples_fails)
        emit(f"TCP  fails total={total_fails} avg={avg_fails:.2f}/s peak={peak_fails:.2f}/s")
    if samples_bytes_in:
        avg_in = sum(samples_bytes_in) / len(samples_bytes_in)
        peak_in = max(samples_bytes_in)
        emit(f"NET  In  total={format_mb(total_bytes_in)}MB avg={format_mb(avg_in)}MB/s peak={format_mb(peak_in)}MB/s")
    if samples_bytes_out:
        avg_out = sum(samples_bytes_out) / len(samples_bytes_out)
        peak_out = max(samples_bytes_out)
        emit(f"NET  Out total={format_mb(total_bytes_out)}MB avg={format_mb(avg_out)}MB/s peak={format_mb(peak_out)}MB/s")


if __name__ == "__main__":
    main()
