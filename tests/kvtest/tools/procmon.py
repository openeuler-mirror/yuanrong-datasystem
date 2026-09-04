#!/usr/bin/env python3
"""Monitor CPU, memory, file-descriptor, and TCP connection-attempt failures of a process by name."""

import argparse
import atexit
import csv
import os
import signal
import socket
import struct
import subprocess
import sys
import time
from urllib.request import urlopen


JEMALLOC_BVAR_COLUMNS = {
    'anon_jemalloc_allocated_bytes': 'jemalloc_allocated_mb',
    'anon_jemalloc_active_bytes': 'jemalloc_active_mb',
    'anon_jemalloc_resident_bytes': 'jemalloc_resident_mb',
    'anon_jemalloc_metadata_bytes': 'jemalloc_metadata_mb',
    'anon_jemalloc_mapped_bytes': 'jemalloc_mapped_mb',
    'anon_jemalloc_retained_bytes': 'jemalloc_retained_mb',
    'anon_jemalloc_dirty_bytes': 'jemalloc_dirty_mb',
    'anon_jemalloc_muzzy_bytes': 'jemalloc_muzzy_mb',
}

CSV_FIELDS = [
    'timestamp', 'pid', 'cpu_pct', 'rss_mb', 'anon_mb', 'shared_mb', 'fd',
    'tcp_fails_per_sec', 'tcp_fail_rate_pct', 'bytes_in_mb_per_sec',
    'bytes_out_mb_per_sec',
] + list(JEMALLOC_BVAR_COLUMNS.values()) + [
    'jemalloc_stats_available', 'jemalloc_stats_read_failures',
]


_NETLINK_INET_DIAG = 4
_SOCK_DIAG_BY_FAMILY = 20
_NLMSG_DONE = 3
_NLMSG_ERROR = 2
_NLM_F_REQUEST = 0x01
_NLM_F_DUMP = 0x100 | 0x200
_INET_DIAG_INFO = 2
_INET_DIAG_INFO_BIT = 1 << (_INET_DIAG_INFO - 1)
_TCP_ESTABLISHED = 1
_NLMSG_HDR_SIZE = 16
_INET_DIAG_MSG_SIZE = 72
_RTA_HDR_SIZE = 4
_TCPI_BYTES_RECEIVED_OFF = 128
_TCPI_BYTES_SENT_OFF = 200
_AF_NETLINK = 16
_AF_INET = 2
_AF_INET6 = 10
_SOCK_RAW = 3
_IPPROTO_TCP = 6
_ENDIAN = '<' if sys.byteorder == 'little' else '>'


def _align4(n):
    return (n + 3) & ~3


def _parse_netlink_diag_response(data, port):
    """Parse a NETLINK_INET_DIAG SOCK_DIAG_BY_FAMILY dump response.

    Returns ``(total_bytes_sent, total_bytes_received, found_any)`` where
    ``found_any`` is True when at least one ESTABLISHED socket on ``port``
    had parseable byte counters. ESTABLISHED sockets carry
    ``tcpi_bytes_received`` (kernel >= 4.4, offset 128) and
    ``tcpi_bytes_sent`` (kernel >= 4.8, offset 200) inside the
    INET_DIAG_INFO rtattr's tcp_info payload. LISTEN sockets have no
    byte counters and are naturally excluded by the state check plus the
    absence of those offsets in a too-short tcp_info.

    On a kernel older than 4.4 the tcp_info payload is < 136 bytes, so
    neither offset is readable — returns found_any=False, and callers
    treat that as (None, None).
    """
    total_sent = 0
    total_recv = 0
    found_any = False
    offset = 0
    while offset + _NLMSG_HDR_SIZE <= len(data):
        nlmsg_len = struct.unpack_from(_ENDIAN + 'I', data, offset)[0]
        if nlmsg_len < _NLMSG_HDR_SIZE or offset + nlmsg_len > len(data):
            break
        nlmsg_type = struct.unpack_from(_ENDIAN + 'H', data, offset + 4)[0]
        if nlmsg_type == _NLMSG_DONE:
            break
        if nlmsg_type == _NLMSG_ERROR:
            break
        if nlmsg_type == _SOCK_DIAG_BY_FAMILY:
            msg_off = offset + _NLMSG_HDR_SIZE
            if msg_off + _INET_DIAG_MSG_SIZE > len(data):
                break
            state = data[msg_off + 1]
            if state != _TCP_ESTABLISHED:
                offset += _align4(nlmsg_len)
                continue
            rta_off = msg_off + _INET_DIAG_MSG_SIZE
            msg_end = offset + nlmsg_len
            while rta_off + _RTA_HDR_SIZE <= msg_end:
                rta_len = struct.unpack_from(_ENDIAN + 'H', data, rta_off)[0]
                rta_type = struct.unpack_from(_ENDIAN + 'H', data, rta_off + 2)[0]
                if rta_len < _RTA_HDR_SIZE:
                    break
                if rta_type == _INET_DIAG_INFO:
                    info_start = rta_off + _RTA_HDR_SIZE
                    info_end = rta_off + rta_len
                    tcp_info = data[info_start:info_end]
                    if len(tcp_info) >= _TCPI_BYTES_RECEIVED_OFF + 8:
                        total_recv += struct.unpack_from(
                            _ENDIAN + 'Q', tcp_info, _TCPI_BYTES_RECEIVED_OFF)[0]
                        found_any = True
                    if len(tcp_info) >= _TCPI_BYTES_SENT_OFF + 8:
                        total_sent += struct.unpack_from(
                            _ENDIAN + 'Q', tcp_info, _TCPI_BYTES_SENT_OFF)[0]
                rta_off += _align4(rta_len)
        offset += _align4(nlmsg_len)
    return total_sent, total_recv, found_any


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
    """Read cumulative bytes_sent + bytes_received across all ESTABLISHED
    TCP sockets on the listening ``port`` via NETLINK_INET_DIAG.

    Queries the kernel's sock_diag interface directly — the same data
    source ``ss -ti`` uses — so the byte counters are identical. No
    external binary required; works with Python 3 alone, which matters
    for slim containers that lack iproute2.

    Returns ``(total_bytes_sent, total_bytes_received)`` — the SUM across
    all matching sockets. ESTABLISHED sockets carry ``bytes_sent`` /
    ``bytes_received`` counters (kernel >= 4.4 for bytes_received,
    >= 4.8 for bytes_sent); the LISTEN socket does not, so it is
    naturally excluded. Counters are cumulative since each socket's
    creation; callers diff successive samples to get a rate:

      BytesOut/s = delta(total_bytes_sent) / dt   (server-sent = outbound)
      BytesIn/s  = delta(total_bytes_received) / dt (server-recv = inbound)

    Sockets that close between samples drop their byte counters from the
    aggregate, so the diff undercounts short-lived connections; for
    long-lived coordinator<->worker connections this is accurate.

    Returns ``(None, None)`` when the netlink socket cannot be opened
    (non-Linux, insufficient privileges), the kernel is too old to emit
    byte counters, no ESTABLISHED sockets match the port filter, or the
    query times out — callers treat None as "no traffic data this
    sample" and skip the rate computation.

    A single request queries both AF_INET and AF_INET6 to cover
    dual-stack listeners (a server bound to ``::`` accepts IPv4
    connections via v4-mapped sockets, which appear under AF_INET6).
    """
    total_sent = 0
    total_recv = 0
    found_any = False

    for family in (_AF_INET, _AF_INET6):
        try:
            sock = socket.socket(
                _AF_NETLINK, _SOCK_RAW, _NETLINK_INET_DIAG)
        except (OSError, ValueError):
            continue
        try:
            sock.settimeout(timeout)
            req = bytearray(_NLMSG_HDR_SIZE + 56)
            struct.pack_into(
                _ENDIAN + 'IHHII', req, 0,
                len(req), _SOCK_DIAG_BY_FAMILY,
                _NLM_F_REQUEST | _NLM_F_DUMP, 1, 0)
            struct.pack_into(
                _ENDIAN + 'BBBBI', req, _NLMSG_HDR_SIZE,
                family, _IPPROTO_TCP, _INET_DIAG_INFO_BIT, 0, 0xFFF)
            struct.pack_into('>H', req, _NLMSG_HDR_SIZE + 8, port)
            sock.sendall(bytes(req))
            chunks = []
            while True:
                try:
                    chunk = sock.recv(8192)
                except socket.timeout:
                    break
                if not chunk:
                    break
                chunks.append(chunk)
                sock.settimeout(0.2)
        except OSError:
            continue
        finally:
            sock.close()
        data = b''.join(chunks) if chunks else b''
        sent, recv, found = _parse_netlink_diag_response(data, port)
        total_sent += sent
        total_recv += recv
        found_any = found_any or found

    if not found_any:
        return None, None
    return total_sent, total_recv


def read_jemalloc_bvars(port, host='127.0.0.1', timeout=2):
    """Read jemalloc metrics from BRPC builtin services, if available."""
    url_host = f'[{host}]' if ':' in host and not host.startswith('[') else host
    # BRPC renders HTML for Python's User-Agent; console=1 forces plain text.
    url = f'http://{url_host}:{port}/vars/anon_jemalloc_*?console=1'
    try:
        with urlopen(url, timeout=timeout) as response:
            body = response.read().decode('utf-8', errors='replace')
    except (OSError, TimeoutError, ValueError):
        return {}

    values = {}
    for line in body.splitlines():
        name, separator, raw_value = line.partition(':')
        if not separator:
            continue
        name = name.strip()
        if not name.startswith('anon_jemalloc_'):
            continue
        try:
            values[name] = float(raw_value.strip())
        except ValueError:
            continue
    return values


def add_jemalloc_metrics(row, bvars):
    available = bvars.get('anon_jemalloc_stats_available')
    failures = bvars.get('anon_jemalloc_stats_read_failures')
    if available is not None:
        row['jemalloc_stats_available'] = int(available)
    if failures is not None:
        row['jemalloc_stats_read_failures'] = int(failures)
    if available != 1:
        return
    for bvar_name, column_name in JEMALLOC_BVAR_COLUMNS.items():
        value = bvars.get(bvar_name)
        if value is not None:
            row[column_name] = f'{value / (1024 * 1024):.3f}'


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
                             "listening port via NETLINK_INET_DIAG (queries kernel "
                             "tcp_info directly, no external binary like ss needed). "
                             "Aggregates bytes_sent / bytes_received across all "
                             "ESTABLISHED sockets on the port. When omitted, no "
                             "traffic monitoring is done.")
    parser.add_argument("--brpc-bvar-port", type=int, default=None,
                        help="Read anon_jemalloc_* metrics from BRPC /vars on "
                             "this port. Missing builtin services or bvars are "
                             "reported as empty CSV cells.")
    parser.add_argument("--brpc-bvar-host", default="127.0.0.1",
                        help="Host where the monitored process exposes BRPC "
                             "builtin services (default: 127.0.0.1).")
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
        needs_header = not os.path.exists(output_path) or os.path.getsize(output_path) == 0
        outfile = open(output_path, "a", buffering=1, newline='')
        atexit.register(outfile.close)
        out = outfile
    else:
        needs_header = True
        out = sys.stdout

    writer = csv.DictWriter(out, fieldnames=CSV_FIELDS, extrasaction='ignore')
    if needs_header:
        writer.writeheader()
        out.flush()

    if args.pid:
        pid = args.pid
    else:
        pid = find_pid(args.process)
        if pid is None:
            print(f"Process '{args.process}' not found", file=sys.stderr)
            sys.exit(1)

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
            break

        dt = now - prev_time
        if dt > 0 and prev_cpu is not None:
            cpu_pct = (cpu_ticks - prev_cpu) / clock_ticks / dt * 100
        else:
            cpu_pct = 0.0

        mem_mb = rss_bytes / (1024 * 1024)
        anon_mb = anon_bytes / (1024 * 1024)
        shared_mb = shared_bytes / (1024 * 1024)

        fails, opens = read_tcp_attempt_fails_stats()
        fails_per_sec = None
        fail_rate = None
        if fails is not None and opens is not None:
            if prev_fails is not None and prev_opens is not None:
                delta_fails = max(0, fails - prev_fails)
                delta_opens = max(0, opens - prev_opens)
            else:
                delta_fails = 0
                delta_opens = 0
            fails_per_sec = delta_fails / dt if dt > 0 else 0.0
            fail_rate = (delta_fails / delta_opens * 100) if delta_opens > 0 else 0.0
            prev_fails = fails
            prev_opens = opens

        bytes_in_per_sec = None
        bytes_out_per_sec = None
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
                prev_sent = sent
                prev_recv = recv

        row = {
            'timestamp': time.strftime("%Y-%m-%dT%H:%M:%S"),
            'pid': pid,
            'cpu_pct': f'{cpu_pct:.3f}',
            'rss_mb': f'{mem_mb:.3f}',
            'anon_mb': f'{anon_mb:.3f}',
            'shared_mb': f'{shared_mb:.3f}',
            'fd': fd_count,
            'tcp_fails_per_sec': (f'{fails_per_sec:.3f}'
                                  if fails_per_sec is not None else None),
            'tcp_fail_rate_pct': (f'{fail_rate:.3f}'
                                  if fail_rate is not None else None),
            'bytes_in_mb_per_sec': (f'{bytes_in_per_sec / (1024 * 1024):.3f}'
                                    if bytes_in_per_sec is not None else None),
            'bytes_out_mb_per_sec': (f'{bytes_out_per_sec / (1024 * 1024):.3f}'
                                     if bytes_out_per_sec is not None else None),
        }
        if args.brpc_bvar_port:
            bvars = read_jemalloc_bvars(args.brpc_bvar_port,
                                        host=args.brpc_bvar_host)
            add_jemalloc_metrics(row, bvars)
        writer.writerow(row)
        out.flush()

        prev_cpu = cpu_ticks
        prev_time = now


if __name__ == "__main__":
    main()
