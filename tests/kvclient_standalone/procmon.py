#!/usr/bin/env python3
"""Monitor CPU and memory usage of a process by name."""

import argparse
import os
import signal
import subprocess
import sys
import time


def find_pid(name):
    """Find PID by exact process name (first match)."""
    try:
        out = subprocess.check_output(["pgrep", "-x", name], text=True).strip()
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


def format_mb(bytes_val):
    return f"{bytes_val / (1024 * 1024):.1f}"


def main():
    parser = argparse.ArgumentParser(description="Monitor process CPU and memory usage")
    parser.add_argument("-p", "--process", required=True, help="Process name to monitor")
    parser.add_argument("-i", "--interval", type=float, default=2, help="Sample interval in seconds (default: 2)")
    parser.add_argument("-d", "--duration", type=float, default=0, help="Monitor duration in seconds, 0=until exit/Ctrl+C (default: 0)")
    args = parser.parse_args()

    clock_ticks = os.sysconf("SC_CLK_TCK")

    pid = find_pid(args.process)
    if pid is None:
        print(f"Process '{args.process}' not found", file=sys.stderr)
        sys.exit(1)

    print(f"Monitoring PID={pid} ({args.process}), interval={args.interval}s"
          + (f", duration={args.duration}s" if args.duration > 0 else ""))

    samples_cpu = []
    samples_mem = []
    prev_cpu = read_proc_stat(pid)
    prev_time = time.monotonic()
    start_time = prev_time

    running = True

    def on_signal(_, __):
        nonlocal running
        running = False

    signal.signal(signal.SIGINT, on_signal)
    signal.signal(signal.SIGTERM, on_signal)

    while running:
        time.sleep(args.interval)

        if args.duration > 0 and (time.monotonic() - start_time) >= args.duration:
            break

        now = time.monotonic()
        cpu_ticks = read_proc_stat(pid)
        rss_bytes = read_proc_statm(pid)

        if cpu_ticks is None or rss_bytes is None:
            print(f"[{time.strftime('%H:%M:%S')}] Process exited")
            break

        dt = now - prev_time
        if dt > 0 and prev_cpu is not None:
            cpu_pct = (cpu_ticks - prev_cpu) / clock_ticks / dt * 100
        else:
            cpu_pct = 0.0

        mem_mb = rss_bytes / (1024 * 1024)

        samples_cpu.append(cpu_pct)
        samples_mem.append(mem_mb)

        ts = time.strftime("%H:%M:%S")
        print(f"[{ts}] PID={pid} CPU={cpu_pct:.1f}% MEM={format_mb(rss_bytes)}MB")

        prev_cpu = cpu_ticks
        prev_time = now

    elapsed = time.monotonic() - start_time
    print()
    print("=== Summary ===")
    print(f"Samples: {len(samples_cpu)}, Duration: {elapsed:.0f}s")
    if samples_cpu:
        print(f"CPU  avg={sum(samples_cpu)/len(samples_cpu):.1f}%  peak={max(samples_cpu):.1f}%")
        avg_mem = sum(samples_mem) / len(samples_mem)
        peak_mem = max(samples_mem)
        print(f"MEM  avg={avg_mem:.1f}MB peak={peak_mem:.1f}MB")


if __name__ == "__main__":
    main()
