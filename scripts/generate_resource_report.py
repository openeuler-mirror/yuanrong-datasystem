#!/usr/bin/env python3
"""Worker resource log analysis - generates interactive HTML report with ECharts.

Parses KVCache worker resource.log files, computes per-time-interval aggregated
resource metrics across hosts, and generates a self-contained HTML report.

Resource.log Format (pipe-separated, 7 header fields + 22 metric groups):
  $1  timestamp                    ISO 8601 with microseconds
  $2  log_level                    I/W/E
  $3  source                       res_metric_collector.cpp:line
  $4  worker_id                    e.g. searchgenrecallkvworker-24-00001
  $5  pid:tid
  $6  trace_id                     UUID
  $7  record_type                  model_kvcache_record
  --- metric fields (enum order, res_metrics.def) ---
  $8  SHARED_MEMORY                memoryUsage/physicalUsage/totalLimit/shareMemRatio/scMemUsage/scMemLimit
  $9  SPILL_HARD_DISK              spaceUsage/physicalSpaceUsage/totalLimit/workerSpillHdUsage
  $10 ACTIVE_CLIENT_COUNT          integer
  $11 OBJECT_COUNT                 integer
  $12 OBJECT_SIZE                  bytes
  $13 WORKER_OC_SERVICE_THREAD     idle/total/max/waiting/usage
  $14 WORKER_WORKER_OC_THREAD      idle/total/max/waiting/usage
  $15 MASTER_WORKER_OC_THREAD      idle/total/max/waiting/usage
  $16 MASTER_OC_SERVICE_THREAD     idle/total/max/waiting/usage
  $17 ETCD_QUEUE                   currentSize/limit/usage
  $18 ETCD_REQUEST_SUCCESS_RATE    float 0-1
  $19 OBS_REQUEST_SUCCESS_RATE     float 0-1
  $20 MASTER_ASYNC_TASKS_THREAD    idle/total/max/waiting/usage
  $21 STREAM_COUNT                 integer
  $22 WORKER_SC_SERVICE_THREAD     idle/total/max/waiting/usage
  $23 WORKER_WORKER_SC_THREAD      idle/total/max/waiting/usage
  $24 MASTER_WORKER_SC_THREAD      idle/total/max/waiting/usage
  $25 MASTER_SC_SERVICE_THREAD     idle/total/max/waiting/usage
  $26 STREAM_REMOTE_SEND_RATE      float 0-1
  $27 SHARED_DISK                  usage/physicalUsage/limit/usageRate
  $28 SC_LOCAL_CACHE               usage/reserved/limit/usageRate
  $29 OC_HIT_NUM                   memHit/diskHit/l2Hit/remoteHit/miss

Usage:
  # Basic usage
  python3 scripts/generate_resource_report.py /path/to/worker_logs \\
      --since "2026-05-05T22:30:00" -o report.html

  # With time range and auto-open
  python3 scripts/generate_resource_report.py /path/to/worker_logs \\
      --since "2026-05-05T22:30:00" --until "2026-05-06T08:00:00" \\
      -o report.html --open

  # WSL: output to Windows drive and open in Edge
  python3 scripts/generate_resource_report.py /path/to/worker_logs \\
      --since "2026-05-05T22:30:00" \\
      -o /mnt/d/html/report.html --open

  # Custom time bin interval (e.g. 30 minutes)
  python3 scripts/generate_resource_report.py /path/to/worker_logs \\
      --since "2026-05-05T22:30:00" --interval 30 -o report.html

Directory layouts (auto-detected):
  Layout 1: <log_dir>/<host_ip>/logs/resource.log
  Layout 2: <log_dir>/<wrapper>/<host_ip>/logs/resource.log
  Layout 3: <log_dir>/resource.log

Arguments:
  log_dir             Root directory containing worker resource.log files
  --since             Start time (inclusive), format: YYYY-MM-DDTHH:MM:SS
  --until             End time (exclusive), format: YYYY-MM-DDTHH:MM:SS
  -o / --output       Output HTML file path (default: resource_report.html)
  --log-pattern       Filename substring to match (default: resource)
  --interval          Time bin interval in minutes (default: 60)
  --open              Open report in browser after generation

HTML Report Sections (10 charts + 1 detail table):
  1. Memory Usage Trend        - Cluster memory trend (Avg/Max/Limit)
  2. Memory Usage Ratio        - Per-hour usage ratio + per-host distribution
  3. Object Cache              - Object count and size trends
  4. Cache Hit Rate            - Hit rate trend + hit/miss stacked distribution
  5. Thread Pool Utilization   - 5 thread pools (Active/Total/Max/Waiting)
  6. ETCD Queue                - Queue depth with limit reference line
  7. Active Clients            - Connected client count trend
  8. ETCD Success Rate         - ETCD request success rate
  9. Per-Host Memory           - Interactive per-host memory trend
  10. Hourly Detail Table      - All metrics in tabular form

Notes:
  - Requires internet access to load ECharts from CDN (jsdelivr.net)
  - OC_HIT_NUM is a cumulative counter; the script computes per-bin deltas
  - Thread pool charts show Active = Total - Idle (not usage%)
"""

import argparse
import html
import math
import os
import re
import subprocess
import sys
import textwrap
from collections import defaultdict
from datetime import datetime


# All gauge metric keys collected per time bin
_GAUGE_KEYS = [
    "mem_usage", "mem_physical", "mem_total_limit", "mem_ratio",
    "sc_mem_usage", "sc_mem_limit", "object_count", "object_size",
    "active_clients", "stream_count",
    "etcd_queue_size", "etcd_queue_usage", "etcd_queue_limit",
    "disk_usage", "disk_rate", "sc_cache_usage", "sc_cache_rate",
]

# Thread pool prefixes and their sub-fields
_TP_PREFIXES = ["worker_oc", "worker_worker_oc", "master_worker_oc", "master_oc", "async"]
_TP_SUB_FIELDS = ["idle", "total", "max", "waiting", "usage"]

# Cumulative counter keys (OC_HIT_NUM)
_OC_HIT_KEYS = ["oc_mem_hit", "oc_disk_hit", "oc_l2_hit", "oc_remote_hit", "oc_miss"]


def _find_log_files(log_dir, pattern="resource"):
    """Find resource.log files supporting 3 directory layouts."""
    log_files = {}
    for root, dirs, files in os.walk(log_dir, followlinks=False):
        for f in files:
            if pattern in f and f.endswith(".log"):
                host = None
                parts = root.replace(log_dir, "").strip("/").split("/")
                for p in parts:
                    if re.match(r"\d+\.\d+\.\d+\.\d+", p):
                        host = p
                        break
                if host is None:
                    host = "localhost"
                log_files.setdefault(host, []).append(os.path.join(root, f))
    return log_files


def _parse_sub(s, delim="/"):
    """Parse slash-separated sub-fields, returns list of floats."""
    if not s or not s.strip():
        return []
    parts = s.strip().split(delim)
    result = []
    for p in parts:
        try:
            result.append(float(p))
        except ValueError:
            result.append(0.0)
    return result


def _fmt_bytes(b):
    """Format bytes to human-readable."""
    if b == 0:
        return "0 B"
    units = ["B", "KB", "MB", "GB", "TB"]
    idx = min(int(math.log(abs(b), 1024)), len(units) - 1)
    return f"{b / (1024 ** idx):.2f} {units[idx]}"


def _fmt_pct(v):
    return f"{v * 100:.1f}%"


def _parse_thread_pools(parts):
    """Parse all thread pool fields, return dict of tp_{name}_{field} = value."""
    m = {}
    pool_defs = [
        ("worker_oc", 12), ("worker_worker_oc", 13),
        ("master_worker_oc", 14), ("master_oc", 15),
        ("async", 19),  # MASTER_ASYNC_TASKS
    ]
    for name, idx in pool_defs:
        raw = parts[idx] if len(parts) > idx and parts[idx] else ""
        sub = _parse_sub(raw)
        if len(sub) >= 5:
            for i, field in enumerate(_TP_SUB_FIELDS):
                m[f"tp_{name}_{field}"] = sub[i]
    return m


def parse_resource_logs(log_files_by_host, since=None, until=None):
    """Parse all resource.log files, return host -> [(timestamp, metrics_dict)]."""
    since_ts = datetime.fromisoformat(since) if since else None
    until_ts = datetime.fromisoformat(until) if until else None

    all_data = {}
    total_lines = 0
    parsed_lines = 0

    for host, files in sorted(log_files_by_host.items()):
        records = []
        for fpath in files:
            with open(fpath, "r", errors="replace") as f:
                for line in f:
                    total_lines += 1
                    line = line.strip()
                    if not line:
                        continue
                    parts = [p.strip() for p in line.split("|")]
                    if len(parts) < 12:
                        continue

                    try:
                        ts = datetime.fromisoformat(parts[0])
                    except (ValueError, IndexError):
                        continue

                    if since_ts and ts < since_ts:
                        continue
                    if until_ts and ts >= until_ts:
                        continue

                    parsed_lines += 1
                    m = {}

                    # $8 SHARED_MEMORY (6 sub-fields)
                    mem = _parse_sub(parts[7]) if len(parts) > 7 else []
                    m["mem_usage"] = mem[0] if len(mem) > 0 else 0
                    m["mem_physical"] = mem[1] if len(mem) > 1 else 0
                    m["mem_total_limit"] = mem[2] if len(mem) > 2 else 0
                    m["mem_ratio"] = mem[3] if len(mem) > 3 else 0
                    m["sc_mem_usage"] = mem[4] if len(mem) > 4 else 0
                    m["sc_mem_limit"] = mem[5] if len(mem) > 5 else 0

                    # $9 SPILL_HARD_DISK (4)
                    spill = _parse_sub(parts[8]) if len(parts) > 8 else []
                    m["spill_space"] = spill[0] if len(spill) > 0 else 0
                    m["spill_physical"] = spill[1] if len(spill) > 1 else 0
                    m["spill_limit"] = spill[2] if len(spill) > 2 else 0

                    # $10-$12 simple values
                    m["active_clients"] = float(parts[9]) if len(parts) > 9 and parts[9] else 0
                    m["object_count"] = float(parts[10]) if len(parts) > 10 and parts[10] else 0
                    m["object_size"] = float(parts[11]) if len(parts) > 11 and parts[11] else 0

                    # Thread pools: $13-$16, $20 (async)
                    m.update(_parse_thread_pools(parts))

                    # $17 ETCD_QUEUE
                    etcd_q = _parse_sub(parts[16]) if len(parts) > 16 else []
                    m["etcd_queue_size"] = etcd_q[0] if len(etcd_q) > 0 else 0
                    m["etcd_queue_limit"] = etcd_q[1] if len(etcd_q) > 1 else 0
                    m["etcd_queue_usage"] = etcd_q[2] if len(etcd_q) > 2 else 0

                    # $18-$19 success rates (-1 = unavailable)
                    m["etcd_success_rate"] = float(parts[17]) if len(parts) > 17 and parts[17] else -1
                    m["obs_success_rate"] = float(parts[18]) if len(parts) > 18 and parts[18] else -1

                    # $21 STREAM_COUNT
                    m["stream_count"] = float(parts[20]) if len(parts) > 20 and parts[20] else 0

                    # $27 SHARED_DISK
                    disk = _parse_sub(parts[26]) if len(parts) > 26 else []
                    m["disk_usage"] = disk[0] if len(disk) > 0 else 0
                    m["disk_physical"] = disk[1] if len(disk) > 1 else 0
                    m["disk_limit"] = disk[2] if len(disk) > 2 else 0
                    m["disk_rate"] = disk[3] if len(disk) > 3 else 0

                    # $28 SC_LOCAL_CACHE
                    sc = _parse_sub(parts[27]) if len(parts) > 27 else []
                    m["sc_cache_usage"] = sc[0] if len(sc) > 0 else 0
                    m["sc_cache_reserved"] = sc[1] if len(sc) > 1 else 0
                    m["sc_cache_limit"] = sc[2] if len(sc) > 2 else 0
                    m["sc_cache_rate"] = sc[3] if len(sc) > 3 else 0

                    # $29 OC_HIT_NUM (cumulative counter)
                    hit = _parse_sub(parts[28]) if len(parts) > 28 else []
                    m["oc_mem_hit"] = hit[0] if len(hit) > 0 else 0
                    m["oc_disk_hit"] = hit[1] if len(hit) > 1 else 0
                    m["oc_l2_hit"] = hit[2] if len(hit) > 2 else 0
                    m["oc_remote_hit"] = hit[3] if len(hit) > 3 else 0
                    m["oc_miss"] = hit[4] if len(hit) > 4 else 0

                    records.append((ts, m))

        records.sort(key=lambda x: x[0])
        all_data[host] = records

    print(f"Parsed {parsed_lines}/{total_lines} records from {len(log_files_by_host)} hosts")
    return all_data


def _time_bin(ts, interval_minutes=60):
    """Round timestamp to time bin."""
    total_minutes = ts.hour * 60 + ts.minute
    bin_minute = (total_minutes // interval_minutes) * interval_minutes
    return ts.replace(hour=bin_minute // 60, minute=bin_minute % 60,
                      second=0, microsecond=0)


def _compute_oc_deltas(all_data, interval_minutes):
    """Compute per-host per-bin OC_HIT_NUM deltas from cumulative counters.

    Returns: {time_bin: {host: (delta_mem, delta_disk, delta_l2, delta_remote, delta_miss)}}
    """
    sorted_bins = sorted(set(
        _time_bin(ts, interval_minutes)
        for records in all_data.values()
        for ts, _ in records
    ))

    # Last OC sample per host per bin
    host_bin_last = {}
    for host, records in all_data.items():
        host_bins = defaultdict(list)
        for ts, m in records:
            tb = _time_bin(ts, interval_minutes)
            oc_vals = tuple(m.get(k, 0) for k in _OC_HIT_KEYS)
            host_bins[tb].append((ts, oc_vals))
        host_bin_last[host] = {}
        for tb, samples in host_bins.items():
            samples.sort(key=lambda x: x[0])
            host_bin_last[host][tb] = samples[-1][1]

    # Compute deltas
    oc_deltas = {}
    for host in host_bin_last:
        prev_vals = None
        for tb in sorted_bins:
            cur_vals = host_bin_last[host].get(tb)
            if cur_vals is not None:
                if prev_vals is not None:
                    delta = tuple(max(0, c - p) for c, p in zip(cur_vals, prev_vals))
                    oc_deltas.setdefault(tb, {})[host] = delta
                prev_vals = cur_vals

    return oc_deltas, sorted_bins


def compute_cluster_stats(all_data, interval_minutes=60):
    """Aggregate per-time-bin cluster-wide statistics.

    Gauge metrics: avg/max/min across samples.
    Cumulative counters (OC_HIT_NUM): per-host delta, then sum across hosts.
    """
    oc_deltas, sorted_bins = _compute_oc_deltas(all_data, interval_minutes)

    # All gauge metric keys including thread pool sub-fields
    tp_keys = [f"tp_{prefix}_{field}" for prefix in _TP_PREFIXES for field in _TP_SUB_FIELDS]
    all_gauge_keys = _GAUGE_KEYS + tp_keys

    # Collect samples per bin
    bins = defaultdict(lambda: {"hosts": set(), **{k: [] for k in all_gauge_keys}})

    for host, records in all_data.items():
        for ts, m in records:
            tb = _time_bin(ts, interval_minutes)
            b = bins[tb]
            b["hosts"].add(host)
            for key in all_gauge_keys:
                if key in m:
                    b[key].append(m[key])
            if m.get("etcd_success_rate", -1) >= 0:
                b.setdefault("etcd_success_rate", []).append(m["etcd_success_rate"])
            if m.get("obs_success_rate", -1) >= 0:
                b.setdefault("obs_success_rate", []).append(m["obs_success_rate"])

    # Aggregate
    result = []
    for tb in sorted(bins.keys()):
        b = bins[tb]
        s = {"time": tb, "host_count": len(b["hosts"])}

        for key in all_gauge_keys:
            vals = b.get(key, [])
            if vals:
                s[f"{key}_avg"] = sum(vals) / len(vals)
                s[f"{key}_max"] = max(vals)
                s[f"{key}_min"] = min(vals)
            else:
                s[f"{key}_avg"] = 0
                s[f"{key}_max"] = 0
                s[f"{key}_min"] = 0

        for key in ["etcd_success_rate", "obs_success_rate"]:
            vals = b.get(key, [])
            if vals:
                s[f"{key}_avg"] = sum(vals) / len(vals)
                s[f"{key}_min"] = min(vals)
            else:
                s[f"{key}_avg"] = -1
                s[f"{key}_min"] = -1

        # OC deltas
        host_deltas = oc_deltas.get(tb, {})
        for i, name in enumerate(_OC_HIT_KEYS):
            vals = [d[i] for d in host_deltas.values()]
            s[f"{name}_sum"] = sum(vals)
            s[f"{name}_avg"] = sum(vals) / len(vals) if vals else 0

        total_hits = sum(s.get(f"{k}_sum", 0) for k in _OC_HIT_KEYS[:4])
        total_requests = total_hits + s.get("oc_miss_sum", 0)
        s["cache_hit_rate"] = total_hits / total_requests if total_requests > 0 else -1
        s["total_hits"] = total_hits
        s["total_requests"] = total_requests

        result.append(s)

    return result


def compute_per_host_stats(all_data, interval_minutes=60):
    """Compute per-host per-time-bin stats for key metrics."""
    result = {}
    for host, records in all_data.items():
        bins = defaultdict(dict)
        oc_last_per_bin = {}
        for ts, m in records:
            tb = _time_bin(ts, interval_minutes)
            for key in ["mem_usage", "mem_ratio", "object_count", "object_size"]:
                if key in m:
                    bins[tb].setdefault(key, []).append(m[key])
            oc_vals = {k: m.get(k, 0) for k in _OC_HIT_KEYS}
            oc_last_per_bin[tb] = (ts, oc_vals)

        # OC deltas
        sorted_tbs = sorted(oc_last_per_bin.keys())
        oc_deltas = {}
        prev_oc = None
        for tb in sorted_tbs:
            cur_oc = oc_last_per_bin[tb][1]
            if prev_oc is not None:
                delta = {k: max(0, cur_oc[k] - prev_oc[k]) for k in _OC_HIT_KEYS}
                oc_deltas[tb] = delta
            prev_oc = cur_oc

        host_stats = []
        for tb in sorted(bins.keys()):
            b = bins[tb]
            s = {"time": tb, "host": host}
            for key, vals in b.items():
                s[f"{key}_avg"] = sum(vals) / len(vals) if vals else 0
                s[f"{key}_max"] = max(vals) if vals else 0
                s[f"{key}_min"] = min(vals) if vals else 0

            oc_d = oc_deltas.get(tb, {})
            total_hits = sum(oc_d.get(k, 0) for k in _OC_HIT_KEYS[:4])
            total_req = total_hits + oc_d.get("oc_miss", 0)
            s["cache_hit_rate"] = total_hits / total_req if total_req > 0 else -1

            host_stats.append(s)

        result[host] = host_stats
    return result


def generate_html(cluster_stats, per_host_stats, all_data, output_path, since, until):
    """Generate self-contained HTML report with ECharts."""
    if not cluster_stats:
        print("No data to report")
        return

    first_ts = cluster_stats[0]["time"]
    last_ts = cluster_stats[-1]["time"]
    host_count = len(all_data)
    total_records = sum(len(v) for v in all_data.values())

    time_labels = [s["time"].strftime("%m-%d %H:%M") for s in cluster_stats]

    # Data series extraction
    mem_usage_avg = [round(s["mem_usage_avg"] / 1024**3, 2) for s in cluster_stats]
    mem_usage_max = [round(s["mem_usage_max"] / 1024**3, 2) for s in cluster_stats]
    mem_limit = [round(s["mem_total_limit_avg"] / 1024**3, 2) for s in cluster_stats]
    mem_ratio_avg = [round(s["mem_ratio_avg"] * 100, 1) for s in cluster_stats]

    obj_count_avg = [round(s["object_count_avg"]) for s in cluster_stats]
    obj_count_max = [round(s["object_count_max"]) for s in cluster_stats]
    obj_size_avg = [round(s["object_size_avg"] / 1024**3, 2) for s in cluster_stats]

    cache_hit_rate = [round(s["cache_hit_rate"] * 100, 2) if s["cache_hit_rate"] >= 0 else "null" for s in cluster_stats]
    total_hits = [round(s["total_hits"]) for s in cluster_stats]
    total_requests = [round(s["total_requests"]) for s in cluster_stats]
    oc_miss = [round(s["oc_miss_sum"]) for s in cluster_stats]
    oc_mem_hit = [round(s["oc_mem_hit_sum"]) for s in cluster_stats]
    oc_disk_hit = [round(s["oc_disk_hit_sum"]) for s in cluster_stats]
    oc_l2_hit = [round(s["oc_l2_hit_sum"]) for s in cluster_stats]
    oc_remote_hit = [round(s["oc_remote_hit_sum"]) for s in cluster_stats]

    def tp_series(prefix):
        active = [round(max(0, s.get(f"tp_{prefix}_total_avg", 0) - s.get(f"tp_{prefix}_idle_avg", 0))) for s in cluster_stats]
        total = [round(s.get(f"tp_{prefix}_total_avg", 0)) for s in cluster_stats]
        mx = [round(s.get(f"tp_{prefix}_max_avg", 0)) for s in cluster_stats]
        waiting = [round(s.get(f"tp_{prefix}_waiting_avg", 0)) for s in cluster_stats]
        return active, total, mx, waiting

    tp_oc_active, tp_oc_total, tp_oc_max, tp_oc_waiting = tp_series("worker_oc")
    tp_ww_oc_active, tp_ww_oc_total, tp_ww_oc_max, tp_ww_oc_waiting = tp_series("worker_worker_oc")
    tp_mw_oc_active, tp_mw_oc_total, tp_mw_oc_max, tp_mw_oc_waiting = tp_series("master_worker_oc")
    tp_m_oc_active, tp_m_oc_total, tp_m_oc_max, tp_m_oc_waiting = tp_series("master_oc")
    tp_async_active, tp_async_total, tp_async_max, tp_async_waiting = tp_series("async")

    etcd_q_size = [round(s["etcd_queue_size_avg"]) for s in cluster_stats]
    etcd_q_max = [round(s["etcd_queue_size_max"]) for s in cluster_stats]
    etcd_q_limit = [round(s["etcd_queue_limit_avg"]) for s in cluster_stats]

    active_clients_avg = [round(s["active_clients_avg"]) for s in cluster_stats]
    active_clients_max = [round(s["active_clients_max"]) for s in cluster_stats]

    etcd_rate = [round(s["etcd_success_rate_avg"] * 100, 2) if s["etcd_success_rate_avg"] >= 0 else "null" for s in cluster_stats]

    # Per-host data
    hosts_sorted = sorted(all_data.keys())
    host_options = "\n".join(f'<option value="{html.escape(h)}">{html.escape(h)}</option>' for h in hosts_sorted)

    def _host_mem_series(host):
        points = per_host_stats.get(host, [])
        items = ",".join(
            '["' + s["time"].strftime("%m-%d %H:%M") + '",' + str(round(s.get("mem_usage_avg", 0) / 1024**3, 2)) + "]"
            for s in points
        )
        return f'"{host}":[{items}]'

    per_host_mem_js = "{" + ",".join(_host_mem_series(h) for h in hosts_sorted) + "}"

    # Summary table
    latest = cluster_stats[-1]
    peak_mem = max(cluster_stats, key=lambda s: s["mem_usage_max"])
    peak_obj = max(cluster_stats, key=lambda s: s["object_count_max"])

    summary_rows = f"""
    <tr><td>Time Range</td><td>{first_ts.strftime('%Y-%m-%d %H:%M')} ~ {last_ts.strftime('%Y-%m-%d %H:%M')}</td></tr>
    <tr><td>Host Count</td><td>{host_count}</td></tr>
    <tr><td>Total Records</td><td>{total_records:,}</td></tr>
    <tr><td>Latest Memory Usage (avg)</td><td>{_fmt_bytes(latest['mem_usage_avg'])} / {_fmt_bytes(latest['mem_total_limit_avg'])} ({_fmt_pct(latest['mem_ratio_avg'])})</td></tr>
    <tr><td>Peak Memory Usage</td><td>{_fmt_bytes(peak_mem['mem_usage_max'])} at {peak_mem['time'].strftime('%m-%d %H:%M')}</td></tr>
    <tr><td>Latest Object Count (avg/host)</td><td>{latest['object_count_avg']:,.0f}</td></tr>
    <tr><td>Peak Object Count</td><td>{peak_obj['object_count_max']:,.0f} at {peak_obj['time'].strftime('%m-%d %H:%M')}</td></tr>
    <tr><td>Latest Cache Hit Rate</td><td>{_fmt_pct(latest['cache_hit_rate']) if latest['cache_hit_rate'] >= 0 else 'N/A'}</td></tr>
    <tr><td>Active Clients</td><td>{latest['active_clients_avg']:.0f}</td></tr>
    """

    # Hourly detail table
    detail_rows = ""
    for s in cluster_stats:
        chr_val = _fmt_pct(s["cache_hit_rate"]) if s["cache_hit_rate"] >= 0 else "N/A"
        detail_rows += f"""
        <tr>
            <td>{s['time'].strftime('%m-%d %H:%M')}</td>
            <td>{s['host_count']}</td>
            <td>{_fmt_bytes(s['mem_usage_avg'])}</td>
            <td>{_fmt_bytes(s['mem_usage_max'])}</td>
            <td>{_fmt_pct(s['mem_ratio_avg'])}</td>
            <td>{s['object_count_avg']:,.0f}</td>
            <td>{s['object_count_max']:,.0f}</td>
            <td>{_fmt_bytes(s['object_size_avg'])}</td>
            <td>{chr_val}</td>
            <td>{s['total_hits']:,.0f}</td>
            <td>{s['total_requests']:,.0f}</td>
            <td>{_fmt_pct(s['tp_worker_oc_usage_avg'])}</td>
            <td>{s['tp_worker_oc_waiting_avg']:.0f}</td>
            <td>{_fmt_pct(s['tp_async_usage_avg'])}</td>
            <td>{s['tp_async_waiting_avg']:.0f}</td>
        </tr>"""

    report_html = f"""<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="UTF-8">
<title>Worker Resource Analysis Report</title>
<script src="https://cdn.jsdelivr.net/npm/echarts@5/dist/echarts.min.js"></script>
<style>
body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; margin: 20px; background: #f5f7fa; }}
h1 {{ color: #1a1a2e; border-bottom: 2px solid #0f3460; padding-bottom: 10px; }}
h2 {{ color: #16213e; margin-top: 30px; }}
.container {{ max-width: 1400px; margin: 0 auto; }}
.chart {{ width: 100%; height: 400px; margin: 15px 0; background: #fff; border-radius: 8px; box-shadow: 0 2px 8px rgba(0,0,0,0.1); padding: 10px; }}
.chart-row {{ display: flex; gap: 20px; flex-wrap: wrap; }}
.chart-half {{ flex: 1; min-width: 600px; }}
table {{ border-collapse: collapse; width: 100%; margin: 15px 0; background: #fff; border-radius: 8px; overflow: hidden; box-shadow: 0 2px 8px rgba(0,0,0,0.1); }}
th, td {{ border: 1px solid #ddd; padding: 8px 12px; text-align: center; font-size: 13px; }}
th {{ background: #16213e; color: #fff; position: sticky; top: 0; }}
tr:nth-child(even) {{ background: #f8f9fa; }}
tr:hover {{ background: #e8f4f8; }}
.summary-table td:first-child {{ font-weight: bold; text-align: right; background: #f0f4f8; width: 250px; }}
.info {{ background: #e3f2fd; padding: 12px 20px; border-radius: 8px; margin: 15px 0; color: #1565c0; }}
select {{ padding: 6px 12px; border-radius: 4px; border: 1px solid #ccc; font-size: 14px; }}
.detail-wrap {{ max-height: 500px; overflow-y: auto; }}
</style>
</head>
<body>
<div class="container">
<h1>Worker Resource Analysis Report</h1>
<div class="info">
  Time: {first_ts.strftime('%Y-%m-%d %H:%M')} ~ {last_ts.strftime('%Y-%m-%d %H:%M')} |
  Hosts: {host_count} | Records: {total_records:,}
  {"| Since: " + since if since else ""}{" | Until: " + until if until else ""}
</div>

<h2>Overview Summary</h2>
<table class="summary-table">{summary_rows}</table>

<h2>1. Memory Usage Trend (内存使用趋势)</h2>
<div class="info">
<b>指标说明</b>：集群内存使用量趋势。Avg Usage = 所有 Worker 内存使用的平均值，Max Usage = 单个 Worker 最高内存使用，Limit = Worker 内存总限额。单位: GB。<br>
<b>正常值</b>：使用率 &lt; 60%，内存使用平稳或随业务量缓慢变化。<br>
<b>异常特征</b>：使用率 &gt; 80%（黄色警戒），&gt; 90%（红色告警）；Max Usage 持续逼近 Limit；内存使用持续上升不回落（疑似泄漏）。<br>
<b>排查建议</b>：① 检查 Object Count 是否异常增长（大量写入未释放）；② 查看 per-Host 图确认是个别 Worker 还是集群整体问题；③ 检查 eviction 配置是否生效；④ 若持续上升不回落，排查客户端是否未正确调用 Remove/Delete。
</div>
<div id="mem_chart" class="chart"></div>

<h2>2. Memory Usage Ratio (内存使用率)</h2>
<div class="chart-row">
<div class="chart-half">
<div class="info">
<b>指标说明</b>：每小时的平均内存使用率（已用/限额）。颜色：绿色 &lt; 60%，黄色 60-80%，红色 &gt; 80%。<br>
<b>正常值</b>：&lt; 60% 为健康，60-80% 需关注。<br>
<b>异常特征</b>：&gt; 80% 触发 eviction 风险；&gt; 90% 新写入可能失败。<br>
<b>排查建议</b>：同上节"内存使用趋势"。
</div>
<div id="mem_ratio_chart" class="chart" style="height:350px"></div></div>
<div class="chart-half">
<div class="info">
<b>指标说明</b>：所有 Host 的最新内存使用量排名（{host_count} 个 Host）。颜色同左。<br>
<b>正常值</b>：各 Host 使用量相对均匀。<br>
<b>异常特征</b>：个别 Host 使用量远高于其他（热点问题）；或所有 Host 同时高（容量不足）。<br>
<b>排查建议</b>：热点 Host 检查 hash 分配是否不均，考虑扩容或调整分片策略。
</div>
<div id="mem_dist_chart" class="chart" style="height:900px"></div></div>
</div>

<h2>3. Object Cache (对象缓存)</h2>
<div class="info">
<b>指标说明</b>：Object Cache 中缓存的对象数量和总大小。Avg/Host = 每 Worker 平均值，Max = 单 Worker 最大值。<br>
<b>正常值</b>：对象数量随业务量波动，趋势与内存使用正相关。<br>
<b>异常特征</b>：对象数量骤降（可能触发大规模 eviction 或数据丢失）；对象数量持续单方向增长不回落（疑似泄漏）。<br>
<b>排查建议</b>：① 骤降时检查是否有 Worker 重启或网络分区；② 持续增长时检查客户端是否未调用 Remove；③ 对比 Object Size 增长判断是大对象还是小对象问题。
</div>
<div class="chart-row">
<div class="chart-half"><div id="obj_count_chart" class="chart" style="height:350px"></div></div>
<div class="chart-half"><div id="obj_size_chart" class="chart" style="height:350px"></div></div>
</div>

<h2>4. Cache Hit Rate (缓存命中率)</h2>
<div class="info">
<b>指标说明</b>：Object Cache 命中率。Mem Hit = 内存命中（最快），Disk Hit = 磁盘命中，L2 Hit = L2 缓存命中，Remote Hit = 远程节点命中（最慢），Miss = 未命中（需回源）。命中率 = 四次命中之和 / 总请求数。99% 标线为参考阈值。<br>
<b>正常值</b>：命中率 &ge; 99%，Mem Hit 占绝大多数。<br>
<b>异常特征</b>：命中率 &lt; 95%；Miss 数量突增；Remote Hit 占比升高（本节点缓存不足）。<br>
<b>排查建议</b>：① Miss 突增：检查是否冷启动（新写入后首次读取）；② Remote Hit 高：检查 Worker 内存是否不足导致 eviction；③ Disk Hit 高：检查内存容量配置是否足够；④ 整体命中率低：评估是否需要扩容 Worker 内存。
</div>
<div id="cache_hit_chart" class="chart"></div>
<div id="cache_detail_chart" class="chart"></div>

<h2>5. Thread Pool Utilization (线程池使用)</h2>
<div class="info">
<b>指标说明</b>：各线程池的线程数统计。Active = 活跃线程数（Total - Idle），Total = 当前线程数，Max = 线程池上限，Waiting = 排队等待的任务数。<br>
<b>5 个线程池</b>：客户端服务（处理客户端读写请求）、Worker 间通信（跨节点对象传输）、Master 下发（管理指令下发到 Worker）、Master 服务（Master 端对象管理）、Master 异步任务（后台异步操作如元数据清理、异步持久化等）。<br>
<b>正常值</b>：Active &lt; Total &lt; Max，Waiting = 0。线程池有一定余量。<br>
<b>异常特征</b>：Waiting &gt; 0（任务排队，处理能力不足）；Active 持续等于 Total（线程池满载）；Total 频繁伸缩（请求波动大）。<br>
<b>排查建议</b>：① Waiting &gt; 0：检查下游依赖（如磁盘 IO、网络、ETCD）是否变慢；② 线程池满载：评估是否需要调大 Max 参数；③ 客户端服务线程池满时，客户端请求会超时，检查 access.log 中 latency 是否升高。
</div>
<div id="tp_oc_chart" class="chart" style="height:380px"></div>
<div id="tp_ww_oc_chart" class="chart" style="height:380px"></div>
<div id="tp_mw_oc_chart" class="chart" style="height:380px"></div>
<div id="tp_m_oc_chart" class="chart" style="height:380px"></div>
<div id="tp_async_chart" class="chart" style="height:380px"></div>

<h2>6. ETCD Queue (ETCD 请求队列)</h2>
<div class="info">
<b>指标说明</b>：ETCD 请求队列大小。Avg = 平均队列深度，Max = 最大队列深度，Limit = 队列容量上限（红色虚线）。<br>
<b>正常值</b>：队列深度接近 0，远低于 Limit（80000）。<br>
<b>异常特征</b>：队列深度持续增长；接近或达到 Limit 的 50% 以上；队列积压不消化。<br>
<b>排查建议</b>：① 检查 ETCD 集群健康状态（etcdctl endpoint health）；② 检查 Worker 与 ETCD 之间的网络延迟；③ 检查 ETCD 磁盘 IO 是否成为瓶颈（etcd 需要 SSD）；④ 队列满时新请求会被拒绝，检查 access.log 中是否有 ETCD 相关错误。
</div>
<div id="etcd_chart" class="chart" style="height:380px"></div>

<h2>7. Active Clients (活跃客户端数)</h2>
<div class="info">
<b>指标说明</b>：当前连接到 Worker 的活跃客户端数量。Avg = 所有 Worker 平均值，Max = 单 Worker 最大值。<br>
<b>正常值</b>：客户端数稳定，与业务预期一致。各 Worker 连接数相对均匀。<br>
<b>异常特征</b>：客户端数骤降到 0（Worker 故障或网络中断）；客户端数异常飙升（连接泄漏或攻击）；个别 Worker 连接数远高于其他（热点）。<br>
<b>排查建议</b>：① 骤降：检查 Worker 进程是否存活、网络是否中断；② 飙升：检查客户端是否正确复用连接（而非频繁创建断开）；③ 热点：检查一致性哈希分配是否均匀。
</div>
<div id="clients_chart" class="chart"></div>

<h2>8. ETCD Request Success Rate (ETCD 请求成功率)</h2>
<div class="info">
<b>指标说明</b>：ETCD 请求的成功率，反映元数据服务的可用性。单位: %。<br>
<b>正常值</b>：100%。任何低于 100% 都意味着有请求失败。<br>
<b>异常特征</b>：成功率 &lt; 100%，出现下跌；持续低于 99%。<br>
<b>排查建议</b>：① 检查 ETCD 集群是否有节点故障（etcdctl member list）；② 检查 Worker 到 ETCD 的网络连通性和延迟；③ 检查 ETCD 是否触发空间配额限制；④ 查看 Worker 日志中 ETCD 相关错误信息。
</div>
<div id="success_rate_chart" class="chart"></div>

<h2>9. Per-Host Memory Usage (按 Host 查看内存)</h2>
<div style="margin:10px 0">
  <select id="host_select" onchange="updateHostChart()">
    <option value="__top10">Top 10 by Peak Memory</option>
    {host_options}
  </select>
</div>
<div id="per_host_chart" class="chart"></div>

<h2>10. Hourly Detail</h2>
<div class="detail-wrap">
<table>
<thead><tr>
  <th>Time</th><th>Hosts</th><th>Mem Avg</th><th>Mem Max</th><th>Mem%</th>
  <th>Obj Avg</th><th>Obj Max</th><th>Obj Size Avg</th>
  <th>Hit Rate</th><th>Hits</th><th>Requests</th>
  <th>OC TP%</th><th>OC Wait</th><th>Async TP%</th><th>Async Wait</th>
</tr></thead>
<tbody>{detail_rows}</tbody>
</table>
</div>

</div>

<script>
var timeLabels = __TIMELABELS__;
var memUsageAvg = __MEM_USAGE_AVG__;
var memUsageMax = __MEM_USAGE_MAX__;
var memLimit = __MEM_LIMIT__;
var memRatioAvg = __MEM_RATIO_AVG__;
var objCountAvg = __OBJ_COUNT_AVG__;
var objCountMax = __OBJ_COUNT_MAX__;
var objSizeAvg = __OBJ_SIZE_AVG__;
var cacheHitRate = __CACHE_HIT_RATE__;
var totalHits = __TOTAL_HITS__;
var totalRequests = __TOTAL_REQUESTS__;
var ocMiss = __OC_MISS__;
var ocMemHit = __OC_MEM_HIT__;
var ocDiskHit = __OC_DISK_HIT__;
var ocL2Hit = __OC_L2_HIT__;
var ocRemoteHit = __OC_REMOTE_HIT__;
var tpOcActive = __TP_OC_ACTIVE__;
var tpOcTotal = __TP_OC_TOTAL__;
var tpOcMax = __TP_OC_MAX__;
var tpOcWaiting = __TP_OC_WAITING__;
var tpWwOcActive = __TP_WW_OC_ACTIVE__;
var tpWwOcTotal = __TP_WW_OC_TOTAL__;
var tpWwOcMax = __TP_WW_OC_MAX__;
var tpWwOcWaiting = __TP_WW_OC_WAITING__;
var tpMwOcActive = __TP_MW_OC_ACTIVE__;
var tpMwOcTotal = __TP_MW_OC_TOTAL__;
var tpMwOcMax = __TP_MW_OC_MAX__;
var tpMwOcWaiting = __TP_MW_OC_WAITING__;
var tpMOcActive = __TP_M_OC_ACTIVE__;
var tpMOcTotal = __TP_M_OC_TOTAL__;
var tpMOcMax = __TP_M_OC_MAX__;
var tpMOcWaiting = __TP_M_OC_WAITING__;
var tpAsyncActive = __TP_ASYNC_ACTIVE__;
var tpAsyncTotal = __TP_ASYNC_TOTAL__;
var tpAsyncMax = __TP_ASYNC_MAX__;
var tpAsyncWaiting = __TP_ASYNC_WAITING__;
var etcdQSize = __ETCD_Q_SIZE__;
var etcdQMax = __ETCD_Q_MAX__;
var etcdQLimit = __ETCD_Q_LIMIT__;
var activeClientsAvg = __ACTIVE_CLIENTS_AVG__;
var activeClientsMax = __ACTIVE_CLIENTS_MAX__;
var etcdRate = __ETCD_RATE__;
var perHostMem = __PER_HOST_MEM__;

function safeArr(arr) {{
  return arr.map(function(v) {{ return v === "null" ? null : v; }});
}}

var colorPalette = ['#5470c6','#91cc75','#fac858','#ee6666','#73c0de','#3ba272','#fc8452','#9a60b4','#ea7ccc'];

// 1. Memory Trend
var c1 = echarts.init(document.getElementById('mem_chart'));
c1.setOption({{
  title: {{ text: 'Memory Usage Trend (GB)', left: 'center' }},
  tooltip: {{ trigger: 'axis' }},
  legend: {{ data: ['Avg Usage','Max Usage','Limit'], bottom: 0 }},
  grid: {{ left: 80, right: 40, top: 50, bottom: 50 }},
  xAxis: {{ type: 'category', data: timeLabels }},
  yAxis: {{ type: 'value', name: 'GB' }},
  series: [
    {{ name: 'Avg Usage', type: 'line', data: memUsageAvg, smooth: true, itemStyle: {{ color: '#5470c6' }} }},
    {{ name: 'Max Usage', type: 'line', data: memUsageMax, smooth: true, itemStyle: {{ color: '#ee6666' }},
      areaStyle: {{ color: 'rgba(238,102,102,0.1)' }} }},
    {{ name: 'Limit', type: 'line', data: memLimit, lineStyle: {{ type: 'dashed' }}, itemStyle: {{ color: '#999' }} }}
  ]
}});

// 2a. Memory Ratio
var c2a = echarts.init(document.getElementById('mem_ratio_chart'));
c2a.setOption({{
  title: {{ text: 'Memory Usage Ratio (%)', left: 'center' }},
  tooltip: {{ trigger: 'axis', formatter: function(p) {{ return p[0].name + '<br/>' + p[0].value + '%'; }} }},
  grid: {{ left: 60, right: 20, top: 50, bottom: 60 }},
  xAxis: {{ type: 'category', data: timeLabels, axisLabel: {{ rotate: 45 }} }},
  yAxis: {{ type: 'value', min: 0, max: 100, name: '%' }},
  series: [{{ type: 'bar', data: memRatioAvg, itemStyle: {{
    color: function(params) {{
      var v = params.value;
      return v > 80 ? '#ee6666' : v > 60 ? '#fac858' : '#91cc75';
    }}
  }} }}]
}});

// 2b. Memory distribution
var latestMem = {{}};
for (var h in perHostMem) {{
  var arr = perHostMem[h];
  if (arr.length > 0) latestMem[h] = arr[arr.length-1][1];
}}
var sortedHosts = Object.keys(latestMem).sort(function(a,b) {{ return latestMem[b]-latestMem[a]; }});
var allHosts = sortedHosts.slice();
var c2b = echarts.init(document.getElementById('mem_dist_chart'));
c2b.setOption({{
  title: {{ text: 'Memory per Host (All ' + sortedHosts.length + ' Hosts, Latest)', left: 'center' }},
  tooltip: {{ trigger: 'axis', formatter: function(p) {{ return p[0].name + ': ' + p[0].value + ' GB'; }} }},
  grid: {{ left: 130, right: 80, top: 50, bottom: 30 }},
  xAxis: {{ type: 'value', name: 'GB' }},
  yAxis: {{ type: 'category', data: allHosts.reverse(), axisLabel: {{ fontSize: 10 }} }},
  series: [{{ type: 'bar', data: allHosts.map(function(h) {{ return latestMem[h]; }}),
    label: {{ show: true, position: 'right', formatter: function(p) {{ return p.value + ' GB'; }}, fontSize: 10 }},
    itemStyle: {{
      color: function(params) {{
        var v = params.value;
        return v > 50 ? '#ee6666' : v > 30 ? '#fac858' : '#91cc75';
      }}
    }}
  }}]
}});

// 3a. Object Count
var c3a = echarts.init(document.getElementById('obj_count_chart'));
c3a.setOption({{
  title: {{ text: 'Object Count', left: 'center' }},
  tooltip: {{ trigger: 'axis' }},
  legend: {{ data: ['Avg/Host','Max'], bottom: 0 }},
  grid: {{ left: 70, right: 20, top: 50, bottom: 80 }},
  xAxis: {{ type: 'category', data: timeLabels, axisLabel: {{ rotate: 45 }} }},
  yAxis: {{ type: 'value' }},
  series: [
    {{ name: 'Avg/Host', type: 'line', data: objCountAvg, smooth: true }},
    {{ name: 'Max', type: 'line', data: objCountMax, smooth: true, lineStyle: {{ type: 'dashed' }} }}
  ]
}});

// 3b. Object Size
var c3b = echarts.init(document.getElementById('obj_size_chart'));
c3b.setOption({{
  title: {{ text: 'Object Size Avg (GB)', left: 'center' }},
  tooltip: {{ trigger: 'axis' }},
  grid: {{ left: 70, right: 20, top: 50, bottom: 60 }},
  xAxis: {{ type: 'category', data: timeLabels, axisLabel: {{ rotate: 45 }} }},
  yAxis: {{ type: 'value', name: 'GB' }},
  series: [{{ type: 'line', data: objSizeAvg, smooth: true, areaStyle: {{ color: 'rgba(84,112,198,0.2)' }} }}]
}});

// 4a. Cache Hit Rate
var c4a = echarts.init(document.getElementById('cache_hit_chart'));
c4a.setOption({{
  title: {{ text: 'Cache Hit Rate (%)', left: 'center' }},
  tooltip: {{ trigger: 'axis' }},
  legend: {{ data: ['Hit Rate','99% threshold'], bottom: 0 }},
  grid: {{ left: 60, right: 40, top: 50, bottom: 50 }},
  xAxis: {{ type: 'category', data: timeLabels }},
  yAxis: {{ type: 'value', min: 0, max: 100, name: '%' }},
  series: [{{ name: 'Hit Rate', type: 'line', data: safeArr(cacheHitRate), smooth: true,
    markLine: {{ data: [{{ yAxis: 99, name: '99%' }}] }},
    itemStyle: {{ color: '#91cc75' }}
  }}]
}});

// 4b. Cache hit detail (stacked bar)
var c4b = echarts.init(document.getElementById('cache_detail_chart'));
c4b.setOption({{
  title: {{ text: 'Cache Hit/Miss Distribution (Sum across Hosts)', left: 'center' }},
  tooltip: {{ trigger: 'axis' }},
  legend: {{ data: ['Mem Hit','Disk Hit','L2 Hit','Remote Hit','Miss'], bottom: 0 }},
  grid: {{ left: 80, right: 40, top: 50, bottom: 50 }},
  xAxis: {{ type: 'category', data: timeLabels }},
  yAxis: {{ type: 'value' }},
  series: [
    {{ name: 'Mem Hit', type: 'bar', stack: 'total', data: ocMemHit, itemStyle: {{ color: '#91cc75' }} }},
    {{ name: 'Disk Hit', type: 'bar', stack: 'total', data: ocDiskHit, itemStyle: {{ color: '#5470c6' }} }},
    {{ name: 'L2 Hit', type: 'bar', stack: 'total', data: ocL2Hit, itemStyle: {{ color: '#fac858' }} }},
    {{ name: 'Remote Hit', type: 'bar', stack: 'total', data: ocRemoteHit, itemStyle: {{ color: '#73c0de' }} }},
    {{ name: 'Miss', type: 'bar', stack: 'total', data: ocMiss, itemStyle: {{ color: '#ee6666' }} }}
  ]
}});

// 5. Thread pools
function tpChart(domId, title, active, total, max, waiting) {{
  var c = echarts.init(document.getElementById(domId));
  c.setOption({{
    title: {{ text: title, left: 'center' }},
    tooltip: {{ trigger: 'axis' }},
    legend: {{ data: ['Active','Total','Max','Waiting'], bottom: 0 }},
    grid: {{ left: 60, right: 20, top: 50, bottom: 80 }},
    xAxis: {{ type: 'category', data: timeLabels, axisLabel: {{ rotate: 45 }} }},
    yAxis: {{ type: 'value', name: 'Threads' }},
    series: [
      {{ name: 'Active', type: 'bar', data: active, itemStyle: {{ color: '#5470c6' }} }},
      {{ name: 'Total', type: 'line', data: total, lineStyle: {{ type: 'dashed' }}, itemStyle: {{ color: '#91cc75' }} }},
      {{ name: 'Max', type: 'line', data: max, lineStyle: {{ type: 'dotted' }}, itemStyle: {{ color: '#999' }} }},
      {{ name: 'Waiting', type: 'line', data: waiting, itemStyle: {{ color: '#ee6666' }} }}
    ]
  }});
  return c;
}}

var c5a = tpChart('tp_oc_chart', '对象缓存-客户端服务（处理客户端读写请求）', tpOcActive, tpOcTotal, tpOcMax, tpOcWaiting);
var c5b = tpChart('tp_ww_oc_chart', '对象缓存-Worker间通信（跨节点对象传输）', tpWwOcActive, tpWwOcTotal, tpWwOcMax, tpWwOcWaiting);
var c5c = tpChart('tp_mw_oc_chart', '对象缓存-Master下发（管理指令下发到Worker）', tpMwOcActive, tpMwOcTotal, tpMwOcMax, tpMwOcWaiting);
var c5d = tpChart('tp_m_oc_chart', '对象缓存-Master服务（Master端对象管理）', tpMOcActive, tpMOcTotal, tpMOcMax, tpMOcWaiting);
var c5e = tpChart('tp_async_chart', 'Master异步任务（后台异步操作）', tpAsyncActive, tpAsyncTotal, tpAsyncMax, tpAsyncWaiting);

// 6. ETCD Queue
var c6 = echarts.init(document.getElementById('etcd_chart'));
c6.setOption({{
  title: {{ text: 'ETCD Queue Size', left: 'center' }},
  tooltip: {{ trigger: 'axis' }},
  legend: {{ data: ['Avg','Max','Limit'], bottom: 0 }},
  grid: {{ left: 60, right: 20, top: 40, bottom: 90 }},
  xAxis: {{ type: 'category', data: timeLabels, axisLabel: {{ rotate: 45 }} }},
  yAxis: {{ type: 'value' }},
  series: [
    {{ name: 'Avg', type: 'line', data: etcdQSize, smooth: true }},
    {{ name: 'Max', type: 'line', data: etcdQMax, smooth: true, lineStyle: {{ type: 'dashed' }} }},
    {{ name: 'Limit', type: 'line', data: etcdQLimit, lineStyle: {{ type: 'dotted', color: '#ee6666' }},
      itemStyle: {{ color: '#ee6666' }}, symbol: 'none' }}
  ]
}});

// 7. Active Clients
var c7 = echarts.init(document.getElementById('clients_chart'));
c7.setOption({{
  title: {{ text: 'Active Clients', left: 'center' }},
  tooltip: {{ trigger: 'axis' }},
  legend: {{ data: ['Avg','Max'], bottom: 0 }},
  grid: {{ left: 60, right: 20, top: 50, bottom: 80 }},
  xAxis: {{ type: 'category', data: timeLabels, axisLabel: {{ rotate: 45 }} }},
  yAxis: {{ type: 'value' }},
  series: [
    {{ name: 'Avg', type: 'line', data: activeClientsAvg, smooth: true, itemStyle: {{ color: '#5470c6' }} }},
    {{ name: 'Max', type: 'line', data: activeClientsMax, smooth: true, lineStyle: {{ type: 'dashed' }}, itemStyle: {{ color: '#ee6666' }} }}
  ]
}});

// 8. ETCD Request Success Rate
var c8 = echarts.init(document.getElementById('success_rate_chart'));
c8.setOption({{
  title: {{ text: 'ETCD Request Success Rate (%)', left: 'center' }},
  tooltip: {{ trigger: 'axis' }},
  grid: {{ left: 60, right: 20, top: 50, bottom: 60 }},
  xAxis: {{ type: 'category', data: timeLabels, axisLabel: {{ rotate: 45 }} }},
  yAxis: {{ type: 'value', min: 0, max: 100, name: '%' }},
  series: [{{ type: 'line', data: safeArr(etcdRate), smooth: true, itemStyle: {{ color: '#5470c6' }} }}]
}});

// 9. Per-Host Memory
function updateHostChart() {{
  var sel = document.getElementById('host_select').value;
  var hosts, series = [];

  if (sel === '__top10') {{
    hosts = sortedHosts.slice(0, 10);
  }} else {{
    hosts = [sel];
  }}

  hosts.forEach(function(h, i) {{
    var data = perHostMem[h] || [];
    series.push({{
      name: h, type: 'line', smooth: true,
      data: data.map(function(d) {{ return d[1]; }}),
      itemStyle: {{ color: colorPalette[i % colorPalette.length] }}
    }});
  }});

  var xData = (perHostMem[hosts[0]] || []).map(function(d) {{ return d[0]; }});

  c9.setOption({{
    title: {{ text: 'Per-Host Memory Trend (GB)', left: 'center' }},
    tooltip: {{ trigger: 'axis' }},
    legend: {{ data: hosts, bottom: 0, type: 'scroll' }},
    grid: {{ left: 80, right: 40, top: 50, bottom: 90 }},
    xAxis: {{ type: 'category', data: xData, axisLabel: {{ rotate: 45 }} }},
    yAxis: {{ type: 'value', name: 'GB' }},
    series: series
  }});
}}

var c9 = echarts.init(document.getElementById('per_host_chart'));
updateHostChart();

// Responsive
window.addEventListener('resize', function() {{
  [c1,c2a,c2b,c3a,c3b,c4a,c4b,c5a,c5b,c5c,c5d,c5e,c6,c7,c8,c9].forEach(function(c) {{ c.resize(); }});
}});
</script>
</body>
</html>"""

    # Inject JS data via __PLACEHOLDER__ replacement
    def js_arr(lst):
        return "[" + ",".join(str(v) for v in lst) + "]"

    def js_str_arr(lst):
        return "[" + ",".join(f'"{v}"' for v in lst) + "]"

    replacements = {
        "__TIMELABELS__": js_str_arr(time_labels),
        "__MEM_USAGE_AVG__": js_arr(mem_usage_avg),
        "__MEM_USAGE_MAX__": js_arr(mem_usage_max),
        "__MEM_LIMIT__": js_arr(mem_limit),
        "__MEM_RATIO_AVG__": js_arr(mem_ratio_avg),
        "__OBJ_COUNT_AVG__": js_arr(obj_count_avg),
        "__OBJ_COUNT_MAX__": js_arr(obj_count_max),
        "__OBJ_SIZE_AVG__": js_arr(obj_size_avg),
        "__CACHE_HIT_RATE__": js_arr(cache_hit_rate),
        "__TOTAL_HITS__": js_arr(total_hits),
        "__TOTAL_REQUESTS__": js_arr(total_requests),
        "__OC_MISS__": js_arr(oc_miss),
        "__OC_MEM_HIT__": js_arr(oc_mem_hit),
        "__OC_DISK_HIT__": js_arr(oc_disk_hit),
        "__OC_L2_HIT__": js_arr(oc_l2_hit),
        "__OC_REMOTE_HIT__": js_arr(oc_remote_hit),
        "__TP_OC_ACTIVE__": js_arr(tp_oc_active),
        "__TP_OC_TOTAL__": js_arr(tp_oc_total),
        "__TP_OC_MAX__": js_arr(tp_oc_max),
        "__TP_OC_WAITING__": js_arr(tp_oc_waiting),
        "__TP_WW_OC_ACTIVE__": js_arr(tp_ww_oc_active),
        "__TP_WW_OC_TOTAL__": js_arr(tp_ww_oc_total),
        "__TP_WW_OC_MAX__": js_arr(tp_ww_oc_max),
        "__TP_WW_OC_WAITING__": js_arr(tp_ww_oc_waiting),
        "__TP_MW_OC_ACTIVE__": js_arr(tp_mw_oc_active),
        "__TP_MW_OC_TOTAL__": js_arr(tp_mw_oc_total),
        "__TP_MW_OC_MAX__": js_arr(tp_mw_oc_max),
        "__TP_MW_OC_WAITING__": js_arr(tp_mw_oc_waiting),
        "__TP_M_OC_ACTIVE__": js_arr(tp_m_oc_active),
        "__TP_M_OC_TOTAL__": js_arr(tp_m_oc_total),
        "__TP_M_OC_MAX__": js_arr(tp_m_oc_max),
        "__TP_M_OC_WAITING__": js_arr(tp_m_oc_waiting),
        "__TP_ASYNC_ACTIVE__": js_arr(tp_async_active),
        "__TP_ASYNC_TOTAL__": js_arr(tp_async_total),
        "__TP_ASYNC_MAX__": js_arr(tp_async_max),
        "__TP_ASYNC_WAITING__": js_arr(tp_async_waiting),
        "__ETCD_Q_SIZE__": js_arr(etcd_q_size),
        "__ETCD_Q_MAX__": js_arr(etcd_q_max),
        "__ETCD_Q_LIMIT__": js_arr(etcd_q_limit),
        "__ACTIVE_CLIENTS_AVG__": js_arr(active_clients_avg),
        "__ACTIVE_CLIENTS_MAX__": js_arr(active_clients_max),
        "__ETCD_RATE__": js_arr(etcd_rate),
        "__PER_HOST_MEM__": per_host_mem_js,
    }

    for placeholder, value in replacements.items():
        report_html = report_html.replace(placeholder, value)

    with open(output_path, "w", encoding="utf-8") as f:
        f.write(report_html)

    print(f"Report generated: {output_path} ({len(report_html)} bytes)")


def main():
    parser = argparse.ArgumentParser(
        description="Worker resource log analysis - generate interactive HTML report with ECharts",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=textwrap.dedent("""\
            directory layouts (auto-detected):
              Layout 1  <log_dir>/<host_ip>/logs/resource.log
                        e.g. /data/worker_logs/192.168.1.10/logs/resource.log
                             /data/worker_logs/192.168.1.11/logs/resource.log

              Layout 2  <log_dir>/<wrapper>/<host_ip>/logs/resource.log
                        e.g. /data/worker_logs/kvcache_logs_20260506/192.168.1.10/logs/resource.log

              Layout 3  <log_dir>/resource.log   (single host, flat)

            log_dir points to the root that contains one or more resource.log
            files.  The script walks subdirectories looking for files whose name
            contains --log-pattern (default "resource") and ends with .log.
            Host IP is extracted from the first directory named like an IP
            address (x.x.x.x) in the relative path.

            examples:
              # analyse last night's logs (default 1-hour bins)
              python3 generate_resource_report.py /data/worker_logs \\
                  --since "2026-05-05T22:30:00" -o report.html

              # narrower time range with 30-min bins
              python3 generate_resource_report.py /data/worker_logs \\
                  --since "2026-05-06T00:00:00" --until "2026-05-06T06:00:00" \\
                  --interval 30 -o report.html

              # WSL: write to Windows drive, auto-open in Edge
              python3 generate_resource_report.py /data/worker_logs \\
                  --since "2026-05-05T22:30:00" \\
                  -o /mnt/d/html/report.html --open

              # tarball-extracted logs (Layout 2)
              python3 generate_resource_report.py ./kvcache_logs_20260506 \\
                  --since "2026-05-05T00:00:00" -o report.html

            report sections:
              1. Memory Usage Trend        (cluster avg/max/limit)
              2. Memory Usage Ratio        (usage% + per-host distribution)
              3. Object Cache              (object count & size)
              4. Cache Hit Rate            (hit rate trend + hit/miss breakdown)
              5. Thread Pool Utilization   (5 pools: Active/Total/Max/Waiting)
              6. ETCD Queue                (queue depth vs limit)
              7. Active Clients            (connected client count)
              8. ETCD Success Rate         (ETCD request success rate)
              9. Per-Host Memory           (interactive per-host selector)
             10. Hourly Detail Table       (all metrics in tabular form)

            notes:
              - ECharts is loaded from CDN (jsdelivr.net); report needs internet.
              - OC_HIT_NUM is a cumulative counter; per-bin deltas are computed.
              - Thread pool "Active" = Total - Idle (actual working threads).
        """)
    )
    parser.add_argument(
        "log_dir",
        help="root directory containing worker resource.log files (see directory layouts above)"
    )
    parser.add_argument(
        "--since", required=True,
        help="start time (inclusive), format: YYYY-MM-DDTHH:MM:SS"
    )
    parser.add_argument(
        "--until",
        help="end time (exclusive), format: YYYY-MM-DDTHH:MM:SS (default: end of logs)"
    )
    parser.add_argument(
        "-o", "--output", default="resource_report.html",
        help="output HTML file path (default: resource_report.html)"
    )
    parser.add_argument(
        "--log-pattern", default="resource",
        help="filename substring to match log files (default: resource)"
    )
    parser.add_argument(
        "--interval", type=int, default=60,
        help="time bin size in minutes; smaller = finer chart granularity (default: 60)"
    )
    parser.add_argument(
        "--open", action="store_true",
        help="open report in browser after generation (WSL: opens Edge via file:///)"
    )
    args = parser.parse_args()

    print(f"Scanning {args.log_dir} for {args.log_pattern}*.log...")
    log_files = _find_log_files(args.log_dir, args.log_pattern)
    if not log_files:
        print(f"No {args.log_pattern} log files found in {args.log_dir}")
        sys.exit(1)

    total_files = sum(len(v) for v in log_files.values())
    print(f"Found {total_files} files across {len(log_files)} hosts")

    print("Parsing logs...")
    all_data = parse_resource_logs(log_files, args.since, args.until)

    print("Computing cluster statistics...")
    cluster_stats = compute_cluster_stats(all_data, args.interval)

    print("Computing per-host statistics...")
    per_host_stats = compute_per_host_stats(all_data, args.interval)

    print("Generating HTML report...")
    generate_html(cluster_stats, per_host_stats, all_data, args.output, args.since, args.until)

    if args.open:
        out_path = os.path.abspath(args.output)
        if out_path.startswith("/mnt/"):
            # WSL: convert /mnt/d/html/... to file:///D:/html/...
            drive_letter = out_path[5].upper()
            rest = out_path[6:]
            file_url = f"file:///{drive_letter}:{rest}"
            subprocess.run(["cmd.exe", "/c", "start", "msedge", file_url])
        else:
            subprocess.run(["xdg-open", out_path])


if __name__ == "__main__":
    main()
