#!/usr/bin/env python3
"""KVCache Access Log Analysis - Generate interactive HTML report.

Reads KVCache access logs (client or worker), computes per-hour per-API statistics,
and generates a self-contained HTML file with ECharts interactive charts.

Usage:
    python3 generate_access_log_report.py <log_dir> --since "YYYY-MM-DDTHH:MM:SS"

Directory layouts (auto-detected):
    Layout 1:  <log_dir>/<host_ip>/logs/access.log
    Layout 2:  <log_dir>/<wrapper>/<host_ip>/logs/access.log
    Layout 3:  <log_dir>/access.log

Arguments:
    log_dir             Root directory containing access log files
    --since             Start time (inclusive), format: YYYY-MM-DDTHH:MM:SS
    --until             End time (exclusive), format: YYYY-MM-DDTHH:MM:SS
    -o / --output       Output HTML file path (default: kvcache_log_analysis.html)
    --log-pattern       Filename substring to match (default: access)
    --open              Open report in browser after generation

Report Sections:
    Summary Cards       Total requests, peak QPS, errors, success rate
    QPS Trend           Per-hour QPS per API (count / actual time span)
    Latency Trends      Avg / P90 / P99 / P99.99 / Max per API per hour
    Error Count         Per-hour error count per API (bar chart)
    Error Distribution  Error code distribution (pie chart)
    Latency Distribution Per-API latency distribution (grouped bar chart)
    Overall Summary     Per-API aggregate stats table
    Latency Distribution Detail  Per-hour per-API detail table
    Hourly Detail       Per-hour per-API detail with QPS and success rate
"""

import argparse
import html
import json
import os
import re
import statistics
import subprocess
import sys
import textwrap
from collections import defaultdict
from datetime import datetime


def _find_log_files(log_dir, pattern="access"):
    """Find access log files supporting 3 directory layouts."""
    log_files = {}
    for root, dirs, files in os.walk(log_dir, followlinks=False):
        for f in files:
            if pattern in f and f.endswith(".log") and not f.endswith(".gz"):
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


def parse_records(log_files, since_str, until_str=None):
    """Parse access log files and return records with timestamps."""
    since = since_str[:19]
    until = until_str[:19] if until_str else None
    records = []
    hour_min_sec = defaultdict(lambda: float("inf"))
    hour_max_sec = defaultdict(lambda: 0.0)
    skipped = 0

    for host, files in log_files.items():
        for fpath in files:
            with open(fpath, "r", errors="replace") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    ts_str = line[:26]
                    ts_short = ts_str[:19]
                    if ts_short < since:
                        continue
                    if until and ts_short >= until:
                        continue
                    parts = line.split(" | ")
                    if len(parts) < 11:
                        skipped += 1
                        continue
                    try:
                        retcode = int(parts[7].strip())
                        api = parts[8].strip()
                        latency = int(parts[9].strip())
                        data_size = int(parts[10].strip())
                    except (ValueError, IndexError):
                        skipped += 1
                        continue

                    hour_str = ts_str[:13]
                    try:
                        ts_sec = datetime.strptime(ts_short, "%Y-%m-%dT%H:%M:%S").timestamp()
                    except ValueError:
                        skipped += 1
                        continue

                    records.append((hour_str, api, retcode, latency, data_size))
                    hour_min_sec[hour_str] = min(hour_min_sec[hour_str], ts_sec)
                    hour_max_sec[hour_str] = max(hour_max_sec[hour_str], ts_sec)

    if skipped:
        print(f"Skipped {skipped} malformed lines")

    hour_span_sec = {}
    for h in hour_min_sec:
        span = hour_max_sec[h] - hour_min_sec[h]
        hour_span_sec[h] = max(span, 1.0)

    return records, hour_span_sec


def compute_stats(records, hour_span_sec):
    """Compute per-hour per-API statistics."""
    groups = defaultdict(list)
    for hour_str, api, retcode, latency, data_size in records:
        groups[(hour_str, api)].append((retcode, latency, data_size))

    hours = sorted(set(h for h, _ in groups.keys()))
    apis = sorted(set(a for _, a in groups.keys()))
    hour_labels = [h.replace("T", " ") + ":00" for h in hours]

    def _percentile(sorted_vals, pct):
        n = len(sorted_vals)
        return sorted_vals[min(int(n * pct), n - 1)]

    qps_data = {api: [] for api in apis}
    avg_data = {api: [] for api in apis}
    p90_data = {api: [] for api in apis}
    p99_data = {api: [] for api in apis}
    p9999_data = {api: [] for api in apis}
    min_data = {api: [] for api in apis}
    max_data = {api: [] for api in apis}
    err_data = {api: [] for api in apis}

    table_rows = []
    for hour in hours:
        span = hour_span_sec.get(hour, 3600.0)
        for api in apis:
            items = groups.get((hour, api), [])
            n = len(items)
            if n == 0:
                for d in [qps_data, avg_data, p90_data, p99_data, p9999_data, min_data, max_data, err_data]:
                    d[api].append(None)
                continue

            latencies = sorted([l for _, l, _ in items])
            data_sizes = [s for _, _, s in items]

            qps = round(n / span, 1)
            avg_lat = round(statistics.mean(latencies), 1)
            p90_lat = _percentile(latencies, 0.90)
            p99_lat = _percentile(latencies, 0.99)
            p9999_lat = _percentile(latencies, 0.9999)
            min_lat = latencies[0]
            max_lat = latencies[-1]
            errors = sum(1 for r, _, _ in items if r != 0)
            succ_rate = round((n - errors) / n * 100, 4)

            qps_data[api].append(qps)
            avg_data[api].append(round(avg_lat / 1000, 2))
            p90_data[api].append(round(p90_lat / 1000, 2))
            p99_data[api].append(round(p99_lat / 1000, 2))
            p9999_data[api].append(round(p9999_lat / 1000, 2))
            min_data[api].append(round(min_lat / 1000, 2))
            max_data[api].append(round(max_lat / 1000, 2))
            err_data[api].append(errors)

            table_rows.append({
                "hour": hour.replace("T", " ") + ":00",
                "api": api,
                "count": n,
                "qps": qps,
                "avg": round(avg_lat / 1000, 2),
                "p90": round(p90_lat / 1000, 2),
                "p99": round(p99_lat / 1000, 2),
                "p9999": round(p9999_lat / 1000, 2),
                "min": round(min_lat / 1000, 2),
                "max": round(max_lat / 1000, 2),
                "errors": errors,
                "succ": succ_rate,
            })

    return {
        "hours": hours, "apis": apis, "hour_labels": hour_labels,
        "qps_data": qps_data, "avg_data": avg_data,
        "p90_data": p90_data, "p99_data": p99_data, "p9999_data": p9999_data,
        "min_data": min_data, "max_data": max_data, "err_data": err_data,
        "table_rows": table_rows,
    }


def compute_overall(records):
    """Compute overall per-API aggregate statistics."""
    total_by_api = defaultdict(lambda: {"count": 0, "errors": 0, "latencies": [], "sizes": []})
    for _, api, retcode, latency, data_size in records:
        total_by_api[api]["count"] += 1
        if retcode != 0:
            total_by_api[api]["errors"] += 1
        total_by_api[api]["latencies"].append(latency)
        total_by_api[api]["sizes"].append(data_size)

    summary_rows = []
    for api in sorted(total_by_api.keys()):
        d = total_by_api[api]
        n = d["count"]
        lats = sorted(d["latencies"])
        sizes = d["sizes"]
        summary_rows.append({
            "api": api,
            "total": n,
            "errors": d["errors"],
            "succ": round((n - d["errors"]) / n * 100, 4) if n else 0,
            "avg": round(statistics.mean(lats) / 1000, 2) if lats else 0,
            "p90": round(lats[min(int(n * 0.90), n - 1)] / 1000, 2) if lats else 0,
            "p99": round(lats[min(int(n * 0.99), n - 1)] / 1000, 2) if lats else 0,
            "p9999": round(lats[min(int(n * 0.9999), n - 1)] / 1000, 2) if lats else 0,
            "min": round(lats[0] / 1000, 2) if lats else 0,
            "max": round(lats[-1] / 1000, 2) if lats else 0,
            "avg_size": round(statistics.mean(sizes), 0) if sizes else 0,
            "max_size": max(sizes) if sizes else 0,
        })

    # Error code distribution
    api_err_codes = defaultdict(lambda: defaultdict(int))
    for _, api, retcode, _, _ in records:
        if retcode != 0:
            api_err_codes[api][retcode] += 1

    err_dist_labels = []
    err_dist_values = []
    for api in sorted(api_err_codes.keys()):
        for rc in sorted(api_err_codes[api].keys()):
            err_dist_labels.append(f"{html.escape(api)}<br>retcode={rc}")
            err_dist_values.append(api_err_codes[api][rc])

    total_errors = sum(d["errors"] for d in total_by_api.values())
    overall_succ = (1 - total_errors / len(records)) * 100 if records else 100

    return {
        "summary_rows": summary_rows,
        "err_dist_labels": err_dist_labels,
        "err_dist_values": err_dist_values,
        "total_errors": total_errors,
        "overall_succ": overall_succ,
        "total_by_api": total_by_api,
    }


def _fmt_size(b):
    if b == 0:
        return "0"
    for unit in ["B", "KB", "MB", "GB"]:
        if b < 1024:
            return f"{b:.1f} {unit}"
        b /= 1024
    return f"{b:.1f} TB"


def _badge_class(succ):
    if succ >= 99.99:
        return "badge-ok"
    elif succ >= 99.9:
        return "badge-warn"
    return "badge-err"


API_COLORS = {
    "DS_KV_CLIENT_GET": "#5470c6",
    "DS_KV_CLIENT_EXIST": "#91cc75",
    "DS_KV_CLIENT_SET": "#fac858",
    "DS_KV_CLIENT_CREATE": "#ee6666",
}


def _make_series(api, data_map, chart_type="line"):
    return {
        "name": api.replace("DS_KV_CLIENT_", ""),
        "type": chart_type,
        "data": data_map[api],
        "smooth": True,
        "itemStyle": {"color": API_COLORS.get(api, "#999")},
        "lineStyle": {"width": 2},
    }


def generate_html(stats, overall, host_count, output_path, since_str, until_str):
    """Generate self-contained HTML report with ECharts."""
    hours = stats["hours"]
    apis = stats["apis"]
    hour_labels = stats["hour_labels"]

    if not hour_labels:
        print("No data to report")
        sys.exit(1)

    # Pre-compute JSON for charts
    hour_labels_json = json.dumps(hour_labels)
    qps_series = json.dumps([_make_series(a, stats["qps_data"]) for a in apis])
    avg_series = json.dumps([_make_series(a, stats["avg_data"]) for a in apis])
    p90_series = json.dumps([_make_series(a, stats["p90_data"]) for a in apis])
    p99_series = json.dumps([_make_series(a, stats["p99_data"]) for a in apis])
    p9999_series = json.dumps([_make_series(a, stats["p9999_data"]) for a in apis])
    max_series = json.dumps([_make_series(a, stats["max_data"]) for a in apis])
    err_series = json.dumps([_make_series(a, stats["err_data"], "bar") for a in apis])
    err_dist_json = json.dumps([
        dict(name=l, value=v)
        for l, v in zip(overall["err_dist_labels"], overall["err_dist_values"])
    ])

    # Latency distribution bar chart
    lat_dist_parts = []
    for api in apis:
        api_name = api.replace("DS_KV_CLIENT_", "")
        for cat, src, opacity in [
            ("Min", stats["min_data"], 0.4), ("Avg", stats["avg_data"], 0.6),
            ("P90", stats["p90_data"], 0.75), ("P99", stats["p99_data"], 0.85),
            ("P99.99", stats["p9999_data"], 0.95), ("Max", stats["max_data"], 1.0),
        ]:
            lat_dist_parts.append({
                "name": f"{api_name} {cat}",
                "type": "bar",
                "data": src[api],
                "itemStyle": {"color": API_COLORS.get(api, "#999"), "opacity": opacity},
            })
    lat_dist_series = json.dumps(lat_dist_parts)

    # Peak QPS for summary cards
    def _peak_qps(api_name):
        vals = stats["qps_data"].get(api_name, [])
        non_none = [v for v in vals if v is not None]
        if not non_none:
            return 0, "N/A"
        peak = max(non_none)
        idx = [i for i, v in enumerate(vals) if v == peak][0]
        return peak, hour_labels[idx]

    peak_exist, peak_exist_hour = _peak_qps("DS_KV_CLIENT_EXIST")
    peak_get, peak_get_hour = _peak_qps("DS_KV_CLIENT_GET")

    # Summary table
    def _build_summary_table(rows):
        parts = ['<table>\n<tr><th>API</th><th class="num">Total</th><th class="num">Errors</th>',
                 '<th class="num">Success Rate</th>',
                 '<th class="num">Avg (ms)</th><th class="num">P90 (ms)</th><th class="num">P99 (ms)</th><th class="num">P99.99 (ms)</th>',
                 '<th class="num">Min (ms)</th><th class="num">Max (ms)</th>',
                 '<th class="num">Avg Size</th><th class="num">Max Size</th></tr>\n']
        for r in rows:
            parts.append(f'<tr><td><b>{html.escape(r["api"])}</b></td>')
            parts.append(f'<td class="num">{r["total"]:,}</td>')
            parts.append(f'<td class="num">{r["errors"]}</td>')
            parts.append(f'<td class="num"><span class="badge {_badge_class(r["succ"])}">{r["succ"]:.4f}%</span></td>')
            parts.append(f'<td class="num">{r["avg"]}</td><td class="num">{r["p90"]}</td>')
            parts.append(f'<td class="num">{r["p99"]}</td><td class="num">{r["p9999"]}</td>')
            parts.append(f'<td class="num">{r["min"]}</td><td class="num">{r["max"]}</td>')
            parts.append(f'<td class="num">{_fmt_size(r["avg_size"])}</td><td class="num">{_fmt_size(r["max_size"])}</td>')
            parts.append('</tr>\n')
        parts.append('</table>')
        return "".join(parts)

    def _build_detail_table(rows):
        parts = ['<table>\n<tr><th>Hour</th><th>API</th><th class="num">Count</th><th class="num">QPS</th>',
                 '<th class="num">Avg (ms)</th><th class="num">P90 (ms)</th><th class="num">P99 (ms)</th><th class="num">P99.99 (ms)</th>',
                 '<th class="num">Min (ms)</th><th class="num">Max (ms)</th>',
                 '<th class="num">Errors</th><th class="num">Success Rate</th></tr>\n']
        for r in rows:
            parts.append(f'<tr><td>{html.escape(r["hour"])}</td>')
            parts.append(f'<td>{html.escape(r["api"].replace("DS_KV_CLIENT_", ""))}</td>')
            parts.append(f'<td class="num">{r["count"]:,}</td><td class="num">{r["qps"]}</td>')
            parts.append(f'<td class="num">{r["avg"]}</td><td class="num">{r["p90"]}</td>')
            parts.append(f'<td class="num">{r["p99"]}</td><td class="num">{r["p9999"]}</td>')
            parts.append(f'<td class="num">{r["min"]}</td><td class="num">{r["max"]}</td>')
            parts.append(f'<td class="num">{r["errors"]}</td>')
            parts.append(f'<td class="num"><span class="badge {_badge_class(r["succ"])}">{r["succ"]:.4f}%</span></td>')
            parts.append('</tr>\n')
        parts.append('</table>')
        return "".join(parts)

    summary_html = _build_summary_table(overall["summary_rows"])
    detail_html = _build_detail_table(stats["table_rows"])

    # Build report
    time_range = f"{hour_labels[0]} ~ {hour_labels[-1]}"
    total_records = sum(d["count"] for d in overall["total_by_api"].values())

    report_html = f"""<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>KVCache Access Log Analysis</title>
<script src="https://cdn.jsdelivr.net/npm/echarts@5.5.0/dist/echarts.min.js"></script>
<style>
  * {{ margin: 0; padding: 0; box-sizing: border-box; }}
  body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; background: #f5f7fa; color: #333; }}
  .header {{ background: linear-gradient(135deg, #1a1a2e 0%, #16213e 100%); color: white; padding: 30px 40px; }}
  .header h1 {{ font-size: 28px; font-weight: 600; }}
  .header .meta {{ margin-top: 8px; font-size: 14px; opacity: 0.8; }}
  .container {{ max-width: 1400px; margin: 0 auto; padding: 24px; }}
  .summary-cards {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(240px, 1fr)); gap: 16px; margin-bottom: 24px; }}
  .card {{ background: white; border-radius: 12px; padding: 20px; box-shadow: 0 2px 8px rgba(0,0,0,0.06); }}
  .card .label {{ font-size: 13px; color: #999; margin-bottom: 8px; }}
  .card .value {{ font-size: 28px; font-weight: 700; color: #1a1a2e; }}
  .card .sub {{ font-size: 12px; color: #666; margin-top: 4px; }}
  .chart-grid {{ display: grid; grid-template-columns: 1fr 1fr; gap: 20px; margin-bottom: 24px; }}
  .chart-box {{ background: white; border-radius: 12px; padding: 20px; box-shadow: 0 2px 8px rgba(0,0,0,0.06); }}
  .chart-box.full {{ grid-column: 1 / -1; }}
  .chart-box h3 {{ font-size: 16px; font-weight: 600; margin-bottom: 12px; color: #1a1a2e; }}
  .chart-container {{ width: 100%; height: 380px; }}
  .chart-container.tall {{ height: 440px; }}
  table {{ width: 100%; border-collapse: collapse; font-size: 13px; }}
  table th {{ background: #1a1a2e; color: white; padding: 10px 12px; text-align: left; position: sticky; top: 0; z-index:1; }}
  table td {{ padding: 8px 12px; border-bottom: 1px solid #eee; }}
  table tr:hover {{ background: #f0f4ff; }}
  table .num {{ text-align: right; font-variant-numeric: tabular-nums; }}
  .badge {{ display: inline-block; padding: 2px 8px; border-radius: 10px; font-size: 11px; font-weight: 600; }}
  .badge-ok {{ background: #e8f5e9; color: #2e7d32; }}
  .badge-warn {{ background: #fff3e0; color: #ef6c00; }}
  .badge-err {{ background: #ffebee; color: #c62828; }}
  .section-title {{ font-size: 20px; font-weight: 600; margin: 32px 0 16px; color: #1a1a2e; }}
  .table-wrap {{ background: white; border-radius: 12px; box-shadow: 0 2px 8px rgba(0,0,0,0.06); overflow: auto; max-height: 600px; }}
  .note {{ font-size: 12px; color: #999; margin-top: 4px; font-style: italic; }}
</style>
</head>
<body>

<div class="header">
  <h1>KVCache Access Log Analysis Report</h1>
  <div class="meta">
    Time range: {time_range} | Records: {total_records:,} | Client hosts: {host_count} | Generated: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
  </div>
</div>

<div class="container">

<!-- Summary Cards -->
<div class="summary-cards">
  <div class="card">
    <div class="label">Total Requests</div>
    <div class="value">{total_records:,}</div>
    <div class="sub">{len(hours)} hours covered</div>
  </div>
  <div class="card">
    <div class="label">Peak QPS (EXIST)</div>
    <div class="value">{peak_exist:.1f}</div>
    <div class="sub">at {peak_exist_hour}</div>
  </div>
  <div class="card">
    <div class="label">Peak QPS (GET)</div>
    <div class="value">{peak_get:.1f}</div>
    <div class="sub">at {peak_get_hour}</div>
  </div>
  <div class="card">
    <div class="label">Total Errors</div>
    <div class="value">{overall["total_errors"]:,}</div>
    <div class="sub">Overall success rate: {overall["overall_succ"]:.4f}%</div>
  </div>
</div>

<!-- QPS Trend -->
<div class="chart-grid">
  <div class="chart-box full">
    <h3>QPS Trend (per hour)<span class="note"> &mdash; QPS = count / actual time span within each hour</span></h3>
    <div id="chart-qps" class="chart-container"></div>
  </div>
</div>

<!-- Latency Trends -->
<div class="chart-grid">
  <div class="chart-box">
    <h3>Average Latency Trend (ms)</h3>
    <div id="chart-avg" class="chart-container"></div>
  </div>
  <div class="chart-box">
    <h3>P90 Latency Trend (ms)</h3>
    <div id="chart-p90" class="chart-container"></div>
  </div>
  <div class="chart-box">
    <h3>P99 Latency Trend (ms)</h3>
    <div id="chart-p99" class="chart-container"></div>
  </div>
  <div class="chart-box">
    <h3>P99.99 Latency Trend (ms)</h3>
    <div id="chart-p9999" class="chart-container"></div>
  </div>
  <div class="chart-box">
    <h3>Max Latency Trend (ms)</h3>
    <div id="chart-max" class="chart-container"></div>
  </div>
  <div class="chart-box">
    <h3>Error Count (per hour)</h3>
    <div id="chart-errors" class="chart-container"></div>
  </div>
</div>

<!-- Error Distribution -->
<div class="chart-grid">
  <div class="chart-box full">
    <h3>Error Code Distribution</h3>
    <div id="chart-err-dist" class="chart-container" style="height:300px;"></div>
  </div>
</div>

<!-- Latency Distribution Chart -->
<div class="chart-grid">
  <div class="chart-box full">
    <h3>Latency Distribution per API (Min / Avg / P90 / P99 / P99.99 / Max)</h3>
    <div id="chart-lat-dist" class="chart-container tall"></div>
  </div>
</div>

<!-- Overall Summary Table -->
<div class="section-title">Overall Summary by API</div>
<div class="table-wrap" style="max-height:none; margin-bottom:24px;">
  {summary_html}
</div>

<!-- Hourly Detail Table -->
<div class="section-title">Hourly Detail Table</div>
<div class="table-wrap">
  {detail_html}
</div>

</div>

<script>
var baseOpt = {{
  tooltip: {{ trigger: 'axis', confine: true }},
  legend: {{ top: 0, textStyle: {{ fontSize: 12 }} }},
  grid: {{ top: 50, left: 60, right: 30, bottom: 40 }},
  xAxis: {{ type: 'category', data: {hour_labels_json}, axisLabel: {{ rotate: 30, fontSize: 11 }} }},
  yAxis: {{ type: 'value', axisLabel: {{ fontSize: 11 }} }},
}};

function mergeOpt(overrides) {{ return Object.assign({{}}, baseOpt, overrides); }}

// QPS
echarts.init(document.getElementById('chart-qps')).setOption(mergeOpt({{
  yAxis: {{ type: 'value', name: 'QPS', axisLabel: {{ fontSize: 11 }} }},
  series: {qps_series}
}}));

// Avg latency
echarts.init(document.getElementById('chart-avg')).setOption(mergeOpt({{
  yAxis: {{ type: 'value', name: 'ms', axisLabel: {{ fontSize: 11 }} }},
  series: {avg_series}
}}));

// P90 latency
echarts.init(document.getElementById('chart-p90')).setOption(mergeOpt({{
  yAxis: {{ type: 'value', name: 'ms', axisLabel: {{ fontSize: 11 }} }},
  series: {p90_series}
}}));

// P99 latency
echarts.init(document.getElementById('chart-p99')).setOption(mergeOpt({{
  yAxis: {{ type: 'value', name: 'ms', axisLabel: {{ fontSize: 11 }} }},
  series: {p99_series}
}}));

// P99.99 latency
echarts.init(document.getElementById('chart-p9999')).setOption(mergeOpt({{
  yAxis: {{ type: 'value', name: 'ms', axisLabel: {{ fontSize: 11 }} }},
  series: {p9999_series}
}}));

// Max latency
echarts.init(document.getElementById('chart-max')).setOption(mergeOpt({{
  yAxis: {{ type: 'value', name: 'ms', axisLabel: {{ fontSize: 11 }} }},
  series: {max_series}
}}));

// Errors
echarts.init(document.getElementById('chart-errors')).setOption(mergeOpt({{
  yAxis: {{ type: 'value', name: 'count', minInterval: 1, axisLabel: {{ fontSize: 11 }} }},
  series: {err_series}
}}));

// Error code distribution
echarts.init(document.getElementById('chart-err-dist')).setOption({{
  tooltip: {{ trigger: 'item', confine: true }},
  series: [{{
    type: 'pie',
    radius: ['35%', '65%'],
    center: ['50%', '55%'],
    label: {{ fontSize: 11, formatter: '{{b}}\\n{{c}} ({{d}}%)' }},
    data: {err_dist_json}
  }}]
}});

// Latency distribution bar
echarts.init(document.getElementById('chart-lat-dist')).setOption(mergeOpt({{
  legend: {{ type: 'scroll', top: 0, textStyle: {{ fontSize: 11 }} }},
  grid: {{ top: 60, left: 60, right: 30, bottom: 40 }},
  tooltip: {{ trigger: 'axis', confine: true }},
  yAxis: {{ type: 'value', name: 'ms', axisLabel: {{ fontSize: 11 }} }},
  series: {lat_dist_series}
}}));

// Responsive resize
window.addEventListener('resize', function() {{
  document.querySelectorAll('.chart-container').forEach(function(el) {{
    var chart = echarts.getInstanceByDom(el);
    if (chart) chart.resize();
  }});
}});
</script>
</body>
</html>"""

    os.makedirs(os.path.dirname(os.path.abspath(output_path)), exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(report_html)

    print(f"Report generated: {output_path} ({len(report_html)} bytes)")


def main():
    parser = argparse.ArgumentParser(
        description="KVCache Access Log Analysis - Generate interactive HTML report with ECharts",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=textwrap.dedent("""\
            directory layouts (auto-detected):
              Layout 1  <log_dir>/<host_ip>/logs/access.log
              Layout 2  <log_dir>/<wrapper>/<host_ip>/logs/access.log
              Layout 3  <log_dir>/access.log   (single host, flat)

            examples:
              python3 generate_access_log_report.py /data/client_logs \\
                  --since "2026-05-05T22:30:00" -o report.html

              python3 generate_access_log_report.py /data/client_logs \\
                  --since "2026-05-05T22:30:00" --until "2026-05-06T08:00:00" \\
                  -o report.html --open

              # WSL: output to Windows drive and open in Edge
              python3 generate_access_log_report.py /data/client_logs \\
                  --since "2026-05-05T22:30:00" \\
                  -o /mnt/d/html/report.html --open

            report sections:
              Summary Cards           Total requests, peak QPS, errors
              QPS Trend               Per-hour QPS per API
              Latency Trends          Avg / P90 / P99 / P99.99 / Max
              Error Count             Per-hour error count (bar chart)
              Error Distribution      Error code pie chart
              Latency Distribution    Per-API latency grouped bars
              Overall Summary         Per-API aggregate stats table
              Hourly Detail           Per-hour per-API detail table

            notes:
              - ECharts is loaded from CDN (jsdelivr.net); report needs internet.
              - QPS = count / actual time span within each hour (handles partial hours).
        """),
    )
    parser.add_argument(
        "log_dir",
        help="root directory containing access log files (see directory layouts above)",
    )
    parser.add_argument(
        "--since", required=True,
        help='start time (inclusive), format: "YYYY-MM-DDTHH:MM:SS"',
    )
    parser.add_argument(
        "--until",
        help='end time (exclusive), format: "YYYY-MM-DDTHH:MM:SS" (default: end of logs)',
    )
    parser.add_argument(
        "-o", "--output", default="kvcache_log_analysis.html",
        help="output HTML file path (default: kvcache_log_analysis.html)",
    )
    parser.add_argument(
        "--log-pattern", default="access",
        help="filename substring to match log files (default: access)",
    )
    parser.add_argument(
        "--open", action="store_true",
        help="open report in browser after generation (WSL: opens Edge via file:///)",
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
    records, hour_span_sec = parse_records(log_files, args.since, args.until)
    print(f"Parsed {len(records)} records")

    if not records:
        print("No records matched the time range")
        sys.exit(1)

    print("Computing statistics...")
    stats = compute_stats(records, hour_span_sec)
    overall = compute_overall(records)

    print("Generating HTML report...")
    generate_html(stats, overall, len(log_files), args.output, args.since, args.until)

    if args.open:
        output_path = os.path.abspath(args.output)
        try:
            result = subprocess.run(
                ["wslpath", "-w", output_path], capture_output=True, text=True
            )
            if result.returncode == 0:
                win_path = result.stdout.strip()
                url = f"file:///{win_path.replace(chr(92), '/')}"
                subprocess.Popen(["cmd.exe", "/c", "start", "ms-edge", url])
            else:
                subprocess.Popen(["xdg-open", output_path])
        except FileNotFoundError:
            subprocess.Popen(["xdg-open", output_path])


if __name__ == "__main__":
    main()
