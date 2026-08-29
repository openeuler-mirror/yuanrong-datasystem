#!/usr/bin/env python3
# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Create per-worker API and emitted URMA completion-log QPS HTML reports.

API QPS is calculated from worker access-log records selected by ``--type``.
URMA write QPS is optional (``--urma-write``) and is calculated from the
timestamps of emitted ``[URMA_ELAPSED_TOTAL]`` log lines. Those lines can be
sampled by server logging configuration, so the resulting metric is explicitly
an emitted-log QPS, not necessarily the complete URMA write completion QPS.
"""

import argparse
import gzip
import html
import math
import os
import sys
import time
import uuid
import re
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, wait
from datetime import datetime, timezone
from pathlib import Path
import plotly.graph_objects as go


def parse_timestamp(ts_str):
    if "." in ts_str:
        base, frac = ts_str.split(".")
        frac = frac.ljust(6, "0")[:6]
        ts_str = base + "." + frac
        return datetime.strptime(ts_str, "%Y-%m-%dT%H:%M:%S.%f").replace(tzinfo=timezone.utc)
    return datetime.strptime(ts_str, "%Y-%m-%dT%H:%M:%S").replace(tzinfo=timezone.utc)


def parse_access_line(line):
    line = line.strip()
    if not line:
        return None
    parts = [part.strip() for part in line.split("|")]
    if len(parts) < 10:
        return None
    try:
        return parse_timestamp(parts[0]), parts[8]
    except ValueError:
        return None


ISO_TIMESTAMP_RE = re.compile(r"(?P<timestamp>\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,6})?)")
GLOG_TIMESTAMP_RE = re.compile(
    r"^[IWEF](?P<month>\d{2})(?P<day>\d{2})\s+(?P<hour>\d{2}):(?P<minute>\d{2}):"
    r"(?P<second>\d{2})(?:\.(?P<microsecond>\d{1,6}))?\s+\d+\s+"
)


def parse_urma_elapsed_line(line):
    """Return the timestamp of an emitted URMA completion log line."""
    if "[URMA_ELAPSED_TOTAL]" not in line:
        return None
    match = ISO_TIMESTAMP_RE.search(line)
    if match is not None:
        try:
            return parse_timestamp(match.group("timestamp"))
        except ValueError:
            return None
    match = GLOG_TIMESTAMP_RE.match(line)
    if match is None:
        return None
    values = match.groupdict()
    try:
        return datetime(datetime.now().year, int(values["month"]), int(values["day"]), int(values["hour"]),
                        int(values["minute"]), int(values["second"]),
                        int((values["microsecond"] or "").ljust(6, "0") or 0), tzinfo=timezone.utc)
    except ValueError:
        return None


def read_log_file(filepath_str, parser):
    """Read one plain or compressed log file and return parser results."""
    filepath = Path(filepath_str)
    results = []
    opener = gzip.open if str(filepath).endswith(".gz") else open
    t0 = time.time()
    try:
        with opener(filepath, "rt", encoding="utf-8", errors="ignore") as fh:
            for line in fh:
                parsed = parser(line)
                if parsed:
                    results.append(parsed)
    except Exception as e:
        sys.stderr.write("  [WARN] read " + filepath.name + " failed: " + str(e) + "\n")
    elapsed_ms = (time.time() - t0) * 1000
    print("  [DONE] " + filepath.name + " -> " + str(len(results)) + " records (" + str(round(elapsed_ms, 1)) + "ms)")
    return str(filepath), results


def collect_all_logs(root_dir, log_kind):
    """Collect direct worker log files for an access or URMA elapsed source."""
    tasks = []
    for worker_dir in sorted(root_dir.iterdir()):
        if not worker_dir.is_dir():
            continue
        worker_name = worker_dir.name
        for f in sorted(worker_dir.iterdir()):
            if f.is_file() and ((log_kind == "access" and f.name.startswith("access"))
                                or (log_kind == "urma" and ".INFO." in f.name.upper())):
                tasks.append((worker_name, str(f)))
    return tasks


def parallel_read_logs(tasks, max_workers, parser):
    """Read all task files concurrently and group parser results by worker."""
    worker_records = defaultdict(list)
    if not tasks:
        return worker_records

    if max_workers <= 1:
        print("  mode: sequential")
        t0 = time.time()
        for worker_name, filepath in tasks:
            _, records = read_log_file(filepath, parser)
            worker_records[worker_name].extend(records)
        print("  total read time: " + str(round((time.time() - t0) * 1000, 1)) + "ms")
        return worker_records

    print("  mode: parallel (" + str(max_workers) + " threads)")
    t0 = time.time()
    futures = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        for worker_name, filepath in tasks:
            future = executor.submit(read_log_file, filepath, parser)
            futures.append((future, worker_name))

        wait([f for f, _ in futures])

        for future, worker_name in futures:
            try:
                _, records = future.result()
                worker_records[worker_name].extend(records)
            except Exception as e:
                print("  [ERROR] " + str(e), file=sys.stderr)

    total_ms = (time.time() - t0) * 1000
    print("  total read time: " + str(round(total_ms, 1)) + "ms (parallel)")
    return worker_records


def compute_qps(timestamps, interval_ms):
    if not timestamps:
        return [], []
    timestamps.sort()
    interval_sec = interval_ms / 1000.0
    start_ts = timestamps[0].timestamp()
    end_ts = timestamps[-1].timestamp()
    buckets = defaultdict(int)
    for ts in timestamps:
        bucket_idx = int((ts.timestamp() - start_ts) / interval_sec)
        buckets[bucket_idx] += 1
    max_idx = int((end_ts - start_ts) / interval_sec)
    times = []
    qps = []
    for i in range(max_idx + 1):
        t = datetime.fromtimestamp(start_ts + i * interval_sec, tz=timezone.utc)
        times.append(t)
        qps.append(buckets.get(i, 0) / interval_sec)
    return times, qps


def compute_stats(qps_list):
    if not qps_list:
        return {"avg": 0, "max": 0, "p90": 0, "p99": 0}
    sorted_qps = sorted(qps_list)
    n = len(sorted_qps)
    avg_val = sum(sorted_qps) / n
    max_val = sorted_qps[-1]
    p90_idx = max(0, math.ceil(n * 0.90) - 1)
    p99_idx = max(0, math.ceil(n * 0.99) - 1)
    return {
        "avg": round(avg_val, 2),
        "max": round(max_val, 2),
        "p90": round(sorted_qps[p90_idx], 2),
        "p99": round(sorted_qps[p99_idx], 2)
    }


def build_qps_page(worker_qps_data, metric_label, interval_ms, color_offset=0):
    workers = sorted(worker_qps_data.keys())
    colors = [
        "#1f77b4", "#ff7f0e", "#2ca02c", "#d62728", "#9467bd",
        "#8c564b", "#e377c2", "#7f7f7f", "#bcbd22", "#17becf",
        "#aec7e8", "#ffbb78", "#98df8a", "#ff9896", "#c5b0d5",
        "#c49c94", "#f7b6d2", "#c7c7c7", "#dbdb8d", "#9edae5",
        "#393b79", "#5254a3", "#6b6ecf", "#9c9ede", "#637939",
        "#8ca252", "#b5cf6b", "#cedb9c", "#8c6d31", "#bd9e39"
    ]

    chart_divs = []
    plot_ids = []
    stats_html_parts = []
    for idx, worker in enumerate(workers):
        times, qps = worker_qps_data[worker]
        plot_id = "chart_" + str(idx) + "_" + uuid.uuid4().hex[:8]
        plot_ids.append(plot_id)
        color = colors[(idx + color_offset) % len(colors)]
        stats = compute_stats(qps)

        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=times, y=qps, mode="lines", name=worker,
            line=dict(color=color, width=1.2),
            hovertemplate="<b>" + worker + "</b><br>time: %{x|%Y-%m-%d %H:%M:%S.%3f}<br>QPS: %{y:.2f}<extra></extra>",
            showlegend=False
        ))

        fig.update_layout(
            title=dict(text="Worker: " + worker, font=dict(size=13, color="#333"), x=0.02, xanchor="left"),
            height=260,
            margin=dict(l=60, r=30, t=50, b=40),
            xaxis=dict(
                title="time" if idx == len(workers) - 1 else None,
                showticklabels=True,
                rangeslider=dict(visible=True, thickness=0.08, bgcolor="#f0f0f0"),
                tickfont=dict(size=10)
            ),
            yaxis=dict(title=metric_label, tickfont=dict(size=10), showgrid=True, gridcolor="rgba(0,0,0,0.05)"),
            template="plotly_white",
            showlegend=False,
            hovermode="x unified"
        )

        div_html = fig.to_html(full_html=False, include_plotlyjs=False, div_id=plot_id)
        stats_line = (
            "<div class=\"stats-row\">"
            "<span class=\"stat-item\"><b>avg:</b> " + str(stats["avg"]) + "</span>"
            "<span class=\"stat-item\"><b>max:</b> " + str(stats["max"]) + "</span>"
            "<span class=\"stat-item\"><b>p90:</b> " + str(stats["p90"]) + "</span>"
            "<span class=\"stat-item\"><b>p99:</b> " + str(stats["p99"]) + "</span>"
            "</div>")
        chart_divs.append(div_html)
        stats_html_parts.append(stats_line)

    plot_ids_json = str(plot_ids).replace("'", '"')

    chart_blocks = []
    for i in range(len(workers)):
        chart_blocks.append("<div class=\"chart-wrapper\">" + chart_divs[i] + stats_html_parts[i] + "</div>")
    return "\n".join(chart_blocks), plot_ids_json


def build_html_report(api_qps_data, urma_qps_data, op_type, interval_ms, output_path):
    pages = []
    plot_ids_by_page = []
    if api_qps_data:
        escaped_type = html.escape(op_type)
        api_charts, api_plot_ids = build_qps_page(api_qps_data, "API QPS", interval_ms)
        pages.append(("api-page", "Worker API QPS", api_charts,
                      "Access-log API QPS for type: " + escaped_type + ".", api_plot_ids))
    if urma_qps_data:
        urma_charts, urma_plot_ids = build_qps_page(urma_qps_data, "Emitted log QPS", interval_ms, color_offset=1)
        pages.append(("urma-page", "URMA Write QPS", urma_charts,
                      "Counts emitted [URMA_ELAPSED_TOTAL] lines, not necessarily every URMA write completion.",
                      urma_plot_ids))
    if not pages:
        print("no data", file=sys.stderr)
        return

    css_lines = [
        "* { box-sizing: border-box; }",
        ("body { font-family: -apple-system, BlinkMacSystemFont, Segoe UI, Roboto, "
         "Helvetica Neue, Arial, sans-serif; margin: 0; padding: 0; background: #f0f2f5; }"),
        (".header { text-align: center; padding: 24px 20px; background: "
         "linear-gradient(135deg, #667eea 0%, #764ba2 100%); color: white; "
         "box-shadow: 0 2px 8px rgba(0,0,0,0.15); position: sticky; top: 0; z-index: 100; }"),
        ".header h1 { margin: 0; font-size: 22px; font-weight: 600; }",
        ".header .meta { margin-top: 8px; font-size: 13px; opacity: 0.9; }",
        (".header .meta span { display: inline-block; background: rgba(255,255,255,0.2); "
         "padding: 3px 10px; border-radius: 12px; margin: 2px 4px; }"),
        (".charts-container { max-width: 1500px; margin: 0 auto; padding: 20px; display: "
         "flex; flex-direction: column; gap: 16px; }"),
        (".chart-wrapper { background: white; border-radius: 10px; padding: 12px 12px 10px 12px; "
         "box-shadow: 0 1px 3px rgba(0,0,0,0.08); transition: box-shadow 0.2s; }"),
        ".chart-wrapper:hover { box-shadow: 0 4px 12px rgba(0,0,0,0.12); }",
        (".stats-row { display: flex; flex-wrap: wrap; gap: 16px; padding: 8px 4px 4px 4px; "
         "border-top: 1px solid #eee; margin-top: 4px; }"),
        ".stat-item { font-size: 12px; color: #555; }",
        ".stat-item b { color: #333; }",
        (".hint { text-align: center; color: #666; font-size: 12px; padding: 10px; "
         "background: white; border-radius: 8px; margin-bottom: 10px; }"),
        ".tabs { display: flex; gap: 8px; margin-bottom: 12px; }",
        (".tab { border: 0; border-radius: 7px; padding: 9px 14px; background: #dde1ee; "
         "color: #35405a; cursor: pointer; font-weight: 600; }"),
        ".tab.active { background: #5d6fd8; color: white; }",
        ".tab-page { display: none; } .tab-page.active { display: block; }"
    ]
    css = "\n".join(css_lines)

    for page_id, _, _, _, plot_ids_json in pages:
        plot_ids_by_page.append((page_id, plot_ids_json))
    js_lines = [
        "var isSyncing = false;",
        "function syncXAxis(plotIds, sourceId, xRange) {",
        "    if (isSyncing) return;",
        "    isSyncing = true;",
        "    for (var i = 0; i < plotIds.length; i++) {",
        "        if (plotIds[i] !== sourceId) {",
        "            Plotly.relayout(plotIds[i], { xaxis: { range: xRange } });",
        "        }",
        "    }",
        "    setTimeout(function() { isSyncing = false; }, 50);",
        "}",
        "function bindPage(plotIds) {",
        "    for (var i = 0; i < plotIds.length; i++) {",
        "        (function(pid) {",
        "            var el = document.getElementById(pid);",
        "            if (!el) return;",
        "            el.on(\"plotly_relayout\", function(ed) {",
        "                if (ed[\"xaxis.range[0]\"] !== undefined && ed[\"xaxis.range[1]\"] !== undefined) {",
        "                    syncXAxis(plotIds, pid, [ed[\"xaxis.range[0]\"], ed[\"xaxis.range[1]\"]]);",
        "                }",
        "            });",
        "        })(plotIds[i]);",
        "    }",
        "}"
    ]
    for _, plot_ids_json in plot_ids_by_page:
        js_lines.append("bindPage(" + plot_ids_json + ");")
    js = "\n    ".join(js_lines)

    tab_html = []
    page_html = []
    for index, (page_id, title, charts, hint, _) in enumerate(pages):
        active = " active" if index == 0 else ""
        tab_html.append("<button class=\"tab" + active + "\" data-page=\"" + page_id + "\">" + title + "</button>")
        page_html.append("<section id=\"" + page_id + "\" class=\"tab-page" + active + "\"><div class=\"hint\">"
                         + hint + " Interval: " + str(interval_ms) + "ms.</div>" + charts + "</section>")

    parts = [
        "<!DOCTYPE html>",
        "<html>",
        "<head>",
        "<meta charset=\"utf-8\">",
        "<meta name=\"viewport\" content=\"width=device-width, initial-scale=1.0\">",
        "<title>Worker QPS Report</title>",
        "<script src=\"https://cdn.plot.ly/plotly-3.1.0.min.js\"></script>",
        "<style>",
        css,
        "</style>",
        "</head>",
        "<body>",
        "<div class=\"header\">",
        "<h1>Worker QPS Report</h1>",
        "<div class=\"meta\">",
        "<span>API type: " + (html.escape(op_type) if op_type else "not requested") + "</span>",
        "<span>interval: " + str(interval_ms) + "ms</span>",
        "<span>API workers: " + str(len(api_qps_data)) + "</span>",
        "<span>URMA workers: " + str(len(urma_qps_data)) + "</span>",
        "</div></div>",
        "<div class=\"charts-container\">",
        "<div class=\"tabs\">" + "".join(tab_html) + "</div>",
        "".join(page_html),
        "</div>",
        "<script>",
        js,
        "document.querySelectorAll('.tab').forEach(function(tab) { tab.addEventListener('click', function() {"
        "document.querySelectorAll('.tab').forEach(function(item) { item.classList.toggle('active', item === tab); });"
        +
        ("document.querySelectorAll('.tab-page').forEach(function(item) { "
         "item.classList.toggle('active', item.id === tab.dataset.page); });") +
        "window.dispatchEvent(new Event('resize')); }); });",
        "</script>",
        "</body>",
        "</html>"
    ]
    report_html = "\n".join(parts)

    with open(output_path, "w", encoding="utf-8") as f:
        f.write(report_html)
    print("report generated: " + os.path.abspath(output_path))


def main():
    parser = argparse.ArgumentParser(
        description="Generate per-worker API QPS and optional emitted URMA completion-log QPS reports.",
        epilog=("Examples:\n"
                "  %(prog)s --type DS_POSIX_GET --dir /data/logs/collected_worker_logs "
                "--interval 100 --jobs 16 --output create_qps.html\n"
                "  %(prog)s --urma-write --dir /data/logs/collected_worker_logs --interval 100\n"
                "  %(prog)s --type DS_POSIX_GET --urma-write --dir /data/logs/collected_worker_logs"),
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--dir", default="collected_worker_logs", metavar="DIR",
                        help="worker-log root (default: ./collected_worker_logs)")
    parser.add_argument("--type", metavar="API_TYPE",
                        help="access-log API operation to count, for example DS_POSIX_GET")
    parser.add_argument("--urma-write", action="store_true",
                        help=("add a URMA Write QPS tab from [URMA_ELAPSED_TOTAL] timestamps; this counts emitted "
                              "log lines and may not equal all URMA write completions"))
    parser.add_argument("--interval", type=int, default=1000, metavar="MS",
                        help="QPS bucket width in milliseconds (default: 1000)")
    parser.add_argument("--output", default="worker_qps_report.html", metavar="HTML",
                        help="output HTML report path (default: worker_qps_report.html)")
    parser.add_argument("--jobs", type=int, default=4, metavar="N",
                        help="parallel log-reader threads; set 1 for sequential reads (default: 4)")
    args = parser.parse_args()
    if not args.type and not args.urma_write:
        parser.error("at least one of --type or --urma-write is required")
    if args.interval <= 0:
        parser.error("--interval must be greater than zero")
    if args.jobs <= 0:
        parser.error("--jobs must be greater than zero")

    root_dir = Path(args.dir)
    if not root_dir.exists():
        print("error: dir not found: " + str(root_dir), file=sys.stderr)
        sys.exit(1)

    print("API type: " + (args.type or "not requested"))
    print("URMA write emitted-log QPS: " + ("enabled" if args.urma_write else "disabled"))
    print("interval: " + str(args.interval) + "ms")
    print("parallel: " + str(args.jobs) + " threads")
    print("log_dir: " + str(root_dir.absolute()))
    print("-" * 50)

    api_qps_data = {}
    urma_qps_data = {}
    if args.type:
        access_tasks = collect_all_logs(root_dir, "access")
        print("access log files: " + str(len(access_tasks)))
        access_records = parallel_read_logs(access_tasks, args.jobs, parse_access_line)
        for worker_name in sorted(access_records):
            records = access_records[worker_name]
            timestamps = [timestamp for timestamp, record_type in records if record_type == args.type]
            print("API worker: " + worker_name + " | total: " + str(len(records))
                  + " | matched: " + str(len(timestamps)))
            if timestamps:
                api_qps_data[worker_name] = compute_qps(timestamps, args.interval)
        if not api_qps_data:
            print("warning: no access-log records found for type: " + args.type, file=sys.stderr)

    if args.urma_write:
        urma_tasks = collect_all_logs(root_dir, "urma")
        print("URMA INFO log files: " + str(len(urma_tasks)))
        urma_records = parallel_read_logs(urma_tasks, args.jobs, parse_urma_elapsed_line)
        for worker_name in sorted(urma_records):
            timestamps = urma_records[worker_name]
            print("URMA worker: " + worker_name + " | emitted [URMA_ELAPSED_TOTAL] lines: " + str(len(timestamps)))
            if timestamps:
                urma_qps_data[worker_name] = compute_qps(timestamps, args.interval)
        if not urma_qps_data:
            print("warning: no emitted [URMA_ELAPSED_TOTAL] lines found", file=sys.stderr)

    if not api_qps_data and not urma_qps_data:
        print("no reportable QPS data found", file=sys.stderr)
        sys.exit(1)
    build_html_report(api_qps_data, urma_qps_data, args.type, args.interval, args.output)


if __name__ == "__main__":
    main()
