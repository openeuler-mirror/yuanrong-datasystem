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
"""Generate a lazily loaded URMA post/inflight correlation report.

The input layout, timestamp formats, INFO-log filtering, compressed-log support,
worker prefix filtering, and --list mode match plot_urma_chip_inflight.py.  This
report intentionally has a smaller scope: it compares four time series for one
selected worker:

  * URMA_ELAPSED_TOTAL total cost in milliseconds;
  * total srcChipInflight sampled by URMA_ELAPSED_TOTAL; and
  * total srcChipInflight sampled immediately after PostJettyRw.
  * request arrival, posted, and completed counts per 1ms bucket, counted from
    ServerRecv, URMA_POST_AFTER, and URMA_POLL_JFC Got event records.

Each worker's samples are written to a separate JSON file and a separate,
self-contained HTML page in <html-stem>_data.  The main HTML switches an iframe
to the selected worker page, so it works when opened directly with file:// and
does not load every worker's chart data at startup.

Examples:
  python3 plot_urma_performance.py -f collected_worker_logs -o urma_performance.html
  python3 plot_urma_performance.py --list dirs.txt -o reports --prefix workerA
"""

import argparse
import gzip
import hashlib
import json
import os
import re
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime


ISO_TS_RE = re.compile(
    r"(?P<year>\d{4})-(?P<mon>\d{2})-(?P<day>\d{2})T"
    r"(?P<h>\d{2}):(?P<m>\d{2}):(?P<s>\d{2})(?:\.(?P<us>\d{1,6}))?"
)
GLOG_TS_RE = re.compile(
    r"^(?P<sev>[IWEF])(?P<mon>\d{2})(?P<day>\d{2})\s+"
    r"(?P<h>\d{2}):(?P<m>\d{2}):(?P<s>\d{2})(?:\.(?P<us>\d{1,6}))?\s+\d+\s+"
)
INFLIGHT_RE = re.compile(r"srcChipInflight:\{([^}]*)\}")
CHIP_CNT_RE = re.compile(r"(\d+):(-?\d+)")
TOTAL_ELAPSED_MS_RE = re.compile(
    r"\btotal cost\s+(?P<elapsed>-?(?:\d+(?:\.\d*)?|\.\d+))ms\b", re.IGNORECASE
)
CURRENT_YEAR = datetime.now().year


def parse_ts(line):
    """Parse the custom ISO timestamp first, then the glog timestamp."""
    match = ISO_TS_RE.search(line) or GLOG_TS_RE.match(line)
    if match is None:
        return None
    fields = match.groupdict()
    try:
        return datetime(int(fields.get("year") or CURRENT_YEAR), int(fields["mon"]), int(fields["day"]),
                        int(fields["h"]), int(fields["m"]), int(fields["s"]),
                        int((fields.get("us") or "").ljust(6, "0") or 0))
    except ValueError:
        return None


def parse_total_inflight(line):
    """Return the sum of all reported source-chip inflight counts."""
    match = INFLIGHT_RE.search(line)
    if match is None:
        return None
    return sum(int(item.group(2)) for item in CHIP_CNT_RE.finditer(match.group(1)))


def open_log(path):
    return gzip.open(path, "rt", encoding="utf-8", errors="replace") if path.endswith(".gz") else open(
        path, "r", encoding="utf-8", errors="replace")


def is_info_log(name):
    return ".INFO." in name.upper()


def scan_worker(worker_dir):
    """Read independent total-elapsed, post-after, completion, and request-arrival samples."""
    elapsed_samples = []
    post_samples = []
    completion_times = []
    request_times = []
    info_files = sorted(entry.path for entry in os.scandir(worker_dir)
                        if entry.is_file() and is_info_log(entry.name))
    for path in info_files:
        try:
            with open_log(path) as log_file:
                for line in log_file:
                    if "URMA_ELAPSED_TOTAL" in line:
                        timestamp = parse_ts(line)
                        inflight = parse_total_inflight(line)
                        elapsed = TOTAL_ELAPSED_MS_RE.search(line)
                        if timestamp is not None and inflight is not None and elapsed is not None:
                            elapsed_samples.append((timestamp, float(elapsed.group("elapsed")), inflight))
                    elif "URMA_POST_AFTER" in line:
                        timestamp = parse_ts(line)
                        inflight = parse_total_inflight(line)
                        if timestamp is not None and inflight is not None:
                            post_samples.append((timestamp, inflight))
                    elif "URMA_POLL_JFC" in line and "Got event" in line:
                        timestamp = parse_ts(line)
                        if timestamp is not None:
                            completion_times.append(timestamp)
                    elif "ServerRecv ts" in line:
                        # Query access logs are emitted on completion; ServerRecv is the arrival-time signal.
                        timestamp = parse_ts(line)
                        if timestamp is not None:
                            request_times.append(timestamp)
        except (OSError, EOFError, gzip.BadGzipFile) as exc:
            print(f"warning: failed to read log file {path}: {exc}", file=sys.stderr)
    elapsed_samples.sort(key=lambda sample: sample[0])
    post_samples.sort(key=lambda sample: sample[0])
    completion_times.sort()
    request_times.sort()
    return elapsed_samples, post_samples, completion_times, request_times, len(info_files)


def downsample(samples, width):
    """Mean values and a median timestamp preserve existing report semantics."""
    if width == 1:
        return samples
    result = []
    for start in range(0, len(samples), width):
        bucket = samples[start:start + width]
        midpoint = bucket[len(bucket) // 2][0]
        values = [sum(row[index] for row in bucket) / len(bucket) for index in range(1, len(bucket[0]))]
        result.append((midpoint, *values))
    return result


def qpm_samples(timestamps):
    """Count timestamped request records in fixed 1ms wall-clock buckets."""
    buckets = {}
    for timestamp in timestamps:
        bucket_us = (timestamp.microsecond // 1000) * 1000
        bucket = timestamp.replace(microsecond=bucket_us)
        buckets[bucket] = buckets.get(bucket, 0) + 1
    return sorted(buckets.items())


def find_workers(root, prefixes):
    workers = []
    for entry in os.scandir(root):
        if not entry.is_dir() or (prefixes and not any(entry.name.startswith(prefix) for prefix in prefixes)):
            continue
        with os.scandir(entry.path) as children:
            if any(child.is_file() and is_info_log(child.name) for child in children):
                workers.append((entry.name, entry.path))
    return sorted(workers)


def worker_payload(name, worker_dir, downsample_width):
    elapsed, post, completions, request_times, info_files = scan_worker(worker_dir)
    top_elapsed = sorted(elapsed, key=lambda sample: sample[1], reverse=True)[:1000]
    post_qpm = qpm_samples([timestamp for timestamp, _ in post])
    completion_qpm = qpm_samples(completions)
    request_qpm = qpm_samples(request_times)
    elapsed = downsample(elapsed, downsample_width)
    post = downsample(post, downsample_width)
    return {
        "name": name,
        "infoFiles": info_files,
        "topElapsed": [(row[1], row[0].isoformat()) for row in top_elapsed],
        "elapsed": {
            "time": [row[0].isoformat() for row in elapsed],
            "totalMs": [row[1] for row in elapsed],
            "inflight": [row[2] for row in elapsed],
        },
        "postAfter": {
            "time": [row[0].isoformat() for row in post],
            "inflight": [row[1] for row in post],
        },
        "qpm": {
            "postTime": [row[0].isoformat() for row in post_qpm],
            "posts": [row[1] for row in post_qpm],
            "completionTime": [row[0].isoformat() for row in completion_qpm],
            "completions": [row[1] for row in completion_qpm],
            "requestTime": [row[0].isoformat() for row in request_qpm],
            "requests": [row[1] for row in request_qpm],
        },
    }


def safe_filename(name, ordinal):
    digest = hashlib.sha256(name.encode("utf-8")).hexdigest()[:12]
    return f"worker_{ordinal:04d}_{digest}.json"


def worker_html_page(payload, title):
    data = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
    script = '''const data=__DATA__;
function trace(x,y,name,color){return {x,y,name,mode:"lines",line:{color,width:1.4},hovertemplate:"%{x}<br>"+name+": %{y}<extra></extra>"}}
function layout(yTitle){return {height:300,margin:{l:65,r:25,t:18,b:52},template:"plotly_white",hovermode:"x unified",xaxis:{title:"timestamp",rangeslider:{visible:true,thickness:.08,bgcolor:"#f0f0f0"},tickfont:{size:10}},yaxis:{title:yTitle,rangemode:"tozero",tickfont:{size:10},gridcolor:"rgba(0,0,0,.05)"},showlegend:false}}
function plot(id,x,y,name,color,yTitle){Plotly.newPlot(id,[trace(x,y,name,color)],layout(yTitle),{responsive:true,displaylogo:false})}
plot("elapsed",data.elapsed.time,data.elapsed.totalMs,"URMA total elapsed (ms)","#ff7f0e","milliseconds");
plot("elapsed-inflight",data.elapsed.time,data.elapsed.inflight,"inflight at URMA elapsed","#17becf","inflight count");
plot("post-inflight",data.postAfter.time,data.postAfter.inflight,"inflight after post","#9467bd","inflight count");
Plotly.newPlot("qpm",[trace(data.qpm.postTime,data.qpm.posts,"posts per 1ms","#2ca02c"),trace(data.qpm.completionTime,data.qpm.completions,"Got event completions per 1ms","#1f77b4")],layout("request count / 1ms"),{responsive:true,displaylogo:false});
const chartIds=["elapsed","elapsed-inflight","post-inflight","qpm"];let syncedRange=null;let syncing=false;
function syncRange(sourceId,event){const range=event["xaxis.range"]||((event["xaxis.range[0]"]!==undefined&&event["xaxis.range[1]"]!==undefined)?[event["xaxis.range[0]"],event["xaxis.range[1]"]]:null);const reset=event["xaxis.autorange"]===true;if(!range&&!reset)return;const key=reset?"__auto__":range[0]+"|"+range[1];if(syncing||key===syncedRange)return;syncedRange=key;syncing=true;const update=reset?{"xaxis.autorange":true}:{"xaxis.range":range};Promise.all(chartIds.filter(id=>id!==sourceId).map(id=>Plotly.relayout(id,update))).finally(()=>{syncing=false})}
chartIds.forEach(id=>document.getElementById(id).on("plotly_relayout",event=>syncRange(id,event)));'''.replace("__DATA__", data)
    return '''<!doctype html><html lang="en"><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>'''+title+'''</title><script src="https://cdn.plot.ly/plotly-3.1.0.min.js"></script><style>*{box-sizing:border-box}body{margin:0;background:#f0f2f5;color:#252a34;font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,Arial,sans-serif}.container{max-width:1500px;margin:auto;padding:20px}.hint,.card{background:#fff;border-radius:10px;box-shadow:0 1px 3px #0002}.hint{padding:10px;text-align:center;color:#666;font-size:12px;margin-bottom:16px}.card{padding:12px;margin-bottom:16px}.card:hover{box-shadow:0 4px 12px #0003}.card h2{margin:2px 8px 8px;font-size:16px}.chart{height:300px}</style><main class="container"><p class="hint">Orange: URMA total elapsed. Cyan: srcChipInflight recorded in URMA_ELAPSED_TOTAL. Purple: srcChipInflight recorded immediately after post. Green: URMA_POST_AFTER request count per 1ms. Blue: URMA_POLL_JFC Got event completion count per 1ms. Select a time range in any chart to synchronize all four; double-click any chart to reset all ranges.</p><section class="card"><h2>URMA total elapsed</h2><div id="elapsed" class="chart"></div></section><section class="card"><h2>Inflight at URMA elapsed</h2><div id="elapsed-inflight" class="chart"></div></section><section class="card"><h2>Inflight after post</h2><div id="post-inflight" class="chart"></div></section><section class="card"><h2>Requests per 1ms (QPM)</h2><div id="qpm" class="chart"></div></section></main><script>'''+script+'''</script></html>'''


def qpm_worker_html_page(payload, title):
    data = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
    script = '''const data=__DATA__;function trace(x,y,name,color){return {x,y,name,mode:"lines",line:{color,width:1.4},hovertemplate:"%{x}<br>"+name+": %{y}<extra></extra>"}}function layout(){return {margin:{l:70,r:30,t:40,b:70},hovermode:"x unified",xaxis:{title:"timestamp",rangeslider:{visible:true}},yaxis:{title:"request count / 1ms",rangemode:"tozero"}}}Plotly.newPlot("qpm",[trace(data.qpm.requestTime,data.qpm.requests,"BRPC ServerRecv requests per 1ms","#d62728"),trace(data.qpm.postTime,data.qpm.posts,"URMA_POST_AFTER per 1ms","#2ca02c"),trace(data.qpm.completionTime,data.qpm.completions,"URMA_POLL_JFC Got event per 1ms","#1f77b4")],layout(),{responsive:true,displaylogo:false});'''.replace("__DATA__", data)
    return '''<!doctype html><html lang="en"><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>'''+title+'''</title><script src="https://cdn.plot.ly/plotly-3.1.0.min.js"></script><style>body{margin:0;background:#f4f6f8;color:#18212b;font:14px system-ui,sans-serif}.container{max-width:1280px;margin:0 auto;padding:16px 24px}.hint{color:#51606e;line-height:1.5}.card{background:#fff;border:1px solid #dde3e8;border-radius:8px;padding:12px;margin:16px 0;box-shadow:0 1px 2px #0000000d}.chart{height:min(75vh,760px)}</style><main class="container"><section class="card"><h2>QPM per 1ms</h2><p class="hint">Red: BRPC ServerRecv request arrival. Green: URMA_POST_AFTER. Blue: URMA_POLL_JFC Got event. `DS_POSIX_QUERY_AND_GET` access logs are intentionally excluded because they are emitted after QueryAndGet completes and therefore do not represent request arrival. Each point is the number of records in one fixed 1ms wall-clock bucket.</p><div id="qpm" class="chart"></div></section></main><script>'''+script+'''</script></html>'''


def html_page(workers, title):
    metadata = json.dumps(workers, ensure_ascii=False, separators=(",", ":"))
    script = '''const workers=__WORKERS__;const picker=document.getElementById("worker");const frame=document.getElementById("worker-report");const status=document.getElementById("status");function loadWorker(index){const worker=workers[index];if(!worker)return;frame.src=worker.reportFile;status.textContent=worker.name+": "+worker.elapsedSamples+" URMA_ELAPSED_TOTAL samples, "+worker.postSamples+" URMA_POST_AFTER samples ("+worker.infoFiles+" INFO logs)"}picker.addEventListener("change",()=>loadWorker(picker.selectedIndex));workers.forEach(worker=>{const option=document.createElement("option");option.textContent=worker.name;picker.append(option)});const selected=new URLSearchParams(location.search).get("worker"),index=workers.findIndex(worker=>worker.name===selected);if(workers.length){picker.selectedIndex=index>=0?index:0;loadWorker(picker.selectedIndex)}else status.textContent="No worker directories with INFO logs were found."'''.replace("__WORKERS__", metadata)
    return '''<!doctype html><html lang="en"><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>'''+title+'''</title><style>*{box-sizing:border-box}body{margin:0;background:#f0f2f5;color:#252a34;font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,Arial,sans-serif}.header{padding:22px max(24px,calc((100vw - 1280px)/2));text-align:center;color:#fff;background:linear-gradient(135deg,#667eea,#764ba2);box-shadow:0 2px 8px #0003}.header h1{margin:0 0 8px;font-size:22px}.container{max-width:1500px;margin:20px auto;padding:0 20px}.controls,.hint{background:#fff;border-radius:10px;box-shadow:0 1px 3px #0002}.controls{display:flex;gap:12px;align-items:center;padding:12px}.controls select{min-width:280px;padding:8px;border:1px solid #ccd2df;border-radius:6px}.hint,#status{color:#666;line-height:1.5}.hint{padding:10px;text-align:center;font-size:12px}iframe{width:100%;height:2550px;border:0;background:#f0f2f5}</style><header class="header"><h1>URMA post/inflight correlation</h1><div>The selected worker report is loaded independently and works when this file is opened directly.</div></header><main class="container"><div class="controls"><label for="worker">Worker</label><select id="worker"></select></div><p id="status"></p><p class="hint">Each worker has four independent charts: URMA elapsed, inflight at completion, inflight after post, and requests per 1ms.</p><iframe id="worker-report" title="Selected worker report"></iframe></main><script>'''+script+'''</script></html>'''


def elapsed_top_index_page(workers, title, report_name):
    rows = "".join(f'<tr data-worker="{name}"><td>{name}</td><td>{count}</td></tr>' for name, count in workers)
    return '''<!doctype html><html lang="en"><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>'''+title+'''</title><style>*{box-sizing:border-box}body{margin:0;background:#f0f2f5;color:#252a34;font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,Arial,sans-serif}.header{padding:22px 20px;text-align:center;color:#fff;background:linear-gradient(135deg,#667eea,#764ba2);box-shadow:0 2px 8px #0003}.header h1{margin:0 0 8px;font-size:22px}.container{max-width:920px;margin:24px auto;padding:0 20px}.card{background:#fff;border-radius:10px;padding:16px;box-shadow:0 1px 3px #0002}.hint{color:#666;font-size:13px;line-height:1.5}table{width:100%;border-collapse:collapse;margin-top:14px}th,td{padding:10px 12px;text-align:left;border-bottom:1px solid #edf0f4}th{color:#4c5770;font-size:12px;text-transform:uppercase;letter-spacing:.03em}tbody tr{cursor:pointer}tbody tr:hover{background:#f1f4ff}td:last-child{text-align:right;font-variant-numeric:tabular-nums}</style><header class="header"><h1>Top 1000 URMA elapsed by worker</h1><div>Click a worker to open the post/inflight report with that worker selected.</div></header><main class="container"><section class="card"><p class="hint">The global top 1000 is ranked by `URMA_ELAPSED_TOTAL` total cost before downsampling. The count is the number of records contributed by each worker.</p><table><thead><tr><th>Worker</th><th>Records in global top 1000</th></tr></thead><tbody>'''+rows+'''</tbody></table></section></main><script>const report=decodeURIComponent("'''+report_name+'''");document.querySelectorAll("tr[data-worker]").forEach(row=>row.addEventListener("click",()=>{location.href=report+"?worker="+encodeURIComponent(row.dataset.worker)}));</script></html>'''


def qpm_index_page(workers, title):
    metadata = json.dumps(workers, ensure_ascii=False, separators=(",", ":"))
    script = '''const workers=__WORKERS__;const picker=document.getElementById("worker");const frame=document.getElementById("worker-report");const status=document.getElementById("status");function loadWorker(index){const worker=workers[index];if(!worker)return;frame.src=worker.reportFile;status.textContent=worker.name+": BRPC ServerRecv="+worker.requestSamples+", post="+worker.postSamples+", poll="+worker.completionSamples+" 1ms buckets"}picker.addEventListener("change",()=>loadWorker(picker.selectedIndex));workers.forEach(worker=>{const option=document.createElement("option");option.textContent=worker.name;picker.append(option)});if(workers.length)loadWorker(0);else status.textContent="No worker directories with INFO logs were found."'''.replace("__WORKERS__", metadata)
    return '''<!doctype html><html lang="en"><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>'''+title+'''</title><style>body{margin:0;background:#f4f6f8;color:#18212b;font:14px system-ui,sans-serif}.header{padding:22px max(24px,calc((100vw - 1280px)/2));background:#152536;color:#fff}.header h1{margin:0 0 8px;font-size:22px}.container{max-width:1280px;margin:24px auto;padding:0 24px}.controls{display:flex;gap:12px;align-items:center;margin-bottom:12px}.controls select{min-width:280px;padding:8px}.hint,#status{color:#51606e;line-height:1.5}iframe{width:100%;height:900px;border:0;background:#f4f6f8}</style><header class="header"><h1>URMA QPM correlation</h1><div>BRPC ServerRecv request-arrival, post, and poll Got event counts per 1ms.</div></header><main class="container"><div class="controls"><label for="worker">Worker</label><select id="worker"></select></div><p id="status"></p><iframe id="worker-report" title="Selected worker QPM report"></iframe></main><script>'''+script+'''</script></html>'''


def output_paths(output):
    html_path = os.path.abspath(output)
    stem, extension = os.path.splitext(html_path)
    if extension.lower() != ".html":
        html_path = html_path + ".html"
        stem = html_path[:-5]
    return html_path, stem + "_data"


def generate_report(root, output, prefixes, jobs, downsample_width, report_mode):
    workers = find_workers(root, prefixes)
    html_path, data_dir = output_paths(output)
    os.makedirs(os.path.dirname(html_path) or ".", exist_ok=True)
    os.makedirs(data_dir, exist_ok=True)
    payloads = [None] * len(workers)
    with ThreadPoolExecutor(max_workers=jobs) as executor:
        futures = {executor.submit(worker_payload, name, path, downsample_width): index
                   for index, (name, path) in enumerate(workers)}
        for future in as_completed(futures):
            payloads[futures[future]] = future.result()
    metadata = []
    for index, payload in enumerate(payloads):
        filename = safe_filename(payload["name"], index)
        chart_payload = {key: value for key, value in payload.items() if key != "topElapsed"}
        with open(os.path.join(data_dir, filename), "w", encoding="utf-8") as data_file:
            json.dump(chart_payload, data_file, ensure_ascii=False, separators=(",", ":"))
        report_name = filename[:-5] + ".html"
        qpm_name = filename[:-5] + "_qpm.html"
        if report_mode in ("old", "both"):
            with open(os.path.join(data_dir, report_name), "w", encoding="utf-8") as worker_report:
                worker_report.write(worker_html_page(chart_payload, payload["name"] + " URMA post/inflight"))
            metadata.append({"name": payload["name"], "reportFile": os.path.basename(data_dir) + "/" + report_name,
                             "elapsedSamples": len(payload["elapsed"]["time"]),
                             "postSamples": len(payload["postAfter"]["time"]), "infoFiles": payload["infoFiles"]})
        if report_mode in ("qpm", "both"):
            with open(os.path.join(data_dir, qpm_name), "w", encoding="utf-8") as qpm_report:
                qpm_report.write(qpm_worker_html_page(chart_payload, payload["name"] + " URMA QPM"))
            if report_mode == "qpm":
                metadata.append({"name": payload["name"], "reportFile": os.path.basename(data_dir) + "/" + qpm_name,
                                 "requestSamples": len(payload["qpm"]["requests"]),
                                 "postSamples": len(payload["qpm"]["posts"]),
                                 "completionSamples": len(payload["qpm"]["completions"])})
        print(f"  - {payload['name']}: URMA_ELAPSED_TOTAL={len(payload['elapsed']['time'])}, "
              f"URMA_POST_AFTER={len(payload['postAfter']['time'])}")
    if report_mode in ("old", "both"):
        with open(html_path, "w", encoding="utf-8") as report_file:
            report_file.write(html_page(metadata, "URMA post/inflight correlation"))
        top_samples = []
        for payload in payloads:
            top_samples.extend((elapsed_ms, payload["name"], timestamp) for elapsed_ms, timestamp in payload["topElapsed"])
        top_workers = {}
        for _, name, _ in sorted(top_samples, reverse=True)[:1000]:
            top_workers[name] = top_workers.get(name, 0) + 1
        top_path = html_path[:-5] + "_top_elapsed.html"
        with open(top_path, "w", encoding="utf-8") as top_file:
            top_file.write(elapsed_top_index_page(sorted(top_workers.items(), key=lambda item: (-item[1], item[0])),
                                                  "Top 1000 URMA elapsed by worker", os.path.basename(html_path)))
    if report_mode in ("qpm", "both"):
        qpm_metadata = []
        for payload in payloads:
            qpm_metadata.append({"name": payload["name"], "reportFile": os.path.basename(data_dir) + "/" + safe_filename(payload["name"], payloads.index(payload))[:-5] + "_qpm.html",
                                 "requestSamples": len(payload["qpm"]["requests"]),
                                 "postSamples": len(payload["qpm"]["posts"]), "completionSamples": len(payload["qpm"]["completions"])})
        qpm_path = html_path[:-5] + "_qpm.html"
        with open(qpm_path, "w", encoding="utf-8") as qpm_file:
            qpm_file.write(qpm_index_page(qpm_metadata, "URMA QPM correlation"))
    if report_mode in ("old", "both"):
        print(f"report: {html_path}")
        print(f"top elapsed index: {html_path[:-5]}_top_elapsed.html")
    if report_mode in ("qpm", "both"):
        print(f"qpm report: {html_path[:-5]}_qpm.html")
    print(f"worker data: {data_dir}")


def read_roots(list_path):
    with open(list_path, "r", encoding="utf-8") as input_file:
        return [line.strip() for line in input_file if line.strip() and not line.lstrip().startswith("#")]


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    input_group = parser.add_mutually_exclusive_group(required=True)
    input_group.add_argument("-f", "--file", help="worker-log root directory")
    input_group.add_argument("--list", help="text file containing one worker-log root per line")
    parser.add_argument("-o", "--out", required=True, help="HTML file for -f, or output directory for --list")
    parser.add_argument("--prefix", help="comma-separated worker-name prefixes to include")
    parser.add_argument("-j", "--jobs", type=int, default=os.cpu_count() or 1, help="worker scan threads")
    parser.add_argument("--downsample", type=int, default=1, help="mean aggregate every N samples")
    parser.add_argument("--report", choices=("old", "qpm", "both"), default="old",
                        help="generate old report, QPM-only report, or both (default: old)")
    args = parser.parse_args()
    if args.jobs < 1 or args.downsample < 1:
        parser.error("--jobs and --downsample must be positive")
    prefixes = [item for item in (args.prefix or "").split(",") if item]
    if args.file:
        if not os.path.isdir(args.file):
            parser.error(f"not a directory: {args.file}")
        generate_report(args.file, args.out, prefixes, args.jobs, args.downsample, args.report)
        return
    roots = read_roots(args.list)
    os.makedirs(args.out, exist_ok=True)
    for root in roots:
        if not os.path.isdir(root):
            print(f"warning: skipped non-directory root: {root}", file=sys.stderr)
            continue
        name = os.path.basename(os.path.normpath(root)) or "report"
        digest = hashlib.sha256(os.path.abspath(root).encode("utf-8")).hexdigest()[:10]
        generate_report(root, os.path.join(args.out, f"{name}_{digest}.html"), prefixes, args.jobs, args.downsample,
                        args.report)


if __name__ == "__main__":
    main()
