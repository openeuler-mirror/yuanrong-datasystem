#!/usr/bin/env python3
"""Self-verifying DataSystem slow/error trace triage.

The analyzer accepts plain log files, directories, and gzip-wrapped tar bundles.
It groups lines by trace id, then produces JSON/Markdown summaries across time,
worker, access flow, latency, breakdown, RPC slow, URMA elapsed, and errors.
"""

import argparse
import gzip
import io
import json
import os
import re
import statistics
import sys
import tarfile
import tempfile
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path


TRACE_ID_RE = re.compile(r"\b[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\b", re.I)
TS_RE = re.compile(r"^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?)")
WORKER_RE = re.compile(r"(kv[^/\s|]*worker[^/\s|]*)", re.I)
ACCESS_RE = re.compile(r"\|\s*(-?\d+)\s*\|\s*([A-Z0-9_]+)\s*\|\s*(\d+)\s*\|\s*(\d+)")
BREAKDOWN_BLOCK_RE = re.compile(r"exceed\s+3ms:\s*\{([^}]*)\}", re.I)
BREAKDOWN_ITEM_RE = re.compile(r"([A-Za-z][A-Za-z0-9_ /.-]*?)\s*:\s*([\d.]+)\s*ms")
RPC_SLOW_RE = re.compile(r"\[(?:ZMQ_)?RPC_FRAMEWORK_SLOW\].*?(?:method=|method:)\s*([A-Za-z0-9_.:/-]+)")
RPC_SLOW_FIELD_RE = re.compile(
    r"\b(e2e_us|client_req_framework_us|remote_processing_us|client_rsp_framework_us|"
    r"server_req_queue_us|server_exec_us|server_rsp_queue_us|network_residual_us)=(\d+)"
)
LATENCY_SUMMARY_RE = re.compile(r"latencySummary:\{([^}]*)\}")
SUMMARY_ITEM_RE = re.compile(r"([A-Za-z][A-Za-z0-9_.-]*)\s*:\s*(\d+)")
URMA_TOTAL_RE = re.compile(r"\[URMA_ELAPSED_TOTAL\].*?cost\s+([\d.]+)ms", re.I)
URMA_POLL_RE = re.compile(r"\[URMA_ELAPSED_POLL_JFC\].*?cost\s+([\d.]+)ms", re.I)
URMA_NOTIFY_RE = re.compile(r"\[URMA_ELAPSED_NOTIFY\].*?cost\s+([\d.]+)ms", re.I)
URMA_THREAD_RE = re.compile(r"\[URMA_ELAPSED_THREAD_SHED\].*?cost\s+([\d.]+)ms", re.I)
URMA_PERF_RE = re.compile(r"\[URMA_PERF\].*?([A-Za-z][A-Za-z0-9_./-]*)\s*[:=]\s*([\d.]+)\s*(us|ms)?", re.I)
ERROR_PATTERNS = (
    "RPC deadline exceeded",
    "URMA_WAIT_TIMEOUT",
    "K_NOT_FOUND",
    "Object in use",
    "Key not found",
    "Etcd is abnormal",
    "fallback payload rejected",
)


def _iter_input_lines(paths):
    for raw_path in paths:
        path = Path(raw_path)
        if path.is_dir():
            for root, _, files in os.walk(path):
                for name in files:
                    yield from _iter_file(Path(root) / name)
        else:
            yield from _iter_file(path)


def _iter_file(path):
    if tarfile.is_tarfile(path):
        with tarfile.open(path, "r:*") as tar:
            for member in tar.getmembers():
                if not member.isfile():
                    continue
                stream = tar.extractfile(member)
                if stream is None:
                    continue
                text = io.TextIOWrapper(stream, encoding="utf-8", errors="replace")
                for line_no, line in enumerate(text, 1):
                    yield str(path), member.name, line_no, line.rstrip("\n")
        return
    opener = gzip.open if path.suffix == ".gz" else open
    try:
        with opener(path, "rt", encoding="utf-8", errors="replace") as f:
            for line_no, line in enumerate(f, 1):
                yield str(path), path.name, line_no, line.rstrip("\n")
    except (OSError, UnicodeError):
        return


def _percentiles(values):
    if not values:
        return {}
    vals = sorted(values)

    def pct(p):
        if len(vals) == 1:
            return vals[0]
        return vals[min(int(len(vals) * p), len(vals) - 1)]

    return {
        "count": len(vals),
        "min": round(vals[0], 3),
        "p50": round(statistics.median(vals), 3),
        "p90": round(pct(0.90), 3),
        "p99": round(pct(0.99), 3),
        "max": round(vals[-1], 3),
    }


def _add_metric(bucket, key, value):
    item = bucket.setdefault(key, {"count": 0, "sum": 0.0, "max": 0.0})
    item["count"] += 1
    item["sum"] = round(item["sum"] + value, 3)
    item["max"] = round(max(item["max"], value), 3)


def _worker_from(source, member, line):
    for text in (member, source, line):
        m = WORKER_RE.search(text)
        if m:
            return m.group(1)
    parts = [p.strip() for p in line.split(" | ")]
    if len(parts) > 3 and parts[3]:
        return parts[3]
    return "unknown"


def _timestamp(line):
    m = TS_RE.search(line)
    if not m:
        return None
    try:
        return datetime.fromisoformat(m.group(1))
    except ValueError:
        return None


def _classify(trace):
    max_access = max(trace["access_latency_ms"] or [0])
    max_urma = max(trace["urma_total_ms"] or [0])
    memory_copy_us = trace["latency_summary_us"].get("client.process.memory_copy", 0)
    set_total_us = trace["latency_summary_us"].get("client.process.set", 0)
    if trace["errors"] and max_urma >= 50:
        return "client_deadline_with_urma_wait"
    if trace["errors"] and max_access and 18 <= max_access <= 25:
        return "client_deadline_20ms"
    if memory_copy_us >= 1000 and memory_copy_us >= max(set_total_us, 1):
        return "write_memory_copy_dominant"
    if trace["errors"]:
        return "deadline_or_error"
    if max_urma >= 50:
        return "remote_fast_transport_wait"
    if trace["rpc_slow"]:
        return "rpc_slow"
    if trace["access_latency_ms"]:
        return "access_latency_only"
    return "unknown"


def analyze_inputs(paths, code_ref="unknown"):
    traces = defaultdict(lambda: {
        "lines": 0,
        "workers": Counter(),
        "timestamps": [],
        "flows": Counter(),
        "access_latency_ms": [],
        "breakdown_ms": Counter(),
        "rpc_slow": Counter(),
        "rpc_slow_fields_us": defaultdict(list),
        "urma_total_ms": [],
        "urma_poll_jfc_ms": [],
        "urma_notify_ms": [],
        "urma_thread_sched_ms": [],
        "urma_perf": Counter(),
        "latency_summary_us": Counter(),
        "latency_summary_raw": [],
        "errors": Counter(),
        "evidence": [],
    })
    all_ts = []
    worker_counts = Counter()
    flow_counts = Counter()
    access_latencies = []
    breakdown = {}
    rpc_slow = defaultdict(lambda: {"count": 0, "fields_us": defaultdict(list)})
    urma = defaultdict(list)
    urma_perf = defaultdict(list)
    latency_summary = defaultdict(list)
    errors = Counter()

    for source, member, line_no, line in _iter_input_lines(paths):
        m = TRACE_ID_RE.search(line)
        if not m:
            continue
        trace_id = m.group(0)
        trace = traces[trace_id]
        trace["lines"] += 1
        worker = _worker_from(source, member, line)
        trace["workers"][worker] += 1
        worker_counts[worker] += 1
        ts = _timestamp(line)
        if ts:
            trace["timestamps"].append(ts.isoformat())
            all_ts.append(ts)
        if len(trace["evidence"]) < 12:
            trace["evidence"].append({"source": source, "member": member, "line": line_no, "text": line})

        access = ACCESS_RE.search(line)
        if access:
            status, operation, duration_us, _size = access.groups()
            latency_ms = int(duration_us) / 1000.0
            trace["flows"][operation] += 1
            flow_counts[operation] += 1
            if status != "0":
                errors[f"status={status}"] += 1
                trace["errors"][f"status={status}"] += 1
            trace["access_latency_ms"].append(latency_ms)
            access_latencies.append(latency_ms)

        block = BREAKDOWN_BLOCK_RE.search(line)
        if block:
            for key, value in BREAKDOWN_ITEM_RE.findall(block.group(1)):
                value_ms = float(value)
                name = " ".join(key.split())
                trace["breakdown_ms"][name] += value_ms
                _add_metric(breakdown, name, value_ms)

        rpc = RPC_SLOW_RE.search(line)
        if rpc:
            method = rpc.group(1)
            trace["rpc_slow"][method] += 1
            rpc_slow[method]["count"] += 1
            for field, raw_value in RPC_SLOW_FIELD_RE.findall(line):
                val = int(raw_value)
                trace["rpc_slow_fields_us"][field].append(val)
                rpc_slow[method]["fields_us"][field].append(val)

        summary = LATENCY_SUMMARY_RE.search(line)
        if summary:
            raw = "latencySummary:{" + summary.group(1) + "}"
            if len(trace["latency_summary_raw"]) < 8:
                trace["latency_summary_raw"].append(raw)
            for key, raw_value in SUMMARY_ITEM_RE.findall(summary.group(1)):
                val = int(raw_value)
                trace["latency_summary_us"][key] += val
                latency_summary[key].append(val)

        for name, regex in (
            ("total", URMA_TOTAL_RE),
            ("poll_jfc", URMA_POLL_RE),
            ("notify", URMA_NOTIFY_RE),
            ("thread_sched", URMA_THREAD_RE),
        ):
            um = regex.search(line)
            if um:
                val = float(um.group(1))
                trace[f"urma_{name}_ms"].append(val)
                urma[name].append(val)

        perf = URMA_PERF_RE.search(line)
        if perf:
            key, raw_value, unit = perf.groups()
            val = float(raw_value)
            if (unit or "ms").lower() == "us":
                val /= 1000.0
            name = " ".join(key.split())
            trace["urma_perf"][name] += val
            urma_perf[name].append(val)

        for pattern in ERROR_PATTERNS:
            if pattern in line:
                errors[pattern] += 1
                trace["errors"][pattern] += 1

    trace_rows = {}
    classifications = Counter()
    for trace_id, trace in traces.items():
        trace["classification"] = _classify(trace)
        classifications[trace["classification"]] += 1
        trace_rows[trace_id] = {
            "classification": trace["classification"],
            "line_count": trace["lines"],
            "workers": dict(trace["workers"]),
            "first_ts": min(trace["timestamps"]) if trace["timestamps"] else None,
            "last_ts": max(trace["timestamps"]) if trace["timestamps"] else None,
            "flows": dict(trace["flows"]),
            "access_latency_ms": _percentiles(trace["access_latency_ms"]),
            "breakdown_ms": {k: round(v, 3) for k, v in trace["breakdown_ms"].items()},
            "rpc_slow": dict(trace["rpc_slow"]),
            "rpc_slow_fields_us": {k: _percentiles(v) for k, v in sorted(trace["rpc_slow_fields_us"].items())},
            "urma_elapsed_ms": {
                "total": _percentiles(trace["urma_total_ms"]),
                "poll_jfc": _percentiles(trace["urma_poll_jfc_ms"]),
                "notify": _percentiles(trace["urma_notify_ms"]),
                "thread_sched": _percentiles(trace["urma_thread_sched_ms"]),
            },
            "urma_perf_ms": {k: round(v, 3) for k, v in trace["urma_perf"].items()},
            "latency_summary_us": dict(trace["latency_summary_us"]),
            "latency_summary_raw": trace["latency_summary_raw"],
            "errors": dict(trace["errors"]),
            "evidence": trace["evidence"],
        }

    return {
        "schema_version": 1,
        "code_ref": code_ref,
        "inputs": [str(p) for p in paths],
        "trace_count": len(trace_rows),
        "dimensions": {
            "time": {
                "first_ts": min(all_ts).isoformat() if all_ts else None,
                "last_ts": max(all_ts).isoformat() if all_ts else None,
            },
            "workers": {k: {"line_count": v} for k, v in worker_counts.most_common()},
            "flow": dict(flow_counts),
            "latency_ms": {"access": _percentiles(access_latencies)},
            "breakdown_ms": breakdown,
            "rpc_slow": {
                k: {
                    "count": v["count"],
                    **{field: _percentiles(vals) for field, vals in sorted(v["fields_us"].items())},
                }
                for k, v in sorted(rpc_slow.items())
            },
            "urma_elapsed": {k: _percentiles(v) for k, v in sorted(urma.items())},
            "urma_perf_ms": {k: _percentiles(v) for k, v in sorted(urma_perf.items())},
            "latency_summary_us": {k: _percentiles(v) for k, v in sorted(latency_summary.items())},
            "errors": dict(errors),
            "classifications": dict(classifications),
        },
        "traces": trace_rows,
    }


def render_markdown(report):
    lines = [
        "# Trace Triage Summary",
        "",
        f"- code_ref: `{report['code_ref']}`",
        f"- trace_count: {report['trace_count']}",
        f"- time_range: {report['dimensions']['time']['first_ts']} -> {report['dimensions']['time']['last_ts']}",
        "",
        "## Top Workers",
    ]
    for worker, item in list(report["dimensions"]["workers"].items())[:10]:
        lines.append(f"- {worker}: {item['line_count']} lines")
    lines.extend(["", "## Flow"])
    for flow, count in report["dimensions"]["flow"].items():
        lines.append(f"- {flow}: {count}")
    lines.extend(["", "## Access Latency Ms", "```json", json.dumps(report["dimensions"]["latency_ms"], indent=2), "```"])
    lines.extend(["", "## Breakdown Ms"])
    for key, item in sorted(report["dimensions"]["breakdown_ms"].items(), key=lambda kv: kv[1]["sum"], reverse=True):
        lines.append(f"- {key}: count={item['count']} sum={item['sum']} max={item['max']}")
    lines.extend(["", "## RPC Slow"])
    for method, item in report["dimensions"]["rpc_slow"].items():
        fields = " ".join(f"{k}={v}" for k, v in item.items() if k != "count")
        lines.append(f"- {method}: count={item['count']} {fields}".rstrip())
    lines.extend(["", "## URMA Elapsed"])
    for name, item in report["dimensions"]["urma_elapsed"].items():
        lines.append(f"- {name}: {item}")
    lines.extend(["", "## Latency Summary Us"])
    for name, item in report["dimensions"]["latency_summary_us"].items():
        lines.append(f"- {name}: {item}")
    lines.extend(["", "## Errors"])
    for error, count in report["dimensions"]["errors"].items():
        lines.append(f"- {error}: {count}")
    lines.extend(["", "## Classifications"])
    for name, count in report["dimensions"]["classifications"].items():
        lines.append(f"- {name}: {count}")
    lines.extend(["", "## Trace Classifications"])
    for trace_id, item in sorted(report["traces"].items()):
        lines.append(f"- {trace_id}: {item['classification']}")
    return "\n".join(lines) + "\n"


def _make_self_test_bundle(path):
    trace_id = "019f7b27-56f0-74f0-9a68-5b3742f11e23"
    content = "\n".join([
        f"2026-07-18T19:20:03.100000 | INFO | access_recorder | 192.168.168.206 | 42 | {trace_id} | - | 0 | DS_KV_CLIENT_GET | 518923 | 4096",
        f"2026-07-18T19:20:03.110000 | INFO | client | 192.168.168.206 | 42 | {trace_id} | Get done latencySummary:{{client.rpc.get:20298, client.process.get:10}}",
        f"2026-07-18T19:20:03.130000 | INFO | worker | 192.168.168.206 | 42 | {trace_id} | [Get] Done, totalCost: 518.9ms, exceed 3ms: {{ ProcessGetObjectRequest: 517 ms, QueryMeta: 0 ms }}",
        f"2026-07-18T19:20:03.150000 | WARN | worker | 192.168.168.206 | 42 | {trace_id} | [ZMQ_RPC_FRAMEWORK_SLOW] e2e_us=8012 client_req_framework_us=100 remote_processing_us=7600 client_rsp_framework_us=120 server_req_queue_us=20 server_exec_us=7500 server_rsp_queue_us=80 network_residual_us=292 method=WorkerOCService.Get",
        f"2026-07-18T19:20:03.200000 | WARN | worker | 192.168.233.92 | 42 | {trace_id} | [URMA_ELAPSED_TOTAL] cost 517.732ms, request id:77, src address:192.168.233.92:31501, target address:192.168.168.206:31501, dataSize:4194304, cpuid:12, status: OK",
        f"2026-07-18T19:20:03.201000 | WARN | worker | 192.168.233.92 | 42 | {trace_id} | [URMA_ELAPSED_POLL_JFC] cost 0.309ms, request id:77",
        f"2026-07-18T19:20:03.202000 | WARN | worker | 192.168.233.92 | 42 | {trace_id} | [URMA_ELAPSED_NOTIFY] cost 0.041ms, request id:77",
        f"2026-07-18T19:20:03.203000 | WARN | worker | 192.168.233.92 | 42 | {trace_id} | [URMA_ELAPSED_THREAD_SHED] cost 12.500ms, request id:77",
        f"2026-07-18T19:20:03.230000 | ERROR | worker | 192.168.168.206 | 42 | {trace_id} | RPC deadline exceeded while waiting WorkerOCService.Get",
    ])
    with tarfile.open(path, "w:gz") as tar:
        data = content.encode("utf-8")
        info = tarfile.TarInfo("kvchachjpworker-0-worker7/worker.log")
        info.size = len(data)
        tar.addfile(info, io.BytesIO(data))


def run_self_test():
    with tempfile.TemporaryDirectory(prefix="ds-trace-triage-") as tmp:
        bundle = Path(tmp) / "fixture.tar.gz"
        _make_self_test_bundle(bundle)
        report = analyze_inputs([str(bundle)], code_ref="self-test")
    assert report["trace_count"] == 1
    assert report["dimensions"]["latency_ms"]["access"]["p50"] == 518.923
    assert report["dimensions"]["breakdown_ms"]["ProcessGetObjectRequest"]["sum"] == 517.0
    assert report["dimensions"]["urma_elapsed"]["total"]["p50"] == 517.732
    assert report["dimensions"]["urma_elapsed"]["poll_jfc"]["p50"] == 0.309
    assert report["dimensions"]["rpc_slow"]["WorkerOCService.Get"]["server_exec_us"]["p50"] == 7500
    assert report["dimensions"]["latency_summary_us"]["client.rpc.get"]["p50"] == 20298
    assert report["dimensions"]["errors"]["RPC deadline exceeded"] == 1
    assert report["dimensions"]["classifications"]["client_deadline_with_urma_wait"] == 1
    report["self_test"] = True
    return report


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("inputs", nargs="*", help="Log files, directories, or gzip-wrapped tar bundles.")
    parser.add_argument("--code-ref", default="unknown", help="Source ref used for CodeGraph/source validation.")
    parser.add_argument("--output-json", help="Write machine-readable summary JSON.")
    parser.add_argument("--output-md", help="Write Markdown summary.")
    parser.add_argument("--self-test", action="store_true", help="Run the built-in fixture and validate parser behavior.")
    args = parser.parse_args(argv)

    if args.self_test:
        report = run_self_test()
        print("self-test passed")
    else:
        if not args.inputs:
            parser.error("inputs are required unless --self-test is used")
        report = analyze_inputs(args.inputs, code_ref=args.code_ref)

    text = json.dumps(report, ensure_ascii=False, indent=2)
    if args.output_json:
        Path(args.output_json).write_text(text + "\n", encoding="utf-8")
    else:
        print(text)
    if args.output_md:
        Path(args.output_md).write_text(render_markdown(report), encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
