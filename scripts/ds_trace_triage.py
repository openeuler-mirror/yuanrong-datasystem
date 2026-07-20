#!/usr/bin/env python3
"""Self-verifying DataSystem slow/error trace triage.

The analyzer accepts plain log files, directories, and gzip-wrapped tar bundles.
It groups lines by trace id, then produces JSON/Markdown summaries across time,
worker, access flow, latency, breakdown, RPC slow, URMA elapsed, and errors.
"""

import argparse
import gzip
import hashlib
import io
import json
import os
import re
import shutil
import statistics
import subprocess
import sys
import tarfile
import tempfile
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path


TRACE_ID_RE = re.compile(r"\b[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\b", re.I)
TS_RE = re.compile(r"(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?)")
WORKER_RE = re.compile(r"(kv[^/\s|]*worker[^/\s|]*)", re.I)
IP_RE = re.compile(r"\b(?:\d{1,3}\.){3}\d{1,3}(?::\d+)?\b")
NOISE_ON_LABEL = "有底噪(dizao)"
NOISE_OFF_LABEL = "无底噪(wudizao)"
DEFAULT_SITE_HTML_MAX_BYTES = 2 * 1024 * 1024
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
URMA_POLL_RE = re.compile(r"\[URMA_ELAPSED_POLL_JFC\].*?cost\s+([\d.]+)\s*(us|ms)", re.I)
URMA_NOTIFY_RE = re.compile(r"\[URMA_ELAPSED_NOTIFY\].*?cost\s+([\d.]+)\s*(us|ms)", re.I)
URMA_THREAD_RE = re.compile(r"\[URMA_ELAPSED_THREAD_SHED\].*?cost\s+([\d.]+)\s*(us|ms)", re.I)
URMA_PERF_RE = re.compile(r"\[URMA_PERF\].*?([A-Za-z][A-Za-z0-9_./-]*)\s*[:=]\s*([\d.]+)\s*(us|ms)?", re.I)
REQUEST_ID_RE = re.compile(r"(?:request id\s*:|requestId[:=])\s*([A-Za-z0-9_-]+)", re.I)
SRC_ADDR_RE = re.compile(r"src address:([^\s,]+)", re.I)
DST_ADDR_RE = re.compile(r"(?:target|dst) address:([^\s,]+)", re.I)
DATA_SIZE_RE = re.compile(r"dataSize:(\d+)|size\[(\d+)\]", re.I)
CPUID_RE = re.compile(r"cpuid:\s*(\d+)", re.I)
STATUS_RE = re.compile(r"status:\s*([^,]+)", re.I)
WAIT_OS_RE = re.compile(r"wait os sched.*?:\s*([\d.]+)ms", re.I)
INFLIGHT_WR_RE = re.compile(r"urma_inflight_wr_count:\s*(\d+)", re.I)
TRANSFER_PATH_RE = re.compile(r"(?:transferPath|path):\s*(UB|RDMA|TCP)\b", re.I)
INFLIGHT_REMOTE_GET_RE = re.compile(r"inflightRemoteGet:\s*(\d+)", re.I)
REMOTE_GET_REQUEST_RE = re.compile(r"Remote get request:\[([^\]]+)\]\s+object:\[([^\]]*)\].*?offset\[(\d+)\]\s+size\[(\d+)\]", re.I)
ERROR_PATTERNS = [
    "RPC deadline exceeded",
    "URMA_WAIT_TIMEOUT",
    "K_NOT_FOUND",
    "Object in use",
    "Key not found",
    "Etcd is abnormal",
    "fallback payload rejected",
]


class ParserRules:
    """Mutable parser extension rules for one analyzer instance."""

    def __init__(self, error_patterns=None, custom_metric_rules=None):
        self.error_patterns = list(error_patterns or ERROR_PATTERNS)
        self.custom_metric_rules = list(custom_metric_rules or [])

    def register_error_pattern(self, pattern):
        if pattern not in self.error_patterns:
            self.error_patterns.append(pattern)

    def register_metric_rule(self, name, pattern, value_group=1, unit_group=None):
        self.custom_metric_rules.append({
            "name": name,
            "regex": re.compile(pattern, re.I),
            "value_group": value_group,
            "unit_group": unit_group,
        })


DEFAULT_RULES = ParserRules()


class TraceInputReader:
    """Read log lines from files, directories, gzip files, and tar bundles."""

    def iter_lines(self, paths):
        for raw_path in paths:
            path = Path(raw_path)
            if path.is_dir():
                for root, _, files in os.walk(path):
                    for name in files:
                        yield from self.iter_file(Path(root) / name)
            else:
                yield from self.iter_file(path)

    def iter_file(self, path):
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


class TraceParser:
    """Parse one log line into trace-scoped facts without aggregating them."""

    def __init__(self, rules=None):
        self.rules = rules or DEFAULT_RULES

    def worker_from(self, source, member, line):
        for text in (member, source, line):
            m = WORKER_RE.search(text)
            if m:
                return m.group(1)
        parts = [p.strip() for p in line.split(" | ")]
        if len(parts) > 3 and parts[3]:
            return parts[3]
        return "unknown"

    def timestamp(self, line):
        m = TS_RE.search(line)
        if not m:
            return None
        try:
            return datetime.fromisoformat(m.group(1))
        except ValueError:
            return None

    def parse_line(self, source, member, line_no, line):
        match = TRACE_ID_RE.search(line)
        if not match:
            return None
        worker = self.worker_from(source, member, line)
        ts = self.timestamp(line)
        parsed = {
            "trace_id": match.group(0),
            "worker": worker,
            "timestamp": ts,
            "evidence": {"source": source, "member": member, "line": line_no, "text": line},
            "ub_events": _extract_ub_events(source, member, line_no, line, ts, worker),
            "errors": [],
            "custom_metrics_ms": {},
        }
        for rule in self.rules.custom_metric_rules:
            cm = rule["regex"].search(line)
            if cm:
                unit = cm.group(rule["unit_group"]) if rule["unit_group"] else "ms"
                parsed["custom_metrics_ms"][rule["name"]] = _ms(cm.group(rule["value_group"]), unit)
        for pattern in self.rules.error_patterns:
            if pattern in line:
                parsed["errors"].append(pattern)
        return parsed


def register_error_pattern(pattern):
    """Register a literal error marker for evolved DataSystem log wording."""
    DEFAULT_RULES.register_error_pattern(pattern)


def register_metric_rule(name, pattern, value_group=1, unit_group=None):
    """Register a custom latency metric extracted as ms from a regex match."""
    DEFAULT_RULES.register_metric_rule(name, pattern, value_group=value_group, unit_group=unit_group)


def _iter_input_lines(paths):
    yield from TraceInputReader().iter_lines(paths)


def _iter_file(path):
    yield from TraceInputReader().iter_file(path)


def _has_noise_token(text):
    lowered = text.lower()
    return any(token in lowered for token in ("dizao", "wudizao", "底噪"))


def _is_noise_off(text):
    lowered = text.lower()
    return any(token in lowered for token in ("wudizao", "wu-dizao", "wu_dizao", "无底噪"))


def _is_noise_on(text):
    lowered = text.lower()
    if _is_noise_off(lowered):
        return False
    return "dizao" in lowered or "底噪" in lowered


def _noise_context_for_path(path):
    path = Path(path)
    return "/".join(part for part in (path.parent.name, path.name) if part)


def _detect_noise_cohort_mode(paths):
    for raw_path in paths:
        path = Path(raw_path)
        if _has_noise_token(_noise_context_for_path(path)):
            return True
        if path.is_dir():
            for root, _, files in os.walk(path):
                root_path = Path(root)
                root_context = "/".join(part for part in (root_path.parent.name, root_path.name) if part)
                if _has_noise_token(root_context) or any(_has_noise_token(name) for name in files):
                    return True
        elif path.exists() and tarfile.is_tarfile(path):
            try:
                with tarfile.open(path, "r:*") as tar:
                    if any(_has_noise_token(member.name) for member in tar.getmembers()):
                        return True
            except tarfile.TarError:
                continue
    return False


def _source_cohort_label(source, member, noise_cohort_mode=False):
    text = f"{_noise_context_for_path(source)}/{member}"
    if _is_noise_off(text):
        return NOISE_OFF_LABEL
    if _is_noise_on(text):
        return NOISE_ON_LABEL
    if noise_cohort_mode:
        return NOISE_OFF_LABEL
    return Path(source).name


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
    return TraceParser().worker_from(source, member, line)


def _timestamp(line):
    return TraceParser().timestamp(line)


def _ms(raw_value, unit):
    value = float(raw_value)
    return value / 1000.0 if (unit or "ms").lower() == "us" else value


def _first_match(regex, line):
    match = regex.search(line)
    return match.group(1).strip() if match else None


def _int_match(regex, line):
    match = regex.search(line)
    if not match:
        return None
    for group in match.groups():
        if group:
            return int(group)
    return None


def _ub_base_event(event_type, source, member, line_no, line, ts, worker):
    event = {
        "event_type": event_type,
        "timestamp": ts.isoformat() if ts else None,
        "worker": worker,
        "source": source,
        "member": member,
        "line": line_no,
        "raw": line,
    }
    request_id = _first_match(REQUEST_ID_RE, line)
    if request_id:
        event["request_id"] = request_id
    src_addr = _first_match(SRC_ADDR_RE, line)
    if src_addr:
        event["src_addr"] = src_addr
    dst_addr = _first_match(DST_ADDR_RE, line)
    if dst_addr:
        event["target_addr"] = dst_addr
    data_size = _int_match(DATA_SIZE_RE, line)
    if data_size is not None:
        event["data_size"] = data_size
    cpuid = _int_match(CPUID_RE, line)
    if cpuid is not None:
        event["cpuid"] = cpuid
    status = _first_match(STATUS_RE, line)
    if status:
        event["status"] = status
    return event


def _extract_ub_events(source, member, line_no, line, ts, worker):
    events = []
    transfer = TRANSFER_PATH_RE.search(line)
    if transfer:
        event = _ub_base_event("transfer_path", source, member, line_no, line, ts, worker)
        event["transfer_path"] = transfer.group(1).upper()
        inflight = _int_match(INFLIGHT_REMOTE_GET_RE, line)
        if inflight is not None:
            event["inflight_remote_get"] = inflight
        cost = re.search(r"cost:\s*([\d.]+)ms|totalCost:\s*([\d.]+)ms", line, re.I)
        if cost:
            event["cost_ms"] = float(next(group for group in cost.groups() if group))
        events.append(event)

    request = REMOTE_GET_REQUEST_RE.search(line)
    if request:
        request_id, object_key, offset, read_size = request.groups()
        event = _ub_base_event("remote_get_start", source, member, line_no, line, ts, worker)
        event.update({
            "request_id": request_id,
            "object_key": object_key,
            "offset": int(offset),
            "read_size": int(read_size),
        })
        events.append(event)

    total = URMA_TOTAL_RE.search(line)
    if total:
        event = _ub_base_event("total", source, member, line_no, line, ts, worker)
        event["cost_ms"] = float(total.group(1))
        wait_os = _first_match(WAIT_OS_RE, line)
        if wait_os:
            event["wait_os_sched_ms"] = float(wait_os)
        inflight_wr = _int_match(INFLIGHT_WR_RE, line)
        if inflight_wr is not None:
            event["urma_inflight_wr_count"] = inflight_wr
        events.append(event)

    for event_type, regex in (("poll_jfc", URMA_POLL_RE), ("notify", URMA_NOTIFY_RE), ("thread_sched", URMA_THREAD_RE)):
        match = regex.search(line)
        if match:
            event = _ub_base_event(event_type, source, member, line_no, line, ts, worker)
            event["cost_ms"] = _ms(match.group(1), match.group(2))
            count = _int_match(re.compile(r"count:\s*(\d+)", re.I), line)
            if count is not None:
                event["count"] = count
            events.append(event)

    return events


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


def _stage(stage, duration_ms=None, confidence="missing", source="missing", fields=None):
    row = {"stage": stage, "confidence": confidence, "source": source}
    if duration_ms is not None:
        row["duration_ms"] = round(duration_ms, 3)
    if fields:
        row["fields"] = fields
    return row


def _build_stage_breakdown(trace):
    flows = trace["flows"]
    summary = trace["latency_summary_us"]
    ub_events = trace["ub_events"]
    breakdown = []
    missing = []
    is_write = any(flow in flows for flow in ("DS_KV_CLIENT_SET", "DS_KV_CLIENT_CREATE", "DS_KV_CLIENT_PUBLISH"))

    if is_write:
        create_us = summary.get("client.rpc.create")
        publish_us = summary.get("client.rpc.publish")
        memory_us = summary.get("client.process.memory_copy")
        meta_us = summary.get("worker.rpc.create_meta")
        breakdown.append(_stage("write.client_to_entry_createbuffer", create_us / 1000.0 if create_us else None,
                                "high" if create_us else "missing", "latencySummary client.rpc.create"))
        breakdown.append(_stage("write.client_memory_copy", memory_us / 1000.0 if memory_us else None,
                                "high" if memory_us else "missing", "latencySummary client.process.memory_copy"))
        breakdown.append(_stage("write.client_to_entry_publish", publish_us / 1000.0 if publish_us else None,
                                "high" if publish_us else "missing", "latencySummary client.rpc.publish"))
        breakdown.append(_stage("write.entry_to_meta_publish", meta_us / 1000.0 if meta_us else None,
                                "high" if meta_us else "missing", "latencySummary worker.rpc.create_meta"))
        if not meta_us:
            missing.append({
                "stage": "write.entry_to_meta_publish",
                "expected": ["worker.rpc.create_meta", "MasterOCService.CreateMeta rpc slow"],
                "impact": "cannot split Publish metadata update from entry worker processing",
                "fallback": "mark missing",
            })
        return breakdown, missing

    client_ms = None
    if summary.get("client.rpc.get"):
        client_ms = summary["client.rpc.get"] / 1000.0
    elif trace["access_latency_ms"]:
        client_ms = max(trace["access_latency_ms"])
    remote_costs = [
        event["cost_ms"] for event in ub_events
        if event.get("event_type") == "transfer_path" and event.get("cost_ms") is not None
    ]
    total_costs = [
        event["cost_ms"] for event in ub_events
        if event.get("event_type") == "total" and event.get("cost_ms") is not None
    ]
    qmeta_us = summary.get("worker.rpc.query_meta")
    breakdown.append(_stage("read.client_to_entry_worker", client_ms, "high" if client_ms is not None else "missing",
                            "client access or latencySummary client.rpc.get"))
    breakdown.append(_stage("read.entry_to_meta_worker", qmeta_us / 1000.0 if qmeta_us else None,
                            "high" if qmeta_us else "missing", "latencySummary worker.rpc.query_meta"))
    breakdown.append(_stage("read.entry_to_data_worker", max(remote_costs) if remote_costs else None,
                            "high" if remote_costs else "missing", "Remote get success / worker.rpc.remote_get"))
    breakdown.append(_stage("read.data_worker_ub_write", max(total_costs) if total_costs else None,
                            "high" if total_costs else "missing", "URMA_ELAPSED_TOTAL"))
    if not qmeta_us:
        missing.append({
            "stage": "read.entry_to_meta_worker",
            "expected": ["worker.rpc.query_meta", "QueryMeta rpc slow"],
            "impact": "cannot split meta lookup from entry worker processing",
            "fallback": "mark missing; keep client_to_entry_worker as observed upper bound",
        })
    return breakdown, missing


def _evidence_coverage(trace):
    has_client = bool(trace["flows"]) or any(k.startswith("client.") for k in trace["latency_summary_us"])
    has_entry = any(event["event_type"] in ("transfer_path", "remote_get_start") for event in trace["ub_events"])
    has_data = any(event["event_type"] in ("total", "poll_jfc", "notify", "thread_sched") for event in trace["ub_events"])
    has_meta = bool(trace["latency_summary_us"].get("worker.rpc.query_meta")
                    or trace["latency_summary_us"].get("worker.rpc.create_meta"))
    return {
        "client": "present" if has_client else "missing",
        "entry_worker": "present" if has_entry else "missing",
        "meta_worker": "present" if has_meta else "missing",
        "data_worker": "present" if has_data else "missing",
        "urma": "present" if has_data else "missing",
        "clock_alignment": "same_host_or_unknown",
    }


def _surface_status(count):
    return "present" if count else "missing"


class TraceAnalyzer:
    """Coordinate input reading, line parsing, trace accumulation, and dimensions."""

    def __init__(self, reader=None, parser=None, rules=None):
        self.rules = rules or DEFAULT_RULES
        self.reader = reader or TraceInputReader()
        self.parser = parser or TraceParser(self.rules)

    def analyze(self, paths, code_ref="unknown"):
        return _analyze_inputs(paths, code_ref=code_ref, reader=self.reader, parser=self.parser, rules=self.rules)


def _analyze_inputs(paths, code_ref="unknown", reader=None, parser=None, rules=None):
    reader = reader or TraceInputReader()
    parser = parser or TraceParser(rules or DEFAULT_RULES)
    noise_cohort_mode = _detect_noise_cohort_mode(paths)
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
        "custom_metrics_ms": Counter(),
        "ub_events": [],
        "latency_summary_us": Counter(),
        "latency_summary_raw": [],
        "errors": Counter(),
        "evidence": [],
        "input_sources": Counter(),
    })
    all_ts = []
    worker_counts = Counter()
    flow_counts = Counter()
    access_latencies = []
    breakdown = {}
    rpc_slow = defaultdict(lambda: {"count": 0, "fields_us": defaultdict(list)})
    urma = defaultdict(list)
    urma_perf = defaultdict(list)
    custom_metrics = defaultdict(list)
    latency_summary = defaultdict(list)
    errors = Counter()
    ub_summary = {"transfer_path": Counter(), "edges": defaultdict(lambda: {"count": 0, "latencies": []})}
    surface_counts = Counter()

    for source, member, line_no, line in reader.iter_lines(paths):
        parsed = parser.parse_line(source, member, line_no, line)
        if not parsed:
            continue
        trace_id = parsed["trace_id"]
        trace = traces[trace_id]
        trace["lines"] += 1
        trace["input_sources"][_source_cohort_label(source, member, noise_cohort_mode)] += 1
        worker = parsed["worker"]
        trace["workers"][worker] += 1
        worker_counts[worker] += 1
        ts = parsed["timestamp"]
        if ts:
            trace["timestamps"].append(ts.isoformat())
            all_ts.append(ts)
        trace["evidence"].append(parsed["evidence"])

        for event in parsed["ub_events"]:
            trace["ub_events"].append(event)
            if event.get("transfer_path"):
                ub_summary["transfer_path"][event["transfer_path"]] += 1
            if event.get("src_addr") and event.get("target_addr") and event.get("event_type") == "total":
                edge = f"{event['src_addr']} -> {event['target_addr']}"
                ub_summary["edges"][edge]["count"] += 1
                if event.get("cost_ms") is not None:
                    ub_summary["edges"][edge]["latencies"].append(event["cost_ms"])

        access = ACCESS_RE.search(line)
        if access:
            surface_counts["client_access"] += 1
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
            surface_counts["rpc_slow"] += 1
            method = rpc.group(1)
            trace["rpc_slow"][method] += 1
            rpc_slow[method]["count"] += 1
            for field, raw_value in RPC_SLOW_FIELD_RE.findall(line):
                val = int(raw_value)
                trace["rpc_slow_fields_us"][field].append(val)
                rpc_slow[method]["fields_us"][field].append(val)

        summary = LATENCY_SUMMARY_RE.search(line)
        if summary:
            surface_counts["latency_summary"] += 1
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
                surface_counts["urma_elapsed"] += 1
                val = _ms(um.group(1), um.group(2) if len(um.groups()) > 1 else "ms")
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

        for name, val in parsed["custom_metrics_ms"].items():
            trace["custom_metrics_ms"][name] += val
            custom_metrics[name].append(val)

        for pattern in parsed["errors"]:
            surface_counts["error"] += 1
            errors[pattern] += 1
            trace["errors"][pattern] += 1

    trace_rows = {}
    classifications = Counter()
    worker_roles = defaultdict(set)
    worker_trace_ids = defaultdict(set)
    worker_slow_counts = Counter()
    worker_error_counts = Counter()
    for trace_id, trace in traces.items():
        trace["classification"] = _classify(trace)
        classifications[trace["classification"]] += 1
        triage_flags = []
        if trace["errors"] and max(trace["urma_total_ms"] or [0]) > max(trace["access_latency_ms"] or [0]):
            triage_flags.append("late_worker_completion")
        for worker in trace["workers"]:
            worker_trace_ids[worker].add(trace_id)
            if trace["classification"] not in ("unknown", "access_latency_only"):
                worker_slow_counts[worker] += 1
            if trace["errors"]:
                worker_error_counts[worker] += sum(trace["errors"].values())
        for event in trace["ub_events"]:
            if event["event_type"] in ("transfer_path", "remote_get_start"):
                worker_roles[event["worker"]].add("entry_worker")
            if event["event_type"] in ("total", "poll_jfc", "notify", "thread_sched"):
                worker_roles[event["worker"]].add("data_worker")
        stage_breakdown, missing_evidence = _build_stage_breakdown(trace)
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
            "custom_metrics_ms": {k: round(v, 3) for k, v in trace["custom_metrics_ms"].items()},
            "ub_events": trace["ub_events"],
            "latency_summary_us": dict(trace["latency_summary_us"]),
            "latency_summary_raw": trace["latency_summary_raw"],
            "errors": dict(trace["errors"]),
            "input_sources": sorted(trace["input_sources"]),
            "triage_flags": triage_flags,
            "stage_breakdown": stage_breakdown,
            "evidence_coverage": _evidence_coverage(trace),
            "missing_evidence": missing_evidence,
            "evidence": trace["evidence"],
        }

    worker_summary = {}
    for worker, item in worker_counts.most_common():
        roles = sorted(worker_roles.get(worker) or {"unknown"})
        worker_summary[worker] = {
            "roles": roles,
            "line_count": item,
            "trace_count": len(worker_trace_ids[worker]),
            "slow_trace_count": worker_slow_counts[worker],
            "error_count": worker_error_counts[worker],
            "coverage": {
                "urma": "present" if "data_worker" in roles else "missing",
                "remote_get": "present" if "entry_worker" in roles else "missing",
            },
        }

    worker_edges = {}
    for edge, item in ub_summary["edges"].items():
        worker_edges[edge] = {"count": item["count"], "p99_ms": _percentiles(item["latencies"]).get("p99")}

    time_buckets = _build_time_buckets(trace_rows, 1000)
    cohorts = _build_cohorts(trace_rows)
    coverage = {
        "surfaces": {
            "client_access": {"events": surface_counts["client_access"],
                              "status": _surface_status(surface_counts["client_access"])},
            "rpc_slow": {"events": surface_counts["rpc_slow"],
                         "status": _surface_status(surface_counts["rpc_slow"])},
            "latency_summary": {"events": surface_counts["latency_summary"],
                                "status": _surface_status(surface_counts["latency_summary"])},
            "urma_elapsed": {"events": surface_counts["urma_elapsed"],
                             "status": _surface_status(surface_counts["urma_elapsed"])},
            "error": {"events": surface_counts["error"], "status": _surface_status(surface_counts["error"])},
        }
    }
    diagnosis = _build_diagnosis(
        errors=errors,
        classifications=classifications,
        access_latencies=access_latencies,
        coverage=coverage,
        cohorts=cohorts,
    )
    recommendations = _build_recommendations(
        classifications=classifications,
        coverage=coverage,
        cohorts=cohorts,
        ub_summary=ub_summary,
    )
    source_appendix = _build_source_appendix(coverage)
    flow_stages = _build_flow_stages(coverage, flow_counts, ub_summary, trace_rows)

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
            "time_buckets": {"1000ms": time_buckets, "10000ms": _build_time_buckets(trace_rows, 10000)},
            "workers": {k: {"line_count": v} for k, v in worker_counts.most_common()},
            "worker_summary": worker_summary,
            "cohorts": cohorts,
            "worker_edges": worker_edges,
            "coverage": coverage,
            "diagnosis": diagnosis,
            "recommendations": recommendations,
            "source_appendix": source_appendix,
            "flow_stages": flow_stages,
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
            "ub_summary": {
                "transfer_path": dict(ub_summary["transfer_path"]),
                "edges": {edge: {"count": item["count"], "latency_ms": _percentiles(item["latencies"])}
                          for edge, item in sorted(ub_summary["edges"].items())},
            },
            "urma_perf_ms": {k: _percentiles(v) for k, v in sorted(urma_perf.items())},
            "custom_metrics_ms": {k: _percentiles(v) for k, v in sorted(custom_metrics.items())},
            "latency_summary_us": {k: _percentiles(v) for k, v in sorted(latency_summary.items())},
            "errors": dict(errors),
            "classifications": dict(classifications),
        },
        "traces": trace_rows,
    }


def analyze_inputs(paths, code_ref="unknown"):
    return TraceAnalyzer().analyze(paths, code_ref=code_ref)


def _build_cohorts(trace_rows):
    cohorts = defaultdict(lambda: {
        "trace_ids": set(),
        "errors": Counter(),
        "classifications": Counter(),
        "access_latencies": [],
        "workers": Counter(),
    })
    for trace_id, trace in trace_rows.items():
        sources = trace.get("input_sources") or ["unknown"]
        for source in sources:
            cohort = cohorts[source]
            cohort["trace_ids"].add(trace_id)
            cohort["errors"].update(trace.get("errors", {}))
            cohort["classifications"][trace.get("classification", "unknown")] += 1
            if trace.get("access_latency_ms", {}).get("p50") is not None:
                cohort["access_latencies"].append(trace["access_latency_ms"]["p50"])
            cohort["workers"].update(trace.get("workers", {}))
    rows = {}
    for source, cohort in sorted(cohorts.items()):
        rows[source] = {
            "trace_count": len(cohort["trace_ids"]),
            "errors": dict(cohort["errors"]),
            "classifications": dict(cohort["classifications"]),
            "access_latency_ms": _percentiles(cohort["access_latencies"]),
            "top_workers": {k: v for k, v in cohort["workers"].most_common(10)},
        }
    return rows


def _build_diagnosis(errors, classifications, access_latencies, coverage, cohorts):
    top_error, top_error_count = (errors.most_common(1)[0] if errors else ("none", 0))
    top_class, top_class_count = (classifications.most_common(1)[0] if classifications else ("unknown", 0))
    access = _percentiles(access_latencies)
    surfaces = coverage.get("surfaces", {})
    present = [name for name, item in surfaces.items() if item.get("status") == "present"]
    missing = [name for name, item in surfaces.items() if item.get("status") != "present"]
    cohort_count = len(cohorts)
    cohort_text = (
        f"输入被拆成 {cohort_count} 个 cohort，报告应先比较每个输入包的分布再合并判断。"
        if cohort_count > 1 else "当前只有一个输入 cohort，重点看该输入内部的 trace/error/worker 分布。"
    )
    return {
        "symptom_line": {
            "label": "错误线",
            "text": f"主要失败表象是 {top_error}（{top_error_count} 次），用于回答客户为什么看到失败。",
        },
        "latency_line": {
            "label": "慢时延线",
            "text": (
                f"access p50={access.get('p50', '')}ms、p99={access.get('p99', '')}ms、max={access.get('max', '')}ms；"
                "再结合 latencySummary、breakdown、RPC slow、URMA/UB edge 判断时间花在哪里。"
            ),
        },
        "evidence_boundary": {
            "label": "证据边界",
            "text": (
                f"已观测面：{', '.join(present) or 'none'}；缺失/未采样面：{', '.join(missing) or 'none'}。"
                "缺失项只能标为观测盲区，不能直接当根因。"
            ),
        },
        "customer_expression": {
            "label": "客户表达",
            "text": (
                f"建议描述为“{top_class} 是当前最大根因族（{top_class_count} 条 trace）”。"
                f"{cohort_text} 该判断来自日志聚合的 observed evidence，源码/CodeGraph 复核应另列。"
            ),
        },
    }


def _build_recommendations(classifications, coverage, cohorts, ub_summary):
    surfaces = coverage.get("surfaces", {})
    recommendations = [{
        "category": "source_validation",
        "title": "固定源码 ref 并用 CodeGraph/源码复核调用链",
        "detail": "报告中的根因族来自日志聚合，应继续用 main/master 对应 ref 验证 timeout 传递、EntryWorker、MetaWorker、DataWorker 和 UB/URMA 分支。",
    }]
    missing = [name for name, item in surfaces.items() if item.get("status") != "present"]
    if missing:
        recommendations.append({
            "category": "observability",
            "title": "补齐缺失观测面",
            "detail": f"当前缺失或未采样：{', '.join(missing)}。这些字段缺失时只能标为观测盲区，不能直接下根因结论。",
        })
    else:
        recommendations.append({
            "category": "observability",
            "title": "保持现有日志字段稳定输出",
            "detail": "client access、latencySummary、RPC slow、URMA elapsed、error 面均出现时，可继续扩大真实脱敏 fixture 做回归。",
        })
    if len(cohorts) > 1:
        recommendations.append({
            "category": "cohort_compare",
            "title": "多输入包按 cohort 对比后再合并结论",
            "detail": "先分别比较每个输入包的 trace_count、errors、classifications、access latency 和 top workers，再判断是否属于有无底噪差异或同源残留问题。",
        })
    if ub_summary.get("transfer_path") or ub_summary.get("edges"):
        recommendations.append({
            "category": "ub_urma",
            "title": "UB/URMA 按 write/wait/notify 时序继续定界",
            "detail": "结合 transfer path、src->target edge、URMA total、poll JFC、notify、thread scheduling、dataSize、cpuid、inflight 判断，不要只凭 URMA total 单字段归因。",
        })
    if classifications.get("client_deadline_20ms") or classifications.get("client_deadline_with_urma_wait"):
        recommendations.append({
            "category": "deadline",
            "title": "拆开 client deadline 和 worker 后续完成阶段",
            "detail": "20ms client timeout 是失败触发点；同 trace 的 worker access、RemotePull、BatchGetObjectRemote、URMA 日志用于判断服务端是否在 deadline 后继续完成。",
        })
    return recommendations


def _build_source_appendix(coverage):
    rows = [
        {
            "log_surface": "access log",
            "flow_stage": "client -> entry worker",
            "source_hint": "ObjectClientImpl / ClientWorkerRemoteApi / Worker OC access path",
            "validation": "Use CodeGraph on pinned main/master ref, then direct source reads for client timeout, status, and WorkerRpc propagation.",
            "report_reading": "Defines user-visible latency/status; use it as symptom line, not as standalone worker-side root cause.",
        },
        {
            "log_surface": "latencySummary",
            "flow_stage": "client -> entry worker createbuffer / publish",
            "source_hint": "client summary emitters around Set/Create/Publish and buffer preparation",
            "validation": "Use CodeGraph to map each summary key to current write path; preserve raw latencySummary text in evidence.",
            "report_reading": "Explains write-side stage contribution even when no standalone slow log crosses threshold.",
        },
        {
            "log_surface": "GetObjMetaInfo / QueryMeta",
            "flow_stage": "entry worker -> meta worker",
            "source_hint": "ClientWorkerRemoteApi::GetObjMetaInfo / meta service query path",
            "validation": "Use CodeGraph and direct source reads to verify timeout budget and meta RPC branch before attributing QueryMeta.",
            "report_reading": "Only call meta path slow when logs expose QueryMeta/GetObjMetaInfo cost; absence is an observation gap.",
        },
        {
            "log_surface": "RPC slow",
            "flow_stage": "RPC framework client/server/network split",
            "source_hint": "brpc_perf_trace.h / rpc framework slow log emitters",
            "validation": "Check method name and fields such as server_exec_us and network_residual_us against current transport code.",
            "report_reading": "Separates server execution, queueing, framework, and residual/network windows.",
        },
        {
            "log_surface": "RemotePull / BatchGetObjectRemote",
            "flow_stage": "entry worker -> data worker",
            "source_hint": "WorkerRemoteWorkerOCApi / WorkerWorkerOCServiceImpl::BatchGetObjectRemote",
            "validation": "Use CodeGraph to verify current remote get branch, fallback, and aggregation behavior.",
            "report_reading": "Explains worker-side completion after client deadline; compare with client access window.",
        },
        {
            "log_surface": "URMA_ELAPSED_TOTAL",
            "flow_stage": "data worker UB write completion",
            "source_hint": "UrmaManager::WaitToFinish and URMA completion handling",
            "validation": "Use CodeGraph plus source reads; compare total with poll_jfc/notify/thread_sched, request id, src/target, dataSize, cpuid, inflight.",
            "report_reading": "Treat as write/wait completion window, not automatically as business logic or QueryMeta root cause.",
        },
        {
            "log_surface": "Publish / CreateBuffer",
            "flow_stage": "entry worker -> meta worker publish",
            "source_hint": "CreateBuffer/Publish client APIs and meta worker publish path",
            "validation": "Use CodeGraph to separate createbuffer, client publish, entry worker publish, and meta worker publish; verify each stage against latencySummary or slow logs.",
            "report_reading": "For write traces, keep createbuffer and publish as separate phases instead of merging all write latency.",
        },
    ]
    missing = [name for name, item in coverage.get("surfaces", {}).items() if item.get("status") != "present"]
    if missing:
        rows.append({
            "log_surface": "missing evidence",
            "flow_stage": "observability boundary",
            "source_hint": ", ".join(missing),
            "validation": "Add or recover the missing logs before turning absence into a root-cause claim.",
            "report_reading": "Mark as observation gap in customer reports.",
        })
    return rows


def _ips_from_text(text):
    return [match.group(0) for match in IP_RE.finditer(text or "")]


def _flow_stage_rollup(trace_rows, stage_names):
    values = []
    trace_ids = []
    workers = Counter()
    ips = Counter()
    top_trace = None
    top_value = None
    for trace_id, trace in trace_rows.items():
        stage_values = [
            stage.get("duration_ms") for stage in trace.get("stage_breakdown", [])
            if stage.get("stage") in stage_names and stage.get("duration_ms") is not None
        ]
        if not stage_values:
            continue
        value = max(stage_values)
        values.append(value)
        trace_ids.append(trace_id)
        if top_value is None or value > top_value:
            top_value = value
            top_trace = trace_id
        workers.update(trace.get("workers", {}))
        for event in trace.get("ub_events", []):
            if event.get("src_addr"):
                ips[event["src_addr"]] += 1
            if event.get("target_addr"):
                ips[event["target_addr"]] += 1
            ips.update(_ips_from_text(event.get("raw", "")))
        for evidence in trace.get("evidence", []):
            ips.update(_ips_from_text(evidence.get("text", "")))
    pct = _percentiles(values)
    return {
        "trace_count": len(set(trace_ids)),
        "p50_ms": pct.get("p50"),
        "p99_ms": pct.get("p99"),
        "max_ms": pct.get("max"),
        "top_trace": top_trace,
        "top_workers": [worker for worker, _ in workers.most_common(3)],
        "top_ips": [ip for ip, _ in ips.most_common(4)],
    }


def _flow_edge_summary(rollup, fallback):
    if not rollup.get("trace_count"):
        return fallback
    parts = [f"p99={rollup.get('p99_ms', '')}ms", f"max={rollup.get('max_ms', '')}ms"]
    if rollup.get("top_ips"):
        parts.append("IP " + ", ".join(rollup["top_ips"][:2]))
    return " / ".join(parts)


def _build_flow_stages(coverage, flow_counts, ub_summary, trace_rows=None):
    trace_rows = trace_rows or {}
    surfaces = coverage.get("surfaces", {})

    def surface_status(name):
        item = surfaces.get(name, {})
        return {
            "status": item.get("status", "missing"),
            "events": item.get("events", 0),
        }

    read_count = sum(count for name, count in flow_counts.items() if "GET" in name)
    write_count = sum(count for name, count in flow_counts.items()
                      if any(op in name for op in ("SET", "CREATE", "PUBLISH")))
    ub_edges = ub_summary.get("edges", {})
    ub_transfer_count = ub_summary.get("transfer_path", {}).get("UB", 0)
    rollups = {
        "read_client_entry": _flow_stage_rollup(trace_rows, {"read.client_to_entry_worker"}),
        "write_client_entry": _flow_stage_rollup(trace_rows, {
            "write.client_to_entry_createbuffer",
            "write.client_to_entry_publish",
        }),
        "read_entry_meta": _flow_stage_rollup(trace_rows, {"read.entry_to_meta_worker"}),
        "write_entry_meta": _flow_stage_rollup(trace_rows, {"write.entry_to_meta_publish"}),
        "client_entry": _flow_stage_rollup(trace_rows, {
            "read.client_to_entry_worker",
            "write.client_to_entry_createbuffer",
            "write.client_to_entry_publish",
        }),
        "entry_meta": _flow_stage_rollup(trace_rows, {
            "read.entry_to_meta_worker",
            "write.entry_to_meta_publish",
        }),
        "entry_data": _flow_stage_rollup(trace_rows, {"read.entry_to_data_worker"}),
        "data_ub": _flow_stage_rollup(trace_rows, {"read.data_worker_ub_write"}),
    }
    nodes = [
        {"id": "client", "label": "Client", "role": "client", "top_ips": rollups["client_entry"].get("top_ips", [])[:2]},
        {"id": "entry", "label": "Entry Worker", "role": "entry_worker", "top_workers": rollups["client_entry"].get("top_workers", [])[:2], "top_ips": rollups["entry_data"].get("top_ips", [])[:2]},
        {"id": "meta", "label": "Meta Worker", "role": "meta_worker", "top_ips": rollups["entry_meta"].get("top_ips", [])[:2]},
        {"id": "data", "label": "Data Worker", "role": "data_worker", "top_workers": rollups["data_ub"].get("top_workers", [])[:2], "top_ips": rollups["data_ub"].get("top_ips", [])[:2]},
        {"id": "ub", "label": "UB/URMA", "role": "transport"},
    ]
    read_edges = [
        {
            "name": "read: client -> entry worker",
            "source": "client",
            "target": "entry",
            "operation": "Get client RPC",
            "evidence": f"{surface_status('client_access')['events']} access events, {read_count} read flows",
            "status": surface_status("client_access")["status"],
            "summary": _flow_edge_summary(rollups["read_client_entry"], "client read access upper bound"),
            "rollup": rollups["read_client_entry"],
            "reason": "客户侧端到端窗口，作为上界，不和内部 RPC/UB 子阶段相加。",
            "report_reading": "客户看到的耗时和错误起点，先用于表象定界。",
        },
        {
            "name": "read: entry worker -> meta worker",
            "source": "entry",
            "target": "meta",
            "operation": "GetObjMetaInfo / QueryMeta",
            "evidence": f"{surface_status('latency_summary')['events']} latencySummary events",
            "status": surface_status("latency_summary")["status"],
            "summary": _flow_edge_summary(rollups["read_entry_meta"], "QueryMeta evidence"),
            "rollup": rollups["read_entry_meta"],
            "reason": "元数据 RPC 阶段；若 p99/max 高，优先复核 QueryMeta/CreateMeta slow log 和 MetaWorker。",
            "report_reading": "只有出现 QueryMeta/GetObjMetaInfo 耗时或 RPC slow 时才归因到元数据路径。",
        },
        {
            "name": "read: entry worker -> data worker",
            "source": "entry",
            "target": "data",
            "operation": "RemotePull / BatchGetObjectRemote",
            "evidence": f"{len(ub_edges)} UB edge buckets, {ub_transfer_count} UB transfer markers",
            "status": "present" if ub_edges or ub_transfer_count else "missing",
            "summary": _flow_edge_summary(rollups["entry_data"], "RemotePull/BatchGetObjectRemote evidence"),
            "rollup": rollups["entry_data"],
            "reason": "EntryWorker 等待 DataWorker 远端数据；常用于解释 client deadline 后服务端仍继续完成。",
            "report_reading": "读取主路径的远端数据获取窗口，用来解释 client deadline 后 worker 继续完成。",
        },
        {
            "name": "read: data worker -> entry worker UB write",
            "source": "data",
            "target": "ub",
            "operation": "UB write completion",
            "evidence": f"{surface_status('urma_elapsed')['events']} URMA elapsed events",
            "status": surface_status("urma_elapsed")["status"],
            "summary": _flow_edge_summary(rollups["data_ub"], "URMA elapsed evidence"),
            "rollup": rollups["data_ub"],
            "reason": "DataWorker UB/URMA write completion；看 total、request id、src/target、dataSize、cpuid 和 inflight。",
            "report_reading": "DataWorker 侧 payload write/wait/notify 时序，不要只凭 total 单字段下根因。",
        },
        {
            "name": "read: UB write -> entry worker",
            "source": "ub",
            "target": "entry",
            "operation": "completion visible to Entry Worker",
            "evidence": "align RemotePull finish, BatchGetObjectRemote, URMA request id when sampled",
            "status": "present" if ub_edges or surface_status("urma_elapsed")["status"] == "present" else "missing",
            "summary": _flow_edge_summary(rollups["data_ub"], "completion alignment"),
            "rollup": rollups["data_ub"],
            "reason": "用 UB completion 与 EntryWorker RemotePull finish 对齐，确认时间差是否在传输后可见阶段。",
            "report_reading": "用于关联 DataWorker completion 与 EntryWorker RemotePull finish 的时间差。",
        },
    ]
    write_edges = [
        {
            "name": "write: client -> entry worker createbuffer",
            "source": "client",
            "target": "entry",
            "operation": "CreateBuffer RPC",
            "evidence": f"{write_count} write flows, {surface_status('latency_summary')['events']} latencySummary events",
            "status": "present" if write_count or surface_status("latency_summary")["status"] == "present" else "missing",
            "summary": _flow_edge_summary(rollups["write_client_entry"], "CreateBuffer/Publish client RPC evidence"),
            "rollup": rollups["write_client_entry"],
            "reason": "写路径客户侧 createbuffer/publish 请求窗口，需要和 Entry→Meta publish 区分。",
            "report_reading": "写流程先拆 client createbuffer/publish，再看 entry/meta 发布。",
        },
        {
            "name": "write: client -> entry worker publish",
            "source": "client",
            "target": "entry",
            "operation": "Publish RPC",
            "evidence": f"{write_count} write flows, {surface_status('latency_summary')['events']} latencySummary events",
            "status": "present" if write_count or surface_status("latency_summary")["status"] == "present" else "missing",
            "summary": _flow_edge_summary(rollups["write_client_entry"], "client publish evidence"),
            "rollup": rollups["write_client_entry"],
            "reason": "写路径 publish 从 Client 到 EntryWorker；慢时延需和本地 memory copy 及 meta publish 分开看。",
            "report_reading": "client publish 是写路径入口，不代表 UB 读传输。",
        },
        {
            "name": "write: entry worker -> meta worker publish",
            "source": "entry",
            "target": "meta",
            "operation": "CreateBuffer / Publish",
            "evidence": f"{write_count} write flows, {surface_status('latency_summary')['events']} latencySummary events",
            "status": "present" if write_count or surface_status("latency_summary")["status"] == "present" else "missing",
            "summary": _flow_edge_summary(rollups["write_entry_meta"], "publish metadata evidence"),
            "rollup": rollups["write_entry_meta"],
            "reason": "写路径元数据发布阶段；需要和 createbuffer/client publish 区分。",
            "report_reading": "写流程需要拆开 createbuffer、client publish、entry publish、meta publish。",
        },
    ]
    compat_edges = []
    for edge in read_edges + write_edges:
        old_edge = dict(edge)
        old_edge["name"] = old_edge["name"].replace("read: ", "").replace("write: ", "")
        compat_edges.append(old_edge)
    return {
        "nodes": nodes,
        "edges": compat_edges,
        "read": {"nodes": nodes, "edges": read_edges},
        "write": {"nodes": nodes[:3], "edges": write_edges},
    }


def _build_time_buckets(trace_rows, bucket_ms):
    buckets = defaultdict(lambda: {
        "trace_ids": set(),
        "error_count": 0,
        "slow_count": 0,
        "access_latencies": [],
        "stage_latencies": defaultdict(list),
        "top_workers": Counter(),
    })
    for trace_id, trace in trace_rows.items():
        first_ts = trace.get("first_ts")
        if not first_ts:
            continue
        dt = datetime.fromisoformat(first_ts)
        epoch_ms = int(dt.timestamp() * 1000)
        start_ms = epoch_ms - (epoch_ms % bucket_ms)
        bucket = buckets[start_ms]
        bucket["trace_ids"].add(trace_id)
        bucket["error_count"] += sum(trace.get("errors", {}).values())
        if trace.get("classification") not in ("unknown", "access_latency_only"):
            bucket["slow_count"] += 1
        if trace.get("access_latency_ms", {}).get("p50") is not None:
            bucket["access_latencies"].append(trace["access_latency_ms"]["p50"])
        for stage in trace.get("stage_breakdown", []):
            duration = stage.get("duration_ms")
            if duration is None or stage.get("confidence") == "missing":
                continue
            bucket["stage_latencies"][stage["stage"]].append(duration)
        for worker in trace.get("workers", {}):
            bucket["top_workers"][worker] += 1
    rows = []
    for start_ms, bucket in sorted(buckets.items()):
        rows.append({
            "bucket_start": datetime.fromtimestamp(start_ms / 1000.0).isoformat(),
            "bucket_ms": bucket_ms,
            "trace_count": len(bucket["trace_ids"]),
            "error_count": bucket["error_count"],
            "slow_count": bucket["slow_count"],
            "p50_access_ms": _percentiles(bucket["access_latencies"]).get("p50"),
            "p99_access_ms": _percentiles(bucket["access_latencies"]).get("p99"),
            "stage_breakdown_ms": {
                stage: _percentiles(values)
                for stage, values in sorted(bucket["stage_latencies"].items())
            },
            "burst_score": max(bucket["slow_count"], bucket["error_count"], 1),
            "gap_score": 0,
            "top_workers": [w for w, _ in bucket["top_workers"].most_common(3)],
        })
    return rows


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


def _slug(text):
    clean = re.sub(r"[^A-Za-z0-9_.-]+", "-", text.strip()).strip("-").lower()
    return clean or "trace-run"


def _input_identity(path):
    p = Path(path)
    h = hashlib.sha256()
    members = []
    if p.is_file():
        with open(p, "rb") as f:
            for chunk in iter(lambda: f.read(1024 * 1024), b""):
                h.update(chunk)
        if tarfile.is_tarfile(p):
            with tarfile.open(p, "r:*") as tar:
                members = sorted(member.name for member in tar.getmembers() if member.isfile())
        return {"path": str(p), "size": p.stat().st_size, "sha256": h.hexdigest(), "members": members}
    h.update(str(p).encode("utf-8"))
    return {"path": str(p), "size": 0, "sha256": h.hexdigest(), "members": members}


def _script_version():
    h = hashlib.sha256()
    with open(Path(__file__), "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()[:16]


def _cache_key(inputs, code_ref, case_name, scenario):
    identities = sorted((_input_identity(path) for path in inputs), key=lambda item: item["path"])
    payload = {
        "script_version": _script_version(),
        "code_ref": code_ref,
        "case_name": case_name,
        "scenario": scenario,
        "inputs": identities,
    }
    raw = json.dumps(payload, sort_keys=True, ensure_ascii=False).encode("utf-8")
    return hashlib.sha256(raw).hexdigest(), identities


def _find_cached_run(out_root, cache_key):
    for manifest_path in sorted(out_root.glob("*/manifest.json")):
        try:
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        if manifest.get("cache", {}).get("key") == cache_key:
            return manifest_path.parent
    return None


def _safe_member_path(member_name):
    pure = Path(member_name)
    safe_parts = [part for part in pure.parts if part not in ("", ".", "..")]
    return Path(*safe_parts) if safe_parts else Path("member.log")


def _preserve_raw_inputs(inputs, run_dir):
    raw_inputs = run_dir / "raw" / "inputs"
    raw_extracted = run_dir / "raw" / "extracted"
    raw_inputs.mkdir(parents=True, exist_ok=True)
    raw_extracted.mkdir(parents=True, exist_ok=True)
    for raw in inputs:
        p = Path(raw)
        if p.is_file():
            copied = raw_inputs / p.name
            shutil.copy2(p, copied)
            if tarfile.is_tarfile(p):
                extract_root = raw_extracted / p.name
                with tarfile.open(p, "r:*") as tar:
                    for member in tar.getmembers():
                        if not member.isfile():
                            continue
                        stream = tar.extractfile(member)
                        if stream is None:
                            continue
                        target = extract_root / _safe_member_path(member.name)
                        target.parent.mkdir(parents=True, exist_ok=True)
                        with open(target, "wb") as out:
                            shutil.copyfileobj(stream, out)


def _write_inputs_doc(run_dir, manifest):
    lines = [
        "# Trace Triage Inputs",
        "",
        f"- case_name: `{manifest.get('case_name', '')}`",
        f"- scenario: `{manifest.get('scenario', '')}`",
        f"- code_ref: `{manifest.get('code_ref', '')}`",
        f"- analysis_created_at: `{manifest.get('analysis_created_at', '')}`",
        "",
        "## Input Packages",
        "",
    ]
    for index, item in enumerate(manifest.get("inputs", []), 1):
        source = item.get("path", "")
        name = Path(source).name or f"input-{index}"
        lines.extend([
            f"### {index}. `{name}`",
            "",
            f"- source_path: `{source}`",
            f"- size_bytes: {item.get('size', 0)}",
            f"- sha256: `{item.get('sha256', '')}`",
        ])
        raw_copy = Path("raw") / "inputs" / name
        if (Path(run_dir) / raw_copy).exists():
            lines.append(f"- preserved_raw: `{raw_copy.as_posix()}`")
        members = item.get("members", [])
        if members:
            extract_root = Path("raw") / "extracted" / name
            lines.append(f"- extracted_root: `{extract_root.as_posix()}`")
            lines.append("- members:")
            for member in members:
                lines.append(f"  - `{member}`")
        else:
            lines.append("- members: none")
        lines.append("")
    (Path(run_dir) / "inputs.md").write_text("\n".join(lines), encoding="utf-8")


def _write_site_publish_doc(run_dir, manifest):
    run_dir = Path(run_dir)
    filename = f"{run_dir.name}.html"
    remote_path = f"/var/www/html/perf/{filename}"
    url = f"https://yche.me/perf/{filename}"
    source_html = run_dir / "report.site.html"
    source_size = source_html.stat().st_size if source_html.exists() else 0
    lines = [
        "# yche.me Publish Checklist",
        "",
        f"- case_name: `{manifest.get('case_name', '')}`",
        f"- scenario: `{manifest.get('scenario', '')}`",
        f"- source_html: `report.site.html`",
        f"- source_size_bytes: `{source_size}`",
        f"- default_publish_limit_bytes: `{DEFAULT_SITE_HTML_MAX_BYTES}`",
        f"- target_host: `xqyun-32c32g`",
        f"- target_path: `{remote_path}`",
        f"- url: `{url}`",
        "",
        "## Commands",
        "",
        "```bash",
        f"scp report.site.html xqyun-32c32g:{remote_path}",
        f"curl -fsSI {url}",
        "```",
        "",
        "## Verification",
        "",
        "- HTTP status should be 200.",
        "- Open the URL and verify navigation, ECharts, provenance, coverage, trace filters, downloads, and selected logs.",
        "- Do not publish oversized throw-away pages; pass `--max-site-html-mb` only after reviewing why the page is large.",
    ]
    (run_dir / "site_publish.md").write_text("\n".join(lines) + "\n", encoding="utf-8")
    return {"publish_doc": "site_publish.md", "target_path": remote_path, "url": url}


def _write_json(path, value):
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _read_json(path):
    return json.loads(Path(path).read_text(encoding="utf-8"))


def _update_manifest(run_dir, updater):
    manifest_path = Path(run_dir) / "manifest.json"
    manifest = _read_json(manifest_path)
    updater(manifest)
    _write_json(manifest_path, manifest)
    return manifest


def _render_html(report, title, site=False, manifest=None):
    data = json.dumps(report, ensure_ascii=False).replace("</script>", "<\\/script>")
    manifest_data = json.dumps(manifest or {}, ensure_ascii=False).replace("</script>", "<\\/script>")
    base_style = """<style>
:root{--bg:#f6f8fb;--card:#fff;--text:#172033;--muted:#5f6b7a;--border:#dfe5ee;--blue:#2563eb;--orange:#ea580c;--red:#dc2626;--green:#059669;--purple:#7c3aed;--amber:#ca8a04}
*{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--text);font-family:'Microsoft YaHei',-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif}
.layout{display:flex}aside{position:fixed;left:0;top:0;width:245px;height:100vh;background:#fff;border-right:1px solid var(--border);padding:18px 14px;overflow:auto}
aside h2{font-size:16px;margin:0 0 12px}nav a{display:block;color:#324055;text-decoration:none;padding:8px 10px;border-radius:6px;font-size:13px;margin:2px 0}
nav a.active,nav a:hover{background:#eaf2ff;color:#1d4ed8}nav a.sub{padding-left:22px;color:#64748b;font-size:12px}
main{margin-left:245px;width:calc(100% - 245px);padding:22px 28px 50px}section{margin-bottom:20px}
h1{font-size:26px;margin:0 0 8px}h2{font-size:21px;margin:8px 0 12px}h3{font-size:15px;text-align:center;margin:10px 0}
.subtitle,.note,.insight{color:var(--muted);line-height:1.65}.cards{display:grid;grid-template-columns:repeat(4,1fr);gap:10px}
.card{background:#fff;border:1px solid var(--border);border-radius:8px;padding:12px}.panel{background:#fff;border:1px solid var(--border);border-radius:8px;padding:16px;margin:12px 0;box-shadow:0 2px 10px rgba(20,35,60,.04)}
.k{color:#64748b;font-size:12px}.v,.metric{font-size:24px;font-weight:700;margin:4px 0}.n,.muted,.small{color:#64748b;font-size:12px}.bad{color:var(--red)!important;font-weight:700}.warn{color:#b45309!important;font-weight:700}.ok{color:var(--green)!important;font-weight:700}
tr.hotrow td{background:#fff1f2}tr.warnrow td{background:#fffbeb}
.log-tag{display:inline-block;border-radius:4px;padding:0 4px;margin:0 1px;font-weight:700}.log-error{background:#fee2e2;color:#991b1b}.log-deadline{background:#ffedd5;color:#9a3412}.log-urma{background:#ede9fe;color:#5b21b6}.log-rpc{background:#dbeafe;color:#1e40af}.log-latency{background:#dcfce7;color:#166534}.log-slow{background:#fef3c7;color:#92400e}.log-field{background:#e2e8f0;color:#334155}
.log-legend,.stage-legend{display:flex;flex-wrap:wrap;gap:6px;margin:8px 0}.log-legend span,.stage-legend span{font-size:12px}
.stage-pill{display:inline-flex;align-items:center;gap:5px;border:1px solid var(--border);border-radius:999px;padding:2px 8px;background:#fff;color:#475569}.stage-dot{width:10px;height:10px;border-radius:2px;display:inline-block}
.compare2{display:grid;grid-template-columns:1fr 1fr;gap:12px}.chart-grid{display:grid;grid-template-columns:1fr 1fr;gap:12px}.chart{height:360px;width:100%}.caption{text-align:center;color:#64748b;font-size:12px;margin-top:6px}
table{width:100%;border-collapse:collapse;table-layout:fixed;background:#fff}th,td{border-bottom:1px solid var(--border);padding:8px 9px;text-align:left;vertical-align:top;font-size:13px;word-break:break-word}
th{background:#f8fafc;color:#475569}.num{text-align:right;font-variant-numeric:tabular-nums}.trace-id{font-family:'Cascadia Code',Consolas,monospace;font-size:12px}
.controls{display:flex;gap:8px;flex-wrap:wrap;margin:8px 0 12px;align-items:center}input,select,button{border:1px solid var(--border);background:#fff;border-radius:6px;padding:7px 9px;font-size:13px}
button{cursor:pointer}button.primary{background:var(--blue);color:#fff;border-color:var(--blue)}button:disabled{opacity:.45;cursor:not-allowed}.pager{background:#fff;border:1px solid var(--border);border-radius:8px;padding:10px}.mini-pager{display:flex;justify-content:center;gap:8px;align-items:center;margin-top:8px;color:#64748b;font-size:12px}
.selected-row{background:#fff7e6}.logbox,pre{white-space:pre-wrap;background:#0f172a;color:#dbeafe;padding:12px;border-radius:8px;max-height:520px;overflow:auto;font-family:'Cascadia Code',Consolas,monospace;font-size:12px;line-height:1.5}
.trace-log-groups{display:flex;flex-direction:column;gap:10px;margin-top:10px}.trace-log-block{border:1px solid var(--border);border-radius:8px;overflow:hidden;background:#fff}.trace-log-block h4{margin:0;padding:8px 12px;background:#f8fafc;color:#0f172a;font-size:13px;display:flex;justify-content:space-between;gap:12px}.trace-log-block pre{border-radius:0;margin:0;max-height:none}.trace-log-count{color:#64748b;font-weight:500}
code{font-family:'Cascadia Code',Consolas,monospace;font-size:12px}
@media(max-width:900px){.layout{display:block}aside{position:relative;width:auto;height:auto}main{margin-left:0;width:100%;padding:16px}.chart-grid,.compare2,.cards{grid-template-columns:1fr}}
</style>"""
    stylesheet = ('<link rel="stylesheet" href="/assets/css/site.css">' if site else "") + base_style
    script_ref = '<script src="/assets/js/site.js"></script>' if site else ""
    template = """<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>__TITLE__</title>
  __STYLESHEET__
  <script src="https://cdn.jsdelivr.net/npm/echarts@5/dist/echarts.min.js"></script>
</head>
<body>
  <div class="layout">
    <aside><h2>Trace 分析报告</h2><nav id="nav">
      <a href="#s1">1. 结论</a>
      <a href="#s2">2. 根因分布</a>
      <a class="sub" href="#cohort-chart">图 2-0 输入对比</a>
      <a class="sub" href="#classification-chart">图 2-1 分类</a>
      <a class="sub" href="#error-chart">图 2-2 错误</a>
      <a href="#s3">3. 时延 Breakdown</a>
      <a class="sub" href="#latency-chart">图 3-1 时延</a>
      <a class="sub" href="#time-breakdown-chart">图 3-3 时间桶</a>
      <a href="#s4">4. Worker / UB</a>
      <a class="sub" href="#flow-stage-chart">图 4-0 流程图</a>
      <a href="#s5">5. Trace 查看</a>
      <a class="sub" href="#top-trace-table">表 5-1 Top Trace</a>
      <a href="#s6">6. 建议与口径</a>
      <a class="sub" href="#source-appendix-table">表 6-2 代码映射</a>
      <a href="#s7">7. 原始 JSON</a>
    </nav>
    </aside>
    <main>
      <section id="s1">
        <h1>__TITLE__</h1>
        <p class="subtitle" id="report-subtitle"></p>
        <div id="summary" class="cards"></div>
        <div class="panel"><h3>运行与输入来源</h3><table id="run-metadata-table"></table></div>
        <div class="panel insight" id="report-insight"></div>
        <div class="panel"><h3>客户化诊断口径</h3><ul id="diagnosis-list"></ul><div class="controls"><button class="primary" id="download-report-summary">下载分析摘要</button></div></div>
        <div class="panel"><h3>日志覆盖与缺失观测面</h3><table id="coverage-table"></table></div>
      </section>
      <section id="s2">
        <h2>2. 错误根因与分类分布</h2>
        <div class="panel"><div id="cohort-chart" class="chart"></div><div class="caption">图 2-0 输入包/cohort 对比：多个日志包独立统计，再对比分类和错误分布</div></div>
        <div class="panel"><h3>表 2-0 输入包/cohort 对比</h3><table id="cohort-table"></table></div>
        <div class="chart-grid">
          <div class="panel"><div id="classification-chart" class="chart"></div><div class="caption">图 2-1 分类分布</div></div>
          <div class="panel"><div id="error-chart" class="chart"></div><div class="caption">图 2-2 错误文本/状态分布</div></div>
        </div>
        <div class="compare2">
          <div class="panel"><h3>表 2-1 分类聚合</h3><table id="classification-table"></table></div>
          <div class="panel"><h3>Error Breakdown</h3><table id="error-table"></table></div>
        </div>
      </section>
      <section id="s3">
        <h2>3. 时延 Breakdown</h2>
        <div class="panel insight">Breakdown 不做简单相加：Client/access 是等待窗口；Entry/DataWorker 指标可能发生在 client deadline 之后。图中 p99/max 用于找尾部，p50 用于看普遍水平。</div>
        <div class="chart-grid">
          <div class="panel"><div id="latency-chart" class="chart"></div><div class="caption">图 3-1 Top 时延指标：图中使用短名称，完整字段见 tooltip 和表 3-1。</div></div>
          <div class="panel"><div id="flow-chart" class="chart"></div><div class="caption">图 3-2 访问流程分布：图中使用短名称，完整 flow 见 tooltip 和表 3-2。</div></div>
        </div>
        <div class="panel"><div id="time-breakdown-chart" class="chart"></div><div class="caption">图 3-3 时间桶时延分段：柱状图为同桶内 RPC/UB 子阶段 p99，折线为 client/access p99 上界；client access 不与子阶段相加。</div></div>
        <div class="compare2">
          <div class="panel"><h3>表 3-1 时延指标</h3><table id="latency-table"></table></div>
          <div class="panel"><h3>表 3-2 Flow Breakdown</h3><table id="flow-table"></table></div>
        </div>
      </section>
      <section id="s4">
        <h2>4. Worker / UB 分布</h2>
        <div id="flow-stage-chart" class="panel insight">原 Client→Entry→Meta/Data 流程现在按读写链路分开展示：读取关注 Client→Entry→Meta/Data→UB，写入关注 Client→Entry CreateBuffer/Publish→Meta Publish；两者不能混合相加。</div>
        <div class="panel controls"><label>读写视角 <select id="operation-filter"><option value="">全部读写</option><option value="read">只看读取</option><option value="write">只看写入</option></select></label><span class="muted">联动 Trace、Breakdown、流程 Edge 与 UB Edge。</span></div>
        <div class="chart-grid">
          <div class="panel"><h3>图 4-0a 读取流程：Client→Entry→Meta/Data→UB</h3><div id="read-flow-stage-chart" class="chart"></div><div class="caption">读取路径重点看 Entry→Data RPC 与 DataWorker UB/URMA 是否解释尾部。</div></div>
          <div class="panel"><h3>图 4-0b 写入流程：Client→Entry CreateBuffer/Publish→Meta Publish</h3><div id="write-flow-stage-chart" class="chart"></div><div class="caption">写入路径重点区分 createbuffer、client publish、entry/meta publish。</div></div>
        </div>
        <div class="compare2">
          <div class="panel"><h3>表 4-0a 读取流程阶段证据</h3><table id="read-flow-stage-table"></table></div>
          <div class="panel"><h3>表 4-0b 写入流程阶段证据</h3><table id="write-flow-stage-table"></table></div>
        </div>
        <div class="panel" style="display:none"><table id="flow-stage-table"></table></div>
        <div class="chart-grid">
          <div class="panel"><div id="worker-chart" class="chart"></div><div class="caption">图 4-1 Worker 错误聚合</div></div>
          <div class="panel"><div id="ub-edge-chart" class="chart"></div><div class="caption">图 4-2 UB edge 次数</div></div>
        </div>
        <div class="compare2">
          <div class="panel"><h3>表 4-1 Worker Breakdown</h3><div class="small">默认每页 5 行；error/slow 较高的行会高亮。</div><table id="worker-table"></table><div id="worker-table-pager" class="mini-pager"></div></div>
          <div class="panel"><h3>表 4-2 UB Edges</h3><div class="small">默认每页 5 行；p99/max 超过 5ms/20ms 的 edge 会高亮。</div><table id="ub-edge-table"></table><div id="ub-edge-table-pager" class="mini-pager"></div></div>
        </div>
      </section>
      <section id="s5">
        <h2>5. Trace 查看</h2>
        <div class="panel">
        <div class="controls"><input id="trace-search" placeholder="搜索 trace / worker / 关键词" style="min-width:300px"><select id="class-filter"><option value="">全部分类</option></select><select id="worker-filter"><option value="">全部 Worker</option></select><span class="muted">读写视角使用第 4 节过滤器</span><button id="reset-filter">清空</button></div>
        <div class="controls pager">
          <label>每页 <select id="trace-page-size"><option value="8">8</option><option value="16">16</option><option value="32">32</option><option value="9999">全部</option></select> 条</label>
          <button class="primary" id="prev-page">上一页</button>
          <span id="page-status" class="muted"></span>
          <button class="primary" id="next-page">下一页</button>
        </div>
        <table id="top-trace-table"></table>
        </div>
        <div class="compare2">
          <div class="panel"><h3>图 5-1 选中 Trace Breakdown</h3><div id="selected-trace-chart" class="chart"></div><div id="selected-stage-legend" class="stage-legend"></div><div class="caption">点击上方 Trace 行后联动更新，横向条形图按阶段耗时排序，单位 ms</div><table id="selected-stage-table"></table></div>
          <div class="panel"><h3>表 5-2 选中 Trace 摘要</h3><table id="selected-trace-table"></table><div class="controls"><button class="primary" id="download-selected-raw">下载当前 Trace 裸日志</button><button id="download-filtered-evidence">下载当前过滤证据</button></div></div>
        </div>
        <div class="panel"><h3>Trace 全量日志</h3><div class="small">参考慢 Trace 报告方式：日志按组件连续分块展示；块内保持原始顺序完整展开，ERROR、timeout、RPC slow、latencySummary、RemotePull、URMA 以及大耗时字段会高亮。</div><div class="log-legend" id="log-highlight-legend"></div><div id="selected-trace-log" class="trace-log-groups"></div></div>
      </section>
      <section id="s6">
        <h2>6. 建议与后续口径</h2>
        <div class="panel"><h3>表 6-1 建议与证据边界</h3><table id="recommendation-table"></table></div>
        <div class="panel"><h3>表 6-2 代码与字段映射</h3><table id="source-appendix-table"></table></div>
      </section>
      <section id="s7">
        <h2>7. 原始 JSON 附录</h2>
        <div class="panel"><details>
          <summary>展开 parser 原始 trace JSON</summary>
          <pre id="trace-data"></pre>
        </details></div>
      </section>
    </main>
  </div>
  <script>
  const report = __DATA__;
  const manifest = __MANIFEST__;
  const dim = report.dimensions || {};
  const traces = report.traces || {};
  const traceRows = Object.entries(traces).sort((a,b) => (b[1].access_latency_ms?.max || 0) - (a[1].access_latency_ms?.max || 0));
  let filteredTraceRows = traceRows;
  let currentPage = 0;
  let selectedTraceId = traceRows[0]?.[0] || null;
  let pageSize = 8;
  let activeOperation = '';
  function escapeHtml(value) {
    return String(value ?? '').replace(/[&<>"']/g, ch => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[ch]));
  }
  function pctText(item) {
    if (!item || !item.count) return '';
    return `count=${item.count} p50=${item.p50} p90=${item.p90} p99=${item.p99} max=${item.max}`;
  }
  function scalePercentiles(item, scale) {
    if (!item || !item.count) return null;
    const out = {count:item.count};
    ['min','p50','p90','p99','max'].forEach(key => {
      out[key] = item[key] === undefined || item[key] === null ? null : Number((item[key] * scale).toFixed(3));
    });
    return out;
  }
  const metricLabelMap = {
    'access':'Access',
    'urma.total':'URMA total',
    'urma.poll_jfc':'URMA poll',
    'urma.notify':'URMA notify',
    'urma.thread_sched':'URMA sched',
    'latencySummary.client.process.get':'Client process GET',
    'latencySummary.client.process.set':'Client process SET',
    'latencySummary.client.rpc.get':'Client RPC GET',
    'latencySummary.client.rpc.create':'Client RPC Create',
    'latencySummary.client.rpc.publish':'Client RPC Publish',
    'latencySummary.client.process.memory_copy':'Client memory copy',
    'latencySummary.worker.process.get':'Worker process GET',
    'latencySummary.worker.process.remote_get':'Worker RemoteGet process',
    'latencySummary.worker.rpc.query_meta':'Worker QueryMeta RPC',
    'latencySummary.worker.rpc.remote_get':'Worker RemoteGet RPC',
    'latencySummary.worker.rpc.create_meta':'Worker CreateMeta RPC',
    'latencySummary.worker.process.publish':'Worker Publish'
  };
  function metricLabel(name) {
    if (metricLabelMap[name]) return metricLabelMap[name];
    return String(name || '').replace(/^latencySummary\\./, '').replace(/^urma\\./, 'URMA ').replace(/[._]/g, ' ');
  }
  const flowLabelMap = {
    'DS_KV_CLIENT_GET':'Client GET',
    'DS_POSIX_GET':'Worker GET',
    'DS_KV_CLIENT_SET':'Client SET',
    'DS_POSIX_SET':'Worker SET',
    'DS_KV_CLIENT_CREATE':'Client Create',
    'DS_POSIX_CREATE':'Worker Create',
    'DS_KV_CLIENT_PUBLISH':'Client Publish',
    'DS_POSIX_PUBLISH':'Worker Publish'
  };
  function flowLabel(name) {
    if (flowLabelMap[name]) return flowLabelMap[name];
    return String(name || '').replace(/^DS_/, '').replace(/_/g, ' ');
  }
  function renderTable(id, headers, rows, rowAttrs) {
    const table = document.getElementById(id);
    if (!rows.length) {
      table.innerHTML = `<tbody><tr><td class="muted">No data</td></tr></tbody>`;
      return;
    }
    table.innerHTML = `<thead><tr>${headers.map(h => `<th>${escapeHtml(h)}</th>`).join('')}</tr></thead>` +
      `<tbody>${rows.map((row, idx) => `<tr ${rowAttrs ? rowAttrs(row, idx) : ''}>${row.map(cell => `<td>${escapeHtml(cell)}</td>`).join('')}</tr>`).join('')}</tbody>`;
  }
  const pagedTables = {};
  function renderPagedTable(id, pagerId, headers, rows, rowAttrs, pageSize=5) {
    const state = pagedTables[id] || (pagedTables[id] = {page:0});
    const pages = Math.max(1, Math.ceil(rows.length / pageSize));
    state.page = Math.min(Math.max(state.page, 0), pages - 1);
    const start = state.page * pageSize;
    renderTable(id, headers, rows.slice(start, start + pageSize), (row, idx) => rowAttrs ? rowAttrs(row, start + idx) : '');
    const pager = document.getElementById(pagerId);
    if (!pager) return;
    pager.innerHTML = `<button ${state.page <= 0 ? 'disabled' : ''} data-act="prev">上一页</button><span>第 ${state.page + 1} / ${pages} 页，共 ${rows.length} 行</span><button ${state.page >= pages - 1 ? 'disabled' : ''} data-act="next">下一页</button>`;
    pager.querySelectorAll('button').forEach(button => button.addEventListener('click', () => {
      state.page += button.dataset.act === 'prev' ? -1 : 1;
      renderPagedTable(id, pagerId, headers, rows, rowAttrs, pageSize);
    }));
  }
  function severityClass(value, warn=5, hot=20) {
    const n = Number(value || 0);
    return n >= hot ? 'hotrow' : n >= warn ? 'warnrow' : '';
  }
  const stageLabelMap = {
    'read.entry_to_meta_worker':'Entry→Meta RPC',
    'read.entry_to_data_worker':'Entry→Data RPC',
    'read.data_worker_ub_write':'DataWorker UB/URMA',
    'write.client_to_entry_createbuffer':'Client→Entry CreateBuffer',
    'write.client_memory_copy':'Client Memory Copy',
    'write.client_to_entry_publish':'Client→Entry Publish',
    'write.entry_to_meta_publish':'Entry→Meta Publish'
  };
  const stageColorMap = {
    'read.entry_to_meta_worker':'#0891b2',
    'read.entry_to_data_worker':'#ea580c',
    'read.data_worker_ub_write':'#059669',
    'write.client_to_entry_createbuffer':'#2563eb',
    'write.client_memory_copy':'#7c3aed',
    'write.client_to_entry_publish':'#ca8a04',
    'write.entry_to_meta_publish':'#dc2626'
  };
  function stageDisplayName(stage) {
    if (stageLabelMap[stage]) return stageLabelMap[stage];
    return String(stage || '').replace(/^(read|write)\\./, '').replace(/_/g, ' ');
  }
  function stageDetailText(stage) {
    const display = stageDisplayName(stage);
    return display === stage ? display : `${display} (${stage})`;
  }
  function traceOperation(item) {
    const stageNames = (item.stage_breakdown || []).map(stage => stage.stage || '');
    const flowNames = Object.keys(item.flows || {});
    const hasRead = stageNames.some(name => name.startsWith('read.')) || flowNames.some(name => name.includes('GET'));
    const hasWrite = stageNames.some(name => name.startsWith('write.')) || flowNames.some(name => /(SET|CREATE|PUBLISH)/.test(name));
    return hasRead && hasWrite ? 'mixed' : hasRead ? 'read' : hasWrite ? 'write' : 'unknown';
  }
  function operationMatches(item) {
    if (!activeOperation) return true;
    const op = traceOperation(item);
    return op === activeOperation || op === 'mixed';
  }
  function stageMatchesOperation(stageName) {
    return !activeOperation || String(stageName || '').startsWith(`${activeOperation}.`);
  }
  const palette = ['#2563eb','#ea580c','#059669','#7c3aed','#dc2626','#0891b2','#ca8a04','#64748b'];
  function axisBase(title, extra) {
    return Object.assign({
      title:{text:title, left:'center', top:4, textStyle:{fontSize:14}},
      color:palette,
      grid:{left:58,right:24,top:72,bottom:76,containLabel:true},
      legend:{top:30,type:'scroll'},
      toolbox:{right:10,feature:{saveAsImage:{},dataView:{readOnly:true},restore:{}}},
      tooltip:{trigger:'axis', axisPointer:{type:'shadow'}, confine:true},
      dataZoom:[{type:'inside'},{type:'slider', height:18, bottom:18}]
    }, extra || {});
  }
  function noDataOption(title) {
    return {title:{text:title, left:'center', top:'center', textStyle:{fontSize:14,color:'#94a3b8'}}};
  }
  function chart(id, option) {
    const node = document.getElementById(id);
    if (!node) return;
    if (!window.echarts) {
      node.innerHTML = '<div class="muted">ECharts failed to load; tables below remain available.</div>';
      return;
    }
    const instance = echarts.getInstanceByDom(node) || echarts.init(node);
    instance.setOption(option, true);
  }
  function downloadText(filename, text) {
    const blob = new Blob([text], {type:'text/plain;charset=utf-8'});
    const link = document.createElement('a');
    link.href = URL.createObjectURL(blob);
    link.download = filename;
    document.body.appendChild(link);
    link.click();
    URL.revokeObjectURL(link.href);
    link.remove();
  }
  function highlightLogLine(line) {
    const text = escapeHtml(line);
    return text
      .replace(/\\b(ERROR|FATAL|K_RUNTIME_ERROR|K_TRY_AGAIN|status[:=]?\\s*1001)\\b/gi, '<span class="log-tag log-error">$1</span>')
      .replace(/(\\[?URMA_ELAPSED_(?:TOTAL|POLL_JFC|NOTIFY|THREAD_SHED)\\]?|URMA_WAIT_TIMEOUT|urma_[a-z_]+|request id[:=]?\\s*\\d+|dataSize[:=]?\\s*\\d+|cpuid[:=]?\\s*\\d+|inflight[_a-z]*[:=]?\\s*\\d+)/gi, '<span class="log-tag log-urma">$1</span>')
      .replace(/(\\[?(?:ZMQ_)?RPC_FRAMEWORK_SLOW\\]?|server_exec_us=\\d+|network_residual_us=\\d+|client_req_framework_us=\\d+|client_rsp_framework_us=\\d+|remote_processing_us=\\d+)/gi, '<span class="log-tag log-rpc">$1</span>')
      .replace(/(latencySummary|client\\.rpc\\.[a-z_]+|worker\\.rpc\\.[a-z_]+|worker\\.process\\.[a-z_]+|client\\.process\\.[a-z_]+)/gi, '<span class="log-tag log-latency">$1</span>')
      .replace(/(deadline exceeded|RPC timed out|\\btimeout\\b|20ms deadline)/gi, '<span class="log-tag log-deadline">$1</span>')
      .replace(/(cost(?:Us)?[:=]?\\s*[\\d.]+\\s*(?:us|ms)?|totalCost:\\s*[\\d.]+ms|[A-Za-z0-9_.-]+:\\s*\\d{4,})/gi, '<span class="log-tag log-slow">$1</span>')
      .replace(/(RemotePull|Remote done|BatchGetObjectRemote|ProcessGetObjectRequest|CreateBuffer|Publish|QueryMeta|GetObjMetaInfo)/gi, '<span class="log-tag log-field">$1</span>');
  }
  const componentLabels = {
    client:'Client 侧日志',
    entry_worker:'Entry Worker 日志',
    meta_worker:'Meta Worker 观测',
    data_worker:'Data Worker / UB 日志',
    context:'Trace Context'
  };
  function classifyEvidenceComponent(e) {
    const text = `${e.member || ''} ${e.text || ''}`;
    if (/URMA_ELAPSED|urma_manager|urma_|target address|src address/i.test(text)) return 'data_worker';
    if (/MasterOCService\\.QueryMeta|Query metadata from master|GetObjMetaInfo|meta[_ -]?worker/i.test(text)) return 'meta_worker';
    if (/\\/collected\\/|ds_client_|object_client_impl|client_worker_remote_api|DS_KV_CLIENT|Client\\/WorkerRpc/i.test(text)) return 'client';
    if (/collected_worker_logs|DS_POSIX|worker_oc_service|BatchGetObjectRemote|Remote done|access\\.log/i.test(text)) return 'entry_worker';
    return 'context';
  }
  function renderTraceLogBlocks(evidence) {
    const groups = [];
    (evidence || []).forEach(e => {
      const component = classifyEvidenceComponent(e);
      const line = `${e.member}:${e.line} ${e.text}`;
      const last = groups[groups.length - 1];
      if (!last || last.component !== component) {
        groups.push({component, lines: []});
      }
      groups[groups.length - 1].lines.push(line);
    });
    if (!groups.length) return '<div class="muted">No selected trace log evidence.</div>';
    return groups.map(group =>
      `<div class="trace-log-block trace-log-${escapeHtml(group.component)}">` +
      `<h4><span>${escapeHtml(componentLabels[group.component] || group.component)}</span><span class="trace-log-count">${group.lines.length} lines</span></h4>` +
      `<pre>${group.lines.map(highlightLogLine).join('\\n')}</pre></div>`
    ).join('');
  }
  const access = dim.latency_ms?.access || {};
  const totalErrors = Object.values(dim.errors || {}).reduce((a,b) => a + b, 0);
  const topClass = Object.entries(dim.classifications || {}).sort((a,b) => b[1] - a[1])[0] || ['unknown', 0];
  const diagnosis = dim.diagnosis || {};
  const recommendations = dim.recommendations || [];
  const sourceAppendix = dim.source_appendix || [];
  const flowStages = dim.flow_stages || {nodes: [], edges: [], read:{nodes: [], edges: []}, write:{nodes: [], edges: []}};
  const readFlowStages = flowStages.read || {nodes: flowStages.nodes || [], edges: []};
  const writeFlowStages = flowStages.write || {nodes: flowStages.nodes || [], edges: []};
  document.getElementById('log-highlight-legend').innerHTML = [
    ['log-error','ERROR/status'],
    ['log-deadline','deadline/timeout'],
    ['log-rpc','RPC slow'],
    ['log-latency','latencySummary'],
    ['log-urma','URMA/UB'],
    ['log-slow','>=阈值耗时'],
    ['log-field','关键流程']
  ].map(([cls,label]) => `<span class="log-tag ${cls}">${label}</span>`).join('');
  document.getElementById('report-subtitle').innerHTML =
    `输入日志解析得到 <b>${report.trace_count}</b> 条 trace，错误标记 <b>${totalErrors}</b> 个。` +
    `当前主分类为 <b>${escapeHtml(topClass[0])}</b>，access p99 为 <b>${access.p99 ?? ''}ms</b>。`;
  const diagnosisRows = ['symptom_line','latency_line','evidence_boundary','customer_expression']
    .map(key => diagnosis[key])
    .filter(Boolean);
  document.getElementById('report-insight').innerHTML =
    `<b>核心判断：</b>${escapeHtml(diagnosis.customer_expression?.text || '请结合错误线、慢时延线和证据边界阅读。')}`;
  document.getElementById('diagnosis-list').innerHTML = diagnosisRows
    .map(item => `<li><b>${escapeHtml(item.label)}：</b>${escapeHtml(item.text)}</li>`).join('');
  document.getElementById('summary').innerHTML = [
    ['trace_count', report.trace_count, 'parsed traces'],
    ['errors', totalErrors, 'total error markers'],
    ['access p99 ms', access.p99 ?? '', 'client/access latency'],
    ['code_ref', report.code_ref, 'source reference']
  ].map(([k,v,hint]) => `<div class="card"><div class="k">${escapeHtml(k)}</div><div class="v">${escapeHtml(v)}</div><div class="n">${escapeHtml(hint)}</div></div>`).join('');
  renderTable('run-metadata-table', ['field','value'], [
    ['case_name', manifest.case_name || ''],
    ['scenario', manifest.scenario || ''],
    ['analysis_created_at', manifest.analysis_created_at || ''],
    ['input_document', manifest.input_document || 'inputs.md'],
    ['raw_inputs', 'raw/inputs'],
    ['raw_extracted', 'raw/extracted'],
    ['inputs', (manifest.inputs || []).map(item => `${item.path} (${item.size || 0} bytes)`).join('\\n')]
  ].filter(row => row[1]));
  const classificationRows = Object.entries(dim.classifications || {}).sort((a,b) => b[1]-a[1]);
  const errorRows = Object.entries(dim.errors || {}).sort((a,b) => b[1]-a[1]);
  const cohortRows = Object.entries(dim.cohorts || {}).sort((a,b) => b[1].trace_count - a[1].trace_count);
  const coverageRows = Object.entries(dim.coverage?.surfaces || {}).sort((a,b) => a[0].localeCompare(b[0]));
  renderTable('coverage-table', ['log surface','events','status','reading'], coverageRows.map(([name,item]) => [
    name,
    item.events || 0,
    item.status || 'missing',
    item.status === 'present' ? '可作为 observed evidence 进入定界' : '观测盲区，不能把缺失当根因'
  ]));
  renderTable('cohort-table', ['cohort','traces','classifications','errors','access'], cohortRows.map(([name,item]) => [
    name,
    item.trace_count,
    JSON.stringify(item.classifications || {}),
    JSON.stringify(item.errors || {}),
    pctText(item.access_latency_ms)
  ]));
  renderTable('classification-table', ['classification','count'], classificationRows);
  renderTable('recommendation-table', ['category','title','detail'], recommendations.map(item => [
    item.category,
    item.title,
    item.detail
  ]));
  renderTable('source-appendix-table', ['log surface','flow stage','source hint','validation','report reading'], sourceAppendix.map(item => [
    item.log_surface,
    item.flow_stage,
    item.source_hint,
    item.validation,
    item.report_reading
  ]));
  function renderFlowStageTable(id, graph) {
    renderTable(id, ['stage','summary','operation','IPs','reason','status'], (graph.edges || []).map(edge => [
      edge.name,
      edge.summary || '',
      edge.operation,
      (edge.rollup?.top_ips || []).join(', '),
      edge.reason || edge.report_reading,
      edge.status
    ]));
  }
  renderFlowStageTable('flow-stage-table', flowStages);
  renderFlowStageTable('read-flow-stage-table', readFlowStages);
  renderFlowStageTable('write-flow-stage-table', writeFlowStages);
  renderTable('error-table', ['error','count'], errorRows);
  const latencyRowsForTable = [
    ['access', pctText(dim.latency_ms?.access)],
    ...Object.entries(dim.urma_elapsed || {}).map(([k,v]) => [`urma.${k}`, pctText(v)]),
    ...Object.entries(dim.latency_summary_us || {}).map(([k,v]) => [`latencySummary.${k}`, pctText(v)])
  ].filter(row => row[1]);
  const latencyChartRows = [
    ['access', scalePercentiles(dim.latency_ms?.access, 1)],
    ...Object.entries(dim.urma_elapsed || {}).map(([k,v]) => [`urma.${k}`, scalePercentiles(v, 1)]),
    ...Object.entries(dim.latency_summary_us || {}).map(([k,v]) => [`latencySummary.${k}`, scalePercentiles(v, 0.001)])
  ].filter(([, item]) => item && item.count).sort((a,b) => (b[1].max || 0) - (a[1].max || 0)).slice(0, 12);
  renderTable('latency-table', ['metric','distribution'], latencyRowsForTable);
  const flowRows = Object.entries(dim.flow || {}).sort((a,b) => b[1]-a[1]);
  const workerRows = Object.entries(dim.worker_summary || {})
    .sort((a,b) => (b[1].error_count || 0) - (a[1].error_count || 0) || (b[1].line_count || 0) - (a[1].line_count || 0))
    .slice(0, 40);
  const ubRows = Object.entries(dim.ub_summary?.edges || {})
    .sort((a,b) => (b[1].count || 0) - (a[1].count || 0))
    .slice(0, 40);
  const timeBucketRows = dim.time_buckets?.['1000ms'] || [];
  renderTable('flow-table', ['flow','count'], flowRows);
  const workerTableRows = workerRows
    .map(([worker,item]) => [worker, (item.roles || []).join(','), item.line_count, item.trace_count, item.slow_trace_count || 0, item.error_count || 0]);
  renderPagedTable('worker-table', 'worker-table-pager', ['worker','roles','lines','traces','slow','errors'], workerTableRows,
    row => (Number(row[5]) > 0 ? 'class="hotrow"' : Number(row[4]) > 0 ? 'class="warnrow"' : ''), 5);
  function renderOperationViews() {
    const showRead = activeOperation !== 'write';
    const showWrite = activeOperation !== 'read';
    renderFlowStageTable('read-flow-stage-table', showRead ? readFlowStages : {edges: []});
    renderFlowStageTable('write-flow-stage-table', showWrite ? writeFlowStages : {edges: []});
    renderFlowGraph('read-flow-stage-chart', showRead ? readFlowStages : {nodes: [], edges: []}, 'Read Flow');
    renderFlowGraph('write-flow-stage-chart', showWrite ? writeFlowStages : {nodes: [], edges: []}, 'Write Flow');
    const visibleUbRows = activeOperation === 'write' ? [] : ubRows;
    const ubTableRows = visibleUbRows
      .map(([edge,item]) => [edge, item.count, item.latency_ms?.p99 || '', item.latency_ms?.max || '', pctText(item.latency_ms)]);
    renderPagedTable('ub-edge-table', 'ub-edge-table-pager', ['edge','count','p99 ms','max ms','latency'], ubTableRows,
      row => `class="${severityClass(Math.max(Number(row[2]) || 0, Number(row[3]) || 0))}"`, 5);
    chart('ub-edge-chart', visibleUbRows.length ? axisBase('UB Edge Count / Tail Latency', {xAxis:{type:'category', data:visibleUbRows.slice(0,20).map(r => r[0]), axisLabel:{rotate:35, width:150, overflow:'truncate'}}, yAxis:[{type:'value', name:'count'}, {type:'value', name:'ms'}], series:[
      {name:'UB edges',type:'bar',barMaxWidth:34,data:visibleUbRows.slice(0,20).map(r => r[1].count || 0), itemStyle:{color:'#059669'}, label:{show:true, position:'top'}},
      {name:'p99 ms',type:'line',yAxisIndex:1,smooth:true,data:visibleUbRows.slice(0,20).map(r => r[1].latency_ms?.p99 || 0), itemStyle:{color:'#dc2626'}, markLine:{symbol:'none', lineStyle:{color:'#dc2626',type:'dashed'}, label:{formatter:'20ms'}, data:[{yAxis:20}]}}
    ]}) : noDataOption(activeOperation === 'write' ? 'Write flow has no UB read edge data' : 'No UB edge data'));
  }
  function applyTraceFilters() {
    const query = document.getElementById('trace-search').value.trim().toLowerCase();
    const cls = document.getElementById('class-filter').value;
    const worker = document.getElementById('worker-filter').value;
    filteredTraceRows = traceRows.filter(([traceId, item]) => {
      const haystack = [
        traceId,
        item.classification,
        JSON.stringify(item.errors || {}),
        Object.keys(item.workers || {}).join(' '),
        (item.evidence || []).map(e => e.text).join(' ')
      ].join(' ').toLowerCase();
      return (!cls || item.classification === cls)
        && (!worker || Object.prototype.hasOwnProperty.call(item.workers || {}, worker))
        && operationMatches(item)
        && (!query || haystack.includes(query));
    });
    if (!filteredTraceRows.some(([traceId]) => traceId === selectedTraceId)) {
      selectedTraceId = filteredTraceRows[0]?.[0] || null;
    }
  }
  function renderTracePage() {
    applyTraceFilters();
    const totalPages = Math.max(Math.ceil(filteredTraceRows.length / pageSize), 1);
    currentPage = Math.min(Math.max(currentPage, 0), totalPages - 1);
    const pageRows = filteredTraceRows.slice(currentPage * pageSize, (currentPage + 1) * pageSize);
    renderTable('top-trace-table', ['trace','classification','errors','access','workers'], pageRows.map(([traceId,item]) => [
      traceId,
      item.classification,
      JSON.stringify(item.errors || {}),
      pctText(item.access_latency_ms),
      Object.keys(item.workers || {}).slice(0, 6).join(', ')
    ]), row => `data-trace="${escapeHtml(row[0])}" class="${row[0] === selectedTraceId ? 'selected-row' : ''}"`);
    document.querySelectorAll('#top-trace-table tbody tr').forEach(row => row.addEventListener('click', () => {
      selectedTraceId = row.getAttribute('data-trace');
      renderTracePage();
      renderSelectedTrace();
    }));
    document.getElementById('page-status').textContent = `第 ${currentPage + 1} / ${totalPages} 页，每页 ${pageSize} 条，共 ${filteredTraceRows.length} 条 trace`;
    document.getElementById('prev-page').disabled = currentPage === 0;
    document.getElementById('next-page').disabled = currentPage >= totalPages - 1;
  }
  function renderSelectedTrace() {
    const item = traces[selectedTraceId] || {};
    renderTable('selected-trace-table', ['field','value'], [
      ['trace', selectedTraceId || ''],
      ['classification', item.classification || ''],
      ['errors', JSON.stringify(item.errors || {})],
      ['access', pctText(item.access_latency_ms)],
      ['coverage', JSON.stringify(item.evidence_coverage || {})],
      ['missing_evidence', JSON.stringify(item.missing_evidence || [])]
    ]);
    const stageRows = (item.stage_breakdown || [])
      .filter(s => s.duration_ms !== undefined)
      .filter(s => stageMatchesOperation(s.stage))
      .slice()
      .sort((a,b) => (a.duration_ms || 0) - (b.duration_ms || 0));
    renderTable('selected-stage-table', ['研发流程','duration ms','confidence','source'], stageRows.map(s => [
      stageDetailText(s.stage),
      s.duration_ms,
      s.confidence || '',
      s.source || ''
    ]), row => `class="${severityClass(row[1])}"`);
    const stageColors = ['#2563eb','#ea580c','#059669','#7c3aed','#dc2626','#0891b2','#ca8a04','#64748b'];
    document.getElementById('selected-stage-legend').innerHTML = stageRows.map((stage, idx) =>
      `<span class="stage-pill"><i class="stage-dot" style="background:${stageColors[idx % stageColors.length]}"></i>${escapeHtml(stageDisplayName(stage.stage))}</span>`
    ).join('');
    chart('selected-trace-chart', stageRows.length ? axisBase('Selected Trace Stage Breakdown', {
      legend:{show:false},
      dataZoom:[],
      tooltip:{trigger:'axis',axisPointer:{type:'shadow'},formatter:ps => ps.filter(p => p.data && p.data.value != null).map(p => `研发流程: ${escapeHtml(p.data.display)}<br>耗时: ${p.data.value}ms<br>原始字段: ${escapeHtml(p.data.rawStage || '')}<br>观测来源: ${escapeHtml(p.data.source || '')}`).join('<br><br>')},
      grid:{left:96,right:72,top:44,bottom:36,containLabel:true},
      xAxis:{type:'value', name:'ms'},
      yAxis:{type:'category', data:stageRows.map(s => stageDisplayName(s.stage)), axisLabel:{width:104, overflow:'truncate'}},
      series:[{
        name:'duration_ms',
        type:'bar',
        barMaxWidth:28,
        data:stageRows.map((row, rowIndex) => ({value:row.duration_ms, display:stageDisplayName(row.stage), rawStage:row.stage, source:row.source || '', itemStyle:{color:stageColors[rowIndex % stageColors.length]}})),
        label:{show:true, position:'right', formatter:p => p.data && p.data.value != null ? `${p.data.value}ms` : ''},
        markLine:{symbol:'none', lineStyle:{color:'#dc2626',type:'dashed'}, label:{formatter:'20ms deadline'}, data:[{xAxis:20}]}
      }]
    }) : noDataOption('No selected trace stage data'));
    document.getElementById('selected-trace-log').innerHTML = renderTraceLogBlocks(item.evidence || []);
  }
  document.getElementById('prev-page').addEventListener('click', () => { currentPage -= 1; renderTracePage(); });
  document.getElementById('next-page').addEventListener('click', () => { currentPage += 1; renderTracePage(); });
  document.getElementById('trace-search').addEventListener('input', () => { currentPage = 0; renderTracePage(); renderSelectedTrace(); });
  document.getElementById('operation-filter').addEventListener('change', event => {
    activeOperation = event.target.value;
    currentPage = 0;
    renderOperationViews();
    renderTracePage();
    renderSelectedTrace();
  });
  document.getElementById('class-filter').innerHTML = '<option value="">全部分类</option>' +
    classificationRows.map(([name]) => `<option value="${escapeHtml(name)}">${escapeHtml(name)}</option>`).join('');
  document.getElementById('class-filter').addEventListener('change', () => { currentPage = 0; renderTracePage(); renderSelectedTrace(); });
  const traceWorkerNames = [...new Set(traceRows.flatMap(([, item]) => Object.keys(item.workers || {})))].sort();
  document.getElementById('worker-filter').innerHTML = '<option value="">全部 Worker</option>' +
    traceWorkerNames.map(name => `<option value="${escapeHtml(name)}">${escapeHtml(name)}</option>`).join('');
  document.getElementById('worker-filter').addEventListener('change', () => { currentPage = 0; renderTracePage(); renderSelectedTrace(); });
  document.getElementById('trace-page-size').addEventListener('change', event => {
    pageSize = Number(event.target.value) || 8;
    currentPage = 0;
    renderTracePage();
    renderSelectedTrace();
  });
  document.getElementById('reset-filter').addEventListener('click', () => {
    document.getElementById('trace-search').value = '';
    document.getElementById('class-filter').value = '';
    document.getElementById('worker-filter').value = '';
    document.getElementById('operation-filter').value = '';
    document.getElementById('trace-page-size').value = '8';
    activeOperation = '';
    pageSize = 8;
    currentPage = 0;
    renderOperationViews();
    renderTracePage();
    renderSelectedTrace();
  });
  document.getElementById('download-selected-raw').addEventListener('click', () => {
    const item = traces[selectedTraceId] || {};
    const text = (item.evidence || []).map(e => `${e.member}:${e.line} ${e.text}`).join('\\n');
    downloadText(`${selectedTraceId || 'trace'}-raw.log`, text);
  });
  document.getElementById('download-filtered-evidence').addEventListener('click', () => {
    const rows = filteredTraceRows.map(([traceId, item]) => [
      `# ${traceId} ${item.classification || ''}`,
      ...(item.evidence || []).map(e => `${e.member}:${e.line} ${e.text}`)
    ].join('\\n')).join('\\n\\n');
    downloadText('filtered-trace-evidence.log', rows);
  });
  document.getElementById('download-report-summary').addEventListener('click', () => {
    const lines = [
      '# DataSystem Trace Report Summary',
      '',
      `- case_name: ${manifest.case_name || ''}`,
      `- scenario: ${manifest.scenario || ''}`,
      `- code_ref: ${report.code_ref || ''}`,
      `- trace_count: ${report.trace_count || 0}`,
      '',
      '## Diagnosis',
      ...diagnosisRows.map(item => `- ${item.label}: ${item.text}`),
      '',
      '## Evidence Coverage',
      ...coverageRows.map(([name,item]) => `- ${name}: ${item.status || 'missing'} (${item.events || 0} events)`),
      '',
      '## Recommendations',
      ...recommendations.map(item => `- [${item.category}] ${item.title}: ${item.detail}`),
      '',
      '## Source Mapping',
      ...sourceAppendix.map(item => `- ${item.log_surface} | ${item.flow_stage} | ${item.source_hint}`),
      '',
      '## Inputs',
      ...(manifest.inputs || []).map(item => `- ${item.path} (${item.size || 0} bytes)`),
      '',
    ];
    downloadText('trace-report-summary.md', lines.join('\\n'));
  });
  const navLinks = [...document.querySelectorAll('#nav a')];
  window.addEventListener('scroll', () => {
    let active = navLinks[0];
    for (const link of navLinks) {
      const node = document.querySelector(link.getAttribute('href'));
      if (node && node.getBoundingClientRect().top < 120) active = link;
    }
    navLinks.forEach(link => link.classList.toggle('active', link === active));
  });
  chart('classification-chart', classificationRows.length ? {title:{text:'Classification', left:'center'}, color:palette, tooltip:{trigger:'item', confine:true}, legend:{type:'scroll', bottom:0}, toolbox:{right:10,feature:{saveAsImage:{},dataView:{readOnly:true},restore:{}}}, series:[{type:'pie', radius:['38%','66%'], center:['50%','48%'], avoidLabelOverlap:true, data:classificationRows.map(([name,value]) => ({name,value}))}]} : noDataOption('No classification data'));
  chart('cohort-chart', cohortRows.length ? axisBase('Input Cohort Comparison', {xAxis:{type:'category', data:cohortRows.map(r => r[0]), axisLabel:{rotate:20, width:110, overflow:'truncate'}}, yAxis:{type:'value'}, series:[
    {name:'traces',type:'bar',barMaxWidth:42,data:cohortRows.map(r => r[1].trace_count || 0), label:{show:true, position:'top'}},
    {name:'errors',type:'bar',barMaxWidth:42,data:cohortRows.map(r => Object.values(r[1].errors || {}).reduce((a,b) => a+b, 0)), label:{show:true, position:'top'}, itemStyle:{color:'#dc2626'}}
  ]}) : noDataOption('No cohort data'));
  chart('error-chart', errorRows.length ? axisBase('Errors', {xAxis:{type:'category', data:errorRows.map(r => r[0]), axisLabel:{rotate:25, width:130, overflow:'truncate'}}, yAxis:{type:'value'}, series:[{type:'bar', barMaxWidth:46, data:errorRows.map(r => ({value:r[1], itemStyle:{color:'#dc2626'}})), label:{show:true, position:'top'}}]}) : noDataOption('No error data'));
  chart('latency-chart', latencyChartRows.length ? axisBase('Top Latency Metrics', {
    grid:{left:132,right:42,top:66,bottom:42,containLabel:true},
    tooltip:{trigger:'axis',axisPointer:{type:'shadow'},confine:true,formatter:ps => {
      const idx = ps[0]?.dataIndex ?? 0;
      const rawName = latencyChartRows[idx]?.[0] || '';
      const lines = [`${escapeHtml(metricLabel(rawName))}`, `<span class="muted">${escapeHtml(rawName)}</span>`];
      ps.forEach(p => lines.push(`${escapeHtml(p.seriesName)}: ${p.value} ms`));
      return lines.join('<br>');
    }},
    xAxis:{type:'value', name:'ms'},
    yAxis:{type:'category', data:latencyChartRows.map(r => metricLabel(r[0])), axisLabel:{width:116, overflow:'truncate'}},
    series:['p50','p90','p99','max'].map(key => ({name:key,type:'bar',barMaxWidth:18,data:latencyChartRows.map(([, item]) => item[key] || 0), markLine:key === 'max' ? {symbol:'none', lineStyle:{color:'#dc2626',type:'dashed'}, label:{formatter:'20ms deadline'}, data:[{xAxis:20}]} : undefined}))
  }) : noDataOption('No latency metric data'));
  chart('flow-chart', flowRows.length ? axisBase('Flow Breakdown', {
    grid:{left:112,right:42,top:56,bottom:42,containLabel:true},
    tooltip:{trigger:'axis',axisPointer:{type:'shadow'},confine:true,formatter:ps => {
      const idx = ps[0]?.dataIndex ?? 0;
      const rawName = flowRows[idx]?.[0] || '';
      return `${escapeHtml(flowLabel(rawName))}<br><span class="muted">${escapeHtml(rawName)}</span><br>count: ${ps[0]?.value ?? 0}`;
    }},
    xAxis:{type:'value', name:'count'},
    yAxis:{type:'category', data:flowRows.map(r => flowLabel(r[0])), axisLabel:{width:96, overflow:'truncate'}},
    series:[{name:'count',type:'bar',barMaxWidth:28,data:flowRows.map(r => r[1]),itemStyle:{color:'#2563eb'},label:{show:true,position:'right'}}]
  }) : noDataOption('No flow data'));
  const timeStageNames = [...new Set(timeBucketRows.flatMap(row => Object.keys(row.stage_breakdown_ms || {})))]
    .filter(name => !name.endsWith('client_to_entry_worker'));
  chart('time-breakdown-chart', timeBucketRows.length ? axisBase('Time Bucket Latency Stages', {
    tooltip:{trigger:'axis', axisPointer:{type:'cross'}, confine:true},
    xAxis:{type:'category', data:timeBucketRows.map(r => String(r.bucket_start || '').replace('T','\\n')), axisLabel:{rotate:0}},
    yAxis:{type:'value', name:'ms'},
    series:[
      ...timeStageNames.map(name => ({name:stageLabelMap[name] || name,type:'bar',stack:'stage-p99',barMaxWidth:34,data:timeBucketRows.map(r => r.stage_breakdown_ms?.[name]?.p99 || 0),itemStyle:{color:stageColorMap[name] || '#64748b'}})),
      {name:'client/access p99 upper bound',type:'line',smooth:true,data:timeBucketRows.map(r => r.p99_access_ms || 0), itemStyle:{color:'#2563eb'}, markLine:{symbol:'none', lineStyle:{color:'#dc2626',type:'dashed'}, label:{formatter:'20ms deadline'}, data:[{yAxis:20}]}}
    ]
  }) : noDataOption('No time bucket stage data'));
  function renderFlowGraph(id, graph, title) {
    chart(id, {
    title:{text:title, left:'center', top:4, textStyle:{fontSize:14}},
    tooltip:{trigger:'item', formatter:p => p.dataType === 'edge'
      ? `${escapeHtml(p.data.name)}<br>${escapeHtml(p.data.summary || '')}<br>${escapeHtml(p.data.operation)}<br>${escapeHtml(p.data.reason || '')}<br>${escapeHtml(p.data.evidence || '')}`
      : `${escapeHtml(p.data.label || p.data.name)}<br>${escapeHtml((p.data.top_ips || []).join(', '))}`},
    series:[{
      type:'graph',
      layout:'none',
      roam:true,
      edgeSymbol:['none','arrow'],
      edgeSymbolSize:8,
      label:{show:true},
      edgeLabel:{show:true, formatter:p => p.data.summary || (p.data.status === 'present' ? 'present' : 'missing'), fontSize:11, width:120, overflow:'break'},
      lineStyle:{width:2, color:'#64748b', curveness:.08},
      data:(graph.nodes || []).map((node, idx) => ({
        name:node.id,
        label:[node.label, ...(node.top_workers || []), ...(node.top_ips || [])].slice(0, 3).join('\\n'),
        top_ips:node.top_ips || [],
        x:[80,280,480,480,680][idx] || 80,
        y:[170,170,80,260,260][idx] || 170,
        symbolSize:72,
        itemStyle:{color:{client:'#2563eb',entry_worker:'#059669',meta_worker:'#7c3aed',data_worker:'#ea580c',transport:'#64748b'}[node.role] || '#94a3b8'}
      })),
      links:(graph.edges || []).map(edge => ({
        source:edge.source,
        target:edge.target,
        name:edge.name,
        operation:edge.operation,
        evidence:edge.evidence,
        summary:edge.summary,
        reason:edge.reason,
        rollup:edge.rollup,
        status:edge.status,
        lineStyle:{color:edge.rollup?.max_ms >= 20 ? '#dc2626' : edge.rollup?.max_ms >= 5 ? '#ea580c' : edge.status === 'present' ? '#2563eb' : '#cbd5e1', width:edge.rollup?.max_ms >= 20 ? 4 : 2, type:edge.status === 'present' ? 'solid' : 'dashed'}
      }))
    }]
  });
  }
  chart('worker-chart', workerRows.length ? axisBase('Top Workers by Trace/Error', {xAxis:{type:'category', data:workerRows.slice(0,20).map(r => r[0]), axisLabel:{rotate:35, width:120, overflow:'truncate'}}, yAxis:{type:'value'}, series:[
    {name:'traces',type:'bar',barMaxWidth:34,data:workerRows.slice(0,20).map(r => r[1].trace_count || 0), itemStyle:{color:'#94a3b8'}},
    {name:'slow',type:'bar',barMaxWidth:34,data:workerRows.slice(0,20).map(r => r[1].slow_trace_count || 0), itemStyle:{color:'#ea580c'}},
    {name:'errors',type:'bar',barMaxWidth:34,data:workerRows.slice(0,20).map(r => r[1].error_count || 0), itemStyle:{color:'#dc2626'}, label:{show:true, position:'top'}}
  ]}) : noDataOption('No worker data'));
  renderOperationViews();
  renderTracePage();
  renderSelectedTrace();
  document.getElementById('trace-data').textContent = JSON.stringify(traces, null, 2);
  </script>
  __SCRIPT_REF__
</body>
</html>
"""
    return (template
            .replace("__TITLE__", title)
            .replace("__STYLESHEET__", stylesheet)
            .replace("__DATA__", data)
            .replace("__MANIFEST__", manifest_data)
            .replace("__SCRIPT_REF__", script_ref))


def _build_events(report):
    events = []
    for trace_id, trace in report["traces"].items():
        for evidence in trace.get("evidence", []):
            events.append({
                "schema_version": 1,
                "trace_id": trace_id,
                "ts": evidence["text"].split(" | ", 1)[0],
                "worker": next(iter(trace.get("workers", {"unknown": 1}))),
                "event_type": "raw",
                "source": evidence["source"],
                "member": evidence["member"],
                "line": evidence["line"],
                "raw": evidence["text"],
            })
        for event in trace.get("ub_events", []):
            row = dict(event)
            row["schema_version"] = 1
            row["trace_id"] = trace_id
            row["event_type"] = "ub_" + row["event_type"]
            events.append(row)
    return events


def _build_triage(report):
    by_class = Counter(trace["classification"] for trace in report["traces"].values())
    candidates = []
    for classification, count in by_class.most_common():
        representatives = [
            trace_id for trace_id, trace in report["traces"].items() if trace["classification"] == classification
        ][:5]
        candidates.append({
            "classification": classification,
            "trace_count": count,
            "representative_traces": representatives,
            "evidence_boundary": "observed",
        })
    return {
        "schema_version": 1,
        "root_cause_families": dict(by_class),
        "issue_candidates": candidates,
    }


class TraceReportRenderer:
    """Render machine summaries into stage artifacts."""

    def events(self, report):
        return _build_events(report)

    def triage(self, report):
        return _build_triage(report)

    def markdown(self, report):
        return render_markdown(report)

    def html(self, report, title, site=False, manifest=None):
        return _render_html(report, title, site=site, manifest=manifest)


def _new_run_dir(out_root, case_name, cache_key):
    now = datetime.now()
    run_dir = out_root / f"{now.strftime('%Y%m%d-%H%M%S')}-{_slug(case_name)}-{cache_key[:8]}"
    suffix = 1
    while run_dir.exists():
        suffix += 1
        run_dir = out_root / f"{now.strftime('%Y%m%d-%H%M%S')}-{_slug(case_name)}-{suffix}"
    run_dir.mkdir(parents=True)
    return run_dir, now


class TraceRunPipeline:
    """Manage staged run directories, cache, manifest state, and render targets."""

    def __init__(self, analyzer=None, renderer=None):
        self.analyzer = analyzer or TraceAnalyzer()
        self.renderer = renderer or TraceReportRenderer()

    def parse(self, inputs, out_dir, case_name="trace-case", scenario="", code_ref="unknown", force=False):
        out_root = Path(out_dir)
        out_root.mkdir(parents=True, exist_ok=True)
        cache_key, identities = _cache_key(inputs, code_ref, case_name, scenario)
        if not force:
            cached = _find_cached_run(out_root, cache_key)
            if cached:
                return cached
        run_dir, now = _new_run_dir(out_root, case_name, cache_key)
        _preserve_raw_inputs(inputs, run_dir)

        report = self.analyzer.analyze(inputs, code_ref=code_ref)
        events = self.renderer.events(report)
        manifest = {
            "schema_version": 1,
            "case_name": case_name,
            "scenario": scenario,
            "analysis_created_at": now.isoformat(),
            "code_ref": code_ref,
            "script_version": _script_version(),
            "cache": {"key": cache_key, "status": "created"},
            "trace_time_range": report["dimensions"]["time"],
            "input_document": "inputs.md",
            "inputs": identities,
            "stages": {
                "parse": {"status": "done", "path": "parsed_traces.json"},
                "aggregate": {"status": "pending"},
                "triage": {"status": "pending"},
            },
            "render_targets": {
                "local": {"path": "report.local.html", "status": "pending"},
                "site": {"path": "report.site.html", "status": "pending"},
            },
        }
        _write_json(run_dir / "manifest.json", manifest)
        _write_inputs_doc(run_dir, manifest)
        (run_dir / "events.jsonl").write_text(
            "".join(json.dumps(event, ensure_ascii=False) + "\n" for event in events), encoding="utf-8"
        )
        _write_json(run_dir / "parsed_traces.json", report)
        return run_dir

    def aggregate(self, run_dir):
        run_dir = Path(run_dir)
        report = _read_json(run_dir / "parsed_traces.json")
        _write_json(run_dir / "summary.json", report)
        _update_manifest(run_dir, lambda manifest: manifest["stages"].update({
            "aggregate": {"status": "done", "path": "summary.json"}
        }))
        return run_dir / "summary.json"

    def triage(self, run_dir):
        run_dir = Path(run_dir)
        report = _read_json(run_dir / "summary.json")
        triage = self.renderer.triage(report)
        _write_json(run_dir / "triage.json", triage)
        (run_dir / "triage.md").write_text(self.renderer.markdown(report), encoding="utf-8")
        _update_manifest(run_dir, lambda manifest: manifest["stages"].update({
            "triage": {"status": "done", "path": "triage.json", "markdown": "triage.md"}
        }))
        return run_dir / "triage.json"

    def render_local(self, run_dir):
        run_dir = Path(run_dir)
        report = _read_json(run_dir / "summary.json")
        manifest = _read_json(run_dir / "manifest.json")
        title = f"Trace Triage: {manifest.get('case_name', 'trace-case')}"
        (run_dir / "report.local.html").write_text(
            self.renderer.html(report, title, manifest=manifest), encoding="utf-8"
        )
        _update_manifest(run_dir, lambda item: item["render_targets"].update({
            "local": {"path": "report.local.html", "status": "generated"}
        }))
        return run_dir / "report.local.html"

    def render_site(self, run_dir):
        run_dir = Path(run_dir)
        report = _read_json(run_dir / "summary.json")
        manifest = _read_json(run_dir / "manifest.json")
        title = f"Trace Triage: {manifest.get('case_name', 'trace-case')}"
        (run_dir / "report.site.html").write_text(
            self.renderer.html(report, title, site=True, manifest=manifest), encoding="utf-8"
        )
        publish_doc = _write_site_publish_doc(run_dir, manifest)
        _update_manifest(run_dir, lambda item: item["render_targets"].update({
            "site": {"path": "report.site.html", "status": "generated", **publish_doc}
        }))
        return run_dir / "report.site.html"

    def run(self, inputs, out_dir, case_name="trace-case", scenario="", code_ref="unknown", force=False):
        run_dir = self.parse(inputs, out_dir, case_name=case_name, scenario=scenario,
                             code_ref=code_ref, force=force)
        if not (run_dir / "summary.json").exists():
            self.aggregate(run_dir)
        if not (run_dir / "triage.json").exists():
            self.triage(run_dir)
        if not (run_dir / "report.local.html").exists():
            self.render_local(run_dir)
        if not (run_dir / "report.site.html").exists():
            self.render_site(run_dir)
        return run_dir


def parse_stage(inputs, out_dir, case_name="trace-case", scenario="", code_ref="unknown", force=False):
    return TraceRunPipeline().parse(inputs, out_dir, case_name=case_name, scenario=scenario,
                                    code_ref=code_ref, force=force)


def aggregate_stage(run_dir):
    return TraceRunPipeline().aggregate(run_dir)


def triage_stage(run_dir):
    return TraceRunPipeline().triage(run_dir)


def render_local_stage(run_dir):
    return TraceRunPipeline().render_local(run_dir)


def render_site_stage(run_dir):
    return TraceRunPipeline().render_site(run_dir)


def _publish_size_guard(source_html, max_bytes=DEFAULT_SITE_HTML_MAX_BYTES):
    source_html = Path(source_html)
    size = source_html.stat().st_size
    return {
        "source_size_bytes": size,
        "max_site_html_bytes": max_bytes,
        "size_status": "ok" if size <= max_bytes else "too_large",
    }


def publish_site_stage(run_dir, dry_run=True, max_site_html_bytes=DEFAULT_SITE_HTML_MAX_BYTES):
    run_dir = Path(run_dir)
    manifest = _read_json(run_dir / "manifest.json")
    site_target = manifest.get("render_targets", {}).get("site", {})
    if not (run_dir / site_target.get("path", "report.site.html")).exists():
        TraceRunPipeline().render_site(run_dir)
        manifest = _read_json(run_dir / "manifest.json")
        site_target = manifest.get("render_targets", {}).get("site", {})
    if not (run_dir / site_target.get("publish_doc", "site_publish.md")).exists():
        publish_doc = _write_site_publish_doc(run_dir, manifest)
        site_target = {**site_target, **publish_doc}
    source_html = run_dir / site_target.get("path", "report.site.html")
    size_guard = _publish_size_guard(source_html, max_site_html_bytes)
    live_markers = "not-run"
    if dry_run:
        status = "dry-run"
    elif size_guard["size_status"] != "ok":
        status = "blocked-size-limit"
        _update_manifest(run_dir, lambda item: item["render_targets"]["site"].update({
            **site_target,
            "publish": {
                "status": status,
                "url": site_target.get("url", ""),
                "target_path": site_target.get("target_path", ""),
                "live_markers": live_markers,
                **size_guard,
            },
        }))
        raise SystemExit(
            f"Refuse to publish oversized site HTML: {source_html} is "
            f"{size_guard['source_size_bytes']} bytes > {max_site_html_bytes} bytes. "
            "Review the report or raise --max-site-html-mb intentionally."
        )
    else:
        target_path = site_target.get("target_path", "")
        url = site_target.get("url", "")
        subprocess.run(["scp", str(source_html), f"xqyun-32c32g:{target_path}"], check=True)
        subprocess.run(["curl", "-fsSI", url], check=True)
        result = subprocess.run(["curl", "-fsSL", "-A", "Mozilla/5.0", url],
                                check=True, capture_output=True, text=True)
        for marker in [
            "Trace 分析报告",
            'id="coverage-table"',
            'id="flow-stage-chart"',
            'id="download-report-summary"',
            "/assets/css/site.css",
            "/assets/js/site.js",
        ]:
            assert marker in result.stdout, marker
        live_markers = "verified"
        status = "published"
    _update_manifest(run_dir, lambda item: item["render_targets"]["site"].update({
        **site_target,
        "publish": {
            "status": status,
            "url": site_target.get("url", ""),
            "target_path": site_target.get("target_path", ""),
            "live_markers": live_markers,
            **size_guard,
        },
    }))
    return site_target.get("url", "")


def _verify_html_inline_script(html_path):
    html_path = Path(html_path)
    html = html_path.read_text(encoding="utf-8")
    match = re.search(r"<script>\n  const report = (.*)\n  </script>", html, re.S)
    assert match, "inline report script not found"
    node = shutil.which("node")
    if not node:
        return "inline-script-present"
    with tempfile.NamedTemporaryFile("w", suffix=".js", encoding="utf-8", delete=False) as tmp:
        tmp.write("const report = " + match.group(1))
        tmp_path = Path(tmp.name)
    try:
        subprocess.run([node, "--check", str(tmp_path)], check=True)
    finally:
        try:
            tmp_path.unlink()
        except OSError:
            pass
    return "node-check-passed"


def run_pipeline(inputs, out_dir, case_name="trace-case", scenario="", code_ref="unknown", force=False):
    return TraceRunPipeline().run(inputs, out_dir, case_name=case_name, scenario=scenario,
                                  code_ref=code_ref, force=force)


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
        run_dir = run_pipeline([str(bundle)], Path(tmp) / "runs", case_name="self-test", scenario="fixture",
                               code_ref="self-test")
        assert (run_dir / "manifest.json").exists()
        assert (run_dir / "events.jsonl").exists()
        assert (run_dir / "summary.json").exists()
        assert (run_dir / "triage.json").exists()
        assert (run_dir / "report.local.html").exists()
        assert (run_dir / "report.site.html").exists()
        assert _verify_html_inline_script(run_dir / "report.local.html") in {
            "inline-script-present", "node-check-passed"
        }
        assert (run_dir / "site_publish.md").exists()
        publish_url = publish_site_stage(run_dir, dry_run=True)
        assert publish_url.startswith("https://yche.me/perf/")
        assert (run_dir / "raw" / "inputs" / "fixture.tar.gz").exists()
        assert (run_dir / "raw" / "extracted" / "fixture.tar.gz" / "kvchachjpworker-0-worker7" / "worker.log").exists()
        manifest = json.loads((run_dir / "manifest.json").read_text(encoding="utf-8"))
        assert manifest["render_targets"]["site"]["publish_doc"] == "site_publish.md"
        assert manifest["render_targets"]["site"]["publish"]["status"] == "dry-run"
        run_report = json.loads((run_dir / "summary.json").read_text(encoding="utf-8"))
        run_trace = next(iter(run_report["traces"].values()))
        assert run_trace["stage_breakdown"]
        assert run_trace["evidence_coverage"]["urma"] == "present"
        assert run_report["dimensions"]["coverage"]["surfaces"]["urma_elapsed"]["status"] == "present"
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
    argv = list(argv or sys.argv[1:])
    stage_commands = {"parse", "aggregate", "triage", "render-local", "render-site", "publish-site"}
    if argv and argv[0] in ({"run", "verify"} | stage_commands):
        command = argv.pop(0)
        if command == "verify":
            parser = argparse.ArgumentParser(description="Run built-in trace triage verification.")
            parser.add_argument("--output-json", help="Write machine-readable summary JSON.")
            args = parser.parse_args(argv)
            report = run_self_test()
            print("verify passed")
            if args.output_json:
                Path(args.output_json).write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n",
                                                  encoding="utf-8")
            return 0
        if command == "parse":
            parser = argparse.ArgumentParser(description="Parse trace inputs into a staged run directory.")
            parser.add_argument("inputs", nargs="+", help="Log files, directories, or gzip-wrapped tar bundles.")
            parser.add_argument("--out", required=True, help="Output root for timestamped run directories.")
            parser.add_argument("--case", default="trace-case", help="Case name stored in manifest.")
            parser.add_argument("--scenario", default="", help="Scenario description stored in manifest.")
            parser.add_argument("--code-ref", default="unknown", help="Source ref used for CodeGraph/source validation.")
            parser.add_argument("--force", action="store_true", help="Create a fresh run even when cache matches.")
            args = parser.parse_args(argv)
            print(parse_stage(args.inputs, args.out, case_name=args.case, scenario=args.scenario,
                              code_ref=args.code_ref, force=args.force))
            return 0
        if command in ("aggregate", "triage", "render-local", "render-site", "publish-site"):
            parser = argparse.ArgumentParser(description=f"Run trace triage {command} stage.")
            parser.add_argument("run_dir", help="Existing staged run directory.")
            if command == "publish-site":
                parser.add_argument("--dry-run", action="store_true", help="Prepare publish metadata without copying.")
                parser.add_argument("--max-site-html-mb", type=float, default=2.0,
                                    help="Refuse real yche.me publish when report.site.html exceeds this size.")
            args = parser.parse_args(argv)
            if command == "aggregate":
                print(aggregate_stage(args.run_dir))
            elif command == "triage":
                print(triage_stage(args.run_dir))
            elif command == "render-local":
                print(render_local_stage(args.run_dir))
            elif command == "render-site":
                print(render_site_stage(args.run_dir))
            else:
                max_bytes = int(args.max_site_html_mb * 1024 * 1024)
                url = publish_site_stage(args.run_dir, dry_run=args.dry_run, max_site_html_bytes=max_bytes)
                print(f"{'DRY-RUN ' if args.dry_run else ''}{url}")
            return 0
        parser = argparse.ArgumentParser(description="Run staged DataSystem trace triage.")
        parser.add_argument("inputs", nargs="+", help="Log files, directories, or gzip-wrapped tar bundles.")
        parser.add_argument("--out", required=True, help="Output root for timestamped run directories.")
        parser.add_argument("--case", default="trace-case", help="Case name stored in manifest.")
        parser.add_argument("--scenario", default="", help="Scenario description stored in manifest.")
        parser.add_argument("--code-ref", default="unknown", help="Source ref used for CodeGraph/source validation.")
        parser.add_argument("--force", action="store_true", help="Create a fresh run even when cache matches.")
        args = parser.parse_args(argv)
        run_dir = run_pipeline(args.inputs, args.out, case_name=args.case, scenario=args.scenario,
                               code_ref=args.code_ref, force=args.force)
        print(run_dir)
        return 0

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
