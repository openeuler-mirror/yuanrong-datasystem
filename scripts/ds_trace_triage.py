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
DEFAULT_MAX_TAR_MEMBERS = 10000
DEFAULT_MAX_TAR_TOTAL_BYTES = 1024 * 1024 * 1024
DEFAULT_MAX_TAR_MEMBER_BYTES = 512 * 1024 * 1024
DEFAULT_MAX_EVIDENCE_PER_TRACE = 200
PUBLISH_HOST_ENV = "DS_TRACE_TRIAGE_PUBLISH_HOST"
PUBLISH_ROOT_ENV = "DS_TRACE_TRIAGE_PUBLISH_ROOT"
PUBLISH_BASE_URL_ENV = "DS_TRACE_TRIAGE_PUBLISH_BASE_URL"
DEFAULT_SITE_PUBLIC_BASE_URL = "https://yche.me/perf"
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
URMA_THREAD_LOOP_GAP_RE = re.compile(
    r"\[URMA_ELAPSED_THREAD_SHED\].*?lastPollEndToThisPollStart\s+([\d.]+)us,\s*"
    r"lastPollStartToThisPollStart\s+([\d.]+)us",
    re.I,
)
URMA_PERF_RE = re.compile(r"\[URMA_PERF\].*?([A-Za-z][A-Za-z0-9_./-]*)\s*[:=]\s*([\d.]+)\s*(us|ms)?", re.I)
REQUEST_ID_RE = re.compile(r"(?:request id\s*:|requestId[:=])\s*([A-Za-z0-9_-]+)", re.I)
SRC_ADDR_RE = re.compile(r"src address:\s*([^\s,]+)", re.I)
DST_ADDR_RE = re.compile(r"(?:target|dst) address:\s*([^\s,]+)", re.I)
DATA_SIZE_RE = re.compile(r"dataSize:(\d+)|size\[(\d+)\]", re.I)
CPUID_RE = re.compile(r"cpuid:\s*(\d+)", re.I)
STATUS_RE = re.compile(r"status:\s*([^,]+)", re.I)
WAIT_OS_RE = re.compile(r"wait os sched.*?:\s*([\d.]+)ms", re.I)
INFLIGHT_WR_RE = re.compile(r"urma_inflight_wr_count:\s*(\d+)", re.I)
WAKE_SCHED_RE = re.compile(r"wakeSchedLatencyUs:\s*([\d.]+)", re.I)
SRC_CHIP_INFLIGHT_RE = re.compile(r"srcChipInflight:\s*(\{[^}]*\})", re.I)
SLEEP_TARGET_RE = re.compile(r"nanosleep\(([\d.]+)us\)", re.I)
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

    def __init__(self):
        self.failures = []

    def iter_lines(self, paths):
        for raw_path in paths:
            path = Path(raw_path)
            if path.is_dir():
                for root, _, files in os.walk(path):
                    for name in sorted(files):
                        yield from self.iter_file(Path(root) / name)
            else:
                yield from self.iter_file(path)

    def iter_file(self, path):
        try:
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
            with opener(path, "rt", encoding="utf-8", errors="replace") as f:
                for line_no, line in enumerate(f, 1):
                    yield str(path), path.name, line_no, line.rstrip("\n")
        except (OSError, UnicodeError, tarfile.TarError) as exc:
            self.failures.append({
                "path": str(path),
                "member": path.name,
                "error": type(exc).__name__,
                "message": str(exc),
            })
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
        wake_sched = _first_match(WAKE_SCHED_RE, line)
        if wake_sched:
            event["wake_sched_latency_us"] = float(wake_sched)
        src_chip_inflight = _first_match(SRC_CHIP_INFLIGHT_RE, line)
        if src_chip_inflight:
            event["src_chip_inflight"] = src_chip_inflight
        events.append(event)

    loop_gap = URMA_THREAD_LOOP_GAP_RE.search(line)
    if loop_gap:
        event = _ub_base_event("thread_sched", source, member, line_no, line, ts, worker)
        event["thread_sched_kind"] = "poll_loop_gap"
        event["last_poll_end_to_start_us"] = float(loop_gap.group(1))
        event["last_poll_start_to_start_us"] = float(loop_gap.group(2))
        event["cost_ms"] = event["last_poll_end_to_start_us"] / 1000.0
        events.append(event)

    for event_type, regex in (("poll_jfc", URMA_POLL_RE), ("notify", URMA_NOTIFY_RE), ("thread_sched", URMA_THREAD_RE)):
        match = regex.search(line)
        if match:
            event = _ub_base_event(event_type, source, member, line_no, line, ts, worker)
            event["cost_ms"] = _ms(match.group(1), match.group(2))
            if event_type == "thread_sched":
                sleep_target = _first_match(SLEEP_TARGET_RE, line)
                if sleep_target:
                    event["thread_sched_kind"] = "nanosleep_wake"
                    event["sleep_target_us"] = float(sleep_target)
                else:
                    event["thread_sched_kind"] = "generic"
            count = _int_match(re.compile(r"count:\s*(\d+)", re.I), line)
            if count is not None:
                event["count"] = count
            events.append(event)

    return events


def _classify(trace):
    access_for_deadline = trace.get("access_latency_ms_by_role", {}).get("client") or trace["access_latency_ms"]
    max_access = max(access_for_deadline or [0])
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


class TraceAccumulator:
    """Accumulate parsed log lines into trace-scoped facts and raw dimensions."""

    def __init__(self, paths):
        self.noise_cohort_mode = _detect_noise_cohort_mode(paths)
        self.traces = defaultdict(self._new_trace)
        self.all_ts = []
        self.worker_counts = Counter()
        self.flow_counts = Counter()
        self.access_latencies = []
        self.breakdown = {}
        self.rpc_slow = defaultdict(lambda: {"count": 0, "fields_us": defaultdict(list)})
        self.urma = defaultdict(list)
        self.urma_perf = defaultdict(list)
        self.custom_metrics = defaultdict(list)
        self.latency_summary = defaultdict(list)
        self.errors = Counter()
        self.ub_summary = {"transfer_path": Counter(), "edges": defaultdict(lambda: {"count": 0, "latencies": []})}
        self.surface_counts = Counter()

    @staticmethod
    def _new_trace():
        return {
            "lines": 0,
            "workers": Counter(),
            "timestamps": [],
            "flows": Counter(),
            "access_latency_ms": [],
            "access_latency_ms_by_role": defaultdict(list),
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
            "dropped_evidence": 0,
            "input_sources": Counter(),
            "source_stats": defaultdict(lambda: {
                "errors": Counter(),
                "workers": Counter(),
                "access_latency_ms": [],
                "line_count": 0,
            }),
        }

    def ingest(self, parsed, line):
        trace_id = parsed["trace_id"]
        trace = self.traces[trace_id]
        trace["lines"] += 1
        evidence = parsed["evidence"]
        source_label = _source_cohort_label(evidence["source"], evidence["member"], self.noise_cohort_mode)
        trace["input_sources"][source_label] += 1
        trace["source_stats"][source_label]["line_count"] += 1
        worker = parsed["worker"]
        trace["workers"][worker] += 1
        trace["source_stats"][source_label]["workers"][worker] += 1
        self.worker_counts[worker] += 1
        ts = parsed["timestamp"]
        if ts:
            trace["timestamps"].append(ts.isoformat())
            self.all_ts.append(ts)
        if len(trace["evidence"]) < DEFAULT_MAX_EVIDENCE_PER_TRACE:
            trace["evidence"].append(evidence)
        else:
            trace["dropped_evidence"] += 1
        self._ingest_ub_events(trace, parsed["ub_events"])
        self._ingest_access(trace, line, source_label)
        self._ingest_breakdown(trace, line)
        self._ingest_rpc_slow(trace, line)
        self._ingest_latency_summary(trace, line)
        self._ingest_urma_elapsed(trace, line)
        self._ingest_urma_perf(trace, line)
        self._ingest_custom_metrics(trace, parsed["custom_metrics_ms"])
        self._ingest_errors(trace, parsed["errors"], source_label)

    def _ingest_ub_events(self, trace, events):
        for event in events:
            trace["ub_events"].append(event)
            if event.get("transfer_path"):
                self.ub_summary["transfer_path"][event["transfer_path"]] += 1
            if event.get("src_addr") and event.get("target_addr") and event.get("event_type") == "total":
                edge = f"{event['src_addr']} -> {event['target_addr']}"
                self.ub_summary["edges"][edge]["count"] += 1
                if event.get("cost_ms") is not None:
                    self.ub_summary["edges"][edge]["latencies"].append(event["cost_ms"])

    def _ingest_access(self, trace, line, source_label=None):
        access = ACCESS_RE.search(line)
        if not access:
            return
        self.surface_counts["client_access"] += 1
        status, operation, duration_us, _size = access.groups()
        latency_ms = int(duration_us) / 1000.0
        trace["flows"][operation] += 1
        self.flow_counts[operation] += 1
        if status != "0":
            self.errors[f"status={status}"] += 1
            trace["errors"][f"status={status}"] += 1
        trace["access_latency_ms"].append(latency_ms)
        if source_label:
            trace["source_stats"][source_label]["access_latency_ms"].append(latency_ms)
        role = "client" if "CLIENT" in operation else "worker" if "POSIX" in operation else "unknown"
        trace["access_latency_ms_by_role"][role].append(latency_ms)
        self.access_latencies.append(latency_ms)

    def _ingest_breakdown(self, trace, line):
        block = BREAKDOWN_BLOCK_RE.search(line)
        if not block:
            return
        for key, value in BREAKDOWN_ITEM_RE.findall(block.group(1)):
            value_ms = float(value)
            name = " ".join(key.split())
            trace["breakdown_ms"][name] += value_ms
            _add_metric(self.breakdown, name, value_ms)

    def _ingest_rpc_slow(self, trace, line):
        rpc = RPC_SLOW_RE.search(line)
        if not rpc:
            return
        self.surface_counts["rpc_slow"] += 1
        method = rpc.group(1)
        trace["rpc_slow"][method] += 1
        self.rpc_slow[method]["count"] += 1
        for field, raw_value in RPC_SLOW_FIELD_RE.findall(line):
            val = int(raw_value)
            trace["rpc_slow_fields_us"][field].append(val)
            self.rpc_slow[method]["fields_us"][field].append(val)

    def _ingest_latency_summary(self, trace, line):
        summary = LATENCY_SUMMARY_RE.search(line)
        if not summary:
            return
        self.surface_counts["latency_summary"] += 1
        raw = "latencySummary:{" + summary.group(1) + "}"
        if len(trace["latency_summary_raw"]) < 8:
            trace["latency_summary_raw"].append(raw)
        for key, raw_value in SUMMARY_ITEM_RE.findall(summary.group(1)):
            val = int(raw_value)
            trace["latency_summary_us"][key] += val
            self.latency_summary[key].append(val)

    def _ingest_urma_elapsed(self, trace, line):
        loop_gap = URMA_THREAD_LOOP_GAP_RE.search(line)
        if loop_gap:
            self.surface_counts["urma_elapsed"] += 1
            val = float(loop_gap.group(1)) / 1000.0
            trace["urma_thread_sched_ms"].append(val)
            self.urma["thread_sched"].append(val)
        for name, regex in (
            ("total", URMA_TOTAL_RE),
            ("poll_jfc", URMA_POLL_RE),
            ("notify", URMA_NOTIFY_RE),
            ("thread_sched", URMA_THREAD_RE),
        ):
            um = regex.search(line)
            if um:
                self.surface_counts["urma_elapsed"] += 1
                val = _ms(um.group(1), um.group(2) if len(um.groups()) > 1 else "ms")
                trace[f"urma_{name}_ms"].append(val)
                self.urma[name].append(val)

    def _ingest_urma_perf(self, trace, line):
        perf = URMA_PERF_RE.search(line)
        if not perf:
            return
        key, raw_value, unit = perf.groups()
        val = float(raw_value)
        if (unit or "ms").lower() == "us":
            val /= 1000.0
        name = " ".join(key.split())
        trace["urma_perf"][name] += val
        self.urma_perf[name].append(val)

    def _ingest_custom_metrics(self, trace, metrics):
        for name, val in metrics.items():
            trace["custom_metrics_ms"][name] += val
            self.custom_metrics[name].append(val)

    def _ingest_errors(self, trace, patterns, source_label=None):
        for pattern in patterns:
            self.surface_counts["error"] += 1
            self.errors[pattern] += 1
            trace["errors"][pattern] += 1
            if source_label:
                trace["source_stats"][source_label]["errors"][pattern] += 1

    def finish(self):
        trace_rows, classifications, worker_summary = self._build_trace_rows()
        worker_edges = {
            edge: {"count": item["count"], "p99_ms": _percentiles(item["latencies"]).get("p99")}
            for edge, item in self.ub_summary["edges"].items()
        }
        return {
            "trace_rows": trace_rows,
            "classifications": classifications,
            "worker_summary": worker_summary,
            "worker_edges": worker_edges,
            "all_ts": self.all_ts,
            "worker_counts": self.worker_counts,
            "flow_counts": self.flow_counts,
            "access_latencies": self.access_latencies,
            "breakdown": self.breakdown,
            "rpc_slow": self.rpc_slow,
            "urma": self.urma,
            "urma_perf": self.urma_perf,
            "custom_metrics": self.custom_metrics,
            "latency_summary": self.latency_summary,
            "errors": self.errors,
            "ub_summary": self.ub_summary,
            "surface_counts": self.surface_counts,
        }

    def _build_trace_rows(self):
        trace_rows = {}
        classifications = Counter()
        worker_roles = defaultdict(set)
        worker_trace_ids = defaultdict(set)
        worker_slow_counts = Counter()
        worker_error_counts = Counter()
        for trace_id, trace in self.traces.items():
            trace["classification"] = _classify(trace)
            classifications[trace["classification"]] += 1
            triage_flags = []
            access_for_deadline = trace.get("access_latency_ms_by_role", {}).get("client") or trace["access_latency_ms"]
            if trace["errors"] and max(trace["urma_total_ms"] or [0]) > max(access_for_deadline or [0]):
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
                "access_latency_ms_by_role": {
                    role: _percentiles(values)
                    for role, values in sorted(trace["access_latency_ms_by_role"].items())
                },
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
                "source_stats": {
                    source: {
                        "errors": dict(stats["errors"]),
                        "workers": dict(stats["workers"]),
                        "access_latency_ms": _percentiles(stats["access_latency_ms"]),
                        "line_count": stats["line_count"],
                    }
                    for source, stats in sorted(trace["source_stats"].items())
                },
                "dropped_evidence": trace["dropped_evidence"],
                "triage_flags": triage_flags,
                "stage_breakdown": stage_breakdown,
                "evidence_coverage": _evidence_coverage(trace),
                "missing_evidence": missing_evidence,
                "evidence": trace["evidence"],
            }
        return trace_rows, classifications, self._build_worker_summary(
            worker_roles, worker_trace_ids, worker_slow_counts, worker_error_counts
        )

    def _build_worker_summary(self, worker_roles, worker_trace_ids, worker_slow_counts, worker_error_counts):
        worker_summary = {}
        for worker, item in self.worker_counts.most_common():
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
        return worker_summary


class TraceDimensionBuilder:
    """Convert accumulated trace facts into the stable report schema."""

    def build(self, snapshot, paths, code_ref="unknown", input_failures=None):
        trace_rows = snapshot["trace_rows"]
        surface_counts = snapshot["surface_counts"]
        coverage = self._build_coverage(surface_counts)
        cohorts = _build_cohorts(trace_rows)
        diagnosis = _build_diagnosis(
            errors=snapshot["errors"],
            classifications=snapshot["classifications"],
            access_latencies=snapshot["access_latencies"],
            coverage=coverage,
            cohorts=cohorts,
        )
        recommendations = _build_recommendations(
            classifications=snapshot["classifications"],
            coverage=coverage,
            cohorts=cohorts,
            ub_summary=snapshot["ub_summary"],
        )
        flow_stages = _build_flow_stages(coverage, snapshot["flow_counts"], snapshot["ub_summary"], trace_rows)
        return {
            "schema_version": 1,
            "code_ref": code_ref,
            "inputs": [str(p) for p in paths],
            "trace_count": len(trace_rows),
            "dimensions": {
                "time": self._build_time_range(snapshot["all_ts"]),
                "time_buckets": {"1000ms": _build_time_buckets(trace_rows, 1000),
                                 "10000ms": _build_time_buckets(trace_rows, 10000)},
                "workers": {k: {"line_count": v} for k, v in snapshot["worker_counts"].most_common()},
                "worker_summary": snapshot["worker_summary"],
                "ub_worker_summary": _build_ub_worker_summary(trace_rows),
                "ub_lifecycle_summary": _build_ub_lifecycle_summary(trace_rows),
                "cohorts": cohorts,
                "input_failures": input_failures or [],
                "worker_edges": snapshot["worker_edges"],
                "coverage": coverage,
                "diagnosis": diagnosis,
                "recommendations": recommendations,
                "source_appendix": _build_source_appendix(coverage),
                "flow_stages": flow_stages,
                "flow": dict(snapshot["flow_counts"]),
                "latency_ms": {"access": _percentiles(snapshot["access_latencies"])},
                "breakdown_ms": snapshot["breakdown"],
                "rpc_slow": self._build_rpc_slow(snapshot["rpc_slow"]),
                "urma_elapsed": {k: _percentiles(v) for k, v in sorted(snapshot["urma"].items())},
                "ub_summary": self._build_ub_summary(snapshot["ub_summary"]),
                "urma_perf_ms": {k: _percentiles(v) for k, v in sorted(snapshot["urma_perf"].items())},
                "custom_metrics_ms": {k: _percentiles(v) for k, v in sorted(snapshot["custom_metrics"].items())},
                "latency_summary_us": {k: _percentiles(v) for k, v in sorted(snapshot["latency_summary"].items())},
                "errors": dict(snapshot["errors"]),
                "classifications": dict(snapshot["classifications"]),
            },
            "traces": trace_rows,
        }

    @staticmethod
    def _build_time_range(all_ts):
        return {
            "first_ts": min(all_ts).isoformat() if all_ts else None,
            "last_ts": max(all_ts).isoformat() if all_ts else None,
        }

    @staticmethod
    def _build_coverage(surface_counts):
        return {
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

    @staticmethod
    def _build_rpc_slow(rpc_slow):
        return {
            k: {
                "count": v["count"],
                **{field: _percentiles(vals) for field, vals in sorted(v["fields_us"].items())},
            }
            for k, v in sorted(rpc_slow.items())
        }

    @staticmethod
    def _build_ub_summary(ub_summary):
        return {
            "transfer_path": dict(ub_summary["transfer_path"]),
            "edges": {
                edge: {"count": item["count"], "latency_ms": _percentiles(item["latencies"])}
                for edge, item in sorted(ub_summary["edges"].items())
            },
        }


class TraceAnalyzer:
    """Coordinate input reading, line parsing, trace accumulation, and dimensions."""

    def __init__(self, reader=None, parser=None, rules=None, accumulator_cls=None, dimension_builder=None):
        self.rules = rules or DEFAULT_RULES
        self.reader = reader or TraceInputReader()
        self.parser = parser or TraceParser(self.rules)
        self.accumulator_cls = accumulator_cls or TraceAccumulator
        self.dimension_builder = dimension_builder or TraceDimensionBuilder()
        self.accumulator = None

    def analyze(self, paths, code_ref="unknown", allow_partial_inputs=False):
        self.accumulator = self.accumulator_cls(paths)
        for source, member, line_no, line in self.reader.iter_lines(paths):
            parsed = self.parser.parse_line(source, member, line_no, line)
            if parsed:
                self.accumulator.ingest(parsed, line)
        if self.reader.failures and not allow_partial_inputs:
            failures = "; ".join(f"{item['path']}:{item['error']}" for item in self.reader.failures[:5])
            raise SystemExit(f"Failed to read trace input(s): {failures}. Use --allow-partial-inputs for best-effort analysis.")
        snapshot = self.accumulator.finish()
        try:
            return self.dimension_builder.build(
                snapshot, paths, code_ref=code_ref, input_failures=self.reader.failures
            )
        except TypeError:
            return self.dimension_builder.build(snapshot, paths, code_ref=code_ref)


def _analyze_inputs(paths, code_ref="unknown", reader=None, parser=None, rules=None, allow_partial_inputs=False):
    return TraceAnalyzer(reader=reader, parser=parser, rules=rules).analyze(
        paths, code_ref=code_ref, allow_partial_inputs=allow_partial_inputs
    )


def analyze_inputs(paths, code_ref="unknown", allow_partial_inputs=False):
    return TraceAnalyzer().analyze(paths, code_ref=code_ref, allow_partial_inputs=allow_partial_inputs)


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
            source_stats = trace.get("source_stats", {}).get(source, {})
            cohort = cohorts[source]
            cohort["trace_ids"].add(trace_id)
            source_errors = source_stats.get("errors", {})
            cohort["errors"].update(source_errors)
            if source_errors:
                cohort["classifications"][trace.get("classification", "deadline_or_error")] += 1
            elif source_stats.get("access_latency_ms", {}).get("p50", 0) >= 20:
                cohort["classifications"]["slow_access"] += 1
            else:
                cohort["classifications"][trace.get("classification", "unknown")] += 1
            if source_stats.get("access_latency_ms", {}).get("p50") is not None:
                cohort["access_latencies"].append(source_stats["access_latency_ms"]["p50"])
            cohort["workers"].update(source_stats.get("workers", {}))
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
            "scope": "通用",
            "log_surface": "access log",
            "flow_stage": "client -> entry worker",
            "source_hint": "ObjectClientImpl / ClientWorkerRemoteApi / Worker OC access path",
            "validation": "Use CodeGraph on pinned main/master ref, then direct source reads for client timeout, status, and WorkerRpc propagation.",
            "report_reading": "Defines user-visible latency/status; use it as symptom line, not as standalone worker-side root cause.",
        },
        {
            "scope": "写入",
            "log_surface": "latencySummary",
            "flow_stage": "client -> entry worker createbuffer / publish",
            "source_hint": "client summary emitters around Set/Create/Publish and buffer preparation",
            "validation": "Use CodeGraph to map each summary key to current write path; preserve raw latencySummary text in evidence.",
            "report_reading": "Explains write-side stage contribution even when no standalone slow log crosses threshold.",
        },
        {
            "scope": "读取",
            "log_surface": "GetObjMetaInfo / QueryMeta",
            "flow_stage": "entry worker -> meta worker",
            "source_hint": "ClientWorkerRemoteApi::GetObjMetaInfo / meta service query path",
            "validation": "Use CodeGraph and direct source reads to verify timeout budget and meta RPC branch before attributing QueryMeta.",
            "report_reading": "Only call meta path slow when logs expose QueryMeta/GetObjMetaInfo cost; absence is an observation gap.",
        },
        {
            "scope": "通用",
            "log_surface": "RPC slow",
            "flow_stage": "RPC framework client/server/network split",
            "source_hint": "brpc_perf_trace.h / rpc framework slow log emitters",
            "validation": "Check method name and fields such as server_exec_us and network_residual_us against current transport code.",
            "report_reading": "Separates server execution, queueing, framework, and residual/network windows.",
        },
        {
            "scope": "读取",
            "log_surface": "RemotePull / BatchGetObjectRemote",
            "flow_stage": "entry worker -> data worker",
            "source_hint": "WorkerRemoteWorkerOCApi / WorkerWorkerOCServiceImpl::BatchGetObjectRemote",
            "validation": "Use CodeGraph to verify current remote get branch, fallback, and aggregation behavior.",
            "report_reading": "Explains worker-side completion after client deadline; compare with client access window.",
        },
        {
            "scope": "读取",
            "log_surface": "URMA_ELAPSED_TOTAL",
            "flow_stage": "data worker UB write completion",
            "source_hint": "UrmaManager::WaitToFinish / LogUrmaWaitToFinishElapsed",
            "validation": "Use CodeGraph plus source reads; compare total with wait_for, wakeSchedLatencyUs, srcChipInflight, request id, src/target, dataSize, cpuid, and inflight.",
            "report_reading": "Treat as post/write completion wait window; use wait/wake fields to split OS scheduling from completion cost.",
        },
        {
            "scope": "读取",
            "log_surface": "URMA_ELAPSED_POLL_JFC / NOTIFY / THREAD_SHED",
            "flow_stage": "data worker UB poll and wake scheduling",
            "source_hint": "UrmaManager::PollJfcWait / ds_urma_poll_jfc / ds_urma_wait_jfc / nanosleep",
            "validation": "Use CodeGraph plus source reads; split poll_jfc cost, notify wakeup, poll-loop gap, and nanosleep(1us) wake cost.",
            "report_reading": "When total is high, these fields indicate whether the delay sits in polling, notification, or poll-thread scheduling.",
        },
        {
            "scope": "写入",
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
            "scope": "通用",
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


def _ub_role(event_type):
    if event_type in ("transfer_path", "remote_get_start"):
        return "ub_entry"
    if event_type in ("total", "poll_jfc", "notify", "thread_sched"):
        return "ub_exit"
    return "unknown"


def _build_ub_worker_summary(trace_rows, bucket_ms=1000):
    workers = defaultdict(lambda: {
        "entry_events": 0,
        "exit_events": 0,
        "trace_ids": set(),
        "entry_trace_ids": set(),
        "exit_trace_ids": set(),
        "latencies": [],
        "edges": Counter(),
        "first_ts": None,
        "last_ts": None,
    })
    buckets = defaultdict(lambda: {
        "bucket_start": None,
        "entry_events": 0,
        "exit_events": 0,
        "latencies": [],
        "entry_workers": Counter(),
        "exit_workers": Counter(),
    })
    for trace_id, trace in trace_rows.items():
        for event in trace.get("ub_events", []):
            worker = event.get("worker") or "unknown"
            role = _ub_role(event.get("event_type"))
            if role == "unknown":
                continue
            item = workers[worker]
            item["trace_ids"].add(trace_id)
            if role == "ub_entry":
                item["entry_events"] += 1
                item["entry_trace_ids"].add(trace_id)
            else:
                item["exit_events"] += 1
                item["exit_trace_ids"].add(trace_id)
            if event.get("src_addr") and event.get("target_addr"):
                item["edges"][f"{event['src_addr']} -> {event['target_addr']}"] += 1
            if event.get("cost_ms") is not None:
                item["latencies"].append(event["cost_ms"])
            ts = event.get("timestamp")
            if ts:
                item["first_ts"] = min(filter(None, [item["first_ts"], ts])) if item["first_ts"] else ts
                item["last_ts"] = max(filter(None, [item["last_ts"], ts])) if item["last_ts"] else ts
                bucket_start = ts[:19]
                bucket = buckets[bucket_start]
                bucket["bucket_start"] = bucket_start
                if role == "ub_entry":
                    bucket["entry_events"] += 1
                    bucket["entry_workers"][worker] += 1
                else:
                    bucket["exit_events"] += 1
                    bucket["exit_workers"][worker] += 1
                if event.get("cost_ms") is not None:
                    bucket["latencies"].append(event["cost_ms"])
    worker_rows = {}
    for worker, item in workers.items():
        role = "ub_entry_and_exit" if item["entry_events"] and item["exit_events"] else (
            "ub_entry" if item["entry_events"] else "ub_exit")
        worker_rows[worker] = {
            "role": role,
            "entry_events": item["entry_events"],
            "exit_events": item["exit_events"],
            "trace_count": len(item["trace_ids"]),
            "entry_trace_count": len(item["entry_trace_ids"]),
            "exit_trace_count": len(item["exit_trace_ids"]),
            "latency_ms": _percentiles(item["latencies"]),
            "top_edges": [edge for edge, _ in item["edges"].most_common(5)],
            "first_ts": item["first_ts"],
            "last_ts": item["last_ts"],
        }
    time_rows = []
    for _, item in sorted(buckets.items()):
        time_rows.append({
            "bucket_start": item["bucket_start"],
            "bucket_ms": bucket_ms,
            "entry_events": item["entry_events"],
            "exit_events": item["exit_events"],
            "latency_ms": _percentiles(item["latencies"]),
            "top_entry_workers": [worker for worker, _ in item["entry_workers"].most_common(5)],
            "top_exit_workers": [worker for worker, _ in item["exit_workers"].most_common(5)],
        })
    return {
        "workers": dict(sorted(worker_rows.items(), key=lambda kv: (
            -(kv[1]["latency_ms"].get("max") or 0),
            -(kv[1]["entry_events"] + kv[1]["exit_events"]),
            kv[0],
        ))),
        "time_buckets": time_rows,
    }


def _add_lifecycle_metric(metrics, name, value):
    if value is None:
        return
    try:
        metrics[name].append(float(value))
    except (TypeError, ValueError):
        return


def _parse_chip_inflight(raw):
    chips = {}
    if not raw:
        return chips
    for chip, value in re.findall(r"(\d+)\s*:\s*(\d+)", raw):
        chips[chip] = int(value)
    return chips


def _build_ub_lifecycle_summary(trace_rows):
    metrics = defaultdict(list)
    chip_inflight = defaultdict(list)
    trace_remote_get_wr_counts = defaultdict(list)
    requests = {}

    def request_row(trace_id, event):
        request_id = event.get("request_id")
        if request_id:
            key = "|".join(str(part or "") for part in (trace_id, request_id))
        else:
            key = "|".join(str(part or "") for part in (
                trace_id, event.get("worker"), event.get("src_addr"), event.get("target_addr"), event.get("timestamp")
            ))
        row = requests.setdefault(key, {
            "trace_id": trace_id,
            "request_id": request_id or "",
            "first_ts": event.get("timestamp") or "",
            "last_ts": event.get("timestamp") or "",
            "worker": event.get("worker") or "unknown",
            "src_addr": event.get("src_addr") or "",
            "target_addr": event.get("target_addr") or "",
            "data_size": event.get("data_size"),
            "cpuid": event.get("cpuid"),
            "status": event.get("status") or "",
            "src_chip_inflight": event.get("src_chip_inflight") or "",
            "urma_inflight_wr_count": event.get("urma_inflight_wr_count"),
            "remote_get_wr_count": event.get("inflight_remote_get"),
            "total_ms": None,
            "wait_os_sched_ms": None,
            "wake_sched_latency_ms": None,
            "poll_jfc_ms": None,
            "notify_ms": None,
            "thread_sched_ms": None,
            "poll_loop_gap_ms": None,
            "nanosleep_wake_ms": None,
        })
        if event.get("timestamp"):
            row["first_ts"] = min(filter(None, [row["first_ts"], event["timestamp"]])) if row["first_ts"] else event["timestamp"]
            row["last_ts"] = max(filter(None, [row["last_ts"], event["timestamp"]])) if row["last_ts"] else event["timestamp"]
        for field in ("worker", "src_addr", "target_addr", "status", "src_chip_inflight"):
            if event.get(field):
                row[field] = event[field]
        for field in ("data_size", "cpuid", "urma_inflight_wr_count", "inflight_remote_get"):
            if event.get(field) is not None:
                row["remote_get_wr_count" if field == "inflight_remote_get" else field] = event[field]
        return row

    def update_max(row, field, value):
        if value is None:
            return
        value = float(value)
        row[field] = value if row.get(field) is None else max(row[field], value)

    for trace_id, trace in trace_rows.items():
        for event in trace.get("ub_events", []):
            event_type = event.get("event_type")
            if event.get("inflight_remote_get") is not None:
                _add_lifecycle_metric(metrics, "remote_get_wr_count", event.get("inflight_remote_get"))
                trace_remote_get_wr_counts[trace_id].append(event.get("inflight_remote_get"))
                update_max(request_row(trace_id, event), "remote_get_wr_count", event.get("inflight_remote_get"))
            if event_type == "total":
                _add_lifecycle_metric(metrics, "total_ms", event.get("cost_ms"))
                _add_lifecycle_metric(metrics, "wait_os_sched_ms", event.get("wait_os_sched_ms"))
                _add_lifecycle_metric(metrics, "urma_inflight_wr_count", event.get("urma_inflight_wr_count"))
                for chip, value in _parse_chip_inflight(event.get("src_chip_inflight")).items():
                    chip_inflight[chip].append(value)
                if event.get("wake_sched_latency_us") is not None:
                    _add_lifecycle_metric(metrics, "wake_sched_latency_ms", event["wake_sched_latency_us"] / 1000.0)
                row = request_row(trace_id, event)
                update_max(row, "total_ms", event.get("cost_ms"))
                update_max(row, "wait_os_sched_ms", event.get("wait_os_sched_ms"))
                update_max(row, "urma_inflight_wr_count", event.get("urma_inflight_wr_count"))
                if event.get("wake_sched_latency_us") is not None:
                    update_max(row, "wake_sched_latency_ms", event["wake_sched_latency_us"] / 1000.0)
                continue
            if event_type == "poll_jfc":
                _add_lifecycle_metric(metrics, "poll_jfc_ms", event.get("cost_ms"))
                update_max(request_row(trace_id, event), "poll_jfc_ms", event.get("cost_ms"))
                continue
            if event_type == "notify":
                _add_lifecycle_metric(metrics, "notify_ms", event.get("cost_ms"))
                update_max(request_row(trace_id, event), "notify_ms", event.get("cost_ms"))
                continue
            if event_type == "thread_sched":
                kind = event.get("thread_sched_kind")
                _add_lifecycle_metric(metrics, "thread_sched_ms", event.get("cost_ms"))
                row = request_row(trace_id, event)
                update_max(row, "thread_sched_ms", event.get("cost_ms"))
                if kind == "poll_loop_gap":
                    gap_ms = event.get("last_poll_end_to_start_us", 0) / 1000.0
                    _add_lifecycle_metric(metrics, "poll_loop_gap_ms", gap_ms)
                    update_max(row, "poll_loop_gap_ms", gap_ms)
                elif kind == "nanosleep_wake":
                    _add_lifecycle_metric(metrics, "nanosleep_wake_ms", event.get("cost_ms"))
                    update_max(row, "nanosleep_wake_ms", event.get("cost_ms"))

    request_rows = list(requests.values())
    for row in request_rows:
        if row.get("remote_get_wr_count") is None and trace_remote_get_wr_counts.get(row["trace_id"]):
            row["remote_get_wr_count"] = max(trace_remote_get_wr_counts[row["trace_id"]])
        row["score_ms"] = max(float(row.get(field) or 0) for field in (
            "total_ms", "wait_os_sched_ms", "poll_loop_gap_ms", "nanosleep_wake_ms", "poll_jfc_ms", "notify_ms"
        ))
        for field in (
            "total_ms", "wait_os_sched_ms", "wake_sched_latency_ms", "poll_jfc_ms", "notify_ms",
            "thread_sched_ms", "poll_loop_gap_ms", "nanosleep_wake_ms", "remote_get_wr_count",
            "urma_inflight_wr_count", "score_ms",
        ):
            if row.get(field) is not None:
                row[field] = round(float(row[field]), 3)
    request_rows.sort(key=lambda item: (-item["score_ms"], item["trace_id"], item["request_id"]))
    return {
        "metrics": {name: _percentiles(values) for name, values in sorted(metrics.items())},
        "chip_inflight": {chip: _percentiles(values) for chip, values in sorted(chip_inflight.items())},
        "requests": request_rows[:80],
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
    if p.is_dir():
        total_size = 0
        for fp in sorted(item for item in p.rglob("*") if item.is_file()):
            relative = fp.relative_to(p).as_posix()
            members.append(relative)
            total_size += fp.stat().st_size
            h.update(relative.encode("utf-8"))
            h.update(b"\0")
            with open(fp, "rb") as f:
                for chunk in iter(lambda: f.read(1024 * 1024), b""):
                    h.update(chunk)
            h.update(b"\0")
        return {"path": str(p), "size": total_size, "sha256": h.hexdigest(), "members": members}
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


def _safe_member_path(member_name, extract_root):
    pure = Path(member_name)
    if pure.is_absolute() or any(part in ("", ".", "..") for part in pure.parts):
        raise ValueError(f"Unsafe tar member path: {member_name}")
    target = (Path(extract_root) / pure).resolve()
    target.relative_to(Path(extract_root).resolve())
    return target


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
                member_count = 0
                total_bytes = 0
                with tarfile.open(p, "r:*") as tar:
                    for member in tar.getmembers():
                        if not member.isfile():
                            continue
                        member_count += 1
                        if member_count > DEFAULT_MAX_TAR_MEMBERS:
                            raise ValueError(f"Tar member count exceeds {DEFAULT_MAX_TAR_MEMBERS}: {p}")
                        if member.size > DEFAULT_MAX_TAR_MEMBER_BYTES:
                            raise ValueError(f"Tar member too large: {member.name}")
                        total_bytes += member.size
                        if total_bytes > DEFAULT_MAX_TAR_TOTAL_BYTES:
                            raise ValueError(f"Tar extracted bytes exceed {DEFAULT_MAX_TAR_TOTAL_BYTES}: {p}")
                        stream = tar.extractfile(member)
                        if stream is None:
                            continue
                        target = _safe_member_path(member.name, extract_root)
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


def _site_publish_config(require_target=False):
    host = os.environ.get(PUBLISH_HOST_ENV, "").strip()
    root = os.environ.get(PUBLISH_ROOT_ENV, "").strip().rstrip("/")
    base_url = os.environ.get(PUBLISH_BASE_URL_ENV, DEFAULT_SITE_PUBLIC_BASE_URL).strip().rstrip("/")
    if require_target and (not host or not root):
        raise SystemExit(
            f"Set {PUBLISH_HOST_ENV} and {PUBLISH_ROOT_ENV} before real publish; "
            f"{PUBLISH_BASE_URL_ENV} is optional."
        )
    return {
        "host": host or "<publish-host>",
        "root": root or "<publish-root>/perf",
        "base_url": base_url,
        "is_configured": bool(host and root),
    }


def _write_site_publish_doc(run_dir, manifest):
    run_dir = Path(run_dir)
    filename = f"{run_dir.name}.html"
    publish_config = _site_publish_config()
    remote_path = f"{publish_config['root']}/{filename}"
    url = f"{publish_config['base_url']}/{filename}"
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
        f"- target_host: `{publish_config['host']}`",
        f"- target_path: `{remote_path}`",
        f"- url: `{url}`",
        f"- configured: `{publish_config['is_configured']}`",
        "",
        "## Commands",
        "",
        "```bash",
        f"export {PUBLISH_HOST_ENV}=<publish-host>",
        f"export {PUBLISH_ROOT_ENV}=<publish-root>/perf",
        f"export {PUBLISH_BASE_URL_ENV}={publish_config['base_url']}",
        f"scp report.site.html ${{{PUBLISH_HOST_ENV}}}:${{{PUBLISH_ROOT_ENV}}}/{filename}",
        f"curl -fsSI {url}",
        "```",
        "",
        "## Index Registration",
        "",
        "- Real publish is not complete until the site catalog is updated.",
        "- Add or update one `var P` entry in `<publish-root>/index.html` for "
        f"`perf/{filename}`; keep the edit minimal and preserve existing entries.",
        "- Run an `index.html` JavaScript syntax check after editing, then verify the "
        "new report URL and catalog entry over HTTPS.",
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
:root{--bg:#f6f8fb;--card:#fff;--text:#172033;--muted:#5f6b7a;--border:#dfe5ee;--blue:#2563eb;--orange:#ea580c;--red:#dc2626;--green:#059669;--purple:#7c3aed;--amber:#ca8a04;--report-font-size:13px}
*{box-sizing:border-box}body{margin:0;overflow-x:hidden;background:var(--bg);color:var(--text);font-family:'Microsoft YaHei',-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif}
.layout{display:flex;align-items:flex-start;min-height:100vh}aside{position:sticky;left:0;top:0;flex:0 0 245px;width:245px;height:100vh;background:#fff;border-right:1px solid var(--border);padding:18px 14px;overflow:auto}
aside h2{font-size:16px;margin:0 0 12px}nav a{display:block;color:#324055;text-decoration:none;padding:8px 10px;border-radius:6px;font-size:13px;margin:2px 0}
nav a.active,nav a:hover{background:#eaf2ff;color:#1d4ed8}nav a.sub{padding-left:22px;color:#64748b;font-size:12px}
main{flex:1;min-width:0;width:auto;padding:22px 28px 50px}section{margin-bottom:20px}
h1{font-size:26px;margin:0 0 8px}h2{font-size:21px;margin:8px 0 12px}h3{font-size:15px;text-align:center;margin:10px 0}
.subtitle,.note,.insight{color:var(--muted);line-height:1.65}.section-summary{background:#f8fafc;border-left:4px solid var(--blue);color:#334155;font-size:13px;line-height:1.65}.section-summary b{color:#172033}.summary-points{margin:6px 0 0 18px;padding:0}.summary-points li{margin:3px 0}.summary-key{font-weight:700;color:#1d4ed8}.summary-hot{font-weight:700;color:#991b1b;background:#fee2e2;border-radius:4px;padding:1px 4px}.summary-warn{font-weight:700;color:#9a3412;background:#ffedd5;border-radius:4px;padding:1px 4px}.chapter-guide{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:8px;margin:0;padding:0;list-style:none}.chapter-guide li{border:1px solid var(--border);border-radius:8px;background:#f8fafc;padding:9px 10px;line-height:1.55;font-size:13px}.chapter-guide a{color:#1d4ed8;text-decoration:none;font-weight:700}.cards{display:grid;grid-template-columns:repeat(4,1fr);gap:10px}
.card{background:#fff;border:1px solid var(--border);border-radius:8px;padding:12px}.panel{min-width:0;background:#fff;border:1px solid var(--border);border-radius:8px;padding:16px;margin:12px 0;box-shadow:0 2px 10px rgba(20,35,60,.04)}
.k{color:#64748b;font-size:12px}.v,.metric{font-size:24px;font-weight:700;margin:4px 0}.n,.muted,.small{color:#64748b;font-size:12px}.bad{color:var(--red)!important;font-weight:700}.warn{color:#b45309!important;font-weight:700}.ok{color:var(--green)!important;font-weight:700}
tr.hotrow td{background:#fff1f2}tr.warnrow td{background:#fffbeb}.cell-hot,.cell-warn,.cell-ok{display:inline-block;border-radius:4px;padding:1px 5px;font-weight:700}.cell-hot{background:#fee2e2;color:#991b1b}.cell-warn{background:#ffedd5;color:#9a3412}.cell-ok{background:#dcfce7;color:#166534}
tr.summaryrow td{background:#f8fafc}
.log-tag{display:inline-block;border-radius:4px;padding:0 4px;margin:0 1px;font-weight:700}.log-error{background:#fee2e2;color:#991b1b}.log-deadline{background:#ffedd5;color:#9a3412}.log-urma{background:#ede9fe;color:#5b21b6}.log-rpc{background:#dbeafe;color:#1e40af}.log-latency{background:#dcfce7;color:#166534}.log-slow{background:#fef3c7;color:#92400e}.log-field{background:#e2e8f0;color:#334155}
.log-legend,.stage-legend{display:flex;flex-wrap:wrap;gap:6px;margin:8px 0}.log-legend span,.stage-legend span{font-size:12px}
.stage-pill{display:inline-flex;align-items:center;gap:5px;border:1px solid var(--border);border-radius:999px;padding:2px 8px;background:#fff;color:#475569}.stage-dot{width:10px;height:10px;border-radius:2px;display:inline-block}
.compare2{display:grid;grid-template-columns:1fr 1fr;gap:12px}.chart-grid{display:grid;grid-template-columns:1fr 1fr;gap:12px}.full-row{grid-column:1/-1}.flow-section{display:grid;grid-template-columns:1fr;gap:12px;margin-top:12px}.chart{height:360px;width:100%}.caption{text-align:center;color:#64748b;font-size:var(--report-font-size);margin-top:6px}
table{width:100%;border-collapse:collapse;table-layout:fixed;background:#fff}th,td{border-bottom:1px solid var(--border);padding:8px 9px;text-align:left;vertical-align:top;font-size:var(--report-font-size);word-break:break-word}
th{background:#f8fafc;color:#475569}.sortable-th{cursor:pointer;user-select:none}.sortable-th:hover{background:#eaf2ff}.sort-mark{color:#2563eb;font-size:11px;margin-left:4px}.num{text-align:right;font-variant-numeric:tabular-nums}.trace-id{font-family:'Cascadia Code',Consolas,monospace;font-size:12px}
.table-scroll{width:100%;max-width:100%;overflow-x:auto}.adaptive-table{table-layout:fixed}.nowrap-table{min-width:720px;table-layout:auto}.metadata-table{table-layout:auto}#run-metadata-table th:first-child,#run-metadata-table td:first-child{width:1%;white-space:nowrap;min-width:120px}#run-metadata-table th:last-child,#run-metadata-table td:last-child{width:auto}#ub-lifecycle-table th,#ub-lifecycle-table td{white-space:nowrap}#ub-worker-role-table th:last-child,#ub-worker-role-table td:last-child{width:30%}#ub-request-table th,#ub-request-table td{padding:7px 6px}#ub-request-table th:nth-child(10),#ub-request-table td:nth-child(10),#ub-request-table th:nth-child(11),#ub-request-table td:nth-child(11),#ub-request-table th:nth-child(12),#ub-request-table td:nth-child(12){text-align:right}
.controls{display:flex;gap:8px;flex-wrap:wrap;margin:8px 0 12px;align-items:center}input,select,button{border:1px solid var(--border);background:#fff;border-radius:6px;padding:7px 9px;font-size:13px}
button{cursor:pointer}button.primary{background:var(--blue);color:#fff;border-color:var(--blue)}button:disabled{opacity:.45;cursor:not-allowed}.pager{background:#fff;border:1px solid var(--border);border-radius:8px;padding:10px}.mini-pager{display:flex;justify-content:center;gap:8px;align-items:center;margin-top:8px;color:#64748b;font-size:12px}
.selected-row{background:#fff7e6}.logbox,pre{white-space:pre-wrap;background:#0f172a;color:#dbeafe;padding:12px;border-radius:8px;max-height:520px;overflow:auto;font-family:'Cascadia Code',Consolas,monospace;font-size:12px;line-height:1.5}
.trace-log-groups{display:flex;flex-direction:column;gap:10px;margin-top:10px}.trace-log-block{border:1px solid var(--border);border-radius:8px;overflow:hidden;background:#fff}.trace-log-block h4{margin:0;padding:8px 12px;background:#f8fafc;color:#0f172a;font-size:13px;display:flex;justify-content:space-between;gap:12px;align-items:flex-start}.trace-log-block pre{border-radius:0;margin:0;max-height:none}.trace-log-count{color:#64748b;font-weight:500;white-space:nowrap}.trace-log-heading{display:flex;flex-direction:column;gap:4px;min-width:0}.trace-log-summary{display:flex;flex-wrap:wrap;gap:4px;font-size:12px;color:#64748b;font-weight:500}.trace-log-focus{border:1px solid #e2e8f0;border-radius:999px;padding:1px 7px;background:#fff;color:#475569}.trace-log-focus.hot{border-color:#fecaca;background:#fef2f2;color:#991b1b}.trace-log-focus.warn{border-color:#fed7aa;background:#fff7ed;color:#9a3412}
code{font-family:'Cascadia Code',Consolas,monospace;font-size:12px}
@media(max-width:900px){.layout{display:block}aside{position:relative;width:auto;height:auto}main{margin-left:0;width:100%;padding:16px}.chart-grid,.compare2,.cards,.chapter-guide{grid-template-columns:1fr}}
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
      <a class="sub" href="#overall-guide">表 1-1 整体导读</a>
      <a href="#s2">2. 根因分布</a>
      <a class="sub" href="#cohort-chart">图 2-1 输入包 / Cohort</a>
      <a class="sub" href="#classification-chart">图 2-2 分类分布</a>
      <a class="sub" href="#error-chart">图 2-3 错误分布</a>
      <a href="#s3">3. 时延 Breakdown</a>
      <a class="sub" href="#read-latency-chart">图 3-1 读取时延</a>
      <a class="sub" href="#read-flow-chart">图 3-2 读取 Flow</a>
      <a class="sub" href="#read-time-breakdown-chart">图 3-3 读取时间桶</a>
      <a class="sub" href="#write-latency-chart">图 3-4 写入时延</a>
      <a class="sub" href="#write-flow-chart">图 3-5 写入 Flow</a>
      <a class="sub" href="#write-time-breakdown-chart">图 3-6 写入时间桶</a>
      <a href="#s4">4. Worker / 流程</a>
      <a class="sub" href="#read-flow-stage-chart">图 4-1 读取流程</a>
      <a class="sub" href="#read-worker-chart">图 4-2 读取 Worker</a>
      <a class="sub" href="#write-flow-stage-chart">图 4-3 写入流程</a>
      <a class="sub" href="#write-worker-chart">图 4-4 写入 Worker</a>
      <a href="#s5">5. UB / URMA</a>
      <a class="sub" href="#ub-lifecycle-chart">图 5-1 UB 生命周期</a>
      <a class="sub" href="#ub-wr-count-chart">图 5-2 WR / Inflight</a>
      <a class="sub" href="#ub-worker-role-chart">图 5-3 入口/出口 Worker</a>
      <a class="sub" href="#ub-worker-time-chart">图 5-4 UB 时间桶</a>
      <a class="sub" href="#read-ub-edge-chart">图 5-5 读取 UB Edge</a>
      <a class="sub" href="#write-ub-edge-chart">图 5-6 写入 UB Edge</a>
      <a href="#s6">6. Trace 查看</a>
      <a class="sub" href="#top-trace-table">表 6-1 Top Trace</a>
      <a class="sub" href="#selected-trace-chart">图 6-1 选中 Trace</a>
      <a class="sub" href="#selected-trace-log">日志框 6-3 全量日志</a>
      <a href="#s7">7. 建议与口径</a>
      <a class="sub" href="#recommendation-table">表 7-1 建议</a>
      <a class="sub" href="#source-appendix-common-table">表 7-2 代码映射</a>
      <a href="#s8">8. 原始 JSON</a>
      <a class="sub" href="#trace-data">表 8-1 原始 Trace JSON</a>
    </nav>
    </aside>
    <main>
      <section id="s1">
        <h1>__TITLE__</h1>
        <p class="subtitle" id="report-subtitle"></p>
        <div id="summary" class="cards"></div>
        <div class="panel"><h3>运行与输入来源</h3><table id="run-metadata-table" class="metadata-table"></table></div>
        <div class="panel insight" id="report-insight"></div>
        <div class="panel" id="overall-guide"><h3>表 1-1 整体导读</h3><ul class="chapter-guide" id="chapter-guide-list"></ul></div>
        <div class="panel"><h3>客户化诊断口径</h3><ul id="diagnosis-list"></ul><div class="controls"><button class="primary" id="download-report-summary">下载分析摘要</button></div></div>
        <div class="panel"><h3>日志覆盖与缺失观测面</h3><table id="coverage-table"></table></div>
      </section>
      <section id="s2">
        <h2>2. 错误根因与分类分布</h2>
        <div class="panel section-summary" id="section-summary-s2"></div>
        <div class="panel"><div id="cohort-chart" class="chart"></div><div class="caption">图 2-1 输入包 / Cohort 对比：多个日志包独立统计，再对比分类和错误分布。</div></div>
        <div class="panel"><h3>表 2-1 输入包 / Cohort 对比</h3><table id="cohort-table"></table></div>
        <div class="chart-grid">
          <div class="panel"><div id="classification-chart" class="chart"></div><div class="caption">图 2-2 分类分布：按根因分类聚合 trace。</div></div>
          <div class="panel"><div id="error-chart" class="chart"></div><div class="caption">图 2-3 错误文本 / 状态分布：按错误文本和状态码聚合。</div></div>
        </div>
        <div class="compare2">
          <div class="panel"><h3>表 2-2 分类聚合</h3><table id="classification-table"></table></div>
          <div class="panel"><h3>表 2-3 Error Breakdown</h3><table id="error-table"></table></div>
        </div>
      </section>
      <section id="s3">
        <h2>3. 时延 Breakdown</h2>
        <div class="panel section-summary" id="section-summary-s3"></div>
        <div class="panel insight">读写分开看尾部：读关注 GET/QueryMeta/RemoteGet/UB，写关注 SET/Create/Publish/memory copy。</div>
        <div class="flow-section">
          <div class="panel"><div id="read-latency-chart" class="chart"></div><div class="caption">图 3-1 读取 Top 时延。</div></div>
          <div class="panel"><div id="read-flow-chart" class="chart"></div><div class="caption">图 3-2 读取 Flow 分布：读取接口类型分布。</div></div>
          <div class="panel"><div id="read-time-breakdown-chart" class="chart"></div><div class="caption">图 3-3 读取时间桶 Breakdown：柱为读 RPC/UB 子阶段 p99，线为读 trace access p99。</div></div>
        </div>
        <div class="flow-section">
          <div class="panel"><div id="write-latency-chart" class="chart"></div><div class="caption">图 3-4 写入 Top 时延。</div></div>
          <div class="panel"><div id="write-flow-chart" class="chart"></div><div class="caption">图 3-5 写入 Flow 分布：写入接口类型分布。</div></div>
          <div class="panel"><div id="write-time-breakdown-chart" class="chart"></div><div class="caption">图 3-6 写入时间桶 Breakdown：柱为写 RPC/本地阶段 p99，线为写 trace access p99。</div></div>
        </div>
        <div class="compare2">
          <div class="panel"><h3>表 3-1 读取时延指标</h3><table id="read-latency-table"></table></div>
          <div class="panel"><h3>表 3-2 写入时延指标</h3><table id="write-latency-table"></table></div>
          <div class="panel"><h3>表 3-3 读取 Flow</h3><table id="read-flow-table"></table></div>
          <div class="panel"><h3>表 3-4 写入 Flow</h3><table id="write-flow-table"></table></div>
        </div>
      </section>
      <section id="s4">
        <h2>4. Worker / 流程分布</h2>
        <div class="panel section-summary" id="section-summary-s4"></div>
        <div id="flow-stage-chart" class="panel insight">读写链路分开看：读关注 Entry→Data，写关注 CreateBuffer/Publish/Meta。</div>
        <div id="read-flow-section" class="flow-section">
          <div class="panel"><div id="read-flow-stage-chart" class="chart"></div><div class="caption">图 4-1 读取流程证据块：看 Entry→Data RPC 与 DataWorker UB/URMA。</div></div>
          <div class="panel"><h3>表 4-1 读取流程阶段证据</h3><table id="read-flow-stage-table"></table></div>
          <div class="panel"><div class="controls"><label>Worker 筛选 <select id="read-worker-filter"><option value="">全部 Worker</option></select></label></div><div id="read-worker-chart" class="chart"></div><div class="caption">图 4-2 读取 Worker 分布：读取链路按 worker 聚合。</div></div>
          <div class="panel"><h3>表 4-2 读取 Worker Breakdown</h3><div class="controls"><label>Worker 筛选 <select id="read-worker-table-filter"><option value="">全部 Worker</option></select></label></div><table id="read-worker-table"></table><div id="read-worker-table-pager" class="mini-pager"></div></div>
        </div>
        <div id="write-flow-section" class="flow-section">
          <div class="panel"><div id="write-flow-stage-chart" class="chart"></div><div class="caption">图 4-3 写入流程证据块：区分 createbuffer、client publish、entry/meta publish。</div></div>
          <div class="panel"><h3>表 4-3 写入流程阶段证据</h3><table id="write-flow-stage-table"></table></div>
          <div class="panel"><div class="controls"><label>Worker 筛选 <select id="write-worker-filter"><option value="">全部 Worker</option></select></label></div><div id="write-worker-chart" class="chart"></div><div class="caption">图 4-4 写入 Worker 分布：写入链路按 worker 聚合。</div></div>
          <div class="panel"><h3>表 4-4 写入 Worker Breakdown</h3><div class="controls"><label>Worker 筛选 <select id="write-worker-table-filter"><option value="">全部 Worker</option></select></label></div><table id="write-worker-table"></table><div id="write-worker-table-pager" class="mini-pager"></div></div>
        </div>
        <div class="panel" style="display:none"><table id="flow-stage-table"></table></div>
      </section>
      <section id="s5">
        <h2>5. UB / URMA 分析</h2>
        <div class="panel section-summary" id="section-summary-s5"></div>
        <div class="panel insight">UB 单独看：先看 wait/poll/notify/sched，再看入口/出口 worker，最后看 edge/IP。</div>
        <div class="chart-grid">
          <div class="panel"><div id="ub-lifecycle-chart" class="chart"></div><div class="caption">图 5-1 UB 生命周期：TOTAL、wait_for、poll/notify/sched 等耗时字段，按实际采样字段展示。</div></div>
          <div class="panel"><div id="ub-wr-count-chart" class="chart"></div><div class="caption">图 5-2 WR / Inflight Count：remote get WR、URMA inflight WR、chip inflight，单位 count。</div></div>
          <div class="panel full-row"><div class="controls"><label>Worker 筛选 <select id="ub-worker-role-filter"><option value="">全部 Worker</option></select></label></div><div id="ub-worker-role-chart" class="chart"></div><div class="caption">图 5-3 UB 入口/出口 Worker：入口为 RemoteGet/transferPath，出口为 URMA_ELAPSED。</div></div>
        </div>
        <div class="panel"><div class="controls"><label>Worker 筛选 <select id="ub-worker-time-filter"><option value="">全部 Worker</option></select></label></div><div id="ub-worker-time-chart" class="chart"></div><div class="caption">图 5-4 UB 时间桶：按秒观察入口/出口事件与尾部时延。</div></div>
        <div class="flow-section ub-table-stack">
          <div class="panel"><h3>表 5-1 UB 生命周期指标</h3><div class="table-scroll"><table id="ub-lifecycle-table" class="nowrap-table"></table></div></div>
          <div class="panel"><h3>表 5-2 UB Request Top</h3><table id="ub-request-table" class="adaptive-table"></table><div id="ub-request-table-pager" class="mini-pager"></div></div>
          <div class="panel"><h3>表 5-3 UB Worker 角色</h3><div class="controls"><label>Worker 筛选 <select id="ub-worker-role-table-filter"><option value="">全部 Worker</option></select></label></div><table id="ub-worker-role-table" class="adaptive-table"></table><div id="ub-worker-role-table-pager" class="mini-pager"></div></div>
          <div class="panel"><h3>表 5-4 UB 时间桶</h3><div class="controls"><label>Worker 筛选 <select id="ub-worker-time-table-filter"><option value="">全部 Worker</option></select></label></div><table id="ub-worker-time-table"></table><div id="ub-worker-time-table-pager" class="mini-pager"></div></div>
        </div>
        <div class="flow-section">
          <div class="panel"><div id="read-ub-edge-chart" class="chart"></div><div class="caption">图 5-5 读取 UB Edge：读取 UB 入口/出口 IP 与 worker 关联。</div></div>
          <div class="panel"><h3>表 5-5 读取 UB Edges</h3><table id="read-ub-edge-table"></table><div id="read-ub-edge-table-pager" class="mini-pager"></div></div>
          <div class="panel"><div id="write-ub-edge-chart" class="chart"></div><div class="caption">图 5-6 写入 UB Edge：写入 UB 入口/出口 IP 与 worker 关联。</div></div>
          <div class="panel"><h3>表 5-6 写入 UB Edges</h3><table id="write-ub-edge-table"></table><div id="write-ub-edge-table-pager" class="mini-pager"></div></div>
        </div>
      </section>
      <section id="s6">
        <h2>6. Trace 查看</h2>
        <div class="panel section-summary" id="section-summary-s6"></div>
        <div class="panel">
        <div class="controls"><label>Trace 查看读写视角 <select id="operation-filter"><option value="">全部读写</option><option value="read">只看读取</option><option value="write">只看写入</option></select></label><label>请求状态 <select id="request-status-filter"><option value="">全部请求</option><option value="failed">只看失败</option><option value="success">只看成功</option></select></label><input id="trace-search" placeholder="搜索 trace / worker / 关键词" style="min-width:300px"><select id="class-filter"><option value="">全部分类</option></select><select id="worker-filter"><option value="">全部 Worker</option></select><span class="muted">按 access max 降序；联动 Trace 列表与选中 Trace Breakdown。</span><button id="reset-filter">清空</button></div>
        <h3>表 6-1 Top Trace</h3>
        <table id="top-trace-table"></table>
        <div id="trace-pager" class="controls pager">
          <label>每页 <select id="trace-page-size"><option value="4" selected>4</option><option value="8">8</option><option value="16">16</option><option value="32">32</option><option value="9999">全部</option></select> 条</label>
          <button class="primary" id="prev-page">上一页</button>
          <span id="page-status" class="muted"></span>
          <button class="primary" id="next-page">下一页</button>
        </div>
        </div>
        <div class="compare2">
          <div class="panel"><div id="selected-trace-chart" class="chart"></div><div id="selected-stage-legend" class="stage-legend"></div><div class="caption">图 6-1 选中 Trace Breakdown：点击 Trace 行联动，按阶段耗时排序，单位 ms。</div><table id="selected-stage-table"></table></div>
          <div class="panel"><h3>表 6-2 选中 Trace 摘要</h3><table id="selected-trace-table"></table><div class="controls"><button class="primary" id="download-selected-raw">下载当前 Trace 裸日志</button><button id="download-filtered-evidence">下载当前过滤证据</button></div></div>
        </div>
        <div class="panel"><h3>日志框 6-3 Trace 全量日志</h3><div class="small">按组件分块，保留原始顺序；异常、慢 RPC、latencySummary、RemotePull、URMA 和大耗时会高亮。</div><div class="log-legend" id="log-highlight-legend"></div><div id="selected-trace-log" class="trace-log-groups"></div></div>
      </section>
      <section id="s7">
        <h2>7. 建议与后续口径</h2>
        <div class="panel section-summary" id="section-summary-s7"></div>
        <div class="panel"><h3>表 7-1 建议与证据边界</h3><table id="recommendation-table"></table></div>
        <div class="panel">
          <h3>表 7-2 代码与字段映射</h3>
          <h4>通用字段映射</h4><table id="source-appendix-common-table"></table>
          <h4>读取字段映射</h4><table id="source-appendix-read-table"></table>
          <h4>写入字段映射</h4><table id="source-appendix-write-table"></table>
        </div>
      </section>
      <section id="s8">
        <h2>8. 原始 JSON 附录</h2>
        <div class="panel section-summary" id="section-summary-s8"></div>
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
  function traceAccessLatencyMs(item) {
    const value = item?.access_latency_ms?.max ?? item?.access_latency_ms?.p99 ?? item?.access_latency_ms?.p50 ?? 0;
    const n = Number(value || 0);
    return Number.isFinite(n) ? Number(n.toFixed(3)) : 0;
  }
  const traceRows = Object.entries(traces).sort((a,b) => traceAccessLatencyMs(b[1]) - traceAccessLatencyMs(a[1]));
  let filteredTraceRows = traceRows;
  let currentPage = 0;
  let selectedTraceId = traceRows[0]?.[0] || null;
  let pageSize = 4;
  let activeOperation = '';
  let topTraceSort = null;
  function escapeHtml(value) {
    return String(value ?? '').replace(/[&<>"']/g, ch => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[ch]));
  }
  function workerNameParts(raw) {
    const text = String(raw || '');
    let match = text.match(/client-?(\\d+).*?worker-?(\\d+)/i);
    if (match) return {kind:'client', left:match[1], worker:match[2]};
    match = text.match(/worker-?(\\d+).*?worker-?(\\d+)/i);
    if (match) return {kind:'worker', left:match[1], worker:match[2]};
    match = text.match(/(?:^|[-_/])worker-?(\\d+)/i);
    if (match) return {kind:'worker', worker:match[1]};
    return {kind:'unknown', raw:text};
  }
  function workerDisplayName(raw) {
    const parts = workerNameParts(raw);
    return parts.worker ? `worker ${parts.worker}` : (parts.raw || 'unknown worker');
  }
  function workerAggregateKey(raw) {
    const parts = workerNameParts(raw);
    return parts.worker ? `worker:${parts.worker}` : `raw:${String(raw || 'unknown')}`;
  }
  function workerAggregateLabel(key) {
    const text = String(key || '');
    if (text.startsWith('worker:')) return `worker ${text.slice('worker:'.length)}`;
    if (text.startsWith('raw:')) return text.slice('raw:'.length) || 'unknown worker';
    return workerDisplayName(text);
  }
  function workerMatchesFilter(raw, selectedWorker) {
    return !selectedWorker || workerAggregateKey(raw) === selectedWorker;
  }
  function workerRelationName(raw) {
    const parts = workerNameParts(raw);
    if (parts.kind === 'client' && parts.left && parts.worker) return `client ${parts.left} → worker ${parts.worker}`;
    if (parts.kind === 'worker' && parts.left && parts.worker) return `worker ${parts.left} → worker ${parts.worker}`;
    return workerDisplayName(raw);
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
  const tableSortStates = {};
  function sortCellValue(value, header) {
    const lowerHeader = String(header || '').toLowerCase();
    const text = String(value ?? '').replace(/<[^>]*>/g, '').trim();
    const distributionMatch = text.match(/\\b(max|p99|p90|p50|count)=(-?\\d+(?:\\.\\d+)?)/gi);
    if (distributionMatch && /(distribution|latency|access)/.test(lowerHeader)) {
      const scores = Object.fromEntries(distributionMatch.map(part => {
        const [, key, raw] = part.match(/(max|p99|p90|p50|count)=(-?\\d+(?:\\.\\d+)?)/i);
        return [key.toLowerCase(), Number(raw)];
      }));
      return {type:'number', value:scores.max ?? scores.p99 ?? scores.p90 ?? scores.p50 ?? scores.count ?? 0, text};
    }
    if (/(time|trace|worker|edge|classification|status|access)/.test(lowerHeader) && !/(latency ms|p50|p90|p99|max|count|events|lines|traces|slow|errors|duration|total|wait|wake|poll|notify|sched|data size|cpuid)/.test(lowerHeader)) {
      return {type:'text', value:text.toLowerCase(), text};
    }
    if (typeof value === 'number') return {type:'number', value, text};
    if (/^-?\\d+(?:\\.\\d+)?$/.test(text)) return {type:'number', value:Number(text), text};
    return {type:'text', value:text.toLowerCase(), text};
  }
  function sortRowsForTable(rows, sort, headers=[]) {
    if (!sort || sort.index === undefined || sort.index === null) return rows.slice();
    return rows.map((row, originalIndex) => ({row, originalIndex})).sort((a, b) => {
      const av = sortCellValue(a.row[sort.index], headers[sort.index]);
      const bv = sortCellValue(b.row[sort.index], headers[sort.index]);
      let cmp = 0;
      if (av.type === 'number' && bv.type === 'number') cmp = av.value - bv.value;
      else cmp = av.text.localeCompare(bv.text, 'zh-CN', {numeric:true});
      if (cmp === 0) cmp = a.originalIndex - b.originalIndex;
      return sort.dir === 'desc' ? -cmp : cmp;
    }).map(item => item.row);
  }
  function nextSortState(current, index) {
    if (!current || current.index !== index) return {index, dir:'asc'};
    return {index, dir:current.dir === 'asc' ? 'desc' : 'asc'};
  }
  function renderTable(id, headers, rows, rowAttrs, options={}) {
    const table = document.getElementById(id);
    const sortable = options.sortable !== false;
    const sortState = options.sortState || tableSortStates[id];
    const visibleRows = options.skipSort ? rows.slice() : sortRowsForTable(rows, sortState, headers);
    const headerHtml = headers.map((h, index) => {
      const active = sortState && sortState.index === index;
      const mark = active ? `<span class="sort-mark">${sortState.dir === 'asc' ? '▲' : '▼'}</span>` : '<span class="sort-mark">↕</span>';
      return `<th class="${sortable ? 'sortable-th' : ''}" data-sort-index="${index}" aria-sort="${active ? (sortState.dir === 'asc' ? 'ascending' : 'descending') : 'none'}">${escapeHtml(h)}${sortable ? mark : ''}</th>`;
    }).join('');
    if (!rows.length) {
      table.innerHTML = `<thead><tr>${headerHtml}</tr></thead><tbody><tr><td class="muted" colspan="${headers.length}">No data</td></tr></tbody>`;
      return;
    }
    table.innerHTML = `<thead><tr>${headerHtml}</tr></thead>` +
      `<tbody>${visibleRows.map((row, idx) => `<tr ${rowAttrs ? rowAttrs(row, idx) : ''}>${row.map((cell, cellIdx) => `<td>${formatCell(headers[cellIdx], cell)}</td>`).join('')}</tr>`).join('')}</tbody>`;
    if (sortable) {
      table.querySelectorAll('th[data-sort-index]').forEach(th => th.addEventListener('click', () => {
        const index = Number(th.dataset.sortIndex);
        if (options.onSort) options.onSort(index);
        else {
          tableSortStates[id] = nextSortState(tableSortStates[id], index);
          renderTable(id, headers, rows, rowAttrs, options);
        }
      }));
    }
  }
  function formatCell(header, cell) {
    const text = String(cell ?? '');
    const lowerHeader = String(header || '').toLowerCase();
    const lower = text.toLowerCase();
    const escaped = escapeHtml(text);
    if (!text) return '';
    const numeric = Number(text);
    if (/(error|errors|status|classification)/.test(lowerHeader) && /(deadline|timeout|error|fail|1001|runtime)/i.test(text)) {
      return `<span class="cell-hot">${escaped}</span>`;
    }
    if (/status/.test(lowerHeader) && /(present|ok|success)/i.test(text)) return `<span class="cell-ok">${escaped}</span>`;
    if (/(latency|duration|p99|max|access|slow)/.test(lowerHeader) && Number.isFinite(numeric)) {
      return `<span class="${numeric >= 20 ? 'cell-hot' : numeric >= 5 ? 'cell-warn' : 'cell-ok'}">${escaped}</span>`;
    }
    if (/(slow|errors|count|traces)/.test(lowerHeader) && Number.isFinite(numeric) && numeric > 0) {
      return `<span class="${numeric >= 20 ? 'cell-hot' : 'cell-warn'}">${escaped}</span>`;
    }
    if (/(access|latency|distribution)/.test(lowerHeader) && /(p99|max)=/i.test(text)) {
      return escaped.replace(/\\b(p99|max)=([\\d.]+)/gi, (match, key, value) => {
        const n = Number(value);
        const cls = Number.isFinite(n) && n >= 20 ? 'cell-hot' : Number.isFinite(n) && n >= 5 ? 'cell-warn' : 'cell-ok';
        return `<span class="${cls}">${key}=${value}</span>`;
      });
    }
    if (/(deadline|timeout|error|fail|rpc deadline exceeded|status=1001)/i.test(text)) {
      return escaped.replace(/(deadline|timeout|error|fail|RPC deadline exceeded|status=1001)/gi, '<span class="cell-hot">$1</span>');
    }
    return escaped;
  }
  const pagedTables = {};
  function renderPagedTable(id, pagerId, headers, rows, rowAttrs, pageSize=5) {
    const state = pagedTables[id] || (pagedTables[id] = {page:0, sort:null});
    const sortedRows = sortRowsForTable(rows, state.sort, headers);
    const pages = Math.max(1, Math.ceil(sortedRows.length / pageSize));
    state.page = Math.min(Math.max(state.page, 0), pages - 1);
    const start = state.page * pageSize;
    renderTable(id, headers, sortedRows.slice(start, start + pageSize), (row, idx) => rowAttrs ? rowAttrs(row, start + idx) : '', {
      skipSort:true,
      sortState:state.sort,
      onSort:index => {
        state.sort = nextSortState(state.sort, index);
        state.page = 0;
        renderPagedTable(id, pagerId, headers, rows, rowAttrs, pageSize);
      }
    });
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
  function hasTraceFailure(item) {
    return Object.keys(item.errors || {}).length > 0 || /deadline|error|fail|timeout/i.test(String(item.classification || ''));
  }
  function statusMatchesTrace(item, requestStatus) {
    if (!requestStatus) return true;
    const failed = hasTraceFailure(item);
    return requestStatus === 'failed' ? failed : !failed;
  }
  function traceStartTime(item) {
    return item.first_ts || item.last_ts || '';
  }
  function stageMatchesOperation(stageName) {
    return !activeOperation || String(stageName || '').startsWith(`${activeOperation}.`);
  }
  function flowOperation(name) {
    const text = String(name || '');
    if (/GET/.test(text)) return 'read';
    if (/(SET|CREATE|PUBLISH)/.test(text)) return 'write';
    return 'unknown';
  }
  function metricOperation(name) {
    const text = String(name || '').toLowerCase();
    if (text === 'access') return 'both';
    if (text.includes('urma') || text.includes('remote_get') || text.includes('query_meta') || text.endsWith('.get') || text.includes('process.get')) return 'read';
    if (text.includes('memory_copy') || text.includes('publish') || text.includes('create') || text.endsWith('.set') || text.includes('process.set')) return 'write';
    return 'unknown';
  }
  function percentileFromValues(values) {
    const nums = values.map(Number).filter(Number.isFinite).sort((a,b) => a-b);
    if (!nums.length) return null;
    const pick = pct => nums[Math.min(nums.length - 1, Math.floor((nums.length - 1) * pct))];
    return {count:nums.length,min:nums[0],p50:pick(.5),p90:pick(.9),p99:pick(.99),max:nums[nums.length - 1]};
  }
  function traceRowsForOperation(operation) {
    return traceRows.filter(([, item]) => {
      const op = traceOperation(item);
      return op === operation || op === 'mixed';
    });
  }
  function operationAccessPercentiles(operation) {
    return percentileFromValues(traceRowsForOperation(operation).map(([, item]) => item.access_latency_ms?.max ?? item.access_latency_ms?.p99 ?? item.access_latency_ms?.p50));
  }
  const palette = ['#2563eb','#ea580c','#059669','#7c3aed','#dc2626','#0891b2','#ca8a04','#64748b'];
  const chartTextStyle = {fontSize:13, color:'#475569'};
  function axisBase(title, extra) {
    return Object.assign({
      title:{show:false,text:title},
      color:palette,
      textStyle:chartTextStyle,
      grid:{left:58,right:24,top:72,bottom:76,containLabel:true},
      legend:{top:30,type:'scroll'},
      toolbox:{right:10,feature:{saveAsImage:{},dataView:{readOnly:true},restore:{}}},
      tooltip:{trigger:'axis', axisPointer:{type:'shadow'}, confine:true},
      dataZoom:[{type:'inside'},{type:'slider', height:18, bottom:18}]
    }, extra || {});
  }
  function wideHorizontalGrid(left=120, top=56, bottom=34) {
    return {left:left,right:24,top:top,bottom:bottom,containLabel:false};
  }
  function noDataOption(title) {
    return {title:{text:title, left:'center', top:'center', textStyle:Object.assign({}, chartTextStyle, {color:'#94a3b8'})}};
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
  function autoCenterFlowGraph(chartInstance) {
    if (!chartInstance) return;
    if (chartInstance.dispatchAction) chartInstance.dispatchAction({type:'restore'});
    if (chartInstance.resize) chartInstance.resize();
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
  function traceLogBlockSummary(group) {
    const text = group.lines.join('\\n');
    const focus = [];
    const add = (label, tone='') => {
      if (!focus.some(item => item.label === label)) focus.push({label, tone});
    };
    if (/ERROR|FATAL|Get error|Set error|status[:=]?\\s*1001/i.test(text)) add('error/status', 'hot');
    if (/deadline exceeded|RPC timed out|\\btimeout\\b|20ms deadline/i.test(text)) add('deadline/timeout', 'hot');
    if (/RPC_FRAMEWORK_SLOW|server_exec_us=|network_residual_us=|remote_processing_us=/i.test(text)) add('RPC slow', 'warn');
    if (/latencySummary|client\\.rpc\\.|worker\\.rpc\\.|worker\\.process\\./i.test(text)) add('latencySummary');
    if (/URMA_ELAPSED|URMA_WAIT_TIMEOUT|\\burma[_a-z]*\\b|\\bUB\\b/i.test(text)) add('URMA/UB', 'warn');
    if (/RemotePull|Remote done|BatchGetObjectRemote/i.test(text)) add('RemoteGet');
    if (/QueryMeta|GetObjMetaInfo|MasterOCService/i.test(text)) add('QueryMeta');
    if (/CreateBuffer|Publish/i.test(text)) add('Write path');
    const costs = [...text.matchAll(/(?:cost(?:Us)?|totalCost|framework_us|e2e_us|server_exec_us|network_residual_us|remote_processing_us)[:=]?\\s*([\\d.]+)\\s*(us|ms)?/gi)]
      .map(match => {
        const raw = Number(match[1]);
        if (!Number.isFinite(raw)) return null;
        const unit = (match[2] || '').toLowerCase();
        return unit === 'us' || /(?:costUs|_us)/i.test(match[0]) ? raw / 1000 : raw;
      })
      .filter(value => value !== null);
    if (costs.length) add(`max ${Math.max(...costs).toFixed(3)}ms`, Math.max(...costs) >= 20 ? 'hot' : 'warn');
    const ips = [...new Set([...text.matchAll(/\\b\\d{1,3}(?:\\.\\d{1,3}){3}(?::\\d+)?\\b/g)].map(match => match[0]))].slice(0, 2);
    ips.forEach(ip => add(ip));
    if (!focus.length) add('按原始时间顺序看');
    return '<span>重点:</span>' + focus.slice(0, 7).map(item =>
      `<span class="trace-log-focus ${escapeHtml(item.tone)}">${escapeHtml(item.label)}</span>`
    ).join('');
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
      `<h4><span class="trace-log-heading"><span>${escapeHtml(componentLabels[group.component] || group.component)}</span><span class="trace-log-summary">${traceLogBlockSummary(group)}</span></span><span class="trace-log-count">${group.lines.length} lines</span></h4>` +
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
  function sourceAppendixByScope(scope) {
    return sourceAppendix.filter(item => (item.scope || '通用') === scope);
  }
  function renderSourceAppendixTables() {
    const headers = ['log surface','flow stage','source hint','validation','report reading'];
    const renderScope = (id, scope) => renderTable(id, headers, sourceAppendixByScope(scope).map(item => [
      item.log_surface,
      item.flow_stage,
      item.source_hint,
      item.validation,
      item.report_reading
    ]));
    renderScope('source-appendix-common-table', '通用');
    renderScope('source-appendix-read-table', '读取');
    renderScope('source-appendix-write-table', '写入');
  }
  renderSourceAppendixTables();
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
  const flowRows = Object.entries(dim.flow || {}).sort((a,b) => b[1]-a[1]);
  const workerRows = Object.entries(dim.worker_summary || {})
    .sort((a,b) => (b[1].error_count || 0) - (a[1].error_count || 0) || (b[1].line_count || 0) - (a[1].line_count || 0))
    .slice(0, 40);
  const ubRows = Object.entries(dim.ub_summary?.edges || {})
    .sort((a,b) => (b[1].count || 0) - (a[1].count || 0))
    .slice(0, 40);
  const ubWorkerRows = Object.entries(dim.ub_worker_summary?.workers || {})
    .sort((a,b) => (b[1].latency_ms?.max || 0) - (a[1].latency_ms?.max || 0) || ((b[1].entry_events || 0) + (b[1].exit_events || 0)) - ((a[1].entry_events || 0) + (a[1].exit_events || 0)))
    .slice(0, 40);
  const ubWorkerTimeRows = dim.ub_worker_summary?.time_buckets || [];
  const ubLifecycleMetrics = Object.entries(dim.ub_lifecycle_summary?.metrics || {})
    .sort((a,b) => (b[1].max || 0) - (a[1].max || 0));
  const ubCountMetricNames = new Set(['remote_get_wr_count','urma_inflight_wr_count']);
  const ubLifecycleLatencyMetrics = ubLifecycleMetrics.filter(([name]) => !ubCountMetricNames.has(name));
  const ubChipInflightRows = Object.entries(dim.ub_lifecycle_summary?.chip_inflight || {})
    .sort((a,b) => (b[1].max || 0) - (a[1].max || 0));
  const ubWrCountRows = [
    ...ubLifecycleMetrics.filter(([name]) => ubCountMetricNames.has(name)).map(([name,item]) => [lifecycleLabel(name), item]),
    ...ubChipInflightRows.map(([chip,item]) => [`Chip ${chip} inflight`, item])
  ].sort((a,b) => (b[1].max || 0) - (a[1].max || 0));
  const ubRequestRows = dim.ub_lifecycle_summary?.requests || [];
  const timeBucketRows = dim.time_buckets?.['1000ms'] || [];
  function latencyRowsForOperation(operation) {
    const rows = latencyChartRows.filter(([name]) => metricOperation(name) === operation || metricOperation(name) === 'both');
    const accessRow = ['access', operationAccessPercentiles(operation)];
    return [accessRow, ...rows.filter(([name]) => name !== 'access')]
      .filter(([, item]) => item && item.count)
      .sort((a,b) => (b[1].max || 0) - (a[1].max || 0))
      .slice(0, 12);
  }
  function flowRowsForOperation(operation) {
    return flowRows.filter(([name]) => flowOperation(name) === operation);
  }
  function buildTraceTimeBuckets(operation) {
    const buckets = {};
    traceRowsForOperation(operation).forEach(([, item]) => {
      const ts = traceStartTime(item);
      if (!ts) return;
      const bucket = String(ts).slice(0, 19);
      const target = buckets[bucket] || (buckets[bucket] = {bucket_start:bucket,bucket_ms:1000,access:[],stage_values:{}});
      const accessValue = item.access_latency_ms?.max ?? item.access_latency_ms?.p99 ?? item.access_latency_ms?.p50;
      if (Number.isFinite(Number(accessValue))) target.access.push(Number(accessValue));
      (item.stage_breakdown || []).forEach(stage => {
        if (!String(stage.stage || '').startsWith(`${operation}.`) || stage.duration_ms === undefined) return;
        (target.stage_values[stage.stage] || (target.stage_values[stage.stage] = [])).push(Number(stage.duration_ms));
      });
    });
    return Object.values(buckets).sort((a,b) => String(a.bucket_start).localeCompare(String(b.bucket_start))).map(bucket => {
      const stageBreakdown = {};
      Object.entries(bucket.stage_values).forEach(([name, values]) => { stageBreakdown[name] = percentileFromValues(values) || {}; });
      return {bucket_start:bucket.bucket_start,bucket_ms:bucket.bucket_ms,p99_access_ms:(percentileFromValues(bucket.access) || {}).p99 || 0,stage_breakdown_ms:stageBreakdown};
    });
  }
  function workerFilterValue(id) {
    const node = document.getElementById(id);
    return node ? node.value : '';
  }
  function workerRowsForOperation(operation, selectedWorker='') {
    const workers = {};
    traceRowsForOperation(operation).forEach(([, item]) => {
      const errorCount = Object.values(item.errors || {}).reduce((a,b) => a + b, 0);
      const isSlow = Number(item.access_latency_ms?.max || 0) >= 20;
      const seenWorkerKeys = new Set();
      Object.keys(item.workers || {}).forEach(worker => {
        const key = workerAggregateKey(worker);
        if (!workerMatchesFilter(worker, selectedWorker) || seenWorkerKeys.has(key)) return;
        seenWorkerKeys.add(key);
        const source = dim.worker_summary?.[worker] || {};
        const target = workers[key] || (workers[key] = {display_worker:workerAggregateLabel(key),roles:[],line_count:0,trace_count:0,slow_trace_count:0,error_count:0,relations:new Set()});
        target.roles = [...new Set([...(target.roles || []), ...(source.roles || [])])];
        target.line_count += source.line_count || 0;
        target.relations.add(workerRelationName(worker));
        target.trace_count += 1;
        target.slow_trace_count += isSlow ? 1 : 0;
        target.error_count += errorCount;
      });
    });
    return Object.entries(workers).sort((a,b) => (b[1].error_count || 0) - (a[1].error_count || 0) || (b[1].slow_trace_count || 0) - (a[1].slow_trace_count || 0) || (b[1].trace_count || 0) - (a[1].trace_count || 0)).slice(0, 40);
  }
  function renderLatencySection(operation, title) {
    const rows = latencyRowsForOperation(operation);
    renderTable(`${operation}-latency-table`, ['metric','distribution'], rows.map(([name,item]) => [metricLabel(name), pctText(item)]));
    chart(`${operation}-latency-chart`, rows.length ? axisBase(`${title} Top Latency`, {
      grid:wideHorizontalGrid(150,66,36),
      tooltip:{trigger:'axis',axisPointer:{type:'shadow'},confine:true,formatter:ps => ps.map(p => `${escapeHtml(p.seriesName)}: ${p.value} ms`).join('<br>')},
      xAxis:{type:'value', name:'ms'},
      yAxis:{type:'category', data:rows.map(r => metricLabel(r[0])), axisLabel:{width:138, overflow:'truncate'}},
      series:['p50','p90','p99','max'].map(key => ({name:key,type:'bar',barMaxWidth:18,data:rows.map(([, item]) => item[key] || 0), markLine:key === 'max' ? {symbol:'none', lineStyle:{color:'#dc2626',type:'dashed'}, label:{formatter:'20ms deadline'}, data:[{xAxis:20}]} : undefined}))
    }) : noDataOption(`No ${operation} latency data`));
  }
  function renderFlowSection(operation, title) {
    const rows = flowRowsForOperation(operation);
    renderTable(`${operation}-flow-table`, ['flow','count'], rows);
    chart(`${operation}-flow-chart`, rows.length ? axisBase(`${title} Flow Breakdown`, {
      grid:wideHorizontalGrid(122,56,34),
      tooltip:{trigger:'axis',axisPointer:{type:'shadow'},confine:true},
      xAxis:{type:'value', name:'count'},
      yAxis:{type:'category', data:rows.map(r => flowLabel(r[0])), axisLabel:{width:112, overflow:'truncate'}},
      series:[{name:'count',type:'bar',barMaxWidth:28,data:rows.map(r => r[1]),itemStyle:{color:'#2563eb'},label:{show:true,position:'right'}}]
    }) : noDataOption(`No ${operation} flow data`));
  }
  function renderTimeBucketSection(operation, title) {
    const rows = buildTraceTimeBuckets(operation);
    const stageNames = [...new Set(rows.flatMap(row => Object.keys(row.stage_breakdown_ms || {})))].filter(name => !name.endsWith('client_to_entry_worker'));
    chart(`${operation}-time-breakdown-chart`, rows.length ? axisBase(`${title} Time Bucket Latency Stages`, {
      tooltip:{trigger:'axis', axisPointer:{type:'cross'}, confine:true},
      xAxis:{type:'category', data:rows.map(r => String(r.bucket_start || '').replace('T','\\n')), axisLabel:{rotate:0}},
      yAxis:{type:'value', name:'ms'},
      series:[
        ...stageNames.map(name => ({name:stageLabelMap[name] || name,type:'bar',stack:'stage-p99',barMaxWidth:34,data:rows.map(r => r.stage_breakdown_ms?.[name]?.p99 || 0),itemStyle:{color:stageColorMap[name] || '#64748b'}})),
        {name:'client/access p99 upper bound',type:'line',smooth:true,data:rows.map(r => r.p99_access_ms || 0), itemStyle:{color:'#2563eb'}, markLine:{symbol:'none', lineStyle:{color:'#dc2626',type:'dashed'}, label:{formatter:'20ms deadline'}, data:[{yAxis:20}]}}
      ]
    }) : noDataOption(`No ${operation} time bucket stage data`));
  }
  function renderWorkerSection(operation, title) {
    const chartRows = workerRowsForOperation(operation, workerFilterValue(`${operation}-worker-filter`));
    const tableRowsForOperation = workerRowsForOperation(operation, workerFilterValue(`${operation}-worker-table-filter`));
    const tableRows = tableRowsForOperation.map(([,item]) => {
      const display_worker = item.display_worker || '';
      return [display_worker, (item.roles || []).join(','), item.line_count, item.trace_count, item.slow_trace_count || 0, item.error_count || 0];
    });
    renderPagedTable(`${operation}-worker-table`, `${operation}-worker-table-pager`, ['worker','roles','lines','traces','slow','errors'], tableRows,
      row => (Number(row[5]) > 0 ? 'class="hotrow"' : Number(row[4]) > 0 ? 'class="warnrow"' : ''), 5);
    chart(`${operation}-worker-chart`, chartRows.length ? axisBase(`${title} Workers by Trace/Error`, {xAxis:{type:'category', data:chartRows.slice(0,20).map(r => r[1].display_worker || workerAggregateLabel(r[0])), axisLabel:{rotate:35, width:120, overflow:'truncate'}}, yAxis:{type:'value'}, series:[
      {name:'traces',type:'bar',barMaxWidth:34,data:chartRows.slice(0,20).map(r => r[1].trace_count || 0), itemStyle:{color:'#94a3b8'}},
      {name:'slow',type:'bar',barMaxWidth:34,data:chartRows.slice(0,20).map(r => r[1].slow_trace_count || 0), itemStyle:{color:'#ea580c'}},
      {name:'errors',type:'bar',barMaxWidth:34,data:chartRows.slice(0,20).map(r => r[1].error_count || 0), itemStyle:{color:'#dc2626'}, label:{show:true, position:'top'}}
    ]}) : noDataOption(`No ${operation} worker data`));
  }
  function renderUbSection(operation) {
    const rows = operation === 'read' ? ubRows : [];
    const tableRows = rows.map(([edge,item]) => [edge, item.count, item.latency_ms?.p99 || '', item.latency_ms?.max || '', pctText(item.latency_ms)]);
    renderPagedTable(`${operation}-ub-edge-table`, `${operation}-ub-edge-table-pager`, ['edge','count','p99 ms','max ms','latency'], tableRows,
      row => `class="${severityClass(Math.max(Number(row[2]) || 0, Number(row[3]) || 0))}"`, 5);
    chart(`${operation}-ub-edge-chart`, rows.length ? axisBase(`${operation === 'read' ? 'Read' : 'Write'} UB Edge Count / Tail Latency`, {xAxis:{type:'category', data:rows.slice(0,20).map(r => r[0]), axisLabel:{rotate:35, width:150, overflow:'truncate'}}, yAxis:[{type:'value', name:'count'}, {type:'value', name:'ms'}], series:[
      {name:'UB edges',type:'bar',barMaxWidth:34,data:rows.slice(0,20).map(r => r[1].count || 0), itemStyle:{color:'#059669'}, label:{show:true, position:'top'}},
      {name:'p99 ms',type:'line',yAxisIndex:1,smooth:true,data:rows.slice(0,20).map(r => r[1].latency_ms?.p99 || 0), itemStyle:{color:'#dc2626'}, markLine:{symbol:'none', lineStyle:{color:'#dc2626',type:'dashed'}, label:{formatter:'20ms'}, data:[{yAxis:20}]}}
    ]}) : noDataOption(operation === 'write' ? 'Write flow has no UB read edge data' : 'No read UB edge data'));
  }
  function renderUbWorkerViews() {
    const roleChartWorker = workerFilterValue('ub-worker-role-filter');
    const timeChartWorker = workerFilterValue('ub-worker-time-filter');
    const roleTableWorker = workerFilterValue('ub-worker-role-table-filter');
    const timeTableWorker = workerFilterValue('ub-worker-time-table-filter');
    const filterUbWorkerRows = selectedWorker => selectedWorker ? ubWorkerRows.filter(([worker]) => workerMatchesFilter(worker, selectedWorker)) : ubWorkerRows;
    const filterUbWorkerTimeRows = selectedWorker => selectedWorker ? ubWorkerTimeRows.filter(item =>
      (item.top_entry_workers || []).some(worker => workerMatchesFilter(worker, selectedWorker)) ||
      (item.top_exit_workers || []).some(worker => workerMatchesFilter(worker, selectedWorker))
    ) : ubWorkerTimeRows;
    const chartUbWorkerRows = filterUbWorkerRows(roleChartWorker);
    const tableUbWorkerRows = filterUbWorkerRows(roleTableWorker);
    const chartUbWorkerTimeRows = filterUbWorkerTimeRows(timeChartWorker);
    const tableUbWorkerTimeRows = filterUbWorkerTimeRows(timeTableWorker);
    renderPagedTable('ub-worker-role-table', 'ub-worker-role-table-pager', ['worker','role','entry events','exit events','trace count','p99 ms','max ms','top edges'], tableUbWorkerRows.map(([worker,item]) => {
      const display_worker = workerRelationName(worker);
      const display_top_edges = (item.top_edges || []).map(edge => edge.replace(/\\s*->\\s*/g, ' → ')).join(', ');
      return [
        display_worker,
        item.role || '',
        item.entry_events || 0,
        item.exit_events || 0,
        item.trace_count || 0,
        item.latency_ms?.p99 || '',
        item.latency_ms?.max || '',
        display_top_edges
      ];
    }), row => `class="${severityClass(Math.max(Number(row[5]) || 0, Number(row[6]) || 0))}"`, 5);
    renderPagedTable('ub-worker-time-table', 'ub-worker-time-table-pager', ['time','entry events','exit events','p99 ms','max ms','entry workers','exit workers'], tableUbWorkerTimeRows.map(item => [
      item.bucket_start || '',
      item.entry_events || 0,
      item.exit_events || 0,
      item.latency_ms?.p99 || '',
      item.latency_ms?.max || '',
      (item.top_entry_workers || []).map(workerRelationName).join(', '),
      (item.top_exit_workers || []).map(workerRelationName).join(', ')
    ]), row => `class="${severityClass(Math.max(Number(row[3]) || 0, Number(row[4]) || 0))}"`, 5);
    chart('ub-worker-role-chart', chartUbWorkerRows.length ? axisBase('UB Entry/Exit Workers', {
      tooltip:{trigger:'axis', axisPointer:{type:'shadow'}, confine:true},
      xAxis:{type:'category', data:chartUbWorkerRows.slice(0,20).map(r => workerRelationName(r[0])), axisLabel:{rotate:35, width:120, overflow:'truncate'}},
      yAxis:[{type:'value', name:'events'}, {type:'value', name:'ms'}],
      series:[
        {name:'入口 UB events',type:'bar',stack:'ub-role',barMaxWidth:34,data:chartUbWorkerRows.slice(0,20).map(r => r[1].entry_events || 0),itemStyle:{color:'#2563eb'}},
        {name:'出口 UB events',type:'bar',stack:'ub-role',barMaxWidth:34,data:chartUbWorkerRows.slice(0,20).map(r => r[1].exit_events || 0),itemStyle:{color:'#ea580c'}},
        {name:'p99 ms',type:'line',yAxisIndex:1,data:chartUbWorkerRows.slice(0,20).map(r => r[1].latency_ms?.p99 || 0),itemStyle:{color:'#dc2626'}}
      ]
    }) : noDataOption('No UB worker role data'));
    chart('ub-worker-time-chart', chartUbWorkerTimeRows.length ? axisBase('UB Entry/Exit Time Buckets', {
      tooltip:{trigger:'axis', axisPointer:{type:'cross'}, confine:true},
      xAxis:{type:'category', data:chartUbWorkerTimeRows.map(r => String(r.bucket_start || '').replace('T','\\n')), axisLabel:{rotate:0}},
      yAxis:[{type:'value', name:'events'}, {type:'value', name:'ms'}],
      series:[
        {name:'入口 UB events',type:'bar',stack:'ub-time',barMaxWidth:34,data:chartUbWorkerTimeRows.map(r => r.entry_events || 0),itemStyle:{color:'#2563eb'}},
        {name:'出口 UB events',type:'bar',stack:'ub-time',barMaxWidth:34,data:chartUbWorkerTimeRows.map(r => r.exit_events || 0),itemStyle:{color:'#ea580c'}},
        {name:'p99 ms',type:'line',yAxisIndex:1,smooth:true,data:chartUbWorkerTimeRows.map(r => r.latency_ms?.p99 || 0),itemStyle:{color:'#dc2626'}, markLine:{symbol:'none', lineStyle:{color:'#dc2626',type:'dashed'}, label:{formatter:'20ms'}, data:[{yAxis:20}]}}
      ]
    }) : noDataOption('No UB time bucket data'));
  }
  function lifecycleLabel(name) {
    const labels = {
      'total_ms':'URMA total',
      'wait_os_sched_ms':'wait_for',
      'wake_sched_latency_ms':'wake sched',
      'poll_jfc_ms':'poll_jfc',
      'notify_ms':'notify',
      'thread_sched_ms':'thread sched',
      'poll_loop_gap_ms':'poll loop gap',
      'nanosleep_wake_ms':'nanosleep wake',
      'remote_get_wr_count':'RemoteGet WR count',
      'urma_inflight_wr_count':'URMA inflight WR count'
    };
    return labels[name] || metricLabel(name);
  }
  function renderUbLifecycleViews() {
    function missingMetricCell(value) {
      return value === undefined || value === null || value === '' ? '未采样' : value;
    }
    const metricRows = [
      ...ubLifecycleMetrics.map(([name,item]) => [lifecycleLabel(name), item.count || 0, item.p50 ?? '', item.p90 ?? '', item.p99 ?? '', item.max ?? '']),
      ...ubChipInflightRows.map(([chip,item]) => [`Chip ${chip} inflight`, item.count || 0, item.p50 ?? '', item.p90 ?? '', item.p99 ?? '', item.max ?? ''])
    ];
    renderTable('ub-lifecycle-table', ['metric','count','p50','p90','p99','max'], metricRows,
      row => `class="${severityClass(Math.max(Number(row[4]) || 0, Number(row[5]) || 0))}"`);
    renderPagedTable('ub-request-table', 'ub-request-table-pager',
      ['time','trace','request','worker','total ms','wait ms','wake ms','remote wr','urma wr','poll ms','notify ms','sched ms','edge','cpuid','data size','status','chip inflight'],
      ubRequestRows.map(item => [
        item.first_ts || '',
        item.trace_id || '',
        item.request_id || '',
        item.worker || '',
        item.total_ms ?? '',
        item.wait_os_sched_ms ?? '',
        item.wake_sched_latency_ms ?? '',
        item.remote_get_wr_count ?? '',
        item.urma_inflight_wr_count ?? '',
        missingMetricCell(item.poll_jfc_ms),
        missingMetricCell(item.notify_ms),
        missingMetricCell(Math.max(Number(item.poll_loop_gap_ms || 0), Number(item.nanosleep_wake_ms || 0), Number(item.thread_sched_ms || 0)) || ''),
        `${item.src_addr || ''} -> ${item.target_addr || ''}`,
        item.cpuid ?? '',
        item.data_size ?? '',
        item.status || '',
        item.src_chip_inflight || ''
      ]),
      row => `class="${severityClass(Math.max(Number(row[4]) || 0, Number(row[5]) || 0, Number(row[11]) || 0))}"`, 5);
    chart('ub-lifecycle-chart', ubLifecycleLatencyMetrics.length ? axisBase('UB Lifecycle Breakdown', {
      grid:wideHorizontalGrid(138,66,36),
      tooltip:{trigger:'axis',axisPointer:{type:'shadow'},confine:true,formatter:ps => ps.map(p => `${escapeHtml(p.seriesName)}: ${p.value} ms`).join('<br>')},
      xAxis:{type:'value', name:'ms'},
      yAxis:{type:'category', data:ubLifecycleLatencyMetrics.map(r => lifecycleLabel(r[0])), axisLabel:{width:128, overflow:'truncate'}},
      series:['p50','p90','p99','max'].map(key => ({
        name:key,
        type:'bar',
        barMaxWidth:18,
        data:ubLifecycleLatencyMetrics.map(([, item]) => item[key] || 0),
        markLine:key === 'max' ? {symbol:'none', lineStyle:{color:'#dc2626',type:'dashed'}, label:{formatter:'20ms'}, data:[{xAxis:20}]} : undefined
      }))
    }) : noDataOption('No UB lifecycle data'));
    chart('ub-wr-count-chart', ubWrCountRows.length ? axisBase('UB WR / Inflight Count', {
      grid:wideHorizontalGrid(160,66,36),
      tooltip:{trigger:'axis',axisPointer:{type:'shadow'},confine:true,formatter:ps => ps.map(p => `${escapeHtml(p.seriesName)}: ${p.value} count`).join('<br>')},
      xAxis:{type:'value', name:'count'},
      yAxis:{type:'category', data:ubWrCountRows.map(r => r[0]), axisLabel:{width:150, overflow:'truncate'}},
      series:['p50','p90','p99','max'].map(key => ({
        name:key,
        type:'bar',
        barMaxWidth:18,
        data:ubWrCountRows.map(([, item]) => item[key] || 0)
      }))
    }) : noDataOption('No UB WR/inflight count data'));
  }
  function renderOperationViews() {
    renderTracePage();
    renderSelectedTrace();
  }
  function renderWorkerDependentViews() {
    renderWorkerSection('read', 'Read');
    renderWorkerSection('write', 'Write');
    renderUbWorkerViews();
    renderSectionSummaries();
  }
  function applyTraceFilters() {
    const query = document.getElementById('trace-search').value.trim().toLowerCase();
    const cls = document.getElementById('class-filter').value;
    const worker = document.getElementById('worker-filter').value;
    const requestStatus = document.getElementById('request-status-filter').value;
    filteredTraceRows = traceRows.filter(([traceId, item]) => {
      const haystack = [
        traceId,
        item.classification,
        JSON.stringify(item.errors || {}),
        Object.keys(item.workers || {}).join(' '),
        (item.evidence || []).map(e => e.text).join(' ')
      ].join(' ').toLowerCase();
      return (!cls || item.classification === cls)
        && (!worker || Object.keys(item.workers || {}).some(name => workerMatchesFilter(name, worker)))
        && statusMatchesTrace(item, requestStatus)
        && operationMatches(item)
        && (!query || haystack.includes(query));
    });
    if (!filteredTraceRows.some(([traceId]) => traceId === selectedTraceId)) {
      selectedTraceId = filteredTraceRows[0]?.[0] || null;
    }
  }
  function renderTracePage() {
    applyTraceFilters();
    function topTraceDisplayRows(rows) {
      return rows.map(([traceId,item]) => [
        traceStartTime(item),
        traceId,
        item.classification,
        traceAccessLatencyMs(item),
        JSON.stringify(item.errors || {}),
        pctText(item.access_latency_ms),
        Object.keys(item.workers || {}).slice(0, 6).map(workerRelationName).join(', ')
      ]);
    }
    const topTraceHeaders = ['time','trace','classification','latency ms','errors','access','workers'];
    const sortedDisplayRows = sortRowsForTable(topTraceDisplayRows(filteredTraceRows), topTraceSort, topTraceHeaders);
    if (!sortedDisplayRows.some(row => row[1] === selectedTraceId)) {
      selectedTraceId = sortedDisplayRows[0]?.[1] || null;
    }
    const totalPages = Math.max(Math.ceil(sortedDisplayRows.length / pageSize), 1);
    currentPage = Math.min(Math.max(currentPage, 0), totalPages - 1);
    const pageRows = sortedDisplayRows.slice(currentPage * pageSize, (currentPage + 1) * pageSize);
    renderTable('top-trace-table', topTraceHeaders, pageRows,
      row => `data-trace="${escapeHtml(row[1])}" class="${row[1] === selectedTraceId ? 'selected-row' : ''}"`,
      {
        skipSort:true,
        sortState:topTraceSort,
        onSort:index => {
          topTraceSort = nextSortState(topTraceSort, index);
          currentPage = 0;
          renderTracePage();
          renderSelectedTrace();
        }
      });
    document.querySelectorAll('#top-trace-table tbody tr').forEach(row => row.addEventListener('click', () => {
      selectedTraceId = row.getAttribute('data-trace');
      renderTracePage();
      renderSelectedTrace();
    }));
    document.getElementById('page-status').textContent = `第 ${currentPage + 1} / ${totalPages} 页，每页 ${pageSize} 条，共 ${sortedDisplayRows.length} 条 trace`;
    document.getElementById('prev-page').disabled = currentPage === 0;
    document.getElementById('next-page').disabled = currentPage >= totalPages - 1;
  }
  function renderSelectedTrace() {
    const item = traces[selectedTraceId] || {};
    function compactLatencySummary(summary) {
      return Object.entries(summary || {})
        .sort((a,b) => Number(b[1] || 0) - Number(a[1] || 0))
        .slice(0, 6)
        .map(([key, value]) => `${metricLabel(`latencySummary.${key}`)}=${Number(value / 1000).toFixed(3)}ms`)
        .join('; ');
    }
    const selectedTraceSummaryRows = [
      ['trace', selectedTraceId || ''],
      ['classification', item.classification || ''],
      ['errors', JSON.stringify(item.errors || {})],
      ['client access', pctText(item.access_latency_ms_by_role?.client)],
      ['worker access', pctText(item.access_latency_ms_by_role?.worker)],
      ['access all', pctText(item.access_latency_ms)],
      ['key latencySummary', compactLatencySummary(item.latency_summary_us)],
      ['coverage', JSON.stringify(item.evidence_coverage || {})],
      ['missing_evidence', JSON.stringify(item.missing_evidence || [])]
    ].filter(row => row[1]);
    renderTable('selected-trace-table', ['field','value'], selectedTraceSummaryRows,
      row => row[0] === 'key latencySummary' || /access/.test(row[0]) ? 'class="summaryrow"' : '');
    const visibleStageRows = (item.stage_breakdown || [])
      .filter(s => stageMatchesOperation(s.stage))
      .slice()
      .sort((a,b) => (a.duration_ms || 0) - (b.duration_ms || 0));
    const stageRows = visibleStageRows.filter(s => s.duration_ms !== undefined);
    renderTable('selected-stage-table', ['研发流程','duration ms','confidence','source'], visibleStageRows.map(s => [
      stageDetailText(s.stage),
      s.duration_ms === undefined ? 'missing' : s.duration_ms,
      s.confidence || '',
      s.source || ''
    ]), row => row[1] === 'missing' ? 'class="warnrow"' : `class="${severityClass(row[1])}"`);
    const stageColors = ['#2563eb','#ea580c','#059669','#7c3aed','#dc2626','#0891b2','#ca8a04','#64748b'];
    document.getElementById('selected-stage-legend').innerHTML = stageRows.map((stage, idx) =>
      `<span class="stage-pill"><i class="stage-dot" style="background:${stageColors[idx % stageColors.length]}"></i>${escapeHtml(stageDisplayName(stage.stage))}</span>`
    ).join('');
    chart('selected-trace-chart', stageRows.length ? axisBase('Selected Trace Stage Breakdown', {
      legend:{show:false},
      dataZoom:[],
      tooltip:{trigger:'axis',axisPointer:{type:'shadow'},formatter:ps => ps.filter(p => p.data && p.data.value != null).map(p => `研发流程: ${escapeHtml(p.data.display)}<br>耗时: ${p.data.value}ms<br>原始字段: ${escapeHtml(p.data.rawStage || '')}<br>观测来源: ${escapeHtml(p.data.source || '')}`).join('<br><br>')},
      grid:wideHorizontalGrid(128,44,30),
      xAxis:{type:'value', name:'ms'},
      yAxis:{type:'category', data:stageRows.map(s => stageDisplayName(s.stage)), axisLabel:{width:118, overflow:'truncate'}},
      series:[{
        name:'duration_ms',
        type:'bar',
        barMaxWidth:28,
        data:stageRows.map((row, rowIndex) => ({value:row.duration_ms, display:stageDisplayName(row.stage), rawStage:row.stage, source:row.source || '', itemStyle:{color:stageColors[rowIndex % stageColors.length]}})),
        label:{show:true, position:'right', formatter:p => p.data && p.data.value != null ? `${p.data.value}ms` : ''},
        markLine:{symbol:'none', lineStyle:{color:'#dc2626',type:'dashed'}, label:{formatter:'20ms deadline'}, data:[{xAxis:20}]}
      }]
    }) : noDataOption('No observed stage duration for selected operation'));
    document.getElementById('selected-trace-log').innerHTML = renderTraceLogBlocks(item.evidence || []);
  }
  document.getElementById('prev-page').addEventListener('click', () => { currentPage -= 1; renderTracePage(); });
  document.getElementById('next-page').addEventListener('click', () => { currentPage += 1; renderTracePage(); });
  document.getElementById('trace-search').addEventListener('input', () => { currentPage = 0; renderTracePage(); renderSelectedTrace(); });
  document.getElementById('request-status-filter').addEventListener('change', () => { currentPage = 0; renderTracePage(); renderSelectedTrace(); });
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
  function workerFilterOptions() {
    const options = new Map();
    traceWorkerNames.forEach(name => {
      const key = workerAggregateKey(name);
      if (!options.has(key)) options.set(key, workerAggregateLabel(key));
    });
    return [...options.entries()].sort((a,b) => a[1].localeCompare(b[1], 'zh-CN', {numeric:true}));
  }
  document.getElementById('worker-filter').innerHTML = '<option value="">全部 Worker</option>' +
    workerFilterOptions().map(([value, label]) => `<option value="${escapeHtml(value)}">${escapeHtml(label)}</option>`).join('');
  document.getElementById('worker-filter').addEventListener('change', () => { currentPage = 0; renderTracePage(); renderSelectedTrace(); });
  const workerScopedFilterIds = [
    'read-worker-filter',
    'read-worker-table-filter',
    'write-worker-filter',
    'write-worker-table-filter',
    'ub-worker-role-filter',
    'ub-worker-role-table-filter',
    'ub-worker-time-filter',
    'ub-worker-time-table-filter'
  ];
  workerScopedFilterIds.forEach(id => {
    const node = document.getElementById(id);
    if (!node) return;
    node.innerHTML = '<option value="">全部 Worker</option>' +
      workerFilterOptions().map(([value, label]) => `<option value="${escapeHtml(value)}">${escapeHtml(label)}</option>`).join('');
    node.addEventListener('change', renderWorkerDependentViews);
  });
  document.getElementById('trace-page-size').addEventListener('change', event => {
    pageSize = Number(event.target.value) || 4;
    currentPage = 0;
    renderTracePage();
    renderSelectedTrace();
  });
  document.getElementById('reset-filter').addEventListener('click', () => {
    document.getElementById('trace-search').value = '';
    document.getElementById('class-filter').value = '';
    document.getElementById('worker-filter').value = '';
    document.getElementById('operation-filter').value = '';
    document.getElementById('request-status-filter').value = '';
    document.getElementById('trace-page-size').value = '4';
    activeOperation = '';
    pageSize = 4;
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
  chart('classification-chart', classificationRows.length ? {title:{show:false}, color:palette, textStyle:chartTextStyle, tooltip:{trigger:'item', confine:true}, legend:{type:'scroll', bottom:0}, toolbox:{right:10,feature:{saveAsImage:{},dataView:{readOnly:true},restore:{}}}, series:[{type:'pie', radius:['38%','66%'], center:['50%','48%'], avoidLabelOverlap:true, data:classificationRows.map(([name,value]) => ({name,value}))}]} : noDataOption('No classification data'));
  chart('cohort-chart', cohortRows.length ? axisBase('Input Cohort Comparison', {xAxis:{type:'category', data:cohortRows.map(r => r[0]), axisLabel:{rotate:20, width:110, overflow:'truncate'}}, yAxis:{type:'value'}, series:[
    {name:'traces',type:'bar',barMaxWidth:42,data:cohortRows.map(r => r[1].trace_count || 0), label:{show:true, position:'top'}},
    {name:'errors',type:'bar',barMaxWidth:42,data:cohortRows.map(r => Object.values(r[1].errors || {}).reduce((a,b) => a+b, 0)), label:{show:true, position:'top'}, itemStyle:{color:'#dc2626'}}
  ]}) : noDataOption('No cohort data'));
  chart('error-chart', errorRows.length ? axisBase('Errors', {xAxis:{type:'category', data:errorRows.map(r => r[0]), axisLabel:{rotate:25, width:130, overflow:'truncate'}}, yAxis:{type:'value'}, series:[{type:'bar', barMaxWidth:46, data:errorRows.map(r => ({value:r[1], itemStyle:{color:'#dc2626'}})), label:{show:true, position:'top'}}]}) : noDataOption('No error data'));
  function renderFlowGraph(id, graph, title) {
    chart(id, {
    title:{show:false,text:title},
    textStyle:chartTextStyle,
    toolbox:{right:10,feature:{
      saveAsImage:{},
      dataView:{readOnly:true},
      restore:{},
      myAutoCenter:{
        show:true,
        title:'自适应居中',
        icon:'path://M512 128l96 96-48 48-16-16v160h160l-16-16 48-48 96 96-96 96-48-48 16-16H544v160l16-16 48 48-96 96-96-96 48-48 16 16V608H320l16 16-48 48-96-96 96-96 48 48-16 16h160V384H320l16 16-48 48-96-96 96-96 48 48-16 16h160V256l-16 16-48-48 96-96z',
        onclick:(ecModel, api) => autoCenterFlowGraph(api)
      }
    }},
    tooltip:{trigger:'item', formatter:p => p.dataType === 'edge'
      ? `${escapeHtml(p.data.name)}<br>${escapeHtml(p.data.summary || '')}<br>${escapeHtml(p.data.operation)}<br>${escapeHtml(p.data.reason || '')}<br>${escapeHtml(p.data.evidence || '')}`
      : `${escapeHtml(p.data.label || p.data.name)}<br>${escapeHtml((p.data.top_ips || []).join(', '))}`},
    series:[{
      type:'graph',
      layout:'none',
      roam:true,
      edgeSymbol:['none','arrow'],
      edgeSymbolSize:8,
      label:{show:true, fontSize:13},
      edgeLabel:{show:true, formatter:p => p.data.summary || (p.data.status === 'present' ? 'present' : 'missing'), fontSize:13, width:120, overflow:'break'},
      lineStyle:{width:2, color:'#64748b', curveness:.08},
      data:(graph.nodes || []).map((node, idx) => ({
        name:node.id,
        label:[node.label, ...(node.top_workers || []).map(workerRelationName), ...(node.top_ips || [])].slice(0, 3).join('\\n'),
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
  function fmtMs(value) {
    const n = Number(value);
    return Number.isFinite(n) ? `${Number(n.toFixed(3))}ms` : '未采样';
  }
  function fmtCount(value) {
    const n = Number(value);
    return Number.isFinite(n) ? String(n) : '0';
  }
  function firstEntry(rows, fallback=['无数据', null]) {
    return rows && rows.length ? rows[0] : fallback;
  }
  function topLatencyText(rows) {
    const [name, item] = firstEntry(rows);
    if (!item || !item.count) return '无有效时延样本';
    return `${metricLabel(name)} max=${fmtMs(item.max)}, p99=${fmtMs(item.p99)}`;
  }
  function topTimeBucketText(operation) {
    const rows = buildTraceTimeBuckets(operation);
    if (!rows.length) return '无时间桶样本';
    const top = rows.slice().sort((a,b) => Number(b.p99_access_ms || 0) - Number(a.p99_access_ms || 0))[0];
    const stage = Object.entries(top.stage_breakdown_ms || {})
      .sort((a,b) => Number(b[1]?.p99 || 0) - Number(a[1]?.p99 || 0))[0];
    const stageText = stage ? `${stageDisplayName(stage[0])} p99=${fmtMs(stage[1]?.p99)}` : '子阶段未采样';
    return `${String(top.bucket_start || '').replace('T',' ')} access p99=${fmtMs(top.p99_access_ms)}，${stageText}`;
  }
  function topWorkerText(rows) {
    const [worker, item] = firstEntry(rows);
    if (!item) return '无 worker 聚合样本';
    return `${item.display_worker || workerAggregateLabel(worker)}: traces=${fmtCount(item.trace_count)}, slow=${fmtCount(item.slow_trace_count)}, errors=${fmtCount(item.error_count)}`;
  }
  function topFlowStageText(graph) {
    const edge = (graph.edges || []).slice().sort((a,b) =>
      Number(b.rollup?.max_ms || 0) - Number(a.rollup?.max_ms || 0) ||
      Number(b.rollup?.count || 0) - Number(a.rollup?.count || 0)
    )[0];
    if (!edge) return '流程阶段证据不足';
    return `${edge.name}: ${edge.summary || edge.status || 'observed'}`;
  }
  function topUbMetricText() {
    const [name, item] = firstEntry(ubLifecycleLatencyMetrics);
    if (!item || !item.count) return 'UB 耗时字段未采样';
    return `${lifecycleLabel(name)} max=${fmtMs(item.max)}, p99=${fmtMs(item.p99)}`;
  }
  function topUbWorkerText() {
    const [worker, item] = firstEntry(ubWorkerRows);
    if (!item) return '无 UB worker 聚合样本';
    return `${worker}: role=${item.role || ''}, entry=${fmtCount(item.entry_events)}, exit=${fmtCount(item.exit_events)}, max=${fmtMs(item.latency_ms?.max)}`;
  }
  function topUbEdgeText() {
    const [edge, item] = firstEntry(ubRows);
    if (!item) return '无 UB edge 样本';
    return `${edge}: count=${fmtCount(item.count)}, p99=${fmtMs(item.latency_ms?.p99)}`;
  }
  function topTraceText() {
    const [traceId, item] = firstEntry(traceRows);
    if (!item) return '无 trace 样本';
    const workers = Object.keys(item.workers || {}).slice(0, 3).join(', ') || 'unknown worker';
    return `${String(item.first_ts || item.last_ts || '').replace('T',' ')} ${traceId}: ${item.classification || 'unknown'}, access=${fmtMs(traceAccessLatencyMs(item))}, workers=${workers}`;
  }
  function missingCoverageText() {
    const missing = coverageRows.filter(([, item]) => item.status !== 'present').map(([name]) => name).slice(0, 3);
    return missing.length ? `缺失观测面：${missing.join(', ')}` : '关键观测面已采样';
  }
  function highlightSummaryLatency(match, raw) {
    const value = Number(raw);
    const cls = Number.isFinite(value) && value >= 20 ? 'summary-hot' : Number.isFinite(value) && value >= 5 ? 'summary-warn' : 'summary-key';
    return `<span class="${cls}">${match}</span>`;
  }
  function highlightSummaryText(text) {
    return escapeHtml(text)
      .replace(/\\b(\\d+(?:\\.\\d+)?)ms\\b/g, highlightSummaryLatency)
      .replace(/(deadline|timeout|error|errors|失败|异常|异常耗时|异常时延|缺失|未采样|Top error)/gi, '<span class="summary-hot">$1</span>')
      .replace(/(瓶颈|热点|集中|Top trace|Top edge|access|p99|max|slow|worker|Worker|UB|URMA)/g, '<span class="summary-warn">$1</span>')
      .replace(/(主分类|读取|写入|流程|建议|原始 JSON|证据|时间|入口\\/出口)/g, '<span class="summary-key">$1</span>');
  }
  function summaryPointsHtml(summary) {
    const points = Array.isArray(summary?.points) ? summary.points : String(summary || '').split(/[；;]/).filter(Boolean);
    return `<b>本章结论：</b><ul class="summary-points">${points.map(point => `<li>${highlightSummaryText(point.replace(/[。.]$/, ''))}</li>`).join('')}</ul>`;
  }
  function setSectionSummary(id, summary) {
    const node = document.getElementById(id);
    if (node) node.innerHTML = summaryPointsHtml(summary);
  }
  function chapterSummaryTexts() {
    const topCohort = cohortRows.slice().sort((a,b) => Object.values(b[1].errors || {}).reduce((x,y)=>x+y,0) - Object.values(a[1].errors || {}).reduce((x,y)=>x+y,0))[0];
    const topCohortText = topCohort ? `${topCohort[0]} errors=${fmtCount(Object.values(topCohort[1].errors || {}).reduce((a,b)=>a+b,0))}` : '无 cohort 对比';
    return {
      s2: {points:[`主分类 ${topClass[0]}=${fmtCount(topClass[1])}`, `Top error ${errorRows[0]?.[0] || '无'}=${fmtCount(errorRows[0]?.[1])}`, topCohortText]},
      s3: {points:[`读取瓶颈 ${topLatencyText(latencyRowsForOperation('read'))}`, `写入瓶颈 ${topLatencyText(latencyRowsForOperation('write'))}`, `时间热点：读 ${topTimeBucketText('read')}，写 ${topTimeBucketText('write')}`]},
      s4: {points:[`流程热点：读 ${topFlowStageText(readFlowStages)}；写 ${topFlowStageText(writeFlowStages)}`, `Worker 集中：读 ${topWorkerText(workerRowsForOperation('read'))}`, `Worker 集中：写 ${topWorkerText(workerRowsForOperation('write'))}`]},
      s5: {points:[`UB 耗时 ${topUbMetricText()}`, `入口/出口集中 ${topUbWorkerText()}`, `Top edge ${topUbEdgeText()}`]},
      s6: {points:[`默认按接口 access 时延降序`, `Top trace ${topTraceText()}`, `建议先切换“失败/成功”和“读/写”筛选，再看原始日志块摘要`]},
      s7: {points:[recommendations[0]?.title || '暂无自动建议', missingCoverageText()]},
      s8: {points:[`原始 JSON 保留 ${fmtCount(Object.keys(traces).length)} 条 trace`, '可用于复现 parser 聚合和人工二次确认']}
    };
  }
  function renderChapterGuide(summaries) {
    const guideRows = [
      ['#s2', '2. 根因分布', summaries.s2, '图 2-1/2-2/2-3'],
      ['#s3', '3. 时延 Breakdown', summaries.s3, '图 3-1/3-3'],
      ['#s4', '4. Worker / 流程', summaries.s4, '图 4-1/4-2/4-3/4-4'],
      ['#s5', '5. UB / URMA', summaries.s5, '图 5-1/5-2/5-3/5-4/5-5/5-6'],
      ['#s6', '6. Trace 查看', summaries.s6, '表 6-1 / 图 6-1 / 日志框 6-3'],
      ['#s7', '7. 建议与口径', summaries.s7, '表 7-1/7-2']
    ];
    const node = document.getElementById('chapter-guide-list');
    if (!node) return;
    node.innerHTML = guideRows.map(([href, title, text, refs]) =>
      `<li><a href="${href}">${escapeHtml(title)}</a>${summaryPointsHtml(text)}<div class="small">${escapeHtml(refs)}</div></li>`
    ).join('');
  }
  function renderSectionSummaries() {
    const summaries = chapterSummaryTexts();
    setSectionSummary('section-summary-s2', summaries.s2);
    setSectionSummary('section-summary-s3', summaries.s3);
    setSectionSummary('section-summary-s4', summaries.s4);
    setSectionSummary('section-summary-s5', summaries.s5);
    setSectionSummary('section-summary-s6', summaries.s6);
    setSectionSummary('section-summary-s7', summaries.s7);
    setSectionSummary('section-summary-s8', summaries.s8);
    renderChapterGuide(summaries);
  }
  renderSectionSummaries();
  renderLatencySection('read', 'Read');
  renderLatencySection('write', 'Write');
  renderFlowSection('read', 'Read');
  renderFlowSection('write', 'Write');
  renderTimeBucketSection('read', 'Read');
  renderTimeBucketSection('write', 'Write');
  renderFlowGraph('read-flow-stage-chart', readFlowStages, 'Read Flow');
  renderFlowGraph('write-flow-stage-chart', writeFlowStages, 'Write Flow');
  renderUbLifecycleViews();
  renderUbWorkerViews();
  renderWorkerSection('read', 'Read');
  renderWorkerSection('write', 'Write');
  renderUbSection('read');
  renderUbSection('write');
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


class TraceRunStore:
    """Own run-directory cache, manifest, and artifact file operations."""

    def prepare_parse_run(self, inputs, out_dir, case_name, scenario, code_ref, force=False):
        out_root = Path(out_dir)
        out_root.mkdir(parents=True, exist_ok=True)
        cache_key, identities = _cache_key(inputs, code_ref, case_name, scenario)
        if not force:
            cached = _find_cached_run(out_root, cache_key)
            if cached:
                return {"run_dir": cached, "cached": True}
        run_dir, created_at = _new_run_dir(out_root, case_name, cache_key)
        _preserve_raw_inputs(inputs, run_dir)
        return {
            "run_dir": run_dir,
            "created_at": created_at,
            "cache_key": cache_key,
            "identities": identities,
            "cached": False,
        }

    def write_parse_outputs(self, run_dir, report, events, case_name, scenario, code_ref, created_at, cache_key, identities):
        manifest = {
            "schema_version": 1,
            "case_name": case_name,
            "scenario": scenario,
            "analysis_created_at": created_at.isoformat(),
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
        self.write_json(run_dir / "manifest.json", manifest)
        _write_inputs_doc(run_dir, manifest)
        (run_dir / "events.jsonl").write_text(
            "".join(json.dumps(event, ensure_ascii=False) + "\n" for event in events), encoding="utf-8"
        )
        self.write_json(run_dir / "parsed_traces.json", report)

    def read_json(self, path):
        return _read_json(path)

    def write_json(self, path, value):
        _write_json(path, value)

    def update_manifest(self, run_dir, updater):
        _update_manifest(run_dir, updater)

    def write_text(self, path, text):
        Path(path).write_text(text, encoding="utf-8")

    def write_site_publish_doc(self, run_dir, manifest):
        return _write_site_publish_doc(run_dir, manifest)


class TraceSitePublisher:
    """Publish site reports with size guard and live marker verification."""

    def __init__(self, store=None, pipeline_factory=None):
        self.store = store or TraceRunStore()
        self.pipeline_factory = pipeline_factory or TraceRunPipeline

    def size_guard(self, source_html, max_bytes=DEFAULT_SITE_HTML_MAX_BYTES):
        source_html = Path(source_html)
        size = source_html.stat().st_size
        return {
            "source_size_bytes": size,
            "max_site_html_bytes": max_bytes,
            "size_status": "ok" if size <= max_bytes else "too_large",
        }

    def publish(self, run_dir, dry_run=True, max_site_html_bytes=DEFAULT_SITE_HTML_MAX_BYTES):
        run_dir = Path(run_dir)
        manifest = self.store.read_json(run_dir / "manifest.json")
        site_target = manifest.get("render_targets", {}).get("site", {})
        if not (run_dir / site_target.get("path", "report.site.html")).exists():
            self.pipeline_factory(store=self.store, site_publisher=self).render_site(run_dir)
            manifest = self.store.read_json(run_dir / "manifest.json")
            site_target = manifest.get("render_targets", {}).get("site", {})
        if not (run_dir / site_target.get("publish_doc", "site_publish.md")).exists():
            publish_doc = self.store.write_site_publish_doc(run_dir, manifest)
            site_target = {**site_target, **publish_doc}
        source_html = run_dir / site_target.get("path", "report.site.html")
        size_guard = self.size_guard(source_html, max_site_html_bytes)
        live_markers = "not-run"
        if dry_run:
            status = "dry-run"
        elif size_guard["size_status"] != "ok":
            status = "blocked-size-limit"
            self._record_publish(run_dir, site_target, status, live_markers, size_guard)
            raise SystemExit(
                f"Refuse to publish oversized site HTML: {source_html} is "
                f"{size_guard['source_size_bytes']} bytes > {max_site_html_bytes} bytes. "
                "Review the report or raise --max-site-html-mb intentionally."
            )
        else:
            self._copy_and_verify(source_html, site_target)
            live_markers = "verified"
            status = "published"
        self._record_publish(run_dir, site_target, status, live_markers, size_guard)
        return site_target.get("url", "")

    def _copy_and_verify(self, source_html, site_target):
        publish_config = _site_publish_config(require_target=True)
        target_path = site_target.get("target_path", "")
        url = site_target.get("url", "")
        if target_path.startswith("<publish-root>"):
            filename = Path(target_path).name
            target_path = f"{publish_config['root']}/{filename}"
        subprocess.run(["scp", str(source_html), f"{publish_config['host']}:{target_path}"], check=True)
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

    def _record_publish(self, run_dir, site_target, status, live_markers, size_guard):
        self.store.update_manifest(run_dir, lambda item: item["render_targets"]["site"].update({
            **site_target,
            "publish": {
                "status": status,
                "url": site_target.get("url", ""),
                "target_path": site_target.get("target_path", ""),
                "live_markers": live_markers,
                **size_guard,
            },
        }))


class TraceRunPipeline:
    """Manage staged run directories, cache, manifest state, and render targets."""

    def __init__(self, analyzer=None, renderer=None, store=None, site_publisher=None):
        self.analyzer = analyzer or TraceAnalyzer()
        self.renderer = renderer or TraceReportRenderer()
        self.store = store or TraceRunStore()
        self.site_publisher = site_publisher or TraceSitePublisher(self.store)

    def parse(self, inputs, out_dir, case_name="trace-case", scenario="", code_ref="unknown", force=False,
              allow_partial_inputs=False):
        prepared = self.store.prepare_parse_run(inputs, out_dir, case_name, scenario, code_ref, force=force)
        if prepared["cached"]:
            return prepared["run_dir"]
        run_dir = prepared["run_dir"]
        report = self.analyzer.analyze(inputs, code_ref=code_ref, allow_partial_inputs=allow_partial_inputs)
        events = self.renderer.events(report)
        self.store.write_parse_outputs(
            run_dir, report, events, case_name, scenario, code_ref,
            prepared["created_at"], prepared["cache_key"], prepared["identities"],
        )
        return run_dir

    def aggregate(self, run_dir):
        run_dir = Path(run_dir)
        report = self.store.read_json(run_dir / "parsed_traces.json")
        self.store.write_json(run_dir / "summary.json", report)
        self.store.update_manifest(run_dir, lambda manifest: manifest["stages"].update({
            "aggregate": {"status": "done", "path": "summary.json"}
        }))
        return run_dir / "summary.json"

    def triage(self, run_dir):
        run_dir = Path(run_dir)
        report = self.store.read_json(run_dir / "summary.json")
        triage = self.renderer.triage(report)
        self.store.write_json(run_dir / "triage.json", triage)
        self.store.write_text(run_dir / "triage.md", self.renderer.markdown(report))
        self.store.update_manifest(run_dir, lambda manifest: manifest["stages"].update({
            "triage": {"status": "done", "path": "triage.json", "markdown": "triage.md"}
        }))
        return run_dir / "triage.json"

    def render_local(self, run_dir):
        run_dir = Path(run_dir)
        report = self.store.read_json(run_dir / "summary.json")
        manifest = self.store.read_json(run_dir / "manifest.json")
        title = f"Trace Triage: {manifest.get('case_name', 'trace-case')}"
        self.store.write_text(run_dir / "report.local.html", self.renderer.html(report, title, manifest=manifest))
        self.store.update_manifest(run_dir, lambda item: item["render_targets"].update({
            "local": {"path": "report.local.html", "status": "generated"}
        }))
        return run_dir / "report.local.html"

    def render_site(self, run_dir):
        run_dir = Path(run_dir)
        report = self.store.read_json(run_dir / "summary.json")
        manifest = self.store.read_json(run_dir / "manifest.json")
        title = f"Trace Triage: {manifest.get('case_name', 'trace-case')}"
        self.store.write_text(run_dir / "report.site.html",
                              self.renderer.html(report, title, site=True, manifest=manifest))
        publish_doc = self.store.write_site_publish_doc(run_dir, manifest)
        self.store.update_manifest(run_dir, lambda item: item["render_targets"].update({
            "site": {"path": "report.site.html", "status": "generated", **publish_doc}
        }))
        return run_dir / "report.site.html"

    def run(self, inputs, out_dir, case_name="trace-case", scenario="", code_ref="unknown", force=False,
            allow_partial_inputs=False):
        run_dir = self.parse(inputs, out_dir, case_name=case_name, scenario=scenario,
                             code_ref=code_ref, force=force, allow_partial_inputs=allow_partial_inputs)
        if not (run_dir / "summary.json").exists():
            self.aggregate(run_dir)
        if not (run_dir / "triage.json").exists():
            self.triage(run_dir)
        if not (run_dir / "report.local.html").exists():
            self.render_local(run_dir)
        if not (run_dir / "report.site.html").exists():
            self.render_site(run_dir)
        return run_dir


def parse_stage(inputs, out_dir, case_name="trace-case", scenario="", code_ref="unknown", force=False,
                allow_partial_inputs=False):
    return TraceRunPipeline().parse(inputs, out_dir, case_name=case_name, scenario=scenario,
                                    code_ref=code_ref, force=force, allow_partial_inputs=allow_partial_inputs)


def aggregate_stage(run_dir):
    return TraceRunPipeline().aggregate(run_dir)


def triage_stage(run_dir):
    return TraceRunPipeline().triage(run_dir)


def render_local_stage(run_dir):
    return TraceRunPipeline().render_local(run_dir)


def render_site_stage(run_dir):
    return TraceRunPipeline().render_site(run_dir)


def _publish_size_guard(source_html, max_bytes=DEFAULT_SITE_HTML_MAX_BYTES):
    return TraceSitePublisher().size_guard(source_html, max_bytes)


def publish_site_stage(run_dir, dry_run=True, max_site_html_bytes=DEFAULT_SITE_HTML_MAX_BYTES):
    return TraceSitePublisher().publish(run_dir, dry_run=dry_run, max_site_html_bytes=max_site_html_bytes)


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


def run_pipeline(inputs, out_dir, case_name="trace-case", scenario="", code_ref="unknown", force=False,
                 allow_partial_inputs=False):
    return TraceRunPipeline().run(inputs, out_dir, case_name=case_name, scenario=scenario,
                                  code_ref=code_ref, force=force, allow_partial_inputs=allow_partial_inputs)


def _make_self_test_bundle(path):
    trace_id = "019f7b27-56f0-74f0-9a68-5b3742f11e23"
    content = "\n".join([
        f"2026-07-18T19:20:03.100000 | INFO | access_recorder | 192.0.2.10 | 42 | {trace_id} | - | 0 | DS_KV_CLIENT_GET | 518923 | 4096",
        f"2026-07-18T19:20:03.110000 | INFO | client | 192.0.2.10 | 42 | {trace_id} | Get done latencySummary:{{client.rpc.get:20298, client.process.get:10}}",
        f"2026-07-18T19:20:03.130000 | INFO | worker | 192.0.2.10 | 42 | {trace_id} | [Get] Done, totalCost: 518.9ms, exceed 3ms: {{ ProcessGetObjectRequest: 517 ms, QueryMeta: 0 ms }}",
        f"2026-07-18T19:20:03.150000 | WARN | worker | 192.0.2.10 | 42 | {trace_id} | [ZMQ_RPC_FRAMEWORK_SLOW] e2e_us=8012 client_req_framework_us=100 remote_processing_us=7600 client_rsp_framework_us=120 server_req_queue_us=20 server_exec_us=7500 server_rsp_queue_us=80 network_residual_us=292 method=WorkerOCService.Get",
        f"2026-07-18T19:20:03.200000 | WARN | worker | 192.0.2.20 | 42 | {trace_id} | [URMA_ELAPSED_TOTAL] cost 517.732ms, request id:77, src address: 192.0.2.20, target address: 192.0.2.10, dataSize:4194304, cpuid:12, status: OK",
        f"2026-07-18T19:20:03.201000 | WARN | worker | 192.0.2.20 | 42 | {trace_id} | [URMA_ELAPSED_POLL_JFC] cost 0.309ms, request id:77",
        f"2026-07-18T19:20:03.202000 | WARN | worker | 192.0.2.20 | 42 | {trace_id} | [URMA_ELAPSED_NOTIFY] cost 0.041ms, request id:77",
        f"2026-07-18T19:20:03.203000 | WARN | worker | 192.0.2.20 | 42 | {trace_id} | [URMA_ELAPSED_THREAD_SHED] cost 12.500ms, request id:77",
        f"2026-07-18T19:20:03.230000 | ERROR | worker | 192.0.2.10 | 42 | {trace_id} | RPC deadline exceeded while waiting WorkerOCService.Get",
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
            parser.add_argument("--allow-partial-inputs", action="store_true",
                                help="Continue when some inputs cannot be read; failures are recorded in the report.")
            args = parser.parse_args(argv)
            print(parse_stage(args.inputs, args.out, case_name=args.case, scenario=args.scenario,
                              code_ref=args.code_ref, force=args.force,
                              allow_partial_inputs=args.allow_partial_inputs))
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
        parser.add_argument("--allow-partial-inputs", action="store_true",
                            help="Continue when some inputs cannot be read; failures are recorded in the report.")
        args = parser.parse_args(argv)
        run_dir = run_pipeline(args.inputs, args.out, case_name=args.case, scenario=args.scenario,
                               code_ref=args.code_ref, force=args.force,
                               allow_partial_inputs=args.allow_partial_inputs)
        print(run_dir)
        return 0

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("inputs", nargs="*", help="Log files, directories, or gzip-wrapped tar bundles.")
    parser.add_argument("--code-ref", default="unknown", help="Source ref used for CodeGraph/source validation.")
    parser.add_argument("--allow-partial-inputs", action="store_true",
                        help="Continue when some inputs cannot be read; failures are recorded in the report.")
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
        report = analyze_inputs(args.inputs, code_ref=args.code_ref, allow_partial_inputs=args.allow_partial_inputs)

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
