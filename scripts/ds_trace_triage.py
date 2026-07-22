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
import logging
import os
import re
import shutil
import statistics
import subprocess
import sys
import tarfile
import tempfile
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


_LOG = logging.getLogger("ds_trace_triage")


class TraceTriageError(Exception):
    """CLI-facing fatal error."""


@dataclass(frozen=True)
class UbLineContext:
    source: str
    member: str
    line_no: int
    line: str
    ts: Any
    worker: str


@dataclass(frozen=True)
class RunOptions:
    case_name: str = "trace-case"
    scenario: str = ""
    code_ref: str = "unknown"
    force: bool = False
    allow_partial_inputs: bool = False


def _resolve_run_options(options=None, legacy_args=(), legacy_kwargs=None):
    if isinstance(options, RunOptions):
        if legacy_args or legacy_kwargs:
            raise TypeError("RunOptions cannot be combined with legacy run arguments")
        return options

    names = ("case_name", "scenario", "code_ref", "force", "allow_partial_inputs")
    values = dict(zip(names, ("trace-case", "", "unknown", False, False)))
    positional = (() if options is None else (options,)) + tuple(legacy_args)
    if len(positional) > len(names):
        raise TypeError(f"expected at most {len(names)} legacy run arguments")

    assigned = set()
    for name, value in zip(names, positional):
        values[name] = value
        assigned.add(name)
    for name, value in (legacy_kwargs or {}).items():
        if name not in values:
            raise TypeError(f"unexpected run option: {name}")
        if name in assigned:
            raise TypeError(f"multiple values for run option: {name}")
        values[name] = value
    return RunOptions(**values)


@dataclass
class ParseOutputBundle:
    report: Any
    events: Any
    created_at: Any
    cache_key: str
    identities: Any


@dataclass
class AnalyzerDeps:
    reader: Any = None
    parser: Any = None
    rules: Any = None


TRACE_ID_RE = re.compile(r"\b[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\b", re.I)
TS_RE = re.compile(r"(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?)")
POD_NAME_RE = re.compile(
    r"(kv[A-Za-z0-9_.-]*(?:client|worker)-\d+-(?:worker\d+|master)(?:_\d+)?)",
    re.I,
)
WORKER_POD_NAME_RE = re.compile(r"^kv[A-Za-z0-9_.-]*worker-\d+-(?:worker\d+|master)(?:_\d+)?$", re.I)
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
RPC_SLOW_RE = re.compile(r"\[(?:(?:ZMQ|BRPC)_)?RPC_FRAMEWORK_SLOW\].*?(?:method=|method:)\s*([A-Za-z0-9_.:/-]+)")
RPC_SLOW_FIELD_RE = re.compile(
    r"\b(e2e_us|client_req_framework_us|remote_processing_us|client_rsp_framework_us|"
    r"server_req_queue_us|server_exec_us|server_rsp_queue_us|network_residual_us)=(\d+)"
)
LATENCY_SUMMARY_RE = re.compile(r"latencySummary:\{([^}]*)\}")
SUMMARY_ITEM_RE = re.compile(r"([A-Za-z][A-Za-z0-9_.-]*)\s*:\s*(\d+)")
URMA_TOTAL_RE = re.compile(r"\[URMA_ELAPSED_TOTAL\].*?(?:total\s+)?cost\s+([\d.]+)ms", re.I)
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
REMOTE_GET_REQUEST_RE = re.compile(
    r"Remote get request:\[([^\]]+)\]\s+object:\[([^\]]*)\].*?offset\[(\d+)\]\s+size\[(\d+)\]",
    re.I,
)
RPC_MAX_CONCURRENCY_ERROR = "RPC max concurrency reached"
RPC_MAX_CONCURRENCY_RE = re.compile(
    r"RPC failed,\s*error_code=2004,\s*error_text=Reached server's max_concurrency",
    re.I,
)
ERROR_PATTERNS = [
    "RPC deadline exceeded",
    RPC_MAX_CONCURRENCY_ERROR,
    "URMA_WAIT_TIMEOUT",
    "K_NOT_FOUND",
    "Object in use",
    "Key not found",
    "Etcd is abnormal",
    "fallback payload rejected",
]
BUILTIN_CUSTOM_METRIC_RULES = [
    {
        "name": "wlock_wait",
        "pattern": r"\bWLock\b.{0,80}?(?:cost|elapsed|time|耗时)[:=\s]*([\d.]+)\s*(us|ms)",
        "value_group": 1,
        "unit_group": 2,
    },
]


class ParserRules:
    """Mutable parser extension rules for one analyzer instance."""

    def __init__(self, error_patterns=None, custom_metric_rules=None):
        self.error_patterns = list(error_patterns or ERROR_PATTERNS)
        metric_rules = BUILTIN_CUSTOM_METRIC_RULES if custom_metric_rules is None else custom_metric_rules
        self.custom_metric_rules = [
            {
                "name": rule["name"],
                "regex": rule["regex"] if "regex" in rule else re.compile(rule["pattern"], re.I),
                "value_group": rule.get("value_group", 1),
                "unit_group": rule.get("unit_group"),
            }
            for rule in metric_rules
        ]

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

    def fingerprint(self):
        payload = {
            "errors": sorted(self.error_patterns),
            "metrics": sorted([
                {
                    "name": rule["name"],
                    "pattern": rule["regex"].pattern,
                    "value_group": rule["value_group"],
                    "unit_group": rule["unit_group"],
                }
                for rule in self.custom_metric_rules
            ], key=lambda item: (item["name"], item["pattern"], item["value_group"], item["unit_group"] or "")
            ),
        }
        raw = json.dumps(payload, sort_keys=True, ensure_ascii=False).encode("utf-8")
        return hashlib.sha256(raw).hexdigest()[:16]


DEFAULT_RULES = ParserRules()


class UrmaFieldParser:
    """Extract canonical URMA fields from evolving log field names."""

    STRING_FIELDS = {
        "request_id": [
            REQUEST_ID_RE,
            re.compile(r"\breq(?:uest)?Id[:=]\s*([A-Za-z0-9_-]+)", re.I),
        ],
        "src_addr": [
            SRC_ADDR_RE,
            re.compile(r"\b(?:source|src) address:\s*([^\s,]+)", re.I),
        ],
        "target_addr": [
            DST_ADDR_RE,
            re.compile(r"\b(?:target|dst|destination) address:\s*([^\s,]+)", re.I),
        ],
        "status": [
            STATUS_RE,
            re.compile(r"\bstatusCode[:=]\s*([^,\s]+)", re.I),
        ],
        "src_chip_inflight": [
            SRC_CHIP_INFLIGHT_RE,
            re.compile(r"\b(?:src)?chipInflight:\s*(\{[^}]*\})", re.I),
        ],
    }
    INT_FIELDS = {
        "data_size": [
            DATA_SIZE_RE,
            re.compile(r"\b(?:dataSize|payloadSize|data_size)[:=]\s*(\d+)", re.I),
        ],
        "cpuid": [
            CPUID_RE,
            re.compile(r"\b(?:cpuId|cpu_id|cpuid)[:=]\s*(\d+)", re.I),
        ],
        "urma_inflight_wr_count": [
            INFLIGHT_WR_RE,
            re.compile(r"\b(?:urma_inflight_wr_count|inflightWrCount|wrInflightCount)[:=]\s*(\d+)", re.I),
        ],
    }
    FLOAT_FIELDS = {
        "wait_os_sched_ms": [
            WAIT_OS_RE,
            re.compile(r"\b(?:osSchedWaitMs|waitForMs|wait_for_ms)[:=]\s*([\d.]+)\s*ms?", re.I),
        ],
        "wake_sched_latency_us": [
            WAKE_SCHED_RE,
            re.compile(r"\b(?:wakeSchedLatencyUs|wake_sched_latency_us|wakeLatencyUs)[:=]\s*([\d.]+)", re.I),
        ],
    }

    def enrich_base_event(self, event, line):
        for field, regexes in self.STRING_FIELDS.items():
            value = self._first(regexes, line)
            if value:
                event[field] = value
        for field, regexes in self.INT_FIELDS.items():
            value = self._int(regexes, line)
            if value is not None:
                event[field] = value
        return event

    def enrich_total_event(self, event, line):
        self.enrich_base_event(event, line)
        for field, regexes in self.FLOAT_FIELDS.items():
            value = self._float(regexes, line)
            if value is not None:
                event[field] = value
        return event

    @staticmethod
    def _first(regexes, line):
        for regex in regexes:
            match = regex.search(line)
            if not match:
                continue
            for group in match.groups():
                if group:
                    return group.strip()
        return None

    @staticmethod
    def _int(regexes, line):
        raw = UrmaFieldParser._first(regexes, line)
        return int(raw) if raw is not None else None

    @staticmethod
    def _float(regexes, line):
        raw = UrmaFieldParser._first(regexes, line)
        return float(raw) if raw is not None else None


URMA_FIELDS = UrmaFieldParser()


class TraceInputReader:
    """Read log lines from files, directories, gzip files, and tar bundles."""

    def __init__(self):
        self.failures = []

    def iter_lines(self, paths):
        self.failures.clear()
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
                    member_count = 0
                    total_bytes = 0
                    for member in tar.getmembers():
                        if not member.isfile():
                            continue
                        member_count += 1
                        total_bytes = _check_tar_budget(path, member, member_count, total_bytes)
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

    @staticmethod
    def worker_from(source, member, line):
        for text in (member, source, line):
            m = POD_NAME_RE.search(text)
            if m:
                return m.group(1)
        parts = [p.strip() for p in line.split(" | ")]
        if len(parts) > 3 and parts[3]:
            return parts[3]
        return "unknown"

    @staticmethod
    def timestamp(line):
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
        ub_ctx = UbLineContext(source, member, line_no, line, ts, worker)
        parsed = {
            "trace_id": match.group(0),
            "worker": worker,
            "timestamp": ts,
            "evidence": {
                "source": source,
                "member": member,
                "line": line_no,
                "worker": worker,
                "host_ip": _line_host_ip(line),
                "text": line,
            },
            "ub_events": _extract_ub_events(ub_ctx),
            "errors": [],
            "custom_metrics_ms": {},
        }
        for rule in self.rules.custom_metric_rules:
            cm = rule["regex"].search(line)
            if cm:
                unit = cm.group(rule["unit_group"]) if rule["unit_group"] else "ms"
                parsed["custom_metrics_ms"][rule["name"]] = _ms(cm.group(rule["value_group"]), unit)
        if RPC_MAX_CONCURRENCY_RE.search(line):
            parsed["errors"].append(RPC_MAX_CONCURRENCY_ERROR)
        for pattern in self.rules.error_patterns:
            if pattern in line:
                parsed["errors"].append(pattern)
        parsed["errors"] = list(dict.fromkeys(parsed["errors"]))
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


def _iter_input_leaf_paths(paths):
    for raw_path in paths:
        path = Path(raw_path)
        if path.is_dir():
            for root, _, files in os.walk(path):
                for name in files:
                    yield Path(root) / name
        else:
            yield path


def _duplicate_input_basenames(paths):
    counts = Counter(path.name for path in _iter_input_leaf_paths(paths))
    return {name for name, count in counts.items() if name and count > 1}


def _source_cohort_label(source, member, noise_cohort_mode=False, duplicate_basenames=None):
    text = f"{_noise_context_for_path(source)}/{member}"
    if _is_noise_off(text):
        return NOISE_OFF_LABEL
    if _is_noise_on(text):
        return NOISE_ON_LABEL
    if noise_cohort_mode:
        return NOISE_OFF_LABEL
    path = Path(source)
    if path.name in (duplicate_basenames or set()):
        return "/".join(part for part in (path.parent.name, path.name) if part)
    return path.name or str(path)


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


def _line_host_ip(line):
    parts = [p.strip() for p in line.split(" | ")]
    if len(parts) > 3 and re.fullmatch(r"\d{1,3}(?:\.\d{1,3}){3}(?::\d+)?", parts[3]):
        return parts[3]
    return None


def _ms(raw_value, unit):
    value = float(raw_value)
    return value / 1000.0 if (unit or "ms").lower() == "us" else value


def _check_tar_budget(path, member, member_count, total_bytes):
    if member_count > DEFAULT_MAX_TAR_MEMBERS:
        raise ValueError(f"Tar member count exceeds {DEFAULT_MAX_TAR_MEMBERS}: {path}")
    if member.size > DEFAULT_MAX_TAR_MEMBER_BYTES:
        raise ValueError(f"Tar member too large: {member.name}")
    total_bytes += member.size
    if total_bytes > DEFAULT_MAX_TAR_TOTAL_BYTES:
        raise ValueError(f"Tar extracted bytes exceed {DEFAULT_MAX_TAR_TOTAL_BYTES}: {path}")
    return total_bytes


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


def _ub_base_event(event_type, ctx: UbLineContext):
    event = {
        "event_type": event_type,
        "timestamp": ctx.ts.isoformat() if ctx.ts else None,
        "worker": ctx.worker,
        "source": ctx.source,
        "member": ctx.member,
        "line": ctx.line_no,
        "raw": ctx.line,
    }
    return URMA_FIELDS.enrich_base_event(event, ctx.line)


def _extract_ub_events(ctx: UbLineContext):
    events = []
    line = ctx.line
    transfer = TRANSFER_PATH_RE.search(line)
    if transfer:
        event = _ub_base_event("transfer_path", ctx)
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
        event = _ub_base_event("remote_get_start", ctx)
        event.update({
            "request_id": request_id,
            "object_key": object_key,
            "offset": int(offset),
            "read_size": int(read_size),
        })
        events.append(event)

    total = URMA_TOTAL_RE.search(line)
    if total:
        event = _ub_base_event("total", ctx)
        event["cost_ms"] = float(total.group(1))
        URMA_FIELDS.enrich_total_event(event, line)
        events.append(event)

    loop_gap = URMA_THREAD_LOOP_GAP_RE.search(line)
    if loop_gap:
        event = _ub_base_event("thread_sched", ctx)
        event["thread_sched_kind"] = "poll_loop_gap"
        event["last_poll_end_to_start_us"] = float(loop_gap.group(1))
        event["last_poll_start_to_start_us"] = float(loop_gap.group(2))
        event["cost_ms"] = event["last_poll_end_to_start_us"] / 1000.0
        events.append(event)

    for event_type, regex in (("poll_jfc", URMA_POLL_RE), ("notify", URMA_NOTIFY_RE), ("thread_sched", URMA_THREAD_RE)):
        match = regex.search(line)
        if match:
            event = _ub_base_event(event_type, ctx)
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
    if trace["errors"].get(RPC_MAX_CONCURRENCY_ERROR):
        return "rpc_max_concurrency"
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
    has_data = any(
        event["event_type"] in ("total", "poll_jfc", "notify", "thread_sched")
        for event in trace["ub_events"]
    )
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
        self.duplicate_cohort_basenames = _duplicate_input_basenames(paths)
        self.traces = defaultdict(self._new_trace)
        self.all_ts = []
        self.worker_counts = Counter()
        self.worker_ip_counts = defaultdict(Counter)
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
        source_label = _source_cohort_label(
            evidence["source"], evidence["member"], self.noise_cohort_mode, self.duplicate_cohort_basenames
        )
        trace["input_sources"][source_label] += 1
        trace["source_stats"][source_label]["line_count"] += 1
        worker = parsed["worker"]
        trace["workers"][worker] += 1
        trace["source_stats"][source_label]["workers"][worker] += 1
        self.worker_counts[worker] += 1
        host_ip = evidence.get("host_ip")
        if host_ip and WORKER_POD_NAME_RE.fullmatch(worker):
            self.worker_ip_counts[worker][host_ip] += 1
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
            "worker_ip_counts": self.worker_ip_counts,
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


class TraceDimensionSections:
    """Build derived report sections from accumulated trace rows."""

    @staticmethod
    def build_coverage(surface_counts):
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
    def build_cohorts(trace_rows):
        return _build_cohorts(trace_rows)

    @staticmethod
    def build_diagnosis(errors, classifications, access_latencies, coverage, cohorts):
        return _build_diagnosis(errors, classifications, access_latencies, coverage, cohorts)

    @staticmethod
    def build_recommendations(classifications, coverage, cohorts, ub_summary):
        return _build_recommendations(classifications, coverage, cohorts, ub_summary)

    @staticmethod
    def build_source_appendix(coverage):
        return _build_source_appendix(coverage)

    @staticmethod
    def build_flow_stages(coverage, flow_counts, ub_summary, trace_rows):
        return _build_flow_stages(coverage, flow_counts, ub_summary, trace_rows)

    @staticmethod
    def build_time_buckets(trace_rows):
        return {"1000ms": _build_time_buckets(trace_rows, 1000),
                "10000ms": _build_time_buckets(trace_rows, 10000)}

    @staticmethod
    def build_ub_worker_summary(trace_rows):
        return _build_ub_worker_summary(trace_rows)

    @staticmethod
    def build_ub_lifecycle_summary(trace_rows):
        return _build_ub_lifecycle_summary(trace_rows)


class TraceDimensionBuilder:
    """Convert accumulated trace facts into the stable report schema."""

    def __init__(self, sections=None):
        self.sections = sections or TraceDimensionSections()

    def build(self, snapshot, paths, code_ref="unknown", input_failures=None):
        trace_rows = snapshot["trace_rows"]
        surface_counts = snapshot["surface_counts"]
        coverage = self.sections.build_coverage(surface_counts)
        cohorts = self.sections.build_cohorts(trace_rows)
        diagnosis = self.sections.build_diagnosis(
            errors=snapshot["errors"],
            classifications=snapshot["classifications"],
            access_latencies=snapshot["access_latencies"],
            coverage=coverage,
            cohorts=cohorts,
        )
        recommendations = self.sections.build_recommendations(
            classifications=snapshot["classifications"],
            coverage=coverage,
            cohorts=cohorts,
            ub_summary=snapshot["ub_summary"],
        )
        time_buckets = self.sections.build_time_buckets(trace_rows)
        ub_worker_summary = self.sections.build_ub_worker_summary(trace_rows)
        ub_lifecycle_summary = self.sections.build_ub_lifecycle_summary(trace_rows)
        flow_stages = self.sections.build_flow_stages(
            coverage, snapshot["flow_counts"], snapshot["ub_summary"], trace_rows
        )
        return {
            "schema_version": 1,
            "code_ref": code_ref,
            "inputs": [str(p) for p in paths],
            "trace_count": len(trace_rows),
            "dimensions": {
                "time": self._build_time_range(snapshot["all_ts"]),
                "time_buckets": time_buckets,
                "workers": {k: {"line_count": v} for k, v in snapshot["worker_counts"].most_common()},
                "worker_summary": snapshot["worker_summary"],
                "worker_ip_mapping": self._build_worker_ip_mapping(snapshot["worker_ip_counts"]),
                "ub_worker_summary": ub_worker_summary,
                "ub_lifecycle_summary": ub_lifecycle_summary,
                "cohorts": cohorts,
                "input_failures": input_failures or [],
                "worker_edges": snapshot["worker_edges"],
                "coverage": coverage,
                "diagnosis": diagnosis,
                "recommendations": recommendations,
                "source_appendix": self.sections.build_source_appendix(coverage),
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
    def _build_rpc_slow(rpc_slow):
        return {
            k: {
                "count": v["count"],
                **{field: _percentiles(vals) for field, vals in sorted(v["fields_us"].items())},
            }
            for k, v in sorted(rpc_slow.items())
        }

    @staticmethod
    def _build_worker_ip_mapping(worker_ip_counts):
        assigned_ips = set()
        rows = []
        worker_candidates = sorted(
            worker_ip_counts.items(),
            key=lambda item: (-sum(item[1].values()), item[0]),
        )
        for worker, ip_counts in worker_candidates:
            candidates = sorted(ip_counts.items(), key=lambda item: (-item[1], item[0]))
            pod_ip = next((ip for ip, _count in candidates if ip not in assigned_ips), None)
            if not pod_ip:
                continue
            assigned_ips.add(pod_ip)
            short_match = re.search(r"(?:^|-)worker(\d+)(?:_|$)", worker, re.I)
            short_name = f"worker {short_match.group(1)}" if short_match else "master"
            rows.append({
                "worker_full_name": worker,
                "worker_short_name": short_name,
                "pod_ip": pod_ip,
            })
        return sorted(rows, key=lambda item: item["worker_full_name"])

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
        self.reader.failures.clear()
        self.accumulator = self.accumulator_cls(paths)
        for source, member, line_no, line in self.reader.iter_lines(paths):
            parsed = self.parser.parse_line(source, member, line_no, line)
            if parsed:
                self.accumulator.ingest(parsed, line)
        if self.reader.failures and not allow_partial_inputs:
            failures = "; ".join(f"{item['path']}:{item['error']}" for item in self.reader.failures[:5])
            raise TraceTriageError(
                f"Failed to read trace input(s): {failures}. "
                "Use --allow-partial-inputs for best-effort analysis."
            )
        snapshot = self.accumulator.finish()
        try:
            return self.dimension_builder.build(
                snapshot, paths, code_ref=code_ref, input_failures=self.reader.failures
            )
        except TypeError as exc:
            message = str(exc)
            if "input_failures" not in message and "unexpected keyword" not in message:
                raise
            return self.dimension_builder.build(snapshot, paths, code_ref=code_ref)


def _analyze_inputs(paths, options=None, deps=None):
    options = options or RunOptions(code_ref="unknown")
    deps = deps or AnalyzerDeps()
    return TraceAnalyzer(reader=deps.reader, parser=deps.parser, rules=deps.rules).analyze(
        paths, code_ref=options.code_ref, allow_partial_inputs=options.allow_partial_inputs
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
            "detail": (
                "先分别比较每个输入包的 trace_count、errors、classifications、access latency "
                "和 top workers，再判断是否属于有无底噪差异或同源残留问题。"
            ),
        })
    if ub_summary.get("transfer_path") or ub_summary.get("edges"):
        recommendations.append({
            "category": "ub_urma",
            "title": "UB/URMA 按 write/wait/notify 时序继续定界",
            "detail": (
                "结合 transfer path、src->target edge、URMA total、poll JFC、notify、thread scheduling、"
                "dataSize、cpuid、inflight 判断，不要只凭 URMA total 单字段归因。"
            ),
        })
    if classifications.get("client_deadline_20ms") or classifications.get("client_deadline_with_urma_wait"):
        recommendations.append({
            "category": "deadline",
            "title": "拆开 client deadline 和 worker 后续完成阶段",
            "detail": (
                "20ms client timeout 是失败触发点；同 trace 的 worker access、RemotePull、"
                "BatchGetObjectRemote、URMA 日志用于判断服务端是否在 deadline 后继续完成。"
            ),
        })
    if classifications.get("rpc_max_concurrency"):
        recommendations.append({
            "category": "rpc_capacity",
            "title": "重点确认 RPC 线程数/并发不足",
            "detail": (
                "出现 error_code=2004 / max_concurrency 时，按时间桶和 worker 聚合观察是否集中在少数 worker；"
                "这通常意味着服务端 RPC 并发或线程池容量不足。"
            ),
        })
    return recommendations


def _build_source_appendix(coverage):
    rows = [
        {
            "scope": "通用",
            "log_surface": "access log",
            "flow_stage": "client -> entry worker",
            "source_hint": "ObjectClientImpl / ClientWorkerRemoteApi / Worker OC access path",
            "validation": (
                "Use CodeGraph on pinned main/master ref, then direct source reads for client "
                "timeout, status, and WorkerRpc propagation."
            ),
            "report_reading": (
                "Defines user-visible latency/status; use it as symptom line, not as standalone "
                "worker-side root cause."
            ),
        },
        {
            "scope": "写入",
            "log_surface": "latencySummary",
            "flow_stage": "client -> entry worker createbuffer / publish",
            "source_hint": "client summary emitters around Set/Create/Publish and buffer preparation",
            "validation": (
                "Use CodeGraph to map each summary key to current write path; preserve raw "
                "latencySummary text in evidence."
            ),
            "report_reading": (
                "Explains write-side stage contribution even when no standalone slow log crosses "
                "threshold."
            ),
        },
        {
            "scope": "读取",
            "log_surface": "GetObjMetaInfo / QueryMeta",
            "flow_stage": "entry worker -> meta worker",
            "source_hint": "ClientWorkerRemoteApi::GetObjMetaInfo / meta service query path",
            "validation": (
                "Use CodeGraph and direct source reads to verify timeout budget and meta RPC "
                "branch before attributing QueryMeta."
            ),
            "report_reading": (
                "Only call meta path slow when logs expose QueryMeta/GetObjMetaInfo cost; "
                "absence is an observation gap."
            ),
        },
        {
            "scope": "通用",
            "log_surface": "RPC slow",
            "flow_stage": "RPC framework client/server/network split",
            "source_hint": "brpc_perf_trace.h / rpc framework slow log emitters",
            "validation": (
                "Check method name and fields such as server_exec_us and network_residual_us "
                "against current transport code."
            ),
            "report_reading": "Separates server execution, queueing, framework, and residual/network windows.",
        },
        {
            "scope": "读取",
            "log_surface": "RemotePull / BatchGetObjectRemote",
            "flow_stage": "entry worker -> data worker",
            "source_hint": "WorkerRemoteWorkerOCApi / WorkerWorkerOCServiceImpl::BatchGetObjectRemote",
            "validation": "Use CodeGraph to verify current remote get branch, fallback, and aggregation behavior.",
            "report_reading": (
                "Explains worker-side completion after client deadline; compare with client "
                "access window."
            ),
        },
        {
            "scope": "读取",
            "log_surface": "URMA_ELAPSED_TOTAL",
            "flow_stage": "data worker UB write completion",
            "source_hint": "UrmaManager::WaitToFinish / LogUrmaWaitToFinishElapsed",
            "validation": (
                "Use CodeGraph plus source reads; compare total with wait_for, wakeSchedLatencyUs, "
                "srcChipInflight, request id, src/target, dataSize, cpuid, and inflight."
            ),
            "report_reading": (
                "Treat as post/write completion wait window; use wait/wake fields to split OS "
                "scheduling from completion cost."
            ),
        },
        {
            "scope": "读取",
            "log_surface": "URMA_ELAPSED_POLL_JFC / NOTIFY / THREAD_SHED",
            "flow_stage": "data worker UB poll and wake scheduling",
            "source_hint": "UrmaManager::PollJfcWait / ds_urma_poll_jfc / ds_urma_wait_jfc / nanosleep",
            "validation": (
                "Use CodeGraph plus source reads; split poll_jfc cost, notify wakeup, poll-loop "
                "gap, and nanosleep(1us) wake cost."
            ),
            "report_reading": (
                "When total is high, these fields indicate whether the delay sits in polling, "
                "notification, or poll-thread scheduling."
            ),
        },
        {
            "scope": "写入",
            "log_surface": "Publish / CreateBuffer",
            "flow_stage": "entry worker -> meta worker publish",
            "source_hint": "CreateBuffer/Publish client APIs and meta worker publish path",
            "validation": (
                "Use CodeGraph to separate createbuffer, client publish, entry worker publish, "
                "and meta worker publish; verify each stage against latencySummary or slow logs."
            ),
            "report_reading": (
                "For write traces, keep createbuffer and publish as separate phases instead of "
                "merging all write latency."
            ),
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
    if rollup.get("top_workers"):
        parts.append("worker " + ", ".join(rollup["top_workers"][:2]))
    if rollup.get("top_ips"):
        parts.append("IP " + ", ".join(rollup["top_ips"][:2]))
    return " / ".join(parts)


def _flow_candidate_edges(read_count, write_count, ub_summary):
    observed_transfer_paths = sorted(ub_summary.get("transfer_path", {}).keys())
    return [
        {
            "name": "client -> entry worker",
            "operation": "Client→Entry RPC/UB",
            "source": "client",
            "target": "entry",
            "transports": ["RPC", "UB"],
            "status": "observed" if read_count or write_count else "not_observed",
            "observed_transfer_paths": observed_transfer_paths,
            "report_reading": "当前报告把 Client→Entry 作为入口窗口；传输可能随实现演进从 RPC/TCP/SHM 扩展到 UB。",
        },
        {
            "name": "client -> data worker",
            "operation": "Client→Data UB",
            "source": "client",
            "target": "data",
            "transports": ["UB"],
            "status": "future_candidate",
            "observed_transfer_paths": [],
            "report_reading": "后续若客户端可直接访问 DataWorker，需要解析 client-side UB source/target 并独立成边。",
        },
        {
            "name": "client -> meta worker",
            "operation": "Client→Meta Direct",
            "source": "client",
            "target": "meta",
            "transports": ["RPC", "UB"],
            "status": "future_candidate",
            "observed_transfer_paths": [],
            "report_reading": "后续若客户端可直接访问 MetaWorker，需要从 method、dst/src 和 access path 识别直连元数据边。",
        },
    ]


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
    candidate_edges = _flow_candidate_edges(read_count, write_count, ub_summary)
    rollups = {
        "read_client_entry": _flow_stage_rollup(trace_rows, {"read.client_to_entry_worker"}),
        "write_client_createbuffer": _flow_stage_rollup(trace_rows, {"write.client_to_entry_createbuffer"}),
        "write_client_publish": _flow_stage_rollup(trace_rows, {"write.client_to_entry_publish"}),
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
        {
            "id": "client",
            "label": "Client",
            "role": "client",
            "top_ips": rollups["client_entry"].get("top_ips", [])[:2],
        },
        {
            "id": "entry",
            "label": "Entry Worker",
            "role": "entry_worker",
            "top_workers": rollups["client_entry"].get("top_workers", [])[:2],
            "top_ips": rollups["entry_data"].get("top_ips", [])[:2],
        },
        {
            "id": "meta",
            "label": "Meta Worker",
            "role": "meta_worker",
            "top_ips": rollups["entry_meta"].get("top_ips", [])[:2],
        },
        {
            "id": "data",
            "label": "Data Worker",
            "role": "data_worker",
            "top_workers": rollups["data_ub"].get("top_workers", [])[:2],
            "top_ips": rollups["data_ub"].get("top_ips", [])[:2],
        },
    ]
    read_edges = [
        {
            "name": "read: client -> entry worker",
            "source": "client",
            "target": "entry",
            "operation": "Client→Entry RPC/UB",
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
            "operation": "Entry→Meta RPC",
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
            "operation": "Entry→Data RPC",
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
            "target": "entry",
            "operation": "URMA Write",
            "evidence": f"{surface_status('urma_elapsed')['events']} URMA elapsed events",
            "status": surface_status("urma_elapsed")["status"],
            "summary": _flow_edge_summary(rollups["data_ub"], "URMA elapsed evidence"),
            "rollup": rollups["data_ub"],
            "reason": (
                "DataWorker 通过 URMA Write 反向写回 EntryWorker；看 total、request id、"
                "src/target、dataSize、cpuid 和 inflight。"
            ),
            "report_reading": "读取路径的数据回传阶段，方向是 DataWorker -> EntryWorker，不是 EntryWorker -> DataWorker。",
        },
    ]
    write_edges = [
        {
            "name": "write: client -> entry worker createbuffer",
            "source": "client",
            "target": "entry",
            "operation": "CreateBuffer",
            "evidence": (
                f"{write_count} write flows, "
                f"{surface_status('latency_summary')['events']} latencySummary events"
            ),
            "status": (
                "present"
                if write_count or surface_status("latency_summary")["status"] == "present"
                else "missing"
            ),
            "summary": _flow_edge_summary(
                rollups["write_client_createbuffer"], "CreateBuffer client RPC evidence"
            ),
            "rollup": rollups["write_client_createbuffer"],
            "reason": "写路径客户侧 createbuffer/publish 请求窗口，需要和 Entry→Meta publish 区分。",
            "report_reading": "写流程先拆 client createbuffer/publish，再看 entry/meta 发布。",
        },
        {
            "name": "write: client -> entry worker publish",
            "source": "client",
            "target": "entry",
            "operation": "Client Publish",
            "evidence": (
                f"{write_count} write flows, "
                f"{surface_status('latency_summary')['events']} latencySummary events"
            ),
            "status": (
                "present"
                if write_count or surface_status("latency_summary")["status"] == "present"
                else "missing"
            ),
            "summary": _flow_edge_summary(rollups["write_client_publish"], "client publish evidence"),
            "rollup": rollups["write_client_publish"],
            "reason": "写路径 publish 从 Client 到 EntryWorker；慢时延需和本地 memory copy 及 meta publish 分开看。",
            "report_reading": "client publish 是写路径入口，不代表 UB 读传输。",
        },
        {
            "name": "write: entry worker -> meta worker publish",
            "source": "entry",
            "target": "meta",
            "operation": "Entry→Meta Publish",
            "evidence": (
                f"{write_count} write flows, "
                f"{surface_status('latency_summary')['events']} latencySummary events"
            ),
            "status": (
                "present"
                if write_count or surface_status("latency_summary")["status"] == "present"
                else "missing"
            ),
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
        "candidate_edges": candidate_edges,
        "read": {"nodes": nodes, "edges": read_edges},
        "write": {"nodes": nodes[:3], "edges": write_edges},
    }


def _build_time_buckets(trace_rows, bucket_ms):
    buckets = defaultdict(lambda: {
        "trace_ids": set(),
        "error_count": 0,
        "max_concurrency_error_count": 0,
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
        errors = trace.get("errors", {})
        bucket["error_count"] += sum(errors.values())
        bucket["max_concurrency_error_count"] += errors.get(RPC_MAX_CONCURRENCY_ERROR, 0)
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
            "max_concurrency_error_count": bucket["max_concurrency_error_count"],
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
            key_parts = [str(part or "") for part in (trace_id, request_id)]
            key = "|".join(key_parts)
        else:
            key_parts = []
            fallback_parts = (
                trace_id,
                event.get("worker"),
                event.get("src_addr"),
                event.get("target_addr"),
                event.get("timestamp"),
            )
            for part in fallback_parts:
                key_parts.append(str(part or ""))
            key = "|".join(key_parts)
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
            if row["first_ts"]:
                row["first_ts"] = min(filter(None, [row["first_ts"], event["timestamp"]]))
            else:
                row["first_ts"] = event["timestamp"]
            if row["last_ts"]:
                row["last_ts"] = max(filter(None, [row["last_ts"], event["timestamp"]]))
            else:
                row["last_ts"] = event["timestamp"]
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
        score_fields = (
            "total_ms",
            "wait_os_sched_ms",
            "poll_loop_gap_ms",
            "nanosleep_wake_ms",
            "poll_jfc_ms",
            "notify_ms",
        )
        score_values = [float(row.get(field) or 0) for field in score_fields]
        row["score_ms"] = max(score_values)
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
    lines.extend([
        "",
        "## Access Latency Ms",
        "```json",
        json.dumps(report["dimensions"]["latency_ms"], indent=2),
        "```",
    ])
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


def _preserved_input_name(index, path):
    p = Path(path)
    return f"{index:02d}-{_slug(p.name or 'input')}"


def _input_identity(path, index=None):
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
        return {
            "path": str(p),
            "size": p.stat().st_size,
            "sha256": h.hexdigest(),
            "members": members,
            "preserved_name": _preserved_input_name(index or 1, p),
        }
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
        return {
            "path": str(p),
            "size": total_size,
            "sha256": h.hexdigest(),
            "members": members,
            "preserved_name": _preserved_input_name(index or 1, p),
        }
    h.update(str(p).encode("utf-8"))
    return {"path": str(p), "size": 0, "sha256": h.hexdigest(), "members": members,
            "preserved_name": _preserved_input_name(index or 1, p)}


def _script_version():
    h = hashlib.sha256()
    with open(Path(__file__), "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()[:16]


def _cache_key(inputs, code_ref, case_name, scenario, rules_fingerprint="default"):
    identities = [_input_identity(path, index) for index, path in enumerate(inputs, 1)]
    payload = {
        "script_version": _script_version(),
        "rules_fingerprint": rules_fingerprint,
        "code_ref": code_ref,
        "case_name": case_name,
        "scenario": scenario,
        "inputs": sorted(identities, key=lambda item: item["path"]),
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
    for index, raw in enumerate(inputs, 1):
        p = Path(raw)
        preserved_name = _preserved_input_name(index, p)
        if p.is_dir():
            target_root = raw_inputs / preserved_name
            file_count = 0
            total_bytes = 0
            for fp in sorted(item for item in p.rglob("*") if item.is_file()):
                file_count += 1
                if file_count > DEFAULT_MAX_TAR_MEMBERS:
                    raise ValueError(f"Directory file count exceeds {DEFAULT_MAX_TAR_MEMBERS}: {p}")
                size = fp.stat().st_size
                if size > DEFAULT_MAX_TAR_MEMBER_BYTES:
                    raise ValueError(f"Directory file too large: {fp}")
                total_bytes += size
                if total_bytes > DEFAULT_MAX_TAR_TOTAL_BYTES:
                    raise ValueError(f"Directory preserved bytes exceed {DEFAULT_MAX_TAR_TOTAL_BYTES}: {p}")
                dest = target_root / fp.relative_to(p)
                dest.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(fp, dest)
        if p.is_file():
            copied = raw_inputs / preserved_name
            shutil.copy2(p, copied)
            if tarfile.is_tarfile(p):
                extract_root = raw_extracted / preserved_name
                member_count = 0
                total_bytes = 0
                with tarfile.open(p, "r:*") as tar:
                    for member in tar.getmembers():
                        if not member.isfile():
                            continue
                        member_count += 1
                        total_bytes = _check_tar_budget(p, member, member_count, total_bytes)
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
        preserved_name = item.get("preserved_name") or _preserved_input_name(index, source)
        lines.extend([
            f"### {index}. `{name}`",
            "",
            f"- source_path: `{source}`",
            f"- size_bytes: {item.get('size', 0)}",
            f"- sha256: `{item.get('sha256', '')}`",
        ])
        raw_copy = Path("raw") / "inputs" / preserved_name
        if (Path(run_dir) / raw_copy).exists():
            lines.append(f"- preserved_raw: `{raw_copy.as_posix()}`")
        members = item.get("members", [])
        if members:
            extract_root = Path("raw") / "extracted" / preserved_name
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
        raise TraceTriageError(
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
        (
            "- Open the URL and verify navigation, ECharts, provenance, coverage, "
            "trace filters, downloads, and selected logs."
        ),
        (
            "- Do not publish oversized throw-away pages; pass `--max-site-html-mb` only after "
            "reviewing why the page is large."
        ),
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
    base_style = (
    """<style>"""
    "\n"
    """:root{--bg:#f6f8fb;--card:#fff;--text:#172033;--muted:#5f6b7a;--border:#dfe5ee;--blue:#2563eb;--orange:#ea"""
    """580c;--red:#dc2626;--green:#059669;--purple:#7c3aed;--amber:#ca8a04;--report-font-size:13px}"""
    "\n"
    """*{box-sizing:border-box}body{margin:0;overflow-x:hidden;background:var(--bg);color:var(--text);font-family"""
    """:'Microsoft YaHei',-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif}"""
    "\n"
    """.layout{display:flex;align-items:flex-start;min-height:100vh}aside{position:sticky;left:0;top:0;flex:0 0 """
    """245px;width:245px;height:100vh;background:#fff;border-right:1px solid var(--border);padding:18px """
    """14px;overflow:auto}"""
    "\n"
    """aside h2{font-size:16px;margin:0 0 12px}nav """
    """a{display:block;color:#324055;text-decoration:none;padding:8px """
    """10px;border-radius:6px;font-size:13px;margin:2px 0}"""
    "\n"
    """nav a.active,nav a:hover{background:#eaf2ff;color:#1d4ed8}nav """
    """a.sub{padding-left:22px;color:#64748b;font-size:12px}"""
    "\n"
    """main{flex:1;min-width:0;width:auto;padding:22px 28px 50px}section{margin-bottom:20px}"""
    "\n"
    """h1{font-size:26px;margin:0 0 8px}h2{font-size:21px;margin:8px 0 """
    """12px}h3{font-size:15px;text-align:center;margin:10px 0}"""
    "\n"
    """.subtitle,.note,.insight{color:var(--muted);line-height:1.65}.section-summary{background:#f8fafc;border-le"""
    """ft:4px solid var(--blue);color:#334155;font-size:13px;line-height:1.65}.section-summary """
    """b{color:#172033}.summary-points{margin:6px 0 0 18px;padding:0}.summary-points li{margin:3px """
    """0}.summary-key{font-weight:700;color:#1d4ed8}.summary-hot{font-weight:700;color:#991b1b;background:#fee2e2"""
    """;border-radius:4px;padding:1px """
    """4px}.summary-warn{font-weight:700;color:#9a3412;background:#ffedd5;border-radius:4px;padding:1px """
    """4px}.chapter-guide{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:8px;margin:0;padding:0;l"""
    """ist-style:none}.chapter-guide li{border:1px solid """
    """var(--border);border-radius:8px;background:#f8fafc;padding:9px """
    """10px;line-height:1.55;font-size:13px}.chapter-guide """
    """a{color:#1d4ed8;text-decoration:none;font-weight:700}.cards{display:grid;grid-template-columns:repeat(4,1f"""
    """r);gap:10px}"""
    "\n"
    """.card{background:#fff;border:1px solid """
    """var(--border);border-radius:8px;padding:12px;min-width:0}.panel{min-width:0;background:#fff;border:1px """
    """solid var(--border);border-radius:8px;padding:16px;margin:12px 0;box-shadow:0 2px 10px rgba(20,35,60,.04)}"""
    "\n"
    """.k{color:#64748b;font-size:12px}.v,.metric{font-size:24px;font-weight:700;margin:4px """
    """0}.n,.muted,.small{color:#64748b;font-size:12px}.bad{color:var(--red)!important;font-weight:700}.warn{colo"""
    """r:#b45309!important;font-weight:700}.ok{color:var(--green)!important;font-weight:700}"""
    "\n"
    """tr.hotrow td{background:#fff1f2}tr.warnrow """
    """td{background:#fffbeb}.cell-hot,.cell-warn,.cell-ok{display:inline-block;border-radius:4px;padding:1px """
    """5px;font-weight:700}.cell-hot{background:#fee2e2;color:#991b1b}.cell-warn{background:#ffedd5;color:#9a3412"""
    """}.cell-ok{background:#dcfce7;color:#166534}"""
    "\n"
    """tr.summaryrow td{background:#f8fafc}"""
    "\n"
    """.log-tag{display:inline-block;border-radius:4px;padding:0 4px;margin:0 """
    """1px;font-weight:700}.log-error{background:#fee2e2;color:#991b1b}.log-deadline{background:#ffedd5;color:#9a"""
    """3412}.log-urma{background:#ede9fe;color:#5b21b6}.log-rpc{background:#dbeafe;color:#1e40af}.log-latency{bac"""
    """kground:#dcfce7;color:#166534}.log-slow{background:#fef3c7;color:#92400e}.log-field{background:#e2e8f0;col"""
    """or:#334155}"""
    "\n"
    """.log-legend,.stage-legend{display:flex;flex-wrap:wrap;gap:6px;margin:8px 0}.log-legend span,.stage-legend """
    """span{font-size:12px}"""
    "\n"
    """.stage-pill{display:inline-flex;align-items:center;gap:5px;border:1px solid """
    """var(--border);border-radius:999px;padding:2px """
    """8px;background:#fff;color:#475569}.stage-dot{width:10px;height:10px;border-radius:2px;display:inline-block"""
    """}"""
    "\n"
    """.compare2{display:grid;grid-template-columns:1fr """
    """1fr;gap:12px}.chart-grid{display:grid;grid-template-columns:1fr """
    """1fr;gap:12px}.full-row{grid-column:1/-1}.flow-section{display:grid;grid-template-columns:1fr;gap:12px;marg"""
    """in-top:12px}.flow-pair{display:grid;grid-template-columns:1fr 1fr;gap:12px}.flow-pair """
    """.chart{height:320px}.chart{height:360px;width:100%}.flow-graph-chart{height:360px}.caption{text-align:cent"""
    """er;color:#64748b;font-size:var(--report-font-size);margin-top:6px}"""
    "\n"
    """table{width:100%;border-collapse:collapse;table-layout:fixed;background:#fff}th,td{border-bottom:1px """
    """solid var(--border);padding:8px """
    """9px;text-align:left;vertical-align:top;font-size:var(--report-font-size);word-break:break-word}"""
    "\n"
    """th{background:#f8fafc;color:#475569}.sortable-th{cursor:pointer;user-select:none}.sortable-th:hover{backgr"""
    """ound:#eaf2ff}.sort-mark{color:#2563eb;font-size:11px;margin-left:4px}.num{text-align:right;font-variant-nu"""
    """meric:tabular-nums}.trace-id{font-family:'Cascadia Code',Consolas,monospace;font-size:12px}"""
    "\n"
    """.table-scroll{width:100%;max-width:100%;overflow-x:auto}.adaptive-table{table-layout:fixed}.nowrap-table{m"""
    """in-width:720px;table-layout:auto}.metadata-table{table-layout:auto}#run-metadata-table """
    """th:first-child,#run-metadata-table """
    """td:first-child{width:1%;white-space:nowrap;min-width:120px}#run-metadata-table """
    """th:last-child,#run-metadata-table td:last-child{width:auto}.code-ref-value{font-family:'Cascadia """
    """Code',Consolas,monospace;font-size:12px;line-height:1.45;overflow-wrap:anywhere;word-break:break-all}.code"""
    """-ref-card .v{font-family:'Cascadia """
    """Code',Consolas,monospace;font-size:13px;line-height:1.35;overflow-wrap:anywhere}.code-ref-card """
    """.n{overflow-wrap:anywhere}#ub-lifecycle-table th,#ub-lifecycle-table """
    """td{white-space:nowrap}#ub-worker-role-table th:last-child,#ub-worker-role-table """
    """td:last-child{width:30%}#ub-request-table th,#ub-request-table td{padding:7px 6px}#ub-request-table """
    """th:nth-child(10),#ub-request-table td:nth-child(10),#ub-request-table th:nth-child(11),#ub-request-table """
    """td:nth-child(11),#ub-request-table th:nth-child(12),#ub-request-table td:nth-child(12){text-align:right}"""
    "\n"
    """.controls{display:flex;gap:8px;flex-wrap:wrap;margin:8px 0 """
    """12px;align-items:center}input,select,button{border:1px solid """
    """var(--border);background:#fff;border-radius:6px;padding:7px 9px;font-size:13px}"""
    "\n"
    """button{cursor:pointer}button.primary{background:var(--blue);color:#fff;border-color:var(--blue)}button:dis"""
    """abled{opacity:.45;cursor:not-allowed}.pager{background:#fff;border:1px solid """
    """var(--border);border-radius:8px;padding:10px}.mini-pager{display:flex;justify-content:center;gap:8px;align"""
    """-items:center;margin-top:8px;color:#64748b;font-size:12px}"""
    "\n"
    """.selected-row{background:#fff7e6}.logbox,pre{white-space:pre-wrap;background:#0f172a;color:#dbeafe;padding"""
    """:12px;border-radius:8px;max-height:520px;overflow:auto;font-family:'Cascadia """
    """Code',Consolas,monospace;font-size:12px;line-height:1.5}"""
    "\n"
    """.trace-log-groups{display:flex;flex-direction:column;gap:10px;margin-top:10px}.trace-log-block{border:1px """
    """solid var(--border);border-radius:8px;overflow:hidden;background:#fff}.trace-log-block """
    """h4{margin:0;padding:8px """
    """12px;background:#f8fafc;color:#0f172a;font-size:13px;display:flex;justify-content:space-between;gap:12px;a"""
    """lign-items:flex-start}.trace-log-block """
    """pre{border-radius:0;margin:0;max-height:none}.trace-log-count{color:#64748b;font-weight:500;white-space:no"""
    """wrap}.trace-log-heading{display:flex;flex-direction:column;gap:4px;min-width:0}.trace-log-summary{display:"""
    """flex;flex-wrap:wrap;gap:4px;font-size:12px;color:#64748b;font-weight:500}.trace-log-details{padding:8px """
    """12px;background:#f8fafc;border-top:1px solid """
    """#e2e8f0;color:#475569;font-size:12px;line-height:1.55}.trace-log-details """
    """div+div{margin-top:3px}.trace-latency-hot{font-weight:700;color:#991b1b;background:#fee2e2;border-radius:4"""
    """px;padding:1px 4px}.trace-latency-warn{font-weight:700;color:#9a3412;background:#ffedd5;border-radius:4px;"""
    """padding:1px 4px}.trace-log-focus{border:1px solid #e2e8f0;border-radius:999px;padding:1px """
    """7px;background:#fff;color:#475569}.trace-log-focus.hot{border-color:#fecaca;background:#fef2f2;color:#991b"""
    """1b}.trace-log-focus.warn{border-color:#fed7aa;background:#fff7ed;color:#9a3412}"""
    "\n"
    """code{font-family:'Cascadia Code',Consolas,monospace;font-size:12px}"""
    "\n"
    """@media(max-width:900px){.layout{display:block}aside{position:relative;width:auto;height:auto}main{margin-l"""
    """eft:0;width:100%;padding:16px}.chart-grid,.compare2,.cards,.chapter-guide,.flow-pair{grid-template-columns"""
    """:1fr}}"""
    "\n"
    """</style>"""
)
    stylesheet = ('<link rel="stylesheet" href="/assets/css/site.css">' if site else "") + base_style
    script_ref = '<script src="/assets/js/site.js"></script>' if site else ""
    template = (
    """<!doctype html>"""
    "\n"
    """<html lang="zh-CN">"""
    "\n"
    """<head>"""
    "\n"
    """  <meta charset="utf-8">"""
    "\n"
    """  <meta name="viewport" content="width=device-width, initial-scale=1">"""
    "\n"
    """  <title>__TITLE__</title>"""
    "\n"
    """  __STYLESHEET__"""
    "\n"
    """  <script src="https://cdn.jsdelivr.net/npm/echarts@5/dist/echarts.min.js"></script>"""
    "\n"
    """</head>"""
    "\n"
    """<body>"""
    "\n"
    """  <div class="layout">"""
    "\n"
    """    <aside><h2>Trace 分析报告</h2><nav id="nav">"""
    "\n"
    """      <a href="#s1">1. 结论</a>"""
    "\n"
    """      <a class="sub" href="#overall-guide">表 1-1 整体导读</a>"""
    "\n"
    """      <a href="#s2">2. 根因分布</a>"""
    "\n"
    """      <a class="sub" href="#cohort-chart">图 2-1 输入包 / Cohort</a>"""
    "\n"
    """      <a class="sub" href="#classification-chart">图 2-2 分类分布</a>"""
    "\n"
    """      <a class="sub" href="#error-chart">图 2-3 错误分布</a>"""
    "\n"
    """      <a href="#s3">3. 时延 Breakdown</a>"""
    "\n"
    """      <a class="sub" href="#read-latency-chart">图 3-1 读取时延</a>"""
    "\n"
    """      <a class="sub" href="#rw-flow-chart">图 3-2 读写 Flow</a>"""
    "\n"
    """      <a class="sub" href="#read-time-breakdown-chart">图 3-3 读取时间桶</a>"""
    "\n"
    """      <a class="sub" href="#write-latency-chart">图 3-4 写入时延</a>"""
    "\n"
    """      <a class="sub" href="#write-time-breakdown-chart">图 3-5 写入时间桶</a>"""
    "\n"
    """      <a href="#s4">4. Worker / 流程</a>"""
    "\n"
    """      <a class="sub" href="#read-flow-stage-chart">图 4-1 读取流程</a>"""
    "\n"
    """      <a class="sub" href="#read-worker-chart">图 4-2 读取 Worker</a>"""
    "\n"
    """      <a class="sub" href="#write-flow-stage-chart">图 4-3 写入流程</a>"""
    "\n"
    """      <a class="sub" href="#write-worker-chart">图 4-4 写入 Worker</a>"""
    "\n"
    """      <a class="sub" href="#worker-ip-alias-table">表 4-5 Worker IP 映射</a>"""
    "\n"
    """      <a href="#s5">5. UB / URMA</a>"""
    "\n"
    """      <a class="sub" href="#ub-lifecycle-chart">图 5-1 UB 生命周期</a>"""
    "\n"
    """      <a class="sub" href="#ub-wr-count-chart">图 5-2 WR / Inflight</a>"""
    "\n"
    """      <a class="sub" href="#ub-worker-role-chart">图 5-3 发起/提供 Worker</a>"""
    "\n"
    """      <a class="sub" href="#ub-worker-time-chart">图 5-4 UB 时间桶</a>"""
    "\n"
    """      <a class="sub" href="#read-ub-edge-chart">图 5-5 读取 UB Edge</a>"""
    "\n"
    """      <a class="sub" href="#write-ub-edge-chart">图 5-6 写入 UB Edge</a>"""
    "\n"
    """      <a href="#s6">6. Trace 查看</a>"""
    "\n"
    """      <a class="sub" href="#top-trace-table">表 6-1 Top Trace</a>"""
    "\n"
    """      <a class="sub" href="#selected-trace-chart">图 6-1 选中 Trace</a>"""
    "\n"
    """      <a class="sub" href="#selected-trace-log">日志框 6-3 全量日志</a>"""
    "\n"
    """      <a href="#s7">7. 建议与口径</a>"""
    "\n"
    """      <a class="sub" href="#recommendation-table">表 7-1 建议</a>"""
    "\n"
    """      <a class="sub" href="#source-appendix-common-table">表 7-2 代码映射</a>"""
    "\n"
    """      <a href="#s8">8. 原始 JSON</a>"""
    "\n"
    """      <a class="sub" href="#trace-data">表 8-1 原始 Trace JSON</a>"""
    "\n"
    """    </nav>"""
    "\n"
    """    </aside>"""
    "\n"
    """    <main>"""
    "\n"
    """      <section id="s1">"""
    "\n"
    """        <h1>__TITLE__</h1>"""
    "\n"
    """        <p class="subtitle" id="report-subtitle"></p>"""
    "\n"
    """        <div id="summary" class="cards"></div>"""
    "\n"
    """        <div class="panel"><h3>运行与输入来源</h3><table id="run-metadata-table" """
    """class="metadata-table"></table></div>"""
    "\n"
    """        <div class="panel insight" id="report-insight"></div>"""
    "\n"
    """        <div class="panel" id="overall-guide"><h3>表 1-1 整体导读</h3><ul class="chapter-guide" """
    """id="chapter-guide-list"></ul></div>"""
    "\n"
    """        <div class="panel"><h3>客户化诊断口径</h3><ul id="diagnosis-list"></ul><div class="controls"><button """
    """class="primary" id="download-report-summary">下载分析摘要</button></div></div>"""
    "\n"
    """        <div class="panel"><h3>日志覆盖与缺失观测面</h3><table id="coverage-table"></table></div>"""
    "\n"
    """      </section>"""
    "\n"
    """      <section id="s2">"""
    "\n"
    """        <h2>2. 错误根因与分类分布</h2>"""
    "\n"
    """        <div class="panel section-summary" id="section-summary-s2"></div>"""
    "\n"
    """        <div class="panel"><div id="cohort-chart" class="chart"></div><div class="caption">图 2-1 输入包 / """
    """Cohort 对比：多个日志包独立统计，再对比分类和错误分布。</div></div>"""
    "\n"
    """        <div class="panel"><h3>表 2-1 输入包 / Cohort 对比</h3><table id="cohort-table"></table></div>"""
    "\n"
    """        <div class="chart-grid">"""
    "\n"
    """          <div class="panel"><div id="classification-chart" class="chart"></div><div class="caption">图 """
    """2-2 分类分布：按根因分类聚合 trace。</div></div>"""
    "\n"
    """          <div class="panel"><div id="error-chart" class="chart"></div><div class="caption">图 2-3 错误文本 / """
    """状态分布：按错误文本和状态码聚合。</div></div>"""
    "\n"
    """        </div>"""
    "\n"
    """        <div class="compare2">"""
    "\n"
    """          <div class="panel"><h3>表 2-2 分类聚合</h3><table id="classification-table"></table></div>"""
    "\n"
    """          <div class="panel"><h3>表 2-3 Error Breakdown</h3><table id="error-table"></table></div>"""
    "\n"
    """        </div>"""
    "\n"
    """      </section>"""
    "\n"
    """      <section id="s3">"""
    "\n"
    """        <h2>3. 时延 Breakdown</h2>"""
    "\n"
    """        <div class="panel section-summary" id="section-summary-s3"></div>"""
    "\n"
    """        <div class="panel insight">读写分开看尾部：读关注 GET/QueryMeta/RemoteGet/UB，写关注 SET/Create/Publish/memory """
    """copy。</div>"""
    "\n"
    """        <div class="flow-section">"""
    "\n"
    """          <div class="panel"><div id="read-latency-chart" class="chart"></div><div class="caption">图 3-1 """
    """读取 Top 时延。</div></div>"""
    "\n"
    """          <div id="rw-flow-chart" class="panel">"""
    "\n"
    """            <div class="flow-pair">"""
    "\n"
    """              <div><div id="read-flow-chart" class="chart"></div></div>"""
    "\n"
    """              <div><div id="write-flow-chart" class="chart"></div></div>"""
    "\n"
    """            </div>"""
    "\n"
    """            <div class="caption">图 3-2 读写 Flow：左侧为读取接口分布，右侧为写入接口分布。</div>"""
    "\n"
    """          </div>"""
    "\n"
    """          <div class="panel"><div id="read-time-breakdown-chart" class="chart"></div><div """
    """class="caption">图 3-3 读取时间桶 Breakdown：柱为读 RPC/UB 子阶段 p99，线为读 trace access p99。</div></div>"""
    "\n"
    """        </div>"""
    "\n"
    """        <div class="flow-section">"""
    "\n"
    """          <div class="panel"><div id="write-latency-chart" class="chart"></div><div class="caption">图 3-4 """
    """写入 Top 时延。</div></div>"""
    "\n"
    """          <div class="panel"><div id="write-time-breakdown-chart" class="chart"></div><div """
    """class="caption">图 3-5 写入时间桶 Breakdown：柱为写 RPC/本地阶段 p99，线为写 trace access p99。</div></div>"""
    "\n"
    """        </div>"""
    "\n"
    """        <div class="compare2">"""
    "\n"
    """          <div class="panel"><h3>表 3-1 读取时延指标</h3><table id="read-latency-table"></table></div>"""
    "\n"
    """          <div class="panel"><h3>表 3-2 写入时延指标</h3><table id="write-latency-table"></table></div>"""
    "\n"
    """          <div class="panel"><h3>表 3-3 读取 Flow</h3><table id="read-flow-table"></table></div>"""
    "\n"
    """          <div class="panel"><h3>表 3-4 写入 Flow</h3><table id="write-flow-table"></table></div>"""
    "\n"
    """        </div>"""
    "\n"
    """      </section>"""
    "\n"
    """      <section id="s4">"""
    "\n"
    """        <h2>4. Worker / 流程分布</h2>"""
    "\n"
    """        <div class="panel section-summary" id="section-summary-s4"></div>"""
    "\n"
    """        <div id="flow-stage-chart" class="panel insight">读写链路分开看：读关注 Entry→Data，写关注 """
    """CreateBuffer/Publish/Meta。</div>"""
    "\n"
    """        <div id="read-flow-section" class="flow-section">"""
    "\n"
    """          <div class="panel"><div id="read-flow-stage-chart" class="chart flow-graph-chart"></div><div """
    """class="caption">图 4-1 读取流程证据块：看 Client→Entry RPC/UB、Entry→Data RPC 与 URMA Write。</div></div>"""
    "\n"
    """          <div class="panel"><h3>表 4-1 读取流程阶段证据</h3><table id="read-flow-stage-table"></table></div>"""
    "\n"
    """          <div class="panel"><div class="controls"><label>Worker 筛选 <select """
    """id="read-worker-filter"><option value="">全部 Worker</option></select></label></div><div """
    """id="read-worker-chart" class="chart"></div><div class="caption">图 4-2 读取 Worker 分布：读取链路按 worker """
    """聚合。</div></div>"""
    "\n"
    """          <div class="panel"><h3>表 4-2 读取 Worker Breakdown</h3><div class="controls"><label>Worker 筛选 """
    """<select id="read-worker-table-filter"><option value="">全部 Worker</option></select></label></div><table """
    """id="read-worker-table"></table><div id="read-worker-table-pager" class="mini-pager"></div></div>"""
    "\n"
    """        </div>"""
    "\n"
    """        <div id="write-flow-section" class="flow-section">"""
    "\n"
    """          <div class="panel"><div id="write-flow-stage-chart" class="chart flow-graph-chart"></div><div """
    """class="caption">图 4-3 写入流程证据块：区分 createbuffer、client publish、entry/meta publish。</div></div>"""
    "\n"
    """          <div class="panel"><h3>表 4-3 写入流程阶段证据</h3><table id="write-flow-stage-table"></table></div>"""
    "\n"
    """          <div class="panel"><div class="controls"><label>Worker 筛选 <select """
    """id="write-worker-filter"><option value="">全部 Worker</option></select></label></div><div """
    """id="write-worker-chart" class="chart"></div><div class="caption">图 4-4 写入 Worker 分布：写入链路按 worker """
    """聚合。</div></div>"""
    "\n"
    """          <div class="panel"><h3>表 4-4 写入 Worker Breakdown</h3><div class="controls"><label>Worker 筛选 """
    """<select id="write-worker-table-filter"><option value="">全部 Worker</option></select></label></div><table """
    """id="write-worker-table"></table><div id="write-worker-table-pager" class="mini-pager"></div></div>"""
    "\n"
    """        </div>"""
    "\n"
    """        <div class="panel"><h3>表 4-5 Worker IP / 别名映射</h3><table id="worker-ip-alias-table" """
    """class="adaptive-table"></table><div id="worker-ip-alias-table-pager" class="mini-pager"></div></div>"""
    "\n"
    """        <div class="panel"><h3>表 4-6 候选链路 / 拓扑扩展</h3><div class="small">主图只画已观测边；候选链路用于记录后续 Client→Entry """
    """UB、Client→Data UB、Client→Meta Direct 等拓扑演进。</div><table id="flow-candidate-edge-table" """
    """class="adaptive-table"></table></div>"""
    "\n"
    """        <div class="panel" style="display:none"><table id="flow-stage-table"></table></div>"""
    "\n"
    """      </section>"""
    "\n"
    """      <section id="s5">"""
    "\n"
    """        <h2>5. UB / URMA 分析</h2>"""
    "\n"
    """        <div class="panel section-summary" id="section-summary-s5"></div>"""
    "\n"
    """        <div class="panel insight">UB 单独看：先看 wait/poll/notify/sched，再看入口/出口 worker，最后看 edge/IP。</div>"""
    "\n"
    """        <div class="chart-grid">"""
    "\n"
    """          <div class="panel"><div id="ub-lifecycle-chart" class="chart"></div><div class="caption">图 5-1 """
    """UB 生命周期：TOTAL、wait_for、poll/notify/sched 等耗时字段，按实际采样字段展示。</div></div>"""
    "\n"
    """          <div class="panel"><div id="ub-wr-count-chart" class="chart"></div><div class="caption">图 5-2 """
    """WR / Inflight Count：remote get WR、URMA inflight WR、chip inflight，单位 count。</div></div>"""
    "\n"
    """          <div class="panel full-row"><div class="controls"><label>Worker 筛选 <select """
    """id="ub-worker-role-filter"><option value="">全部 Worker</option></select></label></div><div """
    """id="ub-worker-role-chart" class="chart"></div><div class="caption">图 5-3 UB Worker 角色：数据读取发起端(entry get) """
    """来自 RemoteGet/transferPath，数据提供端(data provider) 来自 URMA_ELAPSED。</div></div>"""
    "\n"
    """        </div>"""
    "\n"
    """        <div class="panel"><div class="controls"><label>Worker 筛选 <select """
    """id="ub-worker-time-filter"><option value="">全部 Worker</option></select></label></div><div """
    """id="ub-worker-time-chart" class="chart"></div><div class="caption">图 5-4 UB """
    """时间桶：按秒观察入口/出口事件与尾部时延。</div></div>"""
    "\n"
    """        <div class="flow-section ub-table-stack">"""
    "\n"
    """          <div class="panel"><h3>表 5-1 UB 生命周期指标</h3><div class="table-scroll"><table """
    """id="ub-lifecycle-table" class="nowrap-table"></table></div></div>"""
    "\n"
    """          <div class="panel"><h3>表 5-2 UB Request Top</h3><table id="ub-request-table" """
    """class="adaptive-table"></table><div id="ub-request-table-pager" class="mini-pager"></div></div>"""
    "\n"
    """          <div class="panel"><h3>表 5-3 UB Worker 角色</h3><div class="controls"><label>Worker 筛选 <select """
    """id="ub-worker-role-table-filter"><option value="">全部 Worker</option></select></label></div><table """
    """id="ub-worker-role-table" class="adaptive-table"></table><div id="ub-worker-role-table-pager" """
    """class="mini-pager"></div></div>"""
    "\n"
    """          <div class="panel"><h3>表 5-4 UB 时间桶</h3><div class="controls"><label>Worker 筛选 <select """
    """id="ub-worker-time-table-filter"><option value="">全部 Worker</option></select></label></div><table """
    """id="ub-worker-time-table"></table><div id="ub-worker-time-table-pager" class="mini-pager"></div></div>"""
    "\n"
    """        </div>"""
    "\n"
    """        <div class="flow-section">"""
    "\n"
    """          <div class="panel"><div id="read-ub-edge-summary" class="section-summary"></div><div """
    """id="read-ub-edge-chart" class="chart"></div><div class="caption">图 5-5 读取 UB Edge：读取 UB 入口/出口 IP 与 worker """
    """关联。</div></div>"""
    "\n"
    """          <div class="panel"><h3>表 5-5 读取 UB Edges</h3><div class="controls"><label>源端 <select """
    """id="read-ub-src-filter"><option value="">全部源端</option></select></label><label>目的端 <select """
    """id="read-ub-dst-filter"><option value="">全部目的端</option></select></label></div><table """
    """id="read-ub-edge-table"></table><div id="read-ub-edge-table-pager" class="mini-pager"></div></div>"""
    "\n"
    """          <div class="panel"><div id="write-ub-edge-summary" class="section-summary"></div><div """
    """id="write-ub-edge-chart" class="chart"></div><div class="caption">图 5-6 写入 UB Edge：写入 UB 入口/出口 IP 与 """
    """worker 关联。</div></div>"""
    "\n"
    """          <div class="panel"><h3>表 5-6 写入 UB Edges</h3><table id="write-ub-edge-table"></table><div """
    """id="write-ub-edge-table-pager" class="mini-pager"></div></div>"""
    "\n"
    """        </div>"""
    "\n"
    """      </section>"""
    "\n"
    """      <section id="s6">"""
    "\n"
    """        <h2>6. Trace 查看</h2>"""
    "\n"
    """        <div class="panel section-summary" id="section-summary-s6"></div>"""
    "\n"
    """        <div class="panel">"""
    "\n"
    """        <div class="controls"><label>Trace 查看读写视角 <select id="operation-filter"><option """
    """value="">全部读写</option><option value="read">只看读取</option><option """
    """value="write">只看写入</option></select></label><label>请求状态 <select id="request-status-filter"><option """
    """value="">全部请求</option><option value="failed">只看失败</option><option """
    """value="success">只看成功</option></select></label><input id="trace-search" placeholder="搜索 trace / worker / """
    """关键词" style="min-width:300px"><select id="class-filter"><option value="">全部分类</option></select><select """
    """id="worker-filter"><option value="">全部 Worker</option></select><span class="muted">按 access max 降序；联动 """
    """Trace 列表与选中 Trace Breakdown。</span><button id="reset-filter">清空</button></div>"""
    "\n"
    """        <h3>表 6-1 Top Trace</h3>"""
    "\n"
    """        <table id="top-trace-table"></table>"""
    "\n"
    """        <div id="trace-pager" class="controls pager">"""
    "\n"
    """          <label>每页 <select id="trace-page-size"><option value="4" selected>4</option><option """
    """value="8">8</option><option value="16">16</option><option value="32">32</option><option """
    """value="9999">全部</option></select> 条</label>"""
    "\n"
    """          <button class="primary" id="prev-page">上一页</button>"""
    "\n"
    """          <span id="page-status" class="muted"></span>"""
    "\n"
    """          <button class="primary" id="next-page">下一页</button>"""
    "\n"
    """        </div>"""
    "\n"
    """        </div>"""
    "\n"
    """        <div class="compare2">"""
    "\n"
    """          <div class="panel"><div id="selected-trace-chart" class="chart"></div><div """
    """id="selected-stage-legend" class="stage-legend"></div><div class="caption">图 6-1 选中 Trace Breakdown：点击 """
    """Trace 行联动，按阶段耗时排序，单位 ms。</div><table id="selected-stage-table"></table></div>"""
    "\n"
    """          <div class="panel"><h3>表 6-2 选中 Trace 摘要</h3><table id="selected-trace-table"></table><div """
    """class="controls"><button class="primary" id="download-selected-raw">下载当前 Trace 裸日志</button><button """
    """id="download-filtered-evidence">下载当前过滤证据</button></div></div>"""
    "\n"
    """        </div>"""
    "\n"
    """        <div class="panel"><h3>日志框 6-3 Trace 全量日志</h3><div class="small">按组件分块，保留原始顺序；异常、慢 """
    """RPC、latencySummary、RemotePull、URMA 和大耗时会高亮。</div><div class="log-legend" """
    """id="log-highlight-legend"></div><div id="selected-trace-log" class="trace-log-groups"></div></div>"""
    "\n"
    """      </section>"""
    "\n"
    """      <section id="s7">"""
    "\n"
    """        <h2>7. 建议与后续口径</h2>"""
    "\n"
    """        <div class="panel section-summary" id="section-summary-s7"></div>"""
    "\n"
    """        <div class="panel"><h3>表 7-1 建议与证据边界</h3><table id="recommendation-table"></table></div>"""
    "\n"
    """        <div class="panel">"""
    "\n"
    """          <h3>表 7-2 代码与字段映射</h3>"""
    "\n"
    """          <h4>通用字段映射</h4><table id="source-appendix-common-table"></table>"""
    "\n"
    """          <h4>读取字段映射</h4><table id="source-appendix-read-table"></table>"""
    "\n"
    """          <h4>写入字段映射</h4><table id="source-appendix-write-table"></table>"""
    "\n"
    """        </div>"""
    "\n"
    """      </section>"""
    "\n"
    """      <section id="s8">"""
    "\n"
    """        <h2>8. 原始 JSON 附录</h2>"""
    "\n"
    """        <div class="panel section-summary" id="section-summary-s8"></div>"""
    "\n"
    """        <div class="panel"><details>"""
    "\n"
    """          <summary>展开 parser 原始 trace JSON</summary>"""
    "\n"
    """          <pre id="trace-data"></pre>"""
    "\n"
    """        </details></div>"""
    "\n"
    """      </section>"""
    "\n"
    """    </main>"""
    "\n"
    """  </div>"""
    "\n"
    """  <script>"""
    "\n"
    """  const report = __DATA__;"""
    "\n"
    """  const manifest = __MANIFEST__;"""
    "\n"
    """  const dim = report.dimensions || {};"""
    "\n"
    """  const traces = report.traces || {};"""
    "\n"
    """  function traceAccessLatencyMs(item) {"""
    "\n"
    """    const value = item?.access_latency_ms?.max ?? item?.access_latency_ms?.p99 ?? """
    """item?.access_latency_ms?.p50 ?? 0;"""
    "\n"
    """    const n = Number(value || 0);"""
    "\n"
    """    return Number.isFinite(n) ? Number(n.toFixed(3)) : 0;"""
    "\n"
    """  }"""
    "\n"
    """  const traceRows = Object.entries(traces).sort((a,b) => traceAccessLatencyMs(b[1]) - """
    """traceAccessLatencyMs(a[1]));"""
    "\n"
    """  let filteredTraceRows = traceRows;"""
    "\n"
    """  let currentPage = 0;"""
    "\n"
    """  let selectedTraceId = traceRows[0]?.[0] || null;"""
    "\n"
    """  let pageSize = 4;"""
    "\n"
    """  let activeOperation = '';"""
    "\n"
    """  let topTraceSort = null;"""
    "\n"
    """  function escapeHtml(value) {"""
    "\n"
    """    return String(value ?? '').replace(/[&<>"']/g, ch => """
    """({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[ch]));"""
    "\n"
    """  }"""
    "\n"
    """  function shortCodeRef(value) {"""
    "\n"
    """    const text = String(value || '');"""
    "\n"
    """    return text.length > 16 ? `${text.slice(0, 12)}...${text.slice(-7)}` : text;"""
    "\n"
    """  }"""
    "\n"
    """  function formatMetadataValue(field, value) {"""
    "\n"
    """    if (field === 'code_ref') {"""
    "\n"
    """      return `<span class="code-ref-value" title="${escapeHtml(value)}">${escapeHtml(value)}</span>`;"""
    "\n"
    """    }"""
    "\n"
    """    return formatCell('value', value);"""
    "\n"
    """  }"""
    "\n"
    """  function workerNameParts(raw) {"""
    "\n"
    """    const text = String(raw || '');"""
    "\n"
    """    let match = text.match(/client-?(\\d+).*?worker-?(\\d+)/i);"""
    "\n"
    """    if (match) return {kind:'client', left:match[1], worker:match[2]};"""
    "\n"
    """    match = text.match(/worker-?(\\d+).*?worker-?(\\d+)/i);"""
    "\n"
    """    if (match) return {kind:'worker', left:match[1], worker:match[2]};"""
    "\n"
    """    match = text.match(/(?:^|[-_/])worker-?(\\d+)/i);"""
    "\n"
    """    if (match) return {kind:'worker', worker:match[1]};"""
    "\n"
    """    return {kind:'unknown', raw:text};"""
    "\n"
    """  }"""
    "\n"
    """  function workerDisplayName(raw) {"""
    "\n"
    """    const parts = workerNameParts(raw);"""
    "\n"
    """    return parts.worker ? `worker ${parts.worker}` : (parts.raw || 'unknown worker');"""
    "\n"
    """  }"""
    "\n"
    """  function workerAggregateKey(raw) {"""
    "\n"
    """    const parts = workerNameParts(raw);"""
    "\n"
    """    return parts.worker ? `worker:${parts.worker}` : `raw:${String(raw || 'unknown')}`;"""
    "\n"
    """  }"""
    "\n"
    """  function workerAggregateLabel(key) {"""
    "\n"
    """    const text = String(key || '');"""
    "\n"
    """    if (text.startsWith('worker:')) return `worker ${text.slice('worker:'.length)}`;"""
    "\n"
    """    if (text.startsWith('raw:')) return text.slice('raw:'.length) || 'unknown worker';"""
    "\n"
    """    return workerDisplayName(text);"""
    "\n"
    """  }"""
    "\n"
    """  function workerMatchesFilter(raw, selectedWorker) {"""
    "\n"
    """    return !selectedWorker || workerAggregateKey(raw) === selectedWorker;"""
    "\n"
    """  }"""
    "\n"
    """  function workerRelationName(raw) {"""
    "\n"
    """    const parts = workerNameParts(raw);"""
    "\n"
    """    if (parts.kind === 'client' && parts.left && parts.worker) return `client ${parts.left} → worker """
    """${parts.worker}`;"""
    "\n"
    """    if (parts.kind === 'worker' && parts.left && parts.worker) return `worker ${parts.left} → worker """
    """${parts.worker}`;"""
    "\n"
    """    return workerDisplayName(raw);"""
    "\n"
    """  }"""
    "\n"
    """  function refreshControlsVisibility(select) {"""
    "\n"
    """    const controls = select?.closest('.controls');"""
    "\n"
    """    if (!controls) return;"""
    "\n"
    """    const labels = [...controls.querySelectorAll('label')];"""
    "\n"
    """    if (!labels.length) return;"""
    "\n"
    """    controls.style.display = labels.every(label => label.style.display === 'none') ? 'none' : '';"""
    "\n"
    """  }"""
    "\n"
    """  function hideEmptyFilterLabel(select, optionCount) {"""
    "\n"
    """    if (!select) return;"""
    "\n"
    """    const label = select.closest('label');"""
    "\n"
    """    if (label) {"""
    "\n"
    """      label.style.display = optionCount ? '' : 'none';"""
    "\n"
    """    } else {"""
    "\n"
    """      select.style.display = optionCount ? '' : 'none';"""
    "\n"
    """    }"""
    "\n"
    """    refreshControlsVisibility(select);"""
    "\n"
    """  }"""
    "\n"
    """  function setSelectOptions(id, defaultLabel, options) {"""
    "\n"
    """    const select = document.getElementById(id);"""
    "\n"
    """    if (!select) return null;"""
    "\n"
    """    const normalized = (options || []).map(item => Array.isArray(item) ? item : [item, item]);"""
    "\n"
    """    const current = select.value;"""
    "\n"
    """    select.innerHTML = `<option value="">${escapeHtml(defaultLabel)}</option>` +"""
    "\n"
    """      normalized.map(([value, label]) => `<option """
    """value="${escapeHtml(value)}">${escapeHtml(label)}</option>`).join('');"""
    "\n"
    """    const values = normalized.map(([value]) => String(value));"""
    "\n"
    """    select.value = values.includes(current) ? current : '';"""
    "\n"
    """    hideEmptyFilterLabel(select, normalized.length);"""
    "\n"
    """    return select;"""
    "\n"
    """  }"""
    "\n"
    """  function pctText(item) {"""
    "\n"
    """    if (!item || !item.count) return '';"""
    "\n"
    """    return `count=${item.count} p50=${item.p50} p90=${item.p90} p99=${item.p99} max=${item.max}`;"""
    "\n"
    """  }"""
    "\n"
    """  function scalePercentiles(item, scale) {"""
    "\n"
    """    if (!item || !item.count) return null;"""
    "\n"
    """    const out = {count:item.count};"""
    "\n"
    """    ['min','p50','p90','p99','max'].forEach(key => {"""
    "\n"
    """      out[key] = item[key] === undefined || item[key] === null ? null : Number((item[key] * """
    """scale).toFixed(3));"""
    "\n"
    """    });"""
    "\n"
    """    return out;"""
    "\n"
    """  }"""
    "\n"
    """  const metricLabelMap = {"""
    "\n"
    """    'access':'Access',"""
    "\n"
    """    'urma.total':'URMA total',"""
    "\n"
    """    'urma.poll_jfc':'URMA poll',"""
    "\n"
    """    'urma.notify':'URMA notify',"""
    "\n"
    """    'urma.thread_sched':'URMA sched',"""
    "\n"
    """    'latencySummary.client.process.get':'Client process GET',"""
    "\n"
    """    'latencySummary.client.process.set':'Client process SET',"""
    "\n"
    """    'latencySummary.client.rpc.get':'Client RPC GET',"""
    "\n"
    """    'latencySummary.client.rpc.create':'Client RPC Create',"""
    "\n"
    """    'latencySummary.client.rpc.publish':'Client RPC Publish',"""
    "\n"
    """    'latencySummary.client.process.memory_copy':'Client memory copy',"""
    "\n"
    """    'latencySummary.worker.process.get':'Worker process GET',"""
    "\n"
    """    'latencySummary.worker.process.remote_get':'Worker RemoteGet process',"""
    "\n"
    """    'latencySummary.worker.rpc.query_meta':'Worker QueryMeta RPC',"""
    "\n"
    """    'latencySummary.worker.rpc.remote_get':'Worker RemoteGet RPC',"""
    "\n"
    """    'latencySummary.worker.rpc.create_meta':'Worker CreateMeta RPC',"""
    "\n"
    """    'latencySummary.worker.process.publish':'Worker Publish',"""
    "\n"
    """    'custom.wlock_wait':'WLock wait',"""
    "\n"
    """    'wlock_wait':'WLock wait'"""
    "\n"
    """  };"""
    "\n"
    """  function metricLabel(name) {"""
    "\n"
    """    if (metricLabelMap[name]) return metricLabelMap[name];"""
    "\n"
    """    return String(name || '').replace(/^latencySummary\\./, '').replace(/^urma\\./, 'URMA """
    """').replace(/[._]/g, ' ');"""
    "\n"
    """  }"""
    "\n"
    """  const flowLabelMap = {"""
    "\n"
    """    'DS_KV_CLIENT_GET':'Client GET',"""
    "\n"
    """    'DS_POSIX_GET':'Worker GET',"""
    "\n"
    """    'DS_KV_CLIENT_SET':'Client SET',"""
    "\n"
    """    'DS_POSIX_SET':'Worker SET',"""
    "\n"
    """    'DS_KV_CLIENT_CREATE':'Client Create',"""
    "\n"
    """    'DS_POSIX_CREATE':'Worker Create',"""
    "\n"
    """    'DS_KV_CLIENT_PUBLISH':'Client Publish',"""
    "\n"
    """    'DS_POSIX_PUBLISH':'Worker Publish'"""
    "\n"
    """  };"""
    "\n"
    """  function flowLabel(name) {"""
    "\n"
    """    if (flowLabelMap[name]) return flowLabelMap[name];"""
    "\n"
    """    return String(name || '').replace(/^DS_/, '').replace(/_/g, ' ');"""
    "\n"
    """  }"""
    "\n"
    """  const tableSortStates = {};"""
    "\n"
    """  function sortCellValue(value, header) {"""
    "\n"
    """    const lowerHeader = String(header || '').toLowerCase();"""
    "\n"
    """    const text = String(value ?? '').replace(/<[^>]*>/g, '').trim();"""
    "\n"
    """    const distributionMatch = text.match(/\\b(max|p99|p90|p50|count)=(-?\\d+(?:\\.\\d+)?)/gi);"""
    "\n"
    """    if (distributionMatch && /(distribution|latency|access)/.test(lowerHeader)) {"""
    "\n"
    """      const scores = Object.fromEntries(distributionMatch.map(part => {"""
    "\n"
    """        const [, key, raw] = part.match(/(max|p99|p90|p50|count)=(-?\\d+(?:\\.\\d+)?)/i);"""
    "\n"
    """        return [key.toLowerCase(), Number(raw)];"""
    "\n"
    """      }));"""
    "\n"
    """      return {type:'number', value:scores.max ?? scores.p99 ?? scores.p90 ?? scores.p50 ?? scores.count """
    """?? 0, text};"""
    "\n"
    """    }"""
    "\n"
    """    if (/(time|trace|worker|edge|classification|status|access)/.test(lowerHeader) && !/(latency """
    """ms|p50|p90|p99|max|count|events|lines|traces|slow|errors|duration|total|wait|wake|poll|notify|sched|data """
    """size|cpuid)/.test(lowerHeader)) {"""
    "\n"
    """      return {type:'text', value:text.toLowerCase(), text};"""
    "\n"
    """    }"""
    "\n"
    """    if (typeof value === 'number') return {type:'number', value, text};"""
    "\n"
    """    if (/^-?\\d+(?:\\.\\d+)?$/.test(text)) return {type:'number', value:Number(text), text};"""
    "\n"
    """    return {type:'text', value:text.toLowerCase(), text};"""
    "\n"
    """  }"""
    "\n"
    """  function sortRowsForTable(rows, sort, headers=[]) {"""
    "\n"
    """    if (!sort || sort.index === undefined || sort.index === null) return rows.slice();"""
    "\n"
    """    return rows.map((row, originalIndex) => ({row, originalIndex})).sort((a, b) => {"""
    "\n"
    """      const av = sortCellValue(a.row[sort.index], headers[sort.index]);"""
    "\n"
    """      const bv = sortCellValue(b.row[sort.index], headers[sort.index]);"""
    "\n"
    """      let cmp = 0;"""
    "\n"
    """      if (av.type === 'number' && bv.type === 'number') cmp = av.value - bv.value;"""
    "\n"
    """      else cmp = av.text.localeCompare(bv.text, 'zh-CN', {numeric:true});"""
    "\n"
    """      if (cmp === 0) cmp = a.originalIndex - b.originalIndex;"""
    "\n"
    """      return sort.dir === 'desc' ? -cmp : cmp;"""
    "\n"
    """    }).map(item => item.row);"""
    "\n"
    """  }"""
    "\n"
    """  function nextSortState(current, index) {"""
    "\n"
    """    if (!current || current.index !== index) return {index, dir:'asc'};"""
    "\n"
    """    return {index, dir:current.dir === 'asc' ? 'desc' : 'asc'};"""
    "\n"
    """  }"""
    "\n"
    """  function renderTable(id, headers, rows, rowAttrs, options={}) {"""
    "\n"
    """    const table = document.getElementById(id);"""
    "\n"
    """    const sortable = options.sortable !== false;"""
    "\n"
    """    const sortState = options.sortState || tableSortStates[id];"""
    "\n"
    """    const visibleRows = options.skipSort ? rows.slice() : sortRowsForTable(rows, sortState, headers);"""
    "\n"
    """    const headerHtml = headers.map((h, index) => {"""
    "\n"
    """      const active = sortState && sortState.index === index;"""
    "\n"
    """      const mark = active ? `<span class="sort-mark">${sortState.dir === 'asc' ? '▲' : '▼'}</span>` : """
    """'<span class="sort-mark">↕</span>';"""
    "\n"
    """      return `<th class="${sortable ? 'sortable-th' : ''}" data-sort-index="${index}" aria-sort="${active """
    """? (sortState.dir === 'asc' ? 'ascending' : 'descending') : 'none'}">${escapeHtml(h)}${sortable ? mark : """
    """''}</th>`;"""
    "\n"
    """    }).join('');"""
    "\n"
    """    if (!rows.length) {"""
    "\n"
    """      table.innerHTML = `<thead><tr>${headerHtml}</tr></thead><tbody><tr><td class="muted" """
    """colspan="${headers.length}">No data</td></tr></tbody>`;"""
    "\n"
    """      return;"""
    "\n"
    """    }"""
    "\n"
    """    table.innerHTML = `<thead><tr>${headerHtml}</tr></thead>` +"""
    "\n"
    """      `<tbody>${visibleRows.map((row, idx) => `<tr ${rowAttrs ? rowAttrs(row, idx) : ''}>${row.map((cell, """
    """cellIdx) => {"""
    "\n"
    """        const cellHtml = options.formatCell ? options.formatCell(headers[cellIdx], cell, row, cellIdx) : """
    """formatCell(headers[cellIdx], cell);"""
    "\n"
    """        return `<td>${cellHtml}</td>`;"""
    "\n"
    """      }).join('')}</tr>`).join('')}</tbody>`;"""
    "\n"
    """    if (sortable) {"""
    "\n"
    """      table.querySelectorAll('th[data-sort-index]').forEach(th => th.addEventListener('click', () => {"""
    "\n"
    """        const index = Number(th.dataset.sortIndex);"""
    "\n"
    """        if (options.onSort) options.onSort(index);"""
    "\n"
    """        else {"""
    "\n"
    """          tableSortStates[id] = nextSortState(tableSortStates[id], index);"""
    "\n"
    """          renderTable(id, headers, rows, rowAttrs, options);"""
    "\n"
    """        }"""
    "\n"
    """      }));"""
    "\n"
    """    }"""
    "\n"
    """  }"""
    "\n"
    """  function formatCell(header, cell) {"""
    "\n"
    """    const text = String(cell ?? '');"""
    "\n"
    """    const lowerHeader = String(header || '').toLowerCase();"""
    "\n"
    """    const lower = text.toLowerCase();"""
    "\n"
    """    const escaped = escapeHtml(text);"""
    "\n"
    """    if (!text) return '';"""
    "\n"
    """    const numeric = Number(text);"""
    "\n"
    """    if (/(error|errors|status|classification)/.test(lowerHeader) && """
    """/(deadline|timeout|error|fail|1001|runtime)/i.test(text)) {"""
    "\n"
    """      return `<span class="cell-hot">${escaped}</span>`;"""
    "\n"
    """    }"""
    "\n"
    """    if (/status/.test(lowerHeader) && /(present|ok|success)/i.test(text)) return `<span """
    """class="cell-ok">${escaped}</span>`;"""
    "\n"
    """    if (/(latency|duration|p99|max|access|slow)/.test(lowerHeader) && Number.isFinite(numeric)) {"""
    "\n"
    """      return `<span class="${numeric >= 20 ? 'cell-hot' : numeric >= 5 ? 'cell-warn' : """
    """'cell-ok'}">${escaped}</span>`;"""
    "\n"
    """    }"""
    "\n"
    """    if (/(slow|errors|count|traces)/.test(lowerHeader) && Number.isFinite(numeric) && numeric > 0) {"""
    "\n"
    """      return `<span class="${numeric >= 20 ? 'cell-hot' : 'cell-warn'}">${escaped}</span>`;"""
    "\n"
    """    }"""
    "\n"
    """    if (/(access|latency|distribution)/.test(lowerHeader) && /(p99|max)=/i.test(text)) {"""
    "\n"
    """      return escaped.replace(/\\b(p99|max)=([\\d.]+)/gi, (match, key, value) => {"""
    "\n"
    """        const n = Number(value);"""
    "\n"
    """        const cls = Number.isFinite(n) && n >= 20 ? 'cell-hot' : Number.isFinite(n) && n >= 5 ? """
    """'cell-warn' : 'cell-ok';"""
    "\n"
    """        return `<span class="${cls}">${key}=${value}</span>`;"""
    "\n"
    """      });"""
    "\n"
    """    }"""
    "\n"
    """    if (/(deadline|timeout|error|fail|rpc deadline exceeded|status=1001)/i.test(text)) {"""
    "\n"
    """      return escaped.replace(/(deadline|timeout|error|fail|RPC deadline exceeded|status=1001)/gi, '<span """
    """class="cell-hot">$1</span>');"""
    "\n"
    """    }"""
    "\n"
    """    return escaped;"""
    "\n"
    """  }"""
    "\n"
    """  const pagedTables = {};"""
    "\n"
    """  function renderPagedTable(id, pagerId, headers, rows, rowAttrs, pageSize=5) {"""
    "\n"
    """    const state = pagedTables[id] || (pagedTables[id] = {page:0, sort:null});"""
    "\n"
    """    const sortedRows = sortRowsForTable(rows, state.sort, headers);"""
    "\n"
    """    const pages = Math.max(1, Math.ceil(sortedRows.length / pageSize));"""
    "\n"
    """    state.page = Math.min(Math.max(state.page, 0), pages - 1);"""
    "\n"
    """    const start = state.page * pageSize;"""
    "\n"
    """    renderTable(id, headers, sortedRows.slice(start, start + pageSize), (row, idx) => rowAttrs ? """
    """rowAttrs(row, start + idx) : '', {"""
    "\n"
    """      skipSort:true,"""
    "\n"
    """      sortState:state.sort,"""
    "\n"
    """      onSort:index => {"""
    "\n"
    """        state.sort = nextSortState(state.sort, index);"""
    "\n"
    """        state.page = 0;"""
    "\n"
    """        renderPagedTable(id, pagerId, headers, rows, rowAttrs, pageSize);"""
    "\n"
    """      }"""
    "\n"
    """    });"""
    "\n"
    """    const pager = document.getElementById(pagerId);"""
    "\n"
    """    if (!pager) return;"""
    "\n"
    """    pager.innerHTML = `<button ${state.page <= 0 ? 'disabled' : ''} data-act="prev">上一页</button><span>第 """
    """${state.page + 1} / ${pages} 页，共 ${rows.length} 行</span><button ${state.page >= pages - 1 ? 'disabled' : """
    """''} data-act="next">下一页</button>`;"""
    "\n"
    """    pager.querySelectorAll('button').forEach(button => button.addEventListener('click', () => {"""
    "\n"
    """      state.page += button.dataset.act === 'prev' ? -1 : 1;"""
    "\n"
    """      renderPagedTable(id, pagerId, headers, rows, rowAttrs, pageSize);"""
    "\n"
    """    }));"""
    "\n"
    """  }"""
    "\n"
    """  function severityClass(value, warn=5, hot=20) {"""
    "\n"
    """    const n = Number(value || 0);"""
    "\n"
    """    return n >= hot ? 'hotrow' : n >= warn ? 'warnrow' : '';"""
    "\n"
    """  }"""
    "\n"
    """  function maxLatencyFromText(value) {"""
    "\n"
    """    const text = String(value || '');"""
    "\n"
    """    const keyed = [...text.matchAll(/\\b(?:p50|p90|p99|max)=(-?\\d+(?:\\.\\d+)?)/gi)].map(match => """
    """Number(match[1]));"""
    "\n"
    """    const ms = [...text.matchAll(/=(-?\\d+(?:\\.\\d+)?)ms\\b/gi)].map(match => Number(match[1]));"""
    "\n"
    """    const values = keyed.concat(ms).filter(Number.isFinite);"""
    "\n"
    """    return values.length ? Math.max(...values) : 0;"""
    "\n"
    """  }"""
    "\n"
    """  function severityCellClass(value) {"""
    "\n"
    """    const cls = severityClass(value);"""
    "\n"
    """    return cls === 'hotrow' ? 'cell-hot' : cls === 'warnrow' ? 'cell-warn' : '';"""
    "\n"
    """  }"""
    "\n"
    """  function highlightLatencyTokens(value) {"""
    "\n"
    """    return escapeHtml(value)"""
    "\n"
    """      .replace(/\\b((?:p50|p90|p99|max)=)(-?\\d+(?:\\.\\d+)?)/gi, (match, prefix, raw) => {"""
    "\n"
    """        const cls = severityCellClass(Number(raw));"""
    "\n"
    """        return cls ? `<span class="${cls}">${prefix}${raw}</span>` : match;"""
    "\n"
    """      })"""
    "\n"
    """      .replace(/(=)(-?\\d+(?:\\.\\d+)?)(ms\\b)/gi, (match, prefix, raw, suffix) => {"""
    "\n"
    """        const cls = severityCellClass(Number(raw));"""
    "\n"
    """        return cls ? `${prefix}<span class="${cls}">${raw}${suffix}</span>` : match;"""
    "\n"
    """      });"""
    "\n"
    """  }"""
    "\n"
    """  function selectedTraceSummaryValueHtml(field, value) {"""
    "\n"
    """    const text = String(value ?? '');"""
    "\n"
    """    if (!text) return '';"""
    "\n"
    """    if (field === 'errors' && value !== '{}') return `<span class="cell-hot">${escapeHtml(text)}</span>`;"""
    "\n"
    """    if (field === 'classification' && /deadline|error|slow|tail|timeout/i.test(value)) return `<span """
    """class="cell-hot">${escapeHtml(text)}</span>`;"""
    "\n"
    """    const summarySeverity = severityClass(maxLatencyFromText(value));"""
    "\n"
    """    if (/(access|latencySummary)/i.test(field) && summarySeverity) return highlightLatencyTokens(value);"""
    "\n"
    """    return formatCell('value', value);"""
    "\n"
    """  }"""
    "\n"
    """  const stageLabelMap = {"""
    "\n"
    """    'read.client_to_entry_worker':'Client→Entry RPC',"""
    "\n"
    """    'read.entry_to_meta_worker':'Entry→Meta RPC',"""
    "\n"
    """    'read.entry_to_data_worker':'Entry→Data RPC',"""
    "\n"
    """    'read.data_worker_ub_write':'URMA Write',"""
    "\n"
    """    'write.client_to_entry_createbuffer':'Client→Entry CreateBuffer',"""
    "\n"
    """    'write.client_memory_copy':'Client Memory Copy',"""
    "\n"
    """    'write.client_to_entry_publish':'Client→Entry Publish',"""
    "\n"
    """    'write.entry_to_meta_publish':'Entry→Meta Publish'"""
    "\n"
    """  };"""
    "\n"
    """  const stageColorMap = {"""
    "\n"
    """    'read.entry_to_meta_worker':'#0891b2',"""
    "\n"
    """    'read.entry_to_data_worker':'#ea580c',"""
    "\n"
    """    'read.data_worker_ub_write':'#059669',"""
    "\n"
    """    'write.client_to_entry_createbuffer':'#2563eb',"""
    "\n"
    """    'write.client_memory_copy':'#7c3aed',"""
    "\n"
    """    'write.client_to_entry_publish':'#ca8a04',"""
    "\n"
    """    'write.entry_to_meta_publish':'#dc2626'"""
    "\n"
    """  };"""
    "\n"
    """  function stageDisplayName(stage) {"""
    "\n"
    """    if (stageLabelMap[stage]) return stageLabelMap[stage];"""
    "\n"
    """    return String(stage || '').replace(/^(read|write)\\./, '').replace(/_/g, ' ');"""
    "\n"
    """  }"""
    "\n"
    """  function stageDetailText(stage) {"""
    "\n"
    """    const display = stageDisplayName(stage);"""
    "\n"
    """    return display === stage ? display : `${display} (${stage})`;"""
    "\n"
    """  }"""
    "\n"
    """  function traceOperation(item) {"""
    "\n"
    """    const stageNames = (item.stage_breakdown || []).map(stage => stage.stage || '');"""
    "\n"
    """    const flowNames = Object.keys(item.flows || {});"""
    "\n"
    """    const hasRead = stageNames.some(name => name.startsWith('read.')) || flowNames.some(name => """
    """name.includes('GET'));"""
    "\n"
    """    const hasWrite = stageNames.some(name => name.startsWith('write.')) || flowNames.some(name => """
    """/(SET|CREATE|PUBLISH)/.test(name));"""
    "\n"
    """    return hasRead && hasWrite ? 'mixed' : hasRead ? 'read' : hasWrite ? 'write' : 'unknown';"""
    "\n"
    """  }"""
    "\n"
    """  function operationMatches(item) {"""
    "\n"
    """    if (!activeOperation) return true;"""
    "\n"
    """    const op = traceOperation(item);"""
    "\n"
    """    return op === activeOperation || op === 'mixed';"""
    "\n"
    """  }"""
    "\n"
    """  function hasTraceFailure(item) {"""
    "\n"
    """    return Object.keys(item.errors || {}).length > 0 || """
    """/deadline|error|fail|timeout/i.test(String(item.classification || ''));"""
    "\n"
    """  }"""
    "\n"
    """  function statusMatchesTrace(item, requestStatus) {"""
    "\n"
    """    if (!requestStatus) return true;"""
    "\n"
    """    const failed = hasTraceFailure(item);"""
    "\n"
    """    return requestStatus === 'failed' ? failed : !failed;"""
    "\n"
    """  }"""
    "\n"
    """  function traceStartTime(item) {"""
    "\n"
    """    return item.first_ts || item.last_ts || '';"""
    "\n"
    """  }"""
    "\n"
    """  function stageMatchesOperation(stageName) {"""
    "\n"
    """    return !activeOperation || String(stageName || '').startsWith(`${activeOperation}.`);"""
    "\n"
    """  }"""
    "\n"
    """  function flowOperation(name) {"""
    "\n"
    """    const text = String(name || '');"""
    "\n"
    """    if (/GET/.test(text)) return 'read';"""
    "\n"
    """    if (/(SET|CREATE|PUBLISH)/.test(text)) return 'write';"""
    "\n"
    """    return 'unknown';"""
    "\n"
    """  }"""
    "\n"
    """  function metricOperation(name) {"""
    "\n"
    """    const text = String(name || '').toLowerCase();"""
    "\n"
    """    if (text === 'access') return 'both';"""
    "\n"
    """    if (text.includes('urma') || text.includes('remote_get') || text.includes('query_meta') || """
    """text.endsWith('.get') || text.includes('process.get')) return 'read';"""
    "\n"
    """    if (text.includes('memory_copy') || text.includes('publish') || text.includes('create') || """
    """text.endsWith('.set') || text.includes('process.set')) return 'write';"""
    "\n"
    """    return 'unknown';"""
    "\n"
    """  }"""
    "\n"
    """  function percentileFromValues(values) {"""
    "\n"
    """    const nums = values.map(Number).filter(Number.isFinite).sort((a,b) => a-b);"""
    "\n"
    """    if (!nums.length) return null;"""
    "\n"
    """    const pick = pct => nums[Math.min(nums.length - 1, Math.floor((nums.length - 1) * pct))];"""
    "\n"
    """    return {count:nums.length,min:nums[0],p50:pick(.5),p90:pick(.9),p99:pick(.99),max:nums[nums.length - """
    """1]};"""
    "\n"
    """  }"""
    "\n"
    """  function traceRowsForOperation(operation) {"""
    "\n"
    """    return traceRows.filter(([, item]) => {"""
    "\n"
    """      const op = traceOperation(item);"""
    "\n"
    """      return op === operation || op === 'mixed';"""
    "\n"
    """    });"""
    "\n"
    """  }"""
    "\n"
    """  function operationAccessPercentiles(operation) {"""
    "\n"
    """    return percentileFromValues(traceRowsForOperation(operation).map(([, item]) => """
    """item.access_latency_ms?.max ?? item.access_latency_ms?.p99 ?? item.access_latency_ms?.p50));"""
    "\n"
    """  }"""
    "\n"
    """  const palette = ['#2563eb','#ea580c','#059669','#7c3aed','#dc2626','#0891b2','#ca8a04','#64748b'];"""
    "\n"
    """  const chartTextStyle = {fontSize:13, color:'#475569'};"""
    "\n"
    """  function axisBase(title, extra) {"""
    "\n"
    """    return Object.assign({"""
    "\n"
    """      title:{show:false,text:title},"""
    "\n"
    """      color:palette,"""
    "\n"
    """      textStyle:chartTextStyle,"""
    "\n"
    """      grid:{left:58,right:24,top:72,bottom:76,containLabel:true},"""
    "\n"
    """      legend:{top:30,type:'scroll'},"""
    "\n"
    """      tooltip:{trigger:'axis', axisPointer:{type:'shadow'}, confine:true},"""
    "\n"
    """      dataZoom:[{type:'inside'},{type:'slider', height:18, bottom:18}]"""
    "\n"
    """    }, extra || {});"""
    "\n"
    """  }"""
    "\n"
    """  function wideHorizontalGrid(left=120, top=56, bottom=34) {"""
    "\n"
    """    return {left:left,right:24,top:top,bottom:bottom,containLabel:false};"""
    "\n"
    """  }"""
    "\n"
    """  function noDataOption(title) {"""
    "\n"
    """    return {title:{text:title, left:'center', top:'center', textStyle:Object.assign({}, chartTextStyle, """
    """{color:'#94a3b8'})}};"""
    "\n"
    """  }"""
    "\n"
    """  const chartRegistry = new Map();"""
    "\n"
    """  const chartRenderers = new Map();"""
    "\n"
    """  function chart(id, option) {"""
    "\n"
    """    const node = document.getElementById(id);"""
    "\n"
    """    if (!node) return;"""
    "\n"
    """    if (!window.echarts) {"""
    "\n"
    """      node.innerHTML = '<div class="muted">ECharts failed to load; tables below remain available.</div>';"""
    "\n"
    """      return;"""
    "\n"
    """    }"""
    "\n"
    """    const instance = echarts.getInstanceByDom(node) || echarts.init(node);"""
    "\n"
    """    instance.setOption(option, true);"""
    "\n"
    """    instance.resize();"""
    "\n"
    """    chartRegistry.set(id, instance);"""
    "\n"
    """  }"""
    "\n"
    """  function installResponsiveCharts() {"""
    "\n"
    """    let timer = null;"""
    "\n"
    """    const scheduleChartResize = () => {"""
    "\n"
    """      window.clearTimeout(timer);"""
    "\n"
    """      timer = window.setTimeout(() => {"""
    "\n"
    """        chartRenderers.forEach(renderer => renderer());"""
    "\n"
    """        chartRegistry.forEach(instance => instance.resize());"""
    "\n"
    """      }, 80);"""
    "\n"
    """    };"""
    "\n"
    """    window.addEventListener('resize', scheduleChartResize);"""
    "\n"
    """    if (window.ResizeObserver) {"""
    "\n"
    """      const observer = new ResizeObserver(scheduleChartResize);"""
    "\n"
    """      document.querySelectorAll('.chart').forEach(node => observer.observe(node));"""
    "\n"
    """    }"""
    "\n"
    """  }"""
    "\n"
    """  function autoCenterFlowGraph(chartInstance) {"""
    "\n"
    """    if (!chartInstance) return;"""
    "\n"
    """    if (chartInstance.resize) chartInstance.resize();"""
    "\n"
    """  }"""
    "\n"
    """  function downloadText(filename, text) {"""
    "\n"
    """    const blob = new Blob([text], {type:'text/plain;charset=utf-8'});"""
    "\n"
    """    const link = document.createElement('a');"""
    "\n"
    """    link.href = URL.createObjectURL(blob);"""
    "\n"
    """    link.download = filename;"""
    "\n"
    """    document.body.appendChild(link);"""
    "\n"
    """    link.click();"""
    "\n"
    """    URL.revokeObjectURL(link.href);"""
    "\n"
    """    link.remove();"""
    "\n"
    """  }"""
    "\n"
    """  function highlightLogLine(line) {"""
    "\n"
    """    const text = escapeHtml(line);"""
    "\n"
    """    return text"""
    "\n"
    """      .replace(/(\\|\\s*E\\s*\\|)/g, '<span class="log-tag log-error">$1</span>')"""
    "\n"
    """      .replace(/(\\|\\s*F\\s*\\|)/g, '<span class="log-tag log-error">$1</span>')"""
    "\n"
    """      .replace(/(\\|\\s*W\\s*\\|)/g, '<span class="log-tag log-deadline">$1</span>')"""
    "\n"
    """      .replace(/\\b(ERROR|FATAL|K_RUNTIME_ERROR|K_TRY_AGAIN|status[:=]?\\s*1001|RPC """
    """failed|error_code=2004|max_concurrency)\\b/gi, '<span class="log-tag log-error">$1</span>')"""
    "\n"
    """      .replace(/(\\[?URMA_ELAPSED_(?:TOTAL|POLL_JFC|NOTIFY|THREAD_SHED)\\]?|URMA_WAIT_TIMEOUT|urma_[a-z_]+"""
    """|wait os sched[^:,]*?(?:\\([^)]*\\))?:\\s*[\\d.]+ms|wakeSchedLatencyUs:\\s*[\\d.]+|srcChipInflight:\\s*\\{"""
    """[^}]*\\}|request id[:=]?\\s*\\d+|src address:\\s*[^,\\s]+|target """
    """address:\\s*[^,\\s]+|dataSize[:=]?\\s*\\d+|cpuid[:=]?\\s*\\d+|inflight[_a-z]*[:=]?\\s*\\d+)/gi, '<span """
    """class="log-tag log-urma">$1</span>')"""
    "\n"
    """      .replace(/(\\[?(?:(?:ZMQ|BRPC)_)?RPC_FRAMEWORK_SLOW\\]?|server_exec_us=\\d+|network_residual_us=\\d+"""
    """|client_req_framework_us=\\d+|client_rsp_framework_us=\\d+|remote_processing_us=\\d+)/gi, '<span """
    """class="log-tag log-rpc">$1</span>')"""
    "\n"
    """      .replace(/(latencySummary|client\\.rpc\\.[a-z_]+|worker\\.rpc\\.[a-z_]+|worker\\.process\\.[a-z_]+|c"""
    """lient\\.process\\.[a-z_]+)/gi, '<span class="log-tag log-latency">$1</span>')"""
    "\n"
    """      .replace(/(deadline exceeded|RPC timed out|\\btimeout\\b|20ms deadline)/gi, '<span class="log-tag """
    """log-deadline">$1</span>')"""
    "\n"
    """      .replace(/(WLock[^,;\\n]*(?:cost|elapsed|time|耗时)[:=\\s]*[\\d.]+\\s*(?:us|ms))/gi, '<span """
    """class="log-tag log-slow">$1</span>')"""
    "\n"
    """      .replace(/(cost(?:Us)?[:=]?\\s*[\\d.]+\\s*(?:us|ms)?|totalCost:\\s*[\\d.]+ms|[A-Za-z0-9_.-]+:\\s*\\d"""
    """{4,})/gi, '<span class="log-tag log-slow">$1</span>')"""
    "\n"
    """      .replace(/(RemotePull|Remote """
    """done|BatchGetObjectRemote|ProcessGetObjectRequest|CreateBuffer|Publish|QueryMeta|GetObjMetaInfo)/gi, """
    """'<span class="log-tag log-field">$1</span>');"""
    "\n"
    """  }"""
    "\n"
    """  const componentLabels = {"""
    "\n"
    """    client:'Client 侧日志',"""
    "\n"
    """    entry_worker:'Entry Worker 日志',"""
    "\n"
    """    meta_worker:'Meta Worker 观测',"""
    "\n"
    """    data_worker:'Data Worker / UB 日志',"""
    "\n"
    """    context:'Trace Context'"""
    "\n"
    """  };"""
    "\n"
    """  function classifyEvidenceComponent(e) {"""
    "\n"
    """    const text = `${e.member || ''} ${e.text || ''}`;"""
    "\n"
    """    if (/URMA_ELAPSED|urma_manager|urma_|target address|src address/i.test(text)) return 'data_worker';"""
    "\n"
    """    if (/MasterOCService\\.QueryMeta|Query metadata from master|GetObjMetaInfo|meta[_ """
    """-]?worker/i.test(text)) return 'meta_worker';"""
    "\n"
    """    if (/\\/collected\\/|ds_client_|object_client_impl|client_worker_remote_api|DS_KV_CLIENT|Client\\/Work"""
    """erRpc/i.test(text)) return 'client';"""
    "\n"
    """    if (/collected_worker_logs|DS_POSIX|worker_oc_service|BatchGetObjectRemote|Remote """
    """done|access\\.log/i.test(text)) return 'entry_worker';"""
    "\n"
    """    return 'context';"""
    "\n"
    """  }"""
    "\n"
    """  function traceLogBlockSummary(group) {"""
    "\n"
    """    const text = group.lines.join('\\n');"""
    "\n"
    """    const focus = [];"""
    "\n"
    """    const add = (label, tone='') => {"""
    "\n"
    """      if (!focus.some(item => item.label === label)) focus.push({label, tone});"""
    "\n"
    """    };"""
    "\n"
    """    if (/ERROR|FATAL|Get error|Set """
    """error|status[:=]?\\s*1001|error_code=2004|max_concurrency/i.test(text)) add('error/status', 'hot');"""
    "\n"
    """    if (/error_code=2004|max_concurrency/i.test(text)) add('RPC 线程数/并发不足', 'hot');"""
    "\n"
    """    if (/deadline exceeded|RPC timed out|\\btimeout\\b|20ms deadline/i.test(text)) """
    """add('deadline/timeout', 'hot');"""
    "\n"
    """    if (/RPC_FRAMEWORK_SLOW|server_exec_us=|network_residual_us=|remote_processing_us=/i.test(text)) """
    """add('RPC slow', 'warn');"""
    "\n"
    """    if (/\\bWLock\\b/i.test(text)) add('WLock 耗时', 'warn');"""
    "\n"
    """    if (/latencySummary|client\\.rpc\\.|worker\\.rpc\\.|worker\\.process\\./i.test(text)) """
    """add('latencySummary');"""
    "\n"
    """    if (/URMA_ELAPSED|URMA_WAIT_TIMEOUT|\\burma[_a-z]*\\b|\\bUB\\b/i.test(text)) add('URMA/UB', 'warn');"""
    "\n"
    """    if (/RemotePull|Remote done|BatchGetObjectRemote/i.test(text)) add('RemoteGet');"""
    "\n"
    """    if (/QueryMeta|GetObjMetaInfo|MasterOCService/i.test(text)) add('QueryMeta');"""
    "\n"
    """    if (/CreateBuffer|Publish/i.test(text)) add('Write path');"""
    "\n"
    """    const costs = [...text.matchAll(/(?:cost(?:Us)?|totalCost|framework_us|e2e_us|server_exec_us|network_r"""
    """esidual_us|remote_processing_us)[:=]?\\s*([\\d.]+)\\s*(us|ms)?/gi)]"""
    "\n"
    """      .map(match => {"""
    "\n"
    """        const raw = Number(match[1]);"""
    "\n"
    """        if (!Number.isFinite(raw)) return null;"""
    "\n"
    """        const unit = (match[2] || '').toLowerCase();"""
    "\n"
    """        return unit === 'us' || /(?:costUs|_us)/i.test(match[0]) ? raw / 1000 : raw;"""
    "\n"
    """      })"""
    "\n"
    """      .filter(value => value !== null);"""
    "\n"
    """    if (costs.length) add(`max ${Math.max(...costs).toFixed(3)}ms`, Math.max(...costs) >= 20 ? 'hot' : """
    """'warn');"""
    "\n"
    """    const ips = [...new Set([...text.matchAll(/\\b\\d{1,3}(?:\\.\\d{1,3}){3}(?::\\d+)?\\b/g)].map(match """
    """=> match[0]))].slice(0, 2);"""
    "\n"
    """    ips.forEach(ip => add(ip));"""
    "\n"
    """    if (!focus.length) add('按原始时间顺序看');"""
    "\n"
    """    return '<span>重点:</span>' + focus.slice(0, 7).map(item =>"""
    "\n"
    """      `<span class="trace-log-focus ${escapeHtml(item.tone)}">${escapeHtml(item.label)}</span>`"""
    "\n"
    """    ).join('');"""
    "\n"
    """  }"""
    "\n"
    """  function extractLatencyDetails(text) {"""
    "\n"
    """    const details = [];"""
    "\n"
    """    const seen = new Set();"""
    "\n"
    """    const add = (label, value, unitHint='') => {"""
    "\n"
    """      const raw = Number(value);"""
    "\n"
    """      if (!Number.isFinite(raw)) return;"""
    "\n"
    """      const unit = (unitHint || '').toLowerCase();"""
    "\n"
    """      const ms = unit === 'us' || /(?:costUs|_us|LatencyUs)$/i.test(label) ? raw / 1000 : raw;"""
    "\n"
    """      const key = `${label}:${ms.toFixed(3)}`;"""
    "\n"
    """      if (!seen.has(key)) {"""
    "\n"
    """        seen.add(key);"""
    "\n"
    """        details.push({label, ms});"""
    "\n"
    """      }"""
    "\n"
    """    };"""
    "\n"
    """    for (const match of """
    """text.matchAll(/\\b(costUs|cost|totalCost|framework_us|e2e_us|server_exec_us|network_residual_us|remote_pro"""
    """cessing_us|wakeSchedLatencyUs)[:=]?\\s*([\\d.]+)\\s*(us|ms)?/gi)) {"""
    "\n"
    """      add(match[1], match[2], match[3] || '');"""
    "\n"
    """    }"""
    "\n"
    """    for (const match of """
    """text.matchAll(/\\b(WLock[^,;\\n]{0,80}?(?:cost|elapsed|time|耗时))[:=\\s]*([\\d.]+)\\s*(us|ms)/gi)) {"""
    "\n"
    """      add(match[1].trim(), match[2], match[3] || '');"""
    "\n"
    """    }"""
    "\n"
    """    for (const match of text.matchAll(/\\b([A-Za-z][A-Za-z0-9_. -]{1,80})\\s*:\\s*([\\d.]+)\\s*ms\\b/g)) {"""
    "\n"
    """      add(match[1].trim(), match[2], 'ms');"""
    "\n"
    """    }"""
    "\n"
    """    return details.sort((a,b) => b.ms - a.ms).slice(0, 8);"""
    "\n"
    """  }"""
    "\n"
    """  function traceLatencyClass(item) {"""
    "\n"
    """    const value = Number(item?.ms);"""
    "\n"
    """    if (!Number.isFinite(value)) return '';"""
    "\n"
    """    return value >= 20 ? 'trace-latency-hot' : value >= 5 ? 'trace-latency-warn' : '';"""
    "\n"
    """  }"""
    "\n"
    """  function renderTraceLatencyValue(item) {"""
    "\n"
    """    const value = `${Number(item.ms).toFixed(3)}ms`;"""
    "\n"
    """    const cls = traceLatencyClass(item);"""
    "\n"
    """    return cls ? `<span class="${cls}">${escapeHtml(value)}</span>` : escapeHtml(value);"""
    "\n"
    """  }"""
    "\n"
    """  function extractDirectionDetails(text) {"""
    "\n"
    """    const out = [];"""
    "\n"
    """    const patterns = ["""
    "\n"
    """      /\\bsrc=([^,\\s]+),\\s*dst=([^,\\s]+)/gi,"""
    "\n"
    """      /\\bsrc address:\\s*([^,\\s]+),\\s*target address:\\s*([^,\\s]+)/gi,"""
    "\n"
    """      /\\bsource address:\\s*([^,\\s]+),\\s*dst address:\\s*([^,\\s]+)/gi,"""
    "\n"
    """      /Get->\\[([^\\]]+)\\]/gi"""
    "\n"
    """    ];"""
    "\n"
    """    patterns.forEach(regex => {"""
    "\n"
    """      for (const match of text.matchAll(regex)) {"""
    "\n"
    """        const right = match[2] || match[1];"""
    "\n"
    """        const left = match[2] ? match[1] : 'client';"""
    "\n"
    """        const value = `${left} → ${right}`;"""
    "\n"
    """        if (!out.includes(value)) out.push(value);"""
    "\n"
    """      }"""
    "\n"
    """    });"""
    "\n"
    """    return out.slice(0, 5);"""
    "\n"
    """  }"""
    "\n"
    """  function extractRemainingTimeWarnings(text) {"""
    "\n"
    """    const warnings = [];"""
    "\n"
    """    for (const match of text.matchAll(/\\b(?:remainingTime|start remainingTime)[:=]?\\s*([\\d.]+)/gi)) {"""
    "\n"
    """      const value = Number(match[1]);"""
    "\n"
    """      if (Number.isFinite(value) && value > 20) warnings.push(`remainingTime 远大于 20ms: ${value}ms`);"""
    "\n"
    """    }"""
    "\n"
    """    return [...new Set(warnings)].slice(0, 5);"""
    "\n"
    """  }"""
    "\n"
    """  function traceLogBlockDetails(group) {"""
    "\n"
    """    const text = group.lines.join('\\n');"""
    "\n"
    """    const rows = [];"""
    "\n"
    """    const remaining = extractRemainingTimeWarnings(text);"""
    "\n"
    """    if (remaining.length) rows.push(`<div><b>告警:</b> ${remaining.map(escapeHtml).join('；')}</div>`);"""
    "\n"
    """    const directions = extractDirectionDetails(text);"""
    "\n"
    """    if (directions.length) rows.push(`<div><b>方向:</b> ${directions.map(escapeHtml).join('；')}</div>`);"""
    "\n"
    """    const latencies = extractLatencyDetails(text);"""
    "\n"
    """    if (latencies.length) rows.push(`<div><b>耗时明细:</b> ${latencies.map(item => """
    """`${escapeHtml(item.label)}=${renderTraceLatencyValue(item)}`).join('；')}</div>`);"""
    "\n"
    """    return rows.length ? `<div class="trace-log-details">${rows.join('')}</div>` : '';"""
    "\n"
    """  }"""
    "\n"
    """  function renderTraceLogBlocks(evidence) {"""
    "\n"
    """    const groups = [];"""
    "\n"
    """    (evidence || []).forEach(e => {"""
    "\n"
    """      const component = classifyEvidenceComponent(e);"""
    "\n"
    """      const line = `${e.member}:${e.line} ${e.text}`;"""
    "\n"
    """      const last = groups[groups.length - 1];"""
    "\n"
    """      if (!last || last.component !== component) {"""
    "\n"
    """        groups.push({component, lines: []});"""
    "\n"
    """      }"""
    "\n"
    """      groups[groups.length - 1].lines.push(line);"""
    "\n"
    """    });"""
    "\n"
    """    if (!groups.length) return '<div class="muted">No selected trace log evidence.</div>';"""
    "\n"
    """    return groups.map(group =>"""
    "\n"
    """      `<div class="trace-log-block trace-log-${escapeHtml(group.component)}">` +"""
    "\n"
    """      `<h4><span class="trace-log-heading"><span>${escapeHtml(componentLabels[group.component] || """
    """group.component)}</span><span class="trace-log-summary">${traceLogBlockSummary(group)}</span></span><span """
    """class="trace-log-count">${group.lines.length} lines</span></h4>` +"""
    "\n"
    """      traceLogBlockDetails(group) +"""
    "\n"
    """      `<pre>${group.lines.map(highlightLogLine).join('\\n')}</pre></div>`"""
    "\n"
    """    ).join('');"""
    "\n"
    """  }"""
    "\n"
    """  const access = dim.latency_ms?.access || {};"""
    "\n"
    """  const totalErrors = Object.values(dim.errors || {}).reduce((a,b) => a + b, 0);"""
    "\n"
    """  const topClass = Object.entries(dim.classifications || {}).sort((a,b) => b[1] - a[1])[0] || ['unknown', """
    """0];"""
    "\n"
    """  const diagnosis = dim.diagnosis || {};"""
    "\n"
    """  const recommendations = dim.recommendations || [];"""
    "\n"
    """  const sourceAppendix = dim.source_appendix || [];"""
    "\n"
    """  const flowStages = dim.flow_stages || {nodes: [], edges: [], read:{nodes: [], edges: []}, write:{nodes: """
    """[], edges: []}};"""
    "\n"
    """  const readFlowStages = flowStages.read || {nodes: flowStages.nodes || [], edges: []};"""
    "\n"
    """  const writeFlowStages = flowStages.write || {nodes: flowStages.nodes || [], edges: []};"""
    "\n"
    """  document.getElementById('log-highlight-legend').innerHTML = ["""
    "\n"
    """    ['log-error','ERROR/status'],"""
    "\n"
    """    ['log-deadline','deadline/timeout'],"""
    "\n"
    """    ['log-rpc','RPC slow'],"""
    "\n"
    """    ['log-latency','latencySummary'],"""
    "\n"
    """    ['log-urma','URMA/UB'],"""
    "\n"
    """    ['log-slow','>=阈值耗时'],"""
    "\n"
    """    ['log-field','关键流程']"""
    "\n"
    """  ].map(([cls,label]) => `<span class="log-tag ${cls}">${label}</span>`).join('');"""
    "\n"
    """  document.getElementById('report-subtitle').innerHTML ="""
    "\n"
    """    `输入日志解析得到 <b>${report.trace_count}</b> 条 trace，错误标记 <b>${totalErrors}</b> 个。` +"""
    "\n"
    """    `当前主分类为 <b>${escapeHtml(topClass[0])}</b>，access p99 为 <b>${access.p99 ?? ''}ms</b>。`;"""
    "\n"
    """  const diagnosisRows = ['symptom_line','latency_line','evidence_boundary','customer_expression']"""
    "\n"
    """    .map(key => diagnosis[key])"""
    "\n"
    """    .filter(Boolean);"""
    "\n"
    """  document.getElementById('report-insight').innerHTML ="""
    "\n"
    """    `<b>核心判断：</b>${escapeHtml(diagnosis.customer_expression?.text || '请结合错误线、慢时延线和证据边界阅读。')}`;"""
    "\n"
    """  document.getElementById('diagnosis-list').innerHTML = diagnosisRows"""
    "\n"
    """    .map(item => `<li><b>${escapeHtml(item.label)}：</b>${escapeHtml(item.text)}</li>`).join('');"""
    "\n"
    """  document.getElementById('summary').innerHTML = ["""
    "\n"
    """    ['trace_count', report.trace_count, 'parsed traces'],"""
    "\n"
    """    ['errors', totalErrors, 'total error markers'],"""
    "\n"
    """    ['access p99 ms', access.p99 ?? '', 'client/access latency'],"""
    "\n"
    """    ['code_ref', report.code_ref, 'source reference']"""
    "\n"
    """  ].map(([k,v,hint]) => k === 'code_ref'"""
    "\n"
    """    ? `<div class="card code-ref-card"><div class="k">${escapeHtml(k)}</div><div class="v" """
    """title="${escapeHtml(v)}">${escapeHtml(shortCodeRef(report.code_ref))}</div><div """
    """class="n">${escapeHtml(hint)}</div></div>`"""
    "\n"
    """    : `<div class="card"><div class="k">${escapeHtml(k)}</div><div class="v">${escapeHtml(v)}</div><div """
    """class="n">${escapeHtml(hint)}</div></div>`).join('');"""
    "\n"
    """  renderTable('run-metadata-table', ['field','value'], ["""
    "\n"
    """    ['case_name', manifest.case_name || ''],"""
    "\n"
    """    ['scenario', manifest.scenario || ''],"""
    "\n"
    """    ['analysis_created_at', manifest.analysis_created_at || ''],"""
    "\n"
    """    ['code_ref', report.code_ref || manifest.code_ref || ''],"""
    "\n"
    """    ['input_document', manifest.input_document || 'inputs.md'],"""
    "\n"
    """    ['raw_inputs', 'raw/inputs'],"""
    "\n"
    """    ['raw_extracted', 'raw/extracted'],"""
    "\n"
    """    ['inputs', (manifest.inputs || []).map(item => `${item.path} (${item.size || 0} bytes)`).join('\\n')]"""
    "\n"
    """  ].filter(row => row[1]), null, {"""
    "\n"
    """    formatCell:(header, cell, row) => header === 'value' ? formatMetadataValue(row[0], cell) : """
    """formatCell(header, cell)"""
    "\n"
    """  });"""
    "\n"
    """  const classificationRows = Object.entries(dim.classifications || {}).sort((a,b) => b[1]-a[1]);"""
    "\n"
    """  const errorRows = Object.entries(dim.errors || {}).sort((a,b) => b[1]-a[1]);"""
    "\n"
    """  const cohortRows = Object.entries(dim.cohorts || {}).sort((a,b) => b[1].trace_count - a[1].trace_count);"""
    "\n"
    """  const coverageRows = Object.entries(dim.coverage?.surfaces || {}).sort((a,b) => """
    """a[0].localeCompare(b[0]));"""
    "\n"
    """  renderTable('coverage-table', ['log surface','events','status','reading'], """
    """coverageRows.map(([name,item]) => ["""
    "\n"
    """    name,"""
    "\n"
    """    item.events || 0,"""
    "\n"
    """    item.status || 'missing',"""
    "\n"
    """    item.status === 'present' ? '可作为 observed evidence 进入定界' : '观测盲区，不能把缺失当根因'"""
    "\n"
    """  ]));"""
    "\n"
    """  renderTable('cohort-table', ['cohort','traces','classifications','errors','access'], """
    """cohortRows.map(([name,item]) => ["""
    "\n"
    """    name,"""
    "\n"
    """    item.trace_count,"""
    "\n"
    """    JSON.stringify(item.classifications || {}),"""
    "\n"
    """    JSON.stringify(item.errors || {}),"""
    "\n"
    """    pctText(item.access_latency_ms)"""
    "\n"
    """  ]));"""
    "\n"
    """  renderTable('classification-table', ['classification','count'], classificationRows);"""
    "\n"
    """  renderTable('recommendation-table', ['category','title','detail'], recommendations.map(item => ["""
    "\n"
    """    item.category,"""
    "\n"
    """    item.title,"""
    "\n"
    """    item.detail"""
    "\n"
    """  ]));"""
    "\n"
    """  function sourceAppendixByScope(scope) {"""
    "\n"
    """    return sourceAppendix.filter(item => (item.scope || '通用') === scope);"""
    "\n"
    """  }"""
    "\n"
    """  function renderSourceAppendixTables() {"""
    "\n"
    """    const headers = ['log surface','flow stage','source hint','validation','report reading'];"""
    "\n"
    """    const renderScope = (id, scope) => renderTable(id, headers, sourceAppendixByScope(scope).map(item => ["""
    "\n"
    """      item.log_surface,"""
    "\n"
    """      item.flow_stage,"""
    "\n"
    """      item.source_hint,"""
    "\n"
    """      item.validation,"""
    "\n"
    """      item.report_reading"""
    "\n"
    """    ]));"""
    "\n"
    """    renderScope('source-appendix-common-table', '通用');"""
    "\n"
    """    renderScope('source-appendix-read-table', '读取');"""
    "\n"
    """    renderScope('source-appendix-write-table', '写入');"""
    "\n"
    """  }"""
    "\n"
    """  renderSourceAppendixTables();"""
    "\n"
    """  function renderFlowStageTable(id, graph) {"""
    "\n"
    """    renderTable(id, ['stage','summary','operation','IPs','异常 IP','reason','status'], (graph.edges || """
    """[]).map(edge => ["""
    "\n"
    """      edge.name,"""
    "\n"
    """      edge.summary || '',"""
    "\n"
    """      edge.operation,"""
    "\n"
    """      (edge.rollup?.top_ips || []).join(', '),"""
    "\n"
    """      flowEdgeAbnormalIps(edge),"""
    "\n"
    """      edge.reason || edge.report_reading,"""
    "\n"
    """      edge.status"""
    "\n"
    """    ]));"""
    "\n"
    """  }"""
    "\n"
    """  function renderFlowCandidateEdgeTable() {"""
    "\n"
    """    const rows = (flowStages.candidate_edges || []).map(edge => ["""
    "\n"
    """      edge.operation,"""
    "\n"
    """      `${edge.source} -> ${edge.target}`,"""
    "\n"
    """      (edge.transports || []).join('/'),"""
    "\n"
    """      edge.status,"""
    "\n"
    """      (edge.observed_transfer_paths || []).join('/') || '未观测',"""
    "\n"
    """      edge.report_reading || ''"""
    "\n"
    """    ]);"""
    "\n"
    """    renderTable('flow-candidate-edge-table', ['candidate','direction','transport','status','observed """
    """path','reading'], rows);"""
    "\n"
    """  }"""
    "\n"
    """  renderFlowStageTable('flow-stage-table', flowStages);"""
    "\n"
    """  renderFlowStageTable('read-flow-stage-table', readFlowStages);"""
    "\n"
    """  renderFlowStageTable('write-flow-stage-table', writeFlowStages);"""
    "\n"
    """  renderFlowCandidateEdgeTable();"""
    "\n"
    """  renderPagedTable('worker-ip-alias-table', 'worker-ip-alias-table-pager', ['worker full name','worker """
    """short name','POD IP'], buildWorkerIpAliasRows(), null, 5);"""
    "\n"
    """  renderTable('error-table', ['error','count'], errorRows);"""
    "\n"
    """  const latencyRowsForTable = ["""
    "\n"
    """    ['access', pctText(dim.latency_ms?.access)],"""
    "\n"
    """    ...Object.entries(dim.urma_elapsed || {}).map(([k,v]) => [`urma.${k}`, pctText(v)]),"""
    "\n"
    """    ...Object.entries(dim.latency_summary_us || {}).map(([k,v]) => [`latencySummary.${k}`, pctText(v)])"""
    "\n"
    """  ].filter(row => row[1]);"""
    "\n"
    """  const latencyChartRows = ["""
    "\n"
    """    ['access', scalePercentiles(dim.latency_ms?.access, 1)],"""
    "\n"
    """    ...Object.entries(dim.urma_elapsed || {}).map(([k,v]) => [`urma.${k}`, scalePercentiles(v, 1)]),"""
    "\n"
    """    ...Object.entries(dim.latency_summary_us || {}).map(([k,v]) => [`latencySummary.${k}`, """
    """scalePercentiles(v, 0.001)])"""
    "\n"
    """  ].filter(([, item]) => item && item.count).sort((a,b) => (b[1].max || 0) - (a[1].max || 0)).slice(0, """
    """12);"""
    "\n"
    """  const flowRows = Object.entries(dim.flow || {}).sort((a,b) => b[1]-a[1]);"""
    "\n"
    """  const workerRows = Object.entries(dim.worker_summary || {})"""
    "\n"
    """    .sort((a,b) => (b[1].error_count || 0) - (a[1].error_count || 0) || (b[1].line_count || 0) - """
    """(a[1].line_count || 0))"""
    "\n"
    """    .slice(0, 40);"""
    "\n"
    """  const ubRows = Object.entries(dim.ub_summary?.edges || {})"""
    "\n"
    """    .sort((a,b) => (b[1].count || 0) - (a[1].count || 0))"""
    "\n"
    """    .slice(0, 40);"""
    "\n"
    """  const ubWorkerRows = Object.entries(dim.ub_worker_summary?.workers || {})"""
    "\n"
    """    .sort((a,b) => (b[1].latency_ms?.max || 0) - (a[1].latency_ms?.max || 0) || ((b[1].entry_events || 0) """
    """+ (b[1].exit_events || 0)) - ((a[1].entry_events || 0) + (a[1].exit_events || 0)))"""
    "\n"
    """    .slice(0, 40);"""
    "\n"
    """  const ubWorkerTimeRows = dim.ub_worker_summary?.time_buckets || [];"""
    "\n"
    """  const ubLifecycleMetrics = Object.entries(dim.ub_lifecycle_summary?.metrics || {})"""
    "\n"
    """    .sort((a,b) => (b[1].max || 0) - (a[1].max || 0));"""
    "\n"
    """  const ubCountMetricNames = new Set(['remote_get_wr_count','urma_inflight_wr_count']);"""
    "\n"
    """  const ubLifecycleLatencyMetrics = ubLifecycleMetrics.filter(([name]) => !ubCountMetricNames.has(name));"""
    "\n"
    """  const ubChipInflightRows = Object.entries(dim.ub_lifecycle_summary?.chip_inflight || {})"""
    "\n"
    """    .sort((a,b) => (b[1].max || 0) - (a[1].max || 0));"""
    "\n"
    """  const ubWrCountRows = ["""
    "\n"
    """    ...ubLifecycleMetrics.filter(([name]) => ubCountMetricNames.has(name)).map(([name,item]) => """
    """[lifecycleLabel(name), item]),"""
    "\n"
    """    ...ubChipInflightRows.map(([chip,item]) => [`Chip ${chip} inflight`, item])"""
    "\n"
    """  ].sort((a,b) => (b[1].max || 0) - (a[1].max || 0));"""
    "\n"
    """  const ubRequestRows = dim.ub_lifecycle_summary?.requests || [];"""
    "\n"
    """  const timeBucketRows = dim.time_buckets?.['1000ms'] || [];"""
    "\n"
    """  function latencyRowsForOperation(operation) {"""
    "\n"
    """    const rows = latencyChartRows.filter(([name]) => metricOperation(name) === operation || """
    """metricOperation(name) === 'both');"""
    "\n"
    """    const accessRow = ['access', operationAccessPercentiles(operation)];"""
    "\n"
    """    return [accessRow, ...rows.filter(([name]) => name !== 'access')]"""
    "\n"
    """      .filter(([, item]) => item && item.count)"""
    "\n"
    """      .sort((a,b) => (b[1].max || 0) - (a[1].max || 0))"""
    "\n"
    """      .slice(0, 12);"""
    "\n"
    """  }"""
    "\n"
    """  function flowRowsForOperation(operation) {"""
    "\n"
    """    return flowRows.filter(([name]) => flowOperation(name) === operation);"""
    "\n"
    """  }"""
    "\n"
    """  function buildTraceTimeBuckets(operation) {"""
    "\n"
    """    const buckets = {};"""
    "\n"
    """    traceRowsForOperation(operation).forEach(([, item]) => {"""
    "\n"
    """      const ts = traceStartTime(item);"""
    "\n"
    """      if (!ts) return;"""
    "\n"
    """      const bucket = String(ts).slice(0, 19);"""
    "\n"
    """      const target = buckets[bucket] || (buckets[bucket] = """
    """{bucket_start:bucket,bucket_ms:1000,access:[],stage_values:{},max_concurrency_error_count:0});"""
    "\n"
    """      const accessValue = item.access_latency_ms?.max ?? item.access_latency_ms?.p99 ?? """
    """item.access_latency_ms?.p50;"""
    "\n"
    """      if (Number.isFinite(Number(accessValue))) target.access.push(Number(accessValue));"""
    "\n"
    """      target.max_concurrency_error_count += Number((item.errors || {})['RPC max concurrency reached'] || """
    """0);"""
    "\n"
    """      (item.stage_breakdown || []).forEach(stage => {"""
    "\n"
    """        if (!String(stage.stage || '').startsWith(`${operation}.`) || stage.duration_ms === undefined) """
    """return;"""
    "\n"
    """        (target.stage_values[stage.stage] || (target.stage_values[stage.stage] = """
    """[])).push(Number(stage.duration_ms));"""
    "\n"
    """      });"""
    "\n"
    """    });"""
    "\n"
    """    return Object.values(buckets).sort((a,b) => """
    """String(a.bucket_start).localeCompare(String(b.bucket_start))).map(bucket => {"""
    "\n"
    """      const stageBreakdown = {};"""
    "\n"
    """      Object.entries(bucket.stage_values).forEach(([name, values]) => { stageBreakdown[name] = """
    """percentileFromValues(values) || {}; });"""
    "\n"
    """      return {bucket_start:bucket.bucket_start,bucket_ms:bucket.bucket_ms,p99_access_ms:(percentileFromVal"""
    """ues(bucket.access) || {}).p99 || """
    """0,max_concurrency_error_count:bucket.max_concurrency_error_count,stage_breakdown_ms:stageBreakdown};"""
    "\n"
    """    });"""
    "\n"
    """  }"""
    "\n"
    """  function workerFilterValue(id) {"""
    "\n"
    """    const node = document.getElementById(id);"""
    "\n"
    """    return node ? node.value : '';"""
    "\n"
    """  }"""
    "\n"
    """  function workerRoleBreakdownForTrace(item, worker) {"""
    "\n"
    """    const out = {entry_get:0,data_provider:0,other:0};"""
    "\n"
    """    (item.ub_events || []).forEach(event => {"""
    "\n"
    """      if (!workerMatchesFilter(event.worker, workerAggregateKey(worker))) return;"""
    "\n"
    """      if (['transfer_path','remote_get_start'].includes(event.event_type)) out.entry_get += 1;"""
    "\n"
    """      else if (['total','poll_jfc','notify','thread_sched'].includes(event.event_type)) out.data_provider """
    """+= 1;"""
    "\n"
    """    });"""
    "\n"
    """    if (!out.entry_get && !out.data_provider) out.other += 1;"""
    "\n"
    """    return out;"""
    "\n"
    """  }"""
    "\n"
    """  function isSameNodeEntryData(item) {"""
    "\n"
    """    return Boolean(item && item.entry_get_count && item.data_provider_count);"""
    "\n"
    """  }"""
    "\n"
    """  function workerRoleSummaryText(item) {"""
    "\n"
    """    const parts = [];"""
    "\n"
    """    if (item.same_node_entry_data || isSameNodeEntryData(item)) parts.push('同节点 entry+data');"""
    "\n"
    """    if (item.entry_get_count) parts.push(`数据读取发起端(entry get)=${item.entry_get_count}`);"""
    "\n"
    """    if (item.data_provider_count) parts.push(`数据提供端(data provider)=${item.data_provider_count}`);"""
    "\n"
    """    if (!parts.length && item.other_role_count) parts.push(`其他/未归因=${item.other_role_count}`);"""
    "\n"
    """    return parts.join(', ') || (item.roles || []).join(',');"""
    "\n"
    """  }"""
    "\n"
    """  function buildWorkerIpAliasRows() {"""
    "\n"
    """    return (dim.worker_ip_mapping || []).map(item => ["""
    "\n"
    """        item.worker_full_name,"""
    "\n"
    """        item.worker_short_name,"""
    "\n"
    """        item.pod_ip"""
    "\n"
    """      ]);"""
    "\n"
    """  }"""
    "\n"
    """  function workerRowsForOperation(operation, selectedWorker='') {"""
    "\n"
    """    const workers = {};"""
    "\n"
    """    traceRowsForOperation(operation).forEach(([, item]) => {"""
    "\n"
    """      const errorCount = Object.values(item.errors || {}).reduce((a,b) => a + b, 0);"""
    "\n"
    """      const isSlow = Number(item.access_latency_ms?.max || 0) >= 20;"""
    "\n"
    """      const seenWorkerKeys = new Set();"""
    "\n"
    """      Object.keys(item.workers || {}).forEach(worker => {"""
    "\n"
    """        const key = workerAggregateKey(worker);"""
    "\n"
    """        if (!workerMatchesFilter(worker, selectedWorker) || seenWorkerKeys.has(key)) return;"""
    "\n"
    """        seenWorkerKeys.add(key);"""
    "\n"
    """        const source = dim.worker_summary?.[worker] || {};"""
    "\n"
    """        const target = workers[key] || (workers[key] = """
    """{display_worker:workerAggregateLabel(key),roles:[],line_count:0,trace_count:0,slow_trace_count:0,error_cou"""
    """nt:0,entry_get_count:0,data_provider_count:0,other_role_count:0,relations:new Set()});"""
    "\n"
    """        const roleBreakdown = workerRoleBreakdownForTrace(item, worker);"""
    "\n"
    """        target.roles = [...new Set([...(target.roles || []), ...(source.roles || [])])];"""
    "\n"
    """        target.line_count += source.line_count || 0;"""
    "\n"
    """        target.relations.add(workerRelationName(worker));"""
    "\n"
    """        target.entry_get_count += roleBreakdown.entry_get;"""
    "\n"
    """        target.data_provider_count += roleBreakdown.data_provider;"""
    "\n"
    """        target.other_role_count += roleBreakdown.other;"""
    "\n"
    """        target.same_node_entry_data = isSameNodeEntryData(target);"""
    "\n"
    """        target.trace_count += 1;"""
    "\n"
    """        target.slow_trace_count += isSlow ? 1 : 0;"""
    "\n"
    """        target.error_count += errorCount;"""
    "\n"
    """      });"""
    "\n"
    """    });"""
    "\n"
    """    return Object.entries(workers).sort((a,b) => (b[1].error_count || 0) - (a[1].error_count || 0) || """
    """(b[1].slow_trace_count || 0) - (a[1].slow_trace_count || 0) || (b[1].trace_count || 0) - """
    """(a[1].trace_count || 0)).slice(0, 40);"""
    "\n"
    """  }"""
    "\n"
    """  function renderLatencySection(operation, title) {"""
    "\n"
    """    const rows = latencyRowsForOperation(operation);"""
    "\n"
    """    renderTable(`${operation}-latency-table`, ['metric','distribution'], rows.map(([name,item]) => """
    """[metricLabel(name), pctText(item)]));"""
    "\n"
    """    chart(`${operation}-latency-chart`, rows.length ? axisBase(`${title} Top Latency`, {"""
    "\n"
    """      grid:wideHorizontalGrid(150,66,36),"""
    "\n"
    """      tooltip:{trigger:'axis',axisPointer:{type:'shadow'},confine:true,formatter:ps => ps.map(p => """
    """`${escapeHtml(p.seriesName)}: ${p.value} ms`).join('<br>')},"""
    "\n"
    """      xAxis:{type:'value', name:'ms'},"""
    "\n"
    """      yAxis:{type:'category', data:rows.map(r => metricLabel(r[0])), axisLabel:{width:138, """
    """overflow:'truncate'}},"""
    "\n"
    """      series:['p50','p90','p99','max'].map(key => ({name:key,type:'bar',barMaxWidth:18,data:rows.map(([, """
    """item]) => item[key] || 0), markLine:key === 'max' ? {symbol:'none', """
    """lineStyle:{color:'#dc2626',type:'dashed'}, label:{formatter:'20ms deadline'}, data:[{xAxis:20}]} : """
    """undefined}))"""
    "\n"
    """    }) : noDataOption(`No ${operation} latency data`));"""
    "\n"
    """  }"""
    "\n"
    """  function renderFlowSection(operation, title) {"""
    "\n"
    """    const rows = flowRowsForOperation(operation);"""
    "\n"
    """    renderTable(`${operation}-flow-table`, ['flow','count'], rows);"""
    "\n"
    """    chart(`${operation}-flow-chart`, rows.length ? axisBase(`${title} Flow Breakdown`, {"""
    "\n"
    """      grid:wideHorizontalGrid(122,56,34),"""
    "\n"
    """      tooltip:{trigger:'axis',axisPointer:{type:'shadow'},confine:true},"""
    "\n"
    """      xAxis:{type:'value', name:'count'},"""
    "\n"
    """      yAxis:{type:'category', data:rows.map(r => flowLabel(r[0])), axisLabel:{width:112, """
    """overflow:'truncate'}},"""
    "\n"
    """      series:[{name:'count',type:'bar',barMaxWidth:28,data:rows.map(r => """
    """r[1]),itemStyle:{color:'#2563eb'},label:{show:true,position:'right'}}]"""
    "\n"
    """    }) : noDataOption(`No ${operation} flow data`));"""
    "\n"
    """  }"""
    "\n"
    """  function renderTimeBucketSection(operation, title) {"""
    "\n"
    """    const rows = buildTraceTimeBuckets(operation);"""
    "\n"
    """    const stageNames = [...new Set(rows.flatMap(row => Object.keys(row.stage_breakdown_ms || """
    """{})))].filter(name => !name.endsWith('client_to_entry_worker'));"""
    "\n"
    """    chart(`${operation}-time-breakdown-chart`, rows.length ? axisBase(`${title} Time Bucket Latency """
    """Stages`, {"""
    "\n"
    """      tooltip:{trigger:'axis', axisPointer:{type:'cross'}, confine:true},"""
    "\n"
    """      xAxis:{type:'category', data:rows.map(r => String(r.bucket_start || '').replace('T','\\n')), """
    """axisLabel:{rotate:0}},"""
    "\n"
    """      yAxis:{type:'value', name:'ms'},"""
    "\n"
    """      series:["""
    "\n"
    """        ...stageNames.map(name => ({name:stageLabelMap[name] || """
    """name,type:'bar',stack:'stage-p99',barMaxWidth:34,data:rows.map(r => r.stage_breakdown_ms?.[name]?.p99 || """
    """0),itemStyle:{color:stageColorMap[name] || '#64748b'}})),"""
    "\n"
    """        {name:'RPC max_concurrency errors',type:'bar',barMaxWidth:24,data:rows.map(r => """
    """r.max_concurrency_error_count || 0),itemStyle:{color:'#991b1b'}},"""
    "\n"
    """        {name:'client/access p99 upper bound',type:'line',smooth:true,data:rows.map(r => r.p99_access_ms """
    """|| 0), itemStyle:{color:'#2563eb'}, markLine:{symbol:'none', lineStyle:{color:'#dc2626',type:'dashed'}, """
    """label:{formatter:'20ms deadline'}, data:[{yAxis:20}]}}"""
    "\n"
    """      ]"""
    "\n"
    """    }) : noDataOption(`No ${operation} time bucket stage data`));"""
    "\n"
    """  }"""
    "\n"
    """  function renderWorkerSection(operation, title) {"""
    "\n"
    """    const chartRows = workerRowsForOperation(operation, workerFilterValue(`${operation}-worker-filter`));"""
    "\n"
    """    const tableRowsForOperation = workerRowsForOperation(operation, """
    """workerFilterValue(`${operation}-worker-table-filter`));"""
    "\n"
    """    const tableRows = tableRowsForOperation.map(([,item]) => {"""
    "\n"
    """      const display_worker = item.display_worker || '';"""
    "\n"
    """      return [display_worker, workerRoleSummaryText(item), item.line_count, item.trace_count, """
    """item.slow_trace_count || 0, item.error_count || 0];"""
    "\n"
    """    });"""
    "\n"
    """    renderPagedTable(`${operation}-worker-table`, `${operation}-worker-table-pager`, """
    """['worker','roles','lines','traces','slow','errors'], tableRows,"""
    "\n"
    """      row => (String(row[1]).includes('同节点 entry+data') || Number(row[5]) > 0 ? 'class="hotrow"' : """
    """Number(row[4]) > 0 ? 'class="warnrow"' : ''), 5);"""
    "\n"
    """    chart(`${operation}-worker-chart`, chartRows.length ? axisBase(`${title} Workers by Trace/Error`, """
    """{xAxis:{type:'category', data:chartRows.slice(0,20).map(r => r[1].display_worker || """
    """workerAggregateLabel(r[0])), axisLabel:{rotate:35, width:120, overflow:'truncate'}}, """
    """yAxis:{type:'value'}, series:["""
    "\n"
    """      {name:'traces',type:'bar',barMaxWidth:34,data:chartRows.slice(0,20).map(r => r[1].trace_count || """
    """0), itemStyle:{color:'#94a3b8'}},"""
    "\n"
    """      {name:'slow',type:'bar',barMaxWidth:34,data:chartRows.slice(0,20).map(r => r[1].slow_trace_count || """
    """0), itemStyle:{color:'#ea580c'}},"""
    "\n"
    """      {name:'errors',type:'bar',barMaxWidth:34,data:chartRows.slice(0,20).map(r => r[1].error_count || """
    """0), itemStyle:{color:'#dc2626'}, label:{show:true, position:'top'}}"""
    "\n"
    """    ]}) : noDataOption(`No ${operation} worker data`));"""
    "\n"
    """  }"""
    "\n"
    """  function splitUbEdge(edge) {"""
    "\n"
    """    const parts = String(edge || '').split(/\\s*->\\s*/);"""
    "\n"
    """    return {src:parts[0] || '', dst:parts[1] || ''};"""
    "\n"
    """  }"""
    "\n"
    """  function ubEdgeDegreeSummary(rows) {"""
    "\n"
    """    const srcStats = {};"""
    "\n"
    """    const dstStats = {};"""
    "\n"
    """    rows.forEach(([edge, item]) => {"""
    "\n"
    """      const parts = splitUbEdge(edge);"""
    "\n"
    """      const count = Number(item?.count || 0);"""
    "\n"
    """      const tail = Math.max(Number(item?.latency_ms?.p99 || 0), Number(item?.latency_ms?.max || 0));"""
    "\n"
    """      const update = (map, ip) => {"""
    "\n"
    """        if (!ip) return;"""
    "\n"
    """        const target = map[ip] || (map[ip] = {ip, count:0, tail_ms:0});"""
    "\n"
    """        target.count += count;"""
    "\n"
    """        target.tail_ms = Math.max(target.tail_ms, tail);"""
    "\n"
    """      };"""
    "\n"
    """      update(srcStats, parts.src);"""
    "\n"
    """      update(dstStats, parts.dst);"""
    "\n"
    """    });"""
    "\n"
    """    const top = map => Object.values(map).sort((a,b) => b.count - a.count || b.tail_ms - a.tail_ms)[0];"""
    "\n"
    """    const topSrc = top(srcStats);"""
    "\n"
    """    const topDst = top(dstStats);"""
    "\n"
    """    const worst = [topSrc, topDst].filter(Boolean).sort((a,b) => b.tail_ms - a.tail_ms || b.count - """
    """a.count)[0];"""
    "\n"
    """    if (!worst) return '无 UB Edge 样本';"""
    "\n"
    """    return `源端出度 Top ${topSrc?.ip || '无'} count=${fmtCount(topSrc?.count)}，目的端入度 Top ${topDst?.ip || '无'} """
    """count=${fmtCount(topDst?.count)}；最大问题 IP ${worst.ip} tail=${fmtMs(worst.tail_ms)}。`;"""
    "\n"
    """  }"""
    "\n"
    """  function renderUbEdgeFilters(operation, rows) {"""
    "\n"
    """    ['src','dst'].forEach(kind => {"""
    "\n"
    """      const select = document.getElementById(`${operation}-ub-${kind}-filter`);"""
    "\n"
    """      if (!select) return;"""
    "\n"
    """      const current = select.value;"""
    "\n"
    """      const values = [...new Set(rows.map(([edge]) => splitUbEdge(edge)[kind]).filter(Boolean))].sort();"""
    "\n"
    """      const label = kind === 'src' ? '全部源端' : '全部目的端';"""
    "\n"
    """      setSelectOptions(`${operation}-ub-${kind}-filter`, label, values);"""
    "\n"
    """      select.value = values.includes(current) ? current : '';"""
    "\n"
    """    });"""
    "\n"
    """  }"""
    "\n"
    """  function ubEdgeFilterValue(operation, kind) {"""
    "\n"
    """    const node = document.getElementById(`${operation}-ub-${kind}-filter`);"""
    "\n"
    """    return node ? node.value : '';"""
    "\n"
    """  }"""
    "\n"
    """  function renderUbSection(operation) {"""
    "\n"
    """    const allRows = operation === 'read' ? ubRows : [];"""
    "\n"
    """    renderUbEdgeFilters(operation, allRows);"""
    "\n"
    """    const srcFilter = ubEdgeFilterValue(operation, 'src');"""
    "\n"
    """    const dstFilter = ubEdgeFilterValue(operation, 'dst');"""
    "\n"
    """    const rows = allRows.filter(([edge]) => {"""
    "\n"
    """      const parts = splitUbEdge(edge);"""
    "\n"
    """      return (!srcFilter || parts.src === srcFilter) && (!dstFilter || parts.dst === dstFilter);"""
    "\n"
    """    });"""
    "\n"
    """    const summaryNode = document.getElementById(`${operation}-ub-edge-summary`);"""
    "\n"
    """    if (summaryNode) summaryNode.textContent = ubEdgeDegreeSummary(rows);"""
    "\n"
    """    const tableRows = rows.map(([edge,item]) => {"""
    "\n"
    """      const parts = splitUbEdge(edge);"""
    "\n"
    """      return [parts.src, parts.dst, item.count, item.latency_ms?.p99 || '', item.latency_ms?.max || '', """
    """pctText(item.latency_ms)];"""
    "\n"
    """    });"""
    "\n"
    """    renderPagedTable(`${operation}-ub-edge-table`, `${operation}-ub-edge-table-pager`, """
    """['源端','目的端','count','p99 ms','max ms','latency'], tableRows,"""
    "\n"
    """      row => `class="${severityClass(Math.max(Number(row[3]) || 0, Number(row[4]) || 0))}"`, 5);"""
    "\n"
    """    chart(`${operation}-ub-edge-chart`, rows.length ? axisBase(`${operation === 'read' ? 'Read' : """
    """'Write'} UB Edge Count / Tail Latency`, {xAxis:{type:'category', data:rows.slice(0,20).map(r => r[0]), """
    """axisLabel:{rotate:35, width:150, overflow:'truncate'}}, yAxis:[{type:'value', name:'count'}, """
    """{type:'value', name:'ms'}], series:["""
    "\n"
    """      {name:'UB edges',type:'bar',barMaxWidth:34,data:rows.slice(0,20).map(r => r[1].count || 0), """
    """itemStyle:{color:'#059669'}, label:{show:true, position:'top'}},"""
    "\n"
    """      {name:'p99 ms',type:'line',yAxisIndex:1,smooth:true,data:rows.slice(0,20).map(r => """
    """r[1].latency_ms?.p99 || 0), itemStyle:{color:'#dc2626'}, markLine:{symbol:'none', """
    """lineStyle:{color:'#dc2626',type:'dashed'}, label:{formatter:'20ms'}, data:[{yAxis:20}]}}"""
    "\n"
    """    ]}) : noDataOption(operation === 'write' ? 'Write flow has no UB read edge data' : 'No read UB edge """
    """data'));"""
    "\n"
    """  }"""
    "\n"
    """  function renderUbWorkerViews() {"""
    "\n"
    """    const roleChartWorker = workerFilterValue('ub-worker-role-filter');"""
    "\n"
    """    const timeChartWorker = workerFilterValue('ub-worker-time-filter');"""
    "\n"
    """    const roleTableWorker = workerFilterValue('ub-worker-role-table-filter');"""
    "\n"
    """    const timeTableWorker = workerFilterValue('ub-worker-time-table-filter');"""
    "\n"
    """    function ubWorkerRoleLabel(role) {"""
    "\n"
    """      if (role === 'entry_and_exit') return '同节点 entry+data / 发起端+提供端';"""
    "\n"
    """      if (role === 'entry') return '数据读取发起端(entry get)';"""
    "\n"
    """      if (role === 'exit') return '数据提供端(data provider)';"""
    "\n"
    """      return role || '';"""
    "\n"
    """    }"""
    "\n"
    """    function aggregateUbWorkerRowsByIdentity(rows, selectedWorker='') {"""
    "\n"
    """      const out = {};"""
    "\n"
    """      rows.forEach(([worker, item]) => {"""
    "\n"
    """        const key = workerAggregateKey(worker);"""
    "\n"
    """        if (!workerMatchesFilter(worker, selectedWorker)) return;"""
    "\n"
    """        const target = out[key];"""
    "\n"
    """        const aggregate = target || (out[key] = {"""
    "\n"
    """          display_worker: workerAggregateLabel(key),"""
    "\n"
    """          entry_events: 0,"""
    "\n"
    """          exit_events: 0,"""
    "\n"
    """          trace_count: 0,"""
    "\n"
    """          latency_ms: {p99: 0, max: 0},"""
    "\n"
    """          top_edges: [],"""
    "\n"
    """          roles: new Set()"""
    "\n"
    """        });"""
    "\n"
    """        aggregate.entry_events += item.entry_events || 0;"""
    "\n"
    """        aggregate.exit_events += item.exit_events || 0;"""
    "\n"
    """        aggregate.trace_count += item.trace_count || 0;"""
    "\n"
    """        aggregate.latency_ms.p99 = Math.max(aggregate.latency_ms.p99 || 0, item.latency_ms?.p99 || 0);"""
    "\n"
    """        aggregate.latency_ms.max = Math.max(aggregate.latency_ms.max || 0, item.latency_ms?.max || 0);"""
    "\n"
    """        (item.top_edges || []).forEach(edge => aggregate.top_edges.push(edge));"""
    "\n"
    """        if (item.entry_events) aggregate.roles.add('ub_entry');"""
    "\n"
    """        if (item.exit_events) aggregate.roles.add('ub_exit');"""
    "\n"
    """      });"""
    "\n"
    """      return Object.entries(out).map(([key, item]) => {"""
    "\n"
    """        const display_ub_role = item.entry_events && item.exit_events ? 'entry_and_exit' : """
    """item.entry_events ? 'entry' : 'exit';"""
    "\n"
    """        item.role = display_ub_role;"""
    "\n"
    """        item.top_edges = [...new Set(item.top_edges)].slice(0, 5);"""
    "\n"
    """        return [key, item];"""
    "\n"
    """      }).sort((a,b) =>"""
    "\n"
    """        Number(b[1].latency_ms?.max || 0) - Number(a[1].latency_ms?.max || 0) ||"""
    "\n"
    """        Number((b[1].entry_events || 0) + (b[1].exit_events || 0)) - Number((a[1].entry_events || 0) + """
    """(a[1].exit_events || 0))"""
    "\n"
    """      );"""
    "\n"
    """    }"""
    "\n"
    """    const filterUbWorkerTimeRows = selectedWorker => selectedWorker ? ubWorkerTimeRows.filter(item =>"""
    "\n"
    """      (item.top_entry_workers || []).some(worker => workerMatchesFilter(worker, selectedWorker)) ||"""
    "\n"
    """      (item.top_exit_workers || []).some(worker => workerMatchesFilter(worker, selectedWorker))"""
    "\n"
    """    ) : ubWorkerTimeRows;"""
    "\n"
    """    const chartUbWorkerRows = aggregateUbWorkerRowsByIdentity(ubWorkerRows, roleChartWorker);"""
    "\n"
    """    const tableUbWorkerRows = aggregateUbWorkerRowsByIdentity(ubWorkerRows, roleTableWorker);"""
    "\n"
    """    const chartUbWorkerTimeRows = filterUbWorkerTimeRows(timeChartWorker);"""
    "\n"
    """    const tableUbWorkerTimeRows = filterUbWorkerTimeRows(timeTableWorker);"""
    "\n"
    """    renderPagedTable('ub-worker-role-table', 'ub-worker-role-table-pager', ['worker','role','发起端 """
    """events','提供端 events','trace count','p99 ms','max ms','top edges'], tableUbWorkerRows.map(([worker,item]) """
    """=> {"""
    "\n"
    """      const display_worker = item.display_worker || workerAggregateLabel(worker);"""
    "\n"
    """      const display_top_edges = (item.top_edges || []).map(edge => edge.replace(/\\s*->\\s*/g, ' → """
    """')).join(', ');"""
    "\n"
    """      return ["""
    "\n"
    """        display_worker,"""
    "\n"
    """        ubWorkerRoleLabel(item.role),"""
    "\n"
    """        item.entry_events || 0,"""
    "\n"
    """        item.exit_events || 0,"""
    "\n"
    """        item.trace_count || 0,"""
    "\n"
    """        item.latency_ms?.p99 || '',"""
    "\n"
    """        item.latency_ms?.max || '',"""
    "\n"
    """        display_top_edges"""
    "\n"
    """      ];"""
    "\n"
    """    }), row => `class="${severityClass(Math.max(Number(row[5]) || 0, Number(row[6]) || 0))}"`, 5);"""
    "\n"
    """    renderPagedTable('ub-worker-time-table', 'ub-worker-time-table-pager', ['time','发起端 events','提供端 """
    """events','p99 ms','max ms','发起端 workers','提供端 workers'], tableUbWorkerTimeRows.map(item => ["""
    "\n"
    """      item.bucket_start || '',"""
    "\n"
    """      item.entry_events || 0,"""
    "\n"
    """      item.exit_events || 0,"""
    "\n"
    """      item.latency_ms?.p99 || '',"""
    "\n"
    """      item.latency_ms?.max || '',"""
    "\n"
    """      [...new Set((item.top_entry_workers || """
    """[]).map(workerAggregateKey))].map(workerAggregateLabel).join(', '),"""
    "\n"
    """      [...new Set((item.top_exit_workers || """
    """[]).map(workerAggregateKey))].map(workerAggregateLabel).join(', ')"""
    "\n"
    """    ]), row => `class="${severityClass(Math.max(Number(row[3]) || 0, Number(row[4]) || 0))}"`, 5);"""
    "\n"
    """    chart('ub-worker-role-chart', chartUbWorkerRows.length ? axisBase('UB Worker Roles', {"""
    "\n"
    """      tooltip:{trigger:'axis', axisPointer:{type:'shadow'}, confine:true},"""
    "\n"
    """      xAxis:{type:'category', data:chartUbWorkerRows.slice(0,20).map(r => r[1].display_worker || """
    """workerAggregateLabel(r[0])), axisLabel:{rotate:35, width:120, overflow:'truncate'}},"""
    "\n"
    """      yAxis:[{type:'value', name:'events'}, {type:'value', name:'ms'}],"""
    "\n"
    """      series:["""
    "\n"
    """        {name:'数据读取发起端 """
    """events',type:'bar',stack:'ub-role',barMaxWidth:34,data:chartUbWorkerRows.slice(0,20).map(r => """
    """r[1].entry_events || 0),itemStyle:{color:'#2563eb'}},"""
    "\n"
    """        {name:'数据提供端 events',type:'bar',stack:'ub-role',barMaxWidth:34,data:chartUbWorkerRows.slice(0,20)."""
    """map(r => r[1].exit_events || 0),itemStyle:{color:'#ea580c'}},"""
    "\n"
    """        {name:'p99 ms',type:'line',yAxisIndex:1,data:chartUbWorkerRows.slice(0,20).map(r => """
    """r[1].latency_ms?.p99 || 0),itemStyle:{color:'#dc2626'}}"""
    "\n"
    """      ]"""
    "\n"
    """    }) : noDataOption('No UB worker role data'));"""
    "\n"
    """    chart('ub-worker-time-chart', chartUbWorkerTimeRows.length ? axisBase('UB Worker Role Time Buckets', {"""
    "\n"
    """      tooltip:{trigger:'axis', axisPointer:{type:'cross'}, confine:true},"""
    "\n"
    """      xAxis:{type:'category', data:chartUbWorkerTimeRows.map(r => String(r.bucket_start || """
    """'').replace('T','\\n')), axisLabel:{rotate:0}},"""
    "\n"
    """      yAxis:[{type:'value', name:'events'}, {type:'value', name:'ms'}],"""
    "\n"
    """      series:["""
    "\n"
    """        {name:'数据读取发起端 events',type:'bar',stack:'ub-time',barMaxWidth:34,data:chartUbWorkerTimeRows.map(r """
    """=> r.entry_events || 0),itemStyle:{color:'#2563eb'}},"""
    "\n"
    """        {name:'数据提供端 events',type:'bar',stack:'ub-time',barMaxWidth:34,data:chartUbWorkerTimeRows.map(r """
    """=> r.exit_events || 0),itemStyle:{color:'#ea580c'}},"""
    "\n"
    """        {name:'p99 ms',type:'line',yAxisIndex:1,smooth:true,data:chartUbWorkerTimeRows.map(r => """
    """r.latency_ms?.p99 || 0),itemStyle:{color:'#dc2626'}, markLine:{symbol:'none', """
    """lineStyle:{color:'#dc2626',type:'dashed'}, label:{formatter:'20ms'}, data:[{yAxis:20}]}}"""
    "\n"
    """      ]"""
    "\n"
    """    }) : noDataOption('No UB time bucket data'));"""
    "\n"
    """  }"""
    "\n"
    """  function lifecycleLabel(name) {"""
    "\n"
    """    const labels = {"""
    "\n"
    """      'total_ms':'URMA total',"""
    "\n"
    """      'wait_os_sched_ms':'wait_for',"""
    "\n"
    """      'wake_sched_latency_ms':'wake sched',"""
    "\n"
    """      'poll_jfc_ms':'poll_jfc',"""
    "\n"
    """      'notify_ms':'notify',"""
    "\n"
    """      'thread_sched_ms':'thread sched',"""
    "\n"
    """      'poll_loop_gap_ms':'poll loop gap',"""
    "\n"
    """      'nanosleep_wake_ms':'nanosleep wake',"""
    "\n"
    """      'remote_get_wr_count':'RemoteGet WR count',"""
    "\n"
    """      'urma_inflight_wr_count':'URMA inflight WR count'"""
    "\n"
    """    };"""
    "\n"
    """    return labels[name] || metricLabel(name);"""
    "\n"
    """  }"""
    "\n"
    """  function renderUbLifecycleViews() {"""
    "\n"
    """    function missingMetricCell(value) {"""
    "\n"
    """      return value === undefined || value === null || value === '' ? '未采样' : value;"""
    "\n"
    """    }"""
    "\n"
    """    const metricRows = ["""
    "\n"
    """      ...ubLifecycleMetrics.map(([name,item]) => [lifecycleLabel(name), item.count || 0, item.p50 ?? '', """
    """item.p90 ?? '', item.p99 ?? '', item.max ?? '']),"""
    "\n"
    """      ...ubChipInflightRows.map(([chip,item]) => [`Chip ${chip} inflight`, item.count || 0, item.p50 ?? """
    """'', item.p90 ?? '', item.p99 ?? '', item.max ?? ''])"""
    "\n"
    """    ];"""
    "\n"
    """    renderTable('ub-lifecycle-table', ['metric','count','p50','p90','p99','max'], metricRows,"""
    "\n"
    """      row => `class="${severityClass(Math.max(Number(row[4]) || 0, Number(row[5]) || 0))}"`);"""
    "\n"
    """    renderPagedTable('ub-request-table', 'ub-request-table-pager',"""
    "\n"
    """      ['time','trace','request','worker','total ms','wait ms','wake ms','remote wr','urma wr','poll """
    """ms','notify ms','sched ms','edge','cpuid','data size','status','chip inflight'],"""
    "\n"
    """      ubRequestRows.map(item => ["""
    "\n"
    """        item.first_ts || '',"""
    "\n"
    """        item.trace_id || '',"""
    "\n"
    """        item.request_id || '',"""
    "\n"
    """        item.worker || '',"""
    "\n"
    """        item.total_ms ?? '',"""
    "\n"
    """        item.wait_os_sched_ms ?? '',"""
    "\n"
    """        item.wake_sched_latency_ms ?? '',"""
    "\n"
    """        item.remote_get_wr_count ?? '',"""
    "\n"
    """        item.urma_inflight_wr_count ?? '',"""
    "\n"
    """        missingMetricCell(item.poll_jfc_ms),"""
    "\n"
    """        missingMetricCell(item.notify_ms),"""
    "\n"
    """        missingMetricCell(Math.max(Number(item.poll_loop_gap_ms || 0), Number(item.nanosleep_wake_ms || """
    """0), Number(item.thread_sched_ms || 0)) || ''),"""
    "\n"
    """        `${item.src_addr || ''} -> ${item.target_addr || ''}`,"""
    "\n"
    """        item.cpuid ?? '',"""
    "\n"
    """        item.data_size ?? '',"""
    "\n"
    """        item.status || '',"""
    "\n"
    """        item.src_chip_inflight || ''"""
    "\n"
    """      ]),"""
    "\n"
    """      row => `class="${severityClass(Math.max(Number(row[4]) || 0, Number(row[5]) || 0, Number(row[11]) """
    """|| 0))}"`, 5);"""
    "\n"
    """    chart('ub-lifecycle-chart', ubLifecycleLatencyMetrics.length ? axisBase('UB Lifecycle Breakdown', {"""
    "\n"
    """      grid:wideHorizontalGrid(138,66,36),"""
    "\n"
    """      tooltip:{trigger:'axis',axisPointer:{type:'shadow'},confine:true,formatter:ps => ps.map(p => """
    """`${escapeHtml(p.seriesName)}: ${p.value} ms`).join('<br>')},"""
    "\n"
    """      xAxis:{type:'value', name:'ms'},"""
    "\n"
    """      yAxis:{type:'category', data:ubLifecycleLatencyMetrics.map(r => lifecycleLabel(r[0])), """
    """axisLabel:{width:128, overflow:'truncate'}},"""
    "\n"
    """      series:['p50','p90','p99','max'].map(key => ({"""
    "\n"
    """        name:key,"""
    "\n"
    """        type:'bar',"""
    "\n"
    """        barMaxWidth:18,"""
    "\n"
    """        data:ubLifecycleLatencyMetrics.map(([, item]) => item[key] || 0),"""
    "\n"
    """        markLine:key === 'max' ? {symbol:'none', lineStyle:{color:'#dc2626',type:'dashed'}, """
    """label:{formatter:'20ms'}, data:[{xAxis:20}]} : undefined"""
    "\n"
    """      }))"""
    "\n"
    """    }) : noDataOption('No UB lifecycle data'));"""
    "\n"
    """    chart('ub-wr-count-chart', ubWrCountRows.length ? axisBase('UB WR / Inflight Count', {"""
    "\n"
    """      grid:wideHorizontalGrid(160,66,36),"""
    "\n"
    """      tooltip:{trigger:'axis',axisPointer:{type:'shadow'},confine:true,formatter:ps => ps.map(p => """
    """`${escapeHtml(p.seriesName)}: ${p.value} count`).join('<br>')},"""
    "\n"
    """      xAxis:{type:'value', name:'count'},"""
    "\n"
    """      yAxis:{type:'category', data:ubWrCountRows.map(r => r[0]), axisLabel:{width:150, """
    """overflow:'truncate'}},"""
    "\n"
    """      series:['p50','p90','p99','max'].map(key => ({"""
    "\n"
    """        name:key,"""
    "\n"
    """        type:'bar',"""
    "\n"
    """        barMaxWidth:18,"""
    "\n"
    """        data:ubWrCountRows.map(([, item]) => item[key] || 0)"""
    "\n"
    """      }))"""
    "\n"
    """    }) : noDataOption('No UB WR/inflight count data'));"""
    "\n"
    """  }"""
    "\n"
    """  function renderOperationViews() {"""
    "\n"
    """    renderTracePage();"""
    "\n"
    """    renderSelectedTrace();"""
    "\n"
    """  }"""
    "\n"
    """  function renderWorkerDependentViews() {"""
    "\n"
    """    renderWorkerSection('read', 'Read');"""
    "\n"
    """    renderWorkerSection('write', 'Write');"""
    "\n"
    """    renderUbWorkerViews();"""
    "\n"
    """    renderSectionSummaries();"""
    "\n"
    """  }"""
    "\n"
    """  function applyTraceFilters() {"""
    "\n"
    """    const query = document.getElementById('trace-search').value.trim().toLowerCase();"""
    "\n"
    """    const cls = document.getElementById('class-filter').value;"""
    "\n"
    """    const worker = document.getElementById('worker-filter').value;"""
    "\n"
    """    const requestStatus = document.getElementById('request-status-filter').value;"""
    "\n"
    """    filteredTraceRows = traceRows.filter(([traceId, item]) => {"""
    "\n"
    """      const haystack = ["""
    "\n"
    """        traceId,"""
    "\n"
    """        item.classification,"""
    "\n"
    """        JSON.stringify(item.errors || {}),"""
    "\n"
    """        Object.keys(item.workers || {}).join(' '),"""
    "\n"
    """        (item.evidence || []).map(e => e.text).join(' ')"""
    "\n"
    """      ].join(' ').toLowerCase();"""
    "\n"
    """      return (!cls || item.classification === cls)"""
    "\n"
    """        && (!worker || Object.keys(item.workers || {}).some(name => workerMatchesFilter(name, worker)))"""
    "\n"
    """        && statusMatchesTrace(item, requestStatus)"""
    "\n"
    """        && operationMatches(item)"""
    "\n"
    """        && (!query || haystack.includes(query));"""
    "\n"
    """    });"""
    "\n"
    """    if (!filteredTraceRows.some(([traceId]) => traceId === selectedTraceId)) {"""
    "\n"
    """      selectedTraceId = filteredTraceRows[0]?.[0] || null;"""
    "\n"
    """    }"""
    "\n"
    """  }"""
    "\n"
    """  function renderTracePage() {"""
    "\n"
    """    applyTraceFilters();"""
    "\n"
    """    function topTraceDisplayRows(rows) {"""
    "\n"
    """      return rows.map(([traceId,item]) => ["""
    "\n"
    """        traceStartTime(item),"""
    "\n"
    """        traceId,"""
    "\n"
    """        item.classification,"""
    "\n"
    """        traceAccessLatencyMs(item),"""
    "\n"
    """        JSON.stringify(item.errors || {}),"""
    "\n"
    """        pctText(item.access_latency_ms),"""
    "\n"
    """        Object.keys(item.workers || {}).slice(0, 6).map(workerRelationName).join(', ')"""
    "\n"
    """      ]);"""
    "\n"
    """    }"""
    "\n"
    """    const topTraceHeaders = ['time','trace','classification','latency ms','errors','access','workers'];"""
    "\n"
    """    const sortedDisplayRows = sortRowsForTable(topTraceDisplayRows(filteredTraceRows), topTraceSort, """
    """topTraceHeaders);"""
    "\n"
    """    if (!sortedDisplayRows.some(row => row[1] === selectedTraceId)) {"""
    "\n"
    """      selectedTraceId = sortedDisplayRows[0]?.[1] || null;"""
    "\n"
    """    }"""
    "\n"
    """    const totalPages = Math.max(Math.ceil(sortedDisplayRows.length / pageSize), 1);"""
    "\n"
    """    currentPage = Math.min(Math.max(currentPage, 0), totalPages - 1);"""
    "\n"
    """    const pageRows = sortedDisplayRows.slice(currentPage * pageSize, (currentPage + 1) * pageSize);"""
    "\n"
    """    renderTable('top-trace-table', topTraceHeaders, pageRows,"""
    "\n"
    """      row => `data-trace="${escapeHtml(row[1])}" class="${row[1] === selectedTraceId ? 'selected-row' : """
    """''}"`,"""
    "\n"
    """      {"""
    "\n"
    """        skipSort:true,"""
    "\n"
    """        sortState:topTraceSort,"""
    "\n"
    """        onSort:index => {"""
    "\n"
    """          topTraceSort = nextSortState(topTraceSort, index);"""
    "\n"
    """          currentPage = 0;"""
    "\n"
    """          renderTracePage();"""
    "\n"
    """          renderSelectedTrace();"""
    "\n"
    """        }"""
    "\n"
    """      });"""
    "\n"
    """    document.querySelectorAll('#top-trace-table tbody tr').forEach(row => row.addEventListener('click', """
    """() => {"""
    "\n"
    """      selectedTraceId = row.getAttribute('data-trace');"""
    "\n"
    """      renderTracePage();"""
    "\n"
    """      renderSelectedTrace();"""
    "\n"
    """    }));"""
    "\n"
    """    document.getElementById('page-status').textContent = `第 ${currentPage + 1} / ${totalPages} 页，每页 """
    """${pageSize} 条，共 ${sortedDisplayRows.length} 条 trace`;"""
    "\n"
    """    document.getElementById('prev-page').disabled = currentPage === 0;"""
    "\n"
    """    document.getElementById('next-page').disabled = currentPage >= totalPages - 1;"""
    "\n"
    """  }"""
    "\n"
    """  function renderSelectedTrace() {"""
    "\n"
    """    const item = traces[selectedTraceId] || {};"""
    "\n"
    """    function compactLatencySummary(summary) {"""
    "\n"
    """      return Object.entries(summary || {})"""
    "\n"
    """        .sort((a,b) => Number(b[1] || 0) - Number(a[1] || 0))"""
    "\n"
    """        .slice(0, 6)"""
    "\n"
    """        .map(([key, value]) => `${metricLabel(`latencySummary.${key}`)}=${Number(value / """
    """1000).toFixed(3)}ms`)"""
    "\n"
    """        .join('; ');"""
    "\n"
    """    }"""
    "\n"
    """    const selectedTraceSummaryRows = ["""
    "\n"
    """      ['trace', selectedTraceId || ''],"""
    "\n"
    """      ['classification', item.classification || ''],"""
    "\n"
    """      ['errors', JSON.stringify(item.errors || {})],"""
    "\n"
    """      ['client access', pctText(item.access_latency_ms_by_role?.client)],"""
    "\n"
    """      ['worker access', pctText(item.access_latency_ms_by_role?.worker)],"""
    "\n"
    """      ['access all', pctText(item.access_latency_ms)],"""
    "\n"
    """      ['key latencySummary', compactLatencySummary(item.latency_summary_us)],"""
    "\n"
    """      ['coverage', JSON.stringify(item.evidence_coverage || {})],"""
    "\n"
    """      ['missing_evidence', JSON.stringify(item.missing_evidence || [])]"""
    "\n"
    """    ].filter(row => row[1]);"""
    "\n"
    """    renderTable('selected-trace-table', ['field','value'], selectedTraceSummaryRows,"""
    "\n"
    """      row => row[0] === 'key latencySummary' || /access/.test(row[0]) ? 'class="summaryrow"' : '',"""
    "\n"
    """      {formatCell:(header, cell, row) => header === 'value' ? selectedTraceSummaryValueHtml(row[0], cell) """
    """: formatCell(header, cell)});"""
    "\n"
    """    const visibleStageRows = (item.stage_breakdown || [])"""
    "\n"
    """      .filter(s => stageMatchesOperation(s.stage))"""
    "\n"
    """      .slice()"""
    "\n"
    """      .sort((a,b) => (a.duration_ms || 0) - (b.duration_ms || 0));"""
    "\n"
    """    const stageRows = visibleStageRows.filter(s => s.duration_ms !== undefined);"""
    "\n"
    """    renderTable('selected-stage-table', ['研发流程','duration ms','confidence','source'], """
    """visibleStageRows.map(s => ["""
    "\n"
    """      stageDetailText(s.stage),"""
    "\n"
    """      s.duration_ms === undefined ? 'missing' : s.duration_ms,"""
    "\n"
    """      s.confidence || '',"""
    "\n"
    """      s.source || ''"""
    "\n"
    """    ]), row => row[1] === 'missing' ? 'class="warnrow"' : `class="${severityClass(row[1])}"`);"""
    "\n"
    """    const stageColors = ['#2563eb','#ea580c','#059669','#7c3aed','#dc2626','#0891b2','#ca8a04','#64748b'];"""
    "\n"
    """    document.getElementById('selected-stage-legend').innerHTML = stageRows.map((stage, idx) =>"""
    "\n"
    """      `<span class="stage-pill"><i class="stage-dot" style="background:${stageColors[idx % """
    """stageColors.length]}"></i>${escapeHtml(stageDisplayName(stage.stage))}</span>`"""
    "\n"
    """    ).join('');"""
    "\n"
    """    chart('selected-trace-chart', stageRows.length ? axisBase('Selected Trace Stage Breakdown', {"""
    "\n"
    """      legend:{show:false},"""
    "\n"
    """      dataZoom:[],"""
    "\n"
    """      tooltip:{trigger:'axis',axisPointer:{type:'shadow'},formatter:ps => ps.filter(p => p.data && """
    """p.data.value != null).map(p => `研发流程: ${escapeHtml(p.data.display)}<br>耗时: ${p.data.value}ms<br>原始字段: """
    """${escapeHtml(p.data.rawStage || '')}<br>观测来源: ${escapeHtml(p.data.source || '')}`).join('<br><br>')},"""
    "\n"
    """      grid:wideHorizontalGrid(128,44,30),"""
    "\n"
    """      xAxis:{type:'value', name:'ms'},"""
    "\n"
    """      yAxis:{type:'category', data:stageRows.map(s => stageDisplayName(s.stage)), axisLabel:{width:118, """
    """overflow:'truncate'}},"""
    "\n"
    """      series:[{"""
    "\n"
    """        name:'duration_ms',"""
    "\n"
    """        type:'bar',"""
    "\n"
    """        barMaxWidth:28,"""
    "\n"
    """        data:stageRows.map((row, rowIndex) => ({value:row.duration_ms, """
    """display:stageDisplayName(row.stage), rawStage:row.stage, source:row.source || '', """
    """itemStyle:{color:stageColors[rowIndex % stageColors.length]}})),"""
    "\n"
    """        label:{show:true, position:'right', formatter:p => p.data && p.data.value != null ? """
    """`${p.data.value}ms` : ''},"""
    "\n"
    """        markLine:{symbol:'none', lineStyle:{color:'#dc2626',type:'dashed'}, label:{formatter:'20ms """
    """deadline'}, data:[{xAxis:20}]}"""
    "\n"
    """      }]"""
    "\n"
    """    }) : noDataOption('No observed stage duration for selected operation'));"""
    "\n"
    """    document.getElementById('selected-trace-log').innerHTML = renderTraceLogBlocks(item.evidence || []);"""
    "\n"
    """  }"""
    "\n"
    """  document.getElementById('prev-page').addEventListener('click', () => { currentPage -= 1; """
    """renderTracePage(); });"""
    "\n"
    """  document.getElementById('next-page').addEventListener('click', () => { currentPage += 1; """
    """renderTracePage(); });"""
    "\n"
    """  document.getElementById('trace-search').addEventListener('input', () => { currentPage = 0; """
    """renderTracePage(); renderSelectedTrace(); });"""
    "\n"
    """  document.getElementById('request-status-filter').addEventListener('change', () => { currentPage = 0; """
    """renderTracePage(); renderSelectedTrace(); });"""
    "\n"
    """  document.getElementById('operation-filter').addEventListener('change', event => {"""
    "\n"
    """    activeOperation = event.target.value;"""
    "\n"
    """    currentPage = 0;"""
    "\n"
    """    renderOperationViews();"""
    "\n"
    """    renderTracePage();"""
    "\n"
    """    renderSelectedTrace();"""
    "\n"
    """  });"""
    "\n"
    """  setSelectOptions('class-filter', '全部分类', classificationRows.map(([name]) => name));"""
    "\n"
    """  document.getElementById('class-filter').addEventListener('change', () => { currentPage = 0; """
    """renderTracePage(); renderSelectedTrace(); });"""
    "\n"
    """  const traceWorkerNames = [...new Set(traceRows.flatMap(([, item]) => Object.keys(item.workers || """
    """{})))].sort();"""
    "\n"
    """  function workerFilterOptions() {"""
    "\n"
    """    const options = new Map();"""
    "\n"
    """    traceWorkerNames.forEach(name => {"""
    "\n"
    """      const key = workerAggregateKey(name);"""
    "\n"
    """      if (!options.has(key)) options.set(key, workerAggregateLabel(key));"""
    "\n"
    """    });"""
    "\n"
    """    return [...options.entries()].sort((a,b) => a[1].localeCompare(b[1], 'zh-CN', {numeric:true}));"""
    "\n"
    """  }"""
    "\n"
    """  setSelectOptions('worker-filter', '全部 Worker', workerFilterOptions());"""
    "\n"
    """  document.getElementById('worker-filter').addEventListener('change', () => { currentPage = 0; """
    """renderTracePage(); renderSelectedTrace(); });"""
    "\n"
    """  const workerScopedFilterIds = ["""
    "\n"
    """    'read-worker-filter',"""
    "\n"
    """    'read-worker-table-filter',"""
    "\n"
    """    'write-worker-filter',"""
    "\n"
    """    'write-worker-table-filter',"""
    "\n"
    """    'ub-worker-role-filter',"""
    "\n"
    """    'ub-worker-role-table-filter',"""
    "\n"
    """    'ub-worker-time-filter',"""
    "\n"
    """    'ub-worker-time-table-filter'"""
    "\n"
    """  ];"""
    "\n"
    """  workerScopedFilterIds.forEach(id => {"""
    "\n"
    """    const node = document.getElementById(id);"""
    "\n"
    """    if (!node) return;"""
    "\n"
    """    setSelectOptions(id, '全部 Worker', workerFilterOptions());"""
    "\n"
    """    node.addEventListener('change', renderWorkerDependentViews);"""
    "\n"
    """  });"""
    "\n"
    """  ['read-ub-src-filter','read-ub-dst-filter'].forEach(id => {"""
    "\n"
    """    const node = document.getElementById(id);"""
    "\n"
    """    if (node) node.addEventListener('change', () => {"""
    "\n"
    """      pagedTables['read-ub-edge-table'] = {page:0, sort:pagedTables['read-ub-edge-table']?.sort || null};"""
    "\n"
    """      renderUbSection('read');"""
    "\n"
    """      renderSectionSummaries();"""
    "\n"
    """    });"""
    "\n"
    """  });"""
    "\n"
    """  document.getElementById('trace-page-size').addEventListener('change', event => {"""
    "\n"
    """    pageSize = Number(event.target.value) || 4;"""
    "\n"
    """    currentPage = 0;"""
    "\n"
    """    renderTracePage();"""
    "\n"
    """    renderSelectedTrace();"""
    "\n"
    """  });"""
    "\n"
    """  document.getElementById('reset-filter').addEventListener('click', () => {"""
    "\n"
    """    document.getElementById('trace-search').value = '';"""
    "\n"
    """    document.getElementById('class-filter').value = '';"""
    "\n"
    """    document.getElementById('worker-filter').value = '';"""
    "\n"
    """    document.getElementById('operation-filter').value = '';"""
    "\n"
    """    document.getElementById('request-status-filter').value = '';"""
    "\n"
    """    document.getElementById('trace-page-size').value = '4';"""
    "\n"
    """    activeOperation = '';"""
    "\n"
    """    pageSize = 4;"""
    "\n"
    """    currentPage = 0;"""
    "\n"
    """    renderOperationViews();"""
    "\n"
    """    renderTracePage();"""
    "\n"
    """    renderSelectedTrace();"""
    "\n"
    """  });"""
    "\n"
    """  document.getElementById('download-selected-raw').addEventListener('click', () => {"""
    "\n"
    """    const item = traces[selectedTraceId] || {};"""
    "\n"
    """    const text = (item.evidence || []).map(e => `${e.member}:${e.line} ${e.text}`).join('\\n');"""
    "\n"
    """    downloadText(`${selectedTraceId || 'trace'}-raw.log`, text);"""
    "\n"
    """  });"""
    "\n"
    """  document.getElementById('download-filtered-evidence').addEventListener('click', () => {"""
    "\n"
    """    const rows = filteredTraceRows.map(([traceId, item]) => ["""
    "\n"
    """      `# ${traceId} ${item.classification || ''}`,"""
    "\n"
    """      ...(item.evidence || []).map(e => `${e.member}:${e.line} ${e.text}`)"""
    "\n"
    """    ].join('\\n')).join('\\n\\n');"""
    "\n"
    """    downloadText('filtered-trace-evidence.log', rows);"""
    "\n"
    """  });"""
    "\n"
    """  document.getElementById('download-report-summary').addEventListener('click', () => {"""
    "\n"
    """    const lines = ["""
    "\n"
    """      '# DataSystem Trace Report Summary',"""
    "\n"
    """      '',"""
    "\n"
    """      `- case_name: ${manifest.case_name || ''}`,"""
    "\n"
    """      `- scenario: ${manifest.scenario || ''}`,"""
    "\n"
    """      `- code_ref: ${report.code_ref || ''}`,"""
    "\n"
    """      `- trace_count: ${report.trace_count || 0}`,"""
    "\n"
    """      '',"""
    "\n"
    """      '## Diagnosis',"""
    "\n"
    """      ...diagnosisRows.map(item => `- ${item.label}: ${item.text}`),"""
    "\n"
    """      '',"""
    "\n"
    """      '## Evidence Coverage',"""
    "\n"
    """      ...coverageRows.map(([name,item]) => `- ${name}: ${item.status || 'missing'} (${item.events || 0} """
    """events)`),"""
    "\n"
    """      '',"""
    "\n"
    """      '## Recommendations',"""
    "\n"
    """      ...recommendations.map(item => `- [${item.category}] ${item.title}: ${item.detail}`),"""
    "\n"
    """      '',"""
    "\n"
    """      '## Source Mapping',"""
    "\n"
    """      ...sourceAppendix.map(item => `- ${item.log_surface} | ${item.flow_stage} | ${item.source_hint}`),"""
    "\n"
    """      '',"""
    "\n"
    """      '## Inputs',"""
    "\n"
    """      ...(manifest.inputs || []).map(item => `- ${item.path} (${item.size || 0} bytes)`),"""
    "\n"
    """      '',"""
    "\n"
    """    ];"""
    "\n"
    """    downloadText('trace-report-summary.md', lines.join('\\n'));"""
    "\n"
    """  });"""
    "\n"
    """  const navLinks = [...document.querySelectorAll('#nav a')];"""
    "\n"
    """  window.addEventListener('scroll', () => {"""
    "\n"
    """    let active = navLinks[0];"""
    "\n"
    """    for (const link of navLinks) {"""
    "\n"
    """      const node = document.querySelector(link.getAttribute('href'));"""
    "\n"
    """      if (node && node.getBoundingClientRect().top < 120) active = link;"""
    "\n"
    """    }"""
    "\n"
    """    navLinks.forEach(link => link.classList.toggle('active', link === active));"""
    "\n"
    """  });"""
    "\n"
    """  chart('classification-chart', classificationRows.length ? {title:{show:false}, color:palette, """
    """textStyle:chartTextStyle, tooltip:{trigger:'item', confine:true}, legend:{type:'scroll', bottom:0}, """
    """series:[{type:'pie', radius:['38%','66%'], center:['50%','48%'], avoidLabelOverlap:true, """
    """data:classificationRows.map(([name,value]) => ({name,value}))}]} : noDataOption('No classification """
    """data'));"""
    "\n"
    """  chart('cohort-chart', cohortRows.length ? axisBase('Input Cohort Comparison', {xAxis:{type:'category', """
    """data:cohortRows.map(r => r[0]), axisLabel:{rotate:20, width:110, overflow:'truncate'}}, """
    """yAxis:{type:'value'}, series:["""
    "\n"
    """    {name:'traces',type:'bar',barMaxWidth:42,data:cohortRows.map(r => r[1].trace_count || 0), """
    """label:{show:true, position:'top'}},"""
    "\n"
    """    {name:'errors',type:'bar',barMaxWidth:42,data:cohortRows.map(r => Object.values(r[1].errors || """
    """{}).reduce((a,b) => a+b, 0)), label:{show:true, position:'top'}, itemStyle:{color:'#dc2626'}}"""
    "\n"
    """  ]}) : noDataOption('No cohort data'));"""
    "\n"
    """  chart('error-chart', errorRows.length ? axisBase('Errors', {xAxis:{type:'category', """
    """data:errorRows.map(r => r[0]), axisLabel:{rotate:25, width:130, overflow:'truncate'}}, """
    """yAxis:{type:'value'}, series:[{type:'bar', barMaxWidth:46, data:errorRows.map(r => ({value:r[1], """
    """itemStyle:{color:'#dc2626'}})), label:{show:true, position:'top'}}]}) : noDataOption('No error data'));"""
    "\n"
    """  function flowToolbox() {"""
    "\n"
    """    return {right:10,feature:{"""
    "\n"
    """      myAutoCenter:{"""
    "\n"
    """        show:true,"""
    "\n"
    """        title:'自适应居中',"""
    "\n"
    """        icon:'path://M512 128l96 96-48 48-16-16v160h160l-16-16 48-48 96 96-96 96-48-48 """
    """16-16H544v160l16-16 48 48-96 96-96-96 48-48 16 16V608H320l16 16-48 48-96-96 96-96 48 48-16 """
    """16h160V384H320l16 16-48 48-96-96 96-96 48 48-16 16h160V256l-16 16-48-48 96-96z',"""
    "\n"
    """        onclick:(ecModel, api) => autoCenterFlowGraph(api)"""
    "\n"
    """      }"""
    "\n"
    """    }};"""
    "\n"
    """  }"""
    "\n"
    """  function flowNodeLabel(node) {"""
    "\n"
    """    return node.label;"""
    "\n"
    """  }"""
    "\n"
    """  function flowEdgeBriefText(edge) {"""
    "\n"
    """    const rollup = edge.rollup || {};"""
    "\n"
    """    const hasSamples = Number(rollup.trace_count || 0) > 0;"""
    "\n"
    """    const maxMs = Number(rollup.max_ms);"""
    "\n"
    """    const p99Ms = Number(rollup.p99_ms);"""
    "\n"
    """    const latency = hasSamples && Number.isFinite(maxMs) && maxMs > 0 ? `max=${maxMs.toFixed(3)}ms` :"""
    "\n"
    """      hasSamples && Number.isFinite(p99Ms) && p99Ms > 0 ? `p99=${p99Ms.toFixed(3)}ms` : '';"""
    "\n"
    """    const worker = (rollup.top_workers || []).map(workerRelationName).filter(Boolean)[0];"""
    "\n"
    """    const abnormalIp = flowEdgeAbnormalIps(edge).split(', ')[0];"""
    "\n"
    """    return [edge.operation, latency || '未采样', abnormalIp ? `异常IP ${abnormalIp}` : """
    """worker].filter(Boolean).join('\\n');"""
    "\n"
    """  }"""
    "\n"
    """  function flowEdgeAbnormalIps(edge) {"""
    "\n"
    """    const isSlow = Number(edge.rollup?.max_ms || 0) >= 20 || Number(edge.rollup?.p99_ms || 0) >= 20;"""
    "\n"
    """    const isMissing = edge.status && edge.status !== 'present';"""
    "\n"
    """    if (!isSlow && !isMissing) return '';"""
    "\n"
    """    return (edge.rollup?.top_ips || []).slice(0, 4).join(', ');"""
    "\n"
    """  }"""
    "\n"
    """  function flowEdgeCurveness(edge) {"""
    "\n"
    """    if (edge.operation === 'CreateBuffer') return .08;"""
    "\n"
    """    if (edge.operation === 'Client Publish') return -.08;"""
    "\n"
    """    if (edge.operation === 'Client→Entry RPC/UB') return 0;"""
    "\n"
    """    if (edge.operation === 'Entry→Meta RPC') return 0;"""
    "\n"
    """    if (edge.operation === 'Entry→Data RPC') return .1;"""
    "\n"
    """    if (edge.operation === 'URMA Write') return -.1;"""
    "\n"
    """    return 0;"""
    "\n"
    """  }"""
    "\n"
    """  function flowEdgeLabelOffset(edge) {"""
    "\n"
    """    if (edge.operation === 'CreateBuffer') return [0, -22];"""
    "\n"
    """    if (edge.operation === 'Client Publish') return [0, 22];"""
    "\n"
    """    if (edge.operation === 'Entry→Data RPC') return [0, 22];"""
    "\n"
    """    if (edge.operation === 'URMA Write') return [0, -22];"""
    "\n"
    """    return [0, 0];"""
    "\n"
    """  }"""
    "\n"
    """  function flowEdgeLabelNodeData(edges, positions) {"""
    "\n"
    """    return edges.map((edge, index) => {"""
    "\n"
    """      const source = positions[edge.source];"""
    "\n"
    """      const target = positions[edge.target];"""
    "\n"
    """      if (!source || !target) return null;"""
    "\n"
    """      const [offsetX, offsetY] = flowEdgeLabelOffset(edge);"""
    "\n"
    """      const maxMs = Number(edge.rollup?.max_ms || 0);"""
    "\n"
    """      return {"""
    "\n"
    """        name:`__flow_edge_label_${index}`,"""
    "\n"
    """        edge_text:flowEdgeBriefText(edge),"""
    "\n"
    """        x:Math.round((source.x + target.x) / 2 + offsetX),"""
    "\n"
    """        y:Math.round((source.y + target.y) / 2 + offsetY),"""
    "\n"
    """        symbolSize:1,"""
    "\n"
    """        fixed:true,"""
    "\n"
    """        silent:true,"""
    "\n"
    """        itemStyle:{color:'transparent', borderColor:'transparent'},"""
    "\n"
    """        tooltip:{show:false},"""
    "\n"
    """        label:{"""
    "\n"
    """          show:true,"""
    "\n"
    """          formatter:p => p.data.edge_text,"""
    "\n"
    """          position:'inside',"""
    "\n"
    """          fontSize:FLOW_GRAPH_LABEL_FONT_SIZE,"""
    "\n"
    """          fontWeight:maxMs >= 20 ? 700 : 500,"""
    "\n"
    """          color:maxMs >= 20 ? '#b91c1c' : '#334155',"""
    "\n"
    """          width:160,"""
    "\n"
    """          overflow:'break',"""
    "\n"
    """          align:'center',"""
    "\n"
    """          verticalAlign:'middle',"""
    "\n"
    """          backgroundColor:'rgba(255,255,255,.9)',"""
    "\n"
    """          borderRadius:3,"""
    "\n"
    """          padding:[2,4]"""
    "\n"
    """        }"""
    "\n"
    """      };"""
    "\n"
    """    }).filter(Boolean);"""
    "\n"
    """  }"""
    "\n"
    """  function flowGraphNodeSize() {"""
    "\n"
    """    return 72;"""
    "\n"
    """  }"""
    "\n"
    """  const FLOW_GRAPH_LABEL_FONT_SIZE = 14;"""
    "\n"
    """  function renderFlowGraph(id, graph, title) {"""
    "\n"
    """    chartRenderers.set(id, () => renderFlowGraph(id, graph, title));"""
    "\n"
    """    const node = document.getElementById(id);"""
    "\n"
    """    const graphWidth = Math.max(720, node?.clientWidth || 0);"""
    "\n"
    """    const flowNodeX = [0.12,0.34,0.58,0.58,0.82].map(r => Math.round(graphWidth * r));"""
    "\n"
    """    const flowNodeY = [170,170,82,258,258];"""
    "\n"
    """    const graphNodePositions = Object.fromEntries((graph.nodes || []).map((item, idx) => [item.id, {"""
    "\n"
    """      x:flowNodeX[idx] || flowNodeX[0],"""
    "\n"
    """      y:flowNodeY[idx] || 130"""
    "\n"
    """    }]));"""
    "\n"
    """    const graphNodeData = (graph.nodes || []).map((node, idx) => ({"""
    "\n"
    """      name:node.id,"""
    "\n"
    """      label:flowNodeLabel(node),"""
    "\n"
    """      top_ips:node.top_ips || [],"""
    "\n"
    """      top_workers:node.top_workers || [],"""
    "\n"
    """      x:graphNodePositions[node.id].x,"""
    "\n"
    """      y:graphNodePositions[node.id].y,"""
    "\n"
    """      symbolSize:flowGraphNodeSize(),"""
    "\n"
    """      fixed:true,"""
    "\n"
    """      itemStyle:{color:{client:'#2563eb',entry_worker:'#059669',meta_worker:'#7c3aed',data_worker:'#ea580c"""
    """',transport:'#64748b'}[node.role] || '#94a3b8'}"""
    "\n"
    """    }));"""
    "\n"
    """    const graphEdgeLabelData = flowEdgeLabelNodeData(graph.edges || [], graphNodePositions);"""
    "\n"
    """    chart(id, {"""
    "\n"
    """    title:{text:title, left:'center', top:4, textStyle:{fontSize:14}},"""
    "\n"
    """    textStyle:chartTextStyle,"""
    "\n"
    """    tooltip:{trigger:'item', formatter:p => p.dataType === 'edge'"""
    "\n"
    """      ? `${escapeHtml(p.data.name)}<br>${escapeHtml(p.data.summary || """
    """'')}<br>${escapeHtml(p.data.operation)}<br>异常 IP: ${escapeHtml(p.data.abnormal_ips || """
    """'')}<br>${escapeHtml(p.data.reason || '')}<br>${escapeHtml(p.data.evidence || '')}`"""
    "\n"
    """      : `${escapeHtml(p.data.label || p.data.name)}<br>${escapeHtml((p.data.top_ips || []).join(', '))}`},"""
    "\n"
    """    series:[{"""
    "\n"
    """      type:'graph',"""
    "\n"
    """      layout:'none',"""
    "\n"
    """      roam:false,"""
    "\n"
    """      draggable:false,"""
    "\n"
    """      edgeSymbol:['none','arrow'],"""
    "\n"
    """      edgeSymbolSize:8,"""
    "\n"
    """      label:{show:true, fontSize:FLOW_GRAPH_LABEL_FONT_SIZE},"""
    "\n"
    """      labelLayout:{hideOverlap:false},"""
    "\n"
    """      edgeLabel:{show:false},"""
    "\n"
    """      lineStyle:{width:2, color:'#64748b', curveness:.08},"""
    "\n"
    """      data:[...graphNodeData, ...graphEdgeLabelData],"""
    "\n"
    """      links:(graph.edges || []).map(edge => ({"""
    "\n"
    """        source:edge.source,"""
    "\n"
    """        target:edge.target,"""
    "\n"
    """        name:edge.name,"""
    "\n"
    """        operation:edge.operation,"""
    "\n"
    """        evidence:edge.evidence,"""
    "\n"
    """        summary:edge.summary,"""
    "\n"
    """        edge_label:flowEdgeBriefText(edge),"""
    "\n"
    """        abnormal_ips:flowEdgeAbnormalIps(edge),"""
    "\n"
    """        reason:edge.reason,"""
    "\n"
    """        rollup:edge.rollup,"""
    "\n"
    """        status:edge.status,"""
    "\n"
    """        lineStyle:{color:edge.rollup?.max_ms >= 20 ? '#dc2626' : edge.rollup?.max_ms >= 5 ? '#ea580c' : """
    """edge.status === 'present' ? '#2563eb' : '#cbd5e1', width:edge.rollup?.max_ms >= 20 ? 4 : 2, """
    """type:edge.status === 'present' ? 'solid' : 'dashed', curveness:flowEdgeCurveness(edge)}"""
    "\n"
    """      }))"""
    "\n"
    """    }]"""
    "\n"
    """  });"""
    "\n"
    """  }"""
    "\n"
    """  function fmtMs(value) {"""
    "\n"
    """    const n = Number(value);"""
    "\n"
    """    return Number.isFinite(n) ? `${Number(n.toFixed(3))}ms` : '未采样';"""
    "\n"
    """  }"""
    "\n"
    """  function fmtCount(value) {"""
    "\n"
    """    const n = Number(value);"""
    "\n"
    """    return Number.isFinite(n) ? String(n) : '0';"""
    "\n"
    """  }"""
    "\n"
    """  function firstEntry(rows, fallback=['无数据', null]) {"""
    "\n"
    """    return rows && rows.length ? rows[0] : fallback;"""
    "\n"
    """  }"""
    "\n"
    """  function topLatencyText(rows) {"""
    "\n"
    """    const [name, item] = firstEntry(rows);"""
    "\n"
    """    if (!item || !item.count) return '无有效时延样本';"""
    "\n"
    """    return `${metricLabel(name)} max=${fmtMs(item.max)}, p99=${fmtMs(item.p99)}`;"""
    "\n"
    """  }"""
    "\n"
    """  function topTimeBucketText(operation) {"""
    "\n"
    """    const rows = buildTraceTimeBuckets(operation);"""
    "\n"
    """    if (!rows.length) return '无时间桶样本';"""
    "\n"
    """    const top = rows.slice().sort((a,b) => Number(b.p99_access_ms || 0) - Number(a.p99_access_ms || """
    """0))[0];"""
    "\n"
    """    const stage = Object.entries(top.stage_breakdown_ms || {})"""
    "\n"
    """      .sort((a,b) => Number(b[1]?.p99 || 0) - Number(a[1]?.p99 || 0))[0];"""
    "\n"
    """    const stageText = stage ? `${stageDisplayName(stage[0])} p99=${fmtMs(stage[1]?.p99)}` : '子阶段未采样';"""
    "\n"
    """    return `${String(top.bucket_start || '').replace('T',' ')} access """
    """p99=${fmtMs(top.p99_access_ms)}，${stageText}`;"""
    "\n"
    """  }"""
    "\n"
    """  function topWorkerText(rows) {"""
    "\n"
    """    const [worker, item] = firstEntry(rows);"""
    "\n"
    """    if (!item) return '无 worker 聚合样本';"""
    "\n"
    """    return `${item.display_worker || workerAggregateLabel(worker)}: traces=${fmtCount(item.trace_count)}, """
    """slow=${fmtCount(item.slow_trace_count)}, errors=${fmtCount(item.error_count)}`;"""
    "\n"
    """  }"""
    "\n"
    """  function topFlowStageText(graph) {"""
    "\n"
    """    const edge = (graph.edges || []).slice().sort((a,b) =>"""
    "\n"
    """      Number(b.rollup?.max_ms || 0) - Number(a.rollup?.max_ms || 0) ||"""
    "\n"
    """      Number(b.rollup?.count || 0) - Number(a.rollup?.count || 0)"""
    "\n"
    """    )[0];"""
    "\n"
    """    if (!edge) return '流程阶段证据不足';"""
    "\n"
    """    return `${edge.name}: ${edge.summary || edge.status || 'observed'}`;"""
    "\n"
    """  }"""
    "\n"
    """  function topUbMetricText() {"""
    "\n"
    """    const [name, item] = firstEntry(ubLifecycleLatencyMetrics);"""
    "\n"
    """    if (!item || !item.count) return 'UB 耗时字段未采样';"""
    "\n"
    """    return `${lifecycleLabel(name)} max=${fmtMs(item.max)}, p99=${fmtMs(item.p99)}`;"""
    "\n"
    """  }"""
    "\n"
    """  function topUbWorkerText() {"""
    "\n"
    """    const [worker, item] = firstEntry(ubWorkerRows);"""
    "\n"
    """    if (!item) return '无 UB worker 聚合样本';"""
    "\n"
    """    return `${worker}: role=${item.role || ''}, entry=${fmtCount(item.entry_events)}, """
    """exit=${fmtCount(item.exit_events)}, max=${fmtMs(item.latency_ms?.max)}`;"""
    "\n"
    """  }"""
    "\n"
    """  function topUbEdgeText() {"""
    "\n"
    """    const [edge, item] = firstEntry(ubRows);"""
    "\n"
    """    if (!item) return '无 UB edge 样本';"""
    "\n"
    """    return `${edge}: count=${fmtCount(item.count)}, p99=${fmtMs(item.latency_ms?.p99)}`;"""
    "\n"
    """  }"""
    "\n"
    """  function topTraceText() {"""
    "\n"
    """    const [traceId, item] = firstEntry(traceRows);"""
    "\n"
    """    if (!item) return '无 trace 样本';"""
    "\n"
    """    const workers = Object.keys(item.workers || {}).slice(0, 3).join(', ') || 'unknown worker';"""
    "\n"
    """    return `${String(item.first_ts || item.last_ts || '').replace('T',' ')} ${traceId}: """
    """${item.classification || 'unknown'}, access=${fmtMs(traceAccessLatencyMs(item))}, workers=${workers}`;"""
    "\n"
    """  }"""
    "\n"
    """  function missingCoverageText() {"""
    "\n"
    """    const missing = coverageRows.filter(([, item]) => item.status !== 'present').map(([name]) => """
    """name).slice(0, 3);"""
    "\n"
    """    return missing.length ? `缺失观测面：${missing.join(', ')}` : '关键观测面已采样';"""
    "\n"
    """  }"""
    "\n"
    """  function highlightSummaryLatency(match, raw) {"""
    "\n"
    """    const value = Number(raw);"""
    "\n"
    """    const cls = Number.isFinite(value) && value >= 20 ? 'summary-hot' : Number.isFinite(value) && value """
    """>= 5 ? 'summary-warn' : 'summary-key';"""
    "\n"
    """    return `<span class="${cls}">${match}</span>`;"""
    "\n"
    """  }"""
    "\n"
    """  function highlightSummaryText(text) {"""
    "\n"
    """    return escapeHtml(text)"""
    "\n"
    """      .replace(/\\b(\\d+(?:\\.\\d+)?)ms\\b/g, highlightSummaryLatency)"""
    "\n"
    """      .replace(/(deadline|timeout|error|errors|失败|异常|异常耗时|异常时延|缺失|未采样|Top error)/gi, '<span """
    """class="summary-hot">$1</span>')"""
    "\n"
    """      .replace(/(瓶颈|热点|集中|Top trace|Top edge|access|p99|max|slow|worker|Worker|UB|URMA)/g, '<span """
    """class="summary-warn">$1</span>')"""
    "\n"
    """      .replace(/(主分类|读取|写入|流程|建议|原始 JSON|证据|时间|入口\\/出口)/g, '<span class="summary-key">$1</span>');"""
    "\n"
    """  }"""
    "\n"
    """  function summaryPointsHtml(summary) {"""
    "\n"
    """    const points = Array.isArray(summary?.points) ? summary.points : String(summary || """
    """'').split(/[；;]/).filter(Boolean);"""
    "\n"
    """    return `<b>本章结论：</b><ul class="summary-points">${points.map(point => """
    """`<li>${highlightSummaryText(point.replace(/[。.]$/, ''))}</li>`).join('')}</ul>`;"""
    "\n"
    """  }"""
    "\n"
    """  function setSectionSummary(id, summary) {"""
    "\n"
    """    const node = document.getElementById(id);"""
    "\n"
    """    if (node) node.innerHTML = summaryPointsHtml(summary);"""
    "\n"
    """  }"""
    "\n"
    """  function chapterSummaryTexts() {"""
    "\n"
    """    const topCohort = cohortRows.slice().sort((a,b) => Object.values(b[1].errors || """
    """{}).reduce((x,y)=>x+y,0) - Object.values(a[1].errors || {}).reduce((x,y)=>x+y,0))[0];"""
    "\n"
    """    const topCohortText = topCohort ? `${topCohort[0]} """
    """errors=${fmtCount(Object.values(topCohort[1].errors || {}).reduce((a,b)=>a+b,0))}` : '无 cohort 对比';"""
    "\n"
    """    return {"""
    "\n"
    """      s2: {points:[`主分类 ${topClass[0]}=${fmtCount(topClass[1])}`, `Top error ${errorRows[0]?.[0] || """
    """'无'}=${fmtCount(errorRows[0]?.[1])}`, topCohortText]},"""
    "\n"
    """      s3: {points:[`读取瓶颈 ${topLatencyText(latencyRowsForOperation('read'))}`, `写入瓶颈 """
    """${topLatencyText(latencyRowsForOperation('write'))}`, `时间热点：读 ${topTimeBucketText('read')}，写 """
    """${topTimeBucketText('write')}`]},"""
    "\n"
    """      s4: {points:[`流程热点：读 ${topFlowStageText(readFlowStages)}；写 ${topFlowStageText(writeFlowStages)}`, """
    """`Worker 集中：读 ${topWorkerText(workerRowsForOperation('read'))}`, `Worker 集中：写 """
    """${topWorkerText(workerRowsForOperation('write'))}`]},"""
    "\n"
    """      s5: {points:[`UB 耗时 ${topUbMetricText()}`, `入口/出口集中 ${topUbWorkerText()}`, `Top edge """
    """${topUbEdgeText()}`]},"""
    "\n"
    """      s6: {points:[`默认按接口 access 时延降序`, `Top trace ${topTraceText()}`, `建议先切换“失败/成功”和“读/写”筛选，再看原始日志块摘要`]},"""
    "\n"
    """      s7: {points:[recommendations[0]?.title || '暂无自动建议', missingCoverageText()]},"""
    "\n"
    """      s8: {points:[`原始 JSON 保留 ${fmtCount(Object.keys(traces).length)} 条 trace`, '可用于复现 parser """
    """聚合和人工二次确认']}"""
    "\n"
    """    };"""
    "\n"
    """  }"""
    "\n"
    """  function renderChapterGuide(summaries) {"""
    "\n"
    """    const guideRows = ["""
    "\n"
    """      ['#s2', '2. 根因分布', summaries.s2, '图 2-1/2-2/2-3'],"""
    "\n"
    """      ['#s3', '3. 时延 Breakdown', summaries.s3, '图 3-1/3-3'],"""
    "\n"
    """      ['#s4', '4. Worker / 流程', summaries.s4, '图 4-1/4-2/4-3/4-4'],"""
    "\n"
    """      ['#s5', '5. UB / URMA', summaries.s5, '图 5-1/5-2/5-3/5-4/5-5/5-6'],"""
    "\n"
    """      ['#s6', '6. Trace 查看', summaries.s6, '表 6-1 / 图 6-1 / 日志框 6-3'],"""
    "\n"
    """      ['#s7', '7. 建议与口径', summaries.s7, '表 7-1/7-2']"""
    "\n"
    """    ];"""
    "\n"
    """    const node = document.getElementById('chapter-guide-list');"""
    "\n"
    """    if (!node) return;"""
    "\n"
    """    node.innerHTML = guideRows.map(([href, title, text, refs]) =>"""
    "\n"
    """      `<li><a href="${href}">${escapeHtml(title)}</a>${summaryPointsHtml(text)}<div """
    """class="small">${escapeHtml(refs)}</div></li>`"""
    "\n"
    """    ).join('');"""
    "\n"
    """  }"""
    "\n"
    """  function renderSectionSummaries() {"""
    "\n"
    """    const summaries = chapterSummaryTexts();"""
    "\n"
    """    setSectionSummary('section-summary-s2', summaries.s2);"""
    "\n"
    """    setSectionSummary('section-summary-s3', summaries.s3);"""
    "\n"
    """    setSectionSummary('section-summary-s4', summaries.s4);"""
    "\n"
    """    setSectionSummary('section-summary-s5', summaries.s5);"""
    "\n"
    """    setSectionSummary('section-summary-s6', summaries.s6);"""
    "\n"
    """    setSectionSummary('section-summary-s7', summaries.s7);"""
    "\n"
    """    setSectionSummary('section-summary-s8', summaries.s8);"""
    "\n"
    """    renderChapterGuide(summaries);"""
    "\n"
    """  }"""
    "\n"
    """  renderSectionSummaries();"""
    "\n"
    """  renderLatencySection('read', 'Read');"""
    "\n"
    """  renderLatencySection('write', 'Write');"""
    "\n"
    """  renderFlowSection('read', 'Read');"""
    "\n"
    """  renderFlowSection('write', 'Write');"""
    "\n"
    """  renderTimeBucketSection('read', 'Read');"""
    "\n"
    """  renderTimeBucketSection('write', 'Write');"""
    "\n"
    """  renderFlowGraph('read-flow-stage-chart', readFlowStages, 'Read Flow');"""
    "\n"
    """  renderFlowGraph('write-flow-stage-chart', writeFlowStages, 'Write Flow');"""
    "\n"
    """  renderUbLifecycleViews();"""
    "\n"
    """  renderUbWorkerViews();"""
    "\n"
    """  renderWorkerSection('read', 'Read');"""
    "\n"
    """  renderWorkerSection('write', 'Write');"""
    "\n"
    """  renderUbSection('read');"""
    "\n"
    """  renderUbSection('write');"""
    "\n"
    """  renderTracePage();"""
    "\n"
    """  renderSelectedTrace();"""
    "\n"
    """  installResponsiveCharts();"""
    "\n"
    """  document.getElementById('trace-data').textContent = JSON.stringify(traces, null, 2);"""
    "\n"
    """  </script>"""
    "\n"
    """  __SCRIPT_REF__"""
    "\n"
    """</body>"""
    "\n"
    """</html>"""
    "\n"
    """"""
)
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

    @staticmethod
    def events(report):
        return _build_events(report)

    @staticmethod
    def triage(report):
        return _build_triage(report)

    @staticmethod
    def markdown(report):
        return render_markdown(report)

    @staticmethod
    def html(report, title, site=False, manifest=None):
        return _render_html(report, title, site=site, manifest=manifest)


def _new_run_dir(out_root, case_name, cache_key):
    now = datetime.now(timezone.utc)
    run_dir = out_root / f"{now.strftime('%Y%m%d-%H%M%S')}-{_slug(case_name)}-{cache_key[:8]}"
    suffix = 1
    while run_dir.exists():
        suffix += 1
        run_dir = out_root / f"{now.strftime('%Y%m%d-%H%M%S')}-{_slug(case_name)}-{suffix}"
    run_dir.mkdir(parents=True)
    return run_dir, now


class TraceRunStore:
    """Own run-directory cache, manifest, and artifact file operations."""

    @staticmethod
    def prepare_parse_run(inputs, out_dir, options, rules_fingerprint="default"):
        options = options or RunOptions()
        out_root = Path(out_dir)
        out_root.mkdir(parents=True, exist_ok=True)
        cache_key, identities = _cache_key(
            inputs, options.code_ref, options.case_name, options.scenario, rules_fingerprint
        )
        if not options.force:
            cached = _find_cached_run(out_root, cache_key)
            if cached:
                return {"run_dir": cached, "cached": True}
        run_dir, created_at = _new_run_dir(out_root, options.case_name, cache_key)
        _preserve_raw_inputs(inputs, run_dir)
        return {
            "run_dir": run_dir,
            "created_at": created_at,
            "cache_key": cache_key,
            "identities": identities,
            "cached": False,
        }

    def write_parse_outputs(self, run_dir, options, bundle):
        options = options or RunOptions()
        manifest = {
            "schema_version": 1,
            "case_name": options.case_name,
            "scenario": options.scenario,
            "analysis_created_at": bundle.created_at.isoformat(),
            "code_ref": options.code_ref,
            "script_version": _script_version(),
            "cache": {"key": bundle.cache_key, "status": "created"},
            "trace_time_range": bundle.report["dimensions"]["time"],
            "input_document": "inputs.md",
            "inputs": bundle.identities,
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
            "".join(json.dumps(event, ensure_ascii=False) + "\n" for event in bundle.events),
            encoding="utf-8",
        )
        self.write_json(run_dir / "parsed_traces.json", bundle.report)

    @staticmethod
    def read_json(path):
        return _read_json(path)

    @staticmethod
    def write_json(path, value):
        _write_json(path, value)

    @staticmethod
    def update_manifest(run_dir, updater):
        _update_manifest(run_dir, updater)

    @staticmethod
    def write_text(path, text):
        Path(path).write_text(text, encoding="utf-8")

    @staticmethod
    def write_site_publish_doc(run_dir, manifest):
        return _write_site_publish_doc(run_dir, manifest)


class TraceSitePublisher:
    """Publish site reports with size guard and live marker verification."""

    def __init__(self, store=None, pipeline_factory=None):
        self.store = store or TraceRunStore()
        self.pipeline_factory = pipeline_factory or TraceRunPipeline

    @staticmethod
    def size_guard(source_html, max_bytes=DEFAULT_SITE_HTML_MAX_BYTES):
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
            raise TraceTriageError(
                f"Refuse to publish oversized site HTML: {source_html} is "
                f"{size_guard['source_size_bytes']} bytes > {max_site_html_bytes} bytes. "
                "Review the report or raise --max-site-html-mb intentionally."
            )
        else:
            self._copy_and_verify(source_html, site_target)
            live_markers = "verified"
            status = "copied"
        self._record_publish(run_dir, site_target, status, live_markers, size_guard)
        return site_target.get("url", "")

    @staticmethod
    def _copy_and_verify(source_html, site_target):
        publish_config = _site_publish_config(require_target=True)
        target_path = site_target.get("target_path", "")
        url = site_target.get("url", "")
        if target_path.startswith("<publish-root>"):
            filename = Path(target_path).name
            target_path = f"{publish_config['root']}/{filename}"
        scp_bin = shutil.which("scp") or "/usr/bin/scp"
        curl_bin = shutil.which("curl") or "/usr/bin/curl"
        subprocess.run(
            [scp_bin, str(source_html), f"{publish_config['host']}:{target_path}"],
            check=True,
        )
        subprocess.run([curl_bin, "-fsSI", url], check=True)
        result = subprocess.run(
            [curl_bin, "-fsSL", "-A", "Mozilla/5.0", url],
            check=True,
            capture_output=True,
            text=True,
        )
        for marker in [
            "Trace 分析报告",
            'id="coverage-table"',
            'id="flow-stage-chart"',
            'id="download-report-summary"',
            "/assets/css/site.css",
            "/assets/js/site.js",
        ]:
            if not (marker in result.stdout):
                raise AssertionError(marker)

    def _record_publish(self, run_dir, site_target, status, live_markers, size_guard):
        self.store.update_manifest(run_dir, lambda item: item["render_targets"]["site"].update({
            **site_target,
            "publish": {
                "status": status,
                "catalog_status": "not-registered" if status == "copied" else "not-run",
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

    def parse(self, inputs, out_dir, options=None, *legacy_args, **legacy_kwargs):
        options = _resolve_run_options(options, legacy_args, legacy_kwargs)
        rules_fingerprint = (
            self.analyzer.rules.fingerprint()
            if hasattr(self.analyzer.rules, "fingerprint")
            else "default"
        )
        prepared = self.store.prepare_parse_run(
            inputs, out_dir, options, rules_fingerprint=rules_fingerprint
        )
        if prepared["cached"]:
            return prepared["run_dir"]
        run_dir = prepared["run_dir"]
        report = self.analyzer.analyze(
            inputs,
            code_ref=options.code_ref,
            allow_partial_inputs=options.allow_partial_inputs,
        )
        events = self.renderer.events(report)
        bundle = ParseOutputBundle(
            report=report,
            events=events,
            created_at=prepared["created_at"],
            cache_key=prepared["cache_key"],
            identities=prepared["identities"],
        )
        self.store.write_parse_outputs(run_dir, options, bundle)
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

    def run(self, inputs, out_dir, options=None, *legacy_args, **legacy_kwargs):
        options = _resolve_run_options(options, legacy_args, legacy_kwargs)
        run_dir = self.parse(inputs, out_dir, options=options)
        if not (run_dir / "summary.json").exists():
            self.aggregate(run_dir)
        if not (run_dir / "triage.json").exists():
            self.triage(run_dir)
        if not (run_dir / "report.local.html").exists():
            self.render_local(run_dir)
        if not (run_dir / "report.site.html").exists():
            self.render_site(run_dir)
        return run_dir


def parse_stage(inputs, out_dir, options=None, *legacy_args, **legacy_kwargs):
    return TraceRunPipeline().parse(inputs, out_dir, options, *legacy_args, **legacy_kwargs)


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
    if not (match):
        raise AssertionError('inline report script not found')
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


def run_pipeline(inputs, out_dir, options=None, *legacy_args, **legacy_kwargs):
    return TraceRunPipeline().run(inputs, out_dir, options, *legacy_args, **legacy_kwargs)


def _make_self_test_bundle(path):
    trace_id = "019f7b27-56f0-74f0-9a68-5b3742f11e23"
    content = "\n".join([
        (
            f"2026-07-18T19:20:03.100000 | INFO | access_recorder | 192.0.2.10 | 42 | "
            f"{trace_id} | - | 0 | DS_KV_CLIENT_GET | 518923 | 4096"
        ),
        (
            f"2026-07-18T19:20:03.110000 | INFO | client | 192.0.2.10 | 42 | {trace_id} | "
            f"Get done latencySummary:{{client.rpc.get:20298, client.process.get:10}}"
        ),
        (
            f"2026-07-18T19:20:03.130000 | INFO | worker | 192.0.2.10 | 42 | {trace_id} | "
            f"[Get] Done, totalCost: 518.9ms, exceed 3ms: "
            f"{{ ProcessGetObjectRequest: 517 ms, QueryMeta: 0 ms }}"
        ),
        (
            f"2026-07-18T19:20:03.150000 | WARN | worker | 192.0.2.10 | 42 | {trace_id} | "
            f"[ZMQ_RPC_FRAMEWORK_SLOW] e2e_us=8012 client_req_framework_us=100 "
            f"remote_processing_us=7600 client_rsp_framework_us=120 "
            f"server_req_queue_us=20 server_exec_us=7500 server_rsp_queue_us=80 "
            f"network_residual_us=292 method=WorkerOCService.Get"
        ),
        (
            f"2026-07-18T19:20:03.200000 | WARN | worker | 192.0.2.20 | 42 | {trace_id} | "
            f"[URMA_ELAPSED_TOTAL] cost 517.732ms, request id:77, "
            f"src address: 192.0.2.20, target address: 192.0.2.10, "
            f"dataSize:4194304, cpuid:12, status: OK"
        ),
        (
            f"2026-07-18T19:20:03.201000 | WARN | worker | 192.0.2.20 | 42 | {trace_id} | "
            f"[URMA_ELAPSED_POLL_JFC] cost 0.309ms, request id:77"
        ),
        (
            f"2026-07-18T19:20:03.202000 | WARN | worker | 192.0.2.20 | 42 | {trace_id} | "
            f"[URMA_ELAPSED_NOTIFY] cost 0.041ms, request id:77"
        ),
        (
            f"2026-07-18T19:20:03.203000 | WARN | worker | 192.0.2.20 | 42 | {trace_id} | "
            f"[URMA_ELAPSED_THREAD_SHED] cost 12.500ms, request id:77"
        ),
        (
            f"2026-07-18T19:20:03.230000 | ERROR | worker | 192.0.2.10 | 42 | {trace_id} | "
            f"RPC deadline exceeded while waiting WorkerOCService.Get"
        ),
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
        run_dir = run_pipeline(
            [str(bundle)],
            Path(tmp) / "runs",
            RunOptions(case_name="self-test", scenario="fixture", code_ref="self-test"),
        )
        if not ((run_dir / 'manifest.json').exists()):
            raise AssertionError("(run_dir / 'manifest.json').exists()")
        if not ((run_dir / 'events.jsonl').exists()):
            raise AssertionError("(run_dir / 'events.jsonl').exists()")
        if not ((run_dir / 'summary.json').exists()):
            raise AssertionError("(run_dir / 'summary.json').exists()")
        if not ((run_dir / 'triage.json').exists()):
            raise AssertionError("(run_dir / 'triage.json').exists()")
        if not ((run_dir / 'report.local.html').exists()):
            raise AssertionError("(run_dir / 'report.local.html').exists()")
        if not ((run_dir / 'report.site.html').exists()):
            raise AssertionError("(run_dir / 'report.site.html').exists()")
        inline_status = _verify_html_inline_script(run_dir / "report.local.html")
        if inline_status not in {"inline-script-present", "node-check-passed"}:
            raise AssertionError(f"unexpected inline script status: {inline_status}")
        if not ((run_dir / 'site_publish.md').exists()):
            raise AssertionError("(run_dir / 'site_publish.md').exists()")
        publish_url = publish_site_stage(run_dir, dry_run=True)
        if not (publish_url.startswith('https://yche.me/perf/')):
            raise AssertionError("publish_url.startswith('https://yche.me/perf/')")
        if not ((run_dir / 'raw' / 'inputs' / '01-fixture.tar.gz').exists()):
            raise AssertionError("(run_dir / 'raw' / 'inputs' / '01-fixture.tar.gz').exists()")
        extracted_log = (
            run_dir / "raw" / "extracted" / "01-fixture.tar.gz"
            / "kvchachjpworker-0-worker7" / "worker.log"
        )
        if not extracted_log.exists():
            raise AssertionError(f"missing extracted worker log: {extracted_log}")
        manifest = json.loads((run_dir / "manifest.json").read_text(encoding="utf-8"))
        if not (manifest['render_targets']['site']['publish_doc'] == 'site_publish.md'):
            raise AssertionError("manifest['render_targets']['site']['publish_doc'] == 'site_publish.md'")
        if not (manifest['render_targets']['site']['publish']['status'] == 'dry-run'):
            raise AssertionError("manifest['render_targets']['site']['publish']['status'] == 'dry-run'")
        run_report = json.loads((run_dir / "summary.json").read_text(encoding="utf-8"))
        run_trace = next(iter(run_report["traces"].values()))
        if not (run_trace['stage_breakdown']):
            raise AssertionError("run_trace['stage_breakdown']")
        if not (run_trace['evidence_coverage']['urma'] == 'present'):
            raise AssertionError("run_trace['evidence_coverage']['urma'] == 'present'")
        if not (run_report['dimensions']['coverage']['surfaces']['urma_elapsed']['status'] == 'present'):
            raise AssertionError("urma_elapsed coverage status is not present")
    if not (report['trace_count'] == 1):
        raise AssertionError("report['trace_count'] == 1")
    if not (report['dimensions']['latency_ms']['access']['p50'] == 518.923):
        raise AssertionError("report['dimensions']['latency_ms']['access']['p50'] == 518.923")
    if not (report['dimensions']['breakdown_ms']['ProcessGetObjectRequest']['sum'] == 517.0):
        raise AssertionError("report['dimensions']['breakdown_ms']['ProcessGetObjectRequest']['sum'] == 517.0")
    if not (report['dimensions']['urma_elapsed']['total']['p50'] == 517.732):
        raise AssertionError("report['dimensions']['urma_elapsed']['total']['p50'] == 517.732")
    if not (report['dimensions']['urma_elapsed']['poll_jfc']['p50'] == 0.309):
        raise AssertionError("report['dimensions']['urma_elapsed']['poll_jfc']['p50'] == 0.309")
    if not (report['dimensions']['rpc_slow']['WorkerOCService.Get']['server_exec_us']['p50'] == 7500):
        raise AssertionError("report['dimensions']['rpc_slow']['WorkerOCService.Get']['server_exec_us']['p50'] == 7500")
    if not (report['dimensions']['latency_summary_us']['client.rpc.get']['p50'] == 20298):
        raise AssertionError("report['dimensions']['latency_summary_us']['client.rpc.get']['p50'] == 20298")
    if not (report['dimensions']['errors']['RPC deadline exceeded'] == 1):
        raise AssertionError("report['dimensions']['errors']['RPC deadline exceeded'] == 1")
    if not (report['dimensions']['classifications']['client_deadline_with_urma_wait'] == 1):
        raise AssertionError("report['dimensions']['classifications']['client_deadline_with_urma_wait'] == 1")
    report["self_test"] = True
    return report


def main(argv=None):
    try:
        _LOG.handlers.clear()
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(logging.Formatter("%(message)s"))
        _LOG.addHandler(handler)
        _LOG.setLevel(logging.INFO)
        _LOG.propagate = False
        argv = list(argv or sys.argv[1:])
        stage_commands = {"parse", "aggregate", "triage", "render-local", "render-site", "publish-site"}
        if argv and argv[0] in ({"run", "verify"} | stage_commands):
            command = argv.pop(0)
            if command == "verify":
                parser = argparse.ArgumentParser(description="Run built-in trace triage verification.")
                parser.add_argument("--output-json", help="Write machine-readable summary JSON.")
                args = parser.parse_args(argv)
                report = run_self_test()
                _LOG.info("verify passed")
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
                parser.add_argument(
                    "--code-ref",
                    default="unknown",
                    help="Source ref used for CodeGraph/source validation.",
                )
                parser.add_argument("--force", action="store_true", help="Create a fresh run even when cache matches.")
                parser.add_argument("--allow-partial-inputs", action="store_true",
                                    help=(
                                        "Continue when some inputs cannot be read; "
                                        "failures are recorded in the report."
                                    ))
                args = parser.parse_args(argv)
                _LOG.info(
                    "%s",
                    parse_stage(
                        args.inputs,
                        args.out,
                        RunOptions(
                            case_name=args.case,
                            scenario=args.scenario,
                            code_ref=args.code_ref,
                            force=args.force,
                            allow_partial_inputs=args.allow_partial_inputs,
                        ),
                    ),
                )
                return 0
            if command in ("aggregate", "triage", "render-local", "render-site", "publish-site"):
                parser = argparse.ArgumentParser(description=f"Run trace triage {command} stage.")
                parser.add_argument("run_dir", help="Existing staged run directory.")
                if command == "publish-site":
                    parser.add_argument(
                        "--dry-run",
                        action="store_true",
                        help="Prepare publish metadata without copying.",
                    )
                    parser.add_argument("--max-site-html-mb", type=float, default=2.0,
                                        help="Refuse real yche.me publish when report.site.html exceeds this size.")
                args = parser.parse_args(argv)
                if command == "aggregate":
                    _LOG.info("%s", aggregate_stage(args.run_dir))
                elif command == "triage":
                    _LOG.info("%s", triage_stage(args.run_dir))
                elif command == "render-local":
                    _LOG.info("%s", render_local_stage(args.run_dir))
                elif command == "render-site":
                    _LOG.info("%s", render_site_stage(args.run_dir))
                else:
                    max_bytes = int(args.max_site_html_mb * 1024 * 1024)
                    url = publish_site_stage(args.run_dir, dry_run=args.dry_run, max_site_html_bytes=max_bytes)
                    _LOG.info("%s", f"{'DRY-RUN ' if args.dry_run else ''}{url}")
                return 0
            parser = argparse.ArgumentParser(description="Run staged DataSystem trace triage.")
            parser.add_argument("inputs", nargs="+", help="Log files, directories, or gzip-wrapped tar bundles.")
            parser.add_argument("--out", required=True, help="Output root for timestamped run directories.")
            parser.add_argument("--case", default="trace-case", help="Case name stored in manifest.")
            parser.add_argument("--scenario", default="", help="Scenario description stored in manifest.")
            parser.add_argument(
                "--code-ref",
                default="unknown",
                help="Source ref used for CodeGraph/source validation.",
            )
            parser.add_argument("--force", action="store_true", help="Create a fresh run even when cache matches.")
            parser.add_argument("--allow-partial-inputs", action="store_true",
                                help="Continue when some inputs cannot be read; failures are recorded in the report.")
            args = parser.parse_args(argv)
            run_dir = run_pipeline(
                args.inputs,
                args.out,
                RunOptions(
                    case_name=args.case,
                    scenario=args.scenario,
                    code_ref=args.code_ref,
                    force=args.force,
                    allow_partial_inputs=args.allow_partial_inputs,
                ),
            )
            _LOG.info("%s", run_dir)
            return 0

        parser = argparse.ArgumentParser(description=__doc__)
        parser.add_argument("inputs", nargs="*", help="Log files, directories, or gzip-wrapped tar bundles.")
        parser.add_argument("--code-ref", default="unknown", help="Source ref used for CodeGraph/source validation.")
        parser.add_argument("--allow-partial-inputs", action="store_true",
                            help="Continue when some inputs cannot be read; failures are recorded in the report.")
        parser.add_argument("--output-json", help="Write machine-readable summary JSON.")
        parser.add_argument("--output-md", help="Write Markdown summary.")
        parser.add_argument(
            "--self-test",
            action="store_true",
            help="Run the built-in fixture and validate parser behavior.",
        )
        args = parser.parse_args(argv)

        if args.self_test:
            report = run_self_test()
            _LOG.info("self-test passed")
        else:
            if not args.inputs:
                parser.error("inputs are required unless --self-test is used")
            report = analyze_inputs(args.inputs, code_ref=args.code_ref, allow_partial_inputs=args.allow_partial_inputs)

        text = json.dumps(report, ensure_ascii=False, indent=2)
        if args.output_json:
            Path(args.output_json).write_text(text + "\n", encoding="utf-8")
        else:
            _LOG.info("%s", text)
        if args.output_md:
            Path(args.output_md).write_text(render_markdown(report), encoding="utf-8")
        return 0

    except TraceTriageError as exc:
        _LOG.error("%s", exc)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
