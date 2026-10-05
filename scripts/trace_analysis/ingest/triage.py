"""Parse trace log fields independently of aggregation and report rendering."""

import hashlib
import json
import re
from dataclasses import dataclass
from datetime import datetime
from typing import Any


@dataclass(frozen=True)
class UbLineContext:
    source: str
    member: str
    line_no: int
    line: str
    ts: Any
    worker: str


TRACE_ID_MAX_SIZE = 49


TRACE_ID_CHARS = r"A-Za-z0-9~.\-/_!@#%^&*()+=:;"


TRACE_ID_FIELD_RE = re.compile(rf"^[{TRACE_ID_CHARS}]{{1,{TRACE_ID_MAX_SIZE}}}$")


TRACE_ID_EXPLICIT_RE = re.compile(
    rf"\btrace[_ ]?id\s*[:=]\s*([{TRACE_ID_CHARS}]{{1,{TRACE_ID_MAX_SIZE}}})",
    re.I,
)


TRACE_ID_PREFIXED_SHORT_UUID_RE = re.compile(
    rf"(?<![{TRACE_ID_CHARS}])([{TRACE_ID_CHARS}]{{1,36}};[0-9a-f]{{12}})(?![{TRACE_ID_CHARS}])",
    re.I,
)


TRACE_ID_UUID_RE = re.compile(
    r"\b[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\b",
    re.I,
)


TS_RE = re.compile(r"(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?)")


POD_NAME_RE = re.compile(
    r"(kv[A-Za-z0-9_.-]*(?:client|worker)-\d+-(?:worker\d+|master)(?:_\d+)?)",
    re.I,
)


WORKER_POD_NAME_RE = re.compile(r"^kv[A-Za-z0-9_.-]*worker-\d+-(?:worker\d+|master)(?:_\d+)?$", re.I)


IP_RE = re.compile(r"\b(?:\d{1,3}\.){3}\d{1,3}(?::\d+)?\b")


COLLECTED_PROCESS_IP_RE = re.compile(
    r"/(?:collected/client_|collected_worker_logs/worker_)((?:\d{1,3}\.){3}\d{1,3})(?:/|$)"
)


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


URMA_TOTAL_RE = re.compile(r"\[URMA_ELAPSED_TOTAL\].*?(?:total\s+)?cost\s*:?\s*([\d.]+)\s*(ms|us)\b", re.I)


URMA_POLL_RE = re.compile(r"\[URMA_ELAPSED_POLL_JFC\].*?cost\s*:?\s*([\d.]+)\s*(us|ms)", re.I)


URMA_NOTIFY_RE = re.compile(r"\[URMA_ELAPSED_NOTIFY\].*?cost\s*:?\s*([\d.]+)\s*(us|ms)", re.I)


URMA_THREAD_RE = re.compile(r"\[URMA_ELAPSED_THREAD_SHED\].*?cost\s*:?\s*([\d.]+)\s*(us|ms)", re.I)


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


FIRST_WRITE_WAKE_SCHED_RE = re.compile(r"\bfirstUrmaWriteWakeSchedLatencyUs[:=]\s*([\d.]+)", re.I)


SECOND_WRITE_WAKE_SCHED_RE = re.compile(r"\bsecondUrmaWriteWakeSchedLatencyUs[:=]\s*([\d.]+)", re.I)


WRITE_WAKE_SCHED_RE = re.compile(r"\burmaWriteWakeSchedLatencyUs[:=]\s*([\d.]+)", re.I)


LEGACY_WAKE_SCHED_RE = re.compile(
    r"\b(?:wakeSchedLatencyUs|wake_sched_latency_us|wakeLatencyUs)[:=]\s*([\d.]+)", re.I
)


SRC_CHIP_INFLIGHT_RE = re.compile(r"\bsrcChipInflight:\s*(\{[^}]*\})", re.I)


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


class UrmaFieldParser:
    """Extract canonical URMA fields from evolving log field names."""

    STRING_FIELDS = {
        "request_id": [
            REQUEST_ID_RE,
            re.compile(r"\breq(?:uest)?Id[:=]\s*([A-Za-z0-9_-]+)", re.I),
        ],
        "src_addr": [
            SRC_ADDR_RE,
            re.compile(r"\b(?:source|src) (?:addr|address):\s*([^\s,]+)", re.I),
        ],
        "target_addr": [
            DST_ADDR_RE,
            re.compile(r"\b(?:tgt|target|dst|destination) (?:addr|address):\s*([^\s,]+)", re.I),
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
        "write_chunk_index": [re.compile(r"\bwriteChunk(?:Index|Idx)[:=]\s*(\d+)", re.I)],
        "write_chunk_count": [re.compile(r"\bwriteChunk(?:Count|Cnt)[:=]\s*(\d+)", re.I)],
    }
    FLOAT_FIELDS = {
        "wait_os_sched_ms": [
            WAIT_OS_RE,
            re.compile(r"\b(?:condition wait|osSchedWaitMs|waitForMs|wait_for_ms)[:=]\s*([\d.]+)\s*ms?", re.I),
        ],
        "completion_observation_latency_us": [
            re.compile(r"\bcompletionObservationLatencyUs[:=]\s*([\d.]+)", re.I)
        ],
        "event_processing_and_wait_latency_us": [
            re.compile(r"\burmaEventProcessingAndWaitLatencyUs[:=]\s*([\d.]+)", re.I)
        ],
    }
    BOOL_FIELDS = {
        "waited_for_notification": [re.compile(r"\bwaited_for_notification[:=]\s*(true|false|0|1)", re.I)],
        "pre_completed_before_wait": [
            re.compile(r"\bpre_completed_before_wait[:=]\s*(true|false|0|1)", re.I)
        ],
        "woken_by_previous_event": [
            re.compile(r"\bwoken_by_previous_event[:=]\s*(true|false|0|1)", re.I)
        ],
        "event_processing_and_wait_latency_valid": [
            re.compile(r"\bevent_processing_and_wait_latency_valid[:=]\s*(true|false|0|1)", re.I)
        ],
    }
    WAKE_SCHED_FIELDS = (
        ("first_write", FIRST_WRITE_WAKE_SCHED_RE),
        ("second_write", SECOND_WRITE_WAKE_SCHED_RE),
        ("write", WRITE_WAKE_SCHED_RE),
        ("legacy", LEGACY_WAKE_SCHED_RE),
    )

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
        for field, regexes in self.BOOL_FIELDS.items():
            value = self._bool(regexes, line)
            if value is not None:
                event[field] = value
        for kind, regex in self.WAKE_SCHED_FIELDS:
            value = self._float([regex], line)
            if value is not None:
                event["wake_sched_latency_us"] = value
                event["wake_sched_kind"] = kind
                break
        if event.get("wake_sched_latency_us") is not None:
            event["wake_sched_inherited"] = bool(event.get("woken_by_previous_event", False))
            event["wake_sched_is_actual"] = (
                not event["wake_sched_inherited"] and event.get("waited_for_notification") is not False
            )
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

    @staticmethod
    def _bool(regexes, line):
        raw = UrmaFieldParser._first(regexes, line)
        if raw is None:
            return None
        return raw.lower() in ("1", "true")


class TraceParser:
    """Parse one log line into trace-scoped facts without aggregating them."""

    def __init__(self, rules=None, *, event_extractor=None, host_ip_parser=None, to_ms=None):
        self.rules = rules or ParserRules()
        self.event_extractor = event_extractor or extract_ub_events
        self.host_ip_parser = host_ip_parser or _line_host_ip
        self.to_ms = to_ms or _ms

    @staticmethod
    def worker_from(source, member, line):
        for text in (member, source, line):
            m = POD_NAME_RE.search(text)
            if m:
                return m.group(1)
        parts = [p.strip() for p in line.split(" | ")]
        if len(parts) > 3 and parts[3]:
            return parts[3]
        collected_process = COLLECTED_PROCESS_IP_RE.search(line) or COLLECTED_PROCESS_IP_RE.search(source)
        if collected_process:
            return collected_process.group(1)
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

    @staticmethod
    def trace_id(line):
        parts = line.split(" | ")
        trace_field = 5 if parts and TS_RE.search(parts[0]) else 4
        if len(parts) > trace_field:
            candidate = parts[trace_field].strip()
            if candidate:
                return candidate if TRACE_ID_FIELD_RE.fullmatch(candidate) else None
            explicit = TRACE_ID_EXPLICIT_RE.search(line)
            return explicit.group(1) if explicit else None

        explicit = TRACE_ID_EXPLICIT_RE.search(line)
        if explicit:
            candidate = explicit.group(1)
            if re.fullmatch(r".+;[0-9a-fA-F]{12}\.", candidate):
                return candidate[:-1]
            return candidate
        prefixed = TRACE_ID_PREFIXED_SHORT_UUID_RE.search(line)
        if prefixed:
            return prefixed.group(1)
        uuid = TRACE_ID_UUID_RE.search(line)
        return uuid.group(0) if uuid else None

    def parse_line(self, source, member, line_no, line):
        trace_id = self.trace_id(line)
        if not trace_id:
            return None
        worker = self.worker_from(source, member, line)
        ts = self.timestamp(line)
        ub_ctx = UbLineContext(source, member, line_no, line, ts, worker)
        parsed = {
            "trace_id": trace_id,
            "worker": worker,
            "timestamp": ts,
            "evidence": {
                "source": source,
                "member": member,
                "line": line_no,
                "worker": worker,
                "host_ip": self.host_ip_parser(line),
                "text": line,
            },
            "ub_events": self.event_extractor(ub_ctx),
            "errors": [],
            "custom_metrics_ms": {},
        }
        for rule in self.rules.custom_metric_rules:
            cm = rule["regex"].search(line)
            if cm:
                unit = cm.group(rule["unit_group"]) if rule["unit_group"] else "ms"
                parsed["custom_metrics_ms"][rule["name"]] = self.to_ms(cm.group(rule["value_group"]), unit)
        if RPC_MAX_CONCURRENCY_RE.search(line):
            parsed["errors"].append(RPC_MAX_CONCURRENCY_ERROR)
        for pattern in self.rules.error_patterns:
            if pattern in line:
                parsed["errors"].append(pattern)
        parsed["errors"] = list(dict.fromkeys(parsed["errors"]))
        return parsed


def _line_host_ip(line):
    parts = [p.strip() for p in line.split(" | ")]
    if len(parts) > 3 and re.fullmatch(r"\d{1,3}(?:\.\d{1,3}){3}(?::\d+)?", parts[3]):
        return parts[3]
    return None


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


def _ub_base_event(event_type, ctx: UbLineContext, *, field_parser=None):
    field_parser = field_parser or URMA_FIELDS
    event = {
        "event_type": event_type,
        "timestamp": ctx.ts.isoformat() if ctx.ts else None,
        "worker": ctx.worker,
        "source": ctx.source,
        "member": ctx.member,
        "line": ctx.line_no,
        "raw": ctx.line,
    }
    return field_parser.enrich_base_event(event, ctx.line)


def extract_ub_events(ctx: UbLineContext, *, field_parser=None):
    field_parser = field_parser or URMA_FIELDS
    events = []
    line = ctx.line.replace("**", "")
    transfer = TRANSFER_PATH_RE.search(line)
    if transfer:
        event = _ub_base_event("transfer_path", ctx, field_parser=field_parser)
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
        event = _ub_base_event("remote_get_start", ctx, field_parser=field_parser)
        event.update({
            "request_id": request_id,
            "object_key": object_key,
            "offset": int(offset),
            "read_size": int(read_size),
        })
        events.append(event)

    total = URMA_TOTAL_RE.search(line)
    if total:
        event = _ub_base_event("total", ctx, field_parser=field_parser)
        event["cost_ms"] = _ms(total.group(1), total.group(2))
        field_parser.enrich_total_event(event, line)
        events.append(event)

    loop_gap = URMA_THREAD_LOOP_GAP_RE.search(line)
    if loop_gap:
        event = _ub_base_event("thread_sched", ctx, field_parser=field_parser)
        event["thread_sched_kind"] = "poll_loop_gap"
        event["last_poll_end_to_start_us"] = float(loop_gap.group(1))
        event["last_poll_start_to_start_us"] = float(loop_gap.group(2))
        event["cost_ms"] = event["last_poll_end_to_start_us"] / 1000.0
        events.append(event)

    for event_type, regex in (("poll_jfc", URMA_POLL_RE), ("notify", URMA_NOTIFY_RE), ("thread_sched", URMA_THREAD_RE)):
        match = regex.search(line)
        if match:
            event = _ub_base_event(event_type, ctx, field_parser=field_parser)
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


URMA_FIELDS = UrmaFieldParser()
