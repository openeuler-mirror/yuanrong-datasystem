"""Build persisted Trace events and diagnosis projections from normalized facts."""
from collections import Counter, defaultdict
import hashlib
import json
from pathlib import PurePosixPath
import re

from ..ingest.triage import TraceParser, TS_RE, _line_host_ip


def _digest(value):
    raw = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _scope(report):
    run = report.get("run_scope")
    if run:
        return str(run), None, "run"
    inputs = report.get("inputs")
    input_scope = report.get("input_scope") or (_digest(sorted(map(str, inputs))) if inputs else None)
    return None, input_scope, "input" if input_scope else "unscoped"


def _process_role(name, allow_named=False):
    patterns = [r"(worker|client)(?:\d+[\w.-]*|[_-]\d+(?:\.\d+){3})?",
                r"kv[\w.-]*?(worker|client)-\d+-(?:worker\d+|master)(?:_\d+)?"]
    if allow_named:
        patterns.append(r"(worker|client)-[A-Za-z][\w-]*")
    for pattern in patterns:
        match = re.fullmatch(pattern, name, re.I)
        if match:
            return match[1].lower()
    return None


def _source_role(path):
    parts = PurePosixPath(str(path).replace("\\", "/")).parts
    directories = parts[:-1]
    for part in reversed(directories):
        match = re.fullmatch(r"(?:collected_)?(worker|client)_logs", part, re.I)
        if match:
            return match[1].lower()
    if directories:
        direct = _process_role(directories[-1], allow_named=True)
        if direct:
            return direct
        if directories[-1].lower() in {"log", "logs"} and len(directories) > 1:
            return _process_role(directories[-2])
    if parts:
        match = re.fullmatch(r"(worker|client)\.(?:log|INFO)(?:[._][\w.-]+)?", parts[-1], re.I)
        if match:
            return match[1].lower()
    return None


def _identity(evidence):
    text = evidence.get("text") or evidence.get("raw") or ""
    match = TS_RE.search(text)
    log = text[match.start():] if match else text
    parts = [part.strip() for part in log.split(" | ")]
    source = evidence.get("source") or ""
    member = evidence.get("member") or ""
    original = re.match(r"^(.*?):(\d+):(?=\d{4}-\d\d-\d\d[T ])", text)
    origin = {key: evidence.get(key) for key in ("source", "member", "line")}
    for key in ("original_member", "original_line"):
        if evidence.get(key) is not None:
            origin[key] = evidence[key]
    if original:
        origin["original_member"] = original[1]
        origin["original_line"] = int(original[2])
    provenance = origin.get("original_member") or member or source
    basename = PurePosixPath(provenance.replace("\\", "/")).name
    log_name = re.search(r"\.(?:log|INFO|WARNING|ERROR)(?:[._]|$)", basename, re.I)
    is_log_source = bool(origin.get("original_member") or log_name)
    role = _source_role(provenance) if is_log_source else None
    worker = evidence.get("worker")
    worker = None if worker in {"unknown", "Unknown", ""} else worker
    supplied_role = _process_role(worker, allow_named=True) if worker else None
    conflicting_role = role and supplied_role and role != supplied_role
    if not worker or conflicting_role:
        worker = TraceParser.worker_from("", provenance, log)
    worker = None if worker in {"unknown", "Unknown", ""} else worker
    host_ip = evidence.get("host_ip") or _line_host_ip(log)
    process = re.fullmatch(r"(\d+):(\d+)", parts[4]) if len(parts) > 4 else None
    pid, tid = process.groups() if process else (None, None)
    file = parts[2].rsplit(":", 1)[0] if len(parts) > 2 and re.search(r"\.(cpp|cc|h):\d+$", parts[2]) else None
    component = None
    if file:
        for pattern, name in ((r"urma|ub_", "URMA"), (r"brpc|rpc|zmq", "RPC"),
                              (r"client|object_posix", "SDK"), (r"master|metadata", "Metadata"),
                              (r"worker", "Worker")):
            if re.search(pattern, file, re.I):
                component = name
                break
        component = component or file
    stamp = TraceParser.timestamp(log)
    directory = str(PurePosixPath(origin.get("original_member") or member).parent)
    # PID reuse cannot be excluded without a process-start identity; keep that limitation explicit.
    process_key = _digest([host_ip, pid, worker, directory]) if host_ip and pid else None
    observed_elapsed = re.search(r"\bprocess_elapsed_ms\s*[:=]\s*(\d+(?:\.\d+)?)\b", log)
    return {
        "worker": worker, "worker_id": worker if role == "worker" else None, "role": role,
        "host_ip": host_ip, "process_id": pid, "thread_id": tid, "process_key": process_key,
        "component": component, "source_file": file, "wall_time": stamp.isoformat() if stamp else None,
        "process_elapsed_ms": float(observed_elapsed[1]) if observed_elapsed else None,
        "source_refs": [origin], "_timestamp": stamp, "_log": log,
    }


def _event(evidence, trace_id, scope, kind):
    run, input_scope, scope_kind = scope
    row = dict(evidence)
    text = evidence.get("text") or evidence.get("raw") or ""
    row.update(_identity(evidence))
    row.update(schema_version=2, trace_id=trace_id, run_scope=run, input_scope=input_scope,
               scope_kind=scope_kind, trace_key={"run_scope": run, "trace_id": trace_id},
               event_type=kind, raw=text, text=text, process_instance_verified=False)
    row.setdefault("ts", text.split(" | ", 1)[0] if kind == "raw" else evidence.get("timestamp"))
    row.setdefault("type", kind)
    for key in ("source", "member", "line"):
        row.setdefault(key, None)
    row["missing_reasons"] = {}
    for key in ("role", "worker_id", "host_ip", "process_id", "thread_id", "component", "wall_time"):
        if row[key] is None:
            row["missing_reasons"][key] = "not_observed_in_line"
    if row["role"] == "client":
        row["missing_reasons"]["worker_id"] = "not_applicable_client"
    if row["process_elapsed_ms"] is None:
        row["missing_reasons"]["process_elapsed_ms"] = "explicit_process_elapsed_not_observed"
    if run is None:
        row["missing_reasons"]["run_scope"] = "run_id_not_provided"
    row["time_domain"] = None
    row["time_basis"] = None
    row["process_relative_ms"] = None
    row["observed_gap_ms"] = None
    if row["process_key"] and row["wall_time"]:
        row["time_domain"] = {"scope": run or input_scope, "process_key": row["process_key"], "clock": "wall"}
        row["time_basis"] = "process_local_wall_clock"
    else:
        row["missing_reasons"]["time_domain"] = "process_identity_or_wall_time_not_observed"
        row["missing_reasons"]["process_relative_ms"] = "process_identity_or_wall_time_not_observed"
        row["missing_reasons"]["observed_gap_ms"] = "process_identity_or_wall_time_not_observed"
    identity = [run, input_scope, scope_kind, trace_id, kind, row["process_key"], row["_log"]]
    if not row["process_key"] or not row["wall_time"]:
        identity.append(row["source_refs"][0])
    row["event_id"] = _digest(identity)
    return row


def _wall_regression(rows):
    sources = defaultdict(list)
    for row in rows:
        for ref in row["source_refs"]:
            line = ref.get("original_line", ref.get("line"))
            if type(line) is int:
                source = (ref.get("source"), ref.get("original_member", ref.get("member")))
                sources[source].append((line, row["_timestamp"]))
    for records in sources.values():
        records.sort(key=lambda record: record[0])
        if any(current[1] < previous[1] for previous, current in zip(records, records[1:])):
            return True
    return False


def _observed_time(events):
    groups = defaultdict(list)
    for row in events:
        if row["time_domain"]:
            groups[(row["trace_id"], row["process_key"])].append(row)
    for rows in groups.values():
        if _wall_regression(rows):
            for row in rows:
                row["missing_reasons"]["process_relative_ms"] = "wall_clock_regression"
                row["missing_reasons"]["observed_gap_ms"] = "wall_clock_regression"
            continue
        unique = {row["wall_time"]: row["_timestamp"] for row in rows}
        times = sorted(unique.values())
        first = times[0]
        gaps = {stamp: (stamp - times[i - 1]).total_seconds() * 1000 if i else None
                for i, stamp in enumerate(times)}
        for row in rows:
            row["process_relative_ms"] = (row["_timestamp"] - first).total_seconds() * 1000
            row["observed_gap_ms"] = gaps[row["_timestamp"]]
            if row["observed_gap_ms"] is None:
                row["missing_reasons"]["observed_gap_ms"] = "first_observed_wall_time"
    for row in events:
        row.pop("_timestamp")
        row.pop("_log")


def build_events(report):
    events = {}
    scope = _scope(report)
    for trace_id, trace in report["traces"].items():
        sources = [(evidence, "raw") for evidence in trace.get("evidence", [])]
        sources.extend((event, "ub_" + event["event_type"]) for event in trace.get("ub_events", []))
        for evidence, kind in sources:
            row = _event(evidence, trace_id, scope, kind)
            previous = events.get(row["event_id"])
            if previous is None:
                events[row["event_id"]] = row
            else:
                for ref in row["source_refs"]:
                    if ref not in previous["source_refs"]:
                        previous["source_refs"].append(ref)
    result = list(events.values())
    _observed_time(result)
    return result


def build_triage(report):
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
