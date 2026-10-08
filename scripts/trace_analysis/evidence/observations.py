"""Persist raw-message observations once; attribution consumes this versioned contract."""

from __future__ import annotations

import hashlib
import re

from .rpc import _evidence_timestamp, _rpc_fields, _transport_phase_maps


_DURATION_PATTERNS = {
    "local_read": (r"QueryAndGet done,.*?\blocalRead:\s*([\d.]+)ms", 1.0),
    "client_transfer": (r"\[TransportGet\].*?phasesUs=\{[^}]*\bdata_transfer:(\d+)", 1000.0),
    "provider_pull": (r"Processing pull object.*?\bcost:\s*([\d.]+)ms", 1.0),
    "provider_finish": (r"\[GetObjectRemote\]\s+finish.*?\bcost:\s*([\d.]+)ms", 1.0),
}
_ATTEMPT_PATTERNS = {
    "inline_attempts": re.compile(
        r"QueryAndGet done,.*?inlineHits?:\s*(\d+).*?transport:\s*UB\b.*?total:\s*([\d.]+)ms", re.I),
    "query_access_attempts": re.compile(r"\|\s*0\s*\|\s*DS_POSIX_QUERY_AND_GET\s*\|\s*(\d+)\s*\|"),
    "query_attempts": re.compile(r"QueryAndGet done,.*?localRead:\s*([\d.]+)ms.*?total:\s*([\d.]+)ms", re.I),
    "timeout_events": re.compile(r"\[URMA(?:[_ -]WAIT[_ -]TIMEOUT)\].*?elapsedMs\s*[=:]\s*([\d.]+)", re.I),
}


def _max_evidence_ms(evidence: list[str], pattern: str, *, divisor: float = 1.0) -> float | None:
    values = []
    for text in evidence:
        match = re.search(pattern, text, re.I)
        if match:
            values.append(float(match.group(1)) / divisor)
    return max(values, default=None)


def _record_attempts(records: list[dict]) -> dict:
    result = {key: [] for key in _ATTEMPT_PATTERNS}
    result["record_sources"] = []
    for index, item in enumerate(records):
        text = item.get("text", "")
        ref = {"collection": "record_sources", "index": len(result["record_sources"])}
        matched = False
        base = {"worker": item.get("worker"), "timestamp": _evidence_timestamp(text) or None,
                "source_ref": ref}
        for kind, pattern in _ATTEMPT_PATTERNS.items():
            match = pattern.search(text)
            if not match:
                continue
            if kind == "inline_attempts":
                if int(match.group(1)) <= 0:
                    continue
                value = {"total_ms": float(match.group(2)), "inline_hits": int(match.group(1))}
            elif kind == "query_access_attempts":
                if not re.search(r"transportType:\s*UB\b", text):
                    continue
                value = {"total_ms": int(match.group(1)) / 1000.0}
            elif kind == "query_attempts":
                value = {"total_ms": float(match.group(2)), "local_read_ms": float(match.group(1))}
            else:
                request = re.search(r"urma_request_id[:_]?(\d+)", text, re.I)
                value = {"elapsed_ms": float(match.group(1)), "request_id": request.group(1) if request else None}
            result[kind].append({**base, **value})
            matched = True
        if matched:
            source = {key: item.get(key) for key in ("source", "member", "line", "worker")}
            source.update(evidence_record_index=index, text_sha256=hashlib.sha256(text.encode("utf-8")).hexdigest())
            result["record_sources"].append(source)
    return result


def build_evidence_facts(evidence: list[str], evidence_records: list[dict]) -> dict:
    result = {"schema_version": 1, "rpc_entries": [], "transport_phase_maps": [],
              "transport_source_refs": [], "local_processing": None, "remote_lock_ms": None,
              "worker_query_done_observed": False, "legacy_pull_src_sentinel": False,
              "durations_ms": dict.fromkeys(_DURATION_PATTERNS), "duration_source_refs": {},
              "observation_source_refs": {}}
    for index, text in enumerate(evidence):
        ref = {"collection": "evidence", "index": index}
        method, fields = _rpc_fields(text)
        if method:
            result["rpc_entries"].append({"method": method, "fields": fields, "source_ref": ref})
        phases = _transport_phase_maps([text])
        result["transport_phase_maps"].extend(phases)
        result["transport_source_refs"].extend([ref] * len(phases))
        local = re.search(r"Local processing done.*?remoteObjects:\s*(\d+).*?costUs:\s*(\d+)", text, re.I)
        lock = re.search(r"RemoteLockEntry:\s*([\d.]+)\s*ms", text, re.I)
        if result["local_processing"] is None and local:
            result["local_processing"] = {"remote_objects": int(local.group(1)), "cost_us": int(local.group(2))}
            result["observation_source_refs"]["local_processing"] = ref
        if result["remote_lock_ms"] is None and lock:
            result["remote_lock_ms"] = float(lock.group(1))
            result["observation_source_refs"]["remote_lock_ms"] = ref
        for key, pattern in (("worker_query_done_observed", r"QueryAndGet done,"),
                             ("legacy_pull_src_sentinel", r"Processing pull object.*\bsrc=:-1")):
            if not result.get(key) and re.search(pattern, text, re.I):
                result[key] = True
                result["observation_source_refs"][key] = ref
        for key, (pattern, divisor) in _DURATION_PATTERNS.items():
            match = re.search(pattern, text, re.I)
            if match:
                value = float(match.group(1)) / divisor
                previous = result["durations_ms"][key]
                if previous is None or value > previous:
                    result["durations_ms"][key] = value
                    result["duration_source_refs"][key] = ref
    result.update(_record_attempts(evidence_records))
    return result


def facts_for(row: dict) -> dict:
    # Legacy in-memory callers are adapted once; raw evidence is immutable after this boundary.
    if "evidence_facts" not in row:
        row["evidence_facts"] = build_evidence_facts(row.get("evidence", []), row.get("evidence_records", []))
    facts = row["evidence_facts"]
    if not isinstance(facts, dict) or type(facts.get("schema_version")) is not int or facts["schema_version"] != 1:
        raise ValueError("unsupported evidence facts schema")
    return facts
