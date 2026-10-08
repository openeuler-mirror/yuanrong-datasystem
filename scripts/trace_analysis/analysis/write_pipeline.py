"""Build the SET budget directly from validated Triage and shared Evidence."""

from ..diagnosis import worker_log_assessment
from ..evidence.normalized import read_observations
from ..evidence.urma import observed_urma_requests, worker_ip_mapping, _group_urma_logical_writes
from ..evidence.write import is_write_flow
from .write_base import _build_write_row
from .write import build_model


def build_write_model(summary, evidence, manifest, local_cache=None, read_path=None):
    traces = summary.get("traces", {})
    entries = evidence.get("traces", {})
    if set(entries) != set(traces):
        raise ValueError("write Evidence Trace identities differ from Triage")
    ip_to_worker = worker_ip_mapping(summary)
    collection = manifest.get("worker_log_coverage") or {}
    rows = []
    for trace_id, trace in traces.items():
        if not is_write_flow(trace):
            continue
        observed = read_observations(trace, entries[trace_id])
        if not observed.client_us:
            continue
        requests = observed_urma_requests(trace, ip_to_worker, local_cache, read_path)
        logical_writes = _group_urma_logical_writes(requests)
        slowest = max(requests, key=lambda item: item["total_ms"], default=None)
        base = {
            "trace_id": trace_id,
            "timestamp": trace.get("first_ts") or "",
            "last_ts": trace.get("last_ts") or "",
            "client_ms": round(observed.client_us / 1000, 6),
            "status": observed.status,
            "size_bytes": observed.size_bytes,
            "evidence": observed.texts,
            "write_evidence_facts": entries[trace_id]["write"],
            "urma_requests": requests,
            "urma_trace": {"slowest_request_id": slowest["request_id"]} if slowest else None,
            "urma_critical_path_ms": round(max(item["slowest_wr_ms"] for item in logical_writes), 6)
            if logical_writes else None,
        }
        row = _build_write_row(base, trace)
        row["worker_log_assessment"] = worker_log_assessment(trace, collection.get(trace_id))
        rows.append(row)
    rows.sort(key=lambda row: (-row["client_ms"], row["timestamp"], row["trace_id"]))
    return build_model({"write_traces": rows})
