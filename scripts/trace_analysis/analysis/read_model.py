"""Validate one Triage run and assemble the read bottleneck model."""

from __future__ import annotations

import collections
import json
import math
from pathlib import Path

from .aggregation import aggregate
from .read_rows import _topology_contract, build_trace_rows
from .stats import _percentile
from .write_base import _build_write_row
from ..diagnosis import WRITE_STAGES, worker_log_assessment
from ..evidence.normalized import load_evidence, summary_digest
from ..evidence.write import is_write_flow


WRITE_STAGE_NAMES = WRITE_STAGES


class InputContractError(ValueError):
    """Raised when a triage run directory does not satisfy the input contract."""


def _share_safe_input(value: object) -> str:
    """Keep report provenance useful without embedding a local directory."""

    normalized = str(value).replace("\\", "/").rstrip("/")
    return normalized.rsplit("/", 1)[-1] or "未命名输入"


def _is_write_flow(trace: dict) -> bool:
    return is_write_flow(trace)


def _aggregate_write(rows: list[dict]) -> dict:
    latencies = [row["client_ms"] for row in rows]
    return {
        "trace_count": len(rows),
        "failed_count": sum(row["failed"] for row in rows),
        "latency": {
            "p50": round(_percentile(latencies, 0.50), 3),
            "p90": round(_percentile(latencies, 0.90), 3),
            "p99": round(_percentile(latencies, 0.99), 3),
            "max": round(max(latencies, default=0.0), 3),
        },
        "stage_totals": {
            name: round(sum(row["write_breakdown_ms"][name] for row in rows), 3)
            for name in WRITE_STAGE_NAMES
        },
        "problem_counts": dict(collections.Counter(row["write_primary_stage"] for row in rows)),
    }


def prepare_read_view(rows: list[dict], aggregate_data: dict, metadata: dict) -> tuple[dict, dict]:
    """Prepare read scope models outside the HTML renderer."""

    scope_aggregates = {"0": aggregate_data}
    ranked = sorted(rows, key=lambda row: (-row["client_ms"], row["timestamp"], row["trace_id"]))
    for count in (100, 1000):
        scoped = aggregate(ranked[:count])
        for key in ("deadline_ms", "deadline_is_reference", "topology"):
            if key in aggregate_data:
                scoped[key] = aggregate_data[key]
        scope_aggregates[str(count)] = scoped
    topology = aggregate_data.get("topology") or _topology_contract(metadata.get("local_cache"))
    return scope_aggregates, topology


def _read_json(path: Path) -> dict:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise InputContractError(f"cannot read valid JSON from {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise InputContractError(f"expected JSON object in {path}")
    return value


def build_analysis(
    run_dir: Path,
    top_n: int = 100,
    deadline_ms: float | None = None,
    local_cache: bool | None = None,
    read_path: str | None = None,
    source_ref: str | None = None,
    evidence_json: Path | None = None,
) -> dict:
    """Build a deterministic report model from one completed triage run directory."""

    run_dir = Path(run_dir)
    required = [run_dir / name for name in ("manifest.json", "summary.json", "triage.json")]
    missing = [path.name for path in required if not path.is_file()]
    if missing:
        required_names = ", ".join(path.name for path in required)
        raise InputContractError(f"run directory requires {required_names}; missing: {', '.join(missing)}")
    if top_n < 0:
        raise ValueError("top_n must be non-negative; zero selects all")
    if local_cache is not None and not isinstance(local_cache, bool):
        raise ValueError("local_cache must be true, false, or omitted")
    if read_path not in {None, "legacy-worker-pull"}:
        raise ValueError("read_path must be legacy-worker-pull or omitted")

    manifest = _read_json(run_dir / "manifest.json")
    summary = _read_json(run_dir / "summary.json")
    _read_json(run_dir / "triage.json")
    evidence_data = load_evidence(Path(evidence_json)) if evidence_json is not None else None
    if evidence_data is not None:
        evidence_traces = evidence_data.get("traces")
        if evidence_data.get("summary_sha256") != summary_digest(run_dir / "summary.json"):
            raise InputContractError("evidence.json does not match triage summary")
        if not isinstance(evidence_traces, dict):
            raise InputContractError("evidence.json does not match triage summary")
        if set(evidence_traces) != set(summary.get("traces", {})):
            raise InputContractError("evidence.json does not match triage summary")
    topology = _topology_contract(local_cache, read_path)
    all_rows = build_trace_rows(summary, local_cache=local_cache, read_path=read_path,
                                evidence_data=evidence_data)
    trace_inputs = summary.get("traces", {})
    collection = manifest.get("worker_log_coverage", {})
    if not isinstance(collection, dict):
        raise InputContractError("worker_log_coverage must map Trace IDs to collection records")
    for trace_id, records in collection.items():
        if trace_id not in trace_inputs or not isinstance(records, list):
            raise InputContractError(f"invalid worker_log_coverage Trace: {trace_id}")
        for record in records:
            message = "worker_log_coverage requires worker, collection_status and evidence_refs"
            if not isinstance(record, dict):
                raise InputContractError(message)
            if not record.get("worker") or record.get("collection_status") not in {
                "not_collected", "collected", "out_of_window", "lifecycle_mismatch"
            }:
                raise InputContractError(message)
            evidence_refs = record.get("evidence_refs", [])
            if not isinstance(evidence_refs, list) or not evidence_refs:
                raise InputContractError(message)
            if any(not isinstance(ref, str) or not ref.strip() for ref in evidence_refs):
                raise InputContractError(message)
    for row in all_rows:
        row["worker_log_assessment"] = worker_log_assessment(
            trace_inputs[row["trace_id"]], collection.get(row["trace_id"]))
    get_rows = [row for row in all_rows if row["get_observed"]]
    client_rows = [row for row in get_rows if row["client_observed"]]
    ranked = sorted(client_rows, key=lambda row: (-row["client_ms"], row["timestamp"], row["trace_id"]))
    rows = ranked[:top_n] if top_n else ranked
    write_candidates = [
        _build_write_row(row, trace_inputs[row["trace_id"]])
        for row in all_rows
        if row["client_observed"] and _is_write_flow(trace_inputs[row["trace_id"]])
    ]
    for row in write_candidates:
        row["worker_log_assessment"] = worker_log_assessment(
            trace_inputs[row["trace_id"]], collection.get(row["trace_id"]))
    write_rows = sorted(
        write_candidates,
        key=lambda row: (-row["client_ms"], row["timestamp"], row["trace_id"]),
    )[:top_n or None]
    write_aggregate = _aggregate_write(write_rows)
    aggregate_data = aggregate(rows)
    for row in rows:
        row.pop("evidence_records", None)
    coverage = {
        "manifest": "present",
        "summary": "present",
        "triage": "present",
        "parsed_traces": "present" if (run_dir / "parsed_traces.json").is_file() else "missing",
        "events": "present" if (run_dir / "events.jsonl").is_file() else "missing",
    }
    limitations = [
        f"{name} is missing; corresponding drilldown uses summary evidence only"
        for name, state in coverage.items()
        if state == "missing"
    ]
    if local_cache is None:
        limitations.append(
            "local_cache mode is unknown; BatchGet and URMA topology remains unconfirmed"
        )
    if read_path == "legacy-worker-pull":
        limitations.append(
            "historical runtime Worker-pull topology is explicitly supplied from trace "
            "evidence; current source may differ"
        )
    if not source_ref:
        limitations.append(
            "current source ref is not supplied; code-path claims require separate main/master verification"
        )
    excluded_non_get = len(all_rows) - len(get_rows)
    excluded_worker_only = len(get_rows) - len(client_rows)
    if excluded_non_get:
        limitations.append(
            f"{excluded_non_get} non-GET traces are excluded from the read model; "
            f"{len(write_rows)} Client write traces are analyzed in the separate write model"
        )
    if excluded_worker_only:
        limitations.append(
            f"{excluded_worker_only} traces lack a Client latency window and are excluded from Client TopN"
        )
    deadline_is_reference = False
    if deadline_ms is None:
        candidate = manifest.get("deadline_ms") or manifest.get("options", {}).get("deadline_ms")
        if candidate is None:
            deadline_ms = 20.0
            deadline_is_reference = True
            limitations.append("deadline is not recorded; 20ms is a visualization reference, not a configured deadline")
        else:
            deadline_ms = float(candidate)
    if not math.isfinite(deadline_ms) or deadline_ms <= 0:
        raise ValueError("deadline_ms must be a positive finite number")
    aggregate_data["deadline_ms"] = deadline_ms
    aggregate_data["deadline_is_reference"] = deadline_is_reference
    aggregate_data["topology"] = topology
    raw_input_archives = []
    for item in manifest.get("inputs", []):
        preserved_name = str(item.get("preserved_name") or "")
        name = Path(preserved_name).name
        if not name or name != preserved_name:
            continue
        if not (run_dir / "raw" / "inputs" / name).is_file():
            continue
        raw_input_archives.append(
            {
                "name": name,
                "size_bytes": int(item.get("size", 0) or 0),
                "sha256": str(item.get("sha256") or ""),
                "download_path": f"raw-inputs/{name}",
            }
        )
    metadata = {
        "case": manifest.get("case_name") or manifest.get("case") or manifest.get("options", {}).get("case"),
        "scenario": manifest.get("scenario") or manifest.get("options", {}).get("scenario"),
        "code_ref": manifest.get("code_ref") or summary.get("code_ref"),
        "inputs": [_share_safe_input(value) for value in summary.get("inputs", [])],
        "run_dir": run_dir.name,
        "local_cache": local_cache,
        "read_path": read_path,
        "current_source_ref": source_ref,
        "raw_input_archives": raw_input_archives,
    }
    return {
        "schema_version": 1,
        "metadata": metadata,
        "topology": topology,
        "source_trace_count": len(all_rows),
        "excluded_without_client_window": excluded_worker_only,
        "excluded_non_get": excluded_non_get,
        "trace_count": len(rows),
        "write_trace_count": len(write_rows),
        "top_requested": top_n,
        "deadline_ms": deadline_ms,
        "evidence_coverage": coverage,
        "limitations": limitations,
        "problem_summary": {
            name: values["trace_count"] for name, values in aggregate_data["problem_summary"].items()
        },
        "aggregate": aggregate_data,
        "write_aggregate": write_aggregate,
        "traces": rows,
        "write_traces": write_rows,
    }
