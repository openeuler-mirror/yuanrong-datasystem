#!/usr/bin/env python3
"""Validate persisted Trace intermediate results before HTML rendering."""
from __future__ import annotations

import argparse
import json
import math
import sys
from dataclasses import fields
from pathlib import Path

from .evidence.observation_validation import evidence_fact_errors
from .analysis.ub_edges import ub_edge_errors
from .evidence.normalized import load_evidence, read_observations, summary_digest
from .evidence.read import ReadObservations
from .evidence.write import is_write_flow, write_fact_errors
from .analysis.budget import _rpc_framework_ms, _valid_write_rpc_fields
from .analysis.issue_model import validate_issue_model

REQUIRED = {
    "bottleneck": {"traces": list, "aggregate": dict},
    "triage": {"traces": dict, "dimensions": dict},
    "numa": {"traces": list, "aggregate": dict, "limitations": list},
}

COVERAGE_FIELDS = ("trace_count", "client_observed", "rpc_observed", "urma_observed", "error_observed")
METRICS = ("rpc", "copy", "urma", "timeout", "errors")


def model_schema_status(data):
    if not isinstance(data, dict):
        raise ValueError("model must be an object")
    if "schema_version" not in data:
        return "legacy_unversioned"
    version = data["schema_version"]
    if type(version) is not int or version != 1:
        raise ValueError("unsupported schema_version: expected integer 1")
    return "supported"


def _contains(value, name):
    if isinstance(value, dict):
        return any(name in str(key).lower() or _contains(item, name) for key, item in value.items())
    if isinstance(value, list):
        return any(_contains(item, name) for item in value)
    return name in str(value).lower()


def metric_presence(value):
    remaining = list(METRICS)

    def match(item):
        nonlocal remaining
        text = str(item).lower()
        remaining = [name for name in remaining if name not in text]

    def visit(item):
        if isinstance(item, dict):
            for key, child in item.items():
                match(key)
                if not remaining:
                    return
                visit(child)
                if not remaining:
                    return
        elif isinstance(item, list):
            for child in item:
                visit(child)
                if not remaining:
                    return
        else:
            match(item)

    visit(value)
    return {name: name not in remaining for name in METRICS}


def _duration(value):
    if not isinstance(value, (int, float)) or isinstance(value, bool):
        return False
    try:
        return math.isfinite(value) and value >= 0
    except OverflowError:
        return False


def _budget_errors(row):
    errors = []
    client = row.get("client_ms")
    if client is not None and not _duration(client):
        errors.append("client_ms must be finite and non-negative")
    present = [name for name in ("attribution_ms", "focus_breakdown_ms") if name in row]
    if client is not None and not present:
        errors.append("observed client_ms requires a stage breakdown")
    for name in present:
        stages = row[name]
        if not isinstance(stages, dict) or not stages:
            errors.append(f"{name} must be a non-empty stage object")
            continue
        if any(not _duration(value) for value in stages.values()):
            errors.append(f"{name} contains non-finite, negative or non-numeric duration")
            continue
        total = sum(stages.values())
        if not _duration(total) or (_duration(client) and abs(total - client) > 0.05):
            errors.append(f"{name} does not close the client budget")
    return errors


def validate(path, kind):
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        result = validate_data(None, kind, path)
        result["errors"] = [f"unreadable JSON: {type(error).__name__}"]
        return result
    return validate_data(data, kind, path)


def validate_data(data, kind, input_label):
    result = {
        "schema_version": 1, "kind": kind, "input": str(input_label), "trace_count": 0,
        "metric_presence": {name: False for name in METRICS}, "valid": False,
    }
    if not isinstance(data, dict):
        result["errors"] = ["model must be an object"]
        return result
    try:
        result["schema_status"] = model_schema_status(data)
    except ValueError as error:
        result["schema_status"] = "unsupported"
        result["errors"] = [str(error)]
        return result
    missing = [key for key in REQUIRED[kind] if key not in data]
    errors = [f"missing {key}" for key in missing]
    if missing:
        result["missing"] = missing
    for key, expected_type in REQUIRED[kind].items():
        if key in data and not isinstance(data[key], expected_type):
            errors.append(f"{key} must be {expected_type.__name__}")
    result["metric_presence"] = metric_presence(data)
    traces = data.get("traces")
    expected_traces = REQUIRED[kind]["traces"]
    bad_budgets = []
    if isinstance(traces, expected_traces):
        result["trace_count"] = len(traces)
        records = traces.items() if isinstance(traces, dict) else enumerate(traces)
        seen = set()
        for index, row in records:
            if not isinstance(row, dict):
                errors.append(f"trace {index} must be an object")
                continue
            trace_id = index if kind == "triage" else row.get("trace_id")
            if not isinstance(trace_id, str) or not trace_id or trace_id in seen:
                errors.append(f"trace {index} requires a unique non-empty trace_id")
            else:
                seen.add(trace_id)
            if kind == "bottleneck":
                errors.extend(f"trace {trace_id}: {message}" for message in evidence_fact_errors(row))
                budget = _budget_errors(row)
                if budget:
                    bad_budgets.append(trace_id)
                    errors.extend(f"trace {trace_id}: {message}" for message in budget)
    if kind == "bottleneck":
        write_rows = data.get("write_traces", [])
        if not isinstance(write_rows, list):
            errors.append("write_traces must be a list")
        else:
            for row in write_rows:
                if not isinstance(row, dict):
                    errors.append("write trace must be an object")
                    continue
                errors.extend(write_fact_errors(row))
    if kind == "triage" and isinstance(traces, dict):
        errors.extend(ub_edge_errors(data))
        dimensions = data.get("dimensions", {})
        ub_summary = dimensions.get("ub_summary", {}) if isinstance(dimensions, dict) else {}
        if isinstance(ub_summary, dict):
            partitioned = "edges_by_operation" in ub_summary
            result["ub_edge_operation_status"] = "partitioned" if partitioned else "legacy_unpartitioned"

    if kind == "bottleneck":
        result["closure_bad_trace_count"] = len(bad_budgets)
        result["closure_bad_examples"] = bad_budgets[:10]
    result["valid"] = not errors
    result["error_count"] = len(errors)
    if errors:
        result["errors"] = errors[:10]
    return result


def validate_model_file(path: Path, kind: str, output: Path) -> dict:
    output.parent.mkdir(parents=True, exist_ok=True)
    result = validate(path, kind)
    output.write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    if not result["valid"]:
        raise RuntimeError(f"{kind} model validation failed: {result.get('errors', [])}")
    return result


def validate_input_inventory(run_dir: Path) -> dict:
    errors = []
    try:
        manifest = json.loads((run_dir / "manifest.json").read_text(encoding="utf-8"))
        inventory = json.loads((run_dir / "inventory.json").read_text(encoding="utf-8"))
    except (OSError, ValueError) as error:
        errors.append(f"input inventory unreadable: {type(error).__name__}")
        return {"schema_version": 1, "valid": False, "errors": errors}
    if not isinstance(manifest, dict) or not isinstance(inventory, dict):
        errors.append("manifest and input inventory must be objects")
        return {"schema_version": 1, "valid": False, "errors": errors}
    version = inventory.get("schema_version")
    if not isinstance(version, int) or isinstance(version, bool) or version != 1:
        errors.append("unsupported input inventory schema")
    expected_run_id = manifest.get("run_id") or manifest.get("case_name")
    if inventory.get("run_id") != expected_run_id:
        errors.append("input inventory Run ID differs from manifest")
    inputs = inventory.get("inputs")
    if not isinstance(inputs, list) or not inputs:
        errors.append("input inventory requires input records")
    elif inputs != manifest.get("inputs"):
        errors.append("input inventory differs from manifest")
    if isinstance(inputs, list):
        for index, item in enumerate(inputs):
            if not isinstance(item, dict):
                errors.append(f"input {index}: object required")
                continue
            size, digest, members = item.get("size"), item.get("sha256"), item.get("members")
            if not isinstance(size, int) or isinstance(size, bool) or size < 0:
                errors.append(f"input {index}: invalid size")
            if (not isinstance(digest, str) or len(digest) != 64
                    or any(char not in "0123456789abcdef" for char in digest)):
                errors.append(f"input {index}: invalid SHA256")
            if (not isinstance(members, list)
                    or any(not isinstance(name, str) or not name or Path(name).is_absolute()
                           or ".." in Path(name).parts for name in members)):
                errors.append(f"input {index}: invalid member list")
            if not isinstance(item.get("path"), str) or not item.get("path"):
                errors.append(f"input {index}: missing source path")
            if not isinstance(item.get("preserved_name"), str) or not item.get("preserved_name"):
                errors.append(f"input {index}: missing preserved name")
        sizes = [item.get("size") for item in inputs if isinstance(item, dict)]
        members = [item.get("members") for item in inputs if isinstance(item, dict)]
        input_count = inventory.get("input_count")
        if not isinstance(input_count, int) or isinstance(input_count, bool) or input_count != len(inputs):
            errors.append("input inventory count mismatch")
        if all(isinstance(value, list) for value in members):
            member_count = inventory.get("listed_member_count")
            if (not isinstance(member_count, int) or isinstance(member_count, bool)
                    or member_count != sum(len(value) for value in members)):
                errors.append("input inventory member count mismatch")
        if all(isinstance(value, int) and not isinstance(value, bool) for value in sizes):
            total_bytes = inventory.get("total_bytes")
            if not isinstance(total_bytes, int) or isinstance(total_bytes, bool) or total_bytes != sum(sizes):
                errors.append("input inventory byte count mismatch")
    return {"schema_version": 1, "valid": not errors, "errors": errors}


def validate_triage_bundle(summary_path: Path, output: Path) -> dict:
    result = validate(summary_path, "triage")
    inventory = validate_input_inventory(summary_path.parent)
    result["inventory"] = inventory
    result["valid"] = result.get("valid") is True and inventory.get("valid") is True
    if inventory.get("valid") is not True:
        result["errors"] = result.get("errors", []) + inventory.get("errors", [])
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    if result.get("valid") is not True:
        raise RuntimeError(f"triage bundle validation failed: {result.get('errors', [])}")
    return result


def validate_issue_file(path: Path, evidence_path: Path, read_path: Path,
                        write_path: Path, output: Path) -> dict:
    try:
        issue, evidence, read, write = (
            json.loads(source.read_text(encoding="utf-8"))
            for source in (path, evidence_path, read_path, write_path)
        )
        result = validate_issue_model(issue, evidence, read, write)
    except (OSError, ValueError, TypeError) as error:
        result = {"schema_version": 1, "valid": False, "errors": [str(error)]}
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    if result.get("valid") is not True:
        raise RuntimeError(f"issue model validation failed: {result.get('errors', [])}")
    return result


def _invalid_numeric_observations(observed: ReadObservations) -> bool:
    numbers = (observed.client_us, observed.worker_us, observed.status, observed.size_bytes)
    for value in numbers:
        if not isinstance(value, int) or isinstance(value, bool):
            return True
    if min(observed.client_us, observed.worker_us, observed.size_bytes) < 0:
        return True
    if not isinstance(observed.rpcs, dict) or not isinstance(observed.summary, dict):
        return True
    if not isinstance(observed.urma_values, list):
        return True
    return any(not _duration(value) for value in observed.urma_values)


def _invalid_other_observations(observed: ReadObservations) -> bool:
    for value in (observed.transport, observed.direct_data_worker, observed.client_observer):
        if not isinstance(value, str):
            return True
    if not isinstance(observed.urma_source_costs, dict):
        return True
    if not isinstance(observed.error_observations, dict):
        return True
    bool_fields = (observed.explicit_remote, observed.urma_total_text_observed,
                   observed.urma_timeout_error_observed)
    return any(not isinstance(value, bool) for value in bool_fields)


def validate_evidence_data(data: dict, summary: dict, expected_sha256: str) -> dict:
    errors = []
    traces = data.get("traces") if isinstance(data, dict) else None
    source_traces = summary.get("traces") if isinstance(summary, dict) else None
    if not isinstance(data, dict) or type(data.get("schema_version")) is not int or data["schema_version"] != 1:
        errors.append("unsupported evidence schema")
    if not isinstance(source_traces, dict) or not isinstance(traces, dict):
        errors.append("summary and evidence require Trace maps")
    elif set(traces) != set(source_traces):
        errors.append("evidence Trace identities differ from triage summary")
    if isinstance(data, dict) and data.get("summary_sha256") != expected_sha256:
        errors.append("evidence source digest differs from triage summary")
    actual_coverage = dict.fromkeys(COVERAGE_FIELDS, 0)
    if not errors:
        expected_fields = {item.name for item in fields(ReadObservations)} - {"texts"}
        for trace_id, entry in traces.items():
            read = entry.get("read") if isinstance(entry, dict) else None
            if not isinstance(read, dict) or set(read) != expected_fields:
                errors.append(f"trace {trace_id}: incomplete read observations")
                continue
            indices = read.get("display_indices")
            evidence_count = len(source_traces[trace_id].get("evidence", []))
            if (not isinstance(indices, list) or any(type(index) is not int or index < 0
                                                     or index >= evidence_count for index in indices)
                    or indices != sorted(set(indices))):
                errors.append(f"trace {trace_id}: invalid display evidence indices")
                continue
            try:
                observed = read_observations(source_traces[trace_id], entry)
            except (TypeError, KeyError, ValueError, IndexError) as error:
                errors.append(f"trace {trace_id}: invalid read observations: {error}")
                continue
            if _invalid_numeric_observations(observed):
                errors.append(f"trace {trace_id}: invalid numeric or container observations")
            if _invalid_other_observations(observed):
                errors.append(f"trace {trace_id}: invalid observation fields")
            for message in evidence_fact_errors({"evidence": observed.texts,
                                                 "evidence_facts": entry.get("facts")}):
                errors.append(f"trace {trace_id}: {message}")
            if is_write_flow(source_traces[trace_id]):
                if "write" not in entry:
                    errors.append(f"trace {trace_id}: missing write observations")
                else:
                    for message in write_fact_errors({
                        "trace_id": trace_id, "evidence": observed.texts,
                        "write_evidence_facts": entry["write"],
                    }):
                        errors.append(f"trace {trace_id}: {message}")
            actual_coverage["trace_count"] += 1
            actual_coverage["client_observed"] += bool(observed.client_us)
            actual_coverage["rpc_observed"] += bool(observed.rpcs)
            actual_coverage["urma_observed"] += bool(observed.urma_values)
            actual_coverage["error_observed"] += bool(source_traces[trace_id].get("errors"))
    coverage = data.get("coverage") if isinstance(data, dict) else None
    if (not isinstance(coverage, dict) or coverage != actual_coverage
            or any(type(value) is not int for value in coverage.values())):
        errors.append("evidence coverage does not match observed Trace facts")
    return {"schema_version": 1, "kind": "evidence", "trace_count": len(traces or {}),
            "valid": not errors, "error_count": len(errors), "errors": errors[:10]}


def validate_evidence_file(path: Path, summary_path: Path, output: Path) -> dict:
    try:
        data = load_evidence(path)
        summary = json.loads(summary_path.read_text(encoding="utf-8"))
        result = validate_evidence_data(data, summary, summary_digest(summary_path))
    except (OSError, ValueError) as error:
        result = {"schema_version": 1, "kind": "evidence", "valid": False,
                  "errors": [str(error)], "error_count": 1}
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    if not result["valid"]:
        raise RuntimeError(f"evidence model validation failed: {result['errors']}")
    return result


def validate_write_model(path, read_path, *, require_phase_schema=False):
    read = json.loads(read_path.read_text())
    write = json.loads(path.read_text())
    return validate_write_data(write, read, require_phase_schema=require_phase_schema)


def validate_write_data(write, read, *, require_phase_schema=False):
    if require_phase_schema and write.get("write_phase_schema_version") != 2:
        raise ValueError(
            "write phase schema 2 is required for rendering; rebuild the write model with pipeline --resume")
    model_schema_status(read)
    expected = {row["trace_id"] for row in read.get("write_traces", [])}
    for kind, rows in (("read", read.get("traces") or []),
                       ("source write", read.get("write_traces") or [])):
        ids = [row["trace_id"] for row in rows]
        if len(ids) != len(set(ids)):
            raise ValueError(f"duplicate {kind} Trace IDs in validation")
    for row in read.get("write_traces", []):
        fact_errors = write_fact_errors(row)
        if fact_errors:
            raise ValueError("; ".join(fact_errors))
    _validate_write_rows(write, expected)


def validate_write_evidence_model(path, summary_path, evidence_path):
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    evidence = load_evidence(evidence_path)
    if evidence.get("summary_sha256") != summary_digest(summary_path):
        raise ValueError("write Evidence does not match Triage summary")
    traces = summary.get("traces", {})
    entries = evidence.get("traces", {})
    if set(traces) != set(entries):
        raise ValueError("write Evidence Trace identities differ from Triage")
    expected = {trace_id for trace_id, trace in traces.items()
                if is_write_flow(trace) and read_observations(trace, entries[trace_id]).client_us}
    write = json.loads(path.read_text(encoding="utf-8"))
    _validate_write_rows(write, expected)
    return {"valid": True, "trace_count": len(expected), **_write_coverage_summary(write)}


def _write_coverage_summary(write):
    if write.get("write_phase_schema_version") != 2:
        return {"coverage_status": "unavailable_legacy_model"}
    rows = write["rows"]
    operation_counts = {}
    wr_coverage = {name: 0 for name in ("applicable", "observed", "unobserved", "not_applicable", "unknown")}
    phase_coverage = {phase: {state: 0 for state in ("observed", "unobserved", "not_applicable")}
                      for phase in ("Create", "Copy", "Publish")}
    wr_phase_counts = {phase: 0 for phase in ("Copy", "Publish", "unconfirmed", "not_applicable")}
    rpc_phase_observed = {phase: 0 for phase in ("Create", "Publish")}
    for row in rows:
        operation = row.get("operation", "UNKNOWN")
        operation_counts[operation] = operation_counts.get(operation, 0) + 1
        applicable = row.get("wr_applicable")
        if applicable is True:
            wr_coverage["applicable"] += 1
            wr_coverage["observed" if row.get("write_wr_events") else "unobserved"] += 1
        elif applicable is False:
            wr_coverage["not_applicable"] += 1
        else:
            wr_coverage["unknown"] += 1
        for phase, observation in row["write_phase_observation"].items():
            phase_coverage[phase][observation["state"]] += 1
        wr_phase_counts[row["wr_phase_attribution"]["phase"]] += 1
        for phase, observation in row["write_rpc_phase_evidence"].items():
            rpc_phase_observed[phase] += observation["state"] == "observed"
    return {
        "coverage_status": "available",
        "operation_counts": operation_counts,
        "wr_coverage": wr_coverage,
        "phase_coverage": phase_coverage,
        "wr_phase_counts": wr_phase_counts,
        "rpc_phase_observed": rpc_phase_observed,
        "budget_closure_bad_trace_count": 0,
        "missing_chunk_count": None,
        "missing_chunk_reason": "logical WR membership cannot be reconstructed from partial chunk logs",
    }


def _validate_write_rows(write, expected):
    model_schema_status(write)
    phase_schema = write.get("write_phase_schema_version")
    if phase_schema not in (None, 1, 2):
        raise ValueError("unsupported write phase schema")
    if not isinstance(write.get("rows"), list):
        raise ValueError("independent write model requires rows")
    ids = [row["trace_id"] for row in write["rows"]]
    if len(ids) != len(set(ids)):
        raise ValueError("duplicate write Trace IDs in validation")
    if set(ids) != expected:
        raise ValueError("write model Trace coverage does not match source analysis")
    for row in write["rows"]:
        fact_errors = write_fact_errors(row)
        if fact_errors:
            raise ValueError(f"{row['trace_id']}: {'; '.join(fact_errors)}")
        stages = row.get("write_breakdown_ms") or {}
        if not stages or any(not isinstance(v, (int, float)) or not math.isfinite(v) or v < 0 for v in stages.values()):
            raise ValueError(f"{row['trace_id']}: write model has invalid stage durations")
        if abs(sum(stages.values()) - row["client_ms"]) > 0.011:
            raise ValueError(f"{row['trace_id']}: write model stage budget does not close")
        if phase_schema in (1, 2):
            try:
                _validate_write_phases(row)
            except ValueError as error:
                raise ValueError(f"{row['trace_id']}: {error}") from error
        if phase_schema == 2:
            try:
                _validate_write_rpc_phases(row)
            except ValueError as error:
                raise ValueError(f"{row['trace_id']}: {error}") from error


def _validate_write_phases(row):
    observations = row.get("write_phase_observation")
    if not isinstance(observations, dict) or set(observations) != {"Create", "Copy", "Publish"}:
        raise ValueError("write phase observation is incomplete")
    for observation in observations.values():
        if not isinstance(observation, dict):
            raise ValueError("write phase observation is invalid")
        state, duration = observation.get("state"), observation.get("parent_ms")
        if state not in {"observed", "unobserved", "not_applicable"}:
            raise ValueError("write phase state is invalid")
        if state != "observed" and duration is not None:
            raise ValueError("unobserved write phase cannot contain a duration")
        if duration is not None:
            valid_duration = (
                isinstance(duration, (int, float))
                and not isinstance(duration, bool)
                and math.isfinite(duration)
                and duration >= 0
            )
            if not valid_duration:
                raise ValueError("write phase duration is invalid")
    applicable = row.get("wr_applicable")
    if applicable is not None and not isinstance(applicable, bool):
        raise ValueError("WR applicability is invalid")
    attribution = row.get("wr_phase_attribution")
    if not isinstance(attribution, dict):
        raise ValueError("WR phase attribution is missing")
    phase, basis = attribution.get("phase"), attribution.get("basis")
    callsite = row.get("write_wr_callsite")
    source = row.get("write_wr_callsite_source")
    callsite_phases = {
        "buffer.memory_copy_ub": "Copy",
        "buffer.publish_ub": "Publish",
        "ub_transporter.set": "Publish",
    }
    if phase in {"Copy", "Publish"}:
        if callsite_phases.get(callsite) != phase or not source or basis != source:
            raise ValueError("WR phase lacks matching callsite evidence")
    elif phase == "not_applicable":
        if applicable is not False or row.get("write_wr_events"):
            raise ValueError("WR phase applicability contradicts events")
    elif phase != "unconfirmed" or callsite is not None or basis != "wr_callsite_not_observed":
        raise ValueError("WR phase is invalid")


def _validate_write_rpc_phases(row):
    phases = row.get("write_rpc_phase_evidence")
    if not isinstance(phases, dict) or set(phases) != {"Create", "Publish"}:
        raise ValueError("RPC phase evidence is incomplete")
    entries = row["write_evidence_facts"]["rpc_entries"]
    metrics = {"e2e_ms": "e2e", "network_ms": "network_residual",
               "queue_ms": "server_req_queue"}
    for phase, item in phases.items():
        if not isinstance(item, dict) or item.get("state") not in {"observed", "unobserved"}:
            raise ValueError("RPC phase evidence state is invalid")
        if item["state"] == "unobserved":
            if any(item.get(name) is not None for name in (*metrics, "framework_ms", "method", "source_ref")):
                raise ValueError("unobserved RPC phase contains measured data")
            continue
        ref, method = item.get("source_ref"), item.get("method")
        source = next((entry for entry in entries
                       if entry["source_ref"] == ref and entry["method"] == method), None)
        method_suffix = method.rsplit(".", 1)[-1].lower() if isinstance(method, str) else ""
        if source is None or phase.lower() not in method_suffix or "meta" in method_suffix:
            raise ValueError("RPC phase source does not match evidence")
        rpc_fields = source["fields"]
        if not _valid_write_rpc_fields(rpc_fields):
            raise ValueError("RPC phase source lacks valid timing")
        framework = _rpc_framework_ms(rpc_fields)
        expected = {name: rpc_fields[key] / 1000 for name, key in metrics.items()}
        expected["framework_ms"] = framework
        for name, value in expected.items():
            actual = item.get(name)
            valid = (
                isinstance(actual, (int, float))
                and not isinstance(actual, bool)
                and math.isfinite(actual)
                and actual >= 0
            )
            if not valid or abs(actual - value) > 0.000001:
                raise ValueError("RPC phase duration differs from evidence")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--kind", choices=REQUIRED, required=True)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    result = validate(args.input, args.kind)
    text = json.dumps(result, ensure_ascii=False, indent=2) + "\n"
    sys.stdout.write(text)
    if args.output:
        args.output.write_text(text, encoding="utf-8")
    return 0 if result["valid"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
