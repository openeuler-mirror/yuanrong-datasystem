"""Aggregate observed failure evidence without promoting correlation to causation."""
from __future__ import annotations

from collections import Counter, defaultdict


def _is_integer(value):
    return isinstance(value, int) and not isinstance(value, bool)


def _valid_affected_ids(ids):
    if not isinstance(ids, list) or not ids:
        return False
    if any(not isinstance(item, str) or not item for item in ids):
        return False
    return len(ids) == len(set(ids))


def _valid_issue_evidence(issue):
    category = issue.get("category")
    if not isinstance(category, str) or not category:
        return False
    if issue.get("confidence") != "observed_failure":
        return False
    return (isinstance(issue.get("facts"), dict)
            and isinstance(issue.get("hypotheses"), list)
            and isinstance(issue.get("missing_evidence"), list))


def _failed_rows(model, field):
    rows = model.get(field)
    if not isinstance(rows, list):
        raise ValueError(f"issue source requires {field} records")
    if any(not isinstance(row, dict) or not isinstance(row.get("failed"), bool)
           or not isinstance(row.get("trace_id"), str) or not row["trace_id"] for row in rows):
        raise ValueError(f"issue source has invalid {field} records")
    return [row for row in rows if row["failed"]]


def _missing_worker_evidence(row):
    assessment = row.get("worker_log_assessment") or {}
    if not isinstance(assessment, dict):
        return []
    missing = []
    if assessment.get("state") == "coverage_unknown":
        missing.append("worker_collection_unknown")
    for target in assessment.get("targets") or []:
        if isinstance(target, dict) and target.get("collection_status") == "not_collected":
            missing.append("worker_logs_absent")
    return missing


def _category(operation, row):
    if operation == "read":
        return str(row.get("error_chain_category") or row.get("error_family") or "未分类")
    return "status:" + str(row.get("status") if row.get("status") is not None else "未观测")


def build_issue_model(run_id, evidence, read, write):
    """Keep GET/SET failure denominators and evidence gaps independent."""
    if not isinstance(run_id, str) or not run_id:
        raise ValueError("issue model requires a Run ID")
    if not isinstance(evidence.get("traces"), dict) or not isinstance(evidence.get("coverage"), dict):
        raise ValueError("issue model requires validated Evidence")
    failed = {"read": _failed_rows(read, "traces"), "write": _failed_rows(write, "rows")}
    groups = defaultdict(list)
    for operation, rows in failed.items():
        for row in rows:
            groups[(operation, _category(operation, row))].append(row)
    issues = []
    for (operation, category), rows in sorted(groups.items()):
        statuses = Counter(str(row.get("status") if row.get("status") is not None else "未观测")
                           for row in rows)
        points = sorted({str(row["error_failure_point"]) for row in rows
                         if row.get("error_failure_point")})
        boundaries = sorted({str(row["error_root_cause_boundary"]) for row in rows
                             if row.get("error_root_cause_boundary")})
        labels = set()
        for row in rows:
            for label in row.get("issues", []):
                if isinstance(label, str) and label:
                    labels.add(label)
        observed_labels = sorted(labels)
        missing = sorted({reason for row in rows for reason in _missing_worker_evidence(row)})
        if not boundaries:
            missing.append("root_cause_boundary_unobserved")
        issues.append({
            "operation": operation, "category": category,
            "affected_ids": sorted(row["trace_id"] for row in rows),
            "facts": {"status_counts": dict(statuses), "failure_points": points,
                      "observed_labels": observed_labels},
            "hypotheses": [], "missing_evidence": sorted(set(missing)),
            "root_cause_boundaries": boundaries, "confidence": "observed_failure",
        })
    timeout_count = sum(len((entry.get("facts") or {}).get("timeout_events") or [])
                        for entry in evidence["traces"].values())
    return {
        "schema_version": 1, "run_id": run_id,
        "source": {"evidence_summary_sha256": evidence.get("summary_sha256")},
        "counts": {
            "read_final_failure_trace_count": len(failed["read"]),
            "write_final_failure_trace_count": len(failed["write"]),
            "observed_error_trace_count": evidence["coverage"].get("error_observed"),
            "timeout_event_count": timeout_count,
            "retry_attempt_count": None,
            "retry_attempt_reason": "not_reconstructed_from_normalized_evidence",
        },
        "issues": issues,
        "limitations": ["错误事件、重试尝试与最终失败是不同分母；同期伴随不证明因果。"],
    }


def validate_issue_model(model, evidence, read, write):
    errors = []
    if not isinstance(model, dict) or not _is_integer(model.get("schema_version")) or model["schema_version"] != 1:
        return {"schema_version": 1, "valid": False, "errors": ["unsupported issue schema"]}
    if not isinstance(model.get("run_id"), str) or not model["run_id"]:
        return {"schema_version": 1, "valid": False, "errors": ["issue Run ID required"]}
    counts = model.get("counts")
    issues = model.get("issues")
    if not isinstance(counts, dict) or not isinstance(issues, list):
        return {"schema_version": 1, "valid": False, "errors": ["issue counts and groups required"]}
    try:
        failed = {"read": _failed_rows(read, "traces"), "write": _failed_rows(write, "rows")}
    except ValueError as error:
        return {"schema_version": 1, "valid": False, "errors": [str(error)]}
    expected = {operation: {row["trace_id"] for row in rows} for operation, rows in failed.items()}
    if any(len(expected[operation]) != len(rows) for operation, rows in failed.items()):
        errors.append("duplicate source failure Trace IDs")
    for operation in ("read", "write"):
        key = operation + "_final_failure_trace_count"
        if not _is_integer(counts.get(key)) or counts.get(key) != len(failed.get(operation, [])):
            errors.append(f"{key} differs from {operation} model")
    coverage = evidence.get("coverage") or {}
    if (not _is_integer(counts.get("observed_error_trace_count"))
            or counts.get("observed_error_trace_count") != coverage.get("error_observed")):
        errors.append("observed error Trace count differs from Evidence")
    traces = evidence.get("traces") or {}
    expected_timeouts = sum(len((entry.get("facts") or {}).get("timeout_events") or [])
                            for entry in traces.values())
    if not _is_integer(counts.get("timeout_event_count")) or counts.get("timeout_event_count") != expected_timeouts:
        errors.append("timeout event count differs from Evidence")
    if counts.get("retry_attempt_count") is not None or not counts.get("retry_attempt_reason"):
        errors.append("retry attempts must stay unobserved until reconstructed")
    if (model.get("source") or {}).get("evidence_summary_sha256") != evidence.get("summary_sha256"):
        errors.append("Evidence digest differs")
    seen = {"read": set(), "write": set()}
    for issue in issues:
        if not isinstance(issue, dict) or issue.get("operation") not in seen:
            errors.append("invalid issue operation")
            continue
        operation = issue.get("operation")
        ids = issue.get("affected_ids")
        if not _valid_affected_ids(ids):
            errors.append("invalid or duplicate affected Trace IDs")
            continue
        recorded = seen.get(operation)
        source_ids = expected.get(operation)
        if recorded is None or source_ids is None:
            errors.append("invalid issue operation")
            continue
        if recorded.intersection(ids) or not set(ids).issubset(source_ids):
            errors.append("issue failure IDs overlap or lack source failures")
        recorded.update(ids)
        if not _valid_issue_evidence(issue):
            errors.append("invalid issue evidence contract")
    for operation, recorded in seen.items():
        if recorded != expected.get(operation):
            errors.append(f"{operation} final failures are not fully covered")
    if not errors and model != build_issue_model(model["run_id"], evidence, read, write):
        errors.append("issue facts or evidence boundaries differ from source models")
    return {"schema_version": 1, "valid": not errors, "errors": errors}
