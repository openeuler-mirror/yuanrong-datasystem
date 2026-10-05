"""Validate suite meaning and pipeline delivery without importing renderers or runners."""
from __future__ import annotations

import json
from pathlib import Path

from .validation import validate_data, validate_write_data
from .evidence.normalized import summary_digest
from .orchestration.publication import current_publication, validate_publication
from .analysis.issue_model import validate_issue_model

MODEL_FIELDS = ("triage_json", "evidence_json", "analysis_json", "write_analysis_json",
                "numa_analysis_json", "issues_analysis_json")
REPORT_FIELDS = ("triage_report", "bottleneck_report", "write_bottleneck_report", "numa_report")


def _result(errors, ids):
    return {"schema_version": 1, "valid": not errors, "errors": errors,
            "run_ids": ids, "browser_validation": "not-performed"}


def _runs(data, label, errors):
    runs = data.get("runs") if isinstance(data, dict) else None
    if not isinstance(runs, list) or not runs:
        errors.append(f"{label}: non-empty runs list required")
        return {}
    result = {}
    for run in runs:
        run_id = run.get("id") if isinstance(run, dict) else None
        invalid_id = not isinstance(run_id, str) or not run_id
        if invalid_id or run_id in {".", ".."} or any(c in run_id for c in "/\\:"):
            errors.append(f"{label}: invalid run id")
        elif run_id in result:
            errors.append(f"{label}: duplicate run id {run_id}")
        else:
            result[run_id] = run
    return result


def _counts(value, path, errors):
    if not isinstance(value, dict):
        return
    for key, item in value.items():
        here = f"{path}.{key}"
        if key == "count" or key.endswith("_count"):
            if item is not None and (type(item) is not int or item < 0):
                errors.append(f"{here}: count must be a non-negative integer")
        if key.endswith("_counts") and isinstance(item, dict):
            if any(type(count) is not int or count < 0 for count in item.values()):
                errors.append(f"{here}: counts must be non-negative integers")
        _counts(item, here, errors)


def _mapping(value):
    return value if isinstance(value, dict) else {}


def _compare(actual, expected, label, errors):
    if actual != expected or type(actual) is not int:
        errors.append(f"{label}: expected {expected}, got {actual}")


def _read(path, errors):
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(value, dict):
            raise ValueError("JSON object required")
        return value
    except (OSError, ValueError) as error:
        errors.append(f"{path.name}: {error}")
        return {}


def _reference(root, value, label, errors):
    if not isinstance(value, str) or not value:
        errors.append(f"{label}: missing artifact reference")
        return None
    path = (root / value).resolve()
    if not path.is_relative_to(root) or not path.is_file():
        errors.append(f"{label}: missing artifact or path outside bundle")
        return None
    return path


def _model_checks(root, cfg, run, errors):
    paths = {field: (_reference(root, cfg.get(field), f"{cfg['id']}.{field}", errors)
                     if field not in ("evidence_json", "issues_analysis_json") or field in cfg else None)
             for field in MODEL_FIELDS}
    models = {}
    for field, path in paths.items():
        if path is None:
            continue
        models[field] = _read(path, errors)
        if run.get(field) and (root / run[field]).resolve() != path:
            errors.append(f"{cfg['id']}.{field}: suite model reference differs from manifest")
    for field, kind in (("triage_json", "triage"), ("analysis_json", "bottleneck"),
                        ("numa_analysis_json", "numa")):
        if paths[field] is not None:
            checked = validate_data(models[field], kind, paths[field])
            errors.extend(f"{cfg['id']}.{kind}: {message}" for message in checked.get("errors", []))
    if paths["evidence_json"] is not None and paths["triage_json"] is not None:
        from .validation import validate_evidence_data
        try:
            evidence_model = models["evidence_json"]
            triage_model = models["triage_json"]
        except KeyError as error:
            errors.append(f"{cfg['id']}.evidence: missing model {error.args[0]}")
        else:
            checked = validate_evidence_data(evidence_model, triage_model,
                                             summary_digest(paths["triage_json"]))
            errors.extend(f"{cfg['id']}.evidence: {message}" for message in checked.get("errors", []))
    if paths["write_analysis_json"] and paths["analysis_json"]:
        try:
            validate_write_data(models["write_analysis_json"], models["analysis_json"])
        except (OSError, ValueError, KeyError, TypeError) as error:
            errors.append(f"{cfg['id']}.write: {error}")
    issue_inputs_ready = (paths["evidence_json"] and paths["analysis_json"]
                          and paths["write_analysis_json"])
    if paths["issues_analysis_json"] and issue_inputs_ready:
        try:
            issue_model = models["issues_analysis_json"]
            evidence_model = models["evidence_json"]
            read_model = models["analysis_json"]
            write_model = models["write_analysis_json"]
        except KeyError as error:
            errors.append(f"{cfg['id']}.issues: missing model {error.args[0]}")
        else:
            if issue_model.get("run_id") != cfg["id"]:
                errors.append(f"{cfg['id']}.issues: Run ID differs from manifest")
            checked = validate_issue_model(issue_model, evidence_model, read_model, write_model)
            errors.extend(f"{cfg['id']}.issues: {message}" for message in checked.get("errors", []))
            if run.get("issue_summary") != issue_model.get("counts"):
                errors.append(f"{cfg['id']}.issues: suite issue summary differs from model")
    triage = models.get("triage_json", {})
    source_ids = set(triage.get("traces", {})) if isinstance(triage.get("traces"), dict) else set()
    if "trace_count" in triage:
        _compare(triage["trace_count"], len(source_ids), f"{cfg['id']}.triage.trace_count", errors)
    for field, summary, rows_key in (("analysis_json", "read_summary", "traces"),
                                     ("write_analysis_json", "write_summary", "rows"),
                                     ("numa_analysis_json", "numa_summary", "traces")):
        rows = models.get(field, {}).get(rows_key)
        if not isinstance(rows, list) or any(not isinstance(row, dict) for row in rows):
            errors.append(f"{cfg['id']}.{field}: {rows_key} must contain records")
            continue
        ids = [row.get("trace_id") for row in rows]
        if any(not isinstance(trace, str) or not trace for trace in ids) or len(set(ids)) != len(ids):
            errors.append(f"{cfg['id']}.{field}: invalid or duplicate Trace IDs")
        elif not set(ids).issubset(source_ids):
            errors.append(f"{cfg['id']}.{field}: Trace IDs absent from triage model")
        _compare(_mapping(run.get(summary)).get("trace_count"), len(rows), f"{cfg['id']}.{summary}", errors)
        if summary != "numa_summary":
            failed = sum(bool(row["failed"]) if "failed" in row
                         else str(row.get("status") or "0") not in ("0", "OK") for row in rows)
            _compare(_mapping(run.get(summary)).get("failed_count"), failed,
                     f"{cfg['id']}.{summary}.failed_count", errors)
    aggregate = _mapping(models.get("numa_analysis_json", {}).get("aggregate"))
    for field in ("slow_wr_count", "dual_chip_trace_count"):
        _compare(_mapping(run.get("numa_summary")).get(field), aggregate.get(field, 0),
                 f"{cfg['id']}.numa_summary.{field}", errors)
    for field in REPORT_FIELDS:
        _reference(root, cfg.get(field), f"{cfg['id']}.{field}", errors)
        if run.get(field) != cfg.get(field):
            errors.append(f"{cfg['id']}.{field}: suite report differs from manifest")
    summary = _read(root / "runs" / cfg["id"] / "run.summary.json", errors)
    if summary != run.get("overview_summary"):
        errors.append(f"{cfg['id']}: persisted Run summary differs from suite")


def validate_suite(suite, manifest=None, root=None, require_models=False):
    errors = []
    if (not isinstance(suite, dict) or type(suite.get("schema_version")) is not int
            or suite["schema_version"] != 1):
        errors.append("suite schema_version must be 1")
    runs = _runs(suite, "suite", errors)
    configured = _runs(manifest, "manifest", errors) if manifest is not None else runs
    if set(configured) != set(runs):
        errors.append("suite and manifest Run sets differ")
    for run_id, run in runs.items():
        _counts(run, run_id, errors)
        count, failed = run.get("trace_count"), run.get("error_count")
        read = _mapping(run.get("read_summary"))
        _compare(read.get("trace_count"), count, f"{run_id}.read_summary.trace_count", errors)
        _compare(read.get("failed_count"), failed, f"{run_id}.read_summary.failed_count", errors)
        if type(count) is int and type(failed) is int and failed > count:
            errors.append(f"{run_id}: failed count exceeds Trace count")
        bands = run.get("bands")
        if isinstance(bands, dict) and all(isinstance(band, dict) for band in bands.values()):
            samples = [band.get("sample_count") for band in bands.values()]
            if all(type(value) is int for value in samples):
                _compare(sum(samples), count, f"{run_id}.bands total", errors)
            else:
                errors.append(f"{run_id}: invalid band sample count")
        else:
            errors.append(f"{run_id}: missing bands")
        for summary in ("read_summary", "write_summary", "numa_summary"):
            value = run.get(summary)
            if not isinstance(value, dict) or type(value.get("trace_count")) is not int:
                errors.append(f"{run_id}.{summary}: Trace count required")
        if require_models and run_id in configured:
            if root is None:
                errors.append("model validation requires bundle root")
            else:
                _model_checks(Path(root).resolve(), configured[run_id], run, errors)
    return _result(errors, list(runs))


def _pipeline_gate(gate, ids, errors, configured):
    if (gate.get("valid") is not True or not isinstance(gate.get("suite"), dict)
            or gate["suite"].get("valid") is not True):
        errors.append("pipeline validation or suite gate did not pass")
    gates = gate.get("runs")
    if not isinstance(gates, dict) or set(gates) != set(ids):
        errors.append("pipeline validation Run set differs from suite")
        return
    for run_id, stages in gates.items():
        required = ("triage", "bottleneck", "write", "numa")
        if "issues_analysis_json" in configured.get(run_id, {}):
            required += ("issues",)
        for stage in required:
            result = stages.get(stage) if isinstance(stages, dict) else None
            if not isinstance(result, dict) or result.get("valid") is not True or result.get("errors"):
                errors.append(f"{run_id}.{stage}: validation gate did not pass")


def validate_pipeline_bundle(root, *, allow_legacy=False):
    root = Path(root).resolve()
    errors = []
    selection = {"source_directory": str(root), "kind": "pipeline"}
    try:
        publication = current_publication(root)
    except (OSError, ValueError) as error:
        return {**_result([f"publication integrity: {error}"], []), **selection}
    if publication is not None:
        selection["publication"] = {"key": publication["key"],
                                    "generation": publication["directory"].relative_to(root).as_posix()}
        root = publication["directory"]
        selection.update(source_directory=str(root), kind="publication")
    elif (root / "publication.json").is_file():
        try:
            generation = validate_publication(root)
        except (OSError, ValueError) as error:
            return {**_result([f"publication integrity: {error}"], []), **selection}
        selection.update(kind="publication", publication={"key": generation["key"], "generation": root.name})
    elif allow_legacy:
        manifests = ("suite.manifest.json", "pipeline.validation.json", "publication.validation.json")
        if not any((root / name).exists() for name in manifests):
            return {**_result([], []), **selection, "kind": "legacy"}
    manifest = _read(root / "suite.manifest.json", errors)
    suite = _read(root / "suite.analysis.json", errors)
    checked = validate_suite(suite, manifest, root, require_models=True)
    errors.extend(checked["errors"])
    if selection["kind"] == "publication":
        gate = _read(root / "publication.validation.json", errors)
        if gate.get("valid") is not True or gate.get("errors"):
            errors.append("publication validation did not pass")
        if gate.get("run_ids") != checked["run_ids"]:
            errors.append("publication validation Run set differs from suite")
    else:
        _pipeline_gate(_read(root / "pipeline.validation.json", errors), checked["run_ids"], errors,
                       _runs(manifest, "manifest", errors))
    _reference(root, "index.html", "suite homepage", errors)
    return {**_result(errors, checked["run_ids"]), **selection}
