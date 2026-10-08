"""Shared validation owns persisted gates without orchestration dependencies."""
from trace_test_loader import REPO_ROOT
import ast
import importlib
import json
import sys
from pathlib import Path

import pytest

ROOT = REPO_ROOT
sys.path.insert(0, str(ROOT / "scripts"))
validation = importlib.import_module("trace_analysis.validation")


def save(path, data):
    path.write_text(json.dumps(data), encoding="utf-8")
    return path


def test_rendering_does_not_import_pipeline_and_validation_is_leaf():
    for name, forbidden in (("render_bundle", {"pipeline"}),
                            ("validation", {"pipeline", "render_bundle", "overview"})):
        tree = ast.parse((ROOT / "scripts/trace_analysis" / f"{name}.py").read_text())
        imports = {node.module for node in ast.walk(tree) if isinstance(node, ast.ImportFrom)}
        assert not (imports & forbidden)


def test_persisted_gate_matches_existing_validator_and_fails_closed(tmp_path):
    source = save(tmp_path / "source.json", {"traces": {}, "dimensions": {}})
    output = tmp_path / "new" / "gate.json"
    result = validation.validate_model_file(source, "triage", output)
    assert result == validation.validate(source, "triage")
    assert json.loads(output.read_text()) == result
    source.write_text("[]")
    with pytest.raises(RuntimeError):
        validation.validate_model_file(source, "triage", output)
    assert json.loads(output.read_text())["valid"] is False


@pytest.mark.parametrize("delta,accepted", [(0, True), (0.0109, True), (0.0111, False)])
def test_write_budget_retains_existing_tolerance(tmp_path, delta, accepted):
    read = save(tmp_path / "read.json", {"write_traces": [{"trace_id": "set-1"}]})
    write = save(tmp_path / "write.json", {"rows": [
        {"trace_id": "set-1", "client_ms": 1, "write_breakdown_ms": {"copy": 1 + delta}}
    ]})
    if accepted:
        validation.validate_write_model(write, read)
    else:
        with pytest.raises(ValueError, match="stage budget does not close"):
            validation.validate_write_model(write, read)


def test_write_coverage_receipt_keeps_phase_and_wr_denominators_separate():
    def states(create, copy, publish):
        return {name: {"state": state} for name, state in
                zip(("Create", "Copy", "Publish"), (create, copy, publish))}
    model = {"write_phase_schema_version": 2, "rows": [
        {"trace_id": "set-1", "operation": "SET", "wr_applicable": True,
         "write_wr_events": [{"request_id": "1", "write_chunk_index": 2, "write_chunk_count": 2}],
         "wr_phase_attribution": {"phase": "Copy"},
         "write_phase_observation": states("observed", "observed", "unobserved"),
         "write_rpc_phase_evidence": {"Create": {"state": "observed"},
                                      "Publish": {"state": "unobserved"}}},
        {"trace_id": "create-1", "operation": "CREATE", "wr_applicable": False,
         "write_wr_events": [], "wr_phase_attribution": {"phase": "not_applicable"},
         "write_phase_observation": states("observed", "not_applicable", "not_applicable"),
         "write_rpc_phase_evidence": {"Create": {"state": "unobserved"},
                                      "Publish": {"state": "unobserved"}}},
    ]}
    result = validation._write_coverage_summary(model)
    assert result["operation_counts"] == {"CREATE": 1, "SET": 1}
    assert result["wr_coverage"] == {"applicable": 1, "observed": 1, "unobserved": 0,
                                     "not_applicable": 1, "unknown": 0}
    assert result["phase_coverage"]["Copy"] == {"observed": 1, "unobserved": 0,
                                                  "not_applicable": 1}
    assert result["wr_phase_counts"] == {"Copy": 1, "Publish": 0,
                                         "unconfirmed": 0, "not_applicable": 1}
    assert result["rpc_phase_observed"] == {"Create": 1, "Publish": 0}
    assert result["missing_chunk_count"] is None


def test_write_coverage_does_not_infer_wr_applicability_from_unknown_operation():
    model = {"write_phase_schema_version": 2, "rows": [{
        "trace_id": "write-1", "operation": "UNKNOWN", "wr_applicable": None,
        "write_wr_events": [], "wr_phase_attribution": {"phase": "unconfirmed"},
        "write_phase_observation": {phase: {"state": "unobserved"}
                                    for phase in ("Create", "Copy", "Publish")},
        "write_rpc_phase_evidence": {phase: {"state": "unobserved"}
                                     for phase in ("Create", "Publish")},
    }]}
    result = validation._write_coverage_summary(model)
    assert result["wr_coverage"] == {"applicable": 0, "observed": 0,
                                     "unobserved": 0, "not_applicable": 0, "unknown": 1}


def test_legacy_write_coverage_does_not_invent_phase_counts():
    assert validation._write_coverage_summary({"rows": [{"trace_id": "set-1"}]}) == {
        "coverage_status": "unavailable_legacy_model"}


def test_render_only_requires_phase_aware_write_model_without_changing_legacy_validation(tmp_path):
    read = save(tmp_path / "read.json", {"write_traces": [{"trace_id": "set-1"}]})
    write = save(tmp_path / "write.json", {"rows": [{"trace_id": "set-1", "client_ms": 1,
        "write_breakdown_ms": {"未解释残差": 1}}]})
    validation.validate_write_model(write, read)
    with pytest.raises(ValueError, match="write phase schema 2.*rebuild"):
        validation.validate_write_model(write, read, require_phase_schema=True)


def test_write_budget_error_identifies_trace(tmp_path):
    read = save(tmp_path / "read.json", {"write_traces": [{"trace_id": "set-1"}]})
    write = save(tmp_path / "write.json", {"rows": [
        {"trace_id": "set-1", "client_ms": 1, "write_breakdown_ms": {"copy": 2}}
    ]})
    with pytest.raises(ValueError, match="set-1.*stage budget does not close"):
        validation.validate_write_model(write, read)


@pytest.mark.parametrize("failure", ["coverage", "duplicate_write", "duplicate_read", "stage"])
def test_write_validation_preserves_trace_and_stage_failures(tmp_path, failure):
    row = {"trace_id": "set-1", "client_ms": 1, "write_breakdown_ms": {"copy": 1}}
    read_data = {"write_traces": [row]}
    write_data = {"rows": [row]}
    if failure == "coverage":
        write_data["rows"] = []
    elif failure == "duplicate_write":
        write_data["rows"] = [row, row]
    elif failure == "duplicate_read":
        read_data["traces"] = [row, row]
    else:
        row["write_breakdown_ms"] = {"copy": -1}
    read = save(tmp_path / "read.json", read_data)
    write = save(tmp_path / "write.json", write_data)
    with pytest.raises(ValueError):
        validation.validate_write_model(write, read)


def test_pipeline_stage_validators_refer_to_shared_validation():
    pipeline = importlib.import_module("trace_analysis.pipeline")
    assert pipeline.validate is validation.validate_model_file
    assert pipeline.validate_write_evidence_model is validation.validate_write_evidence_model
