"""Issue summaries keep observed failures separate from causal hypotheses."""
from trace_test_loader import REPO_ROOT

import json
import sys
from pathlib import Path

sys.path.insert(0, str(REPO_ROOT / "scripts"))

from trace_analysis.analysis.issue_model import build_issue_model, validate_issue_model
from test_ds_trace_stage_resume import make_case
from trace_analysis import pipeline
from trace_analysis import packaging
from trace_analysis.orchestration.bundle import _copy_run
from trace_analysis.orchestration.publication import current_publication


def _inputs():
    evidence = {
        "summary_sha256": "a" * 64,
        "coverage": {"error_observed": 2},
        "traces": {
            "get-1": {"facts": {"timeout_events": [{"request_id": "1"}]}},
            "set-1": {"facts": {"timeout_events": [{"request_id": "2"}, {"request_id": "3"}]}},
        },
    }
    read = {"traces": [
        {"trace_id": "get-1", "failed": True, "status": 1001,
         "error_chain_category": "URMA超时→RPC deadline", "error_failure_point": "WR等待超时",
         "error_root_cause_boundary": "接收端与链路原因未区分",
         "worker_log_assessment": {"state": "coverage_unknown"}},
        {"trace_id": "get-2", "failed": False, "status": 0},
    ]}
    write = {"rows": [
        {"trace_id": "set-1", "failed": True, "status": 1010,
         "issues": ["URMA超时", "回退失败"],
         "worker_log_assessment": {"state": "collection_evidence", "targets": [
             {"collection_status": "not_collected", "termination_supported": False}]}},
        {"trace_id": "set-2", "failed": False, "status": 0},
    ]}
    return evidence, read, write


def test_issue_model_separates_read_write_failures_and_unobserved_retries():
    evidence, read, write = _inputs()
    model = build_issue_model("case", evidence, read, write)
    assert model["counts"] == {
        "read_final_failure_trace_count": 1,
        "write_final_failure_trace_count": 1,
        "observed_error_trace_count": 2,
        "timeout_event_count": 3,
        "retry_attempt_count": None,
        "retry_attempt_reason": "not_reconstructed_from_normalized_evidence",
    }
    assert [(issue["operation"], issue["affected_ids"]) for issue in model["issues"]] == [
        ("read", ["get-1"]), ("write", ["set-1"]),
    ]
    assert all(issue["hypotheses"] == [] for issue in model["issues"])
    assert "worker_collection_unknown" in model["issues"][0]["missing_evidence"]
    assert "worker_logs_absent" in model["issues"][1]["missing_evidence"]
    assert validate_issue_model(model, evidence, read, write)["valid"]


def test_issue_model_validation_rejects_missing_or_duplicate_failure_ids():
    evidence, read, write = _inputs()
    model = build_issue_model("case", evidence, read, write)
    model["issues"][0]["affected_ids"].append("get-1")
    assert not validate_issue_model(model, evidence, read, write)["valid"]
    model = build_issue_model("case", evidence, read, write)
    model["issues"][0]["affected_ids"] = []
    assert not validate_issue_model(model, evidence, read, write)["valid"]
    model = build_issue_model("case", evidence, read, write)
    model["counts"]["read_final_failure_trace_count"] = True
    assert not validate_issue_model(model, evidence, read, write)["valid"]


def test_pipeline_publishes_validated_issue_model_and_rebuilds_tampering(tmp_path):
    manifest, output = make_case(tmp_path)
    first = pipeline.run_pipeline(manifest, output, False)
    run = first["runs"][0]
    issue_path = output / run["issues_analysis_json"]
    model = json.loads(issue_path.read_text())
    assert model["run_id"] == "case"
    assert first["validation"]["runs"]["case"]["issues"]["valid"]
    publication = Path(first["index"]).parent
    assert (publication / "runs/case/issues.analysis.json").is_file()
    issue_path.write_text("{}")
    resumed = pipeline.run_pipeline(manifest, output, False, resume=True)
    assert resumed["validation"]["runs"]["case"]["cache"]["issues"]["reason"] == "artifact_changed"
    assert json.loads((output / resumed["runs"][0]["issues_analysis_json"]).read_text()) == model
    exported = packaging.export(output, tmp_path / "offline", ["index.html"])
    assert "runs/case/issues.analysis.json" in exported["files"]


def test_legacy_publication_without_issue_artifact_still_copies(tmp_path):
    manifest, output = make_case(tmp_path)
    pipeline.run_pipeline(manifest, output, False)
    published = current_publication(output)["directory"]
    config = json.loads((published / "suite.manifest.json").read_text())["runs"][0]
    config.pop("issues_analysis_json")
    config["stage_provenance"].pop("issues")
    target = tmp_path / "legacy-view"
    target.mkdir()
    copied = _copy_run(config, published, target)
    assert "issues_analysis_json" not in copied
    assert not (target / "runs/case/issues.analysis.json").exists()
