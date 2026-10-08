"""Delivery rejects incomplete or internally inconsistent pipeline bundles."""
from trace_test_loader import REPO_ROOT
import copy
import importlib
import json
from pathlib import Path
import sys

import pytest

ROOT = REPO_ROOT
sys.path.insert(0, str(ROOT / "scripts"))


def save(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value), encoding="utf-8")


@pytest.fixture
def bundle(tmp_path):
    root = tmp_path / "report"
    root.mkdir()
    cfg = {"id": "r1", "triage_json": "runs/r1/summary.json", "analysis_json": "runs/r1/read.json",
           "write_analysis_json": "runs/r1/write.json", "numa_analysis_json": "runs/r1/numa.json"}
    save(root / cfg["triage_json"], {"traces": {"t1": {}}, "dimensions": {}, "trace_count": 1})
    save(root / cfg["analysis_json"], {"traces": [{"trace_id": "t1", "failed": False}], "write_traces": [], "aggregate": {}})
    save(root / cfg["write_analysis_json"], {"rows": []})
    save(root / cfg["numa_analysis_json"], {"traces": [{"trace_id": "t1"}], "aggregate": {}, "limitations": []})
    for key, name in (("triage_report", "triage"), ("bottleneck_report", "read"),
                      ("write_bottleneck_report", "write"), ("numa_report", "numa")):
        cfg[key] = f"runs/r1/{name}.html"
        (root / cfg[key]).write_text('<html><head></head><body>report</body></html>')
    run = {**cfg, "trace_count": 1, "error_count": 0, "unmatched_trace_count": 0,
           "read_summary": {"trace_count": 1, "failed_count": 0},
           "write_summary": {"trace_count": 0, "failed_count": 0},
           "numa_summary": {"trace_count": 1, "slow_wr_count": 0, "dual_chip_trace_count": 0},
           "bands": {"5–7ms": {"sample_count": 1, "failed_count": 0}},
           "overview_summary": {"id": "r1", "read": {"count": 1}, "write": {"count": 0}, "numa": {"count": 1}}}
    save(root / "suite.manifest.json", {"schema_version": 1, "runs": [cfg]})
    save(root / "suite.analysis.json", {"schema_version": 1, "runs": [run]})
    save(root / "runs/r1/run.summary.json", run["overview_summary"])
    gate = {"valid": True, "suite": {"valid": True}, "runs": {"r1": {
        stage: {"valid": True} for stage in ("triage", "bottleneck", "write", "numa")}}}
    save(root / "pipeline.validation.json", gate)
    (root / "index.html").write_text('<html><head></head><body>index</body></html>')
    return root


def module():
    return importlib.import_module("trace_analysis.delivery_validation")


def test_pipeline_delivery_verifies_all_runs_models_and_reports(bundle):
    result = module().validate_pipeline_bundle(bundle)
    assert result["valid"] is True
    assert result["run_ids"] == ["r1"]
    assert result["browser_validation"] == "not-performed"


@pytest.mark.parametrize("fault", ["missing_gate", "failed_gate", "missing_run", "failed_stage",
                                  "missing_model", "wrong_count", "negative_count", "nan_count",
                                  "duplicate_run", "missing_report", "stale_summary", "foreign_trace",
                                  "malformed_summary", "numa_count", "corrupt_model"])
def test_delivery_rejects_broken_pipeline_before_creating_package(bundle, tmp_path, fault):
    gate_path = bundle / "pipeline.validation.json"
    gate = json.loads(gate_path.read_text())
    suite = json.loads((bundle / "suite.analysis.json").read_text())
    if fault == "missing_gate": gate_path.unlink()
    elif fault == "failed_gate": gate["valid"] = False
    elif fault == "missing_run": gate["runs"] = {}
    elif fault == "failed_stage": gate["runs"]["r1"]["write"]["valid"] = False
    elif fault == "missing_model": (bundle / "runs/r1/write.json").unlink()
    elif fault == "wrong_count": suite["runs"][0]["read_summary"]["trace_count"] = 99
    elif fault == "negative_count": suite["runs"][0]["error_count"] = -1
    elif fault == "nan_count": suite["runs"][0]["bands"]["5–7ms"]["sample_count"] = float("nan")
    elif fault == "duplicate_run": suite["runs"].append(copy.deepcopy(suite["runs"][0]))
    elif fault == "missing_report": (bundle / "runs/r1/numa.html").unlink()
    elif fault == "stale_summary": save(bundle / "runs/r1/run.summary.json", {"id": "wrong"})
    elif fault == "malformed_summary": suite["runs"][0]["read_summary"] = [1]
    elif fault == "numa_count": suite["runs"][0]["numa_summary"]["slow_wr_count"] = 99
    elif fault == "corrupt_model": (bundle / "runs/r1/numa.json").write_text("{")
    elif fault == "foreign_trace": save(bundle / "runs/r1/numa.json", {"traces": [{"trace_id": "foreign"}], "aggregate": {}, "limitations": []})
    if fault != "missing_gate": save(gate_path, gate)
    save(bundle / "suite.analysis.json", suite)
    assert module().validate_pipeline_bundle(bundle)["valid"] is False
    from trace_analysis import packaging
    with pytest.raises(ValueError, match="delivery validation"):
        packaging.export(bundle, tmp_path / "package", ["index.html"])
    assert not (tmp_path / "package").exists()


def test_valid_pipeline_and_legacy_single_page_can_be_packaged(bundle, tmp_path):
    from trace_analysis import packaging
    result = packaging.export(bundle, tmp_path / "package", ["index.html"])
    assert result["delivery_validation"]["valid"] is True
    notices = result["third_party_notices"]
    assert notices == ["THIRD_PARTY/echarts/LICENSE", "THIRD_PARTY/echarts/NOTICE",
                       "THIRD_PARTY/echarts/licenses/LICENSE-d3",
                       "THIRD_PARTY/echarts/provenance.json"]
    for relative in notices:
        source = packaging.echarts_path().parent / Path(relative).relative_to("THIRD_PARTY/echarts")
        assert (tmp_path / "package" / relative).read_bytes() == source.read_bytes()
    legacy = tmp_path / "legacy"
    legacy.mkdir()
    (legacy / "index.html").write_text('<html><head></head></html>')
    assert packaging.export(legacy, tmp_path / "legacy-package", ["index.html"])["html_pages"] == 1


def test_missing_echarts_notice_rejects_package_before_output(bundle, tmp_path, monkeypatch):
    from trace_analysis import packaging
    monkeypatch.setattr(packaging, "echarts_path", lambda: tmp_path / "missing/echarts.min.js")
    output = tmp_path / "offline"
    with pytest.raises(ValueError, match="ECharts notice missing"):
        packaging.export(bundle, output, ["index.html"])
    assert not output.exists()


def test_suite_semantics_reject_empty_and_mismatched_run_sets(bundle):
    suite = json.loads((bundle / "suite.analysis.json").read_text())
    manifest = json.loads((bundle / "suite.manifest.json").read_text())
    assert module().validate_suite(suite, manifest)["valid"]
    assert not module().validate_suite({"schema_version": 1, "runs": []})["valid"]
    manifest["runs"].append({"id": "missing"})
    assert not module().validate_suite(suite, manifest)["valid"]


@pytest.mark.parametrize("fault", ["count", "run_set"])
def test_pipeline_rejects_tampered_suite_and_persists_failed_gate(tmp_path, monkeypatch, fault):
    from trace_analysis import pipeline
    from test_ds_trace_stage_resume import make_case
    manifest, output = make_case(tmp_path)
    original = pipeline.stages.run_suite

    def tamper(*args, **kwargs):
        result = original(*args, **kwargs)
        path = result.artifacts["analysis_json"]
        model = json.loads(path.read_text())
        if fault == "count":
            model["runs"][0]["read_summary"]["trace_count"] += 1
        else:
            model["runs"] = []
        save(path, model)
        return result

    monkeypatch.setattr(pipeline.stages, "run_suite", tamper)
    with pytest.raises(RuntimeError, match="suite validation failed"):
        pipeline.run_pipeline(manifest, output, False)
    gate = json.loads((output / "pipeline.validation.json").read_text())
    assert gate["valid"] is False
    assert gate["suite"]["valid"] is False
    assert gate["suite"]["errors"]


def test_pipeline_suite_validation_holds_resource_budget(tmp_path, monkeypatch):
    from trace_analysis import pipeline
    from test_ds_trace_stage_resume import make_case
    manifest, output = make_case(tmp_path)
    budgets = []
    budget_class = pipeline.ResourceBudget
    actual = module().validate_suite

    def create_budget(*args, **kwargs):
        value = budget_class(*args, **kwargs)
        budgets.append(value)
        return value

    called = []

    def check(*args, **kwargs):
        assert budgets[0].snapshot()["active"] == 1
        called.append(True)
        return actual(*args, **kwargs)

    monkeypatch.setattr(pipeline, "ResourceBudget", create_budget)
    monkeypatch.setattr(pipeline, "validate_suite", check, raising=False)
    assert pipeline.run_pipeline(manifest, output, False)["validation"]["valid"]
    assert called == [True]
    assert module().validate_pipeline_bundle(output)["valid"]


@pytest.fixture
def published_bundle(bundle, tmp_path):
    import shutil
    from trace_analysis.orchestration import publication
    stable = tmp_path / "stable"
    directory = publication.new_publication(stable)
    shutil.copytree(bundle, directory, dirs_exist_ok=True)
    (directory / "pipeline.validation.json").unlink()
    save(directory / "publication.validation.json", {
        "schema_version": 1, "valid": True, "errors": [], "run_ids": ["r1"],
        "browser_validation": "not-performed"})
    publication.commit_publication(stable, directory, "published-v1")
    save(stable / "pipeline.validation.json", {"valid": False, "runs": {"failed-new-run": {"valid": False}}})
    save(stable / "suite.manifest.json", {"schema_version": 1, "runs": [{"id": "stale"}]})
    return stable, directory


def test_stable_root_uses_published_generation_not_failed_latest_attempt(published_bundle, tmp_path):
    from trace_analysis import packaging
    stable, directory = published_bundle
    result = module().validate_pipeline_bundle(stable)
    assert result["valid"]
    assert result["run_ids"] == ["r1"]
    assert result["publication"]["key"] == "published-v1"
    assert result["publication"]["generation"] == directory.relative_to(stable).as_posix()
    packaged = packaging.export(stable, tmp_path / "published-package", ["index.html"],
                                manifest=stable / "suite.manifest.json")
    assert packaged["delivery_validation"]["publication"] == result["publication"]
    assert packaged["html_pages"] == 5
    assert "trace-publication" not in (tmp_path / "published-package/index.html").read_text()


@pytest.mark.parametrize("fault", ["bad_pointer", "changed_generation", "missing_descriptor"])
def test_corrupt_publication_never_falls_back_to_legacy(published_bundle, bundle, tmp_path, fault):
    from trace_analysis import packaging
    stable, directory = published_bundle
    import shutil
    shutil.copytree(bundle, stable, dirs_exist_ok=True, ignore=shutil.ignore_patterns("index.html"))
    if fault == "bad_pointer":
        (stable / "index.html").write_text('<meta name="trace-publication" content="../bad.json">')
    elif fault == "changed_generation":
        (directory / "runs/r1/read.html").write_text("changed")
    else:
        (directory / "publication.json").unlink()
    assert not module().validate_pipeline_bundle(stable)["valid"]
    with pytest.raises(ValueError, match="delivery validation"):
        packaging.export(stable, tmp_path / "bad-package", ["index.html"])
    assert not (tmp_path / "bad-package").exists()


@pytest.mark.parametrize("fault", ["changed_report", "empty_descriptor", "incomplete_artifacts", "missing_key", "bool_schema", "float_schema"])
def test_direct_generation_checks_its_descriptor_before_export(published_bundle, tmp_path, fault):
    from trace_analysis import packaging
    stable, directory = published_bundle
    descriptor_path = directory / "publication.json"
    descriptor = json.loads(descriptor_path.read_text())
    if fault == "changed_report":
        (directory / "runs/r1/read.html").write_text("changed after publication")
    elif fault == "empty_descriptor":
        descriptor = {}
    elif fault == "incomplete_artifacts":
        descriptor["artifacts"].pop("suite.analysis.json")
    elif fault == "missing_key":
        descriptor.pop("key")
    else:
        descriptor["schema_version"] = True if fault == "bool_schema" else 1.0
    save(descriptor_path, descriptor)
    assert not module().validate_pipeline_bundle(directory)["valid"]
    with pytest.raises(ValueError, match="delivery validation"):
        packaging.export(directory, tmp_path / "invalid-package", ["index.html"])
    assert not (tmp_path / "invalid-package").exists()


def test_direct_generation_can_be_verified_and_exported(published_bundle, tmp_path):
    from trace_analysis import packaging
    _, directory = published_bundle
    assert module().validate_pipeline_bundle(directory)["valid"]
    assert packaging.export(directory, tmp_path / "direct-package", ["index.html"])["html_pages"] == 5


@pytest.mark.parametrize("version", [True, 1.0, "1", None, 2])
def test_suite_rejects_noninteger_or_unsupported_schema(bundle, version):
    suite = json.loads((bundle / "suite.analysis.json").read_text())
    suite["schema_version"] = version
    assert not module().validate_suite(suite)["valid"]
