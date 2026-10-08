"""Successful report generations survive stage, rendering, gate and commit failures."""
import json
from pathlib import Path
from unittest.mock import Mock

import pytest

from test_ds_trace_stage_resume import make_case
from trace_analysis import pipeline, render_bundle
from trace_analysis.orchestration.publication import current_publication


def snapshot(root):
    return {path.relative_to(root): path.read_bytes() for path in root.rglob('*') if path.is_file()}


def assert_unchanged(root, before):
    assert snapshot(root) == before


@pytest.mark.parametrize("failure", ["read", "write", "numa", "render", "gate"])
def test_failed_rebuild_preserves_every_published_byte(tmp_path, monkeypatch, failure):
    manifest, output = make_case(tmp_path)
    pipeline.run_pipeline(manifest, output, False)
    old = current_publication(output)
    old_files = snapshot(old["directory"])
    old_entry = (output / "index.html").read_bytes()
    config = json.loads(manifest.read_text())
    config["runs"][0]["deadline_ms"] = 17
    manifest.write_text(json.dumps(config))
    if failure in ("read", "write", "numa"):
        producer = {"write": "run_write_from_evidence", "numa": "run_numa_from_evidence"}.get(
            failure, "run_" + failure)
        monkeypatch.setattr(pipeline.stages, producer,
                            Mock(side_effect=RuntimeError("injected producer failure")))
    elif failure == "render":
        monkeypatch.setattr(render_bundle, "render_run", Mock(side_effect=RuntimeError("injected render failure")))
    else:
        monkeypatch.setattr(pipeline, "validate_suite", Mock(return_value={"valid": False, "errors": ["injected gate"]}))
    with pytest.raises(RuntimeError):
        pipeline.run_pipeline(manifest, output, True, resume=True)
    assert (output / "index.html").read_bytes() == old_entry
    assert current_publication(output)["directory"] == old["directory"]
    assert_unchanged(old["directory"], old_files)


def test_repaint_publishes_new_view_without_changing_model_generations(tmp_path, monkeypatch):
    manifest, output = make_case(tmp_path)
    first = pipeline.run_pipeline(manifest, output, False)
    old = current_publication(output)
    old_files = snapshot(old["directory"])
    model_paths = [output / first["runs"][0][field] for field in
                   ("triage_json", "analysis_json", "write_analysis_json", "numa_analysis_json")]
    models = {path: path.read_bytes() for path in model_paths}
    for stage in ("run_triage", "run_read", "run_write", "run_numa_from_evidence"):
        monkeypatch.setattr(pipeline.stages, stage, Mock(side_effect=AssertionError("render-only parsed inputs")))
    result = render_bundle.render_bundle(output, jobs=2)
    current = current_publication(output)
    assert current["directory"] != old["directory"]
    assert Path(result["index"]) == current["index"]
    assert all(path.read_bytes() == content for path, content in models.items())
    assert_unchanged(old["directory"], old_files)
    with pytest.raises(ValueError, match="immutable"):
        config = json.loads(current["manifest"].read_text())["runs"][0]
        render_bundle.render_run(config, current["directory"])


def test_warm_resume_reuses_verified_publication(tmp_path):
    manifest, output = make_case(tmp_path)
    first = pipeline.run_pipeline(manifest, output, False)
    original = (output / "index.html").read_bytes()
    second = pipeline.run_pipeline(manifest, output, False, resume=True)
    assert first["index"] == second["index"]
    assert (output / "index.html").read_bytes() == original
    assert all(item["status"] == "hit" for item in second["validation"]["runs"]["case"]["cache"].values())


def test_published_manifest_is_self_contained_and_legacy_alias_resolves(tmp_path):
    manifest, output = make_case(tmp_path)
    result = pipeline.run_pipeline(manifest, output, False)
    current = current_publication(output)
    generation = current["directory"]
    run = json.loads(current["manifest"].read_text())["runs"][0]
    alias = json.loads((output / "suite.manifest.json").read_text())["runs"][0]
    for field in ("input_archive", "triage_json", "analysis_json", "write_analysis_json", "numa_analysis_json",
                  "issues_analysis_json",
                  "triage_report", "bottleneck_report", "write_bottleneck_report", "numa_report"):
        target = (generation / run[field]).resolve()
        assert target.is_relative_to(generation)
        assert target.is_file()
        assert (output / alias[field]).resolve() == target
    assert set(run["stage_provenance"]) == {"triage", "evidence", "read", "write", "numa", "issues"}
    for stage, value in run["stage_provenance"].items():
        record = json.loads((generation / value).read_text())
        assert record["schema_version"] == 1 and record["stage"] == stage
        assert isinstance(record["producer"]["revision"], str)
        assert len(record["producer"]["tool_fingerprint"]) == 64
        assert (output / alias["stage_provenance"][stage]).resolve() == generation / value
    assert Path(result["stable_index"]) == output / "index.html"


def test_render_only_accepts_legacy_manifest_without_triage_json(tmp_path):
    import shutil

    manifest, output = make_case(tmp_path)
    pipeline.run_pipeline(manifest, output, False)
    published = current_publication(output)
    legacy = tmp_path / "legacy"
    shutil.copytree(published["directory"], legacy)
    (legacy / "publication.json").unlink()
    manifest_path = legacy / "suite.manifest.json"
    config = json.loads(manifest_path.read_text())
    del config["runs"][0]["triage_json"]
    manifest_path.write_text(json.dumps(config))
    source_model = legacy / config["runs"][0]["analysis_json"]
    before = source_model.read_bytes()
    result = render_bundle.render_bundle(legacy)
    assert result["valid"]
    assert source_model.read_bytes() == before
    assert (legacy / result["runs"][0]["triage_report"]).is_file()


def test_render_only_rejects_legacy_write_phase_model_before_copying(tmp_path):
    import shutil

    manifest, output = make_case(tmp_path)
    pipeline.run_pipeline(manifest, output, False)
    published = current_publication(output)
    legacy = tmp_path / "legacy-write"
    shutil.copytree(published["directory"], legacy)
    (legacy / "publication.json").unlink()
    config = json.loads((legacy / "suite.manifest.json").read_text())["runs"][0]
    write_path = legacy / config["write_analysis_json"]
    model = json.loads(write_path.read_text())
    model.pop("write_phase_schema_version")
    write_path.write_text(json.dumps(model))
    with pytest.raises(ValueError, match="write phase schema 2.*rebuild"):
        render_bundle.render_bundle(legacy)
    assert not (legacy / "publications").exists()
    assert json.loads((legacy / "render.validation.json").read_text())["status"] == "failed"
