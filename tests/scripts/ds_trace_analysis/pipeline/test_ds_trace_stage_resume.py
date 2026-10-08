"""A presentation change must reuse validated analysis stages."""
from trace_test_loader import REPO_ROOT
import json
import io
import tarfile
import sys
from pathlib import Path
from unittest.mock import Mock

import pytest

sys.path.insert(0, str(REPO_ROOT / "scripts"))
from trace_analysis import pipeline, triage


def make_case(tmp_path):
    archive = tmp_path / "input.tar.gz"
    triage._make_self_test_bundle(archive)
    with tarfile.open(archive) as source:
        payload = source.extractfile(source.getmembers()[0]).read()
    with tarfile.open(archive, "w:gz") as target:
        member = tarfile.TarInfo("time-buckets/GET_20000/019f7b27-56f0-74f0-9a68-5b3742f11e23")
        member.size = len(payload)
        target.addfile(member, io.BytesIO(payload))
    manifest = tmp_path / "manifest.json"
    manifest.write_text(json.dumps({"schema_version": 1, "source_head": "head", "source_base": "base",
                                    "runs": [{"id": "case", "inputs": [str(archive)],
                                              "input_archive": str(archive)}]}))
    output = tmp_path / "report"
    return manifest, output


def test_resume_repaints_without_running_analysis(tmp_path, monkeypatch):
    manifest, output = make_case(tmp_path)
    monkeypatch.setattr(pipeline, "tool_fingerprint", lambda: "style-v1")
    first = pipeline.run_pipeline(manifest, output, False)
    model_paths = [output / first['runs'][0][k] for k in
                   ('triage_json', 'evidence_json', 'analysis_json', 'write_analysis_json', 'numa_analysis_json')]
    original = [p.read_bytes() for p in model_paths]
    monkeypatch.setattr(pipeline, "tool_fingerprint", lambda: "style-v2")

    def forbidden(*args, **kwargs):
        raise AssertionError("cached stage must not execute analysis")

    for name in ('run_triage', 'run_evidence', 'run_read', 'run_write_from_evidence',
                 'run_numa_from_evidence'):
        monkeypatch.setattr(pipeline.stages, name, forbidden)
    second = pipeline.run_pipeline(manifest, output, False, resume=True)
    assert second['validation']['valid']
    assert [p.read_bytes() for p in model_paths] == original
    cache = second['validation']['runs']['case']['cache']
    assert all(cache[s]['status'] == 'hit' for s in ('triage', 'evidence', 'read', 'write', 'numa'))


def stage_spies(monkeypatch):
    spies = {}
    for stage in ("triage", "evidence", "read", "write", "numa"):
        name = {"write": "run_write_from_evidence", "numa": "run_numa_from_evidence"}.get(
            stage, "run_" + stage)
        spies[stage] = Mock(wraps=getattr(pipeline.stages, name))
        monkeypatch.setattr(pipeline.stages, name, spies[stage])
    return spies


def test_read_config_change_reuses_triage_and_rebuilds_read(tmp_path, monkeypatch):
    manifest, output = make_case(tmp_path)
    pipeline.run_pipeline(manifest, output, False)
    config = json.loads(manifest.read_text())
    config["runs"][0]["deadline_ms"] = 17
    manifest.write_text(json.dumps(config))
    spies = stage_spies(monkeypatch)
    resumed = pipeline.run_pipeline(manifest, output, False, resume=True)
    assert resumed["validation"]["valid"]
    assert spies["triage"].call_count == 0
    assert spies["read"].call_count == 1
    cache = resumed["validation"]["runs"]["case"]["cache"]
    assert cache["triage"]["status"] == "hit"
    assert cache["read"]["reason"] == "key_changed"
    assert spies["write"].call_count == 0
    assert cache["write"]["status"] == "hit"
    assert spies["numa"].call_count == 0
    assert cache["numa"]["status"] == "hit"


@pytest.mark.parametrize("failed_stage", ["write", "numa"])
def test_failed_stage_resume_preserves_successful_upstream(tmp_path, monkeypatch, failed_stage):
    manifest, output = make_case(tmp_path)
    name = ("run_write_from_evidence" if failed_stage == "write" else "run_numa_from_evidence")
    original = getattr(pipeline.stages, name)
    monkeypatch.setattr(pipeline.stages, name, Mock(side_effect=RuntimeError("injected stage failure")))
    with pytest.raises(RuntimeError, match="failed Runs: case"):
        pipeline.run_pipeline(manifest, output, False)
    execution = json.loads((output / "runs/case/stage.execution.json").read_text())
    assert execution[failed_stage]["status"] == "failed"
    assert "injected stage failure" in execution[failed_stage]["error"]
    validation = json.loads((output / "pipeline.validation.json").read_text())
    assert validation["valid"] is False
    assert f"stage {failed_stage}" in validation["runs"]["case"]["error"]
    monkeypatch.setattr(pipeline.stages, name, original)
    spies = stage_spies(monkeypatch)
    resumed = pipeline.run_pipeline(manifest, output, False, resume=True)
    cache = resumed["validation"]["runs"]["case"]["cache"]
    assert spies["triage"].call_count == spies["read"].call_count == 0
    assert cache["triage"]["status"] == cache["read"]["status"] == "hit"
    assert spies[failed_stage].call_count == 1
    if failed_stage == "numa":
        assert spies["write"].call_count == 0
        assert cache["write"]["status"] == "hit"
    assert resumed["validation"]["valid"]


def test_tampered_read_model_rebuilds_read_not_triage(tmp_path, monkeypatch):
    manifest, output = make_case(tmp_path)
    initial = pipeline.run_pipeline(manifest, output, False)
    model = output / initial["runs"][0]["analysis_json"]
    original = model.read_bytes()
    model.write_text("{broken")
    spies = stage_spies(monkeypatch)
    resumed = pipeline.run_pipeline(manifest, output, False, resume=True)
    assert spies["triage"].call_count == 0
    assert spies["read"].call_count == 1
    restored = output / resumed["runs"][0]["analysis_json"]
    assert restored != model
    assert restored.read_bytes() == original
    assert model.read_text() == "{broken"
    cache = resumed["validation"]["runs"]["case"]["cache"]
    assert cache["read"]["reason"] == "artifact_changed"
    assert cache["write"]["status"] == cache["numa"]["status"] == "hit"


def test_missing_triage_intermediate_is_repaired_even_when_top_level_artifacts_match(tmp_path, monkeypatch):
    manifest, output = make_case(tmp_path)
    initial = pipeline.run_pipeline(manifest, output, False)
    run_dir = (output / initial["runs"][0]["triage_json"]).parent
    parsed = run_dir / "parsed_traces.json"
    original = parsed.read_bytes()
    parsed.unlink()
    spies = stage_spies(monkeypatch)
    resumed = pipeline.run_pipeline(manifest, output, False, resume=True)
    assert spies["triage"].call_count == 1
    repaired_dir = (output / resumed["runs"][0]["triage_json"]).parent
    assert (repaired_dir / "parsed_traces.json").read_bytes() == original
    assert resumed["validation"]["runs"]["case"]["cache"]["triage"]["reason"] == "artifact_missing"


def test_input_inventory_matches_manifest_and_is_rebuilt_when_changed(tmp_path, monkeypatch):
    from trace_analysis.validation import validate_input_inventory

    manifest, output = make_case(tmp_path)
    initial = pipeline.run_pipeline(manifest, output, False)
    run_dir = (output / initial["runs"][0]["triage_json"]).parent
    inventory_path = run_dir / "inventory.json"
    inventory = json.loads(inventory_path.read_text())
    triage_manifest = json.loads((run_dir / "manifest.json").read_text())
    assert inventory["schema_version"] == 1
    assert inventory["run_id"] == "case"
    assert inventory["inputs"] == triage_manifest["inputs"]
    assert inventory["input_count"] == len(inventory["inputs"])
    assert inventory["listed_member_count"] == sum(len(item["members"]) for item in inventory["inputs"])
    assert validate_input_inventory(run_dir)["valid"]
    changed = {**inventory, "listed_member_count": inventory["listed_member_count"] + 1}
    inventory_path.write_text(json.dumps(changed))
    assert any("member count mismatch" in error for error in validate_input_inventory(run_dir)["errors"])
    inventory_path.write_text(json.dumps({**inventory, "schema_version": True}))
    assert "unsupported input inventory schema" in validate_input_inventory(run_dir)["errors"]
    inventory_path.write_text('{"broken":true}')
    spies = stage_spies(monkeypatch)
    resumed = pipeline.run_pipeline(manifest, output, False, resume=True)
    assert spies["triage"].call_count == 1
    repaired_dir = (output / resumed["runs"][0]["triage_json"]).parent
    assert json.loads((repaired_dir / "inventory.json").read_text()) == inventory
    assert resumed["validation"]["runs"]["case"]["cache"]["triage"]["reason"] == "artifact_changed"


def test_pr_provenance_change_rebuilds_numa(tmp_path, monkeypatch):
    manifest, output = make_case(tmp_path)
    pipeline.run_pipeline(manifest, output, False)
    config = json.loads(manifest.read_text())
    config["pr"] = 2485
    manifest.write_text(json.dumps(config))
    spies = stage_spies(monkeypatch)
    resumed = pipeline.run_pipeline(manifest, output, False, resume=True)
    assert spies["numa"].call_count == 1
    assert spies["triage"].call_count == spies["read"].call_count == 0
    assert resumed["validation"]["runs"]["case"]["cache"]["numa"]["reason"] == "key_changed"


def test_unchanged_resume_reports_current_hits_without_rewriting_checkpoint(tmp_path, monkeypatch):
    manifest, output = make_case(tmp_path)
    pipeline.run_pipeline(manifest, output, False)
    checkpoint = output / "runs/case/pipeline.checkpoint.json"
    original = checkpoint.read_bytes()
    spies = stage_spies(monkeypatch)
    resumed = pipeline.run_pipeline(manifest, output, False, resume=True)
    assert all(spy.call_count == 0 for spy in spies.values())
    assert checkpoint.read_bytes() == original
    cache = resumed["validation"]["runs"]["case"]["cache"]
    assert all(item["status"] == "hit" for item in cache.values())
    assert all(item["reason"] == "verified" for item in cache.values())


@pytest.mark.parametrize("payload", ["[]", "null", "{broken"])
def test_malformed_legacy_checkpoint_does_not_block_verified_stage_resume(tmp_path, monkeypatch, payload):
    manifest, output = make_case(tmp_path)
    pipeline.run_pipeline(manifest, output, False)
    (output / "runs/case/pipeline.checkpoint.json").write_text(payload)
    spies = stage_spies(monkeypatch)
    resumed = pipeline.run_pipeline(manifest, output, False, resume=True)
    assert resumed["validation"]["valid"]
    assert all(spy.call_count == 0 for spy in spies.values())
