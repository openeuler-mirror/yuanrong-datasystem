"""Package stages preserve artifact and option contracts without CLI state."""
from trace_test_loader import REPO_ROOT
import importlib
import sys
from dataclasses import FrozenInstanceError
from pathlib import Path
from unittest.mock import Mock

import pytest

sys.path.insert(0, str(REPO_ROOT / "scripts"))
stages = importlib.import_module("trace_analysis.stages")


def test_triage_returns_paths_without_stdout_or_process_state(tmp_path, monkeypatch):
    source = tmp_path / "input"
    source.mkdir()
    run_dir = tmp_path / "triage" / "generated"
    run_dir.mkdir(parents=True)
    (run_dir / "summary.json").write_text("{}")
    runner = Mock()
    runner.run.return_value = run_dir
    monkeypatch.setattr(stages.triage, "TraceRunPipeline", lambda: runner)
    before = (sys.argv[:], Path.cwd())
    result = stages.run_triage({"id": "r1", "inputs": [str(source)], "allow_partial_inputs": True},
                               {"source_head": "head"}, tmp_path, True)
    assert result.artifacts["run_dir"] == run_dir
    assert result.artifacts["summary_json"] == run_dir / "summary.json"
    options = runner.run.call_args.kwargs["options"]
    assert (options.code_ref, options.force, options.allow_partial_inputs) == ("head", True, True)
    assert before == (sys.argv, Path.cwd())
    with pytest.raises(FrozenInstanceError):
        result.artifacts = {}
    with pytest.raises(TypeError):
        result.artifacts["invalid"] = tmp_path


def test_read_preserves_options_and_disables_write_companion(tmp_path, monkeypatch):
    analysis = {"metadata": {"case": "case"}, "trace_count": 7}
    build, write = Mock(return_value=analysis), Mock()
    monkeypatch.setattr(stages.bottleneck, "build_analysis", build)
    monkeypatch.setattr(stages.bottleneck, "write_outputs", write)
    config = {"top": 100, "local_cache": False, "deadline_ms": 5, "read_path": "legacy-worker-pull"}
    result = stages.run_read(tmp_path / "triage", config, {"source_ref": "ref"}, tmp_path, True)
    assert build.call_args.kwargs == {"top_n": 100, "deadline_ms": 5, "local_cache": False,
                                     "read_path": "legacy-worker-pull", "source_ref": "ref"}
    assert write.call_args.kwargs["write_companion"] is False
    assert write.call_args.kwargs["force"] is True
    assert write.call_args.kwargs["title"] == "case · Top7 关键瓶颈"
    assert result.artifacts["analysis_json"].name == "bottleneck.analysis.json"


def test_write_passes_relative_links_to_shared_output_api(tmp_path, monkeypatch):
    write = Mock()
    monkeypatch.setattr(stages.write_report, "write_outputs", write)
    result = stages.run_write(tmp_path / "bottleneck.analysis.json", tmp_path / "triage/run", tmp_path)
    assert dict(write.call_args.kwargs["links"]) == {
        "读取瓶颈": "bottleneck.local.html", "Trace Triage": "triage/run/report.local.html"}
    assert result.artifacts["analysis_json"].name == "write.refined.analysis.json"


def test_numa_preserves_provenance_and_runtime_options(tmp_path, monkeypatch):
    build, write = Mock(return_value={"traces": []}), Mock()
    monkeypatch.setattr(stages.numa, "build_analysis", build)
    monkeypatch.setattr(stages.numa, "write_outputs", write)
    stages.run_numa(tmp_path / "triage", tmp_path / "read.json", tmp_path / "input.tar",
                    {"client_count": 6}, {"source_head": "h", "source_base": "b", "pr": 2485}, tmp_path, False)
    assert build.call_args.args[3] == {"head": "h", "base": "b", "pr": 2485}
    assert build.call_args.args[4]["client_count"] == 6
    assert write.call_args.kwargs["force"] is False


def test_numa_evidence_stage_has_no_read_model_input(tmp_path, monkeypatch):
    build = Mock(return_value={"traces": []})
    monkeypatch.setattr(stages.numa, "build_analysis", build)
    result = stages.run_numa_from_evidence(stages.NumaEvidenceSpec(
        tmp_path / "triage", tmp_path / "evidence.json", tmp_path / "input.tar",
        {}, {"source_head": "h"}, tmp_path, models_only=True))
    assert build.call_args.args[1] is None
    assert build.call_args.kwargs["evidence_path"] == tmp_path / "evidence.json"
    assert result.artifacts["analysis_json"].is_file()


def test_suite_preserves_canonical_outputs(tmp_path, monkeypatch):
    monkeypatch.setattr(stages.suite, "load_manifest", Mock(return_value={"runs": []}))
    monkeypatch.setattr(stages.suite, "build_suite", Mock(return_value={"runs": []}))
    write = Mock()
    monkeypatch.setattr(stages.suite, "write_outputs", write)
    result = stages.run_suite(tmp_path / "suite.manifest.json", tmp_path, True)
    assert result.artifacts == {"analysis_json": tmp_path / "suite.analysis.json", "report_html": tmp_path / "index.html"}
    assert write.call_args.kwargs["force"] is True


def test_native_stages_produce_same_write_bytes_as_legacy_cli(tmp_path):
    import json
    import subprocess

    archive = tmp_path / "input.tar.gz"
    stages.triage._make_self_test_bundle(archive)
    config = {"id": "native", "inputs": [str(archive)]}
    manifest = {"source_head": "fixture-head", "source_base": "fixture-base"}
    result = stages.run_triage(config, manifest, tmp_path, False)
    run_dir = result.artifacts["run_dir"]
    read = stages.run_read(run_dir, config, manifest, tmp_path, False)
    write = stages.run_write(read.artifacts["analysis_json"], run_dir, tmp_path)
    numa = stages.run_numa(run_dir, read.artifacts["analysis_json"], archive, config, manifest, tmp_path, False)
    for stage in (result, read, write, numa):
        assert all(path.exists() for path in stage.artifacts.values())
    before_html = write.artifacts["report_html"].read_bytes()
    before_json = write.artifacts["analysis_json"].read_bytes()
    relative_triage = (run_dir / "report.local.html").relative_to(tmp_path).as_posix()
    script = REPO_ROOT / "scripts/ds_trace_analysis.py"
    completed = subprocess.run([sys.executable, str(script), "write", "--analysis-json", str(read.artifacts["analysis_json"]),
                                "--output", str(write.artifacts["report_html"]), "--read-report", "bottleneck.local.html",
                                "--triage-report", relative_triage], cwd=tmp_path, capture_output=True, text=True)
    assert completed.returncode == 0, completed.stderr
    assert before_html == write.artifacts["report_html"].read_bytes()
    assert before_json == write.artifacts["analysis_json"].read_bytes()
    assert json.loads(read.artifacts["analysis_json"].read_text())["trace_count"] > 0
    with pytest.raises(FileExistsError):
        stages.run_read(run_dir, config, manifest, tmp_path, False)
    with pytest.raises(FileExistsError):
        stages.run_numa(run_dir, read.artifacts["analysis_json"], archive, config, manifest, tmp_path, False)


def test_write_output_rejects_input_alias_without_modifying_source(tmp_path):
    source = tmp_path / "source.json"
    source.write_text('{"write_traces": []}')
    with pytest.raises(ValueError, match="distinct"):
        stages.write_report.write_outputs(source, source)
    assert source.read_text() == '{"write_traces": []}'


def test_read_normalizes_numeric_strings_and_rejects_invalid_path(tmp_path, monkeypatch):
    build = Mock(return_value={"metadata": {}, "trace_count": 0})
    monkeypatch.setattr(stages.bottleneck, "build_analysis", build)
    monkeypatch.setattr(stages.bottleneck, "write_outputs", Mock())
    stages.run_read(tmp_path, {"top": "100", "deadline_ms": "5.5"}, {}, tmp_path)
    assert build.call_args.kwargs["top_n"] == 100
    assert build.call_args.kwargs["deadline_ms"] == 5.5
    build.reset_mock()
    for config in ({"top": "invalid"}, {"deadline_ms": "invalid"}, {"read_path": "unsupported"}):
        with pytest.raises(ValueError):
            stages.run_read(tmp_path, config, {}, tmp_path)
    build.assert_not_called()


def test_numa_normalizes_numeric_strings_before_build(tmp_path, monkeypatch):
    build = Mock(return_value={})
    monkeypatch.setattr(stages.numa, "build_analysis", build)
    monkeypatch.setattr(stages.numa, "write_outputs", Mock())
    config = {"qps_per_node": "1.5", "client_count": "6", "threads_per_client": "8", "workers_per_node": "3"}
    stages.run_numa(tmp_path, tmp_path, tmp_path, config, {"pr": "2485"}, tmp_path)
    assert build.call_args.args[3]["pr"] == 2485
    assert build.call_args.args[4] == {"qps_per_node": 1.5, "client_count": 6, "threads_per_client": 8, "workers_per_node": 3}
    build.reset_mock()
    with pytest.raises(ValueError):
        stages.run_numa(tmp_path, tmp_path, tmp_path, {"client_count": "6.5"}, {}, tmp_path)
    build.assert_not_called()
