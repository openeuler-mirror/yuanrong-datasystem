"""Pipeline stages produce models; publication renders each page exactly once."""
import json
from unittest.mock import Mock

from test_ds_trace_stage_resume import make_case
from trace_analysis import pipeline, stages, triage, bottleneck, write_report, numa


def test_model_only_stages_return_only_existing_models_and_match_legacy_models(tmp_path):
    manifest_path, output = make_case(tmp_path)
    manifest = json.loads(manifest_path.read_text())
    config = manifest["runs"][0]
    root = tmp_path / "models"
    result = stages.run_triage(config, manifest, root, models_only=True)
    run_dir = result.artifacts["run_dir"]
    read = stages.run_read(run_dir, config, manifest, root, models_only=True)
    write = stages.run_write(read.artifacts["analysis_json"], run_dir, root, models_only=True)
    numa_result = stages.run_numa(run_dir, read.artifacts["analysis_json"],
                                 manifest_path.parent / "input.tar.gz", config, manifest, root, models_only=True)
    for stage in (result, read, write, numa_result):
        assert "report_html" not in stage.artifacts
        assert all(path.exists() for path in stage.artifacts.values())
    assert not list(root.rglob("*.html"))
    legacy = tmp_path / "legacy"
    legacy.mkdir()
    legacy_read = stages.run_read(run_dir, config, manifest, legacy)
    legacy_write = stages.run_write(legacy_read.artifacts["analysis_json"], run_dir, legacy)
    legacy_numa = stages.run_numa(run_dir, legacy_read.artifacts["analysis_json"],
                                  manifest_path.parent / "input.tar.gz", config, manifest, legacy)
    for model, rendered in ((read, legacy_read), (write, legacy_write), (numa_result, legacy_numa)):
        assert model.artifacts["analysis_json"].read_bytes() == rendered.artifacts["analysis_json"].read_bytes()
        assert rendered.artifacts["report_html"].is_file()


def test_pipeline_renders_each_page_once_and_css_change_does_not_analyze(tmp_path, monkeypatch):
    manifest, output = make_case(tmp_path)
    counters = {}
    owners = {"triage": (triage, "_render_html"), "read": (bottleneck, "render_html"),
              "write": (write_report, "render_model"), "numa": (numa, "render_html"),
              "overview": (stages.suite.ds_trace_overview, "render")}
    for label, (owner, name) in owners.items():
        counters[label] = Mock(wraps=getattr(owner, name))
        monkeypatch.setattr(owner, name, counters[label])
    first = pipeline.run_pipeline(manifest, output, False)
    assert first["validation"]["valid"]
    assert {name: spy.call_count for name, spy in counters.items()} == {name: 1 for name in counters}
    assert not list((output / "runs/case/generations").rglob("*.html"))
    for spy in counters.values():
        spy.reset_mock()
    for owner, name in ((triage.TraceRunPipeline, "parse"), (bottleneck, "build_analysis"),
                        (write_report, "build_model"), (numa, "build_analysis")):
        monkeypatch.setattr(owner, name, Mock(side_effect=AssertionError("CSS repaint analyzed inputs")))
    monkeypatch.setattr(pipeline, "tool_fingerprint", lambda: "changed-css")
    second = pipeline.run_pipeline(manifest, output, False, resume=True)
    assert second["validation"]["valid"]
    assert {name: spy.call_count for name, spy in counters.items()} == {name: 1 for name in counters}
    assert all(item["status"] == "hit" for item in second["validation"]["runs"]["case"]["cache"].values())
