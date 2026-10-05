"""Intermediate Trace artifacts belong to model-cache dependencies, not rendering."""
from trace_test_loader import REPO_ROOT
import ast
import copy
import importlib
from pathlib import Path
import sys

ROOT = REPO_ROOT
sys.path.insert(0, str(ROOT / "scripts"))


def test_triage_artifacts_preserve_legacy_event_fields_and_input():
    artifacts = importlib.import_module("trace_analysis.analysis.triage_artifacts")
    rendering = importlib.import_module("trace_analysis.rendering.triage")
    report = {"traces": {
        "t1": {"classification": "slow", "workers": {"worker1": 2},
               "evidence": [{"text": "2026-09-30T01:02:03 | ERROR", "source": "bundle",
                             "member": "worker.log", "line": 5}],
               "ub_events": [{"event_type": "total", "request_id": "7", "cost_ms": 2.635}]},
        "t2": {"classification": "slow"},
        "t3": {"classification": "missing_worker"},
    }}
    before = copy.deepcopy(report)
    events = [
        {"schema_version": 1, "trace_id": "t1", "ts": "2026-09-30T01:02:03",
         "worker": "worker1", "event_type": "raw", "source": "bundle", "member": "worker.log",
         "line": 5, "raw": "2026-09-30T01:02:03 | ERROR"},
        {"schema_version": 1, "trace_id": "t1", "event_type": "ub_total",
         "request_id": "7", "cost_ms": 2.635},
    ]
    triage = {"schema_version": 1, "root_cause_families": {"slow": 2, "missing_worker": 1},
              "issue_candidates": [
                  {"classification": "slow", "trace_count": 2, "representative_traces": ["t1", "t2"],
                   "evidence_boundary": "observed"},
                  {"classification": "missing_worker", "trace_count": 1,
                   "representative_traces": ["t3"], "evidence_boundary": "observed"},
              ]}
    actual = artifacts.build_events(report)
    assert actual == rendering.TraceReportRenderer.events(report)
    assert all(event["schema_version"] == 2 for event in actual)
    assert actual[0]["worker"] is None
    for event, legacy in zip(actual, events):
        assert {key: event[key] for key in legacy if key not in {"schema_version", "worker"}} == {
            key: value for key, value in legacy.items() if key not in {"schema_version", "worker"}
        }
    assert artifacts.build_triage(report) == triage == rendering.TraceReportRenderer.triage(report)
    assert rendering.build_events is artifacts.build_events
    assert rendering.build_triage is artifacts.build_triage
    assert report == before
    tree = ast.parse(Path(artifacts.__file__).read_text())
    imports = {node.module for node in ast.walk(tree) if isinstance(node, ast.ImportFrom) and node.level}
    assert imports == {"ingest.triage"}


def test_triage_model_cache_tracks_artifacts_but_not_render_changes(tmp_path):
    from trace_analysis import stage_versions
    for stage, dependencies in stage_versions.DEPENDENCIES.items():
        for name in (*dependencies, "stages.py", "stage_versions.py", "validation.py"):
            path = tmp_path / name
            if (stage_versions.PACKAGE_ROOT / name).is_dir():
                path = path / "fixture.py"
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text("baseline\n")
    artifact = tmp_path / "analysis/triage_artifacts.py"
    artifact.parent.mkdir(parents=True, exist_ok=True)
    artifact.write_text("artifact v1\n")
    versions = {stage: stage_versions.model_version(stage, tmp_path) for stage in stage_versions.DEPENDENCIES}
    artifact.write_text("artifact v2\n")
    assert stage_versions.model_version("triage", tmp_path) != versions["triage"]
    for stage in ("read", "write", "numa"):
        assert stage_versions.model_version(stage, tmp_path) == versions[stage]
    changed = stage_versions.model_version("triage", tmp_path)
    html_renderer = tmp_path / "rendering/triage.py"
    html_renderer.parent.mkdir(exist_ok=True)
    html_renderer.write_text("presentation only\n")
    assert stage_versions.model_version("triage", tmp_path) == changed


def test_publishing_changes_do_not_invalidate_parsed_models(tmp_path):
    import shutil
    from trace_analysis import stage_versions
    shutil.copytree(stage_versions.PACKAGE_ROOT, tmp_path / "package",
                    ignore=shutil.ignore_patterns("__pycache__", "assets"))
    package = tmp_path / "package"
    before = stage_versions.model_version("triage", package)
    (package / "orchestration/publication.py").write_text("publication changed\n")
    (package / "orchestration/bundle.py").write_text("bundle changed\n")
    assert stage_versions.model_version("triage", package) == before
    (package / "orchestration/store.py").write_text("storage contract changed\n")
    assert stage_versions.model_version("triage", package) != before


def test_triage_dimension_modules_invalidate_model_cache(tmp_path):
    import shutil
    from trace_analysis import stage_versions
    shutil.copytree(stage_versions.PACKAGE_ROOT, tmp_path / "package",
                    ignore=shutil.ignore_patterns("__pycache__", "assets"))
    package = tmp_path / "package"
    before = stage_versions.model_version("triage", package)
    for name in ("triage_accumulator", "triage_builder", "triage_dimensions", "triage_flow",
                 "triage_stats", "triage_ub"):
        module = package / "analysis" / f"{name}.py"
        original = module.read_text()
        module.write_text(original + "\nCACHE_SEMANTIC_CHANGE = True\n")
        assert stage_versions.model_version("triage", package) != before
        module.write_text(original)
