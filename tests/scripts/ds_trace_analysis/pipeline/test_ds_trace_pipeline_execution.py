"""Pipeline admission bounds all Runs and allows independent Evidence consumers to overlap."""
from trace_test_loader import REPO_ROOT
import json
import os
from pathlib import Path
import subprocess
import sys
import threading
import time
from contextlib import contextmanager
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import Mock

import pytest

from test_ds_trace_stage_resume import make_case
from trace_analysis import pipeline, render_bundle
from trace_analysis.cached_stages import CachedStages
from trace_analysis.execution_budget import ResourceBudget
from trace_analysis.stages import StageResult


STAGE_ESTIMATES = {name: 10 for name in ("triage", "evidence", "read", "write", "numa", "issues", "render", "suite")}


def test_pipeline_and_render_only_accept_jobs_above_eight(tmp_path):
    manifest, output = make_case(tmp_path)
    result = pipeline.run_pipeline(manifest, output, False, jobs=16)
    assert result["validation"]["execution"]["slots"] == 16
    assert result["validation"]["valid"]
    assert render_bundle.render_bundle(output, jobs=16)["valid"]


def test_pipeline_records_auto_resource_decision(tmp_path, monkeypatch):
    from trace_analysis import execution_budget
    manifest, output = make_case(tmp_path)
    monkeypatch.setattr(execution_budget, "host_resources", lambda: (80, 100), raising=False)
    config = json.loads(manifest.read_text())
    config["execution"] = {"stage_estimates_mb": STAGE_ESTIMATES}
    manifest.write_text(json.dumps(config))
    result = pipeline.run_pipeline(manifest, output, False, jobs="auto")
    execution = result["validation"]["execution"]
    assert execution["slots"] == 8
    assert execution["memory_mb"] == 80
    assert execution["requested_jobs"] == "auto"
    rendered = render_bundle.render_bundle(output, jobs="auto")
    assert rendered["execution"]["slots"] == 8
    assert rendered["execution"]["memory_mb"] == 80


def test_read_write_and_numa_overlap_after_evidence_with_jobs_three(tmp_path, monkeypatch):
    manifest, output = make_case(tmp_path)
    rendezvous = threading.Barrier(3)
    for name in ("run_read", "run_write_from_evidence", "run_numa_from_evidence"):
        original = getattr(pipeline.stages, name)
        def wrapped(*args, _original=original, **kwargs):
            rendezvous.wait(timeout=3)
            return _original(*args, **kwargs)
        monkeypatch.setattr(pipeline.stages, name, wrapped)
    result = pipeline.run_pipeline(manifest, output, False, jobs=3)
    assert result["validation"]["valid"]
    execution = json.loads((output / "runs/case/stage.execution.json").read_text())
    assert set(execution) == {"triage", "evidence", "read", "write", "numa", "issues"}
    assert all(value["status"] == "miss" for value in execution.values())


@pytest.mark.parametrize("memory_mb,expected_peak", [(None, 2), (10, 1)])
def test_shared_budget_limits_all_runs(tmp_path, monkeypatch, memory_mb, expected_peak):
    manifest, output = make_case(tmp_path)
    config = json.loads(manifest.read_text())
    config["runs"].append({**config["runs"][0], "id": "second"})
    manifest.write_text(json.dumps(config))
    guard = threading.Lock()
    active, peak = 0, 0
    first_two = threading.Barrier(2) if memory_mb is None else None
    for name in ("run_triage", "run_evidence", "run_read", "run_write",
                 "run_numa_from_evidence", "run_suite"):
        original = getattr(pipeline.stages, name)
        def wrapped(*args, _original=original, _name=name, **kwargs):
            nonlocal active, peak
            with guard:
                active += 1
                peak = max(peak, active)
            try:
                if first_two and _name == "run_triage":
                    first_two.wait(timeout=3)
                time.sleep(0.01)
                return _original(*args, **kwargs)
            finally:
                with guard:
                    active -= 1
        monkeypatch.setattr(pipeline.stages, name, wrapped)
    result = pipeline.run_pipeline(manifest, output, False, jobs=2,
                                   memory_mb=memory_mb, stage_estimates_mb=STAGE_ESTIMATES)
    assert result["validation"]["valid"]
    assert peak == expected_peak
    assert active == 0


def test_memory_budget_requires_complete_estimates_before_production(tmp_path, monkeypatch):
    manifest, output = make_case(tmp_path)
    producer = Mock(side_effect=AssertionError("must reject before producing"))
    monkeypatch.setattr(pipeline.stages, "run_triage", producer)
    with pytest.raises(ValueError, match="estimate"):
        pipeline.run_pipeline(manifest, output, False, memory_mb=100,
                              stage_estimates_mb={"triage": 10})
    assert producer.call_count == 0


def test_legacy_memory_estimates_use_read_budget_for_evidence(tmp_path):
    manifest, output = make_case(tmp_path)
    legacy = {key: value for key, value in STAGE_ESTIMATES.items() if key != "evidence"}
    result = pipeline.run_pipeline(manifest, output, False, memory_mb=20, stage_estimates_mb=legacy)
    assert result["validation"]["valid"]
    assert result["validation"]["execution"]["stage_estimates_mb"]["evidence"] == legacy["read"]


def test_restore_validation_render_and_suite_are_admitted_on_resume(tmp_path, monkeypatch):
    manifest, output = make_case(tmp_path)
    monkeypatch.setattr(pipeline, "tool_fingerprint", lambda: "before-style")
    pipeline.run_pipeline(manifest, output, False)
    monkeypatch.setattr(pipeline, "tool_fingerprint", lambda: "after-style")
    local = threading.local()
    class ObservedBudget(ResourceBudget):
        @contextmanager
        def acquire(self, estimated_mb=None):
            with super().acquire(estimated_mb):
                assert not getattr(local, "admitted", False), "nested stage admission deadlocks"
                local.admitted = True
                try:
                    yield
                finally:
                    local.admitted = False
    monkeypatch.setattr(pipeline, "ResourceBudget", ObservedBudget)
    visited = []
    from trace_analysis.stage_cache import StageCache
    original_run = CachedStages.run
    def checked_run(self, *args, **kwargs):
        restore = kwargs.get("restore_validation")
        if restore is not None:
            def checked_restore(report):
                assert getattr(local, "admitted", False), "receipt restore bypassed admission"
                visited.append("restore_validation")
                return restore(report)
            kwargs["restore_validation"] = checked_restore
        return original_run(self, *args, **kwargs)
    monkeypatch.setattr(CachedStages, "run", checked_run)
    for owner, name in ((pipeline, "validate"), (pipeline, "validate_write_evidence_model"),
                        (render_bundle, "render_run"), (pipeline.stages, "run_suite"), (StageCache, "lookup")):
        original = getattr(owner, name)
        def checked(*args, _original=original, _name=name, **kwargs):
            assert _name not in {"validate", "validate_write_evidence_model"}, "verified receipt reran semantic validation"
            assert getattr(local, "admitted", False), f"{_name} bypassed admission"
            visited.append(_name)
            return _original(*args, **kwargs)
        monkeypatch.setattr(owner, name, checked)
    result = pipeline.run_pipeline(manifest, output, False, jobs=2, resume=True)
    assert result["validation"]["valid"]
    assert set(visited) == {"restore_validation", "lookup", "render_run", "run_suite"}


def test_same_run_parallel_stage_diagnostics_survive(tmp_path):
    cache = CachedStages(tmp_path, "run", False, budget=ResourceBudget(2))
    root = tmp_path / "runs/run"
    root.mkdir(parents=True)
    rendezvous = threading.Barrier(2)
    def run(stage):
        model = root / (stage + ".json")
        def produce():
            model.write_text("{}")
            rendezvous.wait(timeout=3)
            return StageResult({"analysis_json": model})
        return cache.run(stage, {}, {}, produce, lambda result: None, lambda paths: None)
    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = [pool.submit(run, stage) for stage in ("write", "numa")]
        for future in futures:
            future.result(timeout=5)
    record = json.loads((root / "stage.execution.json").read_text())
    assert set(record) == {"write", "numa"}
    assert all(item["status"] == "miss" for item in record.values())


def test_process_executor_generates_and_validates_two_runs(tmp_path):
    manifest, output = make_case(tmp_path)
    config = json.loads(manifest.read_text())
    config["runs"].append({**config["runs"][0], "id": "second"})
    manifest.write_text(json.dumps(config))
    result = pipeline.run_pipeline(manifest, output, False, jobs=2, run_executor="process")
    assert result["validation"]["valid"]
    assert result["validation"]["execution"]["run_workers"] == 2
    assert {run["id"] for run in result["runs"]} == {"case", "second"}


def test_process_executor_respects_declared_global_memory(tmp_path):
    manifest, output = make_case(tmp_path)
    config = json.loads(manifest.read_text())
    config["runs"].append({**config["runs"][0], "id": "second"})
    manifest.write_text(json.dumps(config))
    result = pipeline.run_pipeline(manifest, output, False, jobs=2, memory_mb=10,
                                   stage_estimates_mb=STAGE_ESTIMATES, run_executor="process")
    assert result["validation"]["valid"]
    assert result["validation"]["execution"]["run_workers"] == 1


def test_process_executor_supports_spawn_start_method(tmp_path):
    manifest, output = make_case(tmp_path)
    config = json.loads(manifest.read_text())
    config["runs"].append({**config["runs"][0], "id": "second"})
    manifest.write_text(json.dumps(config))
    script = tmp_path / "spawn_pipeline.py"
    script.write_text(
        "import multiprocessing\n"
        "from pathlib import Path\n"
        "from trace_analysis.pipeline import run_pipeline\n"
        "if __name__ == '__main__':\n"
        "    multiprocessing.set_start_method('spawn')\n"
        f"    result = run_pipeline(Path({str(manifest)!r}), Path({str(output)!r}), "
        "False, jobs=2, run_executor='process')\n"
        "    assert result['validation']['valid']\n"
    )
    scripts = REPO_ROOT / "scripts"
    environment = {**os.environ, "PYTHONPATH": str(scripts)}
    result = subprocess.run([sys.executable, str(script)], cwd=tmp_path,
                            env=environment, capture_output=True, text=True, timeout=60)
    assert result.returncode == 0, result.stderr
