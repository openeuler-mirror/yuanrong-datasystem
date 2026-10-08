"""Reject invalid multi-Run inputs before any analysis or output mutation."""
import json
import subprocess
import sys
import tarfile
from unittest.mock import Mock

import pytest

from test_ds_trace_stage_resume import make_case
from trace_test_loader import REPO_ROOT
from trace_analysis import pipeline, stages


@pytest.mark.parametrize("bad_field", ["inputs", "input_archive"])
def test_missing_later_run_input_fails_before_first_run(tmp_path, monkeypatch, bad_field):
    manifest, output = make_case(tmp_path)
    data = json.loads(manifest.read_text())
    second = {**data["runs"][0], "id": "second"}
    second[bad_field] = ([str(tmp_path / "missing.tar")] if bad_field == "inputs"
                         else str(tmp_path / "missing.tar"))
    data["runs"].append(second)
    manifest.write_text(json.dumps(data))
    parse = Mock(side_effect=AssertionError("no Run may start"))
    monkeypatch.setattr(stages, "run_triage", parse)

    with pytest.raises(ValueError, match="second.*" + bad_field):
        pipeline.run_pipeline(manifest, output, False, jobs=2)

    parse.assert_not_called()
    assert not output.exists()


def test_corrupt_archive_fails_before_first_run(tmp_path, monkeypatch):
    manifest, output = make_case(tmp_path)
    bad = tmp_path / "bad.tar.gz"
    bad.write_bytes(b"not a tar")
    data = json.loads(manifest.read_text())
    second = {**data["runs"][0], "id": "second", "input_archive": str(bad)}
    data["runs"].append(second)
    manifest.write_text(json.dumps(data))
    parse = Mock(side_effect=AssertionError("no Run may start"))
    monkeypatch.setattr(stages, "run_triage", parse)

    with pytest.raises(ValueError, match="second.*input_archive.*tar"):
        pipeline.run_pipeline(manifest, output, False, jobs=2)

    parse.assert_not_called()
    assert not output.exists()


def test_schema_error_precedes_archive_io(tmp_path, monkeypatch):
    manifest, output = make_case(tmp_path)
    data = json.loads(manifest.read_text())
    data["sampling"] = "500"
    data["runs"][0]["input_archive"] = str(tmp_path / "missing.tar")
    manifest.write_text(json.dumps(data))
    archive_check = Mock(side_effect=AssertionError("archive must not be scanned"))
    monkeypatch.setattr(pipeline, "_validate_archive", archive_check)

    with pytest.raises(ValueError, match="sampling"):
        pipeline.run_pipeline(manifest, output, False)

    archive_check.assert_not_called()
    assert not output.exists()


@pytest.mark.parametrize("pr", ["https://gitcode.com/openeuler/yuanrong-datasystem/pull/2485", True, "24x5"])
def test_invalid_pr_fails_before_archive_io(tmp_path, monkeypatch, pr):
    manifest, output = make_case(tmp_path)
    data = json.loads(manifest.read_text())
    data["pr"] = pr
    manifest.write_text(json.dumps(data))
    archive_check = Mock(side_effect=AssertionError("archive must not be scanned"))
    monkeypatch.setattr(pipeline, "_validate_archive", archive_check)

    with pytest.raises(ValueError, match="manifest pr"):
        pipeline.preflight_pipeline(manifest)

    archive_check.assert_not_called()
    assert not output.exists()


def test_pipeline_rejects_individual_trace_files_as_cohorts(tmp_path, monkeypatch):
    manifest, output = make_case(tmp_path)
    trace = tmp_path / "getBuffer-1-2-00000001;abcdef123456"
    trace.write_text("trace evidence\n")
    data = json.loads(manifest.read_text())
    data["runs"][0]["inputs"] = [str(trace)]
    manifest.write_text(json.dumps(data))
    parse = Mock(side_effect=AssertionError("no Run may start"))
    monkeypatch.setattr(stages, "run_triage", parse)

    with pytest.raises(ValueError, match="directory or tar archive"):
        pipeline.run_pipeline(manifest, output, False)

    parse.assert_not_called()
    assert not output.exists()


def test_preflight_accepts_plain_tar_without_creating_report(tmp_path):
    manifest, output = make_case(tmp_path)
    plain = tmp_path / "input.tar"
    source = json.loads(manifest.read_text())["runs"][0]["input_archive"]
    with tarfile.open(source, "r:gz") as compressed, tarfile.open(plain, "w") as target:
        for member in compressed:
            if member.isfile():
                target.addfile(member, compressed.extractfile(member))
    data = json.loads(manifest.read_text())
    data["runs"][0].update({"inputs": [str(plain)], "input_archive": str(plain)})
    manifest.write_text(json.dumps(data))

    result = pipeline.preflight_pipeline(manifest)

    assert len(result["runs"]) == 1
    assert result["runs"][0]["id"] == "case"
    assert not output.exists()


def test_one_cold_run_records_total_preflight_suite_and_stage_times(tmp_path):
    manifest, output = make_case(tmp_path)

    result = pipeline.run_pipeline(manifest, output, False)

    execution = result["validation"]["execution"]
    for field in ("total_wall_seconds", "preflight_wall_seconds", "suite_wall_seconds"):
        assert isinstance(execution[field], float)
        assert execution[field] >= 0
    assert execution["total_wall_seconds"] >= execution["preflight_wall_seconds"]
    assert execution["total_wall_seconds"] >= execution["suite_wall_seconds"]
    stages = json.loads((output / "runs" / "case" / "stage.execution.json").read_text())
    assert {"triage", "evidence", "read", "write", "numa", "issues"} <= set(stages)
    assert all(item["wall_seconds"] >= 0 for item in stages.values())


def test_cli_preflight_does_not_require_or_create_output(tmp_path):
    manifest, output = make_case(tmp_path)

    completed = subprocess.run(
        [sys.executable, str(REPO_ROOT / "scripts/ds_trace_analysis.py"), "pipeline",
         "--manifest", str(manifest), "--preflight-only"],
        check=True, capture_output=True, text=True, cwd=REPO_ROOT,
    )

    assert json.loads(completed.stdout)["run_count"] == 1
    assert not output.exists()
