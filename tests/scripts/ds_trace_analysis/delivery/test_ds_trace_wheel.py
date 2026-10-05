"""The standalone wheel carries the CLI and every offline report asset."""
from trace_test_loader import REPO_ROOT

import os
import json
from pathlib import Path
import subprocess
import sys
import zipfile


ROOT = REPO_ROOT
BUILDER = ROOT / "scripts" / "trace_analysis_dist" / "build_wheel.py"


def test_wheel_installs_and_runs_outside_checkout(tmp_path):
    output = tmp_path / "dist"
    subprocess.run([sys.executable, str(BUILDER), "--output", str(output)],
                   check=True, capture_output=True, text=True, timeout=60)
    wheel, = output.glob("ds_trace_analysis-*.whl")
    with zipfile.ZipFile(wheel) as archive:
        build_info = json.loads(archive.read("trace_analysis/_build_info.json"))
        for relative in ("LICENSE", "NOTICE", "licenses/LICENSE-d3", "provenance.json"):
            assert archive.read("trace_analysis/assets/vendor/echarts/" + relative)
    assert build_info["schema_version"] == 1
    assert isinstance(build_info["revision"], str) and build_info["revision"]
    target = tmp_path / "installed"
    subprocess.run([sys.executable, "-m", "pip", "install", "--no-deps", "--target", str(target),
                    str(wheel)], check=True, capture_output=True, text=True, timeout=60)
    environment = {**os.environ, "PYTHONPATH": str(target)}
    command = target / ("Scripts" if os.name == "nt" else "bin") / "ds-trace-analysis"
    result = subprocess.run([str(command), "pipeline", "--help"], cwd=tmp_path,
                            env=environment, capture_output=True, text=True, timeout=20)
    assert result.returncode == 0, result.stderr
    assert "--render-only" in result.stdout
    assert "--run-executor" in result.stdout
    probe = subprocess.run([sys.executable, "-c",
                            "from trace_analysis.resources import asset_path, echarts_path; "
                            "from trace_analysis.analysis import triage_builder, triage_accumulator, read_rows, read_model; "
                            "assert triage_builder.TraceDimensionBuilder; "
                            "assert triage_accumulator.TraceAccumulator; "
                            "assert read_rows.build_trace_rows; "
                            "assert read_model.build_analysis; "
                            "assert asset_path('read.html').is_file(); assert echarts_path().is_file()"],
                           cwd=tmp_path, env=environment, capture_output=True, text=True)
    assert probe.returncode == 0, probe.stderr
    revision = subprocess.run(
        [sys.executable, "-c", "from trace_analysis.resources import producer_revision; print(producer_revision())"],
        cwd=tmp_path, env=environment, capture_output=True, text=True, check=True,
    )
    assert revision.stdout.strip() == build_info["revision"]
