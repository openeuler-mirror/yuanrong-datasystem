"""The unified CLI works outside the checkout and resources remain relocatable."""
import hashlib
import json
from pathlib import Path
import subprocess
import sys

import pytest
from trace_test_loader import REPO_ROOT, load_fresh

ROOT = REPO_ROOT
SCRIPTS = ROOT / "scripts"
sys.path.insert(0, str(SCRIPTS))
from trace_analysis import resources


def test_package_shortens_nested_triage_path_and_rewrites_homepage_link(tmp_path):
    from trace_analysis import packaging

    root = tmp_path / "source"
    output = tmp_path / "offline"
    run_id = "long-run-" + "many-clients-" * 7
    generation = "generation-" + run_id + "-abcdef12"
    triage = Path("runs") / run_id / "triage" / generation / "report.local.html"
    read = Path("runs") / run_id / "bottleneck.local.html"
    (root / triage).parent.mkdir(parents=True)
    (root / triage).write_text('<html><head></head><body><a href="../../bottleneck.local.html">read</a></body></html>')
    (root / read).write_text('<html><head></head><body>read</body></html>')
    (root / "index.html").write_text(
        '<html><head></head><body><script>const REPORT_DATA = '
        + json.dumps({"runs": [{"links": {"triage_report": triage.as_posix()}}], "downloads": []})
        + ';</script></body></html>'
    )
    manifest = tmp_path / "manifest.json"
    manifest.write_text(json.dumps({"runs": [{"triage_report": triage.as_posix(),
                                               "bottleneck_report": read.as_posix()}]}))

    result = packaging.export(root, output, ["index.html"], manifest=manifest)

    digest = hashlib.sha256(triage.as_posix().encode()).hexdigest()[:12]
    short = Path("runs") / run_id / "triage" / digest / "report.local.html"
    assert (output / short).is_file()
    assert len(short.as_posix()) < len(triage.as_posix())
    assert result["files"][triage.as_posix()] == short.as_posix()
    assert short.as_posix() in (output / "index.html").read_text()
    assert triage.as_posix() not in (output / "index.html").read_text()
    assert '../../bottleneck.local.html' in (output / short).read_text()


def test_one_public_trace_script():
    assert sorted(path.name for path in SCRIPTS.glob("ds_trace_*.py")) == ["ds_trace_analysis.py"]


@pytest.mark.parametrize("command,marker", [
    ("pipeline", "--render-only"),
    ("triage", "--allow-partial-inputs"),
    ("read", "--deadline-ms"),
    ("write", "--analysis-json"),
    ("numa", "--source-head"),
    ("suite", "--layout"),
    ("validate", "--kind"),
    ("package", "--output"),
])
def test_stage_help_works_outside_checkout(tmp_path, command, marker):
    args = [str(SCRIPTS / "ds_trace_analysis.py"), command, "--help"]
    result = subprocess.run([sys.executable, *args], cwd=tmp_path, capture_output=True, text=True)
    assert result.returncode == 0, result.stderr
    assert marker in result.stdout


def test_cli_validator_has_machine_output(tmp_path):
    source = tmp_path / "model.json"
    source.write_text(json.dumps({"traces": [], "aggregate": {}}))
    args = ["--input", str(source), "--kind", "bottleneck"]
    result = subprocess.run([sys.executable, str(SCRIPTS / "ds_trace_analysis.py"),
                             "validate", *args], cwd=tmp_path, capture_output=True, text=True)
    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout)["valid"]



def test_nested_fingerprint_covers_content_and_path_but_not_python_caches(tmp_path):
    asset = tmp_path / "assets/read/chart.js"
    asset.parent.mkdir(parents=True)
    asset.write_text("same")
    first = resources.tool_fingerprint(tmp_path)
    cache = tmp_path / "__pycache__/parser.pyc"
    cache.parent.mkdir()
    cache.write_bytes(b"generated")
    assert resources.tool_fingerprint(tmp_path) == first
    asset.write_text("edit")
    second = resources.tool_fingerprint(tmp_path)
    assert second != first
    asset.rename(asset.with_name("renamed.js"))
    assert resources.tool_fingerprint(tmp_path) != second


def test_producer_revision_requires_matching_checkout_or_embedded_build_info(tmp_path):
    empty = tmp_path / "empty" / "trace_analysis"
    empty.mkdir(parents=True)
    assert resources.producer_revision(empty) == "unknown"
    stamped = tmp_path / "stamped" / "trace_analysis"
    stamped.mkdir(parents=True)
    (stamped / "_build_info.json").write_text('{"schema_version":1,"revision":"commit+dirty"}')
    assert resources.producer_revision(stamped) == "commit+dirty"
    malformed = tmp_path / "malformed" / "trace_analysis"
    malformed.mkdir(parents=True)
    (malformed / "_build_info.json").write_text('{"revision":17}')
    assert resources.producer_revision(malformed) == "unknown"


def test_resources_are_independent_of_cwd_and_skill_directory(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    assert resources.asset_path("read.html").is_file()
    assert resources.asset_path("shared.css").is_file()
    assert resources.echarts_path().is_file()
    assert ".skills" not in str(resources.echarts_path())
    with pytest.raises(ValueError):
        resources.asset_path("../read.html")


def test_package_module_globals_remain_patchable(monkeypatch):
    module = load_fresh("triage")
    monkeypatch.setattr(module, "tool_fingerprint", lambda: "patched-fingerprint")
    assert module._script_version() == "patched-fingerprint"[:16]



def test_unified_render_cli_spawn_reaches_validation(tmp_path):
    fields = (
        "triage_report", "bottleneck_report", "write_bottleneck_report", "numa_report",
        "analysis_json", "write_analysis_json", "numa_analysis_json",
    )
    config = {key: key + ".json" for key in fields}
    config["id"] = "missing-evidence"
    (tmp_path / "suite.manifest.json").write_text(json.dumps({"runs": [config]}))
    argv = [str(SCRIPTS / "ds_trace_analysis.py"), "pipeline", "--render-only",
            "--output", str(tmp_path), "--jobs", "2"]
    probe = tmp_path / "spawn_probe.py"
    probe.write_text(
        "import multiprocessing, runpy, sys\n"
        "if __name__ == '__main__':\n"
        "    multiprocessing.set_start_method('spawn')\n"
        f"    sys.path.insert(0, {str(SCRIPTS)!r})\n"
        f"    sys.argv = {argv!r}\n"
        "    runpy.run_path(sys.argv[0], run_name='__main__')\n"
    )
    result = subprocess.run([sys.executable, str(probe)], cwd=tmp_path,
                            capture_output=True, text=True, timeout=30)
    assert result.returncode != 0
    assert "unreadable JSON: FileNotFoundError" in result.stderr
    assert "BrokenProcessPool" not in result.stderr
    assert "ImportError" not in result.stderr
