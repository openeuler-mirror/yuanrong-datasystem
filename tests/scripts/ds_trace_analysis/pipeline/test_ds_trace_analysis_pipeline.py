from pathlib import Path
from trace_test_loader import REPO_ROOT, load_fresh
import json
import io
import tarfile

ROOT = REPO_ROOT
SCRIPT = ROOT / "scripts/trace_analysis/pipeline.py"


def load_module():
    return load_fresh("pipeline")


def test_pipeline_keeps_run_outputs_isolated_and_copies_archive(tmp_path, monkeypatch):
    module = load_module()
    source_archive = tmp_path / "run.tar.gz"
    with tarfile.open(source_archive, "w:gz") as stream:
        member = tarfile.TarInfo("trace.log")
        member.size = len(b"archive")
        stream.addfile(member, io.BytesIO(b"archive"))
    input_dir = tmp_path / "logs"
    input_dir.mkdir()
    manifest = tmp_path / "pipeline.json"
    manifest.write_text(json.dumps({
        "schema_version": 1,
        "source_head": "pr-head",
        "source_base": "main-head",
        "runs": [{
            "id": "wr50", "label": "WR capacity=50", "inputs": [str(input_dir)],
            "input_archive": str(source_archive), "local_cache": False,
            "implementation": "pr2422", "size": "8MB", "load": "50",
            "client_shape": "single", "top": 0,
        }],
    }), encoding="utf-8")
    output = tmp_path / "report"

    from types import SimpleNamespace

    def triage_stage(config, manifest, run_root, force, **kwargs):
        run_dir = run_root / "triage" / "staged"
        run_dir.mkdir(parents=True)
        (run_dir / "manifest.json").write_text("{}")
        (run_dir / "inventory.json").write_text("{}")
        (run_dir / "summary.json").write_text("{}")
        (run_dir / "triage.json").write_text("{}")
        (run_dir / "parsed_traces.json").write_text("{}")
        (run_dir / "events.jsonl").write_text("")
        (run_dir / "report.local.html").write_text("<html><body></body></html>")
        return SimpleNamespace(artifacts={"run_dir": run_dir, "summary_json": run_dir / "summary.json",
                                          "report_html": run_dir / "report.local.html"})

    def model_stage(run_root, html_name, json_name, model):
        html = run_root / html_name
        analysis = run_root / json_name
        html.write_text("<html><body></body></html>")
        analysis.write_text(json.dumps(model))
        return SimpleNamespace(artifacts={"report_html": html, "analysis_json": analysis})

    monkeypatch.setattr(module.stages, "run_triage", triage_stage)
    monkeypatch.setattr(module.stages, "run_read", lambda run_dir, cfg, manifest, root, force, **kwargs:
                        model_stage(root, "bottleneck.local.html", "bottleneck.analysis.json", {"traces": []}))
    monkeypatch.setattr(module.stages, "run_write", lambda analysis, run_dir, root, **kwargs:
                        model_stage(root, "bottleneck.write.html", "write.refined.analysis.json", {"rows": []}))
    monkeypatch.setattr(module.stages, "run_numa_from_evidence", lambda spec:
                        model_stage(spec.run_root, "numa.local.html", "numa.analysis.json",
                                    {"traces": [], "aggregate": {}, "limitations": []}))
    monkeypatch.setattr(module.stages, "run_suite", lambda manifest, root, force:
                        model_stage(root, "index.html", "suite.analysis.json", {}))
    monkeypatch.setattr(module, "validate", lambda path, kind, output: {"valid": True, "kind": kind})
    monkeypatch.setattr(module, "validate_triage_bundle", lambda path, output: {"valid": True, "kind": "triage"})
    monkeypatch.setattr(module, "validate_evidence_file", lambda *args: {"valid": True, "kind": "evidence"})
    monkeypatch.setattr(module, "validate_suite", lambda *args, **kwargs: {"valid": True, "errors": []})
    from trace_analysis import render_bundle
    def render_view(config, root):
        for key in ("triage_report", "bottleneck_report", "write_bottleneck_report", "numa_report"):
            (root / config[key]).write_text("<html><body></body></html>")
    monkeypatch.setattr(render_bundle, "render_run", render_view)
    result = module.run_pipeline(manifest, output, force=True)
    assert Path(result["index"]).is_file()
    assert (output / result["runs"][0]["input_archive"]).read_bytes() == source_archive.read_bytes()
    suite_manifest = json.loads((output / "suite.manifest.json").read_text())
    run = suite_manifest["runs"][0]
    assert run["bottleneck_report"].endswith("bottleneck.local.html")
    assert run["write_bottleneck_report"].endswith("bottleneck.write.html")
    assert run["numa_report"].endswith("numa.local.html")
    assert run["write_analysis_json"].endswith("write.refined.analysis.json")
    assert run["numa_analysis_json"].endswith("numa.analysis.json")


def test_report_switcher_links_nested_paths_and_is_idempotent(tmp_path):
    module = load_module()
    reports = {key: tmp_path / key / (key + ' report.html') for key in ('triage', 'read', 'write', 'numa')}
    for path in reports.values():
        path.parent.mkdir()
        path.write_text('<html><body><main>report</main></body></html>')
    module.link_reports(reports)
    once = {key: path.read_text() for key, path in reports.items()}
    module.link_reports(reports)
    import re
    from urllib.parse import unquote
    for key, path in reports.items():
        text = path.read_text()
        assert text == once[key]
        assert text.count('aria-current="page"') == 2  # selector plus current link
        links = re.findall(r'<a href="([^"]+)"', text)
        assert len(links) == 4
        assert 'target="_blank"' not in text
        assert all((path.parent / unquote(link)).resolve().is_file() for link in links)
