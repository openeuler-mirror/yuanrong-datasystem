"""Lock rendering output while separating it from log parsing."""
import ast
import copy
import hashlib
import re
import importlib
from trace_test_loader import REPO_ROOT, load_fresh
from pathlib import Path
import sys

import pytest

ROOT = REPO_ROOT
sys.path.insert(0, str(ROOT / "scripts"))


@pytest.fixture
def report():
    return {
        "code_ref": "test", "trace_count": 1,
        "dimensions": {
            "time": {"first_ts": "a", "last_ts": "b"}, "workers": {"w": {"line_count": 1}},
            **{key: {} for key in ("flow", "latency_ms", "breakdown_ms", "rpc_slow",
                                  "urma_elapsed", "latency_summary_us", "errors", "classifications")},
        },
        "traces": {"get-x": {
            "classification": "slow", "workers": {"w": 1},
            "evidence": [{"text": "t | <script>", "source": "s", "member": "m", "line": 1}],
            "ub_events": [{"event_type": "elapsed", "duration_ms": 2}], "rpc_calls": [],
        }},
    }


@pytest.fixture
def assets(tmp_path):
    for name in ("triage.css", "charts.js", "chapter_navigation.js", "trace_visuals.js", "log_fields.js", "shared.css"):
        (tmp_path / name).write_text(name, encoding="utf-8")
    (tmp_path / "triage.html").write_text(
        '<head>__TITLE____STYLESHEET__</head><body>__DATA____MANIFEST____SCRIPT_REF__'
        '__TRACE_PAYLOADS____DEFERRED_TRACES__</body>', encoding="utf-8")
    return lambda name: tmp_path / name


@pytest.mark.parametrize("site,digest", [
    (False, "cfb337e2960f9da1769075176d7757f371f0a25d68f19ca80975bd8b5bb084cf"),
    (True, "4f02e538ef8e19c9e43f7e3c66e263ed0640e4f7b88c5a3cdfa99c5995d5fd9e"),
])
def test_render_output_matches_before_extraction(report, assets, site, digest):
    rendering = importlib.import_module("trace_analysis.rendering.triage")
    before = copy.deepcopy(report)
    html = rendering.render_html(report, "标题", site, {"x": "</script>"}, asset_resolver=assets)
    assert "log_fields.js\ntrace_visuals.js" in html
    reference_html = html.replace("\nlog_fields.js", "", 1)
    assert hashlib.sha256(re.sub(r'<script id="report-registry">.*?</script>', "", reference_html, flags=re.S).encode()).hexdigest() == digest
    assert report == before
    assert hashlib.sha256(rendering.render_markdown(report).encode()).hexdigest() == (
        "b20c906255c7bfbb3d6d8bf61fb1a130733b71819fd3ef91bd075ecb64ffb772")
    events = rendering.TraceReportRenderer.events(report)
    assert [event['schema_version'] for event in events] == [2, 2]
    assert [event['event_type'] for event in events] == ['raw', 'ub_elapsed']
    assert all(event['worker'] is None for event in events)
    assert all(event['missing_reasons']['worker_id'] == 'not_observed_in_line' for event in events)
    assert events[0]['raw'] == 't | <script>' and events[0]['source'] == 's'
    assert events[1]['duration_ms'] == 2

    assert rendering.TraceReportRenderer.triage(report)["root_cause_families"] == {"slow": 1}
    assert report == before


def test_renderer_has_no_parser_dependency():
    rendering = importlib.import_module("trace_analysis.rendering.triage")
    tree = ast.parse(Path(rendering.__file__).read_text())
    imports = [node.module for node in ast.walk(tree) if isinstance(node, ast.ImportFrom)]
    assert all(module in {"resources", "analysis.triage_artifacts", "registry", "template"} for module in imports)
    assert not any(isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
                   and node.func.id in {"exec", "eval", "__import__"} for node in ast.walk(tree))


def test_triage_renderer_retains_monkeypatch_scope(report, assets, monkeypatch):
    triage = load_fresh("triage")
    monkeypatch.setattr(triage, "asset_path", assets)
    html = triage.TraceRunPipeline().renderer.html(report, "标题", manifest={"x": "</script>"})
    assert "log_fields.js\ntrace_visuals.js" in html
    reference_html = html.replace("\nlog_fields.js", "", 1)
    assert hashlib.sha256(re.sub(r'<script id="report-registry">.*?</script>', "", reference_html, flags=re.S).encode()).hexdigest() == (
        "cfb337e2960f9da1769075176d7757f371f0a25d68f19ca80975bd8b5bb084cf")
    monkeypatch.setattr(triage, "_build_events", lambda report: ["patched"])
    assert triage.TraceRunPipeline().renderer.events(report) == ["patched"]
