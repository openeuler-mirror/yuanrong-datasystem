"""Read rendering consumes prepared models without importing attribution code."""

import ast
import copy
import importlib
from pathlib import Path

from test_ds_trace_bottleneck import load_module, run_dir


def test_read_renderer_is_independent_of_analysis():
    load_module()
    renderer = importlib.import_module("trace_analysis.rendering.read")
    tree = ast.parse(Path(renderer.__file__).read_text(encoding="utf-8"))
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom):
            assert node.module not in {"bottleneck", "pipeline", "triage"}
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name):
            assert node.func.id not in {"aggregate", "build_analysis", "exec", "eval"}


def test_renderer_and_legacy_api_match_without_mutating_model(run_dir):
    module = load_module()
    renderer = importlib.import_module("trace_analysis.rendering.read")
    analysis = module.build_analysis(run_dir, top_n=0)
    original = copy.deepcopy(analysis)
    scopes, topology = module.prepare_read_view(analysis["traces"], analysis["aggregate"], analysis["metadata"])
    actual = renderer.render_html(
        analysis, "P3 renderer <fixture>", scope_aggregates=scopes, topology=topology,
    )
    assert actual == module.render_html(analysis, "P3 renderer <fixture>")
    assert analysis == original
    assert set(scopes) == {"0", "100", "1000"}
    assert "P3 renderer &lt;fixture&gt;" in actual


def test_legacy_template_override_reaches_renderer(run_dir, monkeypatch):
    module = load_module()
    analysis = module.build_analysis(run_dir, top_n=0)
    monkeypatch.setattr(module, "HTML_TEMPLATE", module.HTML_TEMPLATE + "<!-- custom template -->")
    assert module.render_html(analysis, "custom").endswith("<!-- custom template -->")
