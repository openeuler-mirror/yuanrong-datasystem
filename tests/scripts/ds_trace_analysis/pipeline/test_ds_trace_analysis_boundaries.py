"""Read attribution and report summaries have one-way dependencies."""

import ast
import copy
import importlib
from pathlib import Path

import pytest

from test_ds_trace_bottleneck import load_module, run_dir


@pytest.mark.parametrize("name", ["aggregation", "correlation", "read", "stats", "budget", "contracts"])
def test_analysis_modules_do_not_import_or_load_facade(name):
    load_module()
    module = importlib.import_module(f"trace_analysis.analysis.{name}")
    for node in ast.walk(ast.parse(Path(module.__file__).read_text())):
        if isinstance(node, ast.ImportFrom):
            assert (node.module or "").split(".")[-1] not in {"bottleneck", "pipeline", "triage", "render_bundle"}
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name):
            assert node.func.id not in {"exec", "eval", "globals"}


def test_aggregate_module_matches_facade_without_mutation(run_dir):
    module = load_module()
    aggregation = importlib.import_module("trace_analysis.analysis.aggregation")
    read = importlib.import_module("trace_analysis.analysis.read")
    analysis = module.build_analysis(run_dir, top_n=0)
    rows = analysis["traces"]
    original = copy.deepcopy(rows)
    assert module.aggregate is aggregation.aggregate
    assert aggregation.aggregate(rows) == module.aggregate(rows)
    assert rows == original
    assert module._apply_focus_breakdown is read._apply_focus_breakdown
    assert module._apply_query_rpc_attribution is read._apply_query_rpc_attribution
