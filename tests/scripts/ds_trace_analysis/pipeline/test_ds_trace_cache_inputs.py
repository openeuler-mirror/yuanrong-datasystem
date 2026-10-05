"""Model cache inputs reflect consumer dependencies rather than whole report files."""
from trace_test_loader import REPO_ROOT
import copy
import importlib
import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(REPO_ROOT / "scripts"))


def api():
    return importlib.import_module("trace_analysis.cache_inputs")


@pytest.fixture
def model():
    row = {"trace_id": "write-1", "timestamp": "2026-09-20T01:00:00", "client_ms": 5.0,
           "status": 0, "failed": False, "evidence": [], "create_rpc_ms": 0,
           "publish_rpc_ms": 0, "write_breakdown_ms": {"未解释残差": 5.0},
           "write_urma_ms": None, "primary_problem": "write-residual"}
    return {"metadata": {"title": "original"}, "traces": [
        {"trace_id": "read-1", "client_ms": 8.0, "status": 0, "failed": False,
         "evidence": [], "primary_problem": "RPC网络", "transport": "UB",
         "direct_data_worker": "worker-a", "focus_breakdown_ms": {"RPC网络": 8.0}}],
        "write_traces": [row], "aggregate": {"read_count": 1}}


def save(tmp_path, model):
    path = tmp_path / "bottleneck.analysis.json"
    path.write_text(json.dumps(model, ensure_ascii=False), encoding="utf-8")
    return path


def test_numa_drops_display_but_retains_read_diagnosis(tmp_path, model):
    module = api()
    before = module.numa_model_inputs(save(tmp_path, model))
    model["traces"][0]["focus_breakdown_ms"] = {"changed": 8}
    model["aggregate"] = {"restyled": True}
    assert module.numa_model_inputs(save(tmp_path, model)) == before
    model["traces"][0]["primary_problem"] = "QueryMeta"
    assert module.numa_model_inputs(save(tmp_path, model)) != before


def test_projected_models_preserve_actual_consumer_output(model):
    module = api()
    from trace_analysis.numa import build_trace_records
    original = copy.deepcopy(model)
    assert build_trace_records({}, model, {}) == build_trace_records({}, module.project_numa_inputs(model), {})
    projected = module.project_numa_inputs(model)
    projected["write_traces"][0]["evidence"].append("isolated")
    assert model == original


def test_numa_preserves_duplicate_precedence(tmp_path, model):
    module = api()
    model["traces"].append({**model["traces"][0], "client_ms": 9})
    before = module.numa_model_inputs(save(tmp_path, model))
    model["traces"].reverse()
    assert module.numa_model_inputs(save(tmp_path, model)) != before


def test_invalid_json_values_cannot_enter_cache(tmp_path, model):
    module = api()
    model["write_traces"][0]["client_ms"] = float("nan")
    with pytest.raises(ValueError):
        module.numa_model_inputs(save(tmp_path, model))


def test_numa_projection_covers_current_consumer_field_reads():
    import ast
    import inspect
    from trace_analysis import numa
    module = api()
    fields = set()
    for function in (numa._client_ms, numa._status, numa._evidence_raw, numa.build_trace_records):
        for node in ast.walk(ast.parse(inspect.getsource(function))):
            if (isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
                    and isinstance(node.func.value, ast.Name) and node.func.value.id == "row"
                    and node.func.attr == "get" and node.args and isinstance(node.args[0], ast.Constant)):
                fields.add(node.args[0].value)
            if (isinstance(node, ast.Subscript) and isinstance(node.value, ast.Name)
                    and node.value.id == "row" and isinstance(node.slice, ast.Constant)):
                fields.add(node.slice.value)
    assert fields == set(module.NUMA_ROW_FIELDS)
