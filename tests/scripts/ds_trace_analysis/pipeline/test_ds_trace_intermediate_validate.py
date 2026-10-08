"""Persisted models must fail closed before rendering invalid evidence."""
from trace_test_loader import load_fresh
import json
from pathlib import Path

import pytest

validator = load_fresh("validation")


def check(tmp_path, model, kind="bottleneck"):
    path = tmp_path / "model.json"
    path.write_text(json.dumps(model), encoding="utf-8")
    return validator.validate(path, kind)


@pytest.mark.parametrize("kind,model", [
    ("triage", {"traces": {}, "dimensions": {}}),
    ("bottleneck", {"traces": [], "aggregate": {}}),
    ("numa", {"traces": [], "aggregate": {}, "limitations": []}),
])
def test_empty_observed_dataset_is_valid_but_wrong_shapes_are_not(tmp_path, kind, model):
    assert check(tmp_path, model, kind)["valid"]
    for field in model:
        broken = {**model, field: "wrong type"}
        assert not check(tmp_path, broken, kind)["valid"]
        broken = {key: value for key, value in model.items() if key != field}
        assert not check(tmp_path, broken, kind)["valid"]
    assert not check(tmp_path, [], kind)["valid"]


@pytest.mark.parametrize("duration", [float("nan"), float("inf"), -1, "1", None, True])
def test_invalid_durations_are_rejected_even_when_sum_would_close(tmp_path, duration):
    row = {"trace_id": "trace-one", "client_ms": 1, "attribution_ms": {"a": duration, "b": 2}}
    result = check(tmp_path, {"traces": [row], "aggregate": {}})
    assert not result["valid"]
    assert result["closure_bad_trace_count"] == 1
    assert "trace-one" in result["errors"][0]


def test_each_persisted_stage_model_must_close_independently(tmp_path):
    row = {"trace_id": "trace-one", "client_ms": 1, "attribution_ms": {"a": 1},
           "focus_breakdown_ms": {"URMA通信": 20}}
    assert not check(tmp_path, {"traces": [row], "aggregate": {}})["valid"]
    row["focus_breakdown_ms"] = {"a": 0.25, "b": 0.75}
    assert check(tmp_path, {"traces": [row], "aggregate": {}})["valid"]
    row["client_ms"] = float("nan")
    assert not check(tmp_path, {"traces": [row], "aggregate": {}})["valid"]


def test_missing_budget_duplicate_trace_and_non_object_rows_fail(tmp_path):
    row = {"trace_id": "trace-one", "client_ms": 1}
    assert not check(tmp_path, {"traces": [row], "aggregate": {}})["valid"]
    row["attribution_ms"] = {"a": 1}
    assert not check(tmp_path, {"traces": [row, row], "aggregate": {}})["valid"]
    assert not check(tmp_path, {"traces": [None], "aggregate": {}})["valid"]


def test_unknown_client_is_preserved_and_invalid_json_has_explicit_failure(tmp_path):
    row = {"trace_id": "trace-one", "client_ms": None}
    assert check(tmp_path, {"traces": [row], "aggregate": {}})["valid"]
    path = tmp_path / "broken.json"
    path.write_text("{")
    result = validator.validate(path, "bottleneck")
    assert not result["valid"]
    assert result["errors"] == ["unreadable JSON: JSONDecodeError"]


@pytest.mark.parametrize("version", [2, True, "1", None])
def test_unknown_or_malformed_model_schema_is_rejected(tmp_path, version):
    result = check(tmp_path, {"schema_version": version, "traces": [], "aggregate": {}})
    assert not result["valid"]
    assert "schema_version" in result["errors"][0]


def test_unversioned_legacy_models_are_explicitly_identified(tmp_path):
    assert check(tmp_path, {"traces": [], "aggregate": {}})["schema_status"] == "legacy_unversioned"
    assert check(tmp_path, {"schema_version": 1, "traces": [], "aggregate": {}})["schema_status"] == "supported"


@pytest.mark.parametrize("write_version", [None, 1])
def test_write_validation_accepts_legacy_and_versioned_models(tmp_path, write_version):
    read_path, write_path = tmp_path / "read.json", tmp_path / "write.json"
    read_path.write_text(json.dumps({"traces": [], "write_traces": []}))
    write = {"rows": []}
    if write_version is not None:
        write["schema_version"] = write_version
    write_path.write_text(json.dumps(write))
    validator.validate_write_model(write_path, read_path)


@pytest.mark.parametrize("field", ["read", "write"])
def test_write_validation_rejects_future_model_schema(tmp_path, field):
    read_path, write_path = tmp_path / "read.json", tmp_path / "write.json"
    models = {"read": {"traces": [], "write_traces": []}, "write": {"rows": []}}
    models[field]["schema_version"] = 2
    read_path.write_text(json.dumps(models["read"]))
    write_path.write_text(json.dumps(models["write"]))
    with pytest.raises(ValueError, match="schema_version"):
        validator.validate_write_model(write_path, read_path)
