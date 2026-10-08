"""Input budgets and run artifacts retain their pre-extraction contracts."""
from trace_test_loader import REPO_ROOT
import ast
import importlib
import io
import json
from pathlib import Path
import sys
import tarfile

import pytest

ROOT = REPO_ROOT
sys.path.insert(0, str(ROOT / "scripts"))


def bundle(path):
    with tarfile.open(path, "w:gz") as tar:
        for name in ("first.log", "second.log"):
            item = tarfile.TarInfo(name)
            item.size = 1
            tar.addfile(item, io.BytesIO(b"x"))
    return path


def test_inventory_reader_and_preserver_enforce_same_budget(tmp_path):
    inventory = importlib.import_module("trace_analysis.ingest.inventory")
    source = bundle(tmp_path / "input.tar.gz")
    budget = inventory.TarBudget(max_members=1)
    with pytest.raises(ValueError, match="Tar member count exceeds 1"):
        list(inventory.TraceInputReader(budget=budget).iter_lines([source]))
    with pytest.raises(ValueError, match="Tar member count exceeds 1"):
        inventory.TraceInputInventory(budget=budget).preserve([source], tmp_path / "run")


def test_identity_and_store_manifest_contract(tmp_path):
    inventory = importlib.import_module("trace_analysis.ingest.inventory")
    storage = importlib.import_module("trace_analysis.orchestration.store")
    contracts = importlib.import_module("trace_analysis.orchestration.contracts")
    source = bundle(tmp_path / "input.tar.gz")
    store = storage.TraceRunStore(version_provider=lambda: "fixed")
    options = contracts.RunOptions(code_ref="ref", case_name="case")
    prepared = store.prepare_parse_run([source], tmp_path / "runs", options)
    report = {"dimensions": {"time": {"first_ts": None, "last_ts": None}}, "traces": {}}
    parsed = contracts.ParseOutputBundle(report, [], prepared["created_at"], prepared["cache_key"], prepared["identities"])
    store.write_parse_outputs(prepared["run_dir"], options, parsed)
    manifest = store.read_json(prepared["run_dir"] / "manifest.json")
    assert manifest["script_version"] == "fixed"
    assert manifest["inputs"] == [inventory.input_identity(source, 1)]
    assert manifest["stages"]["parse"] == {"status": "done", "path": "parsed_traces.json"}
    assert store.prepare_parse_run([source], tmp_path / "runs", options) == {
        "run_dir": prepared["run_dir"], "cached": True}
    assert json.loads((prepared["run_dir"] / "parsed_traces.json").read_text()) == report
    assert (prepared["run_dir"] / "raw/extracted/01-input.tar.gz/first.log").read_text() == "x"
    assert "first.log" in (prepared["run_dir"] / "inputs.md").read_text()


def test_new_modules_do_not_import_triage_runner():
    for name in ("trace_analysis.ingest.inventory", "trace_analysis.orchestration.store"):
        module = importlib.import_module(name)
        tree = ast.parse(Path(module.__file__).read_text())
        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom):
                assert node.module not in {"triage", "pipeline", "stages", "stage_cache"}
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Name):
                assert node.func.id not in {"exec", "eval", "globals"}


def test_legacy_budget_changes_apply_to_existing_reader_and_store(tmp_path, monkeypatch):
    from trace_analysis import triage
    source = bundle(tmp_path / "input.tar.gz")
    reader, store = triage.TraceInputReader(), triage.TraceRunStore()
    monkeypatch.setattr(triage, "DEFAULT_MAX_TAR_MEMBERS", 1)
    with pytest.raises(ValueError, match="Tar member count exceeds 1"):
        list(reader.iter_lines([source]))
    with pytest.raises(ValueError, match="Tar member count exceeds 1"):
        store.prepare_parse_run([source], tmp_path / "runs", triage.RunOptions())
