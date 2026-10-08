"""Stage cache only reuses complete, content-verified artifact sets."""
from trace_test_loader import REPO_ROOT
import importlib
import json
import sys
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest

sys.path.insert(0, str(REPO_ROOT / "scripts"))
cache_module = importlib.import_module("trace_analysis.stage_cache")


def test_key_is_canonical_immutable_and_invalidates_each_dependency():
    inputs, config = {"b": "hash-b", "a": "hash-a"}, {"top": 0, "nested": {"mode": "read"}}
    snapshot = json.dumps([inputs, config])
    key = cache_module.make_stage_key(inputs, config, "v1")
    assert key == cache_module.make_stage_key(dict(reversed(list(inputs.items()))), config, "v1")
    assert key != cache_module.make_stage_key(inputs, config, "v2")
    assert key != cache_module.make_stage_key({"a": "changed"}, config, "v1")
    assert key != cache_module.make_stage_key(inputs, {"top": 100}, "v1")
    assert json.dumps([inputs, config]) == snapshot


def test_hit_requires_every_recorded_file_and_returns_immutable_paths(tmp_path):
    cache = cache_module.StageCache(tmp_path / "cache")
    files = {"model": tmp_path / "model.json", "page": tmp_path / "read.html"}
    for path in files.values():
        path.write_text("first", encoding="utf-8")
    assert cache.lookup("run", "read", "key").reason == "manifest_missing"
    cache.record_success("run", "read", "key", files)
    hit = cache.lookup("run", "read", "key")
    assert (hit.status, hit.reason, dict(hit.artifacts)) == ("hit", "verified", files)
    with pytest.raises(TypeError):
        hit.artifacts["other"] = tmp_path
    assert cache.lookup("run", "read", "changed").reason == "key_changed"
    files["page"].write_text("tampered", encoding="utf-8")
    assert cache.lookup("run", "read", "key").reason == "artifact_changed"
    files["page"].unlink()
    assert cache.lookup("run", "read", "key").reason == "artifact_missing"


def test_failed_producer_and_invalid_outputs_preserve_success_manifest(tmp_path):
    cache = cache_module.StageCache(tmp_path / "cache")
    artifact = tmp_path / "model.json"
    artifact.write_text("{}")
    cache.record_success("run", "parse", "old", {"model": artifact})
    manifest = cache.manifest_path("run", "parse")
    before = manifest.read_bytes()
    for artifacts in ({}, {"directory": tmp_path}, {"missing": tmp_path / "missing"}):
        with pytest.raises((ValueError, OSError)):
            cache.record_success("run", "parse", "new", artifacts)
        assert manifest.read_bytes() == before
    def failed_producer():
        raise RuntimeError("failed before returning artifacts")
    with pytest.raises(RuntimeError):
        cache.record_success("run", "parse", "new", failed_producer())
    assert manifest.read_bytes() == before
    assert cache.lookup("run", "parse", "old").status == "hit"


def test_incomplete_temp_is_ignored_and_atomic_replace_failure_preserves_success(tmp_path, monkeypatch):
    cache = cache_module.StageCache(tmp_path / "cache")
    artifact = tmp_path / "model.json"
    artifact.write_text("{}")
    cache.record_success("run", "parse", "old", {"model": artifact})
    manifest = cache.manifest_path("run", "parse")
    manifest.with_suffix(".json.tmp").write_text('{"unfinished":')
    assert cache.lookup("run", "parse", "old").status == "hit"
    before = manifest.read_bytes()
    def fail_replace(*args):
        raise OSError("simulated replace failure")
    monkeypatch.setattr(cache_module.os, "replace", fail_replace)
    with pytest.raises(OSError):
        cache.record_success("run", "parse", "new", {"model": artifact})
    assert manifest.read_bytes() == before
    assert cache.lookup("run", "parse", "old").status == "hit"


@pytest.mark.parametrize("payload", ["{bad", "null", "[]", '{}', '{"schema_version": 99}',
    '{"schema_version":1,"run_id":"run","stage":"read","key":"key","artifacts":{}}'])
def test_corrupt_or_empty_manifest_is_a_diagnostic_miss(tmp_path, payload):
    cache = cache_module.StageCache(tmp_path)
    manifest = cache.manifest_path("run", "read")
    manifest.parent.mkdir(parents=True, exist_ok=True)
    manifest.write_text(payload)
    result = cache.lookup("run", "read", "key")
    assert result.status == "miss"
    assert result.reason == "manifest_invalid"
    assert not result.artifacts


def test_parallel_runs_and_stages_have_distinct_records(tmp_path):
    cache = cache_module.StageCache(tmp_path / "cache")
    artifact = tmp_path / "file"
    artifact.write_text("data")
    identities = [("run/a", "read"), ("run_a", "read"), ("run/a", "write"), ("../run", "../../read")]
    def record(identity):
        cache.record_success(*identity, "key", {"file": artifact})
    with ThreadPoolExecutor(max_workers=4) as pool:
        list(pool.map(record, identities))
    paths = [cache.manifest_path(*identity) for identity in identities]
    assert len(set(paths)) == len(identities)
    assert all(path.is_relative_to(cache.root) for path in paths)
    assert all(cache.lookup(*identity, "key").status == "hit" for identity in identities)


def test_same_length_content_change_is_detected_without_relying_on_mtime(tmp_path):
    import os

    cache = cache_module.StageCache(tmp_path / "cache")
    artifact = tmp_path / "model"
    artifact.write_text("first")
    cache.record_success("run", "read", "key", {"model": artifact})
    previous = artifact.stat()
    artifact.write_text("other")
    os.utime(artifact, ns=(previous.st_atime_ns, previous.st_mtime_ns))
    assert cache.lookup("run", "read", "key").reason == "artifact_changed"


def test_missing_schema_fields_and_wrong_identity_are_rejected(tmp_path):
    cache = cache_module.StageCache(tmp_path / "cache")
    artifact = tmp_path / "model"
    artifact.write_text("{}")
    cache.record_success("run", "read", "key", {"model": artifact})
    manifest = cache.manifest_path("run", "read")
    original = json.loads(manifest.read_text())
    for field, value in (("run_id", "other"), ("stage", "other"), ("key", None),
                         ("artifacts", {"model": {"path": "relative", "sha256": "x" * 64}}),
                         ("artifacts", {"model": []})):
        record = dict(original)
        record[field] = value
        manifest.write_text(json.dumps(record))
        assert cache.lookup("run", "read", "key").reason == "manifest_invalid"
