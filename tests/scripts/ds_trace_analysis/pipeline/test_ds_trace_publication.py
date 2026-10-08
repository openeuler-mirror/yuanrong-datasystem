"""A publication has one commit point and never overwrites an older generation."""
from trace_test_loader import REPO_ROOT
import importlib
import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(REPO_ROOT / "scripts"))
publication = importlib.import_module("trace_analysis.orchestration.publication")


def ready(root, label):
    generation = publication.new_publication(root)
    for name, content in {"index.html": label, "suite.manifest.json": '{}',
                          "suite.analysis.json": '{}', "publication.validation.json": '{"valid":true}'}.items():
        (generation / name).write_text(content)
    return generation


def test_success_changes_only_the_entry_pointer(tmp_path):
    old = ready(tmp_path, "old")
    publication.commit_publication(tmp_path, old, "key-1")
    frozen = {p: p.read_bytes() for p in old.rglob('*') if p.is_file()}
    new = ready(tmp_path, "new")
    publication.commit_publication(tmp_path, new, "key-2")
    current = publication.current_publication(tmp_path)
    assert current["directory"] == new
    assert current["key"] == "key-2"
    assert all(path.read_bytes() == content for path, content in frozen.items())
    assert current["index"].read_text() == "new"
    assert "http-equiv=\"refresh\"" in (tmp_path / "index.html").read_text()


@pytest.mark.parametrize("failure", ["validation", "replace", "interrupt"])
def test_failed_or_interrupted_publish_keeps_the_old_report(tmp_path, monkeypatch, failure):
    old = ready(tmp_path, "old")
    publication.commit_publication(tmp_path, old, "old-key")
    entry = (tmp_path / "index.html").read_bytes()
    new = ready(tmp_path, "new")
    if failure == "validation":
        (new / "publication.validation.json").write_text('{"valid":false}')
        error_type = ValueError
    else:
        error_type = OSError if failure == "replace" else KeyboardInterrupt
        def failed_replace(*args):
            raise error_type("interrupted before commit")
        monkeypatch.setattr(publication.os, "replace", failed_replace)
    with pytest.raises(error_type):
        publication.commit_publication(tmp_path, new, "new-key")
    assert (tmp_path / "index.html").read_bytes() == entry
    assert publication.current_publication(tmp_path)["directory"] == old


def test_corrupted_published_artifacts_are_not_reused(tmp_path):
    generation = ready(tmp_path, "old")
    publication.commit_publication(tmp_path, generation, "key")
    (generation / "suite.analysis.json").write_text('{"tampered":true}')
    with pytest.raises(ValueError, match="changed"):
        publication.current_publication(tmp_path)


def test_legacy_entry_has_no_publication_pointer(tmp_path):
    (tmp_path / "index.html").write_text('<html><body>legacy</body></html>')
    assert publication.current_publication(tmp_path) is None


def test_incomplete_generation_and_path_escape_are_rejected(tmp_path):
    partial = publication.new_publication(tmp_path)
    (partial / "index.html").write_text("partial")
    with pytest.raises((ValueError, OSError)):
        publication.commit_publication(tmp_path, partial, "key")
    (tmp_path / "index.html").write_text('<meta name="trace-publication" content="../outside.json">')
    with pytest.raises(ValueError):
        publication.current_publication(tmp_path)


def test_failed_stage_generation_preserves_previous_success(tmp_path):
    from trace_analysis.cached_stages import CachedStages
    from trace_analysis.stages import StageResult

    cache = CachedStages(tmp_path, "case", True)
    def good(directory):
        model = directory / "model.json"
        model.write_text('{"stable":true}')
        return StageResult({"analysis_json": model})
    first = cache.run("read", {}, {}, good, lambda result: None, lambda paths: None, generation=True)
    old = first.artifacts["analysis_json"]
    def failed(directory):
        (directory / "model.json").write_text("partial")
        raise RuntimeError("producer interrupted")
    with pytest.raises(RuntimeError, match="producer interrupted"):
        cache.run("read", {}, {"changed": True}, failed, lambda result: None, lambda paths: None, generation=True)
    assert old.read_text() == '{"stable":true}'
    restored = cache.run("read", {}, {}, good, lambda result: None,
                         lambda paths: StageResult(paths), generation=True)
    assert restored.artifacts["analysis_json"] == old


def test_process_exit_before_pointer_replace_keeps_previous_generation(tmp_path):
    import os
    import subprocess

    old = ready(tmp_path, "old")
    publication.commit_publication(tmp_path, old, "old-key")
    entry = (tmp_path / "index.html").read_bytes()
    new = ready(tmp_path, "new")
    script = (
        "import os,sys; from pathlib import Path; "
        "from trace_analysis.orchestration import publication; "
        "publication.os.replace=lambda *args: os._exit(73); "
        "publication.commit_publication(Path(sys.argv[1]), Path(sys.argv[2]), 'new-key')"
    )
    environment = {**os.environ, "PYTHONPATH": str(REPO_ROOT / "scripts")}
    child = subprocess.run([sys.executable, "-c", script, str(tmp_path), str(new)],
                           env=environment, timeout=10, capture_output=True, text=True)
    assert child.returncode == 73, child.stderr
    assert (tmp_path / "index.html").read_bytes() == entry
    assert publication.current_publication(tmp_path)["directory"] == old
    assert list(tmp_path.glob('.index-*.tmp'))


def test_stage_provenance_is_explicit_hashed_and_rebuilt_if_tampered(tmp_path, monkeypatch):
    from trace_analysis.cached_stages import CachedStages
    from trace_analysis import cached_stages
    from trace_analysis.stages import StageResult

    monkeypatch.setattr(cached_stages, "producer_revision", lambda: "abc123+dirty")
    cache = CachedStages(tmp_path, "case", True, producer_fingerprint="tool-123")
    produced = []
    def produce(directory):
        produced.append(directory)
        model = directory / "model.json"
        model.write_text('{}')
        return StageResult({"analysis_json": model})
    first = cache.run("read", {"input": "abc"}, {"top": 100}, produce,
                      lambda result: None, lambda paths: StageResult(paths), generation=True)
    provenance = first.artifacts["provenance_json"]
    data = json.loads(provenance.read_text())
    assert data["schema_version"] == 1
    assert data["input_hashes"] == {"input": "abc"}
    assert data["effective_config"] == {"top": 100}
    assert data["producer"] == {"tool_fingerprint": "tool-123", "revision": "abc123+dirty"}
    assert data["run_id"] == "case" and data["stage"] == "read"
    assert len(data["rule_version"]) == len(data["cache_key"]) == 64
    provenance.write_text('{"tampered":true}')
    second = cache.run("read", {"input": "abc"}, {"top": 100}, produce,
                       lambda result: None, lambda paths: StageResult(paths), generation=True)
    assert len(produced) == 2
    assert second.artifacts["provenance_json"] != provenance
    assert cache.diagnostics["read"]["reason"] == "artifact_changed"


def test_legacy_stage_cache_without_provenance_is_rebuilt(tmp_path):
    from trace_analysis.cached_stages import CachedStages
    from trace_analysis.stages import StageResult

    cache = CachedStages(tmp_path, "case", True)
    root = tmp_path / "runs/case"
    root.mkdir(parents=True)
    legacy = root / "model.json"
    legacy.write_text('{}')
    cache.run("read", {}, {}, lambda: StageResult({"analysis_json": legacy}),
              lambda result: None, lambda paths: StageResult(paths))
    def produce(directory):
        model = directory / "model.json"
        model.write_text('{}')
        return StageResult({"analysis_json": model})
    result = cache.run("read", {}, {}, produce, lambda result: None,
                       lambda paths: StageResult(paths), generation=True)
    assert result.artifacts["analysis_json"] != legacy
    assert result.artifacts["provenance_json"].is_file()
    assert cache.diagnostics["read"]["reason"] == "provenance_missing"


@pytest.mark.parametrize("missing", ["manifest.json", "inventory.json", "parsed_traces.json", "triage.json", "events.jsonl"])
def test_incomplete_triage_generation_is_never_committed(tmp_path, missing):
    from trace_analysis.cached_stages import CachedStages
    from trace_analysis.stages import StageResult

    cache = CachedStages(tmp_path, "case", True)
    def produce(directory):
        for name in ("summary.json", "manifest.json", "inventory.json", "parsed_traces.json", "triage.json", "events.jsonl"):
            (directory / name).write_text("" if name == "events.jsonl" else "{}")
        (directory / missing).unlink()
        return StageResult({"run_dir": directory, "summary_json": directory / "summary.json"})
    with pytest.raises(RuntimeError, match="required triage artifacts"):
        cache.run("triage", {}, {}, produce, lambda result: None, lambda paths: None, generation=True)
    assert not cache.cache.manifest_path("case", "triage").exists()
    assert cache.diagnostics["triage"]["status"] == "failed"


@pytest.mark.parametrize("version", [True, 1.0, "1", None, 2])
def test_pointer_rejects_invalid_descriptor_schema_even_with_matching_digest(tmp_path, version):
    import re
    generation = ready(tmp_path, "report")
    publication.commit_publication(tmp_path, generation, "key")
    descriptor_path = generation / "publication.json"
    descriptor = json.loads(descriptor_path.read_text())
    descriptor["schema_version"] = version
    descriptor_path.write_text(json.dumps(descriptor))
    entry = tmp_path / "index.html"
    entry.write_text(re.sub('data-sha256="[^"]+"',
                           f'data-sha256="{publication._file_hash(descriptor_path)}"', entry.read_text()))
    with pytest.raises(ValueError, match="descriptor"):
        publication.current_publication(tmp_path)
