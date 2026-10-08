"""The public Evidence stage must bind to Triage and feed the read model."""

import hashlib
import json
from unittest.mock import Mock

import pytest

from test_ds_trace_stage_resume import make_case
from trace_analysis import bottleneck, pipeline
from trace_analysis.analysis import read_initial
from trace_analysis.evidence.normalized import summary_digest
from trace_analysis.validation import validate_evidence_data


def _artifacts(tmp_path):
    manifest, output = make_case(tmp_path)
    result = pipeline.run_pipeline(manifest, output, False)
    run = result["runs"][0]
    return output, output / run["triage_json"], output / run["evidence_json"]


def test_summary_digest_reads_multiple_blocks(tmp_path):
    payload = b"a" * (1024 * 1024 + 7)
    source = tmp_path / "summary.json"
    source.write_bytes(payload)
    assert summary_digest(source) == hashlib.sha256(payload).hexdigest()


def test_evidence_is_compact_valid_and_consumed_without_reparsing(tmp_path, monkeypatch):
    _, summary_path, evidence_path = _artifacts(tmp_path)
    summary = json.loads(summary_path.read_text())
    evidence = json.loads(evidence_path.read_text())
    checked = validate_evidence_data(evidence, summary, summary_digest(summary_path))
    assert checked["valid"] and checked["trace_count"] == len(summary["traces"])
    first_trace = next(iter(summary["traces"].values()))
    assert first_trace["evidence"][0]["text"] not in json.dumps(evidence["traces"])
    baseline = bottleneck.build_analysis(summary_path.parent, top_n=0, source_ref="head")
    forbidden = Mock(side_effect=AssertionError("read stage reparsed the raw evidence"))
    monkeypatch.setattr(read_initial, "extract_read_observations", forbidden)
    with_evidence = bottleneck.build_analysis(summary_path.parent, top_n=0, source_ref="head",
                                              evidence_json=evidence_path)
    assert with_evidence == baseline
    assert forbidden.call_count == 0


def test_evidence_rejects_wrong_summary_and_missing_trace(tmp_path):
    _, summary_path, evidence_path = _artifacts(tmp_path)
    summary = json.loads(summary_path.read_text())
    evidence = json.loads(evidence_path.read_text())
    assert not validate_evidence_data(evidence, summary, "0" * 64)["valid"]
    trace_id = next(iter(evidence["traces"]))
    evidence["traces"][trace_id]["read"]["display_indices"] = [999999]
    assert not validate_evidence_data(evidence, summary, summary_digest(summary_path))["valid"]
    evidence = json.loads(evidence_path.read_text())
    evidence["coverage"]["rpc_observed"] += 1
    assert not validate_evidence_data(evidence, summary, summary_digest(summary_path))["valid"]
    evidence = json.loads(evidence_path.read_text())
    evidence["traces"].pop(next(iter(evidence["traces"])))
    assert not validate_evidence_data(evidence, summary, summary_digest(summary_path))["valid"]
    with pytest.raises(ValueError, match="does not match triage summary"):
        mismatched = tmp_path / "wrong-evidence.json"
        mismatched.write_text(json.dumps(evidence))
        bottleneck.build_analysis(summary_path.parent, top_n=0, evidence_json=mismatched)


def test_evidence_rejects_numeric_bool_substitution(tmp_path):
    _, summary_path, evidence_path = _artifacts(tmp_path)
    summary = json.loads(summary_path.read_text())
    original = json.loads(evidence_path.read_text())
    trace_id = next(iter(original["traces"]))
    for field, value in (("explicit_remote", 1), ("client_us", True)):
        evidence = json.loads(evidence_path.read_text())
        evidence["traces"][trace_id]["read"][field] = value
        assert not validate_evidence_data(evidence, summary, summary_digest(summary_path))["valid"]


def test_tampered_evidence_regenerates_without_reparsing_triage(tmp_path, monkeypatch):
    manifest, output = make_case(tmp_path)
    first = pipeline.run_pipeline(manifest, output, False)
    old_path = output / first["runs"][0]["evidence_json"]
    original = old_path.read_bytes()
    old_path.write_text("{}")
    triage = Mock(side_effect=AssertionError("Triage should remain cached"))
    monkeypatch.setattr(pipeline.stages, "run_triage", triage)
    resumed = pipeline.run_pipeline(manifest, output, False, resume=True)
    new_path = output / resumed["runs"][0]["evidence_json"]
    assert new_path != old_path and new_path.read_bytes() == original
    assert resumed["validation"]["runs"]["case"]["cache"]["evidence"]["reason"] == "artifact_changed"
    assert resumed["validation"]["runs"]["case"]["cache"]["triage"]["status"] == "hit"
    assert triage.call_count == 0
