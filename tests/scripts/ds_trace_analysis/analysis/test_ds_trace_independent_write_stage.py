"""The write stage must consume shared observations without a read model."""

import json

from test_ds_trace_bottleneck import trace, chunked_urma_event
from test_ds_trace_stage_resume import make_case
from trace_analysis import bottleneck, pipeline, write_report
from trace_analysis.analysis.write_pipeline import build_write_model
from trace_analysis.evidence.normalized import build_evidence, summary_digest
from trace_analysis.validation import validate_evidence_data


def test_write_model_matches_legacy_budget_without_read_attribution(tmp_path, monkeypatch):
    run_dir = tmp_path / "triage"
    run_dir.mkdir()
    first = trace("set-a", 10, 1, timestamp="2026-08-15T10:00:01.000001", urma_ms=4)
    first["flows"] = {"DS_KV_CLIENT_SET": 1}
    first["latency_summary_us"] = {
        "client.rpc.create": 1000, "client.process.memory_copy": 5000,
        "client.urma.ub_transfer": 4000, "client.rpc.publish": 3000,
    }
    first["ub_events"] = [
        chunked_urma_event("2026-08-15T10:00:01.000001", "worker-a", 2, "41", 0, 2,
                           post_us=1000, observed_us=3000),
        chunked_urma_event("2026-08-15T10:00:01.002001", "worker-a", 3, "42", 1, 2,
                           post_us=3000, observed_us=6000),
    ]
    second = trace("set-b", 8, 1, timestamp="2026-08-15T10:00:02.000001", status=1010)
    second["flows"] = {"DS_KV_CLIENT_PUBLISH": 1}
    summary = {"schema_version": 7, "code_ref": "fixture-ref", "inputs": ["fixture.log"],
               "trace_count": 2, "dimensions": {"input_failures": [], "worker_ip_mapping": {}, "coverage": {}},
               "traces": {"set-a": first, "set-b": second}}
    summary_path = run_dir / "summary.json"
    summary_path.write_text(json.dumps(summary))
    (run_dir / "manifest.json").write_text(json.dumps({"schema_version": 1, "case_name": "fixture"}))
    (run_dir / "triage.json").write_text(json.dumps({"issues": []}))
    evidence = build_evidence(summary, summary_digest(summary_path))
    assert validate_evidence_data(evidence, summary, summary_digest(summary_path))["valid"]
    baseline = write_report.build_model(bottleneck.build_analysis(run_dir, top_n=0))

    monkeypatch.setattr("trace_analysis.evidence.write.build_write_facts",
                        lambda *args: (_ for _ in ()).throw(AssertionError("write facts were reparsed")))
    evidence_path = run_dir / "evidence.json"
    evidence_path.write_text(json.dumps(evidence))
    compatible_read = bottleneck.build_analysis(run_dir, top_n=0, evidence_json=evidence_path)
    assert write_report.build_model(compatible_read) == baseline
    monkeypatch.setattr(bottleneck, "build_trace_rows",
                        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("read budget was used")))
    actual = build_write_model(summary, evidence, {})

    assert actual == baseline
    assert len(actual["rows"][0]["write_wr_events"]) == 2
    evidence["traces"]["set-a"]["write"]["source_hashes"][0] = "0" * 64
    assert not validate_evidence_data(evidence, summary, summary_digest(summary_path))["valid"]


def test_pipeline_write_cache_depends_on_evidence_not_read_model(tmp_path):
    manifest, output = make_case(tmp_path)
    result = pipeline.run_pipeline(manifest, output, False)
    run = result["runs"][0]
    provenance = json.loads((output / run["stage_provenance"]["write"]).read_text())
    inputs = set(provenance["input_hashes"])
    assert any(name.endswith("summary.json") for name in inputs)
    assert any(name.endswith("evidence.json") for name in inputs)
    assert not any("bottleneck.analysis.json" in name for name in inputs)
