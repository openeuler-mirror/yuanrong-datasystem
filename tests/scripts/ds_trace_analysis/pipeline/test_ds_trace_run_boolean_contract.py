"""Run boolean controls fail closed before analysis or publication."""
import json
from unittest.mock import Mock

import pytest

from test_ds_trace_stage_resume import make_case
from trace_analysis import pipeline, stages, triage


INVALID = ["false", "true", "", 0, 1, 0.0, 1.0, [], {}, [False]]


@pytest.mark.parametrize("field,bad", [(field, value) for field in
                         ("allow_partial_inputs", "local_cache") for value in INVALID]
                         + [("allow_partial_inputs", None)])
@pytest.mark.parametrize("stage", ["triage", "read"])
def test_direct_stages_reject_invalid_run_booleans(tmp_path, monkeypatch, field, bad, stage):
    config = {"id": "bad-run", "inputs": [str(tmp_path)], field: bad}
    parse = Mock(side_effect=AssertionError("parsing must not start"))
    build = Mock(side_effect=AssertionError("analysis must not start"))
    monkeypatch.setattr(stages.triage, "TraceRunPipeline", parse)
    monkeypatch.setattr(stages.bottleneck, "build_analysis", build)
    with pytest.raises(ValueError, match=field):
        if stage == "triage":
            stages.run_triage(config, {}, tmp_path)
        else:
            stages.run_read(tmp_path, config, {}, tmp_path)
    parse.assert_not_called()
    build.assert_not_called()


@pytest.mark.parametrize("field,bad", [("allow_partial_inputs", "false"),
                                       ("allow_partial_inputs", None),
                                       ("local_cache", "false"), ("local_cache", 0)])
def test_pipeline_preflights_all_runs_before_any_stage(tmp_path, monkeypatch, field, bad):
    manifest, output = make_case(tmp_path)
    valid = tmp_path / "valid.log"
    valid.write_text("2026-07-20T13:27:00.000000 | INFO | worker | kvworker-0-worker1 | 1 | "
                     "019f7d0f-80ec-73bc-91ce-8e84de820012 | ok\n")
    broken = tmp_path / "broken.gz"
    broken.write_bytes(b"not-a-valid-gzip")
    data = json.loads(manifest.read_text())
    data["runs"].append({"id": "bad-run", "inputs": [str(valid), str(broken)],
                         field: bad})
    manifest.write_text(json.dumps(data))
    parse = Mock(side_effect=AssertionError("even the preceding valid Run must not start"))
    monkeypatch.setattr(stages, "run_triage", parse)
    with pytest.raises(ValueError, match=field):
        pipeline.run_pipeline(manifest, output, False, jobs=2)
    parse.assert_not_called()
    assert not output.exists()


@pytest.mark.parametrize('sampling', ['500', 500, [], {'max_per_band': '500'}])
def test_pipeline_rejects_invalid_sampling_before_starting_a_run(tmp_path, monkeypatch, sampling):
    manifest, output = make_case(tmp_path)
    data = json.loads(manifest.read_text())
    data['sampling'] = sampling
    manifest.write_text(json.dumps(data))
    parse = Mock(side_effect=AssertionError('triage must not start'))
    monkeypatch.setattr(stages, 'run_triage', parse)

    with pytest.raises(ValueError, match='sampling'):
        pipeline.run_pipeline(manifest, output, False)

    parse.assert_not_called()
    assert not output.exists()


@pytest.mark.parametrize("value", [True, False, None])
def test_read_passes_exact_local_cache_value(tmp_path, monkeypatch, value):
    build = Mock(return_value={})
    monkeypatch.setattr(stages.bottleneck, "build_analysis", build)
    stages.run_read(tmp_path, {"local_cache": value}, {}, tmp_path, models_only=True)
    assert build.call_args.kwargs["local_cache"] is value


@pytest.mark.parametrize("partial", [False, True])
def test_real_corrupt_input_preserves_partial_policy(tmp_path, partial):
    manifest, _ = make_case(tmp_path)
    data = json.loads(manifest.read_text())
    broken = tmp_path / "broken.gz"
    broken.write_bytes(b"not-a-valid-gzip")
    config = data["runs"][0]
    config["inputs"].append(str(broken))
    config["allow_partial_inputs"] = partial
    if not partial:
        with pytest.raises(triage.TraceTriageError, match="Failed to read trace input"):
            stages.run_triage(config, data, tmp_path / "stage", models_only=True)
        return
    result = stages.run_triage(config, data, tmp_path / "stage", models_only=True)
    summary = json.loads(result.artifacts["summary_json"].read_text())
    assert summary["trace_count"] > 0
    assert summary["dimensions"]["input_failures"][0]["path"] == str(broken)


def test_boolean_defaults_and_valid_combinations():
    from trace_analysis.orchestration.contracts import validate_run_booleans

    defaults = validate_run_booleans({})
    assert defaults.allow_partial_inputs is False
    assert defaults.local_cache is None
    for partial in (True, False):
        for local_cache in (True, False, None):
            config = {"allow_partial_inputs": partial, "local_cache": local_cache}
            controls = validate_run_booleans(config)
            assert controls.allow_partial_inputs is partial
            assert controls.local_cache is local_cache
            assert config == {"allow_partial_inputs": partial, "local_cache": local_cache}


def test_pipeline_false_corrupt_input_cannot_publish(tmp_path):
    manifest, output = make_case(tmp_path)
    data = json.loads(manifest.read_text())
    broken = tmp_path / "broken.gz"
    broken.write_bytes(b"not-a-valid-gzip")
    data["runs"][0]["inputs"].append(str(broken))
    data["runs"][0]["allow_partial_inputs"] = False
    manifest.write_text(json.dumps(data))
    with pytest.raises(ValueError, match="inputs.*tar"):
        pipeline.run_pipeline(manifest, output, False)
    assert not output.exists()
