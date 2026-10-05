"""Writing presentation consumes a persisted budget without re-attribution."""
from trace_test_loader import REPO_ROOT
import importlib
import json
import sys
from pathlib import Path

sys.path.insert(0, str(REPO_ROOT / "scripts"))


def test_new_write_model_declares_schema_version():
    writer = importlib.import_module("trace_analysis.write_report")
    model = writer.build_model({"write_traces": []})
    assert model["schema_version"] == 1
    assert model["write_phase_schema_version"] == 2
    assert model["rows"] == []


def test_write_model_render_does_not_refine_or_mutate(monkeypatch):
    writer = importlib.import_module("trace_analysis.write_report")
    model = {"schema_version": 1, "write_phase_schema_version": 2, "rows": [], "rules": writer.RULES}
    before = json.dumps(model, sort_keys=True)
    expected, _ = writer.render_html({"write_traces": []}, "empty")

    def forbidden(*args):
        raise AssertionError("presentation must not refine budgets")

    monkeypatch.setattr(writer, "refine", forbidden)
    page = writer.render_model(model, "empty")
    assert page == expected
    assert json.dumps(model, sort_keys=True) == before


def test_write_html_omits_machine_facts_only_and_preserves_model():
    import copy
    import re
    writer = importlib.import_module('trace_analysis.write_report')
    model = {'rows': [{'trace_id': 'fixture', 'write_evidence_facts': {'schema_version': 1},
                       'evidence': ['<raw log>'], 'write_wr_events': [{'request_id': '42', 'ms': .5}],
                       'write_breakdown_ms': {'未解释残差': 1}, 'client_ms': 1}],
             'rules': [], 'metadata': {'input': 'fixture'}}
    before = copy.deepcopy(model)
    page = writer.render_model(model, 'projection')
    marker = re.search(r'const MODEL\s*=\s*', page)
    assert marker is not None
    projected, _ = json.JSONDecoder().raw_decode(page[marker.end():])
    expected = copy.deepcopy(model)
    del expected['rows'][0]['write_evidence_facts']
    assert projected == expected
    assert model == before
