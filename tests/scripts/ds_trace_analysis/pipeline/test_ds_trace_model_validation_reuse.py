"""Validate already-loaded models without duplicate disk reads or input mutation."""
import copy
import json
import shutil
from collections import Counter
from pathlib import Path

import pytest

from test_ds_trace_stage_resume import make_case
from trace_analysis import pipeline, render_bundle, validation, delivery_validation
from trace_analysis.orchestration.publication import current_publication


@pytest.mark.parametrize('data', [ {'schema_version': 1, 'traces': [], 'aggregate': {}},
                                  {'schema_version': 99, 'traces': [], 'aggregate': {}},
                                  {'traces': [{'trace_id': 'bad', 'client_ms': 5,
                                               'attribution_ms': {'rpc': 4}}], 'aggregate': {}}, []])
def test_pure_model_validation_preserves_file_contract_and_input(tmp_path, data):
    path = tmp_path / 'model.json'
    path.write_text(json.dumps(data))
    before = copy.deepcopy(data)
    assert validation.validate_data(data, 'bottleneck', path) == validation.validate(path, 'bottleneck')
    assert data == before


def test_pure_write_validation_preserves_budget_error_and_input():
    read = {'traces': [], 'write_traces': [{'trace_id': 'w'}]}
    write = {'rows': [{'trace_id': 'w', 'client_ms': 5, 'write_breakdown_ms': {'copy': 4}}]}
    before = copy.deepcopy((read, write))
    with pytest.raises(ValueError, match='budget does not close'):
        validation.validate_write_data(write, read)
    assert (read, write) == before


@pytest.mark.parametrize('consumer', ['render', 'delivery'])
def test_each_model_is_read_once_per_consumer(tmp_path, monkeypatch, consumer):
    manifest, root = make_case(tmp_path)
    pipeline.run_pipeline(manifest, root, False)
    published = current_publication(root)
    working = tmp_path / 'working'
    shutil.copytree(published['directory'], working)
    (working / 'publication.json').unlink()
    config = json.loads((working / 'suite.manifest.json').read_text())
    suite = json.loads((working / 'suite.analysis.json').read_text().replace(str(published['directory']), str(working)))
    run = config['runs'][0]
    paths = {working / run[key] for key in delivery_validation.MODEL_FIELDS}
    before = {path: path.read_bytes() for path in paths}
    counts = Counter()
    read_text = Path.read_text
    def counted(path, *args, **kwargs):
        if path in paths:
            counts[path] += 1
        return read_text(path, *args, **kwargs)
    monkeypatch.setattr(Path, 'read_text', counted)
    if consumer == 'render':
        assert render_bundle.render_run(run, working)['valid']
    else:
        assert delivery_validation.validate_suite(suite, config, working, require_models=True)['valid']
    consumed = paths if consumer == 'delivery' else paths - {
        working / run['evidence_json'], working / run['issues_analysis_json']}
    assert counts == Counter({path: 1 for path in consumed})
    assert all(path.read_bytes() == content for path, content in before.items())
