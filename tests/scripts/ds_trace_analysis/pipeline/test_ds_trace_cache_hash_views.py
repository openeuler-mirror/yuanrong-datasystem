"""Private cache hashing borrows its isolated parsed model; public projections still copy."""
import copy
import hashlib
import json
from unittest.mock import Mock

import pytest

from test_ds_trace_cache_inputs import api, model, save


def expected_hash(model, kind, projection):
    payload = {'projection_version': 1, 'kind': kind, 'model': projection(model)}
    text = json.dumps(payload, sort_keys=True, separators=(',', ':'), ensure_ascii=False, allow_nan=False)
    return {kind: hashlib.sha256(text.encode('utf-8')).hexdigest()}


def test_private_hash_does_not_deepcopy(tmp_path, monkeypatch, model):
    module = api()
    path = save(tmp_path, model)
    expected = expected_hash(model, 'numa_base', module.project_numa_inputs)
    monkeypatch.setattr(module.copy, 'deepcopy', Mock(side_effect=AssertionError('unnecessary deepcopy')))
    assert module.numa_model_inputs(path) == expected


@pytest.mark.parametrize('field', ['trace_id', 'client_ms', 'status', 'evidence', 'direct_data_worker',
                                   'primary_problem', 'transport', 'failed', 'future_write_field'])
def test_hash_matches_old_public_projection_for_each_field(tmp_path, model, field):
    module = api()
    for group in ('traces', 'write_traces'):
        model[group][0][field] = {'nested': [field, '中文', None, 1.25]}
    path = save(tmp_path, model)
    expected = expected_hash(model, 'numa_base', module.project_numa_inputs)
    assert module.numa_model_inputs(path) == expected


def test_public_projection_nested_values_remain_isolated(model):
    module = api()
    model['write_traces'][0]['evidence'] = [{'nested': [1]}]
    before = copy.deepcopy(model)
    projected = module.project_numa_inputs(model)
    projected['write_traces'][0]['evidence'][0]['nested'].append(2)
    assert model == before


@pytest.mark.parametrize('bad', [[], {'write_traces': 'bad'}, {'write_traces': [3]},
                                 {'write_traces': [{'client_ms': float('nan')}]}])
def test_bad_models_keep_same_exception_contract(tmp_path, bad):
    module = api()
    with pytest.raises(ValueError) as reference:
        if not isinstance(bad, dict):
            raise ValueError('bottleneck model must be a JSON object')
        expected_hash(bad, 'numa_base', module.project_numa_inputs)
    with pytest.raises(type(reference.value), match=str(reference.value)):
        module.numa_model_inputs(save(tmp_path, bad))


def test_malformed_json_error_is_preserved(tmp_path):
    path = tmp_path / 'broken.json'
    path.write_text('{broken')
    with pytest.raises(json.JSONDecodeError):
        api().numa_model_inputs(path)
