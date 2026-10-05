"""Hash-verified model caches may reuse a bound successful validation receipt."""
import json
from unittest.mock import Mock

import pytest

from test_ds_trace_stage_resume import make_case
from trace_analysis import cached_stages, pipeline
from trace_analysis.cached_stages import CachedStages, restore_model
from trace_analysis.stages import StageResult


def cached_case(tmp_path, monkeypatch):
    monkeypatch.setattr(cached_stages, 'model_version', lambda stage: 'validator-v1')
    cache = CachedStages(tmp_path, 'case', True, producer_fingerprint='producer')
    def produce(target):
        model = target / 'model.json'
        model.write_text('{}')
        return StageResult({'analysis_json': model})
    producer = Mock(side_effect=produce)
    validator = Mock(return_value={'schema_version': 1, 'valid': True, 'trace_count': 0})
    restored = Mock()
    def run():
        return cache.run('read', {'source': 'hash'}, {}, producer, validator, restore_model,
                         generation=True, restore_validation=restored)
    return cache, producer, validator, restored, run


def test_hit_restores_receipt_without_semantic_validation(tmp_path, monkeypatch):
    cache, producer, validator, restored, run = cached_case(tmp_path, monkeypatch)
    run()
    expected = dict(validator.return_value)
    validator.side_effect = AssertionError('semantic validation reran')
    run()
    assert producer.call_count == 1
    assert validator.call_count == 1
    restored.assert_called_once_with(expected)
    assert cache.diagnostics['read']['status'] == 'hit'


@pytest.mark.parametrize('change', ['model', 'missing', 'corrupt', 'rule', 'schema_bool', 'false_result'])
def test_invalid_receipt_or_changed_evidence_requires_new_validation(tmp_path, monkeypatch, change):
    cache, producer, validator, restored, run = cached_case(tmp_path, monkeypatch)
    result = run()
    manifest = cache.cache.manifest_path('case', 'read')
    record = json.loads(manifest.read_text())
    receipt = record['artifacts']['validation_json']['path']
    from pathlib import Path
    receipt = Path(receipt)
    if change == 'model':
        result.artifacts['analysis_json'].write_text('{"changed":true}')
    elif change == 'missing':
        del record['artifacts']['validation_json']
        manifest.write_text(json.dumps(record))
    elif change == 'corrupt':
        receipt.write_text('broken')
    elif change == 'rule':
        monkeypatch.setattr(cached_stages, 'model_version', lambda stage: 'validator-v2')
    else:
        data = json.loads(receipt.read_text())
        if change == 'schema_bool':
            data['schema_version'] = True
        else:
            data['result']['valid'] = False
        receipt.write_text(json.dumps(data))
        # A well-hashed but unsupported receipt must still fail the receipt contract.
        record['artifacts']['validation_json']['sha256'] = cached_stages._file_hash(receipt)
        manifest.write_text(json.dumps(record))
    run()
    assert producer.call_count == 2
    assert validator.call_count == 2
    assert not restored.called
    assert cache.diagnostics['read']['status'] == 'miss'


def test_failed_validation_cannot_replace_success_receipt(tmp_path, monkeypatch):
    cache, producer, validator, restored, run = cached_case(tmp_path, monkeypatch)
    run()
    manifest = cache.cache.manifest_path('case', 'read')
    previous = manifest.read_bytes()
    monkeypatch.setattr(cached_stages, 'model_version', lambda stage: 'validator-v2')
    validator.return_value = {'valid': False, 'errors': ['invalid']}
    with pytest.raises(RuntimeError, match='validation'):
        run()
    assert manifest.read_bytes() == previous


def test_original_api_still_validates_on_each_hit(tmp_path, monkeypatch):
    cache, producer, validator, restored, _ = cached_case(tmp_path, monkeypatch)
    for _ in range(2):
        cache.run('read', {}, {}, producer, validator, restore_model, generation=True)
    assert producer.call_count == 1
    assert validator.call_count == 2


def test_pipeline_hit_restores_diagnostics_without_semantic_model_validation(tmp_path, monkeypatch):
    manifest_path, root = make_case(tmp_path)
    manifest = json.loads(manifest_path.read_text())
    config = manifest['runs'][0]
    first = pipeline.run_one(config, manifest, root, False, True, 'tool')
    for name in ('validate', 'validate_write_evidence_model'):
        monkeypatch.setattr(pipeline, name, Mock(side_effect=AssertionError('semantic validator called')))
    for name in ('triage', 'bottleneck', 'numa', 'write'):
        (root / 'runs/case' / (name + '.validation.json')).unlink(missing_ok=True)
    second = pipeline.run_one(config, manifest, root, False, True, 'tool')
    for name in ('triage', 'bottleneck', 'numa', 'write'):
        assert second['validation'][name] == first['validation'][name]
        assert json.loads((root / 'runs/case' / (name + '.validation.json')).read_text()) == first['validation'][name]
    assert all(item['status'] == 'hit' for item in second['validation']['cache'].values())
