"""Interrupted input preservation and incomplete cache records recover without losing publication."""
import json
from pathlib import Path
from unittest.mock import Mock

import pytest

from test_ds_trace_stage_resume import make_case
from trace_analysis import pipeline
from trace_analysis.stage_cache import StageCache


@pytest.mark.parametrize('stage,field', [('read', 'analysis_json'), ('triage', 'summary_json'),
                                        ('triage', 'events.jsonl')])
def test_missing_required_cache_record_entry_rebuilds(tmp_path, monkeypatch, stage, field):
    manifest, output = make_case(tmp_path)
    pipeline.run_pipeline(manifest, output, False)
    cache = StageCache(output / '.stage-cache')
    record = cache.manifest_path('case', stage)
    data = json.loads(record.read_text())
    del data['artifacts'][field]
    record.write_text(json.dumps(data))
    name = 'run_' + stage
    producer = Mock(wraps=getattr(pipeline.stages, name))
    monkeypatch.setattr(pipeline.stages, name, producer)
    result = pipeline.run_pipeline(manifest, output, False, resume=True)
    assert producer.call_count == 1
    assert result['validation']['runs']['case']['cache'][stage]['reason'] == 'artifact_contract_invalid'


def test_corrupted_preserved_input_rebuilds_from_original(tmp_path):
    manifest, output = make_case(tmp_path)
    first = pipeline.run_pipeline(manifest, output, False)
    archive = output / first['runs'][0]['input_archive']
    archive.write_bytes(b'truncated copy')
    result = pipeline.run_pipeline(manifest, output, False, resume=True)
    assert result['validation']['valid']
    assert archive.read_bytes() == (tmp_path / 'input.tar.gz').read_bytes()


def test_interrupted_input_copy_does_not_publish_partial_archive(tmp_path, monkeypatch):
    manifest, output = make_case(tmp_path)
    first = pipeline.run_pipeline(manifest, output, False)
    archive = output / first['runs'][0]['input_archive']
    archive.unlink()
    entry = (output / 'index.html').read_bytes()
    copy = pipeline.shutil.copy2
    def interrupted(source, destination, *args, **kwargs):
        if Path(destination).is_relative_to(output / 'inputs'):
            Path(destination).write_bytes(b'partial')
            raise KeyboardInterrupt('interrupted archive copy')
        return copy(source, destination, *args, **kwargs)
    monkeypatch.setattr(pipeline.shutil, 'copy2', interrupted)
    with pytest.raises(KeyboardInterrupt):
        pipeline.run_pipeline(manifest, output, False, resume=True)
    assert not archive.exists()
    assert (output / 'index.html').read_bytes() == entry
    monkeypatch.setattr(pipeline.shutil, 'copy2', copy)
    assert pipeline.run_pipeline(manifest, output, False, resume=True)['validation']['valid']
