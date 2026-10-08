"""Pipeline Top selection is presentation-only; legacy analysis limits remain explicit."""
from trace_test_loader import REPO_ROOT
import io
import json
import tarfile
from pathlib import Path
from unittest.mock import Mock

import pytest

from test_ds_trace_stage_resume import make_case
from test_ds_trace_bottleneck import trace
from trace_analysis import pipeline, stages, bottleneck
from trace_analysis.ingest.inventory import input_identity


def test_pipeline_top_keeps_over_1000_get_and_set_ids_and_denominators(tmp_path, monkeypatch):
    traces = {}
    for flow in ('GET', 'SET'):
        for index in range(1001):
            identity = f'{flow}-{index}'
            row = trace(identity, 6, 0, timestamp='2026-08-15T10:00:00.000001')
            row['flows'] = {f'DS_KV_CLIENT_{flow}': 1, f'DS_POSIX_{flow}': 1}
            traces[identity] = row
    archive = tmp_path / 'inputs.tar.gz'
    with tarfile.open(archive, 'w:gz') as package:
        for identity in traces:
            member = tarfile.TarInfo(f'time/GET_5000_7000/{identity}')
            member.size = 1
            package.addfile(member, io.BytesIO(b'x'))
    def normalized_triage(cfg, manifest, target, *args, **kwargs):
        directory = target / 'triage'
        directory.mkdir()
        summary = {'traces': traces, 'dimensions': {}, 'trace_count': len(traces)}
        identity = input_identity(archive)
        inputs = [identity]
        inventory = {'schema_version': 1, 'run_id': cfg['id'], 'inputs': inputs,
                     'input_count': len(inputs), 'listed_member_count': len(identity['members']),
                     'total_bytes': identity['size']}
        for name, data in [('summary.json', summary), ('parsed_traces.json', summary),
                           ('manifest.json', {'run_id': cfg['id'], 'inputs': inputs}),
                           ('inventory.json', inventory), ('triage.json', {})]:
            (directory / name).write_text(json.dumps(data))
        (directory / 'events.jsonl').write_text('')
        return stages.StageResult({'run_dir': directory, 'summary_json': directory / 'summary.json'})
    monkeypatch.setattr(stages, 'run_triage', normalized_triage)
    root = tmp_path / 'report'
    manifest = {'source_head': 'head', 'source_base': 'base'}
    cfg = {'id': 'all', 'inputs': [str(archive)], 'input_archive': str(archive), 'top': 100}
    first = pipeline.run_one(cfg, manifest, root, False, True, 'tool')
    model_paths = [root / first['run'][key] for key in
                   ('triage_json', 'analysis_json', 'write_analysis_json', 'numa_analysis_json')]
    original = [path.read_bytes() for path in model_paths]
    read, write, numa = [json.loads(path.read_text()) for path in model_paths[1:]]
    assert {row['trace_id'] for row in read['traces']} == {f'GET-{i}' for i in range(1001)}
    assert {row['trace_id'] for row in write['rows']} == {f'SET-{i}' for i in range(1001)}
    assert len(numa['traces']) == 2002
    for name in ('run_triage', 'run_read', 'run_write', 'run_numa_from_evidence'):
        monkeypatch.setattr(stages, name, Mock(side_effect=AssertionError('view change reanalyzed')))
    for top in (1000, 0, 100):
        cfg['top'] = top
        result = pipeline.run_one(cfg, manifest, root, False, True, 'tool')
        assert result['run']['view']['read_top'] == top
        assert all(item['status'] == 'hit' for item in result['validation']['cache'].values())
        assert [path.read_bytes() for path in model_paths] == original
    suite_manifest = root / 'test.suite.json'
    suite_manifest.write_text(json.dumps({'schema_version': 1, 'runs': [result['run']]}))
    suite = stages.suite.build_suite(stages.suite.load_manifest(suite_manifest))
    assert suite['runs'][0]['read_summary']['trace_count'] == 1001
    assert suite['runs'][0]['write_summary']['trace_count'] == 1001


def test_pipeline_publication_carries_view_default_without_query_in_artifact_paths(tmp_path):
    manifest, root = make_case(tmp_path)
    config = json.loads(manifest.read_text())
    config['runs'][0]['top'] = 100
    manifest.write_text(json.dumps(config))
    result = pipeline.run_pipeline(manifest, root, False)
    run = result['runs'][0]
    assert '?' not in run['bottleneck_report']
    assert 'data-read-top="100"' in (root / run['bottleneck_report']).read_text()
    assert json.loads((root / run['analysis_json']).read_text())['top_requested'] == 0
    saved = (root / run['analysis_json']).read_bytes()
    for selected in (1000, 0):
        config['runs'][0]['top'] = selected
        manifest.write_text(json.dumps(config))
        refreshed = pipeline.run_pipeline(manifest, root, False, resume=True)
        assert all(state['status'] == 'hit' for state in refreshed['validation']['runs']['case']['cache'].values())
        assert (root / refreshed['runs'][0]['analysis_json']).read_bytes() == saved
        page = root / refreshed['runs'][0]['bottleneck_report']
        assert f'data-read-top="{selected}"' in page.read_text()


@pytest.mark.parametrize('value', [-1, 50, 1.5, True, 'bogus', None])
def test_invalid_pipeline_view_fails_before_producers(tmp_path, monkeypatch, value):
    manifest, root = make_case(tmp_path)
    config = json.loads(manifest.read_text())
    config['runs'][0]['top'] = value
    manifest.write_text(json.dumps(config))
    producer = Mock(side_effect=AssertionError('producer must not run'))
    monkeypatch.setattr(stages, 'run_triage', producer)
    with pytest.raises(ValueError, match='top'):
        pipeline.run_pipeline(manifest, root, False)
    assert not producer.called


@pytest.mark.parametrize('config,expected', [({}, 0), ({'top': '100'}, 100),
                                            ({'view': {'read_top': 1000}}, 1000),
                                            ({'top': 100, 'view': {'read_top': 100}}, 100)])
def test_pipeline_view_normalization(config, expected):
    original = json.dumps(config, sort_keys=True)
    assert pipeline._read_view(config)['read_top'] == expected
    assert json.dumps(config, sort_keys=True) == original


@pytest.mark.parametrize('config', [{'top': 100, 'view': {'read_top': 0}},
                                   {'view': None}, {'view': {'read_top': True}}])
def test_pipeline_rejects_conflicting_or_invalid_view(config):
    with pytest.raises(ValueError, match='view|top'):
        pipeline._read_view(config)


def test_standalone_cli_and_stage_preserve_legacy_input_limit(tmp_path):
    import subprocess
    import sys
    from test_ds_trace_bottleneck import run_dir as fixture
    directory = fixture.__wrapped__(tmp_path)
    result = stages.run_read(directory, {'top': 1}, {}, tmp_path / 'stage', models_only=True)
    assert len(json.loads(result.artifacts['analysis_json'].read_text())['traces']) == 1
    script = REPO_ROOT / 'scripts/ds_trace_analysis.py'
    output = tmp_path / 'legacy.json'
    command = [sys.executable, str(script), 'read', '--run-dir', str(directory), '--top', '1',
               '--skip-write-page', '--output', str(tmp_path / 'legacy.html'), '--analysis-json', str(output)]
    completed = subprocess.run(command, capture_output=True, text=True, timeout=30)
    assert completed.returncode == 0, completed.stderr
    assert len(json.loads(output.read_text())['traces']) == 1
