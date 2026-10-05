"""Contracts found by direct chapter navigation and grouped archive reports."""
from trace_test_loader import REPO_ROOT
import io
import json
import sys
import tarfile
from pathlib import Path

import pytest

sys.path.insert(0, str(REPO_ROOT / 'scripts'))
from trace_analysis.archive import collection_cohort
from trace_analysis.bottleneck import _build_worker_correlation
from trace_analysis.suite import _archive_trace_bands
from trace_analysis.overview import summarize_run


@pytest.mark.parametrize('prefix', ['', 'trace_collect/', 'outer/trace_collect/'])
def test_cohorts_do_not_depend_on_archive_root(prefix):
    assert collection_cohort(prefix + 'all-core/1001/read-one') == 'core/1001'
    assert collection_cohort(prefix + 'time-buckets/GET_5000,7000/read-one') == 'time/GET_5000_7000'
    assert collection_cohort(prefix + 'time-buckets/GET_5000,7000/unique_traces_GET.txt') is None


def test_archive_indices_and_duplicate_members_do_not_inflate_counts(tmp_path):
    path = tmp_path / 'input.tar.gz'
    with tarfile.open(path, 'w:gz') as archive:
        for name in ['time-buckets/GET_5000,7000/a', 'time-buckets/GET_5000,7000/a',
                     'time-buckets/GET_5000,7000/unique_traces_GET.txt']:
            entry = tarfile.TarInfo(name)
            entry.size = 1
            archive.addfile(entry, io.BytesIO(b'x'))
    mapping, counts = _archive_trace_bands(path)
    assert mapping == {'a': '5–7ms'}
    assert counts == {'5–7ms': 1}


def test_worker_local_phases_are_visible_without_inventing_rpc_network():
    row = {'trace_id': 'read-one', 'client_ms': 12, 'evidence_records': [], 'urma_requests': [],
           'query_and_get_breakdown': {'worker': [
               {'owner': ['192.0.2.10', '1'], 'timestamp': '2026-01-01T00:00:00.000000',
                'total_ms': 10, 'phases_ms': {'localRead': 8, 'metadata': 0}}]}}
    result = _build_worker_correlation([row])
    assert {e['dimension'] for e in result['events']} == {'rpc', 'metadata', 'data'}
    assert {e['worker'] for e in result['events']} == {'192.0.2.10'}
    assert all(e['network_ms'] is None and e['status_observed'] is False for e in result['events'])
    assert next(e for e in result['events'] if e['kind'] == 'query_local_read')['latency_ms'] == 8
    assert result['time_buckets'][0]['data']['local_count'] == 1
    assert result['workers'][0]['failure_count'] == 0


def test_overview_consumes_refined_write_rows_and_rejects_duplicate_ids():
    raw = {'traces': [], 'write_traces': [{'trace_id': 'write-one', 'client_ms': 9, 'status': 1}]}
    refined = {'rows': [{'trace_id': 'write-one', 'client_ms': 9, 'status': 1,
                          'write_primary_stage': 'Publish RPC其他'}]}
    result = summarize_run({'id': 'test'}, raw, refined, {})
    assert result['write']['count'] == result['write']['failed'] == 1
    assert result['write']['problems'] == {'Publish RPC其他': 1}
    assert result['numa']['slow_wr'] is None
    with pytest.raises(ValueError, match='duplicate write'):
        summarize_run({'id': 'test'}, raw, {'rows': refined['rows'] * 2}, {})


def test_correlation_single_point_and_worker_reset_are_supported():
    import subprocess
    asset = REPO_ROOT / 'scripts/trace_analysis/assets/read/read_correlation.js'
    script = asset.read_text()
    harness = r'''
const assert=require('assert');const nodes={};const charts=[];
const $=id=>nodes[id]||(nodes[id]={innerHTML:'',dataset:{},value:''});
const seen={};const AGG={worker_correlation:{events:[],slow_wr_threshold_ms:1.5}};
const echarts={getInstanceByDom:()=>null};
const TraceCharts={init:(lib,node)=>({setOption:o=>seen[Object.keys(nodes).find(k=>nodes[k]===node)]=o})};
const e={worker:'worker',timestamp:'2026-01-01T00:00:00',dimension:'data',kind:'query_local_read',latency_ms:5};
renderWorkerCorrelationCharts(buildCorrelationBuckets([e]));
const o=seen['worker-correlation-chart-data'];assert(o.series.some(s=>s.type==='line'&&s.showSymbol));
renderWorkerCorrelationCharts([]);assert(nodes['worker-correlation-chart-data'].dataset.emptyReason==='not_collected');
renderWorkerCorrelationCharts(buildCorrelationBuckets([e]));assert(!nodes['worker-correlation-chart-data'].dataset.emptyReason);
assert(latencyBandMatches(2.5,'2-3'));assert(!latencyBandMatches(3.5,'2-3'));
'''
    subprocess.run(['node', '-e', script + harness], check=True)


def test_indexed_companions_match_same_worker_brute_force():
    import datetime as dt
    rows = []
    origin = dt.datetime(2026, 1, 1)
    for index in range(80):
        stamp = (origin + dt.timedelta(milliseconds=index * 100)).isoformat(timespec='microseconds')
        worker = '192.0.2.' + str(index % 3 + 1)
        rows.append({'trace_id': str(index), 'client_ms': 4, 'evidence_records': [{
            'worker': worker, 'text': stamp + ' [Get] Remote done failed cost: 4ms'}],
            'urma_requests': [{'source_worker': worker, 'timestamp': stamp, 'total_ms': 2,
                               'is_slow': True, 'status': 'OK'}]})
    events = _build_worker_correlation(rows)['events']
    for event in events:
        if event['companions'] is None:
            continue
        stamp = dt.datetime.fromisoformat(event['timestamp'])
        neighbors = [candidate for candidate in events
                     if candidate['worker'] == event['worker']
                     and candidate['source_event_id'] != event['source_event_id']
                     and abs((dt.datetime.fromisoformat(candidate['timestamp']) - stamp).total_seconds()) <= 1]
        expected = sum(candidate['kind'] == 'urma_wr' and candidate['is_slow'] for candidate in neighbors)
        assert event['companions']['slow_wr_count'] == expected
        assert event['companions']['same_trace_event_count'] == 1


def test_pipeline_resume_checks_outputs_and_configuration(tmp_path, monkeypatch):
    from trace_analysis import pipeline
    root = tmp_path / 'report'
    artifact = root / 'runs/example/read.html'
    artifact.parent.mkdir(parents=True)
    artifact.write_text('original')
    archive = tmp_path / 'input.tar.gz'
    archive.write_bytes(b'archive')
    cfg = {'id': 'example', 'inputs': [str(archive)], 'input_archive': str(archive)}
    manifest = {'source_ref': 'head', 'source_base': 'base'}
    result = {'run': {'bottleneck_report': 'runs/example/read.html'}, 'validation': {}}
    checkpoint = artifact.parent / 'pipeline.checkpoint.json'
    checkpoint.write_text(json.dumps({'fingerprint': pipeline.input_fingerprint(cfg, manifest, 'tool'),
                                     'result': result, 'artifacts': pipeline.artifact_hashes(result['run'], root)}))
    def restarted(*args, **kwargs):
        raise RuntimeError('analysis restarted')
    monkeypatch.setattr(pipeline.stages, 'run_triage', restarted)
    with pytest.raises(RuntimeError, match='analysis restarted'):
        pipeline.run_one(cfg, manifest, root, False, True, 'tool')
    artifact.write_text('modified')
    with pytest.raises(RuntimeError, match='analysis restarted'):
        pipeline.run_one(cfg, manifest, root, False, True, 'tool')
    checkpoint.write_text('{')
    with pytest.raises(RuntimeError, match='analysis restarted'):
        pipeline.run_one(cfg, manifest, root, False, True, 'tool')
    assert pipeline.input_fingerprint(cfg, manifest, 'tool') != pipeline.input_fingerprint(cfg, {**manifest, 'source_ref': 'other'}, 'tool')


def test_parallel_run_failure_keeps_completion_gate_closed(tmp_path, monkeypatch):
    import threading
    from trace_analysis import pipeline
    archive = tmp_path / 'input.tar.gz'
    with tarfile.open(archive, 'w:gz') as stream:
        member = tarfile.TarInfo('trace.log')
        member.size = 1
        stream.addfile(member, io.BytesIO(b'x'))
    manifest = tmp_path / 'manifest.json'
    manifest.write_text(json.dumps({'schema_version': 1, 'source_head': 'head', 'source_base': 'base',
                                    'runs': [{'id': run_id, 'inputs': [str(archive)],
                                              'input_archive': str(archive)} for run_id in ('one', 'two')]}))
    barrier = threading.Barrier(2)
    def run_one(config, *args):
        barrier.wait(timeout=3)
        if config['id'] == 'two':
            raise ValueError('invalid model')
        return {'run': config, 'validation': {'valid': True}}
    monkeypatch.setattr(pipeline, 'run_one', run_one)
    with pytest.raises(RuntimeError, match='failed Runs: two'):
        pipeline.run_pipeline(manifest, tmp_path / 'output', False, jobs=2)
    gate = json.loads((tmp_path / 'output/pipeline.validation.json').read_text())
    assert gate['valid'] is False
    assert gate['runs']['one']['valid'] is True
    assert gate['runs']['two']['valid'] is False


def test_worker_phase_missing_owner_and_time_remain_auditable():
    row = {'trace_id': 'unknown', 'client_ms': 5, 'query_and_get_breakdown': {'worker': [
        {'total_ms': 4, 'phases_ms': {'localRead': 3, 'metadata': float('nan')}}]}}
    result = _build_worker_correlation([row])
    assert result['unassigned_event_count'] == result['untimed_event_count'] == 1
    assert not result['time_buckets']
    assert len(result['events']) == 2


def test_render_only_rejects_overlapping_artifacts_before_parallel_writes(tmp_path):
    from trace_analysis.render_bundle import validate_targets
    fields = ('triage_report', 'bottleneck_report', 'write_bottleneck_report', 'numa_report',
              'analysis_json', 'write_analysis_json', 'numa_analysis_json')
    cfg = {'id': 'one', **{key: 'one/' + key for key in fields}}
    validate_targets({'runs': [cfg]}, tmp_path)
    with pytest.raises(ValueError, match='independent'):
        validate_targets({'runs': [cfg, {**cfg, 'id': 'two'}]}, tmp_path)
    with pytest.raises(ValueError, match='unique Run'):
        validate_targets({'runs': [cfg, cfg]}, tmp_path)
