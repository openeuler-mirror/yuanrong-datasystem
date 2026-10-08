"""Attribution consumes persisted observations without reparsing source messages."""
from trace_test_loader import REPO_ROOT
import ast
import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(REPO_ROOT / 'scripts'))
from trace_analysis.evidence import observations
from trace_analysis.analysis import read, read_initial


def test_observations_preserve_rpc_transport_and_source_positions():
    texts = [
        'method=MasterOCService.QueryAndGet e2e_us=8000 server_exec_us=6000 cntl_failed=0',
        '[TransportGet] phasesUs={data_transfer:4000,connection_acquire:100}',
        'Local processing done remoteObjects:0 costUs:1200 RemoteLockEntry: 0.25 ms',
    ]
    facts = observations.build_evidence_facts(texts, [])
    assert facts['schema_version'] == 1
    assert facts['rpc_entries'][0]['method'] == 'MasterOCService.QueryAndGet'
    assert facts['rpc_entries'][0]['fields']['e2e'] == 8000
    assert facts['rpc_entries'][0]['source_ref'] == {'collection': 'evidence', 'index': 0}
    assert facts['transport_phase_maps'] == [{'data_transfer': 4000, 'connection_acquire': 100}]
    assert facts['local_processing'] == {'remote_objects': 0, 'cost_us': 1200}
    assert facts['remote_lock_ms'] == 0.25
    assert facts['durations_ms']['client_transfer'] == 4
    assert json.loads(json.dumps(facts)) == facts


def test_missing_observations_stay_unknown_and_attempts_keep_worker():
    raw = '2026-09-22T18:56:09.140089 QueryAndGet done, inlineHit:1 transport:UB localRead:2ms total:3ms'
    records = [{'worker': 'worker-a', 'text': raw, 'source': 'a.log', 'line': 4},
               {'worker': 'worker-b', 'text': raw, 'source': 'b.log', 'line': 9}]
    facts = observations.build_evidence_facts([], records)
    assert [v['worker'] for v in facts['inline_attempts']] == ['worker-a', 'worker-b']
    assert facts['inline_attempts'][1]['source_ref']['index'] == 1
    assert facts['inline_attempts'][0]['timestamp'] == '2026-09-22T18:56:09.140089'
    assert facts['durations_ms']['provider_pull'] is None
    assert facts['local_processing'] is None
    assert facts['remote_lock_ms'] is None


def test_authoritative_facts_do_not_reparse_raw_evidence(monkeypatch):
    row = {'evidence': ['method=MasterOCService.QueryAndGet e2e_us=8000'],
           'query_meta_ms': 8, 'urma_requests': []}
    facts = observations.facts_for(row)
    monkeypatch.setattr(observations, 'build_evidence_facts',
                        lambda *args: pytest.fail('persisted observations were reparsed'))
    assert observations.facts_for(row) is facts
    detail = read._query_meta_detail(row)
    assert detail['rpc_e2e_ms'] == 8


def test_read_rules_do_not_import_raw_log_parsers():
    for module in (read, read_initial):
        source = Path(module.__file__).read_text()
        tree = ast.parse(source)
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                assert all(alias.name != 're' for alias in node.names)
            if isinstance(node, ast.ImportFrom):
                assert all(alias.name not in {'_rpc_fields', '_transport_phase_maps', '_evidence_timestamp'}
                           for alias in node.names)
        assert 'row.get("evidence"' not in source
        assert 'row["evidence"]' not in source


@pytest.mark.parametrize('version', [True, 2, '1'])
def test_unsupported_fact_schema_is_rejected(version):
    with pytest.raises(ValueError, match='schema'):
        observations.facts_for({'evidence_facts': {'schema_version': version}})


@pytest.mark.parametrize('damage', ['duration', 'reference', 'missing', 'version'])
def test_persisted_model_validator_rejects_corrupt_facts(damage):
    from trace_analysis.validation import validate_data
    row = {'trace_id': 't', 'evidence': ['method=MasterOCService.QueryAndGet e2e_us=8000'],
           'evidence_records': []}
    facts = observations.facts_for(row)
    if damage == 'duration':
        facts['durations_ms']['client_transfer'] = float('nan')
    elif damage == 'reference':
        facts['rpc_entries'][0]['source_ref']['index'] = 7
    elif damage == 'missing':
        del facts['query_attempts']
    else:
        facts['schema_version'] = True
    result = validate_data({'traces': [row], 'aggregate': {}}, 'bottleneck', 'memory')
    assert not result['valid']
    assert any('evidence_facts' in message for message in result['errors'])


@pytest.mark.parametrize('key', ['local_processing', 'remote_lock_ms', 'worker_query_done_observed',
                                 'legacy_pull_src_sentinel'])
def test_observed_fact_requires_provenance(key):
    from trace_analysis.evidence.observation_validation import evidence_fact_errors
    row = {'evidence': ['Local processing done remoteObjects:0 costUs:100 RemoteLockEntry: 1ms',
                        'QueryAndGet done, localRead:1ms', 'Processing pull object src=:-1']}
    facts = observations.facts_for(row)
    del facts['observation_source_refs'][key]
    assert evidence_fact_errors(row)


def test_timeout_identity_must_be_scalar():
    from trace_analysis.evidence.observation_validation import evidence_fact_errors
    row = {'evidence_records': [{'worker': 'w', 'text': '[URMA_WAIT_TIMEOUT] elapsedMs=5'}]}
    observations.facts_for(row)['timeout_events'][0]['request_id'] = {'invalid': 'request'}
    assert evidence_fact_errors(row)


def test_attempt_provenance_survives_compact_persisted_analysis(tmp_path):
    from trace_analysis.bottleneck import build_analysis
    from trace_analysis.validation import validate_data
    from test_ds_trace_bottleneck import trace
    item = trace('t', 9, 7, timestamp='2026-09-22T18:56:09.140089', query_meta_ms=3)
    item['evidence'].append({'source': 'w.log', 'member': 'worker-a/w.log', 'line': 12,
                             'worker': 'worker-a', 'text':
                             '2026-09-22T18:56:09.140089 QueryAndGet done, inlineHit:1 transport:UB localRead:2ms total:3ms'})
    for name, value in [('manifest.json', {}), ('triage.json', {}),
                        ('summary.json', {'traces': {'t': item}, 'dimensions': {}})]:
        (tmp_path / name).write_text(json.dumps(value))
    model = build_analysis(tmp_path, top_n=0)
    row = model['traces'][0]
    assert row['evidence_facts']['inline_attempts']
    assert validate_data(model, 'bottleneck', 'roundtrip')['valid']


def test_read_page_does_not_duplicate_model_only_observations():
    import copy
    from test_ds_trace_bottleneck import trace
    from trace_analysis import bottleneck
    row = bottleneck.build_trace_rows({'traces': {'t': trace(
        't', 9, 7, timestamp='2026-09-22T18:56:09.140089', query_meta_ms=3)}})[0]
    row['evidence_facts']['test_model_only_marker'] = 'MODEL_ONLY_OBSERVATION_MARKER'
    analysis = {'traces': [row], 'aggregate': bottleneck.aggregate([row]), 'metadata': {}}
    before = copy.deepcopy(analysis)
    html = bottleneck.render_html(analysis, 'test')
    assert 'MODEL_ONLY_OBSERVATION_MARKER' not in html
    assert analysis == before


@pytest.mark.parametrize('collection', [[], {}, None])
def test_malformed_source_collection_is_a_validation_error(collection):
    from trace_analysis.evidence.observation_validation import evidence_fact_errors
    row = {'evidence': ['method=MasterOCService.QueryAndGet e2e_us=8000']}
    observations.facts_for(row)['rpc_entries'][0]['source_ref']['collection'] = collection
    assert evidence_fact_errors(row)
