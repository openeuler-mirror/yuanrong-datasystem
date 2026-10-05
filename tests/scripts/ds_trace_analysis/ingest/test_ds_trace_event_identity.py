"""Persist event identity from each log's provenance, never the Trace worker aggregate."""
from trace_test_loader import REPO_ROOT
import copy
import sys
from pathlib import Path

sys.path.insert(0, str(REPO_ROOT / 'scripts'))
from trace_analysis.analysis.triage_artifacts import build_events


def evidence(role, host, pid, second, *, member=None, source='run.tar.gz', text=None):
    return {'source': source, 'member': member or f'{role}/kvcache.INFO.log', 'line': second + 1,
            'worker': f'{role}-instance', 'host_ip': host,
            'text': text or (f'2026-09-30T01:00:{second:02d}.000000 | I | urma_manager.cpp:10 | '
                            f'{host} | {pid}:7 | t1 | jingpai | event')}


def report(items, scope='run-a'):
    return {'run_scope': scope, 'traces': {'t1': {'workers': {'wrong-worker': 99}, 'evidence': items}}}


def test_client_and_each_worker_are_identified_from_their_own_line():
    items=[evidence('client','10.0.0.1',11,0),evidence('worker-a','10.0.0.2',22,1),
           evidence('worker-b','10.0.0.3',33,2)]
    model=report(items);before=copy.deepcopy(model);events=build_events(model)
    assert [e['role'] for e in events] == ['client','worker','worker']
    assert [e['worker_id'] for e in events] == [None,'worker-a-instance','worker-b-instance']
    assert [e['host_ip'] for e in events] == ['10.0.0.1','10.0.0.2','10.0.0.3']
    assert [e['process_id'] for e in events] == ['11','22','33']
    assert all(e['component']=='URMA' and e['schema_version']==2 for e in events)
    assert all(e['trace_key']=={'run_scope':'run-a','trace_id':'t1'} for e in events)
    assert model==before


def test_duplicates_keep_sources_and_ids_are_run_scoped():
    a=evidence('worker-a','10.0.0.2',22,0,member='worker-a/core.log')
    b={**a,'member':'worker-a/time.log','line':9}
    first=build_events(report([a,b]));second=build_events(report([a,b],'run-b'))
    assert len(first)==len(second)==1
    assert len(first[0]['source_refs'])==2
    assert first[0]['event_id']!=second[0]['event_id']
    assert first[0]['event_id']==build_events(report([b,a]))[0]['event_id']


def test_elapsed_is_per_process_and_output_does_not_globally_sort():
    items=[evidence('worker-a','10.0.0.2',22,5),evidence('worker-b','10.0.0.3',33,0),
           evidence('worker-a','10.0.0.2',22,3)]
    events=build_events(report(items))
    assert [e['process_id'] for e in events]==['22','33','22']
    assert [e['process_relative_ms'] for e in events]==[2000,0,0]
    assert [e['observed_gap_ms'] for e in events]==[2000,None,None]
    assert events[0]['time_domain']!=events[1]['time_domain']
    assert events[0]['time_basis']=='process_local_wall_clock'
    assert events[0]['process_instance_verified'] is False
    assert all(e['process_elapsed_ms'] is None for e in events)


def test_unknown_fields_are_null_with_reasons_not_aggregate_guesses():
    events=build_events(report([{'source':'bundle','member':'unknown.log','line':1,'text':'no clock'}]))
    event=events[0]
    for key in ['role','worker_id','process_id','host_ip','wall_time','process_elapsed_ms','time_domain']:
        assert event[key] is None
        assert event['missing_reasons'][key]
    assert event['worker'] is None
    assert event['raw']=='no clock' and event['event_type']=='raw'


def test_unknown_process_does_not_merge_separate_log_origins():
    a={'source':'bundle','member':'a.log','line':1,'text':'same'}
    b={**a,'member':'b.log'}
    assert len(build_events(report([a,b])))==2


def test_derived_ub_event_uses_own_evidence_and_keeps_legacy_fields():
    item=evidence('worker-b','10.0.0.3',33,2)
    data=report([])
    data['traces']['t1']['ub_events']=[{**item,'raw':item['text'],'event_type':'total',
                                      'request_id':'17','cost_ms':2.6}]
    event=build_events(data)[0]
    assert event['event_type']=='ub_total' and event['request_id']=='17' and event['cost_ms']==2.6
    assert event['process_id']=='33' and event['worker_id']=='worker-b-instance'
    assert len(event['source_refs'])==1


def test_wall_clock_regression_in_source_order_is_not_repaired_by_sorting():
    first=evidence('worker-a','10.0.0.2',22,5);first['line']=1
    second=evidence('worker-a','10.0.0.2',22,3);second['line']=2
    events=build_events(report([second,first]))
    assert all(e['process_relative_ms'] is None and e['observed_gap_ms'] is None for e in events)
    assert all(e['missing_reasons']['process_relative_ms']=='wall_clock_regression' for e in events)


def test_wr_elapsed_is_not_process_elapsed_and_unscoped_is_explicit():
    item=evidence('worker-a','10.0.0.2',22,0)
    item['text']+=' elapsedMs:5, process_elapsed_ms:170'
    event=build_events({'traces':{'t1':{'evidence':[item]}}})[0]
    assert event['run_scope'] is None and event['scope_kind']=='unscoped'
    assert event['missing_reasons']['run_scope']=='run_id_not_provided'
    assert event['process_elapsed_ms']==170
    item['text']=item['text'].replace(', process_elapsed_ms:170','')
    assert build_events(report([item]))[0]['process_elapsed_ms'] is None


def test_same_input_and_case_distinct_run_ids_do_not_reuse_scope(tmp_path):
    from trace_analysis import triage
    source=tmp_path/'worker.log'
    source.write_text(evidence('worker-a','10.0.0.2',22,0)['text']+'\n')
    pipeline=triage.TraceRunPipeline()
    first=pipeline.parse([source],tmp_path/'output',triage.RunOptions(case_name='same',run_id='run-a'))
    second=pipeline.parse([source],tmp_path/'output',triage.RunOptions(case_name='same',run_id='run-b'))
    assert first!=second
    import json
    a=json.loads((first/'events.jsonl').read_text().splitlines()[0])
    b=json.loads((second/'events.jsonl').read_text().splitlines()[0])
    assert (a['run_scope'],b['run_scope'])==('run-a','run-b')
    assert a['event_id']!=b['event_id']
    assert json.loads((first/'manifest.json').read_text())['run_id']=='run-a'
    assert pipeline.parse([source],tmp_path/'output',triage.RunOptions(case_name='same',run_id='run-a'))==first


def test_absent_run_id_keeps_legacy_cache_identity(tmp_path):
    from trace_analysis.orchestration.contracts import RunOptions
    from trace_analysis.orchestration.store import TraceRunStore, build_cache_key
    source = tmp_path / 'worker.log'
    source.write_text('line\n')
    store = TraceRunStore(version_provider=lambda: 'fixed-version')
    options = RunOptions('case', 'scenario', 'ref')
    expected, _ = build_cache_key([source], 'ref', 'case', 'scenario',
                                  identity_provider=store.inventory.identity,
                                  version_provider=store.version_provider)
    prepared = store.prepare_parse_run([source], tmp_path / 'output', options)
    assert prepared['cache_key'] == expected


def test_original_line_provenance_and_worker_identity_survive_duplicate_exports():
    a = evidence('worker-a', '10.0.0.2', 22, 0)
    a['text'] = '/logs/worker-a/worker.log:42:' + a['text']
    b = {**a, 'member': 'time/trace.txt', 'source': 'second.tar.gz'}
    event = build_events(report([a, b]))[0]
    assert len(event['source_refs']) == 2
    assert all(ref['original_member'] == '/logs/worker-a/worker.log' for ref in event['source_refs'])
    assert all(ref['original_line'] == 42 for ref in event['source_refs'])
    c = evidence('worker-b', '10.0.0.2', 22, 0)
    events = build_events(report([a, c]))
    assert len(events) == 2
    assert events[0]['process_key'] != events[1]['process_key']


def test_original_worker_log_wins_over_client_cohort_and_case_name():
    original = ('/logs/case_half_worker_1client/'
                'collected_worker_logs/worker_192.0.2.25/kvcache.INFO.20260101062323_001.log')
    item = {'source': '/archives/1client.tar.gz',
            'member': 'time-buckets/DS_KV_CLIENT_GET_10000,20000/getBuffer-test-1;synthetic',
            'line': 1, 'worker': '192.0.2.25',
            'text': original + ':42:2026-01-01T06:21:52.000001 | I | urma_manager.cpp:1700 | '
                    '192.0.2.25 | 9:26 | getBuffer-test-1;synthetic | jingpai | '
                    '[SLOW LOG] [URMA_ELAPSED_TOTAL] [urma_request_id:19] urma post to completion cost: 2ms'}
    event = build_events(report([item]))[0]
    assert event['role'] == 'worker'
    assert event['worker_id'] == '192.0.2.25'
    assert event['component'] == 'URMA'
    assert event['source_refs'][0]['original_member'] == original
    assert (event['process_id'], event['thread_id']) == ('9', '26')


def test_cohort_and_archive_names_are_not_log_process_roles():
    item = {'source': '/archives/worker-client.tar.gz',
            'member': 'time-buckets/DS_KV_CLIENT_GET_10000,20000/trace',
            'text': '2026-01-01T06:21:52.000001 | I | urma_manager.cpp:1700 | '
                    '192.0.2.25 | 9:26 | t1 | jingpai | URMA_ELAPSED_TOTAL'}
    event = build_events(report([item]))[0]
    assert event['role'] is None and event['worker_id'] is None
    assert event['missing_reasons']['role'] == 'not_observed_in_line'


def test_structured_original_member_and_closest_client_directory_take_precedence():
    item = evidence('worker-wrong', '10.0.0.1', 11, 0, member='time/DS_POSIX_GET/trace')
    item['original_member'] = '/cases/worker_failure/collected_client_logs/client_10.0.0.1/kvcache.INFO.log'
    item['original_line'] = 18
    event = build_events(report([item]))[0]
    assert event['role'] == 'client' and event['worker_id'] is None
    assert event['source_refs'][0]['original_member'] == item['original_member']


def test_export_directory_worker_name_is_not_a_role_without_a_log_origin():
    item = {'source': '/archives/worker-client.tar.gz', 'worker': 'worker-export',
            'member': 'worker-export/time-buckets/DS_KV_CLIENT_GET_10000,20000/trace.txt',
            'text': '2026-01-01T06:21:52.000001 | I | urma_manager.cpp:1700 | '
                    '192.0.2.25 | 9:26 | t1 | jingpai | URMA_ELAPSED_TOTAL'}
    event = build_events(report([item]))[0]
    assert event['role'] is None and event['worker_id'] is None


def test_unknown_worker_placeholder_does_not_hide_observed_original_worker():
    for placeholder in ('unknown', 'Unknown', ''):
        item = evidence('worker-a', '10.0.0.2', 22, 0)
        item['worker'] = placeholder
        item['member'] = 'time-buckets/DS_KV_CLIENT_GET_20000/trace.txt'
        item['text'] = '/logs/worker_10.0.0.2/kvcache.INFO.log:42:' + item['text']
        event = build_events(report([item]))[0]
        assert event['role'] == 'worker'
        assert event['worker_id'] == '10.0.0.2'


def test_case_ancestor_names_do_not_identify_a_log_process():
    for member in ('/cases/worker_failure/logs/kvcache.INFO.log',
                   '/cases/client_timeout/logs/kvcache.INFO.log',
                   '/cases/worker_failure/kvcache.INFO.log',
                   '/cases/client_timeout/kvcache.INFO.log'):
        item = {'member': member,
                'text': '2026-01-01T00:00:00.000001 | I | urma_manager.cpp:42 | '
                        '192.0.2.25 | 9:26 | synthetic-trace | test | URMA_ELAPSED_TOTAL'}
        event = build_events(report([item]))[0]
        assert event['role'] is None, member
        assert event['worker_id'] is None


def test_log_directory_role_requires_a_collection_or_nearby_process_identity():
    cases = [('/cases/worker_failure/collected_client_logs/logs/kvcache.INFO.log', 'client'),
             ('/cases/client_timeout/collected_worker_logs/logs/kvcache.INFO.log', 'worker'),
             ('/cases/worker06_192.0.2.25/logs/kvcache.INFO.log', 'worker'),
             ('/cases/client_192.0.2.25/logs/kvcache.INFO.log', 'client'),
             ('/cases/worker-a/kvcache.INFO.log', 'worker')]
    for member, expected in cases:
        item = {'member': member,
                'text': '2026-01-01T00:00:00.000001 | I | urma_manager.cpp:42 | '
                        '192.0.2.25 | 9:26 | synthetic-trace | test | URMA_ELAPSED_TOTAL'}
        event = build_events(report([item]))[0]
        assert event['role'] == expected, member
