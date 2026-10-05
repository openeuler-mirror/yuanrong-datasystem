"""Collection duplicates and incompatible URMA clock endpoints stay distinct."""
from trace_test_loader import load_fresh

import pytest


def module(name):
    return load_fresh({"ds_trace_triage": "triage", "ds_trace_bottleneck": "bottleneck"}[name])


def original_line(number):
    return f'/logs/client.log:{number}:'


def access_line(timestamp='2026-01-01T00:00:00.006000', process='42:1', original=''):
    trace_id = 'setBuffer-1-42-00000001;abcdef123456'
    return (f'{original}{timestamp} | I | access_recorder.cpp:1 | 192.0.2.1 | {process} | {trace_id} | | '
            '0 | DS_KV_CLIENT_SET | 6000 | 8388608 | '
            '{latencySummary:{client.rpc.create:2000,client.process.memory_copy:3000,client.rpc.publish:1000}}')


def test_core_time_copy_preserves_full_write_budget(tmp_path):
    triage, bottleneck = module('ds_trace_triage'), module('ds_trace_bottleneck')
    line = access_line(original=original_line(10))
    core, time = tmp_path/'core.log', tmp_path/'time.log'
    core.write_text(line+'\n'); time.write_text(line+'\n')
    models = []
    for paths in ([str(core)], [str(core), str(time)]):
        summary = triage.analyze_inputs(paths, code_ref='test')
        base = bottleneck.build_trace_rows(summary)[0]
        models.append(bottleneck._build_write_row(base, summary['traces'][base['trace_id']]))
    assert models[0]['write_breakdown_ms'] == models[1]['write_breakdown_ms']
    assert models[1]['write_breakdown_ms']['Create RPC其他'] == 2
    assert models[1]['write_breakdown_ms']['写入MemoryCopy'] == 3
    assert models[1]['write_breakdown_ms']['Publish RPC其他'] == 1
    assert models[1]['client_ms'] == 6


@pytest.mark.parametrize('second', [
    access_line(timestamp='2026-01-01T00:00:00.012000'),
    access_line(process='43:1'),
    access_line(original=original_line(11)),
])
def test_real_access_observations_are_not_trace_level_deduplicated(tmp_path, second):
    first = access_line(original=original_line(10)) if '/logs/' in second else access_line()
    path = tmp_path/'records.log'; path.write_text(first+'\n'+second+'\n')
    trace = next(iter(module('ds_trace_triage').analyze_inputs([str(path)], code_ref='test')['traces'].values()))
    assert trace['latency_summary_us']['client.rpc.create'] == 4000
    assert trace['latency_summary_us']['client.process.memory_copy'] == 6000


@pytest.mark.parametrize('clocks', [
    {},
    {'post':100000, 'notify':101000, 'awake':110000, 'observed':110000},
    {'post':100000, 'notify':101000, 'awake':110000, 'observed':110000,
     'pre_completed_before_wait':1, 'woken_by_previous_event':1},
])
def test_completion_wall_log_time_is_not_used_as_completion_end(clocks):
    trace = {'client_processes':[['192.0.2.1','42']], 'urma_timeout_events':[
        {'owner':['192.0.2.1','42'],'timestamp':'2026-01-01T00:00:00.002000','elapsed_ms':2}],
        'latency_summary_us':{'client.urma.ub_transfer':10000}}
    completed = {'owner':['192.0.2.1','42'],'request_id':'2',
                 'timestamp':'2026-01-01T00:00:00.010000','total_ms':1,'trace_us':clocks}
    budget = {'写入URMA通信':1.,'写入URMA调度/线程开销':0.,'未解释残差':9.}
    before = dict(budget)
    result = module('ds_trace_bottleneck')._urma_timeout_accounting({'urma_requests':[completed]},trace,budget,True)
    assert result['timeout_path_ms'] == 2
    assert result['urma_path_ms'] is None
    assert result['completion_observations'][0]['total_ms'] == 1
    assert result['completion_observations'][0]['trace_us'] == clocks
    assert result['added_ms'] == 0
    assert budget == before
