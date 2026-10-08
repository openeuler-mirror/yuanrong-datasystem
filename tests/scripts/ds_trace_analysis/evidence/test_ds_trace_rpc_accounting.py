"""RPC attempt identity, exclusive budgets, and QueryAndGet stage contracts."""
from trace_test_loader import REPO_ROOT, load_fresh
from pathlib import Path

import pytest


def module(name):
    return load_fresh({"ds_trace_triage": "triage", "ds_trace_bottleneck": "bottleneck"}[name])


def call(method, start, end, network, *, pid='42', server=0, failed=0):
    duration = (end - start) // 1000
    return {'method': method, 'owner': ['192.0.2.10', pid], 'timestamp': '2026-09-01T00:00:00',
            'fields_us': {'e2e_us': duration, 'network_residual_us': network,
                          'server_exec_us': server, 'server_req_queue_us': 0},
            'clocks_ns': {'ClientSend': start, 'ClientRecv': end,
                          'ServerRecv': 900000000, 'ServerSend': 900000000+(duration-network)*1000},
            'cntl_failed': failed, 'cntl_error_code': 1008 if failed else 0}


def analyze(calls):
    return module('ds_trace_bottleneck')._analyze_rpc_calls({'rpc_calls': calls, 'client_processes': [['192.0.2.10','42']]})


def test_serial_different_methods_and_failed_trailer_are_counted():
    a=call('service.WorkerWorkerExchangeUrmaConnectInfo',1000000,4890000,3801,server=88)
    b=call('service.QueryAndGet',5000000,6000000,621,server=379,failed=1)
    result=analyze([a,b])
    assert result['network_ms']==pytest.approx(4.422)
    assert {x['method'] for x in result['calls'] if x['selected']}=={a['method'],b['method']}


def test_overlap_and_foreign_process_not_added_to_client_budget():
    result=analyze([call('service.Outer',1000000,11000000,4000),call('service.Inner',2000000,6000000,3000),
                    call('service.Other',12000000,20000000,7000,pid='43')])
    assert result['network_ms']==4
    assert [x['selection_reason'] for x in result['calls']].count('overlapping_client_interval')==1
    assert any(x['selection_reason']=='different_process' for x in result['calls'])


def test_duplicate_retry_zero_and_missing_clocks():
    first=call('service.QueryAndGet',1000000,3000000,1000)
    second=call('service.QueryAndGet',4000000,6000000,1000)
    zero=call('service.QueryAndGet',7000000,8000000,0,server=1000)
    missing=call('service.QueryAndGet',9000000,10000000,500);missing['clocks_ns']={}
    result=analyze([first,dict(first),second,zero,missing])
    assert result['network_ms']==2
    assert len(result['calls'])==4
    assert any(x['network_ms']==0 and x['selected'] for x in result['calls'])
    assert any(x['network_ms'] is None for x in result['calls'])


def test_invalid_residual_is_not_silently_clamped():
    bad=call('service.QueryAndGet',1000000,3000000,2500)
    result=analyze([bad]);assert result['network_ms'] is None
    assert result['calls'][0]['selection_reason']=='invalid_timing'


def test_focus_budget_is_replaced_not_added_and_never_steals_urma():
    mod=module('ds_trace_bottleneck');focus={'RPC网络相关':1.,'未解释残差':2.,'QueryAndGet其他业务':3.,'URMA通信':4.}
    analysis={'network_ms':4.422};mod._replace_rpc_network_budget(focus,analysis,('未解释残差','QueryAndGet其他业务'))
    assert focus['RPC网络相关']==4.422
    assert sum(focus.values())==pytest.approx(10)
    assert focus['URMA通信']==4
    analysis={'network_ms':9};mod._replace_rpc_network_budget(focus,analysis,('未解释残差','QueryAndGet其他业务'))
    assert analysis['budget_clipped_ms']==pytest.approx(3)


def test_triage_preserves_attempts_and_worker_phases_after_display_cap(tmp_path,monkeypatch):
    mod=module('ds_trace_triage');monkeypatch.setattr(mod,'DEFAULT_MAX_EVIDENCE_PER_TRACE',1)
    tid='getBuffer-42-00000001;abcdef123456'
    prefix=f'2026-09-01T00:00:00.001000 | I | test.cpp:1 | 192.0.2.10 | 42:7 | {tid} | | '
    rpc=prefix+'[BRPC_RPC_FRAMEWORK_SLOW] method=service.QueryAndGet e2e_us=1000 network_residual_us=900 server_req_queue_us=0 server_exec_us=100 ClientSend=1000000 ClientRecv=2000000 ServerRecv=8000000 ServerSend=8100000 cntl_failed=0 cntl_error_code=0'
    worker=prefix+'QueryAndGet done, preprocess: 0.001ms, localRead: 16.757ms, metadata: 0.021ms, delivery: 0.019ms, total: 16.798ms, status: code: [OK]'
    path=tmp_path/'trace.log';path.write_text('\n'.join([prefix+'0 | DS_KV_CLIENT_GET | 18000 | 0 |',rpc,rpc,worker]))
    trace=mod.analyze_inputs([str(path)],code_ref='test')['traces'][tid]
    assert len(trace['evidence'])==1
    assert len(trace['rpc_calls'])==1
    assert trace['client_processes']==[['192.0.2.10','42']]
    phases=trace['query_and_get_calls'][0]
    assert phases['phases_ms']['localRead']==16.757
    assert sum(phases['phases_ms'].values())==pytest.approx(phases['total_ms'])


def test_query_worker_phases_keep_missing_values_and_reject_overlapping_sum():
    mod=module('ds_trace_bottleneck')
    workers=[{'phases_ms':{'preprocess':.001,'localRead':16.757,'metadata':.021,'delivery':.019},'total_ms':16.798},
             {'phases_ms':{'localRead':2},'total_ms':3},
             {'phases_ms':{'preprocess':1,'localRead':3,'metadata':1,'delivery':1},'total_ms':4}]
    result=mod._query_and_get_breakdown(analyze([call('service.QueryAndGet',1000000,3000000,1000,server=1000)]),workers)
    assert result['worker'][0]['stackable']
    assert result['worker'][0]['phases_ms']['localRead']==16.757
    assert not result['worker'][1]['stackable']
    assert 'metadata' not in result['worker'][1]['phases_ms']
    assert result['worker'][1]['exclusion_reason'] == 'missing_phases'
    assert 'preprocess' in result['worker'][1]['missing_phases']
    assert not result['worker'][2]['stackable']
    assert result['worker'][2]['exclusion_reason'] == 'phase_sum_exceeds_total'
    assert sum(result['rpc'][0]['breakdown_ms'].values())==2


def test_query_timeout_window_not_capped_by_another_completed_wr():
    mod=module('ds_trace_bottleneck')
    row={'attribution_ms':{'URMA':4,'URMA超时等待':16,'QueryMeta':.1,'远端供数处理':0,
                          '数据访问父窗口/未细分':0,'RPC排队':0,'RPC网络':.1,'未解释残差':.1},
         'urma_trace':{'slowest_total_ms':5},'query_urma_timeout_ms':16,'evidence':[]}
    mod._apply_focus_breakdown(row)
    assert row['focus_breakdown_ms']['URMA通信']==16
    assert sum(row['focus_breakdown_ms'].values())==pytest.approx(20.3)


def test_query_charts_render_normalized_rpc_and_worker_values():
    import json
    import shutil
    import subprocess

    if not shutil.which('node'):
        pytest.skip('node is required for generated-chart validation')
    mod=module('ds_trace_bottleneck')
    analysis=analyze([call('service.QueryAndGet',1000000,3000000,1000,server=1000)])
    phases={'phases_ms':{'preprocess':.001,'localRead':16.757,'metadata':.021,'delivery':.019},'total_ms':16.798}
    row={'trace_id':'query-test','rpc_analysis':analysis,'query_and_get_breakdown':mod._query_and_get_breakdown(analysis,[phases])}
    program='''const vm=require('node:vm'),assert=require('node:assert/strict');
const charts={},nodes={},sources={};const context={ROWS:ROWS_DATA,WRITE_ROWS:[],scopeRows:()=>context.ROWS,
ReportRegistry:{bindSource:(name,all,scoped)=>sources[name]={all,scoped}},
$:id=>nodes[id]||(nodes[id]={value:id==='rpc-audit-operation'?'GET':'',innerHTML:''}),
esc:s=>String(s),fmt:n=>String(n),chartAt:id=>({setOption:o=>charts[id]=o,off(){},on(){}})};
vm.createContext(context);vm.runInContext(SCRIPT_DATA,context);context.renderQueryBreakdown();
assert.equal(charts['query-rpc-breakdown-chart'].series.reduce((n,s)=>n+s.data[0],0),2);
assert.equal(charts['query-worker-breakdown-chart'].series.find(s=>s.name.startsWith('localRead')).data[0],16.757);
assert.ok(nodes['rpc-audit-rows'].innerHTML.includes('service.QueryAndGet'));
assert.equal(sources.read_rpc_audit.all().calls.length,1);
nodes['rpc-audit-method'].value='missing-method';
assert.equal(sources.read_rpc_audit.scoped().calls.length,0);
assert.equal(sources.read_rpc_audit.all().calls.length,1);
nodes['rpc-audit-method'].value='';
context.ROWS=[];context.renderQueryBreakdown();
assert.equal(sources.read_rpc_audit.all().calls.length,0);
assert.equal(charts['query-rpc-breakdown-chart'].series.length,0);
assert.ok(charts['query-worker-breakdown-chart'].graphic.style.text.includes('未采集到 QueryAndGet 完成记录'));
'''.replace('ROWS_DATA',json.dumps([row])).replace('SCRIPT_DATA',json.dumps(mod.QUERY_BREAKDOWN_SCRIPT))
    subprocess.run(['node'],input=program,text=True,check=True,capture_output=True)


def test_weighted_interval_path_is_not_a_greedy_single_rpc_maximum():
    result=analyze([call('service.Outer',1000000,11000000,5000),
                    call('service.First',1000000,6000000,3000),
                    call('service.Second',6000000,11000000,3000)])
    assert result['network_ms']==6
    assert sum(x['selected'] for x in result['calls'])==2


def test_arbitrary_failed_rpc_is_classified_without_changing_final_status():
    m = module('ds_trace_bottleneck')
    row = {'failed': False, 'rpc_analysis': {'calls': [call('Example.NewMethod', 1000, 2000, 1, failed=1)]}}
    m._apply_explicit_rpc_errors(row)
    assert row['error_family'] == 'RPC报错'
    assert 'Example.NewMethod RPC截止超时' in row['failure_reason']
    assert row['failed'] is False


def test_read_overview_uses_observed_categories_and_preserves_filtered_values():
    import subprocess

    template = module('ds_trace_bottleneck').HTML_TEMPLATE
    palette = next(line for line in template.splitlines() if line.startswith('const PROBLEM_COLORS='))
    palette = (REPO_ROOT / 'scripts/trace_analysis/assets/shared/charts.js').read_text() + '\n' + palette
    summary = template[template.index('function scopedProblemSummary('):template.index('function renderTimeSegments(')]
    overview = template[template.index('function renderProblemOverview(){'):template.index('function renderStageShare(){')]
    program = r'''
const assert=require('node:assert/strict');
const ROWS=[
 {focus_primary_problem:'URMA相关',focus_primary_stage:'URMA通信',focus_breakdown_ms:{URMA通信:6},client_ms:7,failed:false},
 {focus_primary_problem:'新错误分类',focus_primary_stage:'RPC网络相关',focus_breakdown_ms:{RPC网络相关:9},client_ms:10,failed:true},
 {focus_primary_problem:'新错误分类',focus_primary_stage:'RPC网络相关',focus_breakdown_ms:{RPC网络相关:3},client_ms:5,failed:false}];
const STAGE_COLORS={URMA通信:'#123456',RPC网络相关:'#654321'},nodes={},charts={};
let scoped=ROWS;
const scopeRows=()=>scoped,$=id=>nodes[id]||(nodes[id]={style:{}}),esc=String,fmt=String,shortProblem=String;
const percentile=(values,p)=>values.length?[...values].sort((a,b)=>a-b)[Math.floor((values.length-1)*p)]:0;
const chartAt=id=>({setOption:o=>charts[id]=o,resize(){},on(){},off(){}});
PALETTE
FUNCTIONS
function check(expected){renderProblemOverview();const chart=charts['problem-count-chart'];
 const sum=chart.series.reduce((n,s)=>n+s.data.reduce((n,v)=>n+v,0),0);assert.equal(sum,expected);
 const names=chart.yAxis.data;assert.deepEqual([...names].sort(),[...new Set(scoped.map(r=>r.focus_primary_problem))].sort());
 assert.equal(scopedStageTotals(scoped)['RPC网络相关'],scoped.reduce((n,r)=>n+(r.focus_breakdown_ms['RPC网络相关']||0),0));}
check(3);
let names=charts['problem-latency-chart'].yAxis.data,values=charts['problem-latency-chart'].series[2].data;
assert.equal(values[names.indexOf('新错误分类')],9);assert.equal(values[names.indexOf('URMA相关')],6);
scoped=[ROWS[2]];check(1);assert.equal(charts['problem-latency-chart'].series[2].data[0],3);
scoped=[];check(0);assert.deepEqual(charts['problem-latency-chart'].series[0].data,[]);
'''.replace('PALETTE',palette).replace('FUNCTIONS',summary+'\n'+overview)
    result = subprocess.run(['node'], input=program, text=True, capture_output=True)
    assert result.returncode == 0, result.stderr


def test_client_framework_time_is_outside_send_recv_interval():
    rpc = call('service.QueryAndGet', 1000000, 7449000, 6265, server=161)
    rpc['fields_us'].update(e2e_us=6464, remote_processing_us=6449)
    result = analyze([rpc])
    assert result['network_ms'] == pytest.approx(6.265)
    rpc['fields_us']['remote_processing_us'] = 6400
    assert analyze([rpc])['network_ms'] is None


def test_write_wr_events_survive_beyond_display_evidence():
    mod = module('ds_trace_bottleneck')
    events = [dict(request_id='7', timestamp='2026-01-01T00:00:00', source_worker='client-a',
                   target_addr='192.0.2.20:31501', target_worker_mapped='worker-b',
                   total_ms=2.5, wait_completion_ms=2.0, status='code: [OK]')]
    row = mod._build_write_row({'trace_id': 'set-1', 'client_ms': 5, 'status': 0,
                               'urma_requests': events, 'evidence': []}, {})
    assert row['write_wr_events'] == events
    assert row['evidence'] == []


def test_wr_target_mapping_does_not_reuse_read_client_direction():
    mod = module('ds_trace_bottleneck')
    event = {'worker': 'client-a', 'target_addr': '192.0.2.20:31501', 'cost_ms': 2}
    wr = mod._request_from_event(event, 0, {'192.0.2.20': 'worker-b'}, False)
    assert wr['target_worker'] == 'Client'
    assert wr['target_worker_mapped'] == 'worker-b'
    assert mod._request_from_event(event, 0, {}, False)['target_worker_mapped'] is None


def test_timeout_union_accounts_serial_retries_without_counting_overlap_twice():
    mod = module('ds_trace_bottleneck')
    events = [{'owner':['host','1'], 'timestamp':'2026-01-01T00:00:00.015', 'elapsed_ms':15},
              {'owner':['host','1'], 'timestamp':'2026-01-01T00:00:00.020', 'elapsed_ms':4},
              {'owner':['host','1'], 'timestamp':'2026-01-01T00:00:00.015', 'elapsed_ms':15},
              {'owner':['other','2'], 'timestamp':'2026-01-01T00:00:00.015', 'elapsed_ms':10}]
    budget={'URMA通信':15.,'URMA调度/线程开销':0.,'Get其他业务':1.,'QueryAndGet其他业务':1.,'未解释残差':4.}
    result=mod._urma_timeout_accounting({}, {'urma_timeout_events':events,
        'latency_summary_us':{'client.rpc.direct_query_and_get':16000,'client.rpc.direct_get_data':5000}}, budget)
    assert result['timeout_path_ms']==19
    assert result['added_ms']==4
    assert result['unallocated_ms']==0
    assert sum(budget.values())==21
    assert budget['URMA通信']==19


def test_set_transfer_already_includes_timeout_and_preserves_other_work():
    mod=module('ds_trace_bottleneck')
    trace={'client_processes':[['host','1']], 'urma_timeout_events':[
        {'owner':['host','1'],'timestamp':'2026-01-01T00:00:00.020','elapsed_ms':12.329}],
        'latency_summary_us':{'client.urma.ub_transfer':12497}}
    budget={'写入URMA通信':12.493,'写入URMA调度/线程开销':.004,'未解释残差':3.184}
    before=dict(budget);result=mod._urma_timeout_accounting({},trace,budget,True)
    assert budget==before
    assert result['added_ms']==0
    assert result['unallocated_ms']==0
    trace['urma_timeout_events'][0]['owner']=['foreign','1']
    assert mod._urma_timeout_accounting({},trace,budget,True)['urma_path_ms'] is None


def test_triage_timeout_origin_survives_display_cap_and_excludes_propagation(tmp_path,monkeypatch):
    mod=module('ds_trace_triage');monkeypatch.setattr(mod,'DEFAULT_MAX_EVIDENCE_PER_TRACE',1)
    tid='getBuffer-42-00000001;abcdef123456'
    prefix=f'2026-01-01T00:00:00.020000 | W | urma_manager.cpp:1655 | 192.0.2.1 | 42:7 | {tid} | | '
    payload='[URMA_WAIT_TIMEOUT] [urma_request_id:9] timedout waiting, elapsedMs=15.000000'
    p=tmp_path/'input.log';p.write_text(prefix+'first line\n'+prefix+payload+'\n'+prefix+payload+'\n'+prefix.replace('urma_manager.cpp','fast_transport_base.cpp')+payload)
    result=mod.analyze_inputs([str(p)],code_ref='test')['traces'][tid]
    assert len(result['urma_timeout_events'])==1
    assert result['urma_timeout_events'][0]['elapsed_ms']==15
    assert len(result['evidence'])==1


def test_timeout_outside_transport_parent_is_reported_without_forced_reallocation():
    mod=module('ds_trace_bottleneck')
    trace={'client_processes':[['host','1']], 'urma_timeout_events':[
        {'owner':['host','1'],'timestamp':'2026-01-01T00:00:00.050','elapsed_ms':50}],
        'latency_summary_us':{'client.urma.ub_transfer':12000}}
    budget={'写入URMA通信':12.,'写入URMA调度/线程开销':0.,'未解释残差':4.,'写入MemoryCopy':2.}
    before=dict(budget);result=mod._urma_timeout_accounting({},trace,budget,True)
    assert budget==before
    assert result['unallocated_ms']==38


def test_get_timeout_reallocation_preserves_measured_client_local_work():
    mod=module('ds_trace_bottleneck')
    trace={'urma_timeout_events':[{'owner':['worker','1'],'timestamp':'2026-01-01T00:00:00.019','elapsed_ms':19}],
           'latency_summary_us':{'client.rpc.direct_query_and_get':21000,'client.process.direct_materialize':1000}}
    budget={'URMA通信':15.,'URMA调度/线程开销':0.,'未解释残差':4.,'Get其他业务':2.,'QueryAndGet其他业务':0.}
    mod._urma_timeout_accounting({},trace,budget)
    assert budget['URMA通信']==19
    assert budget['未解释残差']==1
    assert budget['Get其他业务']==1
    assert sum(budget.values())==21


@pytest.mark.parametrize("phase_name", ["preproc", "preprocess"])
def test_query_worker_preprocessing_alias_reaches_breakdown(tmp_path, phase_name):
    triage = module("ds_trace_triage")
    tid = "getBuffer-5-80-00081167;bf967d7b59b2"
    line = (f"2026-09-22T18:58:20.134288 | I | worker_query_and_get_impl.cpp:236 | "
            f"192.0.2.47 | 9:271 | {tid} | jingpai | [SLOW LOG] QueryAndGet done, "
            f"{phase_name}: 0.002ms, localRead: 7.111ms, metadata: 0.001ms, "
            "delivery: 0.038ms, total: 7.152ms, status: code: [OK]")
    path = tmp_path / "worker.log"
    path.write_text(line)
    calls = triage.analyze_inputs([str(path)], code_ref="test")["traces"][tid]["query_and_get_calls"]
    assert calls[0]["phases_ms"]["preprocess"] == .002
    result = module("ds_trace_bottleneck")._query_and_get_breakdown(analyze([]), calls)
    assert result["worker"][0]["stackable"]
    assert result["worker"][0]["phase_delta_ms"] == pytest.approx(0)
    assert result["worker"][0]["exclusion_reason"] is None


def test_access_rpc_windows_are_deduplicated_without_inventing_calls(monkeypatch):
    parser = module('ds_trace_bottleneck')
    line = ('2026-01-01T00:00:00.000001 | I | access_recorder.cpp:1 | 192.0.2.1 | 10:11 | '
            'getBuffer-1-10-00000001;abcdef123456 | | 1001 | DS_KV_CLIENT_GET | 9000 | 0 | '
            '{latencySummary:{client.rpc.direct_query_and_get:7000,client.rpc.direct_get_data:1500}}')
    triage = module('ds_trace_triage')
    monkeypatch.setattr(triage, 'DEFAULT_MAX_EVIDENCE_PER_TRACE', 0)
    accumulator = triage.TraceAccumulator([])
    for source in ('core', 'time'):
        parsed = triage.TraceParser().parse_line(source, 'trace', 1, line)
        accumulator.ingest(parsed, line)
    raw = accumulator.traces['getBuffer-1-10-00000001;abcdef123456']
    assert not raw['evidence']
    trace = {'rpc_calls': [], 'client_processes': [['192.0.2.1', '10']],
             'rpc_stage_windows': list(raw['rpc_stage_windows'].values())}
    analysis = parser._analyze_rpc_calls(trace)
    assert not analysis['calls']
    assert analysis['network_ms'] is None
    assert [w['total_ms'] for w in analysis['summary_windows']] == [7, 1.5]
    assert all(w['network_ms'] is None and not w['selected'] for w in analysis['summary_windows'])
    result = parser._query_and_get_breakdown(analysis, [])
    assert result['rpc'][0]['breakdown_ms'] == {'summary': 7}
    assert result['rpc'][0]['evidence_scope'] == 'access_stage'
    later = line.replace('00.000001', '00.000002')
    accumulator.ingest(triage.TraceParser().parse_line('core', 'trace', 2, later), later)
    trace['rpc_stage_windows'] = list(raw['rpc_stage_windows'].values())
    assert len(parser._query_and_get_breakdown(parser._analyze_rpc_calls(trace), [])['rpc']) == 2


def test_query_rpc_detail_takes_precedence_over_summary_window():
    parser = module('ds_trace_bottleneck')
    owner = ['192.0.2.1', '10']
    analysis = {'client_process': owner, 'calls': [{'owner': owner, 'method': 'ds.QueryAndGet',
                'fields_us': {'e2e_us': 6000}, 'network_ms': None}],
                'summary_windows': [{'owner': owner, 'stage_key': 'client.rpc.direct_query_and_get',
                                     'total_ms': 7}]}
    rpc = parser._query_and_get_breakdown(analysis, [])['rpc']
    assert len(rpc) == 1 and rpc[0]['total_ms'] == 6
