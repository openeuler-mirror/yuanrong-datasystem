"""URMA endpoint aliases and operation-specific edges remain observable end to end."""
from trace_test_loader import REPO_ROOT
import copy
import sys
from pathlib import Path

sys.path.insert(0, str(REPO_ROOT / 'scripts'))
from trace_analysis import triage
from trace_analysis.validation import validate_data


def make_report(tmp_path):
    lines=[]
    for identity in ('getBuffer-test;read', 'setStringView-test;write', 'synthetic-unknown'):
        lines.append('2026-01-01T00:00:00.000001 | I | urma_manager.cpp:42 | 192.0.2.1 | 1:2 | '
                     + identity + ' | test | [URMA_ELAPSED_TOTAL] [urma_request_id:3] '
                     'urma post to completion cost: 2ms, src addr:192.0.2.1:31501, tgt addr:192.0.2.2:0')
    path=tmp_path/'worker.log';path.write_text('\n'.join(lines)+'\n')
    return triage.analyze_inputs([str(path)])


def test_new_aliases_reach_separate_read_write_and_unknown_edges(tmp_path):
    report=make_report(tmp_path)
    summary=report['dimensions']['ub_summary']
    assert sum(v['count'] for v in summary['edges'].values()) == 3
    for operation in ('read','write','unknown'):
        edges=summary['edges_by_operation'][operation]
        assert sum(v['count'] for v in edges.values()) == 1
        assert edges['192.0.2.1:31501 -> 192.0.2.2:0']['latency_ms']['max'] == 2
    assert validate_data(report,'triage','fixture')['valid']


def test_validation_rejects_endpoint_parser_loss_and_silent_empty_edges(tmp_path):
    report=make_report(tmp_path)
    for damage in ('endpoints','edges','operation','missing_total'):
        broken=copy.deepcopy(report)
        if damage=='endpoints':
            for row in broken['traces'].values():
                for event in row['ub_events']:
                    event['src_addr']=None
        elif damage=='edges':
            broken['dimensions']['ub_summary']['edges']={}
        elif damage=='operation':
            broken['dimensions']['ub_summary']['edges_by_operation']['write']={}
        else:
            for row in broken['traces'].values():
                row['ub_events']=[]
            broken['dimensions']['ub_summary']['edges']={}
        checked=validate_data(broken,'triage','fixture')
        assert not checked['valid'], damage
        assert any('UB edge' in error for error in checked['errors'])


def test_legacy_aliases_and_ambiguous_business_operation_are_preserved(tmp_path):
    report=make_report(tmp_path)
    trace=report['traces']['getBuffer-test;read']
    trace['flows']={'DS_KV_CLIENT_GET':1,'DS_KV_CLIENT_SET':1}
    from trace_analysis.analysis.ub_edges import group_operation_edges
    grouped=group_operation_edges(report['traces'])
    assert grouped['read']=={}
    assert sum(item['count'] for item in grouped['unknown'].values())==2
    path=tmp_path/'worker.log'
    path.write_text(path.read_text().replace('src addr:', 'src address:').replace('tgt addr:', 'target address:'))
    legacy=triage.analyze_inputs([str(path)])
    assert legacy['dimensions']['ub_summary']['edges']==report['dimensions']['ub_summary']['edges']
    del legacy['dimensions']['ub_summary']['edges_by_operation']
    del legacy['dimensions']['ub_summary']['edge_operation_schema_version']
    checked=validate_data(legacy,'triage','legacy')
    assert checked['valid']
    assert checked['ub_edge_operation_status']=='legacy_unpartitioned'


def test_ub_edge_view_uses_each_operation_and_never_assigns_legacy_totals_to_read():
    import subprocess
    template=(REPO_ROOT/'scripts/trace_analysis/assets/triage/triage.html').read_text()
    helpers=template[template.index('  function ubEdgeModelRows('):template.index('  function latencyRowsForOperation(')]
    program="""const assert=require('assert');
let dim={ub_summary:{edges:{'old -> unknown':{count:9}}}};
let chosen='';const ubEdgeFilterValue=(op,kind)=>kind==='src'?chosen:'';
const splitUbEdge=edge=>({src:edge.split(' -> ')[0],dst:edge.split(' -> ')[1]});
HELPERS
assert.deepEqual(ubEdgeModelRows('read'),[]);assert.deepEqual(ubEdgeModelRows('write'),[]);
dim.ub_summary.edges_by_operation={read:{'a -> b':{count:2}},write:{'b -> a':{count:3}},unknown:{}};
assert.equal(ubEdgeRowsForOperation('read')[0][1].count,2);
assert.equal(ubEdgeRowsForOperation('write')[0][1].count,3);
chosen='absent';assert.deepEqual(ubEdgeRowsForOperation('write'),[]);
delete dim.ub_summary.edges_by_operation.write;
assert.throws(()=>ubEdgeModelRows('write'),/operation field missing/);
""".replace('HELPERS',helpers)
    result=subprocess.run(['node','-e',program],capture_output=True,text=True)
    assert result.returncode==0,result.stderr


def test_invalid_edge_collections_are_errors_not_exceptions(tmp_path):
    report = make_report(tmp_path)
    for field in ('ub_events', 'evidence', 'flows'):
        for value in (None, 'broken', ['broken']):
            broken = copy.deepcopy(report)
            broken['traces']['getBuffer-test;read'][field] = value
            checked = validate_data(broken, 'triage', 'malformed')
            assert not checked['valid'], (field, value)


def test_new_partition_contract_cannot_disappear(tmp_path):
    report = make_report(tmp_path)
    del report['dimensions']['ub_summary']['edges_by_operation']
    assert not validate_data(report, 'triage', 'broken')['valid']


def test_urma_summary_without_client_window_preserves_unknown_ratio(tmp_path):
    from trace_analysis.bottleneck import build_trace_rows
    rows = build_trace_rows(make_report(tmp_path))
    assert len(rows) == 3
    for row in rows:
        assert row['urma_trace']['urma_client_ratio_pct'] is None
        assert 'URMA/Client 未观测' in row['urma_trace']['conclusion']
        assert row['urma_trace']['slowest_total_ms'] == 2
