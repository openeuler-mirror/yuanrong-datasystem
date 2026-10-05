"""A rendered empty series must agree with its declared model and shared filter."""
from trace_test_loader import REPO_ROOT
import sys
import subprocess
from pathlib import Path

import pytest

ROOT = REPO_ROOT
sys.path.insert(0, str(ROOT / "scripts"))


def test_registry_declares_model_fields_and_empty_conditions():
    from trace_analysis.rendering.registry import build_registry
    examples = [('read', 'problem-count-chart'), ('write', 'problem-count'),
                ('numa', 'latency-chart'), ('triage', 'classification-chart')]
    for kind, identity in examples:
        page = f'<div class="chart" id="{identity}"></div>'
        contract = build_registry(page, kind)['components'][0]['data_contract']
        assert contract['model_fields']
        assert contract['empty_when'] == 'source_empty_or_scope_empty'


@pytest.mark.parametrize('scenario', ['nonempty', 'missing', 'empty', 'filtered', 'null', 'forged', 'unbound', 'missing_collection', 'missing_filtered', 'stale_chart', 'stale_table', 'stale_null', 'dynamic_unbound', 'dynamic_bound', 'disposed'])
def test_registry_model_backed_empty_states(scenario):
    script = ROOT / 'scripts/trace_analysis/assets/shared/report_registry.js'
    program = r'''
const fs=require('fs'),vm=require('vm'),assert=require('assert');
const scenario=process.argv[1];
const spec={id:'plot',kind:'chart',title:'Plot',data_contract:{source:'test',collection:'rows',model_fields:['ms'],value_kind:'number',empty_when:'source_empty_or_scope_empty'}};
const node={id:'plot',dataset:{},getBoundingClientRect:()=>({width:300,height:200})};
const context={window:{REPORT_COMPONENT_REGISTRY:{components:[spec],chapters:[],dynamic_families:['wr-timeline']},addEventListener(){}},document:{getElementById:()=>node,querySelectorAll:()=>[node],addEventListener(){}}};
vm.createContext(context);vm.runInContext(fs.readFileSync(process.argv[2],'utf8'),context);
const reg=context.ReportRegistry;
let all={rows:[{ms:5}]},scoped=all;
if(scenario==='missing')all=scoped={rows:[{}]};
if(scenario==='missing_collection')all=scoped={};
if(scenario==='missing_filtered'){all={rows:[{}]};scoped={rows:[]};}
if(scenario==='empty')all=scoped={rows:[]};
if(scenario==='filtered')scoped={rows:[]};
if(scenario==='null'||scenario==='stale_null')all=scoped={rows:[{ms:null}]};
if(scenario==='stale_chart'||scenario==='stale_table'||scenario==='disposed')scoped={rows:[]};
if(scenario!=='unbound')reg.bindSource('test',()=>all,()=>scoped);
if(scenario==='dynamic_unbound')reg.registerFamily('wr-timeline',['plot']);
if(scenario==='dynamic_bound')reg.registerFamily('wr-timeline',[spec]);
reg.chart('plot',{series:[]});
if(scenario==='stale_chart'||scenario==='stale_null'){context.window.echarts={getInstanceByDom:()=>({getOption:()=>({series:[{data:[5]}]})})};reg.chart('plot',{series:[{data:[5]}]});}
if(scenario==='disposed'){context.window.echarts={getInstanceByDom:()=>null};reg.record('plot',{state:'empty',reason:'filtered',sampleCount:3,validCount:3});}
if(scenario==='stale_table'){spec.kind='table';reg.record('plot',{state:'rendered',renderedCount:2});}
if(scenario==='forged')reg.record('plot',{state:'empty',reason:'no_matches',matchedCount:0});
const item=reg.audit({requireComplete:true}).components[0];
const expected={nonempty:['error','source_data_not_rendered'],missing:['error','model_field_missing'],empty:['empty','source_empty'],filtered:['empty','scope_empty'],null:['unavailable','source_values_unobserved'],forged:['error','source_data_not_rendered'],unbound:['error','model_source_unbound'],missing_collection:['error','model_field_missing'],missing_filtered:['error','model_field_missing'],stale_chart:['error','rendered_data_without_source'],stale_table:['error','rendered_data_without_source'],stale_null:['error','rendered_data_without_observation'],dynamic_unbound:['error','dynamic_source_contract_missing'],dynamic_bound:['error','source_data_not_rendered'],disposed:['empty','scope_empty']}[scenario];
assert.strictEqual(item.state,expected[0],JSON.stringify(item));assert.strictEqual(item.reason,expected[1],JSON.stringify(item));
'''
    result = subprocess.run(['node', '-e', program, scenario, str(script)], capture_output=True, text=True)
    assert result.returncode == 0, result.stderr


def test_triage_overview_has_unique_actual_table_captions():
    from trace_analysis.rendering.registry import build_registry
    template = (ROOT / 'scripts/trace_analysis/assets/triage/triage.html').read_text()
    result = build_registry(template, 'triage')
    titles = {item['id']: item['title'] for item in result['components']}
    assert titles['run-metadata-table'] == '表 1-1 运行与输入来源'
    assert titles['coverage-table'] == '表 1-2 日志覆盖与缺失观测面'
    assert '表 1-1 整体导读' not in template
    assert '<h3>运行与输入来源</h3>' not in template


@pytest.mark.parametrize('kind', ['read', 'write', 'numa', 'overview'])
def test_canonical_template_components_have_explicit_sources(kind):
    from trace_analysis.rendering.registry import build_registry
    template = (ROOT / f'scripts/trace_analysis/assets/{kind}/{kind}.html').read_text()
    components = build_registry(template, kind)['components']
    assert components
    assert not [item['id'] for item in components if not item.get('data_contract')]


def test_write_dynamic_table_containers_are_required_data_components():
    from trace_analysis.rendering.registry import build_registry
    template = (ROOT / 'scripts/trace_analysis/assets/write/write.html').read_text()
    components = {item['id']: item for item in build_registry(template, 'write')['components']}
    for identity in ('time-table', 'worker-table', 'trace-table', 'wr-time-table', 'wr-worker-table', 'wr-events-table'):
        assert components[identity]['kind'] == 'table'
        assert components[identity]['data_contract']
        assert components[identity]['title'].startswith('表 ')
