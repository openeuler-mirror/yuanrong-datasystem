from trace_test_loader import REPO_ROOT
import subprocess
from pathlib import Path


def test_timeline_clock_domains_and_svg_evidence():
    asset = REPO_ROOT / 'scripts/trace_analysis/assets/shared/trace_visuals.js'
    script = r'''
const assert=require('assert');eval(require('fs').readFileSync(process.argv[1],'utf8'));
const line=(ts,ip,pid,msg)=>({text:`${ts} | I | file.cpp:1 | ${ip} | ${pid}:1 | tid | | ${msg}`,member:'sample.log',line:ts});
const a=line('2026-01-01T00:00:00.000100','192.0.2.1',1,'RPC timeout');
const b=line('2026-01-01T00:00:00.005200','192.0.2.1',1,'URMA done');
const c=line('2026-01-01T00:00:10.000000','192.0.2.2',2,'RPC');
const m=TraceVisuals.timeline([b,a,c,a,{text:'undated'}]);
assert.equal(m.lanes.length,2);assert.equal(m.undated,1);assert.equal(m.lanes[0].events.length,2);
assert.equal(m.lanes[0].events[0].kind,'timeout');assert(Math.abs(m.lanes[0].events[1].offset_ms-5.1)<.001);
assert.equal(m.lanes[1].events[0].offset_ms,0);
assert.equal(m.lanes[0].events[0].elapsed_ms,null);
assert(Math.abs(m.lanes[0].events[1].elapsed_ms-5.1)<.001);
assert.equal(m.lanes[1].events[0].elapsed_ms,null);
assert.equal(TraceVisuals.component({text:'x | urma_manager.cpp:42 |'}),'URMA');
assert.equal(TraceVisuals.component({text:'x | client_worker_api.cpp:42 |'}),'SDK');
assert.equal(TraceVisuals.component({text:'no source'}),'其他 / 来源未明确');
const original={source:'run1',member:'all-core/a',line:2,text:'/logs/worker.log:123:2026-01-01T00:00:00.000001 | msg'};
const unique=TraceVisuals.uniqueEvidence([original,{...original,member:'time-buckets/a'}, {...original,text:original.text.replace(':123:',':124:')},{...original,source:'run2'},{text:'trace-id',member:'all-core/unique_traces_1001.txt'}]);
assert.equal(unique.records.length,3);assert.equal(unique.duplicates,1);assert.equal(unique.indexRows,1);assert.equal(unique.records[0].origins.length,2);
const svg=TraceVisuals.flowSvg({nodes:[{id:'a',role:'client'},{id:'b',role:'data_worker'}],edges:[{source:'a',target:'b',name:'observed',status:'present'},{source:'b',target:'a',name:'unconfirmed',status:'missing'}]},'<unsafe>');
assert(svg.includes('<svg'));assert(svg.includes('&lt;unsafe&gt;'));assert(svg.includes('observed'));assert(!svg.includes('unconfirmed'));
'''
    subprocess.run(['node','-e',script,str(asset)],check=True)


def test_timeline_click_highlights_evidence_without_interpreting_log_html():
    assets = REPO_ROOT / 'scripts/trace_analysis/assets/shared'
    script = r'''
const assert=require('assert'),fs=require('fs');
eval(fs.readFileSync(process.argv[1]+'/log_fields.js','utf8'));
eval(fs.readFileSync(process.argv[1]+'/trace_visuals.js','utf8'));
const nodes={};global.document={getElementById:id=>nodes[id]||(nodes[id]={})};
const callbacks={};let option;
const chart={setOption:x=>option=x,on:(name,fn)=>callbacks[name]=fn};
const line=(time,msg)=>({member:'/logs/worker01/kvcache.INFO.log',line:1,
 text:`2026-01-01T00:00:00.${time} | E | urma_manager.cpp:42 | 192.0.2.1 | 1:2 | trace-test | test | ${msg}`});
const evidence=[line('000001','URMA_ELAPSED_TOTAL dataSize:4194304'),
 line('000101','RPC failed timeout cntl_error_code=1008 <img src=x onerror=alert(1)>')];
TraceVisuals.renderTimeline({style:{},clientWidth:900},{evidence},
 {getInstanceByDom:()=>null},{init:()=>chart});
const detail=nodes['selected-event-evidence'];
assert(detail.innerHTML?.includes('log-field-urma'),'initial selection must highlight URMA');
const point=option.series.flatMap(s=>s.data).find(p=>p.event.evidence.text===evidence[1].text);
callbacks.click({data:point});
assert(detail.innerHTML.includes('log-field-error'),'clicked failure must highlight error');
assert(detail.innerHTML.includes('cntl_error_code=1008'));
assert(detail.innerHTML.includes('组件：URMA'));
assert(detail.innerHTML.includes('0.100 ms'));
assert(detail.innerHTML.includes('&lt;img'));
assert(!detail.innerHTML.includes('<img'));
assert(!detail.innerHTML.includes('dataSize:4194304'),'click must replace previous evidence');
'''
    subprocess.run(['node', '-e', script, str(assets)], check=True)


def test_continuation_lines_merge_only_copies_of_the_same_source_location():
    asset = REPO_ROOT / 'scripts/trace_analysis/assets/shared/trace_visuals.js'
    script = r'''
const assert=require('assert');eval(require('fs').readFileSync(process.argv[1],'utf8'));
const text='/logs/client/ds_client_108.INFO.log:829:traceId : getBuffer-17;abc]';
const a={source:'run1.tar.gz',member:'all-core/1001/trace',line:11,text};
const b={...a,member:'time-buckets/GET_20000/trace',line:14};
let audit=TraceVisuals.uniqueEvidence([a,b]);
assert.equal(audit.records.length,1,'multiline continuation capture copies must merge');
assert.equal(audit.duplicates,1);assert.equal(audit.records[0].origins.length,2);
for(const distinct of [
 {...b,text:text.replace(':829:',':833:')},
 {...b,text:text.replace('/client/','/worker/')},
 {...b,source:'run2.tar.gz'},
 {...b,text:text+' different error'}
])assert.equal(TraceVisuals.uniqueEvidence([a,distinct]).records.length,2);
for(const raw of ['traceId : same','2026-09-29T01:17:49.123 | E | RPC:12:failed']){
 assert.equal(TraceVisuals.uniqueEvidence([{...a,text:raw},{...b,text:raw}]).records.length,2,
 'unlocated identical text is insufficient evidence to merge different capture members');
}
for(const path of ['worker/kvcache.INFO.log','C:\\logs\\worker.log','worker.log']){
 const raw=path+':12:continuation';
 assert.equal(TraceVisuals.uniqueEvidence([{...a,text:raw},{...b,text:raw}]).records.length,1);
}
'''
    subprocess.run(['node', '-e', script, str(asset)], check=True)
