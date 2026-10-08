import copy
import datetime
from trace_test_loader import REPO_ROOT, load_fresh
from pathlib import Path

MODULE_PATH = REPO_ROOT / "scripts/trace_analysis/write_report.py"
report = load_fresh("write_report")


def fixture():
    timestamp = datetime.datetime(2026, 1, 1).isoformat()
    return {
        "trace_id": "fixture",
        "evidence": [
            timestamp
            + ".000001 | I | file.cpp:1 | client.example | 1:2 | fixture | zone | "
            "[Client/WorkerRpc] Publish done, count: 1, path: UB, costUs: 20114"
        ],
        "write_breakdown_ms": {
            "未解释残差": 20.145,
            "Publish RPC其他": 0,
            "Create RPC其他": 0,
            "写入URMA通信": 16.64,
        },
        "client_ms": 36.785,
        "status": 1001,
        "create_rpc_ms": 0,
        "publish_rpc_ms": 0,
        "write_urma_ms": 16.64,
        "timestamp": timestamp,
    }


def test_parent_moves_from_residual_without_mutating_input():
    row = fixture()
    before = copy.deepcopy(row)
    refined = report.refine(row)
    assert row == before
    assert refined["publish_rpc_ms"] == 20.114
    assert abs(refined["write_breakdown_ms"]["未解释残差"] - 0.031) < 1e-6
    assert abs(sum(refined["write_breakdown_ms"].values()) - row["client_ms"]) < 1e-6


def test_duplicate_observation_is_not_another_attempt():
    row = fixture()
    row["evidence"] *= 2
    assert report.refine(row)["publish_rpc_ms"] == 20.114


def test_invalid_stage_budget_rejected_with_python_optimization():
    import json
    import subprocess
    import sys

    row = fixture()
    row["client_ms"] += 10
    code = (
        "import json,sys; sys.path.insert(0,sys.argv[1]); "
        "from trace_analysis.write_report import refine; refine(json.load(sys.stdin))"
    )
    result = subprocess.run(
        [sys.executable, "-O", "-c", code, str(MODULE_PATH.parents[1])],
        input=json.dumps(row), text=True, capture_output=True, check=False,
    )
    assert result.returncode != 0
    assert "ValueError: write stage budget does not close" in result.stderr


def test_multiple_attempts_and_multiple_observers_stay_unclosed():
    for old, new in [("00:00:00", "00:00:01"), ("client.example", "another.example")]:
        row = fixture()
        row["evidence"].append(row["evidence"][0].replace(old, new))
        assert report.refine(row)["publish_rpc_ms"] == 0


def test_wrong_trace_and_oversized_parent_are_not_attributed():
    for old, new in [("20114", "50000"), ("| fixture |", "| another |")]:
        row = fixture()
        row["evidence"][0] = row["evidence"][0].replace(old, new)
        assert report.refine(row)["publish_rpc_ms"] == 0


def test_existing_parent_and_final_success_survive_error_evidence():
    row = fixture()
    row["publish_rpc_ms"] = 2
    row["status"] = 0
    row["evidence"].append("Cannot assign requested address")
    refined = report.refine(row)
    assert refined["write_breakdown_ms"] == row["write_breakdown_ms"]
    assert refined["client_status_failed"] is False
    assert "E99建连失败" in refined["issues"]


def test_empty_report_and_html_escaping():
    page, model = report.render_html({"write_traces": []}, "<script>bad</script>")
    assert model["rows"] == []
    assert "<script>bad</script>" not in page
    assert "__DATA__" not in page
    assert "write-nav" in page


def test_write_title_matches_read_page_style_without_duplicate_suffix():
    page, _ = report.render_html({"write_traces": []}, "Run04 · 写入分析")
    assert "<title>Run04 · 写入分析</title>" in page
    assert "<h1>Run04 · 写入分析</h1>" in page
    assert "写入分析 · 写入瓶颈" not in page


def test_write_timeline_axis_shows_milliseconds_without_shortening_tooltip():
    import subprocess

    template = (MODULE_PATH.parent / 'assets/write/write.html').read_text()
    timeline = template[template.index('function timeline() {'):
                        template.index('function publishWrRows(')]
    program = r'''
const assert=require('node:assert/strict');
const bandRows=[{trace_id:'set-1',client_timestamp:'2026-09-29T05:24:23.376603',
  client_ms:11.762,write_breakdown_ms:{URMA:11.762}}];
const stageNames=['URMA'],TraceCharts={label:x=>x},esc=String,num=x=>Number(x).toFixed(3);
let timelineRows,option;
const chart=(id,value)=>{option=value;return {off(){},on(){}}};
const $=()=>({scrollIntoView(){}});
TIMELINE
timeline();
assert.deepEqual(option.xAxis.data,['05:24:23.376']);
assert.match(option.tooltip.formatter([{dataIndex:0,value:11.762,seriesName:'URMA'}]),
  /2026-09-29T05:24:23\.376603/);
'''.replace('TIMELINE', timeline)
    result = subprocess.run(['node'], input=program, text=True, capture_output=True)
    assert result.returncode == 0, result.stderr


def test_no_network_links_are_introduced():
    import pytest

    with pytest.raises(ValueError):
        report.render_html({}, "title", [("unsafe", "javascript:alert(1)")])


def test_model_download_uses_embedded_data_without_sidecar():
    import json
    import subprocess

    page, model = report.render_html({'write_traces': [fixture()]}, 'download')
    assert 'href="write.refined.analysis.json"' not in page
    helper = page[page.index('function download('):page.index('const tableState =')]
    handler = page[page.index('$("download-model").onclick'):page.index('$("download-filtered").onclick')]
    program = '''const assert=require('node:assert/strict');
const MODEL=MODEL_DATA,node={},anchor={click(){this.clicked=true}},document={createElement:()=>anchor};
let blob;const $=()=>node,URL={createObjectURL:b=>{blob=b;return 'blob:test'},revokeObjectURL(){}},setTimeout=()=>{};
HELPER
HANDLER
node.onclick();assert.equal(anchor.download,'write.refined.analysis.json');assert.ok(anchor.clicked);
blob.text().then(text=>{assert.deepEqual(JSON.parse(text),MODEL);assert.equal(blob.type,'application/json')});
'''.replace('MODEL_DATA', json.dumps(model)).replace('HELPER', helper).replace('HANDLER', handler)
    result = subprocess.run(['node'], input=program, text=True, capture_output=True)
    assert result.returncode == 0, result.stderr


def test_wr_aggregation_keeps_sender_clocks_and_unknown_completion_separate():
    import subprocess
    template = (MODULE_PATH.parent / 'assets/write/write.html').read_text()
    helpers = template[template.index('function writeWrEvents('):template.index('function renderWriteWr(')]
    program = r'''
const assert=require('node:assert/strict');
HELPERS
const events=writeWrEvents([{trace_id:'set-a',write_wr_events:[
 {source_worker:'client-a',target_worker_mapped:'worker-x',timestamp:'2026-01-01T00:00:00.123456',total_ms:2,status:'code: [OK]'},
 {source_worker:'client-a',target_addr:'192.0.2.9:31501',timestamp:'2026-01-01T00:00:00.199999',total_ms:20,status:'timeout'},
 {source_worker:'client-b',timestamp:'2026-01-01T00:00:00.123456',total_ms:4,status:'code: [OK]',wait_completion_ms:3}
]}]);
assert.equal(events[1].target,'192.0.2.9:31501');
assert.equal(events[2].target,'目标未观测');
assert.equal(wrGroups(events,e=>wrLocalBucket(e,100)).length,1);
assert.equal(wrLocalBucket(events[0],100),'00:00:00.100');
assert.equal(wrLocalBucket({...events[0],timestamp:''},1000),null);
const s=wrStats(events);assert.equal(s.count,3);assert.equal(s.traces,1);
assert.equal(s.completed,2);assert.equal(s.other,1);assert.equal(s.max,4);
assert.equal(s.p50,3);assert.equal(s.wait,3);
assert.equal(wrStats([events[1]]).p90,null);
assert.deepEqual(writeWrEvents([{trace_id:'legacy'}]),[]);
'''.replace('HELPERS', helpers)
    result = subprocess.run(['node'], input=program, text=True, capture_output=True)
    assert result.returncode == 0, result.stderr


def test_wr_overview_excludes_create_only_from_publish_denominator():
    import subprocess
    template = (MODULE_PATH.parent / 'assets/write/write.html').read_text()
    helpers = template[template.index('function publishWrRows('):template.index('function worker()')]
    program = r'''
const assert=require('node:assert/strict');
const filtered=[
 {trace_id:'set-with-wr',operation:'SET',status:0,write_wr_events:[
  {timestamp:'2026-01-01T00:00:00.100000',status:'code: [OK]',total_ms:2}]},
 {trace_id:'set-no-wr',operation:'SET',status:1,client_status_failed:true,write_wr_events:[]},
 {trace_id:'create-only',operation:'CREATE',status:1,client_status_failed:true,write_wr_events:[]}
];
const elements=new Map(),plots=new Map();
const window={innerWidth:1440};
function $(id){if(!elements.has(id))elements.set(id,{value:id==='wr-bucket'?'1000':'',innerHTML:'',textContent:''});return elements.get(id)}
const esc=String,num=v=>Number(v).toFixed(3),table=()=>{},chart=(id,option)=>plots.set(id,option);
function scopedWrEvents(){return writeWrEvents(filtered)}
HELPERS
renderWriteWr();
assert.match($('wr-summary').innerHTML,/WR 适用 Trace<\/span><b>2/);
assert.match($('wr-summary').innerHTML,/未观测 WR<\/span><b>1/);
assert.deepEqual(plots.get('wr-count-chart').series[0].data,[1,1]);
'''.replace('HELPERS', helpers)
    result = subprocess.run(['node'], input=program, text=True, capture_output=True)
    assert result.returncode == 0, result.stderr


def test_legacy_phase_view_distinguishes_missing_model_fields_from_missing_logs():
    import subprocess
    template = (MODULE_PATH.parent / 'assets/write/write.html').read_text()
    helper = template[template.index('function phaseCoverageRows('):
                      template.index("ReportRegistry.bindSource('write_phase'")]
    program = r'''
const assert=require('node:assert/strict');
HELPER
const legacy={trace_id:'t',operation:'SET'};
const rows=phaseCoverageRows([legacy],null);
assert.equal(rows[0].write_phase_observation.Create.state,'model_unavailable');
assert.equal(rows[0].write_phase_observation.Copy.state,'model_unavailable');
assert.equal(legacy.write_phase_observation,undefined);
const current={trace_id:'u',write_phase_observation:{Create:{state:'observed'}}};
assert.strictEqual(phaseCoverageRows([current],2)[0],current);
const unsupported={trace_id:'v',write_phase_observation:{Create:{state:'unobserved'}}};
assert.equal(phaseCoverageRows([unsupported],1)[0].write_phase_observation.Create.state,'model_unavailable');
assert.equal(unsupported.write_phase_observation.Create.state,'unobserved');
'''.replace('HELPER', helper)
    result = subprocess.run(['node'], input=program, text=True, capture_output=True)
    assert result.returncode == 0, result.stderr


def test_selected_write_trace_labels_legacy_model_and_observed_urma_separately():
    import subprocess
    template = (MODULE_PATH.parent / 'assets/write/write.html').read_text()
    helper = template[template.index('function writePhaseCards('):
                      template.index('function selectTrace(')]
    program = r'''
const assert=require('node:assert/strict');
const esc=x=>String(x),num=x=>Number(x).toFixed(3);
HELPER
const legacy=writePhaseCards({operation:'SET',write_urma_ms:19.466},null);
assert.match(legacy,/旧模型缺少阶段字段/);
assert.match(legacy,/19\.466/);
assert.doesNotMatch(legacy,/Create.*未观测/);
const current=writePhaseCards({operation:'SET',write_urma_ms:0.198,
  write_phase_observation:{Create:{state:'unobserved'},Copy:{state:'unobserved'},
    Publish:{state:'observed',parent_ms:11.519}},
  wr_phase_attribution:{phase:'unconfirmed',basis:'wr_callsite_not_observed'}},2);
assert.match(current,/Publish/);assert.match(current,/11\.519/);
assert.match(current,/Create、Copy/);
assert.match(current,/调用点未观测/);
'''.replace('HELPER', helper)
    result = subprocess.run(['node'], input=program, text=True, capture_output=True)
    assert result.returncode == 0, result.stderr


def test_stage_summary_separates_create_publish_rpc_failures_from_copy():
    import subprocess
    template = (MODULE_PATH.parent / 'assets/write/write.html').read_text()
    helper = template[template.index('function writePhaseSummary('):
                      template.index('function renderWritePhases()')]
    program = r'''
const assert=require('node:assert/strict');
HELPER
const rows=[{operation:'SET',write_phase_observation:{Create:{state:'observed',parent_ms:2},
  Copy:{state:'observed',parent_ms:1},Publish:{state:'observed',parent_ms:3}},
  rpc_analysis:{calls:[
    {method:'svc.Create',cntl_failed:0,cntl_error_code:0},
    {method:'svc.CreateMeta',cntl_failed:1,cntl_error_code:1},
    {method:'svc.Publish',cntl_failed:1,cntl_error_code:9}]}}];
const result=writePhaseSummary(rows);
assert.deepEqual(result.map(x=>[x.rpc_calls,x.rpc_failed]),[[1,0],[null,null],[1,1]]);
'''.replace('HELPER', helper)
    result = subprocess.run(['node'], input=program, text=True, capture_output=True)
    assert result.returncode == 0, result.stderr


def test_selected_write_wr_windows_keep_overlaps_and_missing_points_distinct():
    import subprocess
    template = (MODULE_PATH.parent / 'assets/write/write.html').read_text()
    start = template.index('function writeWrWindows(')
    end = template.index('function selectTrace(', start)
    helper = template[start:end]
    program = r'''
const assert=require('node:assert/strict');
HELPER
const seen=writeWrWindows({total_ms:1.31,wait_completion_ms:1.25,trace_us:{
 post:100000,sleep_start:101000,sleep_end:101200,poll_begin:101150,poll_end:101230,
 notify:101250,awake:101300,observed:101310}});
assert.deepEqual(seen,{elapsed:1.31,wait:1.25,sleep:.2,poll:.08,notify_awake:.05,awake_observed:.01});
const missing=writeWrWindows({total_ms:2,trace_us:{sleep_start:10,sleep_end:9}});
assert.equal(missing.sleep,null);
assert.equal(missing.poll,null);
assert.equal(missing.notify_awake,null);
'''.replace('HELPER', helper)
    result = subprocess.run(['node'], input=program, text=True, capture_output=True)
    assert result.returncode == 0, result.stderr


def test_wr_phase_label_does_not_infer_route_from_timing():
    import subprocess
    template = (MODULE_PATH.parent / 'assets/write/write.html').read_text()
    helper = template[template.index('function writeWrPhaseLabel('):template.index('function writeWrWindows(')]
    program = r'''
const assert=require('node:assert/strict');
HELPER
assert.equal(writeWrPhaseLabel({wr_phase_attribution:{phase:'Copy'}}),'Copy');
assert.equal(writeWrPhaseLabel({wr_phase_attribution:{phase:'Publish'}}),'Publish');
assert.equal(writeWrPhaseLabel({write_urma_ms:2,memory_copy_ms:3}),'阶段未确认');
'''.replace('HELPER', helper)
    result = subprocess.run(['node'], input=program, text=True, capture_output=True)
    assert result.returncode == 0, result.stderr


def test_write_phase_summary_keeps_set_phases_and_create_interface_separate():
    import subprocess
    template = (MODULE_PATH.parent / 'assets/write/write.html').read_text()
    start = template.index('function writePhaseSummary(')
    end = template.index('function renderWritePhases(', start)
    helper = template[start:end]
    program = r'''
const assert=require('node:assert/strict');
HELPER
const rows=[
 {operation:'SET',client_status_failed:false,write_phase_observation:{
  Create:{state:'observed',parent_ms:.5},Copy:{state:'observed',parent_ms:1},Publish:{state:'observed',parent_ms:2}}},
 {operation:'SET',client_status_failed:true,write_phase_observation:{
  Create:{state:'unobserved',parent_ms:null},Copy:{state:'unobserved',parent_ms:null},Publish:{state:'unobserved',parent_ms:null}}},
 {operation:'CREATE',client_status_failed:true,write_phase_observation:{
  Create:{state:'observed',parent_ms:3},Publish:{state:'not_applicable',parent_ms:null}}}
];
const result=writePhaseSummary(rows);
assert.deepEqual(result.map(x=>[x.name,x.traces,x.observed,x.unobserved,x.p90_ms]),[
 ['SET · Create',2,1,1,.5],['SET · Copy',2,1,1,1],['SET · Publish',2,1,1,2],
 ['CREATE · Create',1,1,0,3]]);
'''.replace('HELPER', helper)
    result = subprocess.run(['node'], input=program, text=True, capture_output=True)
    assert result.returncode == 0, result.stderr


def test_write_page_chapters_and_component_contracts_are_coherent():
    from trace_analysis.rendering.registry import build_registry
    template = (MODULE_PATH.parent / 'assets/write/write.html').read_text()
    report = build_registry(template, 'write')
    chapters = report['chapters']
    titles = [chapter['title'] for chapter in chapters]
    assert [title.split('.')[0] for title in titles if title[0].isdigit()] == [str(i) for i in range(1, 8)]
    assert next(chapter['title'] for chapter in chapters if chapter['id'] == 'write-downloads') == '附录 · 下载与口径'
    assert next(chapter['title'] for chapter in chapters if chapter['id'] == 'trace-event-timeline') == '7. Trace 时间线'
    ids = {component['id'] for component in report['components']}
    assert {'write-rpc-method-chart', 'write-rpc-histogram-chart',
            'write-phase-table', 'write-rpc-method-table',
            'error-chart', 'latency-band-chart', 'stage-root-table', 'selected-chart',
            'trace-event-chart', 'trace-event-table'} <= ids
    assert [component['id'] for component in report['components']].count('trace-event-chart') == 1
    assert [component['id'] for component in report['components']].count('trace-event-table') == 1
    assert not [component['id'] for component in report['components'] if not component.get('data_contract')]
    navigation = {entry['id']: entry for entry in report['navigation']}
    assert {'wr-time-table', 'wr-worker-table', 'write-phase-table', 'selected-stage-table'} <= navigation.keys()
    assert navigation['write-rpc-method-chart']['title'] == '图 3-1 RPC 方法 / 阶段的 Trace 数与观测数'
    assert navigation['write-rpc-histogram-chart']['title'] == '图 3-2 RPC 耗时分布（调用 / 汇总窗口分列）'
    assert navigation['write-rpc-method-table']['title'] == '表 3-2 全 RPC 汇总'
    assert navigation['trace-event-timeline']['title'] == '7. Trace 时间线'
    assert navigation['trace-event-chart']['title'] == '图 7-1 Trace 分进程事件'
    assert navigation['trace-event-table']['title'] == '表 7-1 Trace 事件明细'


def test_write_rpc_summary_keeps_calls_and_summary_windows_separate():
    import subprocess
    template = (MODULE_PATH.parent / 'assets/write/write.html').read_text()
    helper = template[template.index('function writeRpcSummary('):
                      template.index('function renderWritePhases()')]
    program = r'''
const assert=require('node:assert/strict');
HELPER
const rows=[{trace_id:'a',rpc_analysis:{calls:[
  {method:'svc.Publish',owner:['client','1'],fields_us:{e2e_us:4000},cntl_failed:0,cntl_error_code:0},
  {method:'svc.Publish',owner:['client','1'],fields_us:{e2e_us:7000},cntl_failed:1,cntl_error_code:9}],
  summary_windows:[{stage_label:'Publish',method_hint:'Publish',total_ms:12,evidence_scope:'access_stage',owner:['other','2']},
    {stage_label:'Publish',method_hint:'Publish',total_ms:13,evidence_scope:'access_stage',owner:['client','1']}]}},
  {trace_id:'b',rpc_analysis:{calls:[],summary_windows:[
    {stage_label:'Create',total_ms:5,evidence_scope:'access_stage'}]}}];
const groups=writeRpcSummary(rows);
const calls=groups.find(g=>g.name==='svc.Publish'&&g.kind==='详细调用');
const window=groups.find(g=>g.name==='Publish'&&g.kind==='汇总窗口');
assert.equal(calls.traces,1);assert.equal(calls.count,2);assert.equal(calls.failed,1);
assert.deepEqual(calls.values,[4,7]);
assert.equal(window.count,1);assert.deepEqual(window.values,[12]);
assert.equal(groups.find(g=>g.name==='Create').count,1);
assert.equal(groups.reduce((n,g)=>n+g.count,0),4);
'''.replace('HELPER', helper)
    result = subprocess.run(['node'], input=program, text=True, capture_output=True)
    assert result.returncode == 0, result.stderr


def test_cli_rejects_input_and_output_aliases(tmp_path, monkeypatch):
    import json
    import sys
    import pytest

    for kind in ("same", "symlink", "hardlink", "refined-input", "refined-output"):
        folder = tmp_path / kind
        folder.mkdir()
        source = folder / ("write.refined.analysis.json" if kind == "refined-input" else "input.json")
        source.write_text(json.dumps({"write_traces": []}))
        before = source.read_bytes()
        output = folder / "report.html"
        if kind == "same":
            output = source
        elif kind == "symlink":
            output.symlink_to(source)
        elif kind == "hardlink":
            output.hardlink_to(source)
        elif kind == "refined-output":
            output = folder / "write.refined.analysis.json"
        monkeypatch.setattr(sys, "argv", ["writer", "--analysis-json", str(source), "--output", str(output)])
        with pytest.raises(SystemExit):
            report.main()
        assert source.read_bytes() == before
        if kind in ("refined-input", "refined-output"):
            assert not output.exists()


def test_write_search_coalesces_and_cancels_pending_redraws():
    import subprocess

    source = (MODULE_PATH.parent / "assets/write/write.html").read_text()
    handlers = source[source.index("      let searchTimer;"):source.index('      $("worker-choice").onchange')]
    script = """
const assert = require('node:assert/strict');
const controls = {}, tasks = new Map(); let next = 0, redraws = 0;
const $ = id => controls[id] ||= {value:''};
const applyTraceFilters = () => redraws++;
const setTimeout = (fn, delay) => {assert.equal(delay,150);tasks.set(++next,fn);return next};
const clearTimeout = id => tasks.delete(id);
const flush = () => {const pending=[...tasks.values()];tasks.clear();pending.forEach(fn=>fn())};
""" + handlers + """
for(let i=0;i<20;i++)$('search').oninput();
assert.equal(redraws,0);assert.equal(tasks.size,1);flush();assert.equal(redraws,1);
$('search').oninput();$('operation').oninput();assert.equal(redraws,2);flush();assert.equal(redraws,2);
$('search').oninput();$('reset').onclick();assert.equal(redraws,3);flush();assert.equal(redraws,3);
"""
    subprocess.run(["node", "-e", script], check=True, capture_output=True, text=True)


def test_worker_time_scope_includes_all_timed_workers_by_default():
    import subprocess

    source = (MODULE_PATH.parent / "assets/write/write.html").read_text()
    helper = source[source.index("      function timedWorkerEvents("):
                    source.index("      ReportRegistry.bindSource('write_worker_events'")]
    script = """
const assert = require('node:assert/strict');
HELPER
const rows = [{worker_events:[
  {host:'worker-a',timestamp:'2026-01-01T00:00:00',ms:1},
  {host:'worker-b',timestamp:'2026-01-01T00:00:00',ms:2},
  {host:'worker-a',timestamp:'2026-01-01T00:00:01',ms:3},
  {host:'worker-b',timestamp:'',ms:4},
]}];
assert.deepEqual(timedWorkerEvents(rows,'').map(event=>event.ms),[1,2,3]);
assert.deepEqual(timedWorkerEvents(rows,'worker-a').map(event=>event.ms),[1,3]);
assert.deepEqual(timedWorkerEvents(rows,'worker-b').map(event=>event.ms),[2]);
""".replace("HELPER", helper)
    subprocess.run(["node", "-e", script], check=True, capture_output=True, text=True)


def test_write_navigation_bottom_and_top():
    import subprocess

    source = (MODULE_PATH.parent / "assets/write/write.html").read_text()
    function = source[source.index("      function updateNav()"):source.index('      window.addEventListener("scroll", updateNav')]
    script = """
const assert = require('node:assert/strict');
let scrollY=1000,innerHeight=1440;
const document={documentElement:{scrollHeight:2440}}, positions=[-500,1100];
const $=id=>({getBoundingClientRect:()=>({top:positions[+id]})});
const navLinks=[0,1].map(i=>({hash:'#'+i,attrs:{},classList:{toggle(k,v){this[k]=v}},setAttribute(k,v){this.attrs[k]=v},removeAttribute(k){delete this.attrs[k]}}));
""" + function + """
updateNav();assert.equal(navLinks[1].attrs['aria-current'],'location');assert.equal(navLinks[0].classList.active,false);
scrollY=0;positions[0]=80;updateNav();assert.equal(navLinks[0].attrs['aria-current'],'location');assert.equal(navLinks[1].classList.active,false);
"""
    subprocess.run(["node", "-e", script], check=True, capture_output=True, text=True)
