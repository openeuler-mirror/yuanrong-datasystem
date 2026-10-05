from trace_test_loader import REPO_ROOT
import subprocess
from pathlib import Path

ASSET = REPO_ROOT / 'scripts/trace_analysis/assets/shared/charts.js'


def test_semantic_colors_and_segment_tooltip():
    script = r'''
const assert=require('assert');
const fs=require('fs');eval(fs.readFileSync(process.argv[1],'utf8'));
assert.equal(TraceCharts.label('写入URMA通信'),'URMA 通信耗时');
assert.equal(TraceCharts.label('RPC网络相关'),'RPC 网络耗时');
assert.equal(TraceCharts.label('URMA超时'),'URMA超时');
assert.equal(TraceCharts.color('URMA通信'),TraceCharts.color('写入URMA通信'));
assert.equal(TraceCharts.color('RPC网络相关'),TraceCharts.color('RPC网络'));
assert.equal(TraceCharts.color('URMA超时'),TraceCharts.color('URMA完成等待超时'));
assert.notEqual(TraceCharts.color('URMA通信'),TraceCharts.color('URMA超时'));
assert.equal(TraceCharts.color('read.data_worker_ub_write'),TraceCharts.color('URMA通信'));
assert.equal(TraceCharts.color('Client Memory Copy'),TraceCharts.color('写入MemoryCopy'));
assert.equal(TraceCharts.color('URMA timeout'),TraceCharts.color('URMA超时'));
let selected={};
const source={tooltip:{trigger:'axis',formatter:ps=>'context '+ps[0].dataIndex},
 xAxis:{type:'category',data:['trace <x>']},yAxis:{type:'value',name:'ms'},series:[
 {name:'URMA通信',type:'bar',stack:'latency',data:[2]},
 {name:'RPC网络相关',type:'bar',stack:'latency',data:[3]},
 {name:'other axis',type:'bar',stack:'latency',yAxisIndex:1,data:[100]},
 {name:'other stack',type:'bar',stack:'other',data:[50]},
 {name:'Client',type:'line',data:[5]}]};
source.legend={};
const before=JSON.stringify(source),o=TraceCharts.option(source,()=>selected);
assert.equal(JSON.stringify(source),before);
assert.equal(o.tooltip.trigger,'item');
assert.equal(o.series[0].name,'URMA通信');
assert.equal(o.legend[0].formatter('URMA通信'),'URMA 通信耗时');
assert(!o.legend[0].data?.includes(''));
assert(o.legend[0].textStyle.fontFamily==='Microsoft YaHei');
const p={seriesIndex:0,dataIndex:0,name:'trace <x>',seriesName:'URMA通信',value:2,color:'#f59e0b'};
let t=o.tooltip.formatter(p);
assert(t.includes('40.0%')&&t.includes('2.000 ms')&&t.includes('trace &lt;x&gt;')&&t.includes('context 0'));
selected={'RPC网络相关':false};assert(o.tooltip.formatter(p).includes('100.0%'));
assert.equal(o.series[0].emphasis.focus,'series');
const plain=TraceCharts.option({series:[{type:'bar',data:[1,2],emphasis:{focus:'self'}},{type:'line',data:[1,2],emphasis:{focus:'none'}}]});
assert.equal(plain.textStyle.fontFamily,'Microsoft YaHei');
assert.equal(plain.series[0].label.fontFamily,'Microsoft YaHei');
assert.equal(plain.series[0].emphasis.focus,'self');
assert.equal(plain.series[1].emphasis.focus,'none');
const missing=TraceCharts.option({...source,series:[{...source.series[0],data:[null]}]});
assert(missing.tooltip.formatter({...p,value:null}).includes('未观测'));
const pie=TraceCharts.option({series:[{type:'pie',data:[{name:'写入URMA通信',value:3},{name:'URMA超时',value:1}]}]});
assert.equal(pie.series[0].data[0].itemStyle.color,o.series[0].itemStyle.color);
const horizontal=TraceCharts.option({xAxis:{type:'value',name:'Trace 数'},yAxis:{type:'category'},series:source.series.slice(0,2)});
assert(horizontal.tooltip.formatter(p).includes('Trace 数'));
const wr=TraceCharts.option({series:[{name:'成功WR P90',type:'line',data:[2]},{name:'成功WR max',type:'line',data:[4]}]});
assert.equal(wr.series[0].itemStyle.color,TraceCharts.color('URMA通信'));
assert.notEqual(wr.series[1].itemStyle.color,wr.series[0].itemStyle.color);
assert.notEqual(wr.series[0].lineStyle.type,wr.series[1].lineStyle.type);
const events={},mock={on:(name,fn)=>events[name]=fn,clear:()=>{},setOption:o=>o};
const chart=TraceCharts.init({init:()=>mock});let rendered=chart.setOption(source);
events.legendselectchanged({selected:{'RPC网络相关':false}});
assert(rendered.tooltip.formatter(p).includes('100.0%'));
chart.clear();rendered=chart.setOption(source);
assert(rendered.tooltip.formatter(p).includes('40.0%'));
'''
    subprocess.run(['node', '-e', script, str(ASSET)], check=True)


def test_dense_axes_reserve_space_and_relayout_on_resize():
    script = r'''
const assert=require('assert'),fs=require('fs');eval(fs.readFileSync(process.argv[1],'utf8'));
const input={legend:{},grid:{top:45,bottom:40},xAxis:{type:'category',data:Array.from({length:30},(_,i)=>'long-worker-label-'+i),axisLabel:{interval:0,rotate:0}},yAxis:{type:'value'},dataZoom:[{type:'slider'}],series:[{name:'count',type:'bar',data:Array(30).fill(1)}]};
const o=TraceCharts.option(input,()=>({}),390,400);
assert(o.xAxis[0].axisLabel.rotate>=35);
assert.equal(o.xAxis[0].axisLabel.interval,'auto');
assert(o.grid[0].bottom>100);
let width=390,last;
const mock={on:()=>{},clear:()=>{},getWidth:()=>width,getHeight:()=>400,resize:()=>{},setOption:o=>last=o};
const c=TraceCharts.init({init:()=>mock});c.setOption(input);width=1600;c.resize();
assert(last.xAxis&&last.grid);
'''
    subprocess.run(['node', '-e', script, str(ASSET)], check=True)


def test_worker_time_buckets_pool_raw_events():
    source = (ASSET.parent.parent / 'read/read_correlation.js').read_text()
    start = source.index('function metricFor(')
    end = source.index('function correlationRows()', start)
    script = source[start:end] + r'''
const assert=require('assert');
const events=[{worker:'jp',timestamp:'2026-01-01T10:00:01',dimension:'rpc',trace_id:'a',network_ms:1},{worker:'zp',timestamp:'2026-01-01T10:00:01',dimension:'rpc',trace_id:'b',network_ms:9},{worker:'jp',timestamp:'2026-01-01T10:00:02',dimension:'rpc',trace_id:'a',network_ms:2}];
const b=buildCorrelationBuckets(events);
assert.equal(b.length,2);assert.equal(b[0].rpc.request_count,2);
assert.equal(b[0].trace_count,2);assert.equal(b[0].rpc.network_ms.p90,9);
assert.equal(b[1].second,'2026-01-01T10:00:02');
'''
    subprocess.run(['node', '-e', script], check=True)


def test_chart_height_grows_and_shrinks_with_layout():
    script = r'''
const assert=require('assert'),fs=require('fs');eval(fs.readFileSync(process.argv[1],'utf8'));
let width=390,height=300,last;const node={style:{}};
const mock={on:()=>{},clear:()=>{},getDom:()=>node,getWidth:()=>width,getHeight:()=>height,
 resize:opts=>{if(opts?.height)height=opts.height},setOption:o=>last=o};
const c=TraceCharts.init({init:()=>mock});
const input={legend:{},grid:{top:45,bottom:40},xAxis:{type:'category',data:['a','b']},yAxis:{type:'value'},series:Array.from({length:12},(_,i)=>({name:'long legend '+i,type:'bar',data:[1,2]}))};
c.setOption(input);const narrow=height;
assert(height-last.grid[0].top-last.grid[0].bottom>=260);
width=1600;c.resize();assert(height<narrow);
assert(height-last.grid[0].top-last.grid[0].bottom>=260);
assert.equal(node.style.height,height+'px');
assert(TraceCharts.layoutHeight({grid:{top:50,bottom:50},yAxis:{type:'category',data:Array(20).fill('x')}})>=740);
'''
    subprocess.run(['node', '-e', script, str(ASSET)], check=True)


def test_title_visibility_update_preserves_responsive_layout():
    script = r'''
const assert=require('assert'),fs=require('fs');eval(fs.readFileSync(process.argv[1],'utf8'));
let width=1200,height=400,last;
const node={style:{}};
const mock={on:()=>{},clear:()=>{},getDom:()=>node,getWidth:()=>width,getHeight:()=>height,
 resize:opts=>{if(opts?.height)height=opts.height},setOption:o=>last=o};
const c=TraceCharts.init({init:()=>mock});
const input={legend:{top:0},grid:{top:75,bottom:65},xAxis:{type:'value'},yAxis:{type:'category',data:['Client']},
 series:Array.from({length:10},(_,i)=>({name:'阶段耗时 '+i,type:'bar',data:[i]}))};
c.setOption(input);const wide=last.legend[0].width;
c.setOption({title:{show:false}});
width=300;c.resize();
assert(last.legend[0].width<wide);
assert(last.legend[0].width<=276);
assert.equal(last.series.length,10);
assert(height-last.grid[0].top-last.grid[0].bottom>=260);
assert.equal(last.title.show,false);
c.setOption({title:{show:false}},true);c.resize();assert(!last.legend);
'''
    subprocess.run(['node', '-e', script, str(ASSET)], check=True)


def test_coordinate_arrays_and_missing_scatter_points_are_preserved():
    script = r'''
const assert=require('assert'),fs=require('fs');eval(fs.readFileSync(process.argv[1],'utf8'));
const points=[null,null,[2,8.4],{value:[3,9],name:'failure'}];
const before=JSON.stringify(points);
const result=TraceCharts.option({series:[{name:'错误',type:'scatter',data:points}]}).series[0].data;
assert.equal(JSON.stringify(points),before);
assert.strictEqual(result[0],null);assert.strictEqual(result[1],null);
assert.deepStrictEqual(result[2].value,[2,8.4]);
assert.deepStrictEqual(result[3].value,[3,9]);assert.equal(result[3].name,'failure');
'''
    subprocess.run(['node', '-e', script, str(ASSET)], check=True)


def test_threshold_labels_default_inside_plot_and_keep_explicit_position():
    script = r'''
const assert=require('assert'),fs=require('fs');eval(fs.readFileSync(process.argv[1],'utf8'));
const raw={series:[{type:'line',data:[2],markLine:{label:{formatter:'slow WR'},data:[{yAxis:1.5}]}}]};
const result=TraceCharts.option(raw);
assert.equal(result.series[0].markLine.label.position,'insideEndTop');
assert.equal(raw.series[0].markLine.label.position,undefined);
raw.series[0].markLine.label.position='middle';
assert.equal(TraceCharts.option(raw).series[0].markLine.label.position,'middle');
'''
    subprocess.run(['node', '-e', script, str(ASSET)], check=True)


def test_horizontal_value_axis_name_reserves_edge_space_without_mutating_input():
    script = r'''
const assert=require('assert'),fs=require('fs');eval(fs.readFileSync(process.argv[1],'utf8'));
const raw={grid:{right:8,left:80},xAxis:{type:'value',name:'Trace',nameGap:15},yAxis:{type:'category',data:['A']},series:[]};
const result=TraceCharts.option(raw,()=>({}),400,300);
assert(result.grid[0].right>=60);
assert.equal(raw.grid.right,8);
raw.grid.right='30%';assert(TraceCharts.option(raw,()=>({}),400,300).grid[0].right>=120);
raw.xAxis.nameLocation='middle';assert.equal(TraceCharts.option(raw,()=>({}),400,300).grid.right,'30%');
raw.xAxis.nameLocation='end';raw.xAxis.inverse=true;raw.grid.left=0;
assert(TraceCharts.option(raw,()=>({}),400,300).grid[0].left>=60);
raw.xAxis=[{type:'value',name:'Trace',gridIndex:1}];raw.grid=[{right:8},{right:8}];
const multi=TraceCharts.option(raw,()=>({}),400,300);assert.equal(multi.grid[0].right,8);assert(multi.grid[1].right>=60);
'''
    subprocess.run(['node', '-e', script, str(ASSET)], check=True)


def test_overview_numeric_ticks_fit_plot_width_without_changing_values():
    source = (ASSET.parent.parent / 'overview/overview.js').read_text()
    draw = source[source.index('function draw('):source.index('function render(')]
    script = r'''
const assert=require('assert');
let width=308,config;
const el={style:{},get clientWidth(){return width}};
const $=()=>el,selected=()=>[{label:'sample',count:1731}],charts=[];
const F='Microsoft YaHei',colors=['blue'],STAGE_COLORS={};
const echarts={getInstanceByDom:()=>null};
const TraceCharts={init:()=>({setOption:o=>{config=o},resize:()=>{},
 getModel:()=>({getComponent:()=>({})}),
 getViewOfComponentModel:()=>({group:{y:0,getBoundingRect:()=>({y:0,height:24})}})})};
''' + draw + r'''
draw('count',['events'],r=>r.count);
assert(config.xAxis.splitNumber<=2);
assert.equal(config.xAxis.minInterval,1);
assert.equal(config.xAxis.axisLabel.hideOverlap,true);
assert.equal(config.series[0].data[0],1731);
width=1100;draw('count',['events'],r=>r.count);
assert(config.xAxis.splitNumber>2&&config.xAxis.splitNumber<=5);
draw('latency',['p90'],()=>0.125,{ms:true});
assert.notEqual(config.xAxis.minInterval,1);
assert.equal(config.series[0].data[0],0.125);
'''
    subprocess.run(['node', '-e', script], check=True)


def test_value_axis_density_uses_its_grid_and_preserves_explicit_scale():
    script = r'''
const assert=require('assert'),fs=require('fs');eval(fs.readFileSync(process.argv[1],'utf8'));
const source={grid:{left:160,right:30},xAxis:{type:'value',min:0,max:1000},
 yAxis:{type:'category',data:['one']},series:[{type:'bar',data:[823]}]};
const before=JSON.stringify(source),narrow=TraceCharts.option(source,()=>({}),340,400);
assert(narrow.xAxis[0].splitNumber<=2);
assert.equal(narrow.xAxis[0].axisLabel.hideOverlap,true);
assert.equal(narrow.xAxis[0].max,1000);
assert.equal(JSON.stringify(source),before);
assert(TraceCharts.option(source,()=>({}),1400,400).xAxis[0].splitNumber>2);
source.xAxis.interval=0.125;source.xAxis.max=1;
assert.equal(TraceCharts.option(source,()=>({}),340,400).xAxis[0].interval,0.125);
source.grid=[{left:10,right:10},{left:'40%',right:'15%'}];source.xAxis.gridIndex=1;
assert(TraceCharts.option(source,()=>({}),340,400).xAxis[0].splitNumber<=2);
source.xAxis={type:'time'};
assert.equal(TraceCharts.option(source,()=>({}),340,400).xAxis[0].splitNumber,undefined);
'''
    subprocess.run(['node', '-e', script, str(ASSET)], check=True)


def test_numeric_y_axis_labels_reserve_canvas_edges():
    script = r'''
const assert=require('assert'),fs=require('fs');eval(fs.readFileSync(process.argv[1],'utf8'));
const raw={grid:{left:20,right:12},xAxis:{type:'category',data:['A']},
 yAxis:[{type:'value'},{type:'value',position:'right'}],series:[{type:'bar',data:[1800]}]};
const result=TraceCharts.option(raw,()=>({}),300,400);
assert(result.grid[0].left>=60);
assert(result.grid[0].right>=60);
assert.equal(raw.grid.left,20);
raw.yAxis=[{type:'value'},{type:'value'}];
assert(TraceCharts.option(raw,()=>({}),300,400).grid[0].right>=60);
raw.yAxis=[{type:'value',axisLabel:{inside:true}}];
assert.equal(TraceCharts.option(raw,()=>({}),300,400).grid[0].left,20);
'''
    subprocess.run(['node', '-e', script, str(ASSET)], check=True)


def test_overview_draw_leaves_height_owned_by_shared_layout():
    source = (ASSET.parent.parent / 'overview/overview.js').read_text()
    draw = source[source.index('function draw('):source.index('function render(')]
    script = r'''
const assert=require('assert'),fs=require('fs');eval(fs.readFileSync(process.argv[1],'utf8'));
let width=308,height=400;const el={style:{},get clientWidth(){return width}};
const getComputedStyle=()=>({boxSizing:'border-box',paddingTop:'9px',paddingBottom:'9px',
  borderTopWidth:'1px',borderBottomWidth:'1px'});
const $=()=>el,selected=()=>Array.from({length:18},(_,i)=>({label:'run'+i})),charts=[];
const F='Microsoft YaHei',colors=['blue'],STAGE_COLORS={};
const chart={on:()=>{},clear:()=>{},getDom:()=>el,getWidth:()=>width,getHeight:()=>height,
  resize:opts=>{if(opts?.height)height=opts.height},setOption:()=>{},
  getModel:()=>({getComponent:()=>({})}),
  getViewOfComponentModel:()=>({group:{y:0,getBoundingRect:()=>({y:0,height:24})}})};
const echarts={init:()=>chart,getInstanceByDom:()=>null};
''' + draw + r'''
for(const size of [308,1100,308]){
  width=size;
  draw('chart',Array.from({length:6},(_,i)=>'多行较长图例写入线程阶段说明'+i),()=>1);
  assert.equal(parseFloat(el.style.height),height+20,'DOM content box must contain the canvas');
}
'''
    subprocess.run(['node', '-e', script, str(ASSET)], check=True)


def test_category_axis_names_reserve_endpoints_without_squeezing_known_wr_plots():
    script = r'''
const assert=require('assert'),fs=require('fs');eval(fs.readFileSync(process.argv[1],'utf8'));
for(const name of ['每Trace观测WR数','发送端本地桶','目标Worker','记录序号']){
  const input={grid:{left:55,right:15},xAxis:{type:'category',name,data:['one','two']},
    yAxis:{type:'value'},series:[]};
  const output=TraceCharts.option(input,()=>({}),340,400),grid=output.grid[0];
  assert(grid.right>=75,name+' must reserve the complete end label');
  assert(340-grid.left-grid.right>=140,name+' must retain a usable narrow plot');
  assert.equal(input.grid.right,15);
}
const input={grid:[{left:8,right:8},{left:8,right:'50%'}],
  xAxis:[{type:'category',name:'记录序号',gridIndex:1}],series:[]};
let output=TraceCharts.option(input,()=>({}),340,400);
assert.equal(output.grid[0].right,8);assert.equal(output.grid[1].right,170);
input.xAxis[0].inverse=true;output=TraceCharts.option(input,()=>({}),340,400);
assert(output.grid[1].left>=75);assert.equal(output.grid[0].left,8);
input.xAxis[0].nameLocation='middle';input.grid[1].left=8;
assert.equal(TraceCharts.option(input,()=>({}),340,400).grid[1].left,8);
'''
    subprocess.run(['node', '-e', script, str(ASSET)], check=True)


def test_vertical_axis_names_reserve_canvas_ends_on_their_own_grid():
    script = r'''
const assert=require('assert'),fs=require('fs');eval(fs.readFileSync(process.argv[1],'utf8'));
const input={grid:[{top:25,bottom:8},{top:25,bottom:8}],
  yAxis:[{type:'value',name:'Trace数',gridIndex:1}],series:[]};
let output=TraceCharts.option(input,()=>({}),340,400);
assert(output.grid[1].top>=35);assert.equal(output.grid[0].top,25);assert.equal(input.grid[1].top,25);
input.yAxis[0].inverse=true;output=TraceCharts.option(input,()=>({}),340,400);
assert(output.grid[1].bottom>=35);assert.equal(output.grid[1].top,25);
input.yAxis[0].nameLocation='start';output=TraceCharts.option(input,()=>({}),340,400);
assert(output.grid[1].top>=35);
input.grid[1].top='30%';assert.equal(TraceCharts.option(input,()=>({}),340,400).grid[1].top,120);
input.yAxis[0].nameLocation='middle';input.grid[1].top=25;
assert.equal(TraceCharts.option(input,()=>({}),340,400).grid[1].top,25);
'''
    subprocess.run(['node', '-e', script, str(ASSET)], check=True)
