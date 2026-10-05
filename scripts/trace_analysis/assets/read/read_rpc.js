function rpcObservations(row) {
  const analysis = row.rpc_analysis || {};
  const calls = (analysis.calls || []).map(call => ({...call, evidence_scope:'rpc_call',
    total_ms:call.total_ms ?? (call.fields_us?.e2e_us == null ? null : call.fields_us.e2e_us / 1000)}));
  const windows = (analysis.summary_windows || []).filter(window => !calls.some(call =>
    window.method_hint && String(call.method).split('.').pop() === window.method_hint &&
    JSON.stringify(call.owner) === JSON.stringify(window.owner) && call.total_ms != null));
  return calls.concat(windows.map(window => ({...window, method:window.stage_label + ' · 汇总窗口'})));
}
function renderRpcAnalysis() {
  const grouped = new Map();
  ROWS.forEach(row => rpcObservations(row).forEach(call => {
    const method = call.method || 'RPC未命名';
    const item = grouped.get(method) || {method, traces:new Set(), values:[], calls:0, windows:0, failed:0};
    item.traces.add(row.trace_id);
    if (call.total_ms != null && Number.isFinite(call.total_ms) && call.total_ms >= 0) item.values.push(call.total_ms);
    if (call.evidence_scope === 'access_stage') item.windows++;
    else {item.calls++; if (call.cntl_failed || call.cntl_error_code) item.failed++;}
    grouped.set(method, item);
  }));
  const rows = [...grouped.values()].sort((a,b) => b.traces.size-a.traces.size);
  const shortRpc = value => String(value).replace('datasystem.','').replace('WorkerOCService.','W.')
    .replace('WorkerWorkerOCService.','WW.').replace('MasterOCService.','M.')
    .replace(/(?:client|worker)\.rpc\./g,'');
  const names = rows.map(row => shortRpc(row.method));
  const base = {animation:false,tooltip:{trigger:'axis',confine:true},legend:{top:0},
    grid:{left:48,right:20,top:65,bottom:100},xAxis:{type:'category',data:names,axisLabel:{rotate:32,interval:0}},
    yAxis:{type:'value',name:'观测数',minInterval:1}};
  function draw(id, series) {
    const chart = chartAt(id);chart.clear();
    chart.setOption(rows.length ? {...base,series} : {animation:false,series:[],graphic:{type:'text',left:'center',top:'middle',
      style:{text:'未采集详细RPC或access阶段汇总证据\n不能据此判断没有RPC',fontSize:13,fill:'#64748b'}}});
  }
  draw('rpc-method-chart', [
    {name:'唯一Trace',type:'bar',data:rows.map(r=>r.traces.size),barMaxWidth:28},
    {name:'详细调用',type:'bar',data:rows.map(r=>r.calls),barMaxWidth:28},
    {name:'汇总窗口',type:'bar',data:rows.map(r=>r.windows),barMaxWidth:28},
  ]);
  const bins = ['<2ms','2–5ms','5–10ms','10–20ms','≥20ms'];
  const hist = rows.map(row => {
    const counts = [0,0,0,0,0];
    row.values.forEach(v=>counts[v<2?0:v<5?1:v<10?2:v<20?3:4]++);
    return counts;
  });
  draw('rpc-histogram-chart', bins.map((name,i)=>({name,type:'bar',stack:'rpc-latency',data:hist.map(x=>x[i]),barMaxWidth:30})));
  document.querySelector('#rpc-summary-table tbody').innerHTML = rows.map(row => {
    const values = row.values.sort((a,b)=>a-b);
    return `<tr><td title="${esc(row.method)}">${esc(shortRpc(row.method))}</td><td>${row.traces.size}</td><td>${row.calls} / ${row.windows}</td><td>${row.calls?row.failed:'未观测'}</td><td>${values.length?latencyValue(percentile(values,.5)):'未观测'}</td><td>${values.length?latencyValue(percentile(values,.9)):'未观测'}</td><td>${values.length?latencyValue(values[values.length-1]):'未观测'}</td></tr>`;
  }).join('') || '<tr><td colspan="7" class="empty">当前范围未采集RPC观测</td></tr>';
  paginateRenderedTable($('rpc-summary-table'));
}
