const QUERY_RPC_COLORS={network:'#2563eb',queue:'#7c3aed',server:'#0891b2',framework:'#64748b',unobserved:'#a16207',summary:'#a16207'};
const QUERY_WORKER_COLORS={preprocess:'#2563eb',localRead:'#f59e0b',metadata:'#7c3aed',delivery:'#059669',boundary:'#94a3b8'};
const QUERY_LABELS={summary:'RPC阶段汇总（未分解）',network:'RPC网络/通信',queue:'Server请求排队',server:'Server执行窗口',framework:'RPC框架计时',unobserved:'RPC调用（双端计时未闭合）',preprocess:'preprocess 预处理',localRead:'localRead 本地读取/inline交付',metadata:'metadata 元数据处理',delivery:'delivery 响应交付',boundary:'Worker阶段边界时间'};
let rpcAuditPage=0;
function rpcAuditItems(){
 const op=$('rpc-audit-operation')?.value||'GET';
 return [...ROWS.map(r=>({...r,operation:'GET'})),...(typeof WRITE_ROWS==='undefined'?[]:WRITE_ROWS).map(r=>({...r,operation:'SET'}))].filter(r=>r.operation===op).flatMap(r=>rpcObservations(r).map(c=>({...c,trace_id:r.trace_id,budget:r.rpc_analysis})));
}
function filterRpcAudit(items){const method=$('rpc-audit-method')?.value||'',q=($('rpc-audit-search')?.value||'').toLowerCase();return items.filter(c=>(!method||c.method===method)&&`${c.method} ${c.trace_id}`.toLowerCase().includes(q));}
ReportRegistry.bindSource('read_rpc_audit',()=>({calls:rpcAuditItems()}),()=>({calls:filterRpcAudit(rpcAuditItems())}));
function renderRpcAudit(){
 if(!$('rpc-audit-rows'))return;
 const op=$('rpc-audit-operation')?.value||'GET',items=rpcAuditItems();
 const methods=[...new Set(items.map(c=>c.method).filter(Boolean))].sort();const methodSelect=$('rpc-audit-method');if(methodSelect&&methodSelect.options&&methodSelect.options.length===1&&methodSelect.insertAdjacentHTML)methods.forEach(name=>methodSelect.insertAdjacentHTML('beforeend',`<option value="${esc(name)}">${esc(name)}</option>`));const filtered=filterRpcAudit(items);rpcAuditPage=Math.max(0,Math.min(rpcAuditPage,Math.ceil(filtered.length/4)-1));
 const reasons={summary_only:'阶段汇总窗口；不计入网络残差',serial_client_interval:'计入Client串行预算',overlapping_client_interval:'重叠调用，单独保留',different_process:'其他发起进程',ambiguous_client_process:'Client进程未唯一确定',invalid_timing:'时间戳缺失或不一致'};
 $('rpc-audit-scope').textContent=`${op} · ${filtered.length}条RPC观测 · ${rpcAuditPage+1}/${Math.max(1,Math.ceil(filtered.length/4))}页；所有方法均列出。重叠调用取最大不重叠残差路径，这是保守路径值，不能视为所有并发RPC耗时之和。`;const rpcAuditPages=Math.max(1,Math.ceil(filtered.length/4));$('rpc-audit-page-label').textContent=`第 ${rpcAuditPage+1} / ${rpcAuditPages} 页 · ${filtered.length} 条`;$('rpc-audit-prev').disabled=rpcAuditPage<=0;$('rpc-audit-next').disabled=rpcAuditPage>=rpcAuditPages-1;
 $('rpc-audit-rows').innerHTML=filtered.slice(rpcAuditPage*4,rpcAuditPage*4+4).map(c=>`<tr><td><code>${esc(c.trace_id)}</code></td><td>${esc(c.method)}</td><td>${esc((c.owner||[]).join(':'))}</td><td>${c.evidence_scope==='access_stage'?'未分解 · 窗口 '+fmt(c.total_ms):c.network_ms==null?'未观测/无效':fmt(c.network_ms)}</td><td>${esc(reasons[c.selection_reason]||c.selection_reason)}${c.budget.budget_clipped_ms?`<br>Client预算不足 ${fmt(c.budget.budget_clipped_ms)}`:''}</td><td><details><summary>原始字段</summary><pre style="white-space:pre-wrap;overflow-wrap:anywhere">${esc(JSON.stringify({evidence_scope:c.evidence_scope,stage_key:c.stage_key,total_ms:c.total_ms,fields_us:c.fields_us,clocks_ns:c.clocks_ns,cntl_failed:c.cntl_failed,cntl_error_code:c.cntl_error_code,source:c.source},null,2))}</pre></details></td></tr>`).join('')||'<tr><td colspan="6">未采集到详细RPC或access阶段汇总证据；不能据此认定无RPC耗时。</td></tr>';
 $('rpc-audit-prev').onclick=()=>{rpcAuditPage--;renderRpcAudit()};$('rpc-audit-next').onclick=()=>{rpcAuditPage++;renderRpcAudit()};$('rpc-audit-operation').onchange=()=>{rpcAuditPage=0;$('rpc-audit-method').innerHTML='<option value="">全部 RPC 方法</option>';renderRpcAudit()};$('rpc-audit-method').onchange=()=>{rpcAuditPage=0;renderRpcAudit()};$('rpc-audit-search').oninput=()=>{rpcAuditPage=0;renderRpcAudit()};
}
function renderQueryBreakdown(){
 const rows=scopeRows(),rpc=rows.flatMap(r=>(r.query_and_get_breakdown?.rpc||[]).map(c=>({...c,trace_id:r.trace_id,problem:r.failure_reason||r.error_subcategory||r.focus_primary_problem}))),workers=rows.flatMap(r=>(r.query_and_get_breakdown?.worker||[]).map(c=>({...c,trace_id:r.trace_id,problem:r.failure_reason||r.error_subcategory||r.focus_primary_problem})));
 const worker=workers.filter(c=>c.stackable),unplotted=workers.filter(c=>!c.stackable);
 function draw(id,entries,colors,values){
  const chart=chartAt(id),keys=Object.keys(colors).filter(key=>entries.some(entry=>Object.prototype.hasOwnProperty.call(values(entry),key)));
  if(!entries.length){
   const reasons={missing_phases:'缺少阶段',missing_total:'缺少总耗时',invalid_duration:'耗时无效',phase_sum_exceeds_total:'阶段和超过总耗时'};
   const counts={};unplotted.forEach(c=>{const reason=reasons[c.exclusion_reason]||'阶段不完整或不闭合';counts[reason]=(counts[reason]||0)+1});
   const text=id==='query-worker-breakdown-chart'?(workers.length?Object.entries(counts).map(([k,v])=>`${k} ${v} 条`).join('；'):'当前范围未采集到 QueryAndGet 完成记录；不能据此判断 Worker 被终止'):'当前范围缺少带总耗时的 QueryAndGet RPC 记录';
   chart.clear?.();chart.setOption?.({animation:false,graphic:{type:'text',left:'center',top:'middle',style:{text,fontSize:13,fill:'#64748b',width:280,overflow:'break'}},series:[]});return;
  }
  chart.clear?.();
  chart.setOption({animation:false,legend:{type:'scroll',top:0,left:8,right:8},grid:{left:55,right:15,top:65,bottom:68},tooltip:{trigger:'axis',confine:true,formatter:ps=>{const c=entries[ps[0]?.dataIndex];return c?`<b>${esc(c.trace_id)}</b><br>${esc((c.owner||[]).join(':'))}<br>窗口 ${c.total_ms==null?'未观测':fmt(c.total_ms)}<br>${keys.filter(k=>values(c)[k]>0).map(k=>`${QUERY_LABELS[k]}: ${fmt(values(c)[k])}`).join('<br>')}<br>${esc(c.problem)}`:''}},xAxis:{type:'category',data:entries.map((c,i)=>String(i+1)),name:'记录序号'},yAxis:{type:'value',name:'窗口耗时(ms)'},dataZoom:[{type:'inside'},{type:'slider',height:18,bottom:10}],series:keys.map(k=>({name:QUERY_LABELS[k],type:'bar',stack:id,barMaxWidth:20,itemStyle:{color:colors[k]},data:entries.map(c=>values(c)[k]??0)}))});
  chart.off('click');chart.on('click',p=>{const c=entries[p.dataIndex];if(c){selectedId=c.trace_id;renderTable();renderDetail();$('trace-detail-panel').scrollIntoView({behavior:'smooth'})}});
 }
 draw('query-rpc-breakdown-chart',rpc.filter(c=>c.total_ms!=null),QUERY_RPC_COLORS,c=>c.breakdown_ms||{unobserved:c.total_ms});
 draw('query-worker-breakdown-chart',worker,QUERY_WORKER_COLORS,c=>({...c.phases_ms,boundary:Math.max(0,c.phase_delta_ms)}));
 $('query-breakdown-scope').textContent=`当前 ${rows.length}条GET；详细RPC ${rpc.filter(c=>c.evidence_scope!=='access_stage').length}次，阶段汇总窗口 ${rpc.filter(c=>c.evidence_scope==='access_stage').length}条（不是调用次数），Worker四阶段完整且可堆叠${worker.length}次，未完整/不闭合${unplotted.length}次。RPC与Worker是不同观察层，不能相加，也不按数组序号跨图配对。`;
 $('query-breakdown-unplotted').innerHTML=unplotted.length?`<details><summary>未强行堆叠的Worker记录</summary><pre style="white-space:pre-wrap;overflow-wrap:anywhere">${esc(JSON.stringify(unplotted,null,2))}</pre></details>`:'';
 renderRpcAudit();
}
