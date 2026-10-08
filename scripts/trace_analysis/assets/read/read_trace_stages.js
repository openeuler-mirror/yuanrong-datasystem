function renderReadTraceStages(row){
  const entries=Object.entries(row?.focus_breakdown_ms||{}).filter(([,v])=>Number.isFinite(v)&&v>0);
  const wr=row?.urma_requests||[],models=wr.map(readWrTimelineModel);
  const labels=['Client GET',...wr.flatMap((r,i)=>['Elapsed','sleep','poll','通知→唤醒','唤醒→观察'].map(name=>`Chunk ${r.write_chunk_index||i+1} · ${name}`))];
  const series=entries.map(([name,value])=>({name,type:'bar',stack:'client-stages',barMaxWidth:24,data:labels.map((_,i)=>i===0?value:null)}));
  const windows=['轮询线程 sleep','CQ 轮询','通知→唤醒','唤醒→观察'];
  const names=['WR Elapsed',...windows],colors=['#8052c0','#c18016','#2563eb','#db2777','#16805c'];
  for(let k=0;k<names.length;k++)series.push({name:names[k],type:'bar',stack:'wr-window',barMaxWidth:24,label:{show:true,position:'right',formatter:p=>p.value==null?'':Number(p.value).toFixed(3)+' ms'},itemStyle:{color:colors[k]},data:labels.map((_,i)=>{
    if(i===0||(i-1)%5!==k)return null;
    const index=Math.floor((i-1)/5),r=wr[index];
    return k===0?(Number.isFinite(r.total_ms)?r.total_ms:null):(models[index].intervals.find(s=>s.name===windows[k-1])?.duration_ms??null);
  })});
  const chart=chartAt('read-selected-stage-chart');
  chart.setOption({animation:false,legend:{top:0},grid:{left:155,right:65,top:75,bottom:65},xAxis:{type:'value',name:'耗时 / 时间窗 (ms)',nameLocation:'middle',nameGap:30},yAxis:{type:'category',inverse:true,data:labels,axisLabel:{width:140,overflow:'truncate'}},tooltip:{trigger:'item',confine:true,formatter:p=>{
    if(p.dataIndex===0)return `Trace：${esc(row?.trace_id)}<br>${esc(p.seriesName)}：${p.value} ms`;
    const index=Math.floor((p.dataIndex-1)/5),r=wr[index];
    return `Trace：${esc(row.trace_id)}<br>URMA WR ${esc(r.request_id)} · Chunk ${r.write_chunk_index||index+1}/${r.write_chunk_count||'?'}<br>${esc(p.seriesName)}：${p.value??'未观测'} ms<br>condition wait：${r.wait_completion_ms??'未观测'} ms<br>${esc(r.src_addr||'')} → ${esc(r.target_addr||'')}<br>`+models[index].notes.map(esc).join('<br>');
  }},series,graphic:entries.length||wr.length?[]:[{type:'text',left:'center',top:'middle',style:{text:'请选择 Trace / 未观测到阶段',fill:'#667085',font:'12px Microsoft YaHei'}}]});
  $('read-selected-judgment').textContent=row?`${row.trace_id} · Client ${row.client_ms??'未观测'} ms · ${wr.length} 个 WR。Client 行为互斥 stacked bars；每个 Chunk 单独列出 Elapsed、sleep、poll 和通知唤醒时间窗。各行仅比较时长，不表示同一时钟起点，不能跨行相加。缺失时间点保留未观测，不补零。`:'请选择 Trace';
  $('read-selected-wr-scope').textContent=wr.map((r,i)=>`Chunk ${r.write_chunk_index||i+1}/${r.write_chunk_count||'?'} · WR ${r.request_id||'未记录'}：Elapsed ${r.total_ms??'未观测'} ms，condition wait ${r.wait_completion_ms??'未观测'} ms`).join('；');
}

function readWrTimelineModel(request){
  const t=request.trace_us||{},valid=key=>Number.isFinite(t[key]);
  const keys=['post','wait','sleep_start','sleep_end','poll_begin','poll_end','notify','awake','observed'];
  const origin=valid('post')?t.post:null;
  const spans=[['提交→通知','post','notify'],['调用方等待','wait','observed'],['轮询线程 sleep','sleep_start','sleep_end'],['CQ 轮询','poll_begin','poll_end'],['通知→唤醒','notify','awake'],['唤醒→观察','awake','observed']];
  const intervals=[],missing=[];
  for(const [name,start,end] of spans){
    if(origin===null||!valid(start)||!valid(end)){missing.push(name+'：未观测');continue;}
    if(t[end]<t[start]){missing.push(name+'：时序无效');continue;}
    intervals.push({name,start_ms:(t[start]-origin)/1000,end_ms:(t[end]-origin)/1000,duration_ms:(t[end]-t[start])/1000});
  }
  const notes=[];
  if(t.pre_completed_before_wait===1)notes.push('进入 wait 前已完成；post→wait 不是通信耗时');
  if(t.woken_by_previous_event===1)notes.push('由前一事件唤醒；唤醒→观察不能归因为本 WR 调度');
  if(t.event_processing_and_wait_latency_valid===0)notes.push('事件处理/等待时延标志无效，不把该字段的 0 当作已测量结果');
  const sleep=intervals.find(x=>x.name==='轮询线程 sleep'),wake=intervals.find(x=>x.name==='通知→唤醒');
  if(sleep)notes.push(`已观测一次 sleep ${sleep.duration_ms.toFixed(3)} ms；不是完整轮询休眠总量`);
  if(wake)notes.push(`通知→唤醒 ${wake.duration_ms.toFixed(3)} ms${t.waited_for_notification===1?'（本次等待通知）':'（未确认本次阻塞等待）'}`);
  notes.push('区间可能重叠，不相加；缺少 CQ 就绪时刻，不能把 post→poll 全部归因于网络或 sleep');
  return {intervals,missing,notes,events:keys.filter(valid).map(key=>({key,relative_ms:origin===null?null:(t[key]-origin)/1000})),flags:['waited_for_notification','pre_completed_before_wait','woken_by_previous_event','event_processing_and_wait_latency_valid'].map(key=>key+'='+String(t[key]??'未观测'))};
}
