var TraceVisuals = (() => {
  const esc=v=>String(v??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
  const colors={timeout:'#c18016',error:'#b42318',urma:'#8052c0',rpc:'#2563eb',access:'#16805c',other:'#64748b'};
  function component(event){
    const evidence=event.evidence||event,text=evidence.text||'';
    const original=text.match(/^(.+?):\d+:(?=\d{4}-\d\d-\d\d[T ])/)?.[1];
    const path=String(original||evidence.member||'').replace(/\\/g,'/');
    if(/(?:^|\/)(?:client(?:\/|_|\d)|ds_client|sdk(?:\/|_))/i.test(path))return 'Client';
    if(/(?:^|\/)(?:coordinator(?:\/|_|\.|\d)|ds_coordinator)|collected_coordinator_logs/i.test(path))return 'Coordinator';
    if(/(?:^|\/)(?:worker(?:\/|_|\.|\d)|kvcache\.(?:INFO|WARNING|ERROR))|collected_worker_logs/i.test(path))return 'Worker';
    const file=text.match(/\|\s*([^|]+\.(?:cpp|cc|h)):\d+\s*\|/)?.[1]||'';
    if(/client|object_posix/i.test(file))return 'Client';
    if(/coordinator/i.test(file))return 'Coordinator';
    if(/worker/i.test(file))return 'Worker';
    return '组件未观测';
  }
  function actor(event){
    const evidence=event.evidence||event,text=evidence.text||'';
    const original=text.match(/^(.+?):\d+:(?=\d{4}-\d\d-\d\d[T ])/)?.[1];
    const role=component(event);
    const process=event.process&&event.process!=='进程未观测'?event.process:original||evidence.member||'实例未观测';
    return {role,instance:role+' · '+process};
  }
  function uniqueEvidence(evidence){
    const records=[],seen=new Map();let duplicates=0,indexRows=0;
    for(const e of evidence||[]){
      if(/(?:^|\/)unique_traces?[^/]*\.txt$/i.test(e.member||'')){indexRows++;continue;}
      const raw=String(e.text||'');
      const prefix=raw.match(/^(.+?):(\d+):/);
      const original=prefix && (
        /^\d{4}-\d\d-\d\d[T ]/.test(raw.slice(prefix[0].length)) ||
        /^(?:\/|[A-Za-z]:[\\/]|[^:\s]+[\\/])/.test(prefix[1]) ||
        /\.log(?:[._]\d+)*$/i.test(prefix[1]));
      const key=original?JSON.stringify([e.source,raw]):JSON.stringify([e.source,e.member,e.line,raw]);
      const origin={source:e.source,member:e.member,line:e.line};
      if(seen.has(key)){seen.get(key).origins.push(origin);duplicates++;continue;}
      const record={...e,origins:[origin]};seen.set(key,record);records.push(record);
    }
    return {records,duplicates,indexRows,rawCount:(evidence||[]).length};
  }
  function timeline(evidence){
    evidence=uniqueEvidence(evidence).records;
    const lanes=new Map(),seen=new Set();let undated=0;
    for(const e of evidence||[]){
      const text=e.text||'',m=text.match(/(\d{4}-\d\d-\d\d[T ]\d\d:\d\d:\d\d)(?:\.(\d+))?/);
      if(!m){undated++;continue}
      const time=Date.parse(m[1].replace(' ','T')+'Z')+Number('0.'+(m[2]||'0'))*1000;
      if(!Number.isFinite(time)){undated++;continue}
      const fields=text.slice(m.index).split('|').map(x=>x.trim()),process=fields[4]?.match(/^(\d+):\d+$/)?.[1];
      const lane=process&&fields[3]?fields[3]+' / PID '+process:(e.member||e.source||'来源未标明');
      const key=[lane,e.member,e.line,text].join('\0');if(seen.has(key))continue;seen.add(key);
      const kind=/timeout|timed out|deadline|超时/i.test(text)?'timeout':/\|\s*[EF]\s*\||failed|error|失败/i.test(text)?'error':/URMA|UB_|urma_/i.test(text)?'urma':/RPC|QueryAndGet|RemoteGet/i.test(text)?'rpc':/DS_KV_CLIENT|DS_POSIX/.test(text)?'access':'other';
      if(!lanes.has(lane))lanes.set(lane,[]);lanes.get(lane).push({time,kind,evidence:e,timestamp:m[0]});
    }
    return {undated,lanes:[...lanes].map(([name,events])=>{events.sort((a,b)=>a.time-b.time);return {name,events:events.map((e,i)=>({...e,component:component(e),offset_ms:e.time-events[0].time,elapsed_ms:i?e.time-events[i-1].time:null}))}})};
  }
  function eventEvidenceHtml(event,process){
    const evidence=event.evidence||{};
    const ms=value=>value==null?'未观测':value.toFixed(3)+' ms';
    const header=[process,'组件：'+(event.component||component(event)),event.timestamp,
      '进程内累计：'+ms(event.offset_ms)+' · elapsed（相邻日志）：'+(event.elapsed_ms==null?'首条 —':ms(event.elapsed_ms)),
      (evidence.member||evidence.source||'来源未记录')+(evidence.line!=null?':'+evidence.line:'')];
    return header.map(esc).join('\n')+'\n'+TraceLogFields.render(evidence.text||'');
  }
  function renderTimeline(node,item,echarts,charts){
    const model=item.timelineModel||timeline(item.evidence),events=model.lanes.flatMap((lane,i)=>lane.events.map(e=>({...e,lane:i}))),lanes=model.lanes.map(x=>x.name);
    document.getElementById('selected-event-evidence').textContent='';document.getElementById('selected-event-coverage').textContent='';
    const old=echarts.getInstanceByDom(node);if(old)old.dispose();node.innerHTML='';
    if(!events.length){node.textContent='未观测到带时间戳的保留证据';return}
    node.style.height=Math.max(340,lanes.length*56+130)+'px';
    const c=charts.init(echarts,node);c.setOption({animation:false,legend:{top:0},grid:{left:node.clientWidth<600?125:180,right:24,top:65,bottom:100},tooltip:{trigger:'item',confine:true,extraCssText:'max-width:calc(100vw - 24px);white-space:normal;overflow-wrap:anywhere;',formatter:p=>{const e=p.data.event;return esc(lanes[e.lane])+'<br>'+esc(e.timestamp)+'<br>组件：<b>'+esc(e.component||component(e))+'</b><br>进程内累计：<b>'+e.offset_ms.toFixed(3)+' ms</b><br>elapsed（相邻日志）：<b>'+(e.elapsed_ms==null?'首条 —':e.elapsed_ms.toFixed(3)+' ms')+'</b>'}},xAxis:{type:'value',name:'进程内相对时间 (ms)',nameLocation:'middle',nameGap:28,axisLabel:{formatter:v=>Number(v).toFixed(3)}},yAxis:{type:'category',data:lanes,axisLabel:{width:node.clientWidth<600?110:160,overflow:'truncate'}},dataZoom:[{type:'inside'},{type:'slider',bottom:8,height:18}],series:Object.entries(colors).map(([kind,color])=>({name:{timeout:'超时',error:'错误',urma:'URMA',rpc:'RPC',access:'Access',other:'其他'}[kind],type:'scatter',symbolSize:10,itemStyle:{color},data:events.filter(e=>e.kind===kind).map(e=>({value:[e.offset_ms,e.lane],event:e}))}))});
    const detail=document.getElementById('selected-event-evidence');
    const show=e=>{detail.innerHTML=eventEvidenceHtml(e,lanes[e.lane]);};
    c.on('click',p=>show(p.data.event));show(events[0]);
    document.getElementById('selected-event-coverage').textContent=`保留证据 ${events.length} 条 · ${lanes.length} 个进程/来源 · 无时间戳 ${model.undated} 条 · 未保留 ${item.dropped_evidence||0} 条。每行独立归零，跨行不表示同时发生；散点为日志时刻，elapsed 为同进程相邻日志间隔，不是阶段执行时长；组件按日志来源识别。`;
    return c;
  }
  function flowSvg(graph,title){
    const marker='flow-arrow-'+Array.from(title).map(c=>c.codePointAt(0).toString(16)).join('-');
    const nodes=graph.nodes||[],edges=(graph.edges||[]).filter(e=>e.status==='present'),width=Math.max(760,nodes.length*180),height=180+edges.length*100;
    const positions=new Map(nodes.map((n,i)=>[n.id,90+i*(width-180)/Math.max(1,nodes.length-1)]));
    const box=(x,y,w,h,text)=>`<foreignObject x="${x}" y="${y}" width="${w}" height="${h}"><div xmlns="http://www.w3.org/1999/xhtml" style="font-family:Microsoft YaHei,sans-serif;font-size:13px;line-height:1.5;text-align:center;overflow-wrap:anywhere;background:white">${esc(text)}</div></foreignObject>`;
    return `<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 ${width} ${height}" role="img" aria-label="${esc(title)}" style="display:block;width:100%;min-width:760px"><title>${esc(title)}</title><defs><marker id="${marker}" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto"><path d="M0 0L10 5L0 10z" fill="#2563eb"/></marker></defs>${nodes.map(n=>{const x=positions.get(n.id);return `<rect x="${x-78}" y="12" width="156" height="80" rx="8" fill="white" stroke="#64748b"/>`+box(x-70,20,140,64,({client:'SDK',entry_worker:'入口 Worker',data_worker:'Data Worker',meta_worker:'Meta Worker',transport:'Transport'}[n.role]||n.label||n.id)+'\n'+((n.top_ips||[]).slice(0,2).join(' / ')||'IP 未观测'))+`<path d="M${x} 96V${height-25}" stroke="#dce3ef" stroke-dasharray="4 4"/>`}).join('')}${edges.map((e,i)=>{const x=positions.get(e.source),y=positions.get(e.target),top=150+i*100;if(x===undefined||y===undefined)return '';return `<g><title>${esc(e.evidence||e.summary)}</title><path d="M${x} ${top}L${y} ${top}" stroke="#2563eb" stroke-width="2" marker-end="url(#${marker})"/>`+box(Math.max(5,(x+y)/2-145),top+10,290,75,(i+1)+'. '+(e.operation||e.name)+' · '+(e.rollup?.trace_count??'未统计')+' Trace'+(e.rollup?.max_ms!=null?' / max '+Number(e.rollup.max_ms).toFixed(3)+' ms':''))+'</g>'}).join('')}</svg>`;
  }
  return {timeline,renderTimeline,eventEvidenceHtml,flowSvg,component,actor,uniqueEvidence};
})();
