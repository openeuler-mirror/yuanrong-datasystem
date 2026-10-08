(() => {
  function install(){
    const rows=typeof ROWS!=='undefined'?ROWS:typeof ALL!=='undefined'?ALL:[];
    const nav=document.querySelector('#nav,#write-nav');
    if(!nav)return;
    const escape=value=>String(value??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
    const section=document.getElementById('trace-event-timeline')||document.createElement('section');
    if(!section.isConnected){
      section.id='trace-event-timeline';section.className='panel';
      section.innerHTML=`<h2>8. Trace 事件时间线</h2><p class="caption">按进程展示保留日志的先后顺序；每个进程独立归零，不用跨机墙钟差计算通信耗时。</p><div class="controls"><input id="event-trace-search" aria-label="搜索时间线 Trace" placeholder="搜索 Trace ID"><select id="event-trace-select" aria-label="时间线 Trace"></select></div><div class="controls"><select id="event-component-filter" aria-label="筛选日志组件"></select><select id="event-process-filter" aria-label="筛选进程"></select></div><p id="event-filter-summary" class="caption"></p><p id="selected-event-coverage" class="caption"></p><div id="trace-event-chart" class="chart"></div><h3 class="chart-caption">图 8-1 Trace 分进程事件时间线</h3><h3 id="trace-event-table-title" class="table-caption">表 8-1 Trace 事件明细</h3><div class="table-wrap"><table id="trace-event-table"><thead><tr><th>进程 / 本地时间</th><th>进程内累计 / 相邻 elapsed</th><th>组件 / 类型</th><th>事件证据</th></tr></thead><tbody id="event-table-body"></tbody></table></div><div id="event-table-pager" class="pager"></div><details class="analysis-details" open><summary>选中事件原始日志</summary><pre id="selected-event-evidence"></pre></details>`;
      const appendix=document.getElementById('source-logic');
      if(appendix){appendix.before(section);appendix.querySelector('h2').textContent='附录 9. 源码与访问拓扑';nav.querySelector('a[href="#source-logic"]').textContent='附录 9. 源码与访问拓扑';}
      else {const download=document.getElementById('download-model')?.closest('section');if(download){download.before(section);download.querySelector('h2').textContent='附录 · 下载与口径';for(const a of nav.querySelectorAll('a[href="#write-downloads"]'))a.textContent='附录 · 下载与口径';}else document.querySelector('main').append(section);}
      for(const [id,label,sub] of [['trace-event-timeline','8. Trace 事件时间线',false],['trace-event-chart','图 8-1 Trace 分进程事件时间线',true],['trace-event-table-title','表 8-1 Trace 事件明细',true]]){
        const link=document.createElement('a');link.href='#'+id;link.textContent=label;if(sub)link.className='sub';
        const target=document.getElementById(id),caption=id==='trace-event-chart'?target.nextElementSibling:id==='trace-event-timeline'?target.querySelector('h2'):target;caption.dataset.navigationTarget=id;
        const after=[...nav.querySelectorAll('a')].find(a=>a.textContent.startsWith('附录')||a.textContent.startsWith('9. 下载'));if(after)after.before(link);else nav.append(link);
      }
    }
    const get=id=>document.getElementById(id),names={timeout:'超时',error:'错误',urma:'URMA',rpc:'RPC',access:'Access',other:'其他'};
    let events=[],page=1,chart,model={lanes:[],undated:0},currentRow;
    const highlight=text=>TraceLogFields.decorate(escape(text).replace(/(timed out|timeout|deadline exceeded|\bERROR\b|\bfailed\b|URMA_ELAPSED_TOTAL|QueryAndGet|QueryMeta|RemoteGet|ClientSend|ClientRecv)/gi,'<mark>$1</mark>'));
    const component=TraceVisuals.component;
    function show(event){get('selected-event-evidence').innerHTML=TraceVisuals.eventEvidenceHtml(event,event.process);}
    function scopedEvents(){
      const process=get('event-process-filter').value,selectedComponent=get('event-component-filter').value;
      return events.filter(e=>(!process||e.process===process)&&(!selectedComponent||e.component===selectedComponent));
    }
    ReportRegistry.bindSource('trace_events',()=>({events}),()=>({events:scopedEvents()}));
    function table(){
      const filtered=scopedEvents();
      const size=filtered.length<=20?20:4,pages=Math.max(1,Math.ceil(filtered.length/size));page=Math.min(page,pages);
      get('event-table-body').innerHTML=filtered.slice((page-1)*size,page*size).map(e=>`<tr data-event="${events.indexOf(e)}" class="event-${e.kind}"><td>${escape(e.process)}<br>${escape(e.timestamp)}</td><td>+${e.offset_ms.toFixed(3)} ms<br><span class="caption">（${e.elapsed_ms==null?'首条 —':e.elapsed_ms.toFixed(3)+' ms'}）</span></td><td><b>${escape(e.component)}</b><br><span class="event-kind">${names[e.kind]}</span></td><td><button>查看原文</button> ${highlight(e.evidence.text.slice(-180))}</td></tr>`).join('')||'<tr><td colspan="4">未观测到匹配事件</td></tr>';
      get('event-table-body').querySelectorAll('tr[data-event]').forEach(tr=>tr.onclick=()=>show(events[Number(tr.dataset.event)]));
      get('event-table-pager').hidden=filtered.length<=20;
      get('event-table-pager').innerHTML=filtered.length<=20?'':`<button id="event-prev" ${page===1?'disabled':''}>上一页</button><span>${page} / ${pages} · ${filtered.length} 条</span><button id="event-next" ${page===pages?'disabled':''}>下一页</button>`;
      if(filtered.length>20){get('event-prev').onclick=()=>{page--;table()};get('event-next').onclick=()=>{page++;table()};}
      return filtered;
    }
    function filter(){
      page=1;const filtered=table(),included=new Set(filtered.map(e=>e.evidence));
      if(chart){chart.dispose();chart=null}
      const view={undated:model.undated,lanes:model.lanes.map(l=>({...l,events:l.events.filter(e=>included.has(e.evidence))})).filter(l=>l.events.length)};
      chart=TraceVisuals.renderTimeline(get('trace-event-chart'),{timelineModel:view,dropped_evidence:currentRow?.dropped_evidence},echarts,TraceCharts);
      if(!chart&&typeof ReportRegistry!=='undefined')ReportRegistry.record('trace-event-chart',{state:events.length?'empty':'unavailable',reason:events.length?'filtered_out':model.undated?'timestamp_not_observed':'trace_events_not_observed',matchedCount:filtered.length,availableCount:events.length,undatedCount:model.undated});
      if(chart){chart.off('click');chart.on('click',p=>show(events.find(e=>e.evidence===p.data.event.evidence)||{...p.data.event,process:view.lanes[p.data.event.lane].name}));}
      get('event-filter-summary').textContent=`筛选后 ${filtered.length} / ${events.length} 条；超时 ${filtered.filter(e=>e.kind==='timeout').length}，错误 ${filtered.filter(e=>e.kind==='error').length}。组件按日志源文件识别；elapsed 为距同进程上一条保留事件的间隔，不是函数执行时长；筛选不改变时间基准。`;
      if(filtered.length)show(filtered[0]);
    }
    function render(){
      const row=rows.find(r=>r.trace_id===get('event-trace-select').value);currentRow=row;model={lanes:[],undated:0};
      if(chart){chart.dispose();chart=null}
      events=[];get('trace-event-chart').textContent='请选择有证据的 Trace';get('selected-event-evidence').textContent='';get('selected-event-coverage').textContent='未选择 Trace';
      if(row){const evidence=(row.evidence_records?.length?row.evidence_records:row.evidence||[]).map(e=>typeof e==='string'?{text:e}:e);model=TraceVisuals.timeline(evidence);events=model.lanes.flatMap(l=>l.events.map((e,i)=>({...e,process:l.name,component:component(e),elapsed_ms:i?e.time-l.events[i-1].time:null})));}
      get('event-process-filter').innerHTML='<option value="">全部进程（按进程分组）</option>'+[...new Set(events.map(e=>e.process))].map(p=>`<option>${escape(p)}</option>`).join('');get('event-component-filter').innerHTML='<option value="">全部日志组件</option>'+[...new Set(events.map(e=>e.component))].map(c=>`<option>${escape(c)}</option>`).join('');filter();
    }
    function options(){const old=get('event-trace-select').value,q=get('event-trace-search').value.toLowerCase(),matches=rows.filter(r=>r.trace_id.toLowerCase().includes(q));get('event-trace-select').innerHTML=matches.map(r=>`<option value="${escape(r.trace_id)}">${escape(r.trace_id)}</option>`).join('');if(matches.some(r=>r.trace_id===old))get('event-trace-select').value=old;render();}
    get('event-trace-search').oninput=options;get('event-trace-select').onchange=render;get('event-process-filter').onchange=filter;get('event-component-filter').onchange=filter;window.addEventListener('resize',()=>chart?.resize());options();
  }
  if(document.readyState==='loading')document.addEventListener('DOMContentLoaded',install,{once:true});else install();
})();
