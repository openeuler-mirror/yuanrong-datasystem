var TraceEvidenceLogs = (() => {
  function traceLogActor(event){
    return TraceVisuals.actor(event);
  }
  function traceLogErrorCategory(event){
    const text=event.evidence.text||'';
    if(/ERPCTIMEDOUT|ETIMEDOUT|DEADLINE_EXCEEDED|timed out|deadline exceeded|reached timeout|超时|\btimeout\b(?!\s*[:=])/i.test(text))return '超时 / Deadline';
    if(/ECONN|ENETUNREACH|EHOSTUNREACH|connection (?:refused|reset|failed)|connect(?:ion)?[^\n]{0,30}fail|连接失败/i.test(text))return '连接失败';
    const failed=/\|\s*[EF]\s*\||\bfailed\b|\berror\b|cntl_failed\s*=\s*1|(?:cntl|ds)_error_code\s*=\s*-?[1-9]\d*|失败|status\s*[:=]\s*[1-9]\d*/i.test(text);
    if(failed&&/URMA|UB_|urma_/i.test(text))return 'URMA 错误';
    if(failed&&/RPC|QueryAndGet|QueryMeta|RemoteGet/i.test(text))return 'RPC 错误';
    return failed?'其他错误':'未标记错误';
  }

  const esc=value=>String(value??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
  function render(host,evidence,highlight){
    const input=(evidence||[]).map(e=>typeof e==='string'?{text:e}:e);
    const audit=TraceVisuals.uniqueEvidence(input),model=TraceVisuals.timeline(audit.records);
    const events=model.lanes.flatMap(l=>l.events.map(e=>({...e,process:l.name})));
    const keys=new Set(events.map(e=>JSON.stringify([e.evidence.source,e.evidence.member,e.evidence.line,e.evidence.text])));
    events.push(...audit.records.filter(e=>!keys.has(JSON.stringify([e.source,e.member,e.line,e.text]))).map(e=>({evidence:e,component:TraceVisuals.component(e),process:'进程未观测',timestamp:'无时间戳',offset_ms:null,elapsed_ms:null})));
    const rows=events.map(e=>({...e,...traceLogActor(e),errorCategory:traceLogErrorCategory(e)}));
    const labels={role:'角色',instance:'实例',component:'组件',errorCategory:'错误类别'};
    host.classList.add('trace-evidence-view');
    host.innerHTML=`<div class="controls">${Object.entries(labels).map(([key,label])=>`<label>${label} <select data-log-filter="${key}" aria-label="日志${label}"><option value="">全部${label}</option>${[...new Set(rows.map(e=>e[key]))].sort().map(v=>`<option value="${esc(v)}">${esc(v)}</option>`).join('')}</select></label>`).join('')}<label>排列 <select data-log-order aria-label="日志排列"><option value="process">进程内时间顺序</option><option value="elapsed">累计时间升序（各进程归零）</option><option value="gap">相邻间隔降序</option></select></label><button type="button" data-log-reset>重置筛选</button></div><p data-log-count class="caption"></p><p class="caption">全量展示匹配的保留证据；筛选不重算 elapsed。各进程独立归零，跨进程累计排序不代表全局先后。错误类别按日志标记识别，不代表根因。</p><table><thead><tr><th>本地时间 / 角色 / 实例</th><th>进程内累计 / 相邻间隔 (ms)</th><th>组件 / 错误类别</th><th>原始证据</th></tr></thead><tbody></tbody></table>`;
    const filters=[...host.querySelectorAll('[data-log-filter]')],order=host.querySelector('[data-log-order]');
    function update(){
      const matched=rows.filter(e=>filters.every(f=>!f.value||e[f.dataset.logFilter]===f.value));
      if(order.value==='elapsed')matched.sort((a,b)=>(a.offset_ms??Infinity)-(b.offset_ms??Infinity));
      if(order.value==='gap')matched.sort((a,b)=>(b.elapsed_ms??-Infinity)-(a.elapsed_ms??-Infinity));
      const ms=v=>v==null?'未观测':v.toFixed(3);
      host.querySelector('tbody').innerHTML=matched.map(e=>`<tr><td data-label="时间 / 实例">${esc(e.timestamp)}<br><b>${esc(e.instance)}</b></td><td data-label="累计 / 间隔 (ms)">${ms(e.offset_ms)} / ${ms(e.elapsed_ms)}</td><td data-label="组件 / 错误"><b>${esc(e.component)}</b><br>${esc(e.errorCategory)}</td><td data-label="原始证据">${highlight(e.evidence.text||'')}</td></tr>`).join('')||'<tr><td colspan="4">无匹配日志</td></tr>';
      host.querySelector('[data-log-count]').textContent=`显示 ${matched.length} / ${rows.length} 条 · 合并采集副本 ${audit.duplicates} 条 · 索引行 ${audit.indexRows} 条（仅展示去重，聚合统计不变）`;
      host.dataset.logCount=String(rows.length);host.dataset.visibleCount=String(matched.length);
    }
    filters.forEach(f=>f.onchange=update);order.onchange=update;
    host.querySelector('[data-log-reset]').onclick=()=>{filters.forEach(f=>f.value='');order.value='process';update()};update();
  }
  return {render};
})();
