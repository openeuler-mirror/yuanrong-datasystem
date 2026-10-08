ReportDiagnostics.run('correlation-controls', () => {
  (AGG.worker_correlation?.workers || []).forEach(item => $('correlation-worker-filter').insertAdjacentHTML('beforeend', `<option value="${esc(item.worker)}">${item.roles?.includes('client')?'Client · ':item.roles?.some(r=>['worker_handler','urma_source_worker'].includes(r))?'Worker · ':'RPC调用端 · '}${esc(shortWorker(item.worker))} · ${item.event_count}事件</option>`));
  const selects = ['correlation-worker-filter','correlation-category-filter','correlation-status-filter','correlation-relation-filter','correlation-latency-band-filter'];
  const times = ['correlation-time-start','correlation-time-end'];
  selects.forEach(id => $(id).onchange = () => {correlationPage=1;renderWorkerCorrelation()});
  times.forEach(id => $(id).oninput = () => {correlationPage=1;renderWorkerCorrelation()});
  $('correlation-reset-filter').onclick = () => {
    [...selects,...times].forEach(id => $(id).value='');
    $('correlation-status-filter').value='all';correlationPage=1;renderWorkerCorrelation();
  };
});
[
  ['kpis', () => renderKpis()],
  ['timeSegments', () => renderTimeSegments()],
  ['problemOverview', () => renderProblemOverview()],
  ['errorAnalysis', () => renderErrorAnalysis()],
  ['stageShare', () => renderStageShare()],
  ['problemGuidance', () => renderProblemGuidance()],
  ['timeFindings', () => renderTimeFindings()],
  ['rpc', () => renderRpcAnalysis()],
  ['urma', () => renderUrmaAnalysis()],
  ['queryMeta', () => renderQueryMetaAnalysis()],
  ['workerCorrelation', () => renderWorkerCorrelation()],
  ['workers', () => renderWorkers()],
  ['timeline', () => renderTimeline()]
].forEach(([name, render]) => ReportDiagnostics.run(name, render));
document.querySelectorAll('h2,h3').forEach(title => {
  if (/^图\s*\d/.test(title.textContent.trim())) title.classList.add('chart-title');
});
