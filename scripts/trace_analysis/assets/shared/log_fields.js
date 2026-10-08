var TraceLogFields = (() => {
  const escape=value=>String(value??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
  const fields=/\b(?:(?:client|worker)\.[\w.]+|(?:client|worker)(?:Summary|_summary)|latencySummary|URMA_ELAPSED_TOTAL|RPC_FRAMEWORK_SLOW|(?:BRPC|ZMQ)_RPC_FRAMEWORK_SLOW|urma_request_id|dataSize|writeChunkIdx|writeChunkCnt|writeChunkIndex|writeChunkCount|cpuid|urma_inflight_wr_cnt|urma_inflight_wr_count|urma_event_map_size|wr_token|outstanding|capacity|waiting|peak|postSrcChipInflight|srcChipInflight|trace_us|firstUrmaWriteWakeSchedLatencyUs|completionObservationLatencyUs|urmaEventProcessingAndWaitLatencyUs|ClientSend|ClientRecv|ServerRecv|ServerSend|(?:[A-Za-z][\w]*_us)|cntl_error_code|ds_error_code|cntl_failed|resp_attachment_bytes|totalCost|costUs|elapsedMs|status|method|QueryAndGet|QueryMeta|RemoteGet|BatchGetObjectRemote|GetObjectRemote|condition wait|urma post to completion cost)\b(?:\s*[:=]\s*(?:\{[^{}\n]*\}|\[[^\]\n]*\]|-?\d+(?:\.\d+)?\s*(?:ms|us)?))?/gi;
  const errors=/\b(?:ERROR|FATAL|failed|timeout|timed out|deadline exceeded|ECONNRESET|ECONNREFUSED|ERPCTIMEDOUT)\b/gi;
  const tokens=new RegExp(fields.source+'|'+errors.source,'gi');
  const errorToken=new RegExp('^(?:'+errors.source+')$','i');
  function render(text){
    let output='',last=0;
    for(const m of String(text).matchAll(tokens)){
      const token=m[0],kind=errorToken.test(token)?'error':/^(client|worker)\.|summary/i.test(token)?'summary':/urma|wr_|chunk|chip|cpuid|dataSize|outstanding|capacity|waiting|peak|trace_us/i.test(token)?'urma':'rpc';
      output+=escape(String(text).slice(last,m.index))+`<span class="log-field log-field-${kind}">${escape(token)}</span>`;last=m.index+token.length;
    }
    return output+escape(String(text).slice(last));
  }
  function decorate(html){
    const template=document.createElement('template');template.innerHTML=html;
    const walker=document.createTreeWalker(template.content,NodeFilter.SHOW_TEXT),nodes=[];
    while(walker.nextNode())if(!walker.currentNode.parentElement?.closest('.log-field,mark'))nodes.push(walker.currentNode);
    for(const node of nodes){const output=render(node.textContent);if(output===escape(node.textContent))continue;const fragment=document.createElement('template');fragment.innerHTML=output;node.replaceWith(fragment.content);}
    return template.innerHTML;
  }
  return {render,decorate};
})();
