var ReportRegistry = (() => {
  const specs = window.REPORT_COMPONENT_REGISTRY || {components:[],chapters:[]};
  const expected = new Map(specs.components.map(item=>[item.id,item]));
  const states = new Map();
  const sources = new Map();
  const failures = [];
  let epoch = 0;
  function bindSource(name, all, scoped=all) {
    if(typeof all!=='function'||typeof scoped!=='function')throw new Error('Model source requires data getters');
    sources.set(name,{all,scoped});
  }
  function fieldValues(value, path) {
    if(!path.length)return [value];
    const [key,...rest]=path;
    if(value===null||typeof value!=='object')throw new Error('model_field_missing');
    if(key==='*')return Object.values(value).flatMap(item=>fieldValues(item,rest));
    if(!Object.prototype.hasOwnProperty.call(value,key))throw new Error('model_field_missing');
    return fieldValues(value[key],rest);
  }
  function sourceSummary(spec) {
    const contract=spec.data_contract,source=sources.get(contract.source);
    if(!source)throw new Error('model_source_unbound');
    function summarize(model){
      const collection=fieldValues(model,contract.collection.split('.'))[0];
      if(collection===null||typeof collection!=='object')throw new Error('model_collection_invalid');
      const rows=Object.values(collection);
      const values=rows.flatMap(row=>contract.model_fields.flatMap(path=>fieldValues(row,path?path.split('.'):[])));
      const valid=values.filter(value=>contract.value_kind==='number'?typeof value==='number'&&Number.isFinite(value):typeof value==='string'&&value.length>0);
      return {count:rows.length,observed:valid.length};
    }
    const all=summarize(source.all()),scoped=summarize(source.scoped());
    return {sourceCount:all.count,matchedCount:scoped.count,observedCount:scoped.observed};
  }
  function verifySource(spec,state) {
    if(state.state==='error'||state.state==='pending')return state;
    if(!spec.data_contract)return spec.family?{...state,state:'error',reason:'dynamic_source_contract_missing'}:state;
    try{
      const summary=sourceSummary(spec),result={...state,...summary};
      const node=document.getElementById(spec.id),instance=spec.kind==='chart'?window.echarts?.getInstanceByDom(node):null;
      const plotted=instance?values(instance.getOption()):window.echarts?.getInstanceByDom?{sampleCount:0,validCount:0}:state;
      const visible=spec.kind==='table'?(node?.querySelectorAll?visibleTableRows(node).length:state.renderedCount||0):plotted.sampleCount||0;
      if((!summary.sourceCount||!summary.matchedCount)&&visible)return {...result,state:'error',reason:'rendered_data_without_source'};
      if(!summary.observedCount&&(spec.kind==='table'?visible:plotted.validCount))return {...result,state:'error',reason:'rendered_data_without_observation'};
      if(!summary.sourceCount)return {...result,state:'empty',reason:'source_empty'};
      if(!summary.matchedCount)return {...result,state:'empty',reason:'scope_empty'};
      if(!summary.observedCount)return {...result,state:'unavailable',reason:'source_values_unobserved'};
      if(state.state!=='rendered')return {...result,state:'error',reason:'source_data_not_rendered'};
      return result;
    }catch(error){return {...state,state:'error',reason:error.message};}
  }
  function record(id, detail) {
    const prior=states.get(id)||{};
    const state={id,...prior,...detail,revision:detail.revision??prior.expectedRevision??epoch};
    if(!['rendered','empty','unavailable','error'].includes(state.state))Object.assign(state,{state:'error',reason:'invalid_render_state'});
    if(['empty','unavailable'].includes(state.state)&&!state.reason)Object.assign(state,{state:'error',reason:'missing_state_reason'});
    states.set(id,state);
    const node=document.getElementById(id);
    if(node){node.dataset.reportState=state.state;node.dataset.reportReason=state.reason||'';node.dataset.reportRevision=String(state.revision)}
    return state;
  }
  function beginScope(scopeKey, ids) {
    epoch++;
    for(const id of ids){const previous=states.get(id)||{};states.set(id,{...previous,id,state:'pending',reason:'filter_update_pending',scopeKey,expectedRevision:epoch})}
    return epoch;
  }
  function values(option) {
    const series=Array.isArray(option?.series)?option.series:option?.series?[option.series]:[];
    const semantic=series.filter(item=>['graph','sankey','custom','tree','treemap','sunburst'].includes(item.type)).flatMap(item=>item.data||[]);
    const all=series.flatMap(item=>item.data||[]);
    const valid=all.filter(item=>{
      const value=item&&typeof item==='object'&&!Array.isArray(item)?item.value:item;
      const numbers=Array.isArray(value)?value:[value];
      return numbers.some(x=>typeof x==='number'&&Number.isFinite(x));
    });
    return {seriesCount:series.length,sampleCount:all.length,validCount:valid.length+semantic.length};
  }
  function chart(id, option) {
    const count=values(option);
    let state='rendered',reason='numeric_series';
    if(!count.sampleCount){state='empty';reason='no_series_samples'}
    else if(!count.validCount){state='unavailable';reason='no_observed_numeric_values'}
    return record(id,{state,reason,...count});
  }
  function visibleTableRows(node){
    return [...node.querySelectorAll('tbody tr')].filter(row=>!row.hidden&&!row.querySelector('.empty')&&!row.classList.contains('empty')&&row.cells.length>0&&!(row.cells.length===1&&row.cells[0].colSpan>1));
  }
  function table(id) {
    const node=document.getElementById(id);if(!node)return;
    const observed=visibleTableRows(node);
    return record(id,{state:observed.length?'rendered':'empty',reason:observed.length?'table_rows':'no_visible_rows',renderedCount:observed.length});
  }
  function registerFamily(family, entries) {
    if(!specs.dynamic_families.includes(family))throw new Error('Unknown report component family: '+family);
    const components=entries.map(item=>typeof item==='string'?{id:item}:item),ids=components.map(item=>item.id);
    for(const [id,spec] of expected)if(spec.family===family&&!ids.includes(id)){expected.delete(id);states.delete(id)}
    for(const item of components)expected.set(item.id,{...item,kind:'chart',family,required:true,title:item.title||family});
  }
  function bind() {
    for(const item of [...(specs.chapters||[]),...specs.components]){
      const node=document.getElementById(item.id);if(!node)continue;
      const links=[...document.querySelectorAll('a[href^="#"]')].filter(link=>link.getAttribute('href')==='#'+item.id);
      for(const link of links)link.textContent=item.title;
      if(!item.kind){
        const heading=node.querySelector(':scope > h1, :scope > h2, :scope > h3, :scope > h4');
        if(heading)heading.textContent=item.title;
      }else if(item.kind==='chart'){
        let caption=node.nextElementSibling;
        if(!(caption&&(caption.tagName==='FIGCAPTION'||caption.classList.contains('chart-caption')||caption.classList.contains('caption'))&&/^图/.test(caption.textContent.trim()))){
          let prev=node.previousElementSibling;
          while(prev&&!/^H[1-4]$/.test(prev.tagName)&&!prev.classList.contains('chart'))prev=prev.previousElementSibling;
          caption=prev&&/^H[1-4]$/.test(prev.tagName)&&/^图/.test(prev.textContent.trim())?prev:document.createElement('div');
          node.after(caption);
        }
        caption.classList.add('chart-caption');caption.textContent=item.title;caption.dataset.reportCaption=item.id;
      }else if(item.kind==='table'){
        let caption=node.querySelector('caption');
        if(!caption){
          let previous=node.previousElementSibling||node.parentElement?.previousElementSibling;
          const target=node.tagName==='TABLE'?node:null;
          caption=previous&&(/^(H[1-4])$/.test(previous.tagName)||previous.classList.contains('table-caption'))&&/^表/.test(previous.textContent.trim())?previous:target?target.createCaption():document.createElement('div');
          if(!caption.parentNode)node.before(caption);
        }
        caption.classList.add('table-caption');caption.textContent=item.title;caption.dataset.reportCaption=item.id;
        table(item.id);
        new MutationObserver(()=>table(item.id)).observe(node,{subtree:true,childList:true});
      }
    }
  }
  function audit(options={}) {
    const components=[],counts=new Map();
    for(const node of document.querySelectorAll("[id]"))counts.set(node.id,(counts.get(node.id)||0)+1);
    for(const [id,spec] of expected){
      const node=document.getElementById(id);
      let state=states.get(id);
      if(!node)state={id,state:'error',reason:'expected_dom_missing'};
      else if(counts.get(id)>1)state={id,state:'error',reason:'duplicate_dom_id'};
      else if(!state){
        const instance=window.echarts?.getInstanceByDom(node);
        if(instance)state=chart(id,instance.getOption());
        else if(spec.kind==='table')state=table(id);
        else state={id,state:'pending',reason:'render_not_observed'};
      }
      if(state.expectedRevision!==undefined&&state.revision!==state.expectedRevision)state={...state,state:'error',reason:'stale_filter_revision'};
      if(state.state==='rendered'&&spec.kind==='chart'){
        const instance=node&&window.echarts?.getInstanceByDom(node);
        if(!instance)state={...state,state:'error',reason:'rendered_chart_missing_instance'};
        else if(!values(instance.getOption()).validCount)state={...state,state:'error',reason:'rendered_chart_has_no_values'};
      }
      state=verifySource(spec,state);
      const box=node?.getBoundingClientRect();
      if(state.state==='rendered'&&box&&(box.width<=0||box.height<=0)&&options.checkLayout)state={...state,state:'error',reason:'zero_size_component'};
      components.push({...state,kind:spec.kind,title:spec.title});
    }
    return {schema_version:1,page:specs.page_kind,revision:epoch,valid:!failures.length&&components.every(c=>c.state!=='error'&&(!options.requireComplete||c.state!=='pending')),components,errors:[...failures]};
  }
  window.addEventListener('error',event=>failures.push({message:String(event.message||'resource load failed')}));
  window.addEventListener('unhandledrejection',event=>failures.push({message:String(event.reason)}));
  window.addEventListener('load',bind,{once:true});
  document.addEventListener('change',event=>{
    if(event.target?.id?.startsWith('correlation-'))beginScope(event.target.id,[...expected.keys()].filter(id=>id.startsWith('worker-correlation-')));
  },true);
  function title(id){return expected.get(id)?.title||specs.navigation?.find(item=>item.id===id)?.title||specs.chapters?.find(item=>item.id===id)?.title||id}
  return {record,chart,table,beginScope,registerFamily,audit,bind,title,bindSource,dataSummary:values};
})();
