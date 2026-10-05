(() => {
  function install() {
    const workers=document.getElementById('workers');
    if(workers){
      const direct=document.getElementById('direct-worker-chart').closest('.worker-section');
      const urma=document.getElementById('urma-source-chart').closest('.worker-section');
      direct.classList.add('panel');
      direct.querySelector('h3').textContent=ReportRegistry.title('direct-worker-chart');
      urma.querySelector('h3').textContent=ReportRegistry.title('urma-source-chart');
      const scope=document.createElement('p');scope.className='caption';
      scope.textContent='当前读取范围内有处理日志证据的 Worker；不受总览时延分段筛选影响，不代表全部运行负载。';
      direct.prepend(scope);
      document.getElementById('timeline').before(direct);
      document.getElementById('urma-analysis').append(urma);
      for(const [id,label] of [['direct-worker-table','表 1-1 Data Worker 明细'],['urma-source-table','表 4-3 URMA 源 Worker 明细']]){
        const caption=document.createElement('h3');caption.className='table-caption';caption.textContent=ReportRegistry.title(id);
        document.getElementById(id).before(caption);
      }
      const nav=document.getElementById('nav');
      nav.querySelector('a[href="#workers"]')?.remove();
      for(const [id,label] of [['direct-worker-chart','图 1-6 Data Worker 负载与尾延迟'],['direct-worker-table','表 1-1 Data Worker 明细'],['urma-source-chart','图 4-3 URMA 源 Worker 时延'],['urma-source-table','表 4-3 URMA 源 Worker 明细']]){
        const link=document.createElement('a');link.className='sub';link.href='#'+id;link.textContent=ReportRegistry.title(id);nav.append(link);
      }
      workers.remove();
      const legacyTraceSections='#traces,#trace-detail-panel,#trace-log-panel';
      const legacyTitles=[...document.querySelectorAll('#traces > h2,#traces > h3,#trace-detail-panel > h2,#trace-log-panel > h2')];
      for(const link of nav.querySelectorAll('a[href^="#"]')){
        if(document.getElementById(link.hash.slice(1))?.closest(legacyTraceSections))legacyTitles.push(link);
      }
      for(const node of legacyTitles){
        node.textContent=node.textContent.replace(/^8\. Trace/,'7. Trace').replace(/^(表|日志框) 8-/,'$1 7-');
      }
      if(!document.getElementById('trace-event-timeline')){
        for(const node of document.querySelectorAll('#source-logic > h2,#nav a[href="#source-logic"]')){
          node.textContent=node.textContent.replace(/^附录 9\./,'附录 8.');
        }
      }
    }
    const appendix=document.getElementById('source-logic');
    if(appendix)document.querySelector('main').append(appendix);
    for (const nav of document.querySelectorAll('#nav, #write-nav')) {
      const links=[...nav.querySelectorAll('a[href^="#"]')];
      for (const link of links) {
        const target=document.getElementById(link.hash.slice(1));
        if (!target) throw new Error('Missing navigation target: '+link.hash);
        let title;
        if (/^H[1-6]$/.test(target.tagName)||target.tagName==='CAPTION') title=target;
        else if(target.classList.contains('chart')) {
          const next=target.nextElementSibling;
          if((next?.tagName==='FIGCAPTION'||next?.classList.contains('chart-caption'))&&/^图\s*\d/.test(next.textContent.trim()))title=next;
          for(let prev=target.previousElementSibling;prev;prev=prev.previousElementSibling){
            if(title)break;
            if(prev.classList.contains('chart')||prev.tagName==='TABLE')break;
            if(/^H[1-6]$/.test(prev.tagName)&&/^图\s*\d/.test(prev.textContent.trim())){title=prev;break}
          }
          if(!title){title=document.createElement('div');title.textContent=link.textContent;target.after(title)}
          else if(title!==next)target.after(title);
          title.classList.add('chart-caption');
          const chart=window.echarts?.getInstanceByDom(target);
          if(chart)chart.setOption({title:{show:false}});
        } else {
          title=target.querySelector(':scope > h2, :scope > h3, :scope > caption');
          if(!title&&target.previousElementSibling?.matches('h3,.table-caption'))title=target.previousElementSibling;
          if(!title&&target.tagName==='TABLE'){const wrapper=target.parentElement;if(wrapper?.matches('.table-wrap,.worker-table-wrap')&&wrapper.previousElementSibling?.matches('h3,.table-caption'))title=wrapper.previousElementSibling;}
          if(!title&&link.classList.contains('sub')){title=document.createElement('h3');title.className='table-caption';title.textContent=link.textContent;target.before(title)}
        }
        if(title){link.textContent=title.textContent.trim();title.dataset.navigationTarget=target.id}
      }
      const ordered=links.sort((a,b)=>{
        const x=document.getElementById(a.hash.slice(1)),y=document.getElementById(b.hash.slice(1));
        return x===y?0:x.compareDocumentPosition(y)&Node.DOCUMENT_POSITION_FOLLOWING?-1:1;
      });
      for(const link of ordered)link.parentElement.append(link);
    }
  }
  if(document.readyState==='loading')document.addEventListener('DOMContentLoaded',install,{once:true});else install();
})();
