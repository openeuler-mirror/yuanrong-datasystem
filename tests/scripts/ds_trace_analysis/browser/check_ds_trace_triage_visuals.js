const assert=require('assert');
const {pathToFileURL}=require('url');
const path=require('path');
const {chromium}=require(process.env.DS_PLAYWRIGHT_MODULE||'playwright');
(async()=>{
  const browser=await chromium.launch({headless:true,...(process.env.DS_CHROMIUM_EXECUTABLE?{executablePath:process.env.DS_CHROMIUM_EXECUTABLE}:{})});
  try{
    const page=await browser.newPage(),errors=[];
    page.on('pageerror',error=>errors.push(String(error)));
    await page.goto(pathToFileURL(path.resolve(process.argv[2])).href);
    const duplicateIds=await page.evaluate(()=>{const ids=[...document.querySelectorAll('[id]')].map(node=>node.id);return ids.filter((id,index)=>ids.indexOf(id)!==index)});
    assert.deepStrictEqual(duplicateIds,[]);
    assert.strictEqual(await page.locator('#chapter-guide-list .summary-points').count(),0);
    assert.strictEqual(await page.locator('#chapter-guide-list > li').count(),6);
    assert.strictEqual(await page.locator('.flow-graph-chart, a[href="#read-flow-stage-chart"], a[href="#write-flow-stage-chart"]').count(),0);
    for(const kind of ['read','write']){
      await page.locator('#'+kind+'-flow-stage-table').scrollIntoViewIfNeeded();
      assert(await page.locator('#'+kind+'-flow-stage-table tr').count()>1);
    }
    for(const width of [1500,390]){
      await page.setViewportSize({width,height:1000});
      await page.locator('#ub-request-table').scrollIntoViewIfNeeded();
      await page.waitForFunction(()=>document.querySelector('#ub-request-table th'));
      const ub=await page.evaluate(()=>({
        headers:[...document.querySelectorAll('#ub-request-table th')].map(h=>h.childNodes[0].textContent),
        rows:[...document.querySelectorAll('#ub-request-table tbody tr')].map(r=>({
          cells:[...r.querySelectorAll('td')].map(c=>({label:c.dataset.label,text:c.textContent})),
          severity:r.className
        })),
        expected:ubRequestRows.slice(0,4).map(item=>[item.trace_id||'',`${String(item.request_id??'')||'未观测'}\nChunk ${item.write_chunk_index??'?'}/${item.write_chunk_count??'?'}`,item.worker||'未观测',String(item.total_ms??''),String(item.wait_os_sched_ms??''),item.status||'未观测']),
        overflow:document.querySelector('#ub-request-table').scrollWidth>document.querySelector('#ub-request-table').clientWidth+2
      }));
      assert.strictEqual(ub.headers.length,6);
      if(ub.expected.length){
        assert.deepStrictEqual(ub.rows.map(r=>r.cells.map(c=>c.text)),ub.expected);
        assert(ub.rows.every(r=>r.cells.every((c,i)=>c.label===ub.headers[i])));
        assert(ub.rows.length<=4);
        assert(!ub.overflow,'UB request table overflows');
      }
      await page.locator('#selected-event-timeline').scrollIntoViewIfNeeded();
      await page.waitForFunction(()=>window.echarts?.getInstanceByDom(document.getElementById('selected-event-timeline')));
      const count=await page.evaluate(()=>echarts.getInstanceByDom(document.getElementById('selected-event-timeline')).getOption().series.reduce((n,s)=>n+s.data.length,0));
      assert(count>0);
      const clickPoint=await page.evaluate(()=>{
        const node=document.getElementById('selected-event-timeline');
        const chart=echarts.getInstanceByDom(node);
        const point=chart.getOption().series.flatMap(series=>series.data)
          .find(p=>/URMA_ELAPSED_TOTAL|RPC_FRAMEWORK_SLOW|timeout|failed/i.test(p.event.evidence.text));
        if(!point)throw Error('Timeline fixture requires a highlighted event');
        const pixel=chart.convertToPixel({xAxisIndex:0,yAxisIndex:0},point.value),box=node.getBoundingClientRect();
        return {x:box.left+pixel[0],y:box.top+pixel[1]};
      });
      await page.mouse.click(clickPoint.x,clickPoint.y);
      assert(await page.locator('#selected-event-evidence .log-field').count()>0,'Clicked evidence must be highlighted');
      if(width===1500){
        await page.setViewportSize({width:390,height:1000});
        await page.waitForFunction(()=>document.documentElement.scrollWidth<=innerWidth+2);
        assert(await page.locator('#selected-event-evidence .log-field').count()>0,'Resize must retain selected evidence');
        await page.setViewportSize({width,height:1000});
      }

      assert.strictEqual(await page.locator('#trace-log-timeline-table').count(),1);
      const allRows=await page.evaluate(()=>window.traceLogTimelineRows);
      assert(allRows.length>20,'Fixture must exercise unpaginated full logs');
      assert.strictEqual(await page.locator('#trace-log-timeline-table tbody tr').count(),allRows.length);
      assert.strictEqual(await page.locator('#trace-log-timeline-pager').count(),0);
      const workers=allRows.filter(e=>e.role==='Worker');
      assert(allRows.some(e=>e.role==='Client'));
      assert(workers.length>0);
      await page.selectOption('#trace-log-role','Worker');
      assert.strictEqual(await page.locator('#trace-log-timeline-table tbody tr').count(),workers.length);
      await page.selectOption('#trace-log-instance',workers[0].instance);
      assert.strictEqual(await page.locator('#trace-log-timeline-table tbody tr').count(),workers.filter(e=>e.instance===workers[0].instance).length);
      await page.locator('#trace-log-reset').click();
      await page.selectOption('#trace-log-order','elapsed');
      const ordered=await page.locator('#trace-log-timeline-table tbody tr td:nth-child(2)').allTextContents();
      const values=ordered.map(text=>parseFloat(text)).filter(Number.isFinite);
      assert(values.every((value,index)=>!index||value>=values[index-1]));
      await page.locator('#trace-log-reset').click();
      const component=allRows[0].component;
      const category=allRows[0].errorCategory;
      const originalElapsed=await page.locator('#trace-log-timeline-table tbody tr').first().locator('td').nth(1).innerText();
      await page.selectOption('#trace-log-component',component);
      assert.strictEqual(await page.locator('#trace-log-timeline-table tbody tr').count(),allRows.filter(e=>e.component===component).length);
      await page.selectOption('#trace-log-error',category);
      assert.strictEqual(await page.locator('#trace-log-timeline-table tbody tr').count(),allRows.filter(e=>e.component===component&&e.errorCategory===category).length);
      assert.strictEqual(await page.locator('#trace-log-timeline-table tbody tr').first().locator('td').nth(1).innerText(),originalElapsed);
      await page.locator('#trace-log-reset').click();
      assert.strictEqual(await page.locator('#trace-log-timeline-table tbody tr').count(),allRows.length);
      assert((await page.locator('#selected-trace-log').innerText()).includes('合并采集副本'));
      assert((await page.locator('#trace-log-timeline-table thead').innerText()).includes('相邻间隔'));
      const tooltip=await page.evaluate(()=>{
        const option=echarts.getInstanceByDom(document.getElementById('selected-event-timeline')).getOption();
        const point=option.series.flatMap(series=>series.data)[0];
        return option.tooltip[0].formatter({data:point});
      });
      assert(tooltip.includes('组件：')&&tooltip.includes('进程内累计')&&tooltip.includes('elapsed'));
      const evidence=await page.locator('#selected-event-evidence').innerText();
      assert(evidence.includes('组件：')&&evidence.includes('elapsed（相邻日志）'));
      assert((await page.locator('#selected-event-coverage').innerText()).includes('每行独立归零'));
    }
    const actors=await page.evaluate(()=>[
      {text:'/logs/client/worker12/ds_client.INFO:1:2026-09-22T18:00:00 | RPC Worker failed',process:'192.0.2.1 / PID 80'},
      {text:'/logs/worker/worker06/kvcache.INFO:2:2026-09-22T18:00:00 | client connected',process:'192.0.2.2 / PID 9'},
      {text:'/logs/worker/worker07/kvcache.INFO:3:2026-09-22T18:00:00 | client connected',process:'192.0.2.3 / PID 9'},
      {text:'/logs/coordinator/coordinator.INFO:4:2026-09-22T18:00:00 | URMA RPC',process:'192.0.2.5 / PID 10'},
      {text:'Worker RPC client connected',process:'192.0.2.4 / PID 9'}
    ].map(e=>traceLogActor({evidence:{text:e.text},process:e.process})));
    assert.deepStrictEqual(actors.map(e=>e.role),['Client','Worker','Worker','Coordinator','组件未观测']);
    assert.notStrictEqual(actors[1].instance,actors[2].instance);
    const categories=await page.evaluate(()=>['RPC deadline exceeded','connection refused','| E | URMA failed','| E | QueryMeta failed','| E | unexpected state','| I | done','cntl_timeout_ms=20 cntl_deadline_us=123'].map(text=>traceLogErrorCategory({evidence:{text}})));
    assert.deepStrictEqual(categories,['超时 / Deadline','连接失败','URMA 错误','RPC 错误','其他错误','未标记错误','未标记错误']);
    assert.deepStrictEqual(errors,[]);
    console.log('SVG removal, retained evidence tables and process timeline passed at 1500/390px');
  }finally{await browser.close()}
})().catch(error=>{console.error(error);process.exit(1)});
