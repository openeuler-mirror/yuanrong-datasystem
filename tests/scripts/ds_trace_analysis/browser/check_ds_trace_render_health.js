/* Usage: node check_ds_trace_render_health.js read.html [write.html] */
const assert = require('assert');
const {pathToFileURL} = require('url');
const path = require('path');
const {chromium} = require(process.env.DS_PLAYWRIGHT_MODULE || 'playwright');
(async () => {
  assert(process.argv.length > 2, 'provide generated report paths');
  const browser = await chromium.launch({headless:true, ...(process.env.DS_CHROMIUM_EXECUTABLE ? {executablePath:process.env.DS_CHROMIUM_EXECUTABLE} : {})});
  try {
    for (const file of process.argv.slice(2)) {
      const page = await browser.newPage({viewport:{width:1500,height:1000}});
      const errors = [];
      page.on('pageerror', error => errors.push(String(error)));
      await page.goto(pathToFileURL(path.resolve(file)).href);
      await page.waitForFunction(() => window.ReportDiagnostics);
      assert.strictEqual(await page.locator('#urma-trace-table, a[href="#urma-trace-table"]').count(),0);
      const captions=await page.locator('main h3').allTextContents();
      const numbered=captions.map(s=>s.trim()).filter(s=>/^(图|表)\s*\d/.test(s));
      assert.strictEqual(new Set(numbered).size,numbered.length,'Duplicate numbered headings');
      for (const width of [1500,900,390]) {
        await page.setViewportSize({width,height:1000});
        for (const chart of await page.locator('.chart').all()) await chart.scrollIntoViewIfNeeded();
        if(await page.locator('#read-selected-stage-chart').count()){
          const bounds=await page.evaluate(()=>{
            const chart=echarts.getInstanceByDom(document.getElementById('read-selected-stage-chart'));
            const view=chart.getViewOfComponentModel(chart.getModel().getComponent('legend'));
            const rect=view.group.getBoundingRect().clone();rect.applyTransform(view.group.transform);
            return {right:rect.x+rect.width,bottom:rect.y+rect.height,width:chart.getWidth(),top:chart.getOption().grid[0].top};
          });
          assert(bounds.right<=bounds.width+1,'selected Trace legend clipped');
          assert(bounds.bottom<bounds.top,'selected Trace legend overlaps plot');
        }

        const logs=page.locator('.trace-evidence-view');
        assert.strictEqual(await logs.count(),1);
        const total=Number(await logs.getAttribute('data-log-count'));
        assert(total>0);
        assert.strictEqual(await logs.locator('tbody tr').count(),total);
        assert.strictEqual(await logs.locator('details').count(),0);
        const role=logs.locator('[data-log-filter="role"]');
        const choice=await role.locator('option').nth(1).getAttribute('value');
        await role.selectOption(choice);
        assert.strictEqual(await logs.locator('tbody tr').count(),Number(await logs.getAttribute('data-visible-count')));
        await logs.locator('[data-log-order]').selectOption('elapsed');
        const values=(await logs.locator('tbody td:nth-child(2)').allTextContents()).map(parseFloat).filter(Number.isFinite);
        assert(values.every((v,i)=>!i||v>=values[i-1]));
        await logs.locator('[data-log-reset]').click();
        assert.strictEqual(await logs.locator('tbody tr').count(),total);
        const health = await page.evaluate(() => ReportDiagnostics.audit());
        assert(health.valid, JSON.stringify(health));
        assert(health.charts.length, 'no chart containers');
        assert.deepStrictEqual(errors, []);
      }
      if (await page.locator('#query-worker-breakdown-chart').count()) {
        const transition = await page.evaluate(() => {
          const saved = scopeRows;
          const state = () => ReportDiagnostics.audit().charts.find(c => c.id === 'query-worker-breakdown-chart').state;
          const before = state();
          let empty;
          try {scopeRows = () => []; renderQueryBreakdown(); empty = state()}
          finally {scopeRows = saved; renderQueryBreakdown()}
          return [before,empty,state()];
        });
        assert.strictEqual(transition[1], 'empty');
        assert.strictEqual(transition[2], transition[0], 'chart must recover after an empty selection');
      }
      const wrCharts = await page.evaluate(() => ['urma-time-chart','urma-worker-chart','worker-correlation-chart-ub','chip-load-chart'].flatMap(id => {
        const node=document.getElementById(id), chart=node&&echarts.getInstanceByDom(node);
        if(!chart)return [];
        const o=chart.getOption();
        return [{id,series:o.series.map(s=>({name:s.name,type:s.type,axis:s.yAxisIndex||0})),axes:o.yAxis}];
      }));
      for(const chart of wrCharts){
        for(const series of chart.series){
          if(/WR/.test(series.name)){
            assert.strictEqual(series.type,'bar',`${chart.id}: ${series.name} must be bars`);
            assert.strictEqual(series.axis,0,`${chart.id}: WR count axis`);
          }else if(/total|wait|Client ms/.test(series.name)){
            assert.strictEqual(series.type,'line',`${chart.id}: latency must be lines`);
            assert.strictEqual(series.axis,1,`${chart.id}: latency axis`);
          }
        }
      }
      const navigation=await page.evaluate(()=>[...document.querySelectorAll('#nav a[href^="#"],#write-nav a[href^="#"]')].map(a=>{const target=document.getElementById(a.hash.slice(1));const caption=document.querySelector('[data-navigation-target="'+target?.id+'"]');return {href:a.hash,found:!!target,matches:!!caption&&a.textContent.trim()===caption.textContent.trim()}}));
      assert(navigation.length>0);
      assert(navigation.every(x=>x.found&&x.matches),JSON.stringify(navigation.filter(x=>!x.found||!x.matches)));
      await page.setViewportSize({width:1500,height:1000});
      for(const item of navigation.filter(x=>/urma-worker-chart|worker-table|wr-events-table/.test(x.href))){
        await page.locator(`#nav a[href="${item.href}"],#write-nav a[href="${item.href}"]`).evaluate(a=>{if(a.hidden)document.querySelector('[data-nav-toggle="'+a.dataset.navGroup+'"]').click()});
        await page.locator(`#nav a[href="${item.href}"],#write-nav a[href="${item.href}"]`).click();
        await page.waitForFunction(hash=>{const top=document.querySelector(hash).getBoundingClientRect().top;return top>=-2&&top<innerHeight},item.href,{timeout:10000});
        assert.strictEqual(new URL(page.url()).hash,item.href);
        const top=await page.locator(item.href).evaluate(e=>e.getBoundingClientRect().top);
        assert(top>=-2&&top<1000,`${item.href} did not reach viewport: ${top}`);
      }
      if(await page.locator('#worker-correlation-summary').count()){
        assert.strictEqual(await page.locator('#worker-correlation-summary .correlation-kpi').count(),6);
        assert((await page.locator('#query-meta-analysis > h2').innerText()).startsWith('5.'));
        assert((await page.locator('#worker-correlation > h2').innerText()).startsWith('6.'));
        for(const width of [1500,390]){
          await page.setViewportSize({width,height:1000});
          await page.locator('#worker-correlation-summary').scrollIntoViewIfNeeded();
          const overlap=await page.locator('#worker-correlation-summary').evaluate(e=>{const cards=[...e.querySelectorAll('.correlation-kpi')].map(c=>c.getBoundingClientRect());return cards.some((a,i)=>cards.slice(i+1).some(b=>a.left<b.right&&b.left<a.right&&a.top<b.bottom&&b.top<a.bottom))});
          assert(!overlap,'summary cards overlap');
        }
      }
      const chartFontSizes=await page.evaluate(()=>[...document.querySelectorAll('.chart')].flatMap(node=>{
        const option=echarts.getInstanceByDom(node)?.getOption();
        if(!option)return [];
        return ['xAxis','yAxis','legend','tooltip','series','dataZoom'].flatMap(key=>
          (Array.isArray(option[key])?option[key]:option[key]?[option[key]]:[]).flatMap(item=>
            ['textStyle','axisLabel','nameTextStyle','label','endLabel'].filter(style=>item[style]).map(style=>
              ({id:node.id,style:key+'.'+style,size:item[style].fontSize}))));
      }));
      assert(chartFontSizes.length>0,'no chart typography checked');
      assert.deepStrictEqual(chartFontSizes.filter(item=>item.size!==12),[],'chart text must use the shared 12px size');
      if(await page.locator('#read-selected-analysis').count()){
        assert.strictEqual(await page.locator('#read-selected-analysis > h2').innerText(),'选中 Trace · 阶段与判断');
        assert.strictEqual(await page.locator('#nav a[href="#read-selected-analysis"]').count(),0);
        const scatterAudit=await page.evaluate(()=>{
          const chart=echarts.getInstanceByDom(document.getElementById('timeline-chart')),rows=scopeRows(),original=selectedId;
          const markers=chart.getModel().getSeries().filter(s=>s.subType==='scatter');
          const issues=[];let clicked=0;
          for(const series of markers){
            const data=series.getData();
            for(let index=0;index<rows.length;index++){
              const expected=series.name==='URMA超时标记'?rows[index].urma_timeout_observed:rows[index].failed&&!rows[index].urma_timeout_observed;
              if(data.hasValue(index)!==Boolean(expected))issues.push('visibility '+index);
              if(expected){
                const values=data.getValues(data.dimensions,index);
                if(values[0]!==index)issues.push('coordinate '+index);
                if(!clicked){chart.trigger('click',{seriesType:'scatter',seriesIndex:series.seriesIndex,dataIndex:index,value:values});if(selectedId!==rows[index].trace_id)issues.push('selection '+index);clicked++;}
              }
            }
          }
          selectedId=original;renderTable();renderDetail();return {issues,clicked};
        });
        assert.deepStrictEqual(scatterAudit.issues,[],'failure markers lost or point at another Trace');
        assert(scatterAudit.clicked>0,'fixture needs a failure marker');
        const result=await page.evaluate(()=>{
          const original=selectedId,row=ROWS.find(r=>r.urma_requests?.length);selectedId=row.trace_id;renderDetail();
          const option=echarts.getInstanceByDom(document.getElementById('read-selected-stage-chart')).getOption();
          const value=d=>d&&typeof d==='object'?d.value:d;
          const actual=option.series.find(s=>s.name==='WR Elapsed').data.map(value).filter(v=>v!=null);
          const stages=option.series.filter(s=>s.stack==='client-stages').reduce((sum,s)=>sum+Number(value(s.data[0])),0);
          const expectedStages=Object.values(row.focus_breakdown_ms).filter(v=>Number.isFinite(v)&&v>0).reduce((a,b)=>a+b,0);
          const opened=[...document.querySelectorAll('#trace-detail details')].find(d=>d.querySelector('summary')?.textContent==='阶段数值明细')?.open;
          const expected=row.urma_requests.map(r=>r.total_ms);selectedId=original;renderDetail();return {actual,expected,stages,expectedStages,opened};
        });
        assert.deepStrictEqual(result.actual,result.expected);
        assert(Math.abs(result.stages-result.expectedStages)<1e-6);
        assert(result.opened);
        assert.strictEqual(await page.locator('#read-selected-wr-chart,#read-selected-thread-chart,#read-wr-timelines').count(),0);
      }
      assert.strictEqual(await page.locator('#trace-event-timeline').count(),1);
      assert(await page.locator('#event-trace-select option').count()>0);
      assert(await page.locator('#event-table-body tr[data-event]').count()<=20);
      if(await page.locator('#event-table-body tr[data-event]').count()){
        await page.locator('#event-table-body tr[data-event]').first().click();
        assert((await page.locator('#selected-event-evidence').innerText()).length>0);
      }
      await page.evaluate(()=>{
        const rows=typeof ROWS!=='undefined'?ROWS:ALL;
        rows.push({trace_id:'timeline-pagination-fixture',evidence:Array.from({length:21},(_,i)=>
          `${i===20?'/logs/worker01/kvcache.INFO.log':'/logs/client/ds_client.INFO.log'}:1:2026-09-22T18:00:00.${String(i).padStart(3,'0')} | I | ${i===20?'urma_manager':'object_posix'}.cpp:1 | 127.0.0.1 | 80:1 | fixture | ${i===20?'URMA_ELAPSED_TOTAL':'RPC timeout'}`)});
      });
      await page.locator('#event-trace-search').fill('timeline-pagination-fixture');
      assert.strictEqual(await page.locator('#event-table-body tr[data-event]').count(),4);
      assert(await page.locator('#event-table-pager').isVisible());
      await page.locator('#event-component-filter').selectOption({label:'Client'});
      assert.strictEqual(await page.locator('#event-table-body tr[data-event]').count(),20);
      assert(!(await page.locator('#event-table-pager').isVisible()));
      assert(await page.locator('#event-table-body mark').count()>0);
      await page.locator('#event-component-filter').selectOption({label:'Worker'});
      assert.strictEqual(await page.locator('#event-table-body tr[data-event]').count(),1);
      assert((await page.locator('#event-table-body').innerText()).includes('+20.000 ms'));
      assert((await page.locator('#event-table-body').innerText()).includes('（1.000 ms）'));
      assert((await page.locator('#event-table-body').innerText()).includes('URMA'));
      const offsets=await page.evaluate(()=>echarts.getInstanceByDom(document.getElementById('trace-event-chart')).getOption().series.flatMap(s=>s.data.map(d=>d.value[0])));
      assert.deepStrictEqual(offsets,[20]);
      await page.evaluate(()=>{const rows=typeof ROWS!=='undefined'?ROWS:ALL;rows.splice(rows.findIndex(r=>r.trace_id==='timeline-pagination-fixture'),1)});
      await page.locator('#event-trace-search').fill('no-such-trace-fixture');
      assert.strictEqual(await page.locator('#event-table-body tr[data-event]').count(),0);
      await page.locator('#event-trace-search').fill('');
      assert(await page.locator('#event-trace-select option').count()>0);
      const fieldHighlight=await page.evaluate(()=>{
        const text='URMA_ELAPSED_TOTAL urma_request_id:2581 wr_token:{outstanding:4,capacity:50} srcChipInflight:{1:11} e2e_us=2635 cntl_error_code=0 latencySummary client.rpc.direct_query_and_get:20000 worker.process.get:21000 <img src=x onerror=alert(1)>';
        const div=document.createElement('div');div.innerHTML=TraceLogFields.decorate(TraceLogFields.render(text));
        return {text:div.textContent,expected:text,urma:div.querySelectorAll('.log-field-urma').length,rpc:div.querySelectorAll('.log-field-rpc').length,summary:div.querySelectorAll('.log-field-summary').length,unsafe:div.querySelectorAll('img,script').length,nested:div.querySelectorAll('.log-field .log-field').length};
      });
      assert.strictEqual(fieldHighlight.text,fieldHighlight.expected);
      assert(fieldHighlight.urma>=3&&fieldHighlight.rpc>=2&&fieldHighlight.summary>=3);
      assert.strictEqual(fieldHighlight.unsafe,0);assert.strictEqual(fieldHighlight.nested,0);
      const wrongFonts=await page.evaluate(()=>[...document.querySelectorAll('body *')].filter(e=>e.textContent.trim()&&!['SCRIPT','STYLE'].includes(e.tagName)&&!getComputedStyle(e).fontFamily.startsWith('"Microsoft YaHei"')).map(e=>({tag:e.tagName,font:getComputedStyle(e).fontFamily})).slice(0,5));
      assert.deepStrictEqual(wrongFonts,[], 'all report elements must use Microsoft YaHei');
      const missingLogsText = await page.locator('body').innerText();
      assert(missingLogsText.includes('Worker 日志覆盖'), 'missing evidence diagnosis is not visible');
      const health = await page.evaluate(() => ReportDiagnostics.audit());
      const cleared = await page.evaluate(() => {
        const node=document.querySelector('.chart');
        const chart=echarts.getInstanceByDom(node),original=chart.getOption();
        chart.setOption({series:[]},true);
        const missing=ReportDiagnostics.audit().charts.find(c=>c.id===node.id);
        chart.setOption(original,true);
        const restored=ReportDiagnostics.audit().charts.find(c=>c.id===node.id);
        return {missing,restored};
      });
      assert.strictEqual(cleared.missing.state,'error','source-backed chart loss must be reported');
      assert.strictEqual(cleared.missing.reason,'source_data_not_rendered');
      assert.strictEqual(cleared.restored.state,'rendered','restored chart must recover');
      const unregistered = await page.evaluate(() => {
        const before=ReportDiagnostics.audit().charts.length;
        const node = document.createElement('div');node.id='selection-fixture';node.className='chart';
        document.body.append(node);node.textContent='请选择 Worker';
        const after=ReportDiagnostics.audit().charts.length;
        node.remove();return [before,after];
      });
      assert.strictEqual(unregistered[1],unregistered[0],
        'unregistered DOM must not be counted as a report component');
      const failed = await page.evaluate(() => {
        ReportDiagnostics.run('fault-injection', () => {throw new Error('missing renderer fixture')});
        return {health:ReportDiagnostics.audit(), text:document.querySelector('#report-render-failures').textContent};
      });
      assert(!failed.health.valid && failed.text.includes('missing renderer fixture'));
      console.log(JSON.stringify({file,charts:health.charts.length,empty:health.charts.filter(c=>c.state==='empty').length,errors,widths:[1500,900,390],faultInjection:'passed'}));
      await page.close();
    }
  } finally {await browser.close()}
})().catch(error=>{console.error(error);process.exit(1)});
