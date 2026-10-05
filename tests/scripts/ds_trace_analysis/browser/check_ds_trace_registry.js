/* Run against generated pages: node check_ds_trace_registry.js read.html write.html ... */
const assert=require('assert');
const path=require('path');
const {pathToFileURL}=require('url');
const {chromium}=require(process.env.DS_PLAYWRIGHT_MODULE||'playwright');
(async()=>{
  const browser=await chromium.launch({headless:true,...(process.env.DS_CHROMIUM_EXECUTABLE?{executablePath:process.env.DS_CHROMIUM_EXECUTABLE}:{})});
  const results=[];
  try{
    for(const file of process.argv.slice(2)){
      const page=await browser.newPage();const errors=[];
      page.on('pageerror',error=>errors.push(String(error)));
      await page.goto(pathToFileURL(path.resolve(file)).href);
      for(const width of [1500,1280,900,390]){
        await page.setViewportSize({width,height:1000});
        for(const chart of await page.locator('.chart').all())if(await chart.isVisible())await chart.scrollIntoViewIfNeeded();
        await page.waitForTimeout(150);
        const health=await page.evaluate(()=>ReportRegistry.audit({requireComplete:true,checkLayout:true}));
        assert(health.valid,JSON.stringify({file,width,health}));
        const labels=await page.evaluate(()=>REPORT_COMPONENT_REGISTRY.components.flatMap(spec=>[...document.querySelectorAll('a[href]')].filter(link=>link.getAttribute('href')==='#'+spec.id&&link.textContent.trim()!==spec.title).map(link=>({id:spec.id,actual:link.textContent,expected:spec.title}))));
        assert.strictEqual(labels.length,0,JSON.stringify(labels));
        const misplacedCaptions=await page.evaluate(()=>[...document.querySelectorAll('table > caption')].flatMap(caption=>{
          const cells=[...caption.parentElement.querySelectorAll('th,td')].map(cell=>cell.getBoundingClientRect()).filter(box=>box.height>0);
          return cells.length&&caption.getBoundingClientRect().bottom>Math.min(...cells.map(box=>box.top))+1?[caption.textContent]:[];
        }));
        assert.strictEqual(misplacedCaptions.length,0,JSON.stringify({file,width,misplacedCaptions}));
        assert(!errors.length,errors.join('\n'));
        results.push({file,width,components:health.components.length,valid:true});
      }
      if(await page.locator('#correlation-worker-filter').count()){
        const options=await page.locator('#correlation-worker-filter option').evaluateAll(nodes=>nodes.map(node=>node.value));
        for(const worker of options.slice(0,3)){
          await page.locator('#correlation-worker-filter').selectOption(worker);
          await page.waitForTimeout(50);
          const health=await page.evaluate(()=>ReportRegistry.audit({requireComplete:true}));
          assert(health.valid,JSON.stringify(health));
          const scoped=health.components.filter(item=>item.id.startsWith('worker-correlation-'));
          assert(scoped.length>0&&scoped.every(item=>item.revision===health.revision),'worker charts did not share filter revision');
        }
      }
      if(process.env.DS_DEEP_FILTER_AUDIT==='1'){
        const filterIds=['correlation-worker-filter','urma-source-filter','worker-choice','wr-target','wr-sender'];
        for(const id of filterIds){
          if(!(await page.locator('#'+id).count()))continue;
          await page.goto(pathToFileURL(path.resolve(file)).href);
          for(const chart of await page.locator('.chart').all())if(await chart.isVisible())await chart.scrollIntoViewIfNeeded();
          const options=await page.locator('#'+id+' option').evaluateAll(nodes=>nodes.map(node=>node.value));
          for(const value of options){
            await page.locator('#'+id).selectOption(value);
            await page.waitForTimeout(15);
            const health=await page.evaluate(()=>ReportRegistry.audit({requireComplete:true,checkLayout:true}));
            assert(health.valid,JSON.stringify({file,id,value,health}));
            assert(!errors.length,errors.join('\n'));
          }
          results.push({file,filter:id,options:options.length,valid:true});
        }
      }
      const faults=await page.evaluate(()=>{
        const registry=ReportRegistry,spec=REPORT_COMPONENT_REGISTRY.components.find(c=>c.kind==='chart');
        const node=document.getElementById(spec.id),parent=node.parentNode,next=node.nextSibling;
        node.remove();const missing=registry.audit().components.find(c=>c.id===spec.id);parent.insertBefore(node,next);
        const saved={...registry.audit().components.find(c=>c.id===spec.id)};
        registry.chart(spec.id,{series:[{type:'line',data:[null,null]}]});
        const unavailable=registry.audit().components.find(c=>c.id===spec.id);
        const graph=registry.dataSummary({series:[{type:'graph',data:[{name:'worker-a'}]}]});
        registry.beginScope('fixture-filter',[spec.id]);
        const stale=registry.audit({requireComplete:true});
        registry.record(spec.id,{state:'empty',reason:'fixture_zero_matches',matchedCount:0});
        const refreshed=registry.audit().components.find(c=>c.id===spec.id);
        registry.record(spec.id,saved);
        registry.registerFamily('wr-timeline',['fixture-missing-wr']);
        const family=registry.audit().components.find(c=>c.id==='fixture-missing-wr');
        registry.registerFamily('wr-timeline',[]);
        return {missing,unavailable,graph,stale:stale.valid,refreshed,family,sourceObserved:saved.observedCount>0,sourceEmpty:saved.sourceCount===0||saved.matchedCount===0,sourceUnavailable:saved.observedCount===0};
      });
      assert.strictEqual(faults.missing.reason,'expected_dom_missing');
      assert.strictEqual(faults.unavailable.state,faults.sourceEmpty?'empty':faults.sourceObserved?'error':'unavailable');
      assert.strictEqual(faults.graph.validCount,1);
      assert.strictEqual(faults.stale,false);
      assert.strictEqual(faults.refreshed.state,faults.sourceEmpty?'empty':faults.sourceObserved?'error':faults.sourceUnavailable?'unavailable':'empty');
      assert.strictEqual(faults.refreshed.revision,faults.refreshed.expectedRevision);
      assert.strictEqual(faults.family.reason,'expected_dom_missing');
      await page.close();
    }
    console.log(JSON.stringify({valid:true,checks:results},null,2));
  }finally{await browser.close()}
})().catch(error=>{console.error(error);process.exit(1)});
