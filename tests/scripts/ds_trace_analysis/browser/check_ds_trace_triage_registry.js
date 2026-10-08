/* Browser contract regression: pass a generated triage HTML containing read/worker evidence. */
const assert = require('assert');
const path = require('path');
const {pathToFileURL} = require('url');
const {chromium} = require(process.env.DS_PLAYWRIGHT_MODULE || 'playwright');
(async () => {
  const browser = await chromium.launch({headless:true,
    ...(process.env.DS_CHROMIUM_EXECUTABLE ? {executablePath:process.env.DS_CHROMIUM_EXECUTABLE} : {})});
  try {
    const page = await browser.newPage();
    const errors = [];
    page.on('pageerror', error => errors.push(String(error)));
    await page.goto(pathToFileURL(path.resolve(process.argv[2])).href);
    for (const width of [1500,1280,900,390]) {
      await page.setViewportSize({width,height:1000});
      for (const node of await page.locator('.chart').all()) {
        if (await node.isVisible()) await node.scrollIntoViewIfNeeded();
      }
      await page.waitForTimeout(150);
      const health = await page.evaluate(() => ReportRegistry.audit({requireComplete:true}));
      assert(health.valid, JSON.stringify({width,health}));
      assert.equal(health.components.length,47);
    }
    const cases = await page.evaluate(async () => {
      const state = id => ReportRegistry.audit().components.find(item => item.id === id);
      const filter = document.getElementById('read-worker-filter');
      filter.add(new Option('fixture absent worker','__registry_absent__'));
      filter.value = '__registry_absent__';
      document.getElementById('read-worker-chart').scrollIntoView();
      renderWorkerSection('read','Read');
      await new Promise(resolve=>setTimeout(resolve,60));
      const scoped = state('read-worker-chart');
      filter.value = '';renderWorkerSection('read','Read');
      document.getElementById('read-flow-chart').scrollIntoView();
      await new Promise(resolve=>setTimeout(resolve,60));
      ReportRegistry.chart('read-flow-chart',{series:[]});
      const omitted = state('read-flow-chart');
      renderFlowSection('read','Read');
      const first = selectedTraceId;
      const selected = [];
      for (const [id] of traceRows.slice(0,3)) {
        document.getElementById('selected-trace-chart').scrollIntoView();
        selectedTraceId=id;renderSelectedTrace();
        await new Promise(resolve=>setTimeout(resolve,60));
        selected.push({id,state:state('selected-trace-chart'),
          expected:(traces[id].stage_breakdown || []).filter(s=>s.duration_ms!==undefined).length});
      }
      selectedTraceId=null;renderSelectedTrace();
      await new Promise(resolve=>setTimeout(resolve,60));
      const empty = state('selected-trace-table');
      selectedTraceId=first;renderSelectedTrace();
      const contract = REPORT_COMPONENT_REGISTRY.components.find(s=>s.id==='read-flow-chart').data_contract;
      ReportRegistry.bindSource(contract.source,()=>({rows:[1]}),()=>({rows:[]}));
      const stale = state('read-flow-chart');
      ReportRegistry.bindSource(contract.source,()=>({rows:flowRowsForOperation('read').map(([,value])=>value)}));
      ReportRegistry.bindSource(contract.source,()=>({rows:[{}]}));
      const missing = state('read-flow-chart');
      const stageContract=REPORT_COMPONENT_REGISTRY.components.find(s=>s.id==='selected-trace-chart').data_contract;
      ReportRegistry.bindSource(stageContract.source,()=>({rows:[{duration_ms:null}]}));
      const plot=echarts.getInstanceByDom(document.getElementById('selected-trace-chart'));
      plot.setOption({xAxis:{type:'value'},yAxis:{type:'category',data:['sample']},
        series:[{type:'bar',data:[null]}]},true);
      const unavailable=state('selected-trace-chart');
      ReportRegistry.bindSource(stageContract.source,()=>({rows:[{}]}));
      const missingField=state('selected-trace-chart');
      return {scoped,omitted,selected,empty,stale,missing,unavailable,missingField};
    });
    assert.equal(cases.scoped.state,'empty',JSON.stringify(cases));
    assert.equal(cases.scoped.reason,'scope_empty');
    assert.equal(cases.omitted.state,'error');
    assert.equal(cases.omitted.reason,'source_data_not_rendered');
    assert.equal(cases.empty.state,'empty');
    assert.equal(cases.stale.state,'error');
    assert.equal(cases.missing.state,'error');
    assert.equal(cases.unavailable.state,'unavailable');
    assert.equal(cases.missingField.state,'error');
    assert.equal(cases.missingField.reason,'model_field_missing');
    for (const selected of cases.selected) {
      assert.equal(selected.state.sourceCount,selected.expected);
      assert.notEqual(selected.state.state,'error');
      assert.equal(selected.state.revision,selected.state.expectedRevision);
    }
    assert.deepEqual(errors,[]);
    console.log(JSON.stringify({valid:true,widths:[1500,1280,900,390],components:47,cases},null,2));
  } finally {await browser.close();}
})().catch(error=>{console.error(error);process.exit(1);});
