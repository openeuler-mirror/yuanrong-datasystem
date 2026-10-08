#!/usr/bin/env node
'use strict';
const assert = require('node:assert/strict');
const path = require('node:path');
const {pathToFileURL} = require('node:url');
const {chromium} = require(process.env.DS_PLAYWRIGHT_MODULE || 'playwright');

(async () => {
  const file = process.argv[2];
  assert(file, 'usage: check_ds_trace_write_rpc.js write.html');
  const browser = await chromium.launch({headless:true,
    ...(process.env.DS_CHROMIUM_EXECUTABLE ? {executablePath:process.env.DS_CHROMIUM_EXECUTABLE} : {})});
  try {
    const page = await browser.newPage({viewport:{width:1280,height:900}});
    const errors=[];
    page.on('pageerror',error=>errors.push(String(error)));
    await page.goto(pathToFileURL(path.resolve(file)).href);
    await page.waitForFunction(()=>ReportRegistry.audit({requireComplete:true}).valid);
    const inspect=()=>page.evaluate(()=>{
      const option=id=>echarts.getInstanceByDom(document.getElementById(id)).getOption();
      const count=series=>series.flatMap(s=>s.data).reduce((n,x)=>n+Number(x?.value??x??0),0);
      const method=option('write-rpc-method-chart');
      const histogram=option('write-rpc-histogram-chart');
      return {methods:method.yAxis[0].data.length,
        detailed:count(method.series.filter(s=>s.name==='详细调用')),
        windows:count(method.series.filter(s=>s.name==='汇总窗口')),
        histogram:count(histogram.series),
        table:document.querySelector('#write-rpc-method-table .pager')?.textContent,
        audit:ReportRegistry.audit({requireComplete:true,checkLayout:true}).valid};
    });
    const all=await inspect();
    assert(all.audit && all.methods>0 && all.detailed>0 && all.windows>0);
    assert.equal(all.histogram,all.detailed+all.windows);
    await page.locator('#rpc-method-choice').selectOption({index:1});
    const selected=await inspect();
    assert(selected.audit && selected.methods===1 && selected.histogram>0);
    assert(selected.histogram<all.histogram);
    assert.deepEqual(errors,[]);
    await page.close();
  } finally {await browser.close();}
})().catch(error=>{console.error(error);process.exitCode=1;});
