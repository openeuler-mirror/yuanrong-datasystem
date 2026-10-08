#!/usr/bin/env node
const assert = require('node:assert/strict');
const path = require('node:path');
const {pathToFileURL} = require('node:url');
const {chromium} = require(process.env.DS_PLAYWRIGHT_MODULE || 'playwright');

(async () => {
  assert(process.argv[2], 'usage: check_ds_trace_write_overview.js bottleneck.write.html');
  const browser = await chromium.launch({headless:true,
    ...(process.env.DS_CHROMIUM_EXECUTABLE ? {executablePath:process.env.DS_CHROMIUM_EXECUTABLE} : {})});
  try {
    const page = await browser.newPage({viewport:{width:1280,height:900}});
    const errors = [];
    page.on('pageerror', error => errors.push(String(error)));
    await page.goto(pathToFileURL(path.resolve(process.argv[2])).href, {timeout:120000});
    await page.waitForFunction(() => window.ReportRegistry?.audit({requireComplete:true}).valid);
    const layout = await page.evaluate(() => ({
      chapters:[...document.querySelectorAll('main > section.panel > h2')].map(node=>node.textContent.trim()),
      figures:[...document.querySelectorAll('#overview .chart-caption')].map(node=>node.textContent.trim()),
      bands:document.querySelectorAll('#latency-band-controls button').length,
      errorBeforeTime:document.querySelector('#error-analysis')?.compareDocumentPosition(
        document.querySelector('#time')) & Node.DOCUMENT_POSITION_FOLLOWING,
    }));
    assert.deepEqual(layout.chapters.slice(0,4), [
      '1. 总览', '2. 时间序列', '3. 写入阶段与 RPC', '4. URMA WR',
    ]);
    assert.deepEqual(layout.figures.map(title=>title.match(/^图\s+\d+-\d+/)?.[0]),
      ['图 1-1','图 1-2','图 1-3','图 1-4','图 1-5']);
    assert.equal(layout.bands, 6);
    assert(layout.errorBeforeTime);
    const state = () => page.evaluate(() => {
      const chartCount = id => echarts.getInstanceByDom(document.getElementById(id))
        .getOption().series.flatMap(series=>series.data)
        .reduce((sum,value)=>sum+Number(value?.value??value??0),0);
      const traceCount = Number(document.querySelector('#trace-table .pager span')?.textContent
        .match(/·\s*(\d+)\s*条/)?.[1]);
      return {scope:filtered.length,band:bandRows.length,
        kpi:Number(document.querySelector('#kpis .kpi b')?.textContent),
        problemCount:chartCount('problem-count'),errorCount:chartCount('error-chart'),
        bandCount:chartCount('latency-band-chart'),traceCount,
        wrScope:ReportRegistry.audit().components.find(item=>item.id==='wr-count-chart')?.matchedCount,
        valid:ReportRegistry.audit({requireComplete:true}).valid};
    });
    const all = await state();
    assert(all.valid && all.scope>1000 && all.band===all.scope);
    assert.equal(all.problemCount, all.scope);
    assert.equal(all.traceCount, all.scope);
    assert(all.wrScope > 0 && all.wrScope <= all.scope);
    assert(all.bandCount <= all.scope);
    assert.equal(all.errorCount, await page.evaluate(() => bandRows.filter(row=>row.client_status_failed).length));
    await page.locator('#latency-band-controls button[data-band="5-6"]').click();
    const selected = await state();
    const expected = await page.evaluate(() => filtered.filter(row=>row.client_ms>=5&&row.client_ms<6).length);
    assert.equal(selected.band, expected);
    assert.equal(selected.kpi, expected);
    assert.equal(selected.problemCount, expected);
    assert.equal(selected.traceCount, expected);
    assert.equal(selected.wrScope, all.wrScope);
    assert.equal(selected.errorCount, await page.evaluate(() => bandRows.filter(row=>row.client_status_failed).length));
    assert(selected.valid);
    await page.locator('#latency-band-controls button[data-band="all"]').click();
    assert.equal((await state()).traceCount, all.scope);
    await page.locator('#write-top-n').selectOption('100');
    await page.locator('#latency-band-controls button[data-band="5-6"]').click();
    const emptyBand = await state();
    assert.equal(emptyBand.band, await page.evaluate(() => filtered.filter(row=>row.client_ms>=5&&row.client_ms<6).length));
    assert.equal(emptyBand.traceCount, emptyBand.band);
    assert(emptyBand.valid);
    assert.deepEqual(errors, []);
    await page.close();
  } finally {
    await browser.close();
  }
})().catch(error=>{console.error(error);process.exitCode=1;});
