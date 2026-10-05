#!/usr/bin/env node
const assert = require('node:assert/strict');
const path = require('node:path');
const {pathToFileURL} = require('node:url');
const {chromium} = require(process.env.DS_PLAYWRIGHT_MODULE || 'playwright');

(async () => {
  const file = process.argv[2];
  assert(file, 'usage: check_ds_trace_write_scope.js bottleneck.write.html');
  const browser = await chromium.launch({headless:true,
    ...(process.env.DS_CHROMIUM_EXECUTABLE ? {executablePath:process.env.DS_CHROMIUM_EXECUTABLE} : {})});
  try {
    const page = await browser.newPage({viewport:{width:1280,height:900}});
    const errors = [];
    page.on('pageerror', error => errors.push(String(error)));
    await page.goto(pathToFileURL(path.resolve(file)).href);
    await page.waitForFunction(() => typeof echarts !== 'undefined' &&
      echarts.getInstanceByDom(document.getElementById('problem-count')));
    const location = await page.evaluate(() => ({
      scope:document.querySelector('#overview #write-top-n')?.value,
      overviewSelectors:[...document.querySelectorAll('#overview select')].map(node => node.id),
      traceSearch:!!document.querySelector('#traces #search'),
      traceFilters:!!document.querySelector('#traces #operation, #traces #stage'),
      redundantTimelineTop:!!document.querySelector('#time #top'),
    }));
    assert.deepEqual(location, {scope:'0',overviewSelectors:['write-top-n'],
      traceSearch:true,traceFilters:true,redundantTimelineTop:false});
    const state = () => page.evaluate(() => {
      const chart = echarts.getInstanceByDom(document.getElementById('problem-count'));
      const audit = ReportRegistry.audit({requireComplete:true});
      const chartCount = chart.getOption().series.flatMap(series => series.data)
        .reduce((sum, value) => sum + Number(value?.value ?? value ?? 0), 0);
      const kpi = document.querySelector('#kpis .kpi b')?.textContent;
      const traceCount = Number(document.querySelector('#trace-table .pager span')?.textContent.match(/·\s*(\d+)\s*条/)?.[1]);
      return {chartCount,kpi,traceCount,auditValid:audit.valid,selected:audit.components
        .find(item => item.id === 'selected-chart')?.state};
    });
    const all = await state();
    assert(all.auditValid && all.chartCount > 1000 && all.traceCount === all.chartCount);
    await page.locator('#write-top-n').selectOption('100');
    const top100 = await state();
    assert.equal(top100.chartCount, 100);
    assert.equal(top100.traceCount, 100);
    assert(top100.auditValid);
    const topIds = await page.evaluate(() => ({
      expected:[...ALL].sort((a,b)=>b.client_ms-a.client_ms ||
        a.client_timestamp.localeCompare(b.client_timestamp) ||
        a.trace_id.localeCompare(b.trace_id)).slice(0,100).map(row=>row.trace_id),
      actual:filtered.map(row=>row.trace_id),
    }));
    assert.deepEqual(topIds.actual, topIds.expected);
    await page.locator('#search').fill('__no_matching_trace_fixture__');
    await page.waitForTimeout(200);
    const noMatch = await state();
    assert.equal(noMatch.chartCount, 100, 'Trace-only filter changed overview chart');
    assert.equal(noMatch.kpi, top100.kpi, 'Trace-only filter changed overview KPI');
    assert.equal(noMatch.traceCount, 0);
    assert.equal(noMatch.selected, 'empty');
    assert(noMatch.auditValid);
    await page.locator('#search').fill('');
    await page.waitForTimeout(200);
    assert.equal((await state()).traceCount, 100);
    await page.locator('#write-top-n').selectOption('1000');
    assert.equal((await state()).chartCount, 1000);
    await page.locator('#write-top-n').selectOption('0');
    assert.equal((await state()).chartCount, all.chartCount);
    assert.deepEqual(errors, []);
    await page.close();
  } finally {
    await browser.close();
  }
})().catch(error => {console.error(error); process.exitCode = 1;});
