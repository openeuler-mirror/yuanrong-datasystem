#!/usr/bin/env node
const assert = require('node:assert/strict');
const path = require('node:path');
const {pathToFileURL} = require('node:url');
const {chromium} = require(process.env.DS_PLAYWRIGHT_MODULE || 'playwright');

(async () => {
  const file = process.argv[2];
  assert(file, 'usage: check_ds_trace_write_worker_all.js bottleneck.write.html');
  const browser = await chromium.launch({headless:true,
    ...(process.env.DS_CHROMIUM_EXECUTABLE ? {executablePath:process.env.DS_CHROMIUM_EXECUTABLE} : {})});
  try {
    const page = await browser.newPage({viewport:{width:1280,height:900}});
    const errors = [];
    page.on('pageerror', error => errors.push(String(error)));
    await page.goto(pathToFileURL(path.resolve(file)).href);
    await page.waitForFunction(() => typeof echarts !== 'undefined' &&
      echarts.getInstanceByDom(document.getElementById('worker-chart')));
    const hosts = await page.evaluate(() => {
      const observed = new Set(MODEL.rows.flatMap(row => row.worker_events)
        .filter(event => event.timestamp).map(event => event.host));
      return [...document.querySelectorAll('#worker-choice option')]
        .map(option => option.value).filter(value => value && observed.has(value));
    });
    assert(hosts.length > 0, 'fixture has no Worker observations');
    const inspect = () => page.evaluate(() => {
      const pick = document.getElementById('worker-choice').value;
      const chart = echarts.getInstanceByDom(document.getElementById('worker-time'));
      const values = chart?.getOption().series.find(series => series.name === '采样完成数')?.data || [];
      const observed = MODEL.rows.flatMap(row => row.worker_events)
        .filter(event => (!pick || event.host === pick) && event.timestamp).length;
      const registry = ReportRegistry.audit().components.find(item => item.id === 'worker-time');
      return {pick, observed, plotted:values.reduce((sum, value) => sum + Number(value.value ?? value), 0),
        chart:!!chart, registry:registry?.state};
    });
    const all = await inspect();
    assert(all.observed > 0, 'fixture has no timed Worker events');
    assert(all.chart, 'all-Worker time chart is blank on initial load');
    assert.equal(all.plotted, all.observed, 'all-Worker chart omitted events');
    assert.equal(all.registry, 'rendered');
    await page.locator('#worker-choice').selectOption(hosts[0]);
    const one = await inspect();
    assert(one.chart && one.observed > 0);
    assert.equal(one.plotted, one.observed, 'selected Worker chart omitted events');
    await page.locator('#worker-choice').selectOption('');
    const again = await inspect();
    assert.equal(again.plotted, all.observed, 'switching back to all did not restore events');
    assert.equal(again.registry, 'rendered');
    assert.deepEqual(errors, []);
    await page.close();
  } finally {
    await browser.close();
  }
})().catch(error => {console.error(error); process.exitCode = 1;});
