#!/usr/bin/env node
const assert = require('node:assert/strict');
const path = require('node:path');
const {pathToFileURL} = require('node:url');
const {chromium} = require(process.env.DS_PLAYWRIGHT_MODULE || 'playwright');

(async () => {
  const file = process.argv[2];
  assert(file, 'usage: check_ds_trace_write_axis_precision.js bottleneck.write.html');
  const browser = await chromium.launch({headless:true,
    ...(process.env.DS_CHROMIUM_EXECUTABLE ? {executablePath:process.env.DS_CHROMIUM_EXECUTABLE} : {})});
  try {
    const page = await browser.newPage({viewport:{width:1280,height:900}});
    const errors = [];
    page.on('pageerror', error => errors.push(String(error)));
    await page.goto(pathToFileURL(path.resolve(file)).href);
    await page.waitForFunction(() => typeof echarts !== 'undefined' &&
      echarts.getInstanceByDom(document.getElementById('problem-time')));
    const axes = await page.evaluate(() => {
      const result = [];
      for (const node of document.querySelectorAll('.chart')) {
        const chart = echarts.getInstanceByDom(node);
        if (!chart) continue;
        for (const dimension of ['xAxis','yAxis']) {
          for (const axis of chart.getOption()[dimension] || []) {
            if (axis.type !== 'value') continue;
            const formatter = axis.axisLabel?.formatter;
            result.push({chart:node.id, dimension, name:axis.name,
              sample:typeof formatter === 'function' ? formatter(1.2346) : null,
              zero:typeof formatter === 'function' ? formatter(0) : null});
          }
        }
      }
      return result;
    });
    assert.deepEqual(errors, []);
    const durations = axes.filter(axis => /\bms\b/i.test(axis.name));
    const counts = axes.filter(axis => /Trace|WR 数|WR数|记录数|条/.test(axis.name));
    assert(durations.length >= 6, `too few duration axes: ${JSON.stringify(durations)}`);
    for (const axis of durations) {
      assert.equal(axis.sample, '1.235', `${axis.chart}/${axis.dimension} duration precision`);
      assert.equal(axis.zero, '0.000', `${axis.chart}/${axis.dimension} zero precision`);
    }
    for (const axis of counts) {
      assert.notEqual(axis.sample, '1.235', `${axis.chart}/${axis.dimension} count formatted as duration`);
    }
    await page.close();
  } finally {
    await browser.close();
  }
})().catch(error => {console.error(error); process.exitCode = 1;});
