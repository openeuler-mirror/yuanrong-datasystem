'use strict';

const assert = require('node:assert/strict');
const path = require('node:path');
const {pathToFileURL} = require('node:url');
const {chromium} = require(process.env.DS_PLAYWRIGHT_MODULE || 'playwright');

(async () => {
  assert(process.argv[2], 'usage: check_ds_trace_numa_worker_seconds.js numa.local.html');
  const browser = await chromium.launch({headless: true,
    ...(process.env.DS_CHROMIUM_EXECUTABLE ? {executablePath: process.env.DS_CHROMIUM_EXECUTABLE} : {})});
  try {
    const page = await browser.newPage({viewport: {width: 1280, height: 900}});
    const errors = [];
    page.on('pageerror', error => errors.push(String(error)));
    await page.goto(pathToFileURL(path.resolve(process.argv[2])).href, {timeout: 120000});
    await page.waitForFunction(() => window.ReportRegistry?.audit({requireComplete: true}).valid);
    const initial = await page.evaluate(() => {
      const sum = values => values.reduce((total, value) => total + Number(value?.value ?? value ?? 0), 0);
      const option = id => echarts.getInstanceByDom(document.getElementById(id)).getOption();
      const known = worker => worker && worker !== '未明确' && worker !== 'unknown';
      return ['read', 'write'].map((prefix, index) => {
        const rows = DATA.traces.filter(row => row.operation === (index ? 'PUT' : 'GET'));
        const counts = new Map();
        for (const row of rows) {
          if (!known(row.worker)) continue;
          const entry = counts.get(row.worker) || {anomalies: 0, traces: 0};
          entry.traces++;
          entry.anomalies += Number(row.status != null && Number(row.status) !== 0 || row.slow_wr_count > 0);
          counts.set(row.worker, entry);
        }
        const ranked = [...counts].sort((a, b) => b[1].anomalies - a[1].anomalies ||
          b[1].traces - a[1].traces || a[0].localeCompare(b[0]));
        const selected = document.getElementById(prefix + '-worker-time-choice');
        const worker = option(prefix + '-worker-chart');
        const time = option(prefix + '-time-chart');
        const top = option(prefix + '-worker-time-chart');
        const traceSeries = chart => chart.series.find(series => series.name === 'Trace');
        return {
          prefix, ranked, selected: selected.value, options: [...selected.options].map(item => item.value),
          topChoice: document.getElementById(prefix + '-worker-time-top').value,
          rows: rows.length, timed: rows.filter(row => /^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d/.test(row.timestamp || '')).length,
          workerTotal: sum(traceSeries(worker).data), timeTotal: sum(traceSeries(time).data),
          selectedTotal: sum(traceSeries(top).data),
          selectedExpected: rows.filter(row => row.worker === selected.value && row.timestamp).length,
          workerSeries: worker.series.map(series => ({name: series.name, type: series.type,
            axis: series.yAxisIndex || 0})),
          timeSeries: time.series.map(series => ({name: series.name, type: series.type,
            axis: series.yAxisIndex || 0})),
          scope: document.getElementById(prefix + '-worker-scope').textContent,
        };
      });
    });
    for (const result of initial) {
      assert.equal(result.topChoice, '10');
      assert.equal(result.selected, result.ranked[0]?.[0]);
      assert.deepEqual(result.options, result.ranked.slice(0, 10).map(item => item[0]));
      assert.equal(result.workerTotal, result.rows);
      assert.equal(result.timeTotal, result.timed);
      assert.equal(result.selectedTotal, result.selectedExpected);
      assert(result.scope.includes(`${result.rows} 条唯一 Trace`));
      for (const series of [...result.workerSeries, ...result.timeSeries]) {
        if (series.name === '慢 WR 事件') assert.equal(series.type, 'bar');
        if (series.name === 'Client P90 ms') {
          assert.equal(series.type, 'line');
          assert.equal(series.axis, 1);
        }
      }
    }
    await page.locator('#write-worker-time-top').selectOption('5');
    assert.equal(await page.locator('#write-worker-time-choice option').count(),
      Math.min(5, initial[1].ranked.length));
    assert.equal(await page.locator('#read-worker-time-top').inputValue(), '10');
    await page.locator('#write-worker-time-choice').selectOption(initial[1].ranked[1][0]);
    const changed = await page.evaluate(() => {
      const selected = document.getElementById('write-worker-time-choice').value;
      const chart = echarts.getInstanceByDom(document.getElementById('write-worker-time-chart')).getOption();
      const values = chart.series.find(series => series.name === 'Trace').data;
      return {selected, total: values.reduce((sum, item) => sum + Number(item?.value ?? item ?? 0), 0),
        readSelected: document.getElementById('read-worker-time-choice').value,
        valid: ReportRegistry.audit({requireComplete: true}).valid};
    });
    assert.equal(changed.selected, initial[1].ranked[1][0]);
    assert.equal(changed.total, initial[1].ranked[1][1].traces);
    assert.equal(changed.readSelected, initial[0].selected);
    assert(changed.valid);
    await page.locator('#write-worker-time-top').selectOption('0');
    assert.equal(await page.locator('#write-worker-time-choice option').count(), initial[1].ranked.length);
    assert.deepEqual(errors, []);
    const mobile = await browser.newPage({viewport: {width: 390, height: 844}});
    const mobileErrors = [];
    mobile.on('pageerror', error => mobileErrors.push(String(error)));
    await mobile.goto(pathToFileURL(path.resolve(process.argv[2])).href, {timeout: 120000});
    await mobile.waitForFunction(() => window.ReportRegistry?.audit({requireComplete: true}).valid);
    const mobileLayout = await mobile.evaluate(() => {
      const zoom = id => echarts.getInstanceByDom(document.getElementById(id)).getOption().dataZoom?.[0] ||
        {start: 0, end: 100};
      return {
        documentWidth: document.documentElement.scrollWidth,
        viewportWidth: innerWidth,
        workerVisible: zoom('read-worker-chart').end - zoom('read-worker-chart').start,
        timeVisible: zoom('read-time-chart').end - zoom('read-time-chart').start,
        workerCount: echarts.getInstanceByDom(document.getElementById('read-worker-chart')).getOption().xAxis[0].data.length,
        secondCount: echarts.getInstanceByDom(document.getElementById('read-time-chart')).getOption().xAxis[0].data.length,
      };
    });
    assert(mobileLayout.documentWidth <= mobileLayout.viewportWidth);
    assert(mobileLayout.workerVisible * mobileLayout.workerCount / 100 <= 7);
    assert(mobileLayout.timeVisible * mobileLayout.secondCount / 100 <= 13);
    assert.deepEqual(mobileErrors, []);
    console.log(JSON.stringify({initial: initial.map(({prefix, rows, timed, selected}) =>
      ({prefix, rows, timed, selected})), changed, mobileLayout, errors}));
  } finally {
    await browser.close();
  }
})().catch(error => {console.error(error); process.exitCode = 1;});
