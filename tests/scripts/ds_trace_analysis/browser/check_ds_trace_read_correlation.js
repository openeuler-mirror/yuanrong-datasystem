const assert = require('assert');
const {pathToFileURL} = require('url');
const path = require('path');
const {chromium} = require(process.env.DS_PLAYWRIGHT_MODULE || 'playwright');

(async () => {
  const browser = await chromium.launch({headless: true, ...(process.env.DS_CHROMIUM_EXECUTABLE ? {executablePath: process.env.DS_CHROMIUM_EXECUTABLE} : {})});
  try {
    for (const file of process.argv.slice(2)) {
      const page = await browser.newPage({viewport: {width: 1500, height: 1000}});
      const errors = [];
      page.on('pageerror', error => errors.push(error.message));
      await page.goto(pathToFileURL(path.resolve(file)).href + '#worker-correlation');
      await page.locator('#worker-correlation').scrollIntoViewIfNeeded();
      const workers = await page.evaluate(() => {
        const events = AGG.worker_correlation.events;
        return ['', ...new Set([
          events.find(e => e.kind === 'rpc_server')?.worker,
          events.find(e => e.dimension === 'ub' && !events.some(d => d.worker === e.worker && d.dimension === 'data'))?.worker,
        ].filter(Boolean)), ''];
      });
      for (const worker of workers) {
        await page.locator('#correlation-worker-filter').selectOption(worker);
        const states = await page.evaluate(() => ['rpc', 'ub', 'metadata', 'data'].map(dimension => {
          const element = document.getElementById('worker-correlation-chart-' + dimension);
          const chart = echarts.getInstanceByDom(element);
          const option = chart?.getOption();
          let legendGap = null;
          if (chart) {
            const model = chart.getModel(), legend = model.getComponent('legend');
            const view = chart.getViewOfComponentModel(legend), box = view.group.getBoundingRect();
            legendGap = model.getComponent('grid').coordinateSystem.getRect().y - box.y - view.group.y - box.height;
          }
          return {
            dimension, expected: filteredCorrelationEvents().filter(e => e.dimension === dimension).length,
            rendered: !!chart, emptyReason: element.dataset.emptyReason, legendGap,
            visibleLineMarkers: option?.series.filter(s => s.type === 'line').every(s => s.showSymbol),
          };
        }));
        for (const state of states) {
          assert.equal(state.rendered, state.expected > 0, JSON.stringify({worker, state}));
          if (state.rendered) {
            assert(state.visibleLineMarkers, JSON.stringify(state));
            assert(state.legendGap >= 0, JSON.stringify(state));
          } else assert(state.emptyReason, JSON.stringify(state));
        }
      }
      for (const width of [900, 390]) {
        await page.setViewportSize({width, height: 1000});
        await page.waitForTimeout(400);
        assert(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth + 2), 'page overflow');
      }
      assert.deepStrictEqual(errors, []);
      console.log(path.basename(file), 'Worker changes, sparse lines, empty states and responsive layout passed');
      await page.close();
    }
  } finally {
    await browser.close();
  }
})().catch(error => {console.error(error); process.exit(1);});
