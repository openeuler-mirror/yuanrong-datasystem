#!/usr/bin/env node
const assert = require('node:assert/strict');
const path = require('node:path');
const {pathToFileURL} = require('node:url');
const {chromium} = require(process.env.DS_PLAYWRIGHT_MODULE || 'playwright');

(async () => {
  const [readFile, writeFile] = process.argv.slice(2);
  assert(readFile && writeFile, 'usage: check_ds_trace_write_legends.js read.html write.html');
  const browser = await chromium.launch({headless:true,
    ...(process.env.DS_CHROMIUM_EXECUTABLE ? {executablePath:process.env.DS_CHROMIUM_EXECUTABLE} : {})});
  try {
    for (const width of [1500, 900, 390]) {
      const page = await browser.newPage({viewport:{width,height:1000}});
      const errors = [];
      page.on('pageerror', error => errors.push(String(error)));
      await page.goto(pathToFileURL(path.resolve(readFile)).href);
      const readNav = await page.locator('#nav').evaluate(node => getComputedStyle(node).backgroundColor);
      await page.goto(pathToFileURL(path.resolve(writeFile)).href);
      await page.waitForFunction(() => typeof echarts !== 'undefined' && echarts.getInstanceByDom(document.getElementById('problem-count')));
      const state = await page.evaluate(() => {
        const ids = ['problem-count','problem-time','stage-share','issue-chart','worker-chart'];
        const charts = ids.map(id => {
          const chart = echarts.getInstanceByDom(document.getElementById(id));
          const option = chart.getOption();
          const seriesNames = option.series.flatMap(series => series.name ? [series.name] : []);
          const legend = option.legend?.[0];
          const view = chart.getViewOfComponentModel(chart.getModel().getComponent('legend'));
          const bounds = view?.group.getBoundingRect().clone();
          if (bounds) bounds.applyTransform(view.group.getComputedTransform());
          return {id, seriesNames, legendShown:!!legend && legend.show !== false,
            legendNames:legend?.data?.map(item => typeof item === 'string' ? item : item.name) || [],
            legendBounds:bounds && {right:bounds.x+bounds.width,bottom:bounds.y+bounds.height},
            chartWidth:chart.getWidth(), plotTop:option.grid?.[0]?.top};
        });
        return {charts, nav:getComputedStyle(document.getElementById('write-nav')).backgroundColor,
          repeatedLinks:!!document.querySelector('#report-switcher') &&
            getComputedStyle(document.querySelector('main>header .report-links') ||
              document.querySelector('main>header>p')).display !== 'none',
          overflow:document.documentElement.scrollWidth > document.documentElement.clientWidth};
      });
      assert.deepEqual(errors, []);
      assert.equal(state.nav, readNav, `sidebar background differs at ${width}px`);
      assert.equal(state.repeatedLinks, false, `duplicate top links at ${width}px`);
      assert.equal(state.overflow, false, `page overflows at ${width}px`);
      for (const chart of state.charts) {
        assert(chart.legendShown, `${chart.id} has no visible legend`);
        assert(chart.legendNames.length, `${chart.id} has no legend entries`);
        assert(chart.legendNames.every(name => chart.seriesNames.includes(name) || chart.id === 'stage-share'),
          `${chart.id} legend does not match series`);
        assert(chart.legendBounds.right <= chart.chartWidth + 1, `${chart.id} legend is clipped`);
        if (chart.plotTop != null) {
          assert(chart.legendBounds.bottom < chart.plotTop, `${chart.id} legend overlaps plot`);
        }
      }
      await page.close();
    }
  } finally {
    await browser.close();
  }
})().catch(error => {console.error(error); process.exitCode = 1;});
