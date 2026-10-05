const assert = require('assert');
const path = require('path');
const {pathToFileURL} = require('url');
const {chromium} = require(process.env.DS_PLAYWRIGHT_MODULE || 'playwright');

(async () => {
  const browser = await chromium.launch({headless:true,
    executablePath:process.env.DS_CHROMIUM_EXECUTABLE});
  try {
    for (const file of process.argv.slice(2)) {
      const page = await browser.newPage(), errors = [];
      page.on('pageerror', error => errors.push(String(error)));
      await page.goto(pathToFileURL(path.resolve(file)).href);
      await page.waitForSelector('#trace-event-table-title');
      const result = await page.evaluate(() => ({
        pageKind:window.REPORT_COMPONENT_REGISTRY.page_kind,
        chapter:document.querySelector('#trace-event-timeline > h2').textContent.trim(),
        caption:document.getElementById('trace-event-table-title').textContent.trim(),
        navigation:document.querySelector('a[href="#trace-event-table"],a[href="#trace-event-table-title"]').textContent.trim(),
        registry:ReportRegistry.title('trace-event-table'),
        legacy:document.querySelector('#traces > h2')?.textContent.trim(),
        timelineSections:document.querySelectorAll('#trace-event-timeline').length,
        timelineNav:document.querySelectorAll('a[href="#trace-event-timeline"]').length,
        chartNav:document.querySelectorAll('a[href="#trace-event-chart"]').length,
        tableNav:document.querySelectorAll('a[href="#trace-event-table"],a[href="#trace-event-table-title"]').length,
        appendix:document.querySelector('#write-downloads > h2')?.textContent.trim(),
        appendixNavigation:document.querySelector('#write-nav a[href="#write-downloads"]')?.textContent.trim(),
      }));
      const write = result.pageKind === 'write';
      assert.equal(result.chapter, write ? '7. Trace 时间线' : '8. Trace 事件时间线');
      assert.equal(result.timelineSections, 1);
      assert.equal(result.timelineNav, 1);
      assert.equal(result.chartNav, 1);
      assert.equal(result.tableNav, 1);
      for (const field of ['caption', 'navigation', 'registry']) {
        assert.equal(result[field], write ? '表 7-1 Trace 事件明细' : '表 8-1 Trace 事件明细', JSON.stringify(result));
      }
      assert(['read', 'write'].includes(result.pageKind), JSON.stringify(result));
      if (result.pageKind === 'write') {
        assert.equal(result.appendix, '附录 · 下载与口径');
        assert.equal(result.appendixNavigation, result.appendix);
      }
      assert(result.legacy?.startsWith(write ? '6. Trace' : '7. Trace'), JSON.stringify(result));
      assert.deepEqual(errors, []);
      console.log(JSON.stringify({file, ...result, valid:true}));
      await page.close();
    }
  } finally {
    await browser.close();
  }
})().catch(error => {console.error(error); process.exit(1);});
