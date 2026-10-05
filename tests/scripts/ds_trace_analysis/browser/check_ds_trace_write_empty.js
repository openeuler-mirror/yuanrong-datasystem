/* Usage: node check_ds_trace_write_empty.js empty-write.html populated-write.html */
const assert = require('assert');
const path = require('path');
const {pathToFileURL} = require('url');
const {chromium} = require(process.env.DS_PLAYWRIGHT_MODULE || 'playwright');

(async () => {
  assert.strictEqual(process.argv.length, 4, 'provide empty and populated write reports');
  const browser = await chromium.launch({headless:true, ...(process.env.DS_CHROMIUM_EXECUTABLE
    ? {executablePath:process.env.DS_CHROMIUM_EXECUTABLE} : {})});
  try {
    for (const [index, file] of process.argv.slice(2).entries()) {
      const page = await browser.newPage();
      const errors = [];
      page.on('pageerror', error => errors.push(String(error)));
      await page.goto(pathToFileURL(path.resolve(file)).href);
      await page.waitForFunction(() => window.ReportDiagnostics);
      for (const width of [1500, 900, 390]) {
        await page.setViewportSize({width, height:1000});
        await page.locator('#selected-chart').scrollIntoViewIfNeeded();
        const state = async () => page.evaluate(() => {
          const audit = ReportDiagnostics.audit();
          return {valid:audit.valid, state:audit.charts.find(c => c.id === 'selected-chart').state};
        });
        assert.deepStrictEqual(await state(), {valid:true, state:index ? 'rendered' : 'empty'});
        if (index) {
          await page.locator('#search').fill('__no_matching_trace_fixture__');
          await page.waitForFunction(() => ReportDiagnostics.audit().charts
            .find(c => c.id === 'selected-chart').state === 'empty');
          assert.deepStrictEqual(await state(), {valid:true, state:'empty'});
          await page.locator('#search').fill('');
          await page.waitForFunction(() => ReportDiagnostics.audit().charts
            .find(c => c.id === 'selected-chart').state === 'rendered');
          assert.deepStrictEqual(await state(), {valid:true, state:'rendered'});
        }
      }
      assert.deepStrictEqual(errors, []);
      await page.close();
    }
    console.log(JSON.stringify({valid:true, widths:[1500,900,390], emptyAndRecovery:true}));
  } finally {
    await browser.close();
  }
})().catch(error => {console.error(error); process.exit(1)});
