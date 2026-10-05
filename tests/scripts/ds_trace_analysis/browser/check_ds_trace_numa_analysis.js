#!/usr/bin/env node
/* Browser contract check for the PR2081 NUMA trace dashboard. */

let playwright;
try {
  playwright = require('playwright');
} catch (_) {
  if (!process.env.DS_PLAYWRIGHT_MODULE) {
    throw new Error('playwright is unavailable; set DS_PLAYWRIGHT_MODULE');
  }
  playwright = require(process.env.DS_PLAYWRIGHT_MODULE);
}
const { chromium } = playwright;
const { pathToFileURL } = require('url');
const fs = require('fs');
const path = require('path');

async function main() {
  if (!process.argv[2]) throw new Error('usage: check_ds_trace_numa_analysis.js <index.html>');
  const launchOptions = { headless: true };
  if (process.env.DS_CHROMIUM_EXECUTABLE) launchOptions.executablePath = process.env.DS_CHROMIUM_EXECUTABLE;
  const browser = await chromium.launch(launchOptions);
  const page = await browser.newPage({ viewport: { width: 1500, height: 1000 } });
  const errors = [];
  page.on('pageerror', error => errors.push(String(error)));
  page.on('console', message => { if (message.type() === 'error') errors.push(message.text()); });
  await page.goto(pathToFileURL(path.resolve(process.argv[2])).href, { waitUntil: 'load' });
  await page.waitForFunction(() => window.echarts && document.querySelector('#error-chart canvas'));
  const initial = await page.evaluate(() => ({
    traces: DATA.traces.length,
    charts: document.querySelectorAll('.chart canvas').length,
    rows: document.querySelectorAll('#trace-table tbody tr').length,
    pager: document.querySelector('#page-label')?.textContent,
    centered: [...document.querySelectorAll('.chart-title')].every(x => getComputedStyle(x).textAlign === 'center'),
    sourceSteps: document.querySelectorAll('.source-step').length,
    expectedSourceSteps: DATA.source_chain.length,
    reportLinks: [...document.querySelectorAll('.report-links a')].map(x => x.getAttribute('href')),
    runtimeConfig: DATA.metadata.runtime_config,
    runtimeText: document.querySelector('#runtime-config')?.textContent,
  }));
  const cohort = await page.locator('#f-cohort option').nth(1).getAttribute('value');
  if (cohort) await page.locator('#f-cohort').selectOption(cohort);
  const filtered = await page.evaluate(() => ({ expected: DATA.traces.filter(x => !document.querySelector('#f-cohort').value || x.cohorts.includes(document.querySelector('#f-cohort').value)).length, actual: filtered.length }));
  await page.click('#trace-table th[data-key="client_ms"]');
  const detailBefore = await page.locator('#detail-summary').textContent();
  await page.locator('#trace-table tbody tr').first().click();
  const detailAfter = await page.locator('#detail-summary').textContent();
  const [download] = await Promise.all([page.waitForEvent('download'), page.click('#download-filtered')]);
  const downloadText = fs.readFileSync(await download.path(), 'utf8');
  const downloaded = (downloadText.match(/^Trace ID:/gm) || []).length;
  await page.setViewportSize({ width: 900, height: 1000 });
  await page.waitForTimeout(150);
  const responsive = await page.evaluate(() => ({
    pageFits: document.documentElement.scrollWidth <= document.documentElement.clientWidth + 1,
    tableFits: document.querySelector('#trace-table').scrollWidth <= document.querySelector('#trace-table').parentElement.clientWidth + 1,
  }));
  await browser.close();
  if (errors.length) throw new Error(`browser errors: ${errors.join(' | ')}`);
  if (initial.charts < 6 || initial.rows !== Math.min(8, initial.traces) || !initial.pager?.includes(`${initial.traces}条`)) throw new Error(`render: ${JSON.stringify(initial)}`);
  if (!initial.centered || initial.sourceSteps !== initial.expectedSourceSteps) throw new Error(`layout/source: ${JSON.stringify(initial)}`);
  if (initial.reportLinks.join(',') !== 'index.html,bottleneck.html,triage.html') throw new Error(`report links: ${JSON.stringify(initial.reportLinks)}`);
  const runtime = initial.runtimeConfig || {};
  for (const [key, value] of Object.entries(runtime)) {
    const expected = value == null ? '未配置' : String(value);
    if (!initial.runtimeText?.includes(expected)) throw new Error(`runtime rendering ${key}=${expected}: ${initial.runtimeText}`);
  }
  const reportDir = path.dirname(path.resolve(process.argv[2]));
  if (!['bottleneck.html', 'triage.html'].every(name => fs.statSync(path.join(reportDir, name)).size > 0)) throw new Error('linked report is missing');
  if (filtered.expected !== filtered.actual || downloaded !== filtered.actual) throw new Error(`filter/download: ${JSON.stringify({ filtered, downloaded })}`);
  if (!detailBefore || !detailAfter) throw new Error('trace detail is empty');
  if (!responsive.pageFits || !responsive.tableFits) throw new Error(`responsive: ${JSON.stringify(responsive)}`);
  console.log(JSON.stringify({ initial, filtered, downloaded, responsive }, null, 2));
}

main().catch(error => { console.error(error.stack || error); process.exit(1); });
