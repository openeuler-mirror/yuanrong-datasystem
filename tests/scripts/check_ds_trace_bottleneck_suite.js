#!/usr/bin/env node
const path = require('path');
const { pathToFileURL } = require('url');
const { chromium } = require(process.env.DS_PLAYWRIGHT_MODULE || 'playwright');

(async () => {
  const file = process.argv[2];
  const expectedRuns = Number(process.argv[3] || 0);
  if (!file) throw new Error('usage: check_ds_trace_bottleneck_suite.js <index.html> [expected-runs]');
  const browser = await chromium.launch({ headless: true, executablePath: process.env.DS_CHROMIUM_EXECUTABLE });
  const page = await browser.newPage({ viewport: { width: 1280, height: 800 } });
  const errors = [];
  page.on('pageerror', error => errors.push(String(error)));
  await page.goto(pathToFileURL(path.resolve(file)).href);
  await page.waitForTimeout(1000);
  const initial = await page.evaluate(() => ({
    rows: document.querySelectorAll('#run-rows tr').length,
    links: [...document.querySelectorAll('#run-rows a')].map(a => a.getAttribute('href')),
    charts: document.querySelectorAll('canvas').length,
    groups: document.querySelectorAll('.group').length,
    centeredTitles: [...document.querySelectorAll('.chart-title')].every(node => getComputedStyle(node).textAlign === 'center'),
    overflow: document.documentElement.scrollWidth > document.documentElement.clientWidth,
    warning: document.querySelector('.notice')?.textContent || '',
  }));
  if (errors.length) throw new Error(`page errors: ${errors.join('; ')}`);
  if (expectedRuns && initial.rows !== expectedRuns) throw new Error(`expected ${expectedRuns} runs, got ${initial.rows}`);
  const expectedLinks = await page.evaluate(() => D.runs.reduce((count, run) => count + 2 + (run.numa_report ? 1 : 0), 0));
  if (initial.links.length !== expectedLinks) throw new Error(`expected ${expectedLinks} detail links, got ${initial.links.length}`);
  if (initial.charts < 4 || !initial.centeredTitles || initial.overflow) throw new Error(JSON.stringify(initial));
  if (!initial.warning.includes('采集封顶')) throw new Error('sampling warning missing');
  await page.selectOption('#band', '10–20ms');
  await page.waitForTimeout(300);
  const filtered = await page.evaluate(() => ({ rows: document.querySelectorAll('#run-rows tr').length, charts: document.querySelectorAll('canvas').length }));
  console.log(JSON.stringify({ initial, filtered }, null, 2));
  await browser.close();
})().catch(error => { console.error(error); process.exit(1); });
