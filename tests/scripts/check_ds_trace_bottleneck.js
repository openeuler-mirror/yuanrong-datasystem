#!/usr/bin/env node
/* Browser contract check for a generated bottleneck.local.html. */

let playwright;
try {
  playwright = require('playwright');
} catch (_) {
  if (!process.env.DS_PLAYWRIGHT_MODULE) {
    throw new Error('playwright is unavailable; set DS_PLAYWRIGHT_MODULE to its module directory');
  }
  playwright = require(process.env.DS_PLAYWRIGHT_MODULE);
}
const { chromium } = playwright;
const { pathToFileURL } = require('url');
const fs = require('fs');
const path = require('path');

async function main() {
  if (!process.argv[2]) throw new Error('usage: check_ds_trace_bottleneck.js <report.html>');
  const report = path.resolve(process.argv[2]);
  const launchOptions = { headless: true };
  if (process.env.DS_CHROMIUM_EXECUTABLE) launchOptions.executablePath = process.env.DS_CHROMIUM_EXECUTABLE;
  const browser = await chromium.launch(launchOptions);
  const page = await browser.newPage({ viewport: { width: 1500, height: 1000 } });
  const errors = [];
  page.on('pageerror', error => errors.push(String(error)));
  page.on('console', message => { if (message.type() === 'error') errors.push(message.text()); });
  await page.goto(pathToFileURL(report).href, { waitUntil: 'load' });
  await page.waitForFunction(() => window.echarts && document.querySelector('#problem-count-chart canvas'));

  const initial = await page.evaluate(() => ({
    expectedTraces: ROWS.length,
    charts: document.querySelectorAll('.chart canvas').length,
    sortableHeaders: document.querySelectorAll('th[data-sort-key].sortable-header').length,
    traceRows: document.querySelectorAll('#trace-table tbody tr[data-id]').length,
    tracePager: document.querySelector('#page-label')?.textContent,
    edgeRows: document.querySelectorAll('#urma-edge-table tbody tr').length,
    urmaTimeRows: document.querySelectorAll('#urma-time-table tbody tr').length,
    urmaTimePager: document.querySelector('#urma-time-pager')?.textContent,
    nonTransportRows: document.querySelectorAll('#non-transport-table tbody tr[data-id]').length,
    logGroups: document.querySelectorAll('#trace-log-groups .trace-log-group').length,
    fullLineHighlights: document.querySelectorAll('#trace-log-groups .log-line.log-tag-error').length,
    correlationRows: document.querySelectorAll('#worker-correlation-table tbody tr[data-id]').length,
    correlationSummary: document.querySelector('#worker-correlation-summary')?.textContent,
    correlationUbCanvas: document.querySelectorAll('#worker-correlation-chart-ub canvas').length,
    accessLocations: [...document.querySelectorAll('#access-location-filter option')].map(option => option.value).filter(Boolean),
    chartTitleCount: document.querySelectorAll('.chart-title').length,
    nonCenteredChartTitles: [...document.querySelectorAll('.chart-title')].filter(title => getComputedStyle(title).textAlign !== 'center').length,
    timelineCount: echarts.getInstanceByDom(document.querySelector('#timeline-chart'))?.getOption().xAxis[0].data.length,
    latencySegments: AGG.latency_segments?.length,
    deepDiveCounts: {
      urma: URMA_ROWS.length,
      nonTransport: NON_TRANSPORT_ROWS.length,
      correlation: AGG.worker_correlation.events.length,
    },
    correlationSingleColumn: (() => {
      const cards = [...document.querySelectorAll('.correlation-grid > div')];
      return cards.length < 2 || Math.abs(cards[0].getBoundingClientRect().top - cards[1].getBoundingClientRect().top) > 4;
    })(),
    urmaDiagnosisShare: (() => {
      const table = document.querySelector('#urma-trace-table');
      const cell = table?.querySelector('tbody tr[data-id] td:last-child');
      return table && cell ? cell.getBoundingClientRect().width / table.getBoundingClientRect().width : null;
    })(),
  }));

  await page.locator('#time-segment-controls button[data-segment="2"]').click();
  const segmentFiltered = await page.evaluate(() => {
    const segment = AGG.latency_segments.find(item => item.segment_id === 2);
    return {
      expected: segment.trace_count,
      filtered: filtered.length,
      pager: document.querySelector('#page-label')?.textContent,
      timelineCount: echarts.getInstanceByDom(document.querySelector('#timeline-chart'))?.getOption().xAxis[0].data.length,
      active: document.querySelector('#time-segment-controls button.active')?.dataset.segment,
      scope: document.querySelector('#time-segment-scope')?.textContent,
      deepDiveCounts: {
        urma: URMA_ROWS.length,
        nonTransport: NON_TRANSPORT_ROWS.length,
        correlation: AGG.worker_correlation.events.length,
      },
    };
  });
  await page.locator('#time-segment-controls button[data-segment="all"]').click();

  const correlationWorker = await page.evaluate(() => {
    const event = AGG.worker_correlation.events.find(item => item.is_slow || item.failed || item.companions);
    return event?.worker || document.querySelector('#correlation-worker-filter option:nth-child(2)')?.value;
  });
  if (!correlationWorker) throw new Error('worker correlation has no selectable Worker');
  await page.locator('#correlation-worker-filter').selectOption(correlationWorker);
  const filteredCorrelation = await page.evaluate(() => ({
    traceRows: document.querySelectorAll('#trace-table tbody tr[data-id]').length,
    rows: document.querySelectorAll('#worker-correlation-table tbody tr[data-id]').length,
    ubCanvas: document.querySelectorAll('#worker-correlation-chart-ub canvas').length,
    summary: document.querySelector('#worker-correlation-summary')?.textContent,
  }));
  const correlationTrace = await page.locator('#worker-correlation-table tbody tr[data-id]').first().getAttribute('data-id');
  if (correlationTrace) await page.locator('#worker-correlation-table tbody tr[data-id]').first().click();
  const correlationDetailTrace = correlationTrace
    ? await page.locator('#trace-detail code').first().textContent()
    : null;

  await page.click('#trace-table th:nth-child(8)');
  const descending = await page.evaluate(() => ({
    first: Number.parseFloat(document.querySelector('#trace-table tbody td:nth-child(8)')?.textContent),
    max: Math.max(...ROWS.map(row => row.client_ms)),
    header: document.querySelector('#trace-table th:nth-child(8)')?.textContent,
  }));
  await page.click('#trace-table th:nth-child(8)');
  const ascending = await page.evaluate(() => ({
    first: Number.parseFloat(document.querySelector('#trace-table tbody td:nth-child(8)')?.textContent),
    min: Math.min(...ROWS.map(row => row.client_ms)),
    header: document.querySelector('#trace-table th:nth-child(8)')?.textContent,
  }));

  const firstTrace = await page.locator('#trace-table tbody tr').first().getAttribute('data-id');
  await page.click('#trace-table tbody tr');
  const detailTrace = await page.locator('#trace-detail code').first().textContent();

  const [download] = await Promise.all([
    page.waitForEvent('download'),
    page.click('#download-all-traces'),
  ]);
  const downloadText = fs.readFileSync(await download.path(), 'utf8');
  const downloadedTraces = (downloadText.match(/^Trace ID:/gm) || []).length;
  await page.setViewportSize({ width: 900, height: 1000 });
  await page.waitForTimeout(150);
  const responsive = await page.evaluate(() => ({
    pageFits: document.documentElement.scrollWidth <= document.documentElement.clientWidth + 1,
    overflowingTables: [...document.querySelectorAll('.table-wrap, .worker-table-wrap')]
      .filter(node => node.offsetParent !== null && node.scrollWidth > node.clientWidth + 1)
      .map(node => node.querySelector('table')?.id || node.querySelector('table')?.className || 'anonymous'),
  }));
  await browser.close();

  if (errors.length) throw new Error(`browser errors: ${errors.join(' | ')}`);
  if (initial.charts < 8 || initial.sortableHeaders < 40) throw new Error(`render coverage: ${JSON.stringify(initial)}`);
  if (initial.traceRows !== Math.min(8, initial.expectedTraces) || !initial.tracePager?.includes(`${initial.expectedTraces}条`)) throw new Error(`trace pagination: ${JSON.stringify(initial)}`);
  if (!initial.accessLocations.length) throw new Error(`access-location filter: ${JSON.stringify(initial)}`);
  if (!initial.chartTitleCount || initial.nonCenteredChartTitles) throw new Error(`chart title alignment: ${JSON.stringify(initial)}`);
  if (!initial.correlationSingleColumn) throw new Error(`worker correlation charts must be one per row: ${JSON.stringify(initial)}`);
  if (initial.urmaDiagnosisShare !== null && initial.urmaDiagnosisShare < 0.24) throw new Error(`URMA diagnosis column is too narrow: ${JSON.stringify(initial)}`);
  if (initial.latencySegments !== 5 || initial.timelineCount !== initial.expectedTraces) throw new Error(`latency segments initial: ${JSON.stringify(initial)}`);
  if (segmentFiltered.filtered !== segmentFiltered.expected || segmentFiltered.timelineCount !== segmentFiltered.expected || segmentFiltered.active !== '2' || !segmentFiltered.pager?.includes(`${segmentFiltered.expected}条`) || !segmentFiltered.scope?.includes('6–7ms')) {
    throw new Error(`time segment filtering: ${JSON.stringify(segmentFiltered)}`);
  }
  if (JSON.stringify(segmentFiltered.deepDiveCounts) !== JSON.stringify(initial.deepDiveCounts)) throw new Error(`deep dives changed with time segment: ${JSON.stringify({ initial, segmentFiltered })}`);
  if (initial.edgeRows > 8 || initial.nonTransportRows > 8) throw new Error(`table pagination: ${JSON.stringify(initial)}`);
  if (initial.urmaTimeRows > 8 || !initial.urmaTimePager) throw new Error(`URMA time pagination: ${JSON.stringify(initial)}`);
  if (initial.logGroups < 1 || initial.fullLineHighlights !== 0) throw new Error(`log disclosure: ${JSON.stringify(initial)}`);
  const initialUbSafe = initial.correlationUbCanvas === 1 || /未观测[^；]*UB/.test(initial.correlationSummary || '');
  if (!initial.correlationRows || !initialUbSafe || !/1\.5ms|未观测到对应证据/.test(initial.correlationSummary || '')) {
    throw new Error(`worker correlation initial render: ${JSON.stringify(initial)}`);
  }
  const filteredUbSafe = filteredCorrelation.ubCanvas === 1 || /未观测[^；]*UB/.test(filteredCorrelation.summary || '');
  if (filteredCorrelation.traceRows !== initial.traceRows || !filteredCorrelation.rows || !filteredUbSafe) {
    throw new Error(`worker correlation filtering: ${JSON.stringify(filteredCorrelation)}`);
  }
  if (!/1\.5ms|未观测到对应证据/.test(filteredCorrelation.summary || '')) {
    throw new Error(`worker correlation summary: ${JSON.stringify(filteredCorrelation)}`);
  }
  if (correlationTrace && correlationDetailTrace !== correlationTrace) {
    throw new Error(`worker correlation trace linkage: ${correlationTrace} != ${correlationDetailTrace}`);
  }
  if (descending.first !== descending.max || !descending.header?.includes('↓')) throw new Error(`descending sort: ${JSON.stringify(descending)}`);
  if (ascending.first !== ascending.min || !ascending.header?.includes('↑')) throw new Error(`ascending sort: ${JSON.stringify(ascending)}`);
  if (firstTrace !== detailTrace) throw new Error(`trace linkage: ${firstTrace} != ${detailTrace}`);
  if (downloadedTraces !== initial.expectedTraces) throw new Error(`download expected ${initial.expectedTraces} traces, got ${downloadedTraces}`);
  if (!responsive.pageFits || responsive.overflowingTables.length) throw new Error(`responsive table overflow: ${JSON.stringify(responsive)}`);
  console.log(JSON.stringify({ initial, segmentFiltered, filteredCorrelation, descending, ascending, firstTrace, downloadedTraces, responsive }, null, 2));
}

main().catch(error => {
  console.error(error.stack || error);
  process.exit(1);
});
