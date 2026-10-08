'use strict';

const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const {pathToFileURL} = require('node:url');
const {chromium} = require(process.env.DS_PLAYWRIGHT_MODULE || 'playwright');

async function main() {
  const files = process.argv.slice(2);
  assert(files.length, 'Usage: node check_ds_trace_detail_layout.js report.html [report.html ...]');
  const browser = await chromium.launch({headless: true,
    ...(process.env.DS_CHROMIUM_EXECUTABLE ? {executablePath: process.env.DS_CHROMIUM_EXECUTABLE} : {})});
  const results = [];
  try {
    for (const file of files) {
      assert(fs.existsSync(file), `Missing report: ${file}`);
      const page = await browser.newPage();
      const errors = [];
      page.on('pageerror', error => errors.push(String(error)));
      await page.goto(pathToFileURL(path.resolve(file)).href, {timeout: 120000});
      await page.evaluate(() => { document.documentElement.style.scrollBehavior = 'auto'; });
      for (const width of [1920, 1500, 1280, 900, 390]) {
        await page.setViewportSize({width, height: 1000});
        for (const chart of await page.locator('.chart').all()) {
          if (await chart.isVisible()) await chart.scrollIntoViewIfNeeded();
        }
        await page.waitForFunction(() => ReportRegistry.audit({requireComplete: true, checkLayout: true}).valid,
          {}, {timeout: 20000});
        await page.evaluate(() => new Promise(resolve => requestAnimationFrame(() => requestAnimationFrame(resolve))));
        await page.waitForFunction(() => [...document.querySelectorAll('.chart')].every(element => {
          const chart = echarts.getInstanceByDom(element);
          if (!chart || !element.getClientRects().length) return true;
          const style = getComputedStyle(element);
          const contentWidth = element.clientWidth - parseFloat(style.paddingLeft) - parseFloat(style.paddingRight);
          return Math.abs(chart.getWidth() - contentWidth) <= 1;
        }), {}, {timeout: 20000});
        const state = await page.evaluate(() => {
          const issues = [], axes = [], sections = [];
          if (document.documentElement.scrollWidth > window.innerWidth + 2) {
            issues.push({kind: 'page-horizontal-overflow',
              viewportWidth: window.innerWidth, scrollWidth: document.documentElement.scrollWidth});
          }
          for (const element of document.querySelectorAll('.chart')) {
            const chart = echarts.getInstanceByDom(element);
            if (!chart || !element.getClientRects().length) continue;
            for (const dimension of ['xAxis', 'yAxis']) {
              chart.getModel().eachComponent(dimension, axis => {
                const numeric=axis.get('type')==='value';
                const id = `${element.id}/${dimension}/${axis.componentIndex}`;
                const labels = [];
                chart.getViewOfComponentModel(axis).group.traverse(node => {
                  if (node.type !== 'text') return;
                  const isName=node.style.text===axis.get('name');
                  const isNumber=numeric&&/^[-−]?[\d,.]+$/.test(String(node.style.text));
                  if(!isName&&!isNumber)return;
                  for (let ancestor = node; ancestor; ancestor = ancestor.parent) {
                    if (ancestor.ignore || ancestor.invisible) return;
                  }
                  const bounds = node.getBoundingRect().clone();
                  bounds.applyTransform(node.getComputedTransform());
                  const label={text: node.style.text, x: bounds.x, y: bounds.y,
                    width: bounds.width, height: bounds.height};
                  if(isName&&(bounds.x < -1 || bounds.y < -1 || bounds.x+bounds.width>chart.getWidth()+1 ||
                    bounds.y+bounds.height>chart.getHeight()+1))issues.push({id,kind:'clipped-axis-name',label});
                  if(isNumber)labels.push(label);
                });
                labels.sort((a, b) => dimension === 'xAxis' ? a.x - b.x : a.y - b.y);
                for (const label of labels) {
                  if (label.x < -1 || label.y < -1 || label.x + label.width > chart.getWidth() + 1 ||
                      label.y + label.height > chart.getHeight() + 1) {
                    issues.push({id, kind: 'clipped-label', label});
                  }
                }
                for (let index = 1; index < labels.length; index++) {
                  const previous = labels[index - 1], current = labels[index];
                  const gap = dimension === 'xAxis' ? current.x - previous.x - previous.width :
                    current.y - previous.y - previous.height;
                  if (gap < 4) issues.push({id, kind: 'crowded-labels', gap, previous, current});
                }
                if(numeric)axes.push({id, labels});
              });
            }
          }
          const selector = window.REPORT_COMPONENT_REGISTRY?.page_kind === 'numa' ? '#errors, #chips' :
            '.report-triage main > section';
          for (const section of document.querySelectorAll(selector)) {
            const heading = section.querySelector('h2');
            if (!heading) continue;
            const inset = heading.getBoundingClientRect().left - section.getBoundingClientRect().left;
            sections.push({id: section.id, inset});
            if (inset < 14) issues.push({id: section.id, kind: 'chapter-inset', inset});
          }
          for (const id of ['wr-time-table', 'wr-worker-table', 'wr-events-table']) {
            const container = document.getElementById(id);
            const table = container?.querySelector('table');
            if (!table) continue;
            if (id === 'wr-events-table' && container.clientWidth <= 1400 &&
                !container.querySelector('.wr-table-sort')?.getClientRects().length) {
              issues.push({id, kind: 'missing-responsive-sort'});
            }
            if (id !== 'wr-events-table' && (
                getComputedStyle(table).display !== 'table' ||
                getComputedStyle(table.tHead).display !== 'table-header-group' ||
                table.tHead.rows[0].cells.length !== 6 ||
                (table.tBodies[0].rows.length &&
                  getComputedStyle(table.tBodies[0].rows[0]).display !== 'table-row'))) {
              issues.push({id, kind: 'missing-traditional-table'});
            }
            if (container.scrollWidth > container.clientWidth + 1 ||
                table.getBoundingClientRect().right > container.getBoundingClientRect().right + 1) {
              issues.push({id, kind: 'horizontal-table-overflow',
                containerWidth: container.clientWidth, tableWidth: table.getBoundingClientRect().width});
            }
            const cells = [...table.querySelectorAll('tbody tr:first-child td')]
              .filter(cell => cell.getClientRects().length).map(cell => cell.getBoundingClientRect());
            for (let index = 0; index < cells.length; index++) {
              if (cells.slice(index + 1).some(other => cells[index].left < other.right &&
                  other.left < cells[index].right && cells[index].top < other.bottom &&
                  other.top < cells[index].bottom)) {
                issues.push({id, kind: 'overlapping-table-cells'});
                break;
              }
            }
          }
          if (['read', 'bottleneck', 'numa'].includes(window.REPORT_COMPONENT_REGISTRY?.page_kind) && !axes.length) {
            issues.push({kind: 'missing-value-axes'});
          }
          return {issues, axes, sections,
            registry: ReportRegistry.audit({requireComplete: true, checkLayout: true})};
        });
        results.push({file, width, ...state, errors: [...errors]});
      }
      if (await page.locator('#wr-time-table table').count()) {
        await page.setViewportSize({width: 900, height: 1000});
        for (const id of ['wr-time-table', 'wr-worker-table']) {
          const header = page.locator(`#${id} th[data-key="count"]`);
          await header.click();
          assert.equal((await page.evaluate(tableId => tableState[tableId], id)).asc, false);
          await header.click();
          const state = await page.evaluate(tableId => tableState[tableId], id);
          assert.equal(state.key, 'count');
          assert.equal(state.asc, true);
          assert.equal(await header.getAttribute('aria-sort'), 'ascending');
          const counts = (await page.locator(`#${id} tbody td:nth-child(2)`).allTextContents())
            .map(Number);
          assert.deepEqual(counts, [...counts].sort((a, b) => a - b));
        }
        for (const [id, key] of [['wr-events-table', 'total_ms']]) {
          await page.locator(`#${id} .sort-key`).selectOption(key);
          assert.equal(await page.locator(`#${id} .sort-key`).inputValue(), key);
          await page.locator(`#${id} .sort-direction`).click();
          const state = await page.evaluate(tableId => tableState[tableId], id);
          assert.equal(state.key, key);
          assert.equal(state.asc, true);
        }
        await page.evaluate(() => {
          const host = document.createElement('div');
          host.id = 'wr-sort-fixture';
          document.body.append(host);
          table(host.id, [{n: null}, {n: 10}, {n: 2}, {n: 1}],
            [{key: 'n', title: 'P90 ms', value: row => row.n,
              render: row => row.n == null ? '未观测' : String(row.n)}]);
        });
        const fixtureHeader = page.locator('#wr-sort-fixture th[data-key="n"]');
        const fixtureValues = () => page.locator('#wr-sort-fixture tbody td').allTextContents();
        await fixtureHeader.click();
        assert.deepEqual(await fixtureValues(), ['10', '2', '1', '未观测']);
        await fixtureHeader.click();
        assert.deepEqual(await fixtureValues(), ['1', '2', '10', '未观测']);
      }
      await page.close();
    }
    console.log(JSON.stringify(results, null, 2));
    assert(results.every(result => result.registry.valid && !result.issues.length && !result.errors.length),
      'Detail layout validation failed; inspect the JSON diagnostics above');
  } finally {
    await browser.close();
  }
}

main().catch(error => { console.error(error); process.exitCode = 1; });
