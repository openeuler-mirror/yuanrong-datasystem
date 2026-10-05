#!/usr/bin/env node
const path = require('path');
const { pathToFileURL } = require('url');
const { chromium } = require(process.env.DS_PLAYWRIGHT_MODULE || 'playwright');

(async () => {
  if (!process.argv[2]) throw new Error('usage: check_ds_trace_overview_layout.js <index.html>');
  const browser = await chromium.launch({headless:true, executablePath:process.env.DS_CHROMIUM_EXECUTABLE});
  try {
    const page = await browser.newPage();
    const errors = [], states = [];
    page.on('pageerror', error => errors.push(String(error)));
    await page.goto(pathToFileURL(path.resolve(process.argv[2])).href);
    for (const width of [1500, 1280, 900, 390]) {
      await page.setViewportSize({width, height:1000});
      await page.waitForFunction(() => window.ReportRegistry?.audit({requireComplete:true, checkLayout:true}).valid);
      await page.waitForTimeout(250);
      const state = await page.evaluate(() => {
        const collisions = [], geometry = [];
        const sections = [...document.querySelectorAll('main>section')].map(section => {
          const box = section.getBoundingClientRect(), title = section.querySelector('h2').getBoundingClientRect();
          return {id:section.id, inset:title.left-box.left};
        });
        let checked = 0;
        for (const element of document.querySelectorAll('.chart')) {
          const chart = echarts.getInstanceByDom(element);
          if (!chart) continue;
          const style=getComputedStyle(element),box=element.getBoundingClientRect();
          const contentHeight=element.clientHeight-parseFloat(style.paddingTop)-parseFloat(style.paddingBottom);
          const viewport=chart.getZr().painter.getViewportRoot().getBoundingClientRect();
          const caption=element.nextElementSibling;
          if(Math.abs(contentHeight-chart.getHeight())>1)geometry.push({chart:element.id,
            kind:'canvas-content-height',contentHeight,canvasHeight:chart.getHeight()});
          const axis = chart.getModel().getComponent('xAxis');
          if (!axis || axis.get('type') !== 'value') continue;
          const labels = [];
          chart.getViewOfComponentModel(axis).group.traverse(node => {
            if (node.type !== 'text' || node.ignore || node.invisible) return;
            const bounds = node.getBoundingRect().clone();
            bounds.applyTransform(node.getComputedTransform());
            if(node.style.text===axis.get('name')){
              const bottom=viewport.top+bounds.y+bounds.height;
              const contentBottom=box.bottom-parseFloat(style.borderBottomWidth)-parseFloat(style.paddingBottom);
              if(bottom>contentBottom+1 || (caption&&bottom+4>caption.getBoundingClientRect().top)){
                geometry.push({chart:element.id,kind:'axis-name-caption',bottom,contentBottom,
                  captionTop:caption?.getBoundingClientRect().top});
              }
            }
            if(/^-?[\d.]/.test(node.style.text))labels.push({text:node.style.text, left:bounds.x, right:bounds.x+bounds.width});
          });
          labels.sort((a,b) => a.left-b.left);
          for (let i=1; i<labels.length; i++) {
            if (labels[i].left-labels[i-1].right<4) collisions.push({chart:element.id, labels:labels.slice(i-1,i+1)});
          }
          checked++;
        }
        return {sections, checked, collisions, geometry, overflow:document.documentElement.scrollWidth>innerWidth,
          registry:ReportRegistry.audit({requireComplete:true, checkLayout:true}).valid};
      });
      states.push({width, ...state});
      if (state.overflow || !state.registry || !state.checked || state.collisions.length || state.geometry.length ||
          state.sections.some(section => section.inset<14)) throw new Error(JSON.stringify(states));
    }
    if (errors.length) throw new Error(errors.join('\n'));
    console.log(JSON.stringify({valid:true, states}, null, 2));
  } finally {
    await browser.close();
  }
})().catch(error => { console.error(error); process.exit(1); });
