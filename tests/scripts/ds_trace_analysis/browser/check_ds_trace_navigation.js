const assert=require('assert');
const {pathToFileURL}=require('url');
const path=require('path');
const {chromium}=require(process.env.DS_PLAYWRIGHT_MODULE||'playwright');
(async()=>{
  const browser=await chromium.launch({headless:true,...(process.env.DS_CHROMIUM_EXECUTABLE?{executablePath:process.env.DS_CHROMIUM_EXECUTABLE}:{})});
  try{
    for(const file of process.argv.slice(2))for(const width of [1500,390]){
      const page=await browser.newPage({viewport:{width,height:1000}}),errors=[];
      page.on('pageerror',error=>errors.push(String(error)));
      await page.goto(pathToFileURL(path.resolve(file)).href);
      const missing=await page.evaluate(()=>[...document.querySelectorAll('#nav a[href^="#"],#write-nav a[href^="#"],body>nav a[href^="#"]')].filter(a=>!document.getElementById(a.hash.slice(1))).map(a=>a.hash));
      assert.deepStrictEqual(missing,[]);
      if(path.basename(file)==='triage.html'){
        assert((await page.locator('#nav a[href="#selected-event-timeline"]').innerText()).startsWith('图 6-1'));
        assert((await page.locator('#nav a[href="#selected-trace-chart"]').innerText()).startsWith('图 6-2'));
        assert((await page.locator('#nav a[href="#selected-trace-table"]').innerText()).startsWith('表 6-2'));
      }
      if(!await page.locator('.chapter-toggle').count()){
        assert.strictEqual(await page.locator('nav .sub').count(),0);
        assert.deepStrictEqual(errors,[]);
        console.log(path.basename(file),width,'no child navigation to collapse');
        await page.close();continue;
      }
      assert.strictEqual(await page.locator('[data-auto-sub]:not([hidden])').count(),0);
      const incorrectlyVisible=await page.evaluate(()=>[...document.querySelectorAll('[data-auto-sub][hidden]')].filter(e=>e.getClientRects().length>0).map(e=>e.textContent));
      assert.deepStrictEqual(incorrectlyVisible,[],'collapsed links must be visually hidden, not only carry hidden attribute');
      const headerLayout=await page.evaluate(()=>[...document.querySelectorAll('.chapter-heading')].filter(e=>e.getClientRects().length).every(e=>getComputedStyle(e).display==='flex'));
      assert(headerLayout,'chapter label and toggle must share one row');
      await page.setViewportSize({width:width-10,height:950});
      await page.waitForTimeout(200);
      assert.strictEqual(await page.locator('[data-auto-sub]:not([hidden])').count(),0);
      await page.evaluate(()=>{
        const headers=[...document.querySelectorAll('.chapter-heading')];
        const heading=headers[Math.min(2,headers.length-1)];
        const link=heading.querySelector('a');
        window.expectedNavGroup=heading.querySelector('button').dataset.navToggle;
        window.expectedNavTarget=link.hash.slice(1);
        document.getElementById(link.hash.slice(1)).scrollIntoView();
      });
      await page.waitForFunction(()=>{
        const visible=[...document.querySelectorAll('[data-auto-sub]:not([hidden])')];
        return visible.length>0&&visible.every(link=>link.dataset.navGroup===window.expectedNavGroup);
      });
      await page.waitForTimeout(600);
      const position=await page.evaluate(()=>({target:document.getElementById(window.expectedNavTarget).getBoundingClientRect().top,bar:document.getElementById('report-switcher')?.getBoundingClientRect().bottom||0}));
      assert(position.target>=position.bar-2,JSON.stringify(position));
      if(path.basename(file)==='triage.html'){
        await page.evaluate(()=>{
          document.documentElement.style.scrollBehavior='auto';
          const target=document.getElementById('selected-event-timeline');
          const threshold=(document.getElementById('report-switcher')?.getBoundingClientRect().bottom||0)+40;
          scrollTo(0,scrollY+target.getBoundingClientRect().top-threshold+5);
        });
        await page.waitForFunction(()=>document.querySelector('#nav a.active')?.hash==='#selected-event-timeline');
        assert.strictEqual(await page.locator('#nav a.active').getAttribute('aria-current'),'location');
        assert.strictEqual(await page.locator('#nav a.active').getAttribute('hidden'),null);
      }
      assert.deepStrictEqual(errors,[]);
      console.log(path.basename(file),width,'initial/resize collapsed; scroll expands current chapter');
      await page.close();
    }
  }finally{await browser.close()}
})().catch(error=>{console.error(error);process.exit(1)});
