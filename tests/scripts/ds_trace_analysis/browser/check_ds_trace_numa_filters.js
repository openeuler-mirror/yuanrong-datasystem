/* Usage: node check_ds_trace_numa_filters.js numa.html */
const assert=require('assert');
const path=require('path');
const {pathToFileURL}=require('url');
const {chromium}=require(process.env.DS_PLAYWRIGHT_MODULE||'playwright');
(async()=>{
  const browser=await chromium.launch({headless:true,...(process.env.DS_CHROMIUM_EXECUTABLE?{executablePath:process.env.DS_CHROMIUM_EXECUTABLE}:{})});
  try{
    const page=await browser.newPage(),errors=[];
    page.on('pageerror',e=>errors.push(String(e)));
    await page.goto(pathToFileURL(path.resolve(process.argv[2])).href);
    assert(await page.locator('#trace-table tbody tr').count()>0,'fixture needs a Trace');
    const before=await page.locator('#detail-summary').innerText();
    const minimum=await page.evaluate(()=>Math.max(0,...DATA.traces.map(r=>r.client_ms||0))+1);
    await page.fill('#f-min',String(minimum));
    await page.locator('#f-min').dispatchEvent('change');
    assert.strictEqual(await page.locator('#trace-table tbody tr').count(),0);
    assert.strictEqual(await page.locator('#detail-summary').innerText(),'无匹配 Trace');
    assert.strictEqual(await page.locator('#detail-log').innerText(),'');
    await page.fill('#f-min','');
    await page.locator('#f-min').dispatchEvent('change');
    assert(await page.locator('#trace-table tbody tr').count()>0);
    assert.strictEqual(await page.locator('#detail-summary').innerText(),before);
    assert.deepStrictEqual(errors,[]);
  }finally{await browser.close()}
})().catch(error=>{console.error(error);process.exit(1)});
