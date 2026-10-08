const assert=require('assert'),path=require('path'),{pathToFileURL}=require('url');
const {chromium}=require(process.env.DS_PLAYWRIGHT_MODULE||'playwright');
(async()=>{
 const browser=await chromium.launch({headless:true,...(process.env.DS_CHROMIUM_EXECUTABLE?{executablePath:process.env.DS_CHROMIUM_EXECUTABLE}:{})});
 try{for(const file of process.argv.slice(2)){
  const page=await browser.newPage({viewport:{width:1500,height:1000}}),errors=[];
  page.on('pageerror',e=>errors.push(e.message));await page.goto(pathToFileURL(path.resolve(file)).href,{timeout:180000});
  for(const chart of await page.locator('.chart').all())await chart.scrollIntoViewIfNeeded();
  const result=await page.evaluate(()=>{
   const states=['query-rpc-breakdown-chart','rpc-method-chart','rpc-histogram-chart'].map(id=>{
    const chart=echarts.getInstanceByDom(document.getElementById(id)),option=chart?.getOption();
    return{id,positive:option?.series?.some(s=>s.data?.some(v=>Number(v?.value??v)>0)),series:option?.series?.map(s=>s.name)};
   });
   return{states,observations:ROWS.reduce((n,r)=>n+rpcObservations(r).length,0),
    queryWindows:scopeRows().reduce((n,r)=>n+(r.query_and_get_breakdown?.rpc||[]).filter(c=>c.total_ms>0).length,0),
    durations:ROWS.some(r=>rpcObservations(r).some(c=>c.total_ms>0)),health:ReportDiagnostics.audit()};
  });
  if(result.queryWindows)assert(result.states[0].positive,JSON.stringify(result));
  if(result.observations)assert(result.states[1].positive,JSON.stringify(result));
  if(result.durations)assert(result.states[2].positive,JSON.stringify(result));
  assert(result.health.valid,JSON.stringify(result.health));
  for(const width of [900,390]){
   await page.setViewportSize({width,height:1000});await page.waitForTimeout(350);
   assert(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth+2));
   for(const id of ['query-rpc-breakdown-chart','rpc-method-chart','rpc-histogram-chart']){
    const gap=await page.evaluate(id=>{const chart=echarts.getInstanceByDom(document.getElementById(id));if(!chart?.getOption().series?.length)return null;
     const model=chart.getModel(),view=chart.getViewOfComponentModel(model.getComponent('legend')),rect=view.group.getBoundingRect();
     return model.getComponent('grid').coordinateSystem.getRect().y-(rect.y+view.group.y+rect.height);
    },id);assert(gap==null||gap>=0,id+' legend overlaps plot');
   }
  }
  assert.deepEqual(errors,[]);console.log(JSON.stringify({file,...result,errors}));await page.close();
 }}finally{await browser.close()}
})().catch(e=>{console.error(e);process.exit(1)});
