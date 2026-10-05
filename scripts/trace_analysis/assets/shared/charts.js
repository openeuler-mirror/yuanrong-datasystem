/* Shared by offline Trace, read/write bottleneck and NUMA reports. */
var TraceCharts = (() => {
  const palette = Object.freeze({urma:'#f59e0b',network:'#2563eb',timeout:'#b42318',
    copy:'#0ea5a4',scheduling:'#c026d3',framework:'#64748b'});
  const labels = Object.freeze({
    '写入URMA通信':'URMA 通信耗时','URMA通信':'URMA 通信耗时',
    'RPC网络相关':'RPC 网络耗时','RPC网络':'RPC 网络耗时','RPC网络/通信':'RPC 网络耗时',
    '写入MemoryCopy':'MemoryCopy 耗时','MemoryCopy':'MemoryCopy 耗时',
    '写入URMA调度/线程开销':'URMA 调度/线程耗时','URMA调度/线程开销':'URMA 调度/线程耗时',
    'RPC框架':'RPC 框架耗时','RPC框架计时':'RPC 框架耗时',
    '其他调度/线程开销':'其他调度/线程耗时',
    '未解释残差':'Client 未细分耗时','Client未细分窗口':'Client 未细分耗时',
    'Get其他业务':'Get 其他业务耗时','QueryAndGet其他业务':'QueryAndGet 其他业务耗时',
    'Create RPC其他':'Create RPC 其他耗时','Publish RPC其他':'Publish RPC 其他耗时'
  });
  const label = name => labels[name] || name;
  function color(name) {
    const n=String(name || '');
    if (/^(?:read\.data_worker_ub_write|URMA Write|client\.urma\.ub_transfer)$/.test(n)) return palette.urma;
    if (n==='write.client_memory_copy') return palette.copy;
    if (/^(?:成功WR (?:P90|max)|total p90|completion wait p90|wait p90)$/i.test(n)) return palette.urma;
    if (/URMA.*(?:超时|timeout)/i.test(n)) return palette.timeout;
    if (/RPC.*网络|RPC.*residual|network_residual/i.test(n)) return palette.network;
    if (/URMA.*(?:通信|completion|wait|p50|p90|耗时)|^(?:写入)?URMA$/i.test(n)) return palette.urma;
    if (/Memory[ _]?Copy|memcpy|内存拷贝/i.test(n)) return palette.copy;
    if (/URMA.*调度/.test(n)) return palette.scheduling;
    if (/RPC框架/.test(n)) return palette.framework;
    return null;
  }
  const list = v => Array.isArray(v) ? v : v ? [v] : [];
  const distinctPalette = Object.freeze([
    '#2563eb','#f59e0b','#16a34a','#dc3545','#7c5ce7','#0891b2',
    '#ea580c','#4f46e5','#65a30d','#be185d','#0f766e','#9333ea'
  ]);
  const escape = v => String(label(v) ?? '').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
  const number = v => {
    if (v && typeof v === 'object' && !Array.isArray(v)) v=v.value;
    return typeof v==='number' && Number.isFinite(v) ? v : null;
  };
  function uniqueColor(preferred, used) {
    let result=preferred;
    if(!result || used.has(String(result).toLowerCase())) {
      result=distinctPalette.find(c=>!used.has(c.toLowerCase()));
      if(!result) result=`hsl(${(used.size*137.508)%360},65%,42%)`;
    }
    used.add(String(result).toLowerCase());
    return result;
  }
  function legendLayout(input, width=900, height=400) {
    const series=list(input.series), legends=list(input.legend);
    let requiredTop=0,requiredBottom=0;
    const pixels=(v,total,fallback)=>typeof v==='number'?v:
      typeof v==='string' && v.endsWith('%')?parseFloat(v)*total/100:fallback;
    const legend=legends.map(l=>{
      const formatter=name=>label(typeof l.formatter==='function'?l.formatter(name):
        typeof l.formatter==='string'?l.formatter.replace('{name}',name):name);
      if(l.orient==='vertical' || l.show===false) return {...l,formatter};
      const names=l.data || [...new Set(series.flatMap(s=>s.type==='pie'?
        (s.data||[]).map(d=>d.name):s.name?[s.name]:[]))];
      const left=pixels(l.left,width,12),right=pixels(l.right,width,12);
      const available=Math.max(80,Math.min(width-left-right,pixels(l.width,width,width-left-right)));
      const fontSize=l.textStyle?.fontSize || 12,gap=l.itemGap ?? 12;
      let used=0,rows=1;
      const data=[];
      for(const item of names) {
        const name=typeof item==='string'?item:item.name;
        if(name==='' || name==='\n') continue;
        const text=String(formatter(name));
        const textWidth=[...text].reduce((n,c)=>n+(c.charCodeAt(0)>255?fontSize:fontSize*.65),0);
        const itemWidth=Math.min(available,textWidth+(l.itemWidth||25)+16+gap);
        if(used && used+itemWidth>available) {rows++;used=0;}
        data.push(item);used+=itemWidth;
      }
      const space=rows*(Math.max(fontSize,l.itemHeight||14)+gap)+16;
      const bottom=l.bottom!=null && l.top==null;
      if(bottom) requiredBottom=Math.max(requiredBottom,pixels(l.bottom,height,0)+space);
      else requiredTop=Math.max(requiredTop,pixels(l.top,height,0)+space);
      return {...l,type:'plain',orient:'horizontal',left,right,width:available,data,
        textStyle:{...l.textStyle,fontFamily:'Microsoft YaHei',width:Math.max(40,available-50),overflow:'break'},
        itemGap:gap,formatter};
    });
    const result={legend};
    if(input.grid) result.grid=list(input.grid).map(g=>({...g,
      ...(requiredTop?{top:Math.max(pixels(g.top,height,60),requiredTop+(list(input.yAxis).some(a=>a.name)?24:0))}:{}),
      ...(requiredBottom?{bottom:Math.max(pixels(g.bottom,height,60),requiredBottom)}:{})}));
    return result;
  }
  const chartText={fontFamily:'Microsoft YaHei',fontSize:12,fontStyle:'normal'};
  const textStyle=value=>({...value,...chartText});
  function typography(input) {
    const out={...input,textStyle:textStyle(input.textStyle)};
    const map=(key,fn)=>{if(input[key])out[key]=Array.isArray(input[key])?input[key].map(fn):fn(input[key]);};
    for(const key of ['legend','tooltip','dataZoom','title'])map(key,item=>({...item,textStyle:textStyle(item.textStyle),subtextStyle:textStyle(item.subtextStyle)}));
    for(const key of ['xAxis','yAxis'])map(key,axis=>({...axis,axisLabel:textStyle(axis.axisLabel),nameTextStyle:textStyle(axis.nameTextStyle),axisPointer:{...axis.axisPointer,label:textStyle(axis.axisPointer?.label)}}));
    map('series',series=>{
      const result={...series,label:textStyle(series.label),endLabel:textStyle(series.endLabel),emphasis:{...series.emphasis,label:textStyle(series.emphasis?.label)}};
      for(const key of ['markLine','markPoint','markArea'])if(series[key])result[key]={...series[key],label:textStyle(series[key].label)};
      return result;
    });
    return out;
  }
  function option(input, selection=()=>({}), width=900, height=400) {
    input=typography(input);
    const category=list(input.xAxis).find(a=>a.type==='category') || list(input.yAxis).find(a=>a.type==='category');
    const usedColors=new Set(),namedColors=new Map();
    const series=list(input.series).map((s,index)=>{
      const c=color(s.name),next={...s};
      if(s.markLine) next.markLine={...s.markLine,label:{position:'insideEndTop',...s.markLine.label}};
      const requested=c || s.itemStyle?.color || s.lineStyle?.color || list(input.color)[index];
      const seriesColor=namedColors.get(s.name) || uniqueColor(requested,usedColors);
      if(s.name) namedColors.set(s.name,seriesColor);
      next.itemStyle={...s.itemStyle,color:seriesColor};
      next.lineStyle={...s.lineStyle,color:seriesColor};
      if(c===palette.urma && s.type==='line') {
        const variant=/max|最大/i.test(s.name)?'max':/wait|等待/i.test(s.name)?'wait':'total';
        next.lineStyle={...next.lineStyle,type:variant==='max'?'dotted':variant==='wait'?'dashed':'solid'};
        next.symbol=variant==='max'?'triangle':variant==='wait'?'rect':'circle';
      }
      if(Array.isArray(s.data)) {
        const dataColors=new Set();
        next.data=s.data.map((d,i)=>{
          if(d===null) return d;
          let dc;
          if(s.type==='pie') dc=uniqueColor(color(d?.name)||d?.itemStyle?.color||list(input.color)[i],dataColors);
          else if(s.name) dc=seriesColor;
          else dc=color(d?.name || d?.rawStage || d?.display) || (s.type==='bar'?color(category?.data?.[i]):null);
          return dc ? {...(d!==null && typeof d==='object' && !Array.isArray(d) ? d : {value:d}),itemStyle:{...d?.itemStyle,color:dc}} : d;
        });
      }
      if(s.type==='bar' && s.stack) next.emphasis={...s.emphasis,focus:'series',itemStyle:{...s.emphasis?.itemStyle,borderColor:'#172033',borderWidth:1}};
      return next;
    });
    const output={...input,series};
    if(input.legend) Object.assign(output,legendLayout(input,width,height));
    for(const key of ['xAxis','yAxis']) if(input[key]) output[key]=list(input[key]).map(axis=>{
      if(axis.type!=='category') return axis;
      const original=axis.axisLabel||{}, format=(name,index)=>labels[name] ||
        (typeof original.formatter==='function'?original.formatter(name,index):
          typeof original.formatter==='string'?original.formatter.replace('{value}',name):name);
      const data=axis.data||[], horizontal=key==='xAxis', fontSize=original.fontSize||12;
      const lengths=data.map((item,index)=>String(format(item?.value??item,index)).split('\n')
        .reduce((max,line)=>Math.max(max,[...line].reduce((n,c)=>n+(c.charCodeAt(0)>255?fontSize:fontSize*.65),0)),0));
      const maxWidth=Math.min(160,lengths.reduce((a,b)=>Math.max(a,b),0)), slot=Math.max(1,(width-120)/Math.max(1,data.length));
      const rotate=horizontal?Math.max(original.rotate||0,maxWidth+12>slot?45:0):(original.rotate||0);
      if(horizontal && input.grid) {
        const zoom=list(input.dataZoom).some(z=>z.type==='slider')?38:0;
        const bottom=Math.ceil(Math.sin(rotate*Math.PI/180)*maxWidth+fontSize*2+zoom+12);
        output.grid=list(output.grid||input.grid).map(g=>({...g,containLabel:false,
          bottom:Math.max(typeof g.bottom==='number'?g.bottom:0,bottom)}));
      }
      return {...axis,axisLabel:{...original,fontFamily:'Microsoft YaHei',fontStyle:'normal',rotate,
        interval:horizontal?'auto':original.interval,hideOverlap:true,width:horizontal?160:original.width,
        overflow:'truncate',margin:12,formatter:format}};
    });
    for(const axis of list(input.xAxis)) {
      if(!axis.name || ['middle','center'].includes(axis.nameLocation)) continue;
      const fontSize=axis.nameTextStyle?.fontSize||12;
      const nameWidth=Math.max(...String(axis.name).split('\n').map(line=>[...line].reduce((n,c)=>n+(c.charCodeAt(0)>255?fontSize:fontSize*.65),0)));
      const side=((axis.nameLocation==='start')!==Boolean(axis.inverse))?'left':'right';
      const required=Math.ceil(nameWidth+(axis.nameGap??15)+12);
      output.grid=list(output.grid||{}).map((grid,index)=>{
        if(index!==(axis.gridIndex||0))return grid;
        const margin=grid[side]??'10%';
        const pixels=typeof margin==='string'&&margin.endsWith('%')?parseFloat(margin)*width/100:Number(margin)||0;
        return {...grid,[side]:Math.max(pixels,required)};
      });
    }
    for(const axis of list(input.yAxis)) {
      if(!axis.name || axis.show===false || ['middle','center'].includes(axis.nameLocation)) continue;
      const fontSize=axis.nameTextStyle?.fontSize||12;
      const textHeight=String(axis.name).split('\n').length*(axis.nameTextStyle?.lineHeight||fontSize);
      const side=((axis.nameLocation==='start')!==Boolean(axis.inverse))?'bottom':'top';
      const required=Math.ceil(textHeight+(axis.nameGap??15)+12);
      output.grid=list(output.grid||{}).map((grid,index)=>{
        if(index!==(axis.gridIndex||0))return grid;
        const margin=grid[side]??60;
        const pixels=typeof margin==='string'&&margin.endsWith('%')?parseFloat(margin)*height/100:Number(margin)||0;
        return {...grid,[side]:Math.max(pixels,required)};
      });
    }
    const gridPixels=value=>typeof value==='string'&&value.endsWith('%')?
      parseFloat(value)*width/100:Number(value)||0;
    const occupiedYAxisSides=new Map();
    for(const axis of list(output.yAxis)) {
      const gridIndex=axis.gridIndex||0,occupied=occupiedYAxisSides.get(gridIndex)||new Set();
      const side=axis.position||(occupied.has('left')?'right':'left');
      occupied.add(side);occupiedYAxisSides.set(gridIndex,occupied);
      if(axis.type!=='value'||axis.show===false||axis.axisLabel?.show===false||axis.axisLabel?.inside) continue;
      output.grid=list(output.grid||{}).map((grid,index)=>index!==(axis.gridIndex||0)?grid:
        {...grid,[side]:Math.max(gridPixels(grid[side]??'10%'),64)});
    }
    if(output.xAxis) output.xAxis=list(output.xAxis).map(axis=>{
      if(axis.type!=='value') return axis;
      const grid=list(output.grid)[axis.gridIndex||0]||{};
      const available=grid.width!=null?gridPixels(grid.width):
        width-gridPixels(grid.left??'10%')-gridPixels(grid.right??'10%');
      const splits=Math.max(1,Math.floor(available/80));
      return {...axis,splitNumber:Math.min(axis.splitNumber??5,splits),
        axisLabel:{...axis.axisLabel,hideOverlap:true}};
    });
    if(list(input.dataZoom).some(z=>z.type==='slider') && output.grid) {
      output.grid=list(output.grid).map(g=>({...g,bottom:Math.max(typeof g.bottom==='number'?g.bottom:0,90)}));
    }
    for(const s of series) if(s.type==='pie' && s.data?.some(d=>labels[d.name])) {
      s.label={...s.label,formatter:p=>`${label(p.name)}\n${p.percent}%`};
      s.tooltip={...s.tooltip,formatter:p=>`${escape(p.name)}：${p.value} ms (${p.percent}%)`};
    }
    output.textStyle={...output.textStyle,fontFamily:'Microsoft YaHei'};
    for(const key of ['title','tooltip','legend']) if(output[key]) {
      const apply=item=>({...item,textStyle:{...item.textStyle,fontFamily:'Microsoft YaHei'},subtextStyle:{...item.subtextStyle,fontFamily:'Microsoft YaHei'}});
      output[key]=Array.isArray(output[key])?output[key].map(apply):apply(output[key]);
    }
    for(const key of ['xAxis','yAxis']) if(output[key]) output[key]=list(output[key]).map(axis=>({...axis,nameTextStyle:{...axis.nameTextStyle,fontFamily:'Microsoft YaHei'}}));
    output.series=output.series.map(series=>({...series,label:{...series.label,fontFamily:'Microsoft YaHei'}}));
    if(!series.some(s=>s.type==='bar' && s.stack)) return output;
    const prior=input.tooltip || {};
    output.tooltip={...prior,trigger:'item',confine:true,enterable:true,extraCssText:'max-width:520px;max-height:360px;overflow:auto;white-space:normal;',formatter:p=>{
      const s=series[p.seriesIndex];
      if(!s) return '';
      const selected=selection(),value=number(p.value);
      const peers=series.map((x,i)=>({x,i})).filter(({x})=>x.type==='bar' && x.stack===s.stack &&
        (x.xAxisIndex||0)===(s.xAxisIndex||0) && (x.yAxisIndex||0)===(s.yAxisIndex||0) && selected[x.name]!==false);
      const total=peers.reduce((sum,{x})=>sum+Math.max(0,number(x.data?.[p.dataIndex])||0),0);
      const x=list(input.xAxis)[s.xAxisIndex||0],y=list(input.yAxis)[s.yAxisIndex||0];
      const unit=(x?.type==='value' ? x.name : y?.name) || '';
      const percent=s.type==='bar' && s.stack && value!==null && value>=0 && total>0 ? ` · 可见分段合计占比 ${(100*value/total).toFixed(1)}%` : '';
      const formatted=value===null?'未观测':/(ms|耗时|时延)/i.test(unit)?value.toFixed(3):value.toLocaleString('en-US',{maximumFractionDigits:3});
      const header=`<b>${escape(p.name)}</b><br>${escape(p.seriesName)}：<b>${formatted} ${escape(unit)}</b>${percent}`;
      let context='';
      if(typeof prior.formatter==='function') {
        const ps=series.map((item,i)=>({...p,seriesIndex:i,seriesName:item.name,data:item.data?.[p.dataIndex],value:number(item.data?.[p.dataIndex])}));
        context=prior.formatter(prior.trigger==='axis'?ps:p) || '';
      }
      return header+(context?'<hr>'+context:'');
    }};
    return output;
  }
  function layoutHeight(config) {
    const grids=list(config.grid);
    if(!grids.length) return 400;
    const pixels=value=>typeof value==='number'?value:0;
    return Math.max(360,...grids.map((g,index)=>{
      const axes=list(config.yAxis).filter(a=>(a.gridIndex||0)===index && a.type==='category');
      const rows=axes.reduce((n,a)=>Math.max(n,a.data?.length||0),0);
      return pixels(g.top)+pixels(g.bottom)+Math.max(260,rows*32)+24;
    }));
  }
  function init(library,...args) {
    const chart=library.init(...args),setOption=chart.setOption.bind(chart),clear=chart.clear.bind(chart);
    const resize=chart.resize?.bind(chart),node=chart.getDom?.();
    let lastInput=null;
    let selected={};
    const layout=input=>{
      let config=option(input,()=>selected,chart.getWidth?.()||900,chart.getHeight?.()||400);
      if(node?.style && resize) {
        const height=layoutHeight(config);
        if(Math.abs((chart.getHeight?.()||0)-height)>1) {
          const css=typeof getComputedStyle==='function'?getComputedStyle(node):null;
          const inset=css?.boxSizing==='border-box'?['paddingTop','paddingBottom','borderTopWidth','borderBottomWidth'].reduce((sum,key)=>sum+(parseFloat(css[key])||0),0):0;
          node.style.height=(height+inset)+'px';
          resize({height});
          config=option(input,()=>selected,chart.getWidth?.()||900,height);
        }
      }
      return config;
    };
    chart.clear=()=>{selected={};lastInput=null;return clear();};
    chart.on('legendselectchanged',event=>{selected=event.selected || {};});
    chart.setOption=(input,...rest)=>{
      for(const legend of list(input.legend)) Object.assign(selected,legend.selected || {});
      const replace=rest[0]===true || rest[0]?.notMerge;
      if(lastInput && !replace && Object.keys(input).every(key=>key==='title')) {
        lastInput={...lastInput,title:input.title};
      } else lastInput=input;
      try {
        const result=setOption(layout(lastInput),...rest);
        if(node?.id && typeof ReportRegistry!=='undefined')ReportRegistry.chart(node.id,chart.getOption?.()||lastInput);
        return result;
      } catch(error) {
        if(node?.id && typeof ReportRegistry!=='undefined')ReportRegistry.record(node.id,{state:'error',reason:'echarts_exception',message:String(error)});
        throw error;
      }
    };
    if(resize) chart.resize=(...rest)=>{
      if(chart.isDisposed?.()) return;
      const result=resize(...rest);
      if(lastInput) setOption(layout(lastInput));
      return result;
    };
    return chart;
  }
  return {palette,color,label,option,layoutHeight,init};
})();
