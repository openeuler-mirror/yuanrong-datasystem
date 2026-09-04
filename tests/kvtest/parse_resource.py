#!/usr/bin/env python3
"""Generate a self-contained interactive HTML report from procmon CSV."""

import argparse
import csv
import datetime
import html
import json
import os
import sys


METRICS = {
    'cpu_pct': ('CPU', '%', 'cpu'),
    'rss_mb': ('RSS', ' MB', 'memory'),
    'anon_mb': ('Anon', ' MB', 'memory'),
    'shared_mb': ('Shared', ' MB', 'memory'),
    'fd': ('File descriptors', '', 'fd'),
    'tcp_fails_per_sec': ('TCP failures/s', '/s', 'tcp'),
    'tcp_fail_rate_pct': ('TCP fail rate', '%', 'cpu'),
    'bytes_in_mb_per_sec': ('Bytes in', ' MB/s', 'network'),
    'bytes_out_mb_per_sec': ('Bytes out', ' MB/s', 'network'),
    'jemalloc_allocated_mb': ('Jemalloc allocated', ' MB', 'jemalloc'),
    'jemalloc_active_mb': ('Jemalloc active', ' MB', 'jemalloc'),
    'jemalloc_resident_mb': ('Jemalloc resident', ' MB', 'jemalloc'),
    'jemalloc_metadata_mb': ('Jemalloc metadata', ' MB', 'jemalloc'),
    'jemalloc_mapped_mb': ('Jemalloc mapped', ' MB', 'jemalloc'),
    'jemalloc_retained_mb': ('Jemalloc retained', ' MB', 'jemalloc'),
    'jemalloc_dirty_mb': ('Jemalloc dirty', ' MB', 'jemalloc'),
    'jemalloc_muzzy_mb': ('Jemalloc muzzy', ' MB', 'jemalloc'),
    'jemalloc_stats_available': ('Jemalloc stats available', '', 'status'),
    'jemalloc_stats_read_failures': ('Jemalloc read failures', '', 'status'),
}

CHARTS = {
    'cpu': 'CPU and failure rate',
    'memory': 'Process memory',
    'jemalloc': 'Jemalloc memory',
    'network': 'Network throughput',
    'fd': 'File descriptors',
    'tcp': 'TCP failures',
    'status': 'Jemalloc status',
}


def _epoch_ms(value, path, line_number):
    try:
        parsed = datetime.datetime.fromisoformat(value)
    except ValueError as error:
        raise ValueError(
            f'{path}:{line_number}: invalid timestamp {value!r}') from error
    if parsed.tzinfo is None:
        parsed = parsed.astimezone()
    return int(parsed.timestamp() * 1000)


def load_resource_csv(path):
    """Load one resource_monitor.csv and discard columns with no values."""
    rows = []
    with open(path, newline='', encoding='utf-8-sig') as stream:
        reader = csv.DictReader(stream)
        if not reader.fieldnames or 'timestamp' not in reader.fieldnames:
            raise ValueError(f'{path}: missing timestamp CSV column')
        for line_number, source in enumerate(reader, 2):
            timestamp = (source.get('timestamp') or '').strip()
            if not timestamp:
                continue
            row = {'timestamp': timestamp,
                   'epoch_ms': _epoch_ms(timestamp, path, line_number)}
            pid = (source.get('pid') or '').strip()
            if pid:
                try:
                    row['pid'] = int(pid)
                except ValueError as error:
                    raise ValueError(
                        f'{path}:{line_number}: invalid pid {pid!r}') from error
            for key in METRICS:
                raw_value = (source.get(key) or '').strip()
                if not raw_value:
                    continue
                try:
                    row[key] = float(raw_value)
                except ValueError as error:
                    raise ValueError(
                        f'{path}:{line_number}: invalid {key} {raw_value!r}') from error
            rows.append(row)
    if not rows:
        raise ValueError(f'{path}: no resource samples found')
    metrics = [key for key in METRICS if any(key in row for row in rows)]
    return {'path': os.path.abspath(path), 'rows': rows, 'metrics': metrics}


def _chart_config(metrics):
    charts = []
    for group, title in CHARTS.items():
        keys = [key for key in metrics if METRICS[key][2] == group]
        if keys:
            charts.append({'title': title, 'metrics': [
                {'key': key, 'label': METRICS[key][0], 'unit': METRICS[key][1]}
                for key in keys]})
    return charts


def render_html(data, source_name=None, title='Resource monitor report'):
    """Render a standalone report with nearest-sample hover and click lock."""
    payload = json.dumps({
        'source': source_name or data['path'],
        'rows': data['rows'],
        'charts': _chart_config(data['metrics']),
    }, ensure_ascii=False).replace('</', '<\\/')
    safe_title = html.escape(title)
    return f'''<!doctype html>
<html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>{safe_title}</title><style>
:root{{--bg:#f4f7fb;--card:#fff;--text:#182230;--muted:#667085;--grid:#e4e7ec}}
*{{box-sizing:border-box}}body{{margin:0;background:var(--bg);color:var(--text);font:14px system-ui,sans-serif}}
main{{max-width:1440px;margin:auto;padding:24px}}h1{{margin:0 0 4px}}.sub{{color:var(--muted);margin-bottom:18px}}
.chart{{background:var(--card);border:1px solid var(--grid);border-radius:12px;padding:16px;margin:14px 0}}
.chart h2{{font-size:17px;margin:0 0 10px}}.plot{{position:relative;height:320px}}canvas{{width:100%;height:100%;cursor:crosshair}}
.legend{{display:flex;flex-wrap:wrap;gap:12px;margin-top:8px}}.legend label{{cursor:pointer}}.swatch{{display:inline-block;width:18px;height:3px;margin-right:5px;vertical-align:middle}}
.tooltip{{display:none;position:absolute;top:8px;z-index:2;pointer-events:none;white-space:pre;background:#101828ee;color:#fff;border-radius:6px;padding:8px 10px;font:12px ui-monospace,monospace}}
.hint{{color:var(--muted);font-size:12px}}@media(max-width:600px){{main{{padding:12px}}.plot{{height:260px}}}}
</style></head><body><main><h1>{safe_title}</h1>
<div class="sub"></div><div class="hint">悬停显示最近采样点的精确时间和数值；单击可锁定，再次单击解锁。</div>
<div id="charts"></div></main><script>
const DATA={payload};
const COLORS=['#175cd3','#d92d20','#039855','#7a5af8','#dc6803','#0086c9','#c11574','#4e5ba6'];
document.querySelector('.sub').textContent=`${{DATA.source}} · ${{DATA.rows.length}} samples`;
const fmt=v=>Number.isInteger(v)?String(v):Number(v).toFixed(3).replace(/0+$/,'').replace(/\\.$/,'');
function nearestIndex(target){{let best=0,delta=Infinity;DATA.rows.forEach((r,i)=>{{const d=Math.abs(r.epoch_ms-target);if(d<delta){{best=i;delta=d}}}});return best}}
function createChart(config,chartIndex){{
 const card=document.createElement('section');card.className='chart';
 card.innerHTML=`<h2>${{config.title}}</h2><div class="plot"><canvas data-point-index=""></canvas><div class="tooltip"></div></div><div class="legend"></div>`;
 document.getElementById('charts').appendChild(card);
 const canvas=card.querySelector('canvas'),tip=card.querySelector('.tooltip'),legend=card.querySelector('.legend');
 const lines=config.metrics.map((m,i)=>({{...m,color:COLORS[(chartIndex*3+i)%COLORS.length],visible:true}}));
 lines.forEach(line=>{{const label=document.createElement('label');label.innerHTML=`<input type="checkbox" checked> <i class="swatch" style="background:${{line.color}}"></i>${{line.label}}`;label.querySelector('input').onchange=e=>{{line.visible=e.target.checked;draw()}};legend.appendChild(label)}});
 let geom=null,hover=null,locked=null,lastX=0;
 function selected(){{return locked===null?hover:locked}}
 function draw(){{
  const rect=canvas.getBoundingClientRect(),ratio=window.devicePixelRatio||1;canvas.width=rect.width*ratio;canvas.height=rect.height*ratio;
  const ctx=canvas.getContext('2d');ctx.scale(ratio,ratio);const W=rect.width,H=rect.height,p={{l:62,r:18,t:12,b:40}};
  const visible=lines.filter(l=>l.visible),values=visible.flatMap(l=>DATA.rows.flatMap(r=>r[l.key]===undefined?[]:[r[l.key]]));
  let ymin=values.length?Math.min(0,...values):0,ymax=values.length?Math.max(...values):1;if(ymax<=ymin)ymax=ymin+1;ymax*=1.08;
  const xmin=DATA.rows[0].epoch_ms,xmax=Math.max(xmin+1000,DATA.rows[DATA.rows.length-1].epoch_ms);
  const sx=x=>p.l+(x-xmin)/(xmax-xmin)*(W-p.l-p.r),sy=y=>H-p.b-(y-ymin)/(ymax-ymin)*(H-p.t-p.b);
  ctx.font='12px system-ui';ctx.fillStyle='#667085';ctx.strokeStyle='#e4e7ec';ctx.lineWidth=1;
  for(let i=0;i<=5;i++){{const y=p.t+i*(H-p.t-p.b)/5;ctx.beginPath();ctx.moveTo(p.l,y);ctx.lineTo(W-p.r,y);ctx.stroke();ctx.fillText(fmt(ymax-(ymax-ymin)*i/5),5,y+4)}}
  [0,.25,.5,.75,1].forEach(f=>{{const x=p.l+f*(W-p.l-p.r),t=xmin+f*(xmax-xmin);ctx.fillText(new Date(t).toLocaleTimeString(undefined,{{hour12:false}}),Math.max(2,Math.min(W-75,x-30)),H-12)}});
  visible.forEach(line=>{{ctx.strokeStyle=line.color;ctx.lineWidth=2;ctx.beginPath();let started=false;DATA.rows.forEach(r=>{{if(r[line.key]===undefined){{started=false;return}}const x=sx(r.epoch_ms),y=sy(r[line.key]);started?ctx.lineTo(x,y):ctx.moveTo(x,y);started=true}});ctx.stroke()}});
  geom={{W,H,p,xmin,xmax,sx,sy,visible}};const index=selected();canvas.dataset.pointIndex=index===null?'':String(index);
  if(index!==null){{const row=DATA.rows[index],x=sx(row.epoch_ms);ctx.strokeStyle='#344054';ctx.setLineDash([4,4]);ctx.beginPath();ctx.moveTo(x,p.t);ctx.lineTo(x,H-p.b);ctx.stroke();ctx.setLineDash([]);visible.forEach(line=>{{if(row[line.key]===undefined)return;ctx.fillStyle='#fff';ctx.strokeStyle=line.color;ctx.lineWidth=2;ctx.beginPath();ctx.arc(x,sy(row[line.key]),4,0,Math.PI*2);ctx.fill();ctx.stroke()}})}}
 }}
 function show(index,mx){{if(index===null)return;const row=DATA.rows[index],linesText=geom.visible.map(line=>`${{line.label}}: ${{row[line.key]===undefined?'N/A':fmt(row[line.key])+line.unit}}`);tip.textContent=`${{locked===null?'':'Locked · '}}${{row.timestamp}}\\n${{linesText.join('\\n')}}`;tip.style.display='block';tip.style.left=Math.max(4,Math.min(mx+12,geom.W-tip.offsetWidth-6))+'px'}}
 canvas.onmousemove=e=>{{if(!geom||locked!==null)return;const r=canvas.getBoundingClientRect(),mx=e.clientX-r.left;lastX=mx;if(mx<geom.p.l||mx>geom.W-geom.p.r){{hover=null;tip.style.display='none';draw();return}}const target=geom.xmin+(mx-geom.p.l)/(geom.W-geom.p.l-geom.p.r)*(geom.xmax-geom.xmin);hover=nearestIndex(target);draw();show(hover,mx)}};
 canvas.onclick=e=>{{if(locked!==null){{locked=null;hover=null;tip.style.display='none';draw();return}}const r=canvas.getBoundingClientRect(),mx=e.clientX-r.left;if(mx<geom.p.l||mx>geom.W-geom.p.r)return;const target=geom.xmin+(mx-geom.p.l)/(geom.W-geom.p.l-geom.p.r)*(geom.xmax-geom.xmin);locked=nearestIndex(target);lastX=mx;draw();show(locked,mx)}};
 canvas.onmouseleave=()=>{{if(locked===null){{hover=null;tip.style.display='none';draw()}}}};
 new ResizeObserver(()=>{{draw();if(locked!==null)show(locked,lastX)}}).observe(canvas);draw();
}}
DATA.charts.forEach(createChart);
</script></body></html>'''


def main(argv=None):
    parser = argparse.ArgumentParser(
        description='Generate interactive HTML from resource_monitor.csv')
    parser.add_argument('input', help='procmon resource_monitor.csv')
    parser.add_argument('-o', '--output', default='resource_monitor.html')
    parser.add_argument('--title', default='Resource monitor report')
    args = parser.parse_args(argv)
    try:
        data = load_resource_csv(args.input)
        report = render_html(data, os.path.basename(args.input), args.title)
        with open(args.output, 'w', encoding='utf-8') as stream:
            stream.write(report)
    except (OSError, ValueError) as error:
        parser.error(str(error))
    print(f'Generated {args.output} from {len(data["rows"])} samples')
    return 0


if __name__ == '__main__':
    sys.exit(main())
