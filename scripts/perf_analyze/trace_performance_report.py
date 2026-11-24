#!/usr/bin/env python3
# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Description: Build a static client or worker performance report with lazily loaded metric data.
# Usage: python3 scripts/perf_analyze/trace_performance_report.py -f logs -t worker -o report; requires Plotly
#   and ripgrep or grep; extract the archive and open index.html.
"""Build a lazy-loaded static client/worker performance report (works from file://)."""
import argparse,gzip,json,math,re,subprocess,logging,time,shutil,importlib.util,os,io,tarfile,tempfile,atexit,shlex
import base64
from concurrent.futures import ProcessPoolExecutor
from concurrent.futures import ThreadPoolExecutor
from collections import defaultdict
from datetime import datetime
from pathlib import Path

TS=re.compile(r'(\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d(?:\.\d{1,6})?)')
URMA=re.compile(r'(?:total\s+cost|completion\s+cost|cost)\s*:?\s*([\d.]+)\s*ms',re.I)
POST=re.compile(r'trace_us:\{post:(\d+)',re.I)
SEND=re.compile(r'(?:chip_id\s*:\s*(\d+)(?:\[[^]]+\])?\s*->|sendChipId\s*:?\s*(\d+))',re.I)
NUMA=re.compile(r'numa_write_counts:\{([^}]+)\}',re.I)
INFLIGHT=re.compile(r'(?:chip_inflight:\{post:\[([^]]*)\],cur:\[([^]]*)\]\}|(postSrcChipInflight|srcChipInflight)\s*:\s*(\{[^}]*\}|\d+))',re.I)
ACTIVE_JETTY=re.compile(r'active_jetty\s*:\s*(\d+)',re.I)
LOG=logging.getLogger('trace_performance_report')
WIDTHS=(1000,10000,100000,1000000)
AGGREGATE_RECORDS={}
URMA_TIMEOUT_INVALID_U64=(1<<64)-1
def filtered_archive_command(root,prefix,needles):
 script='''import gzip,json,pathlib,sys,tarfile,tempfile,shutil
r=pathlib.Path(sys.argv[1]);p=sys.argv[2] or None;n=json.loads(sys.argv[3]);s=[x for x in r.iterdir() if x.is_dir() and (p is None or x.name.startswith(p))];s=([x for x in (r/"collected",r/"collected_worker_logs") if x.is_dir()] or s) if p is None else s
def rd(x):
 o=gzip.open if x.name.endswith(".gz") else open
 with o(x,"rt",encoding="utf-8",errors="replace") as f: yield from f
files=[x for d in s for x in d.rglob("*.log*") if x.is_file()];out=pathlib.Path(tempfile.mkdtemp(prefix="perf_filter_"))/r.name;(out/"collected").mkdir(parents=True);(out/"collected_worker_logs").mkdir(parents=True)
for x in files:
 t=out/"collected" if x.name.lower().startswith("ds_client") else out/"collected_worker_logs";z=t/x.name;z.parent.mkdir(parents=True,exist_ok=True)
 with z.open("w") as q:
  for l in rd(x):
   if ("access" not in x.name.lower() or any(v in l for v in n)):q.write(l)
with tarfile.open(fileobj=sys.stdout.buffer,mode="w|gz",compresslevel=1) as a:a.add(out,arcname=r.name)
shutil.rmtree(out.parent,ignore_errors=True)'''
 enc=base64.b64encode(script.encode()).decode();return 'python3 -c "import base64;exec(base64.b64decode({}))" {} {} {}'.format(shlex.quote(repr(enc)),shlex.quote(str(root)),shlex.quote(prefix or ''),shlex.quote(json.dumps(list(needles))))
def archive_command(root,prefix=None):
 find_expr="-type f \\( -name '*.log' -o -name '*.log.*' -o -name '*.gz' -o -iname '*INFO*' \\)"
 scope=(f"find {shlex.quote(root.name)} -mindepth 1 -maxdepth 1 -type d -name {shlex.quote(prefix+'*')} -exec find {{}} {find_expr} -print0 \\;" if prefix else f"find {shlex.quote(root.name)} \\( -path '*/collected' -o -path '*/collected_worker_logs' \\) -type d -exec find {{}} {find_expr} -print0 \\;")
 return f"cd -- {shlex.quote(str(root.parent))} && {scope} | tar --null --files-from=- -I 'gzip -1' -cf -"
def tree_bytes(root):
 return sum(path.stat().st_size for path in Path(root).rglob('*') if path.is_file())
def stage_cross_node(root,jobs,prefix=None,needles=None):
 root=Path(root).expanduser().resolve();LOG.info('[cross-node] discovering Kubernetes nodes root=%s jobs=%d',root,jobs);payload=json.loads(subprocess.check_output(['kubectl','get','nodes','-o','json'],text=True));targets=[]
 for item in payload.get('items',[]):
  ip=next((x.get('address') for x in item.get('status',{}).get('addresses',[]) if x.get('type')=='InternalIP'),None)
  if ip:targets.append((item.get('metadata',{}).get('name') or ip,ip))
 if not targets:raise RuntimeError('kubectl returned no nodes with InternalIP')
 LOG.info('[cross-node] discovered %d node(s): %s',len(targets),', '.join(f'{name}({ip})' for name,ip in targets))
 stage=Path(tempfile.mkdtemp(prefix='perf_analyze_cross_node_'));atexit.register(shutil.rmtree,stage,ignore_errors=True);marker=stage/'.cross_node_roots';marker.write_text('[]',encoding='utf8');LOG.info('[cross-node] phase=stage_created path=%s staged_bytes=%d',stage,tree_bytes(stage))
 def fetch(target):
  name,ip=target;command=filtered_archive_command(root,prefix,needles) if needles else archive_command(root,prefix);result=subprocess.run(['ssh','-o','BatchMode=yes','-o','ConnectTimeout=10',f'root@{ip}',command],stdout=subprocess.PIPE,stderr=subprocess.PIPE,check=False)
  if result.returncode:return name,False,result.stderr.decode(errors='replace').strip()
  dest=stage/re.sub(r'[^A-Za-z0-9_.-]+','_',name);dest.mkdir(parents=True,exist_ok=True)
  with tarfile.open(fileobj=io.BytesIO(result.stdout),mode='r:gz') as archive:archive.extractall(dest,filter='data')
  return name,True,str(dest),len(result.stdout),tree_bytes(dest)
 failures=0
 with ThreadPoolExecutor(max_workers=max(1,min(jobs,len(targets)))) as pool:
  for name,ok,value,*extra in pool.map(fetch,targets):
   if not ok:failures+=1;LOG.warning('[cross-node] fetch failed: %s: %s',name,value)
   else:
    LOG.info('[cross-node] fetched node=%s transfer_bytes=%d staged_bytes=%d stage=%s',name,extra[0],extra[1],value)
    roots=json.loads(marker.read_text(encoding='utf8'));roots.append(str(Path(value)/root.name));marker.write_text(json.dumps(roots),encoding='utf8')
 if failures==len(targets):raise RuntimeError('all cross-node fetches failed')
 LOG.info('[cross-node] staging complete stage=%s staged_bytes=%d succeeded=%d failed=%d',stage,tree_bytes(stage),len(targets)-failures,failures);return stage
def discover_log_roots(root,prefix=None):
 root=Path(root).expanduser().resolve()
 if not root.is_dir():raise ValueError(f'input directory does not exist: {root}')
 marker=root/'.cross_node_roots'
 if marker.is_file():
  clients=[];workers=[]
  for node_root in json.loads(marker.read_text(encoding='utf8')):
   node_clients,node_workers=discover_log_roots(node_root,prefix);clients.extend(node_clients);workers.extend(node_workers)
  return sorted(set(clients)),sorted(set(workers))
 if prefix is None:
  client_root,worker_root=root/'collected',root/'collected_worker_logs'
  if client_root.is_dir() or worker_root.is_dir():return ([client_root] if client_root.is_dir() else [],[worker_root] if worker_root.is_dir() else [])
 scopes=sorted((p for p in root.iterdir() if p.is_dir() and (prefix is None or p.name.startswith(prefix))),key=str)
 if prefix is not None and not scopes:raise ValueError(f'no second-level directory under {root} starts with {prefix!r}')
 clients=set();workers=set()
 for scope in scopes:
  for directory,_,filenames in os.walk(scope):
   names=[name.lower() for name in filenames]
   if any(name.startswith('ds_client_access') and name.endswith(('.log','.log.gz')) for name in names):clients.add(Path(directory))
   if any(name.startswith('access') and not name.startswith('ds_client_access') and name.endswith(('.log','.log.gz')) for name in names):workers.add(Path(directory))
 return sorted(clients),sorted(workers)
def access_log_dirs(roots,source_type):
 prefix='ds_client_access' if source_type=='client' else 'access'
 result=[]
 for root in roots:
  for directory,_,filenames in os.walk(root):
   names=[name.lower() for name in filenames]
   if any(name.startswith(prefix) and (source_type=='client' or not name.startswith('ds_client_access')) and name.endswith(('.log','.log.gz')) for name in names):result.append(Path(directory))
 return sorted(set(result))
def elapsed(start):
 return f'{(time.perf_counter()-start)*1000:.1f} ms'
def time_of(s):
 m=TS.search(s)
 if not m:return None
 try:return datetime.fromisoformat(m.group(1)).timestamp()
 except ValueError:return None
def search_lines(p,search,patterns):
 if search=='rg':cmd=['rg','-z','-j','2','--no-messages','-e',patterns[0]]+sum((['-e',x] for x in patterns[1:]),[])+[str(p)]
 else:cmd=['zgrep' if str(p).endswith('.gz') else 'grep','-E','|'.join('('+x+')' for x in patterns),str(p)]
 result=subprocess.run(cmd,stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True,encoding='utf8',errors='ignore')
 if result.returncode not in (0,1):raise RuntimeError(f'{search} failed for {p}: {result.stderr.strip()}')
 return result.stdout.splitlines()
def eligible(fs,detail,include_access=True):
 return [p for p in fs if not p.name.endswith('.bak') and (('access' in p.name.lower()) if not detail else ((include_access and 'access' in p.name.lower()) or ('.info.' in p.name.lower())))]
def empty_records(): return {k:[] for k in ('access','access_count','cost','post','post_raw','chip','inflight','numa','active_jetty')}
def merge_records(parts):
 out=empty_records()
 for part in parts:
  for k,v in part.items():out[k].extend(v)
 return out
def scan_job(job):
 """Process-pool entry point; one job owns one or more log files."""
 fs,api,detail,search=job
 return scan(fs,api,detail,search)
def scan_qps_job(job):
 name,fs,api,search=job
 started=time.perf_counter(); timestamps=[]; files_seen=0; lines_seen=0
 for p in eligible(fs,False):
  try:
   files_seen+=1
   for line in search_lines(p,search,[re.escape(api)]):
     lines_seen+=1; fields=line.split('|')
     if len(fields)<=9 or fields[8].strip()!=api:continue
     try:float(fields[9].strip())
     except ValueError:continue
     t=time_of(line)
     if t is not None:timestamps.append(t)
  except (OSError,gzip.BadGzipFile):pass
 if not timestamps:score=0
 else:
  origin=min(timestamps); counts={int((t-origin)*1000000//1000000) for t in timestamps}
  score=len(timestamps)/len(counts)
 LOG.info('scan rank worker=%s files=%d lines=%d requests=%d qps=%.3f elapsed=%s',name,files_seen,lines_seen,len(timestamps),score,elapsed(started))
 return name,score
def scan(fs,api,detail=False,search='grep'):
 started=time.perf_counter(); files_seen=0; lines_seen=0
 d=empty_records()
 for p in eligible(fs,detail):
  try:
   files_seen+=1
   access_file='access' in p.name.lower()
   patterns=[re.escape(api)] if access_file else []
   if detail:patterns+=['URMA_ELAPSED_TOTAL','numa_write_counts']
   for line in search_lines(p,search,patterns):
     lines_seen+=1
     access_fields=None
     if access_file:
      fields=line.split('|')
      if len(fields)>9 and fields[8].strip()==api:access_fields=fields
     urma=detail and 'URMA_ELAPSED_TOTAL' in line
     numa=detail and 'numa_write_counts' in line
     if access_fields is None and not urma and not numa:continue
     t=time_of(line)
     if t is None:continue
     if access_fields is not None:
      try:
       latency=float(access_fields[9].strip()); d['access'].append((t,latency)); d['access_count'].append((t,1))
      except ValueError:pass
     if urma:
      m=URMA.search(line)
      if m:d['cost'].append((t,float(m.group(1))*1000))
      m=POST.search(line)
      if m:
       post=int(m.group(1))
       # 9a2564 emits trace offsets, so a valid post point is 0 rather than
       # the former absolute timestamp.  Use the record timestamp for those
       # points, and ignore the timeout sentinel altogether.
       if post != URMA_TIMEOUT_INVALID_U64:
        d['post_raw'].append((t,int(t*1000000) if post == 0 else post))
      m=SEND.search(line)
      if m:d['chip'].append((t,int(m.group(1) or m.group(2))))
      m=ACTIVE_JETTY.search(line)
      if m:d['active_jetty'].append((t,int(m.group(1))))
      for im in INFLIGHT.finditer(line):
       if im.group(1) is not None:
        groups=(('post',im.group(1)),('cur',im.group(2)))
       else:
        groups=((im.group(3),im.group(4)),)
       for key,value in groups:
        values=re.findall(r':\s*(\d+)',value)
        nums=[int(x) for x in values] if values else ([int(value)] if value.isdigit() else [])
        if nums:d['inflight'].append((t,key,nums))
     if numa:
      m=NUMA.search(line)
      if m:
       d['numa'].append((t,{k:int(v) for k,v in re.findall(r'(\w+)\s*:\s*(\d+)',m.group(1))}))
  except (OSError,gzip.BadGzipFile):pass
 LOG.info('scan detail=%s files=%d lines=%d records=%s elapsed=%s',detail,files_seen,lines_seen,{k:len(v) for k,v in d.items()},elapsed(started))
 return d
def stat(v):
 if not v:return {}
 if len(v)==1:return {'a':round(v[0],3),'b':v[0],'c':v[0],'d':v[0],'e':v[0]}
 a=sorted(v); q=lambda p:a[min(len(a)-1,max(0,math.ceil(len(a)*p)-1))]
 return {'a':round(sum(a)/len(a),3),'b':q(.5),'c':q(.9),'d':q(.99),'e':a[-1]}
def bucket(v,w,origin,reducer=max,with_stats=True):
 grouped=defaultdict(list) if with_stats else defaultdict(int)
 for t,x in v:
  key=int((t-origin)*1000000//w)
  if with_stats:grouped[key].append(x)
  else:grouped[key]+=x
 return [[origin+k*w/1000000,reducer(values) if with_stats else values,stat(values) if with_stats else None] for k,values in sorted(grouped.items())]
def aggregate_width(d,w):
 started=time.perf_counter()
 raw_post=d['post_raw']
 if raw_post:
  first_t,first_post=min(raw_post,key=lambda x:x[1])
  post=[(first_t+(p-first_post)/1000000,1) for _,p in raw_post]
 else:post=[]
 origin=min((item[0] for values in (d['access'],d['cost'],post,d['chip'],d['inflight'],d['numa'],d['active_jetty']) for item in values), default=0)
 chip=defaultdict(list); numa=defaultdict(list); inflight=defaultdict(list)
 for t,c in d['chip']:chip[str(c)].append((t,1))
 for t,vals in d['numa']:
  for k,v in vals.items():numa[k].append((t,v))
 for t,key,vals in d['inflight']:
  for i,v in enumerate(vals[:2]):inflight[key+str(i+1)].append((t,v))
 result={'access':bucket(d['access'],w,origin),'access_count':bucket(d['access_count'],w,origin,sum,False),'cost':bucket(d['cost'],w,origin),'post':bucket(post,w,origin,sum,False),'chip':{k:bucket(v,w,origin,sum,False) for k,v in chip.items()},'inflight':{k:bucket(v,w,origin) for k,v in inflight.items()},'numa':{k:bucket(v,w,origin) for k,v in numa.items()},'active_jetty':bucket(d['active_jetty'],w,origin)}
 LOG.info('aggregate bucket=%d records=%s elapsed=%s',w,{k:len(v) for k,v in d.items()},elapsed(started))
 return result
def aggregate_width_job(job):
 name,w,out,wid=job
 result=aggregate_width(AGGREGATE_RECORDS[name],w)
 for metric,value in result.items():write_fragment(Path(out)/'data'/wid/'fragments'/(str(w)+'_'+metric+'.js'),metric,value)
 return w
def write_js(p,var,obj):
 p.parent.mkdir(parents=True,exist_ok=True);p.write_text('window.%s=%s;\n'%(var,json.dumps(obj,separators=(',',':'))))
CSS='''*{box-sizing:border-box}body{margin:0;font:14px -apple-system,BlinkMacSystemFont,"Segoe UI",Arial;background:#f4f6fa;color:#263238}.top{padding:20px 28px;background:linear-gradient(120deg,#1b376d,#396bc4);color:#fff}.top h2{margin:0;font-size:22px}.controls{display:flex;gap:12px;align-items:end;flex-wrap:wrap;background:#fff;padding:14px 24px;box-shadow:0 2px 8px #ccd}.controls label{display:flex;flex-direction:column;gap:5px;font-size:12px;color:#40506a}.controls select,.controls input,.controls button{height:35px;padding:6px 10px;border:1px solid #cbd5e1;border-radius:6px;background:#fff}.controls input{min-width:205px}.controls button{background:#2563eb;color:#fff;border:0;font-weight:600;cursor:pointer}.wrap{max-width:1540px;margin:auto;padding:18px}.chart{background:#fff;margin:16px 0;padding:14px;border-radius:10px;box-shadow:0 2px 7px #d8dce5}.chart h3{margin:0 0 7px;color:#1f3b70}.plot{height:330px}.stats{font:12px ui-monospace,SFMono-Regular,Menlo,monospace;color:#40506a;border-top:1px solid #edf0f5;padding:9px 3px;white-space:pre-line;line-height:1.65}.checks{display:flex;gap:15px;flex-wrap:wrap;padding:10px 0}.checks label{cursor:pointer}.error{color:#b42318;background:#fff1f0;padding:12px;border-radius:7px}a{color:#2563eb;font-weight:600;text-decoration:none}'''
PLOTLY_FILE='vendor/plotly-2.35.2.min.js'
PAGE_HEAD='''<!doctype html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><script src="PLOTLY"></script><style>CSS</style></head>'''
COMMON_JS='''const $=id=>document.getElementById(id),COL=['#2563eb','#dc2626','#16a34a','#9333ea','#ea580c','#0891b2'];
function iso(t){let d=new Date(t*1000),p=n=>String(n).padStart(2,'0'),ms=String(d.getMilliseconds()).padStart(3,'0');return d.getFullYear()+'-'+p(d.getMonth()+1)+'-'+p(d.getDate())+'T'+p(d.getHours())+':'+p(d.getMinutes())+':'+p(d.getSeconds())+'.'+ms}function stats(v,label,unit=''){if(!v.length)return label+': no data';v=v.slice().sort((a,b)=>a-b);let q=p=>v[Math.min(v.length-1,Math.ceil(v.length*p)-1)],f=x=>x+unit;return label+': avg '+f((v.reduce((a,b)=>a+b,0)/v.length).toFixed(2))+' | p50 '+f(q(.5))+' | p90 '+f(q(.9))+' | p99 '+f(q(.99))+' | pmax '+f(v[v.length-1])}
function restore(lo,hi){$('width').value='1000000';$('start').value=iso(lo);$('end').value=iso(hi)}function range(lo,hi){let a=Date.parse($('start').value)/1000,b=Date.parse($('end').value)/1000;return [Number.isFinite(a)?a:lo,Number.isFinite(b)?b:hi]}
function save(){}function hover(label,unit='',count=false){return count?label+': %{y}':label+': %{y}'+unit+' | avg: %{customdata.a}'+unit+' | p50: %{customdata.b}'+unit+' | p90: %{customdata.c}'+unit+' | p99: %{customdata.d}'+unit+' | pmax: %{customdata.e}'+unit+'<extra></extra>'}'''
def write_fragment(path, key, value):
 """Use JavaScript fragments so reports also work when opened with file://."""
 path.parent.mkdir(parents=True, exist_ok=True)
 path.write_text('window.REPORT_FRAGMENTS=window.REPORT_FRAGMENTS||{};window.REPORT_FRAGMENTS['+json.dumps(key)+']='+json.dumps(value,separators=(',',':'))+';\n',encoding='utf8')
def ensure_plotly(out):
 """Bundle the locally installed Plotly runtime; report generation is offline-safe."""
 target=out/PLOTLY_FILE
 if target.is_file() and target.stat().st_size > 1000000:
  return
 spec=importlib.util.find_spec('plotly')
 candidates=[]
 if spec and spec.origin:
  package=Path(spec.origin).parent
  candidates.extend((package/'package_data'/'plotly.min.js',package/'plotly.min.js'))
 source=next((p for p in candidates if p.is_file() and p.stat().st_size > 1000000),None)
 if source is None:
  raise RuntimeError('local Plotly JavaScript was not found; install the Python plotly package in this same interpreter (pip install plotly)')
 target.parent.mkdir(parents=True,exist_ok=True)
 shutil.copyfile(source,target)
 LOG.info('local Plotly runtime ready: %s (%d bytes)',target,target.stat().st_size)
def page_head(depth=0):
 return PAGE_HEAD.replace('CSS',CSS).replace('PLOTLY',('../'*depth)+PLOTLY_FILE)
def make_fragments(out, worker_id, data):
 """Split pre-aggregated data once; browser subsequently parses only selected series."""
 for width, bucket in data['buckets'].items():
  for metric, value in bucket.items():
   write_fragment(out/'data'/worker_id/'fragments'/(str(width)+'_'+metric+'.js'),metric,value)
def make_index_v2(out,manifest):
 html='''<body><div class="top"><h2>Worker QPM / QP10ms / QP100ms / QPS</h2><div id="meta"></div></div><div class="controls"><label>Bucket<select id="width"><option value="1000">1ms</option><option value="10000">10ms</option><option value="100000">100ms</option><option value="1000000">1s</option></select></label><label>Start<input id="start"></label><label>End<input id="end"></label><button id="query">Query</button></div><main class="wrap" id="charts"><p>Select a bucket/time range and click Query.</p></main><script src="data/manifest.js"></script><script>COMMON
 function fragment(src,key){return new Promise((ok,no)=>{let s=document.createElement('script');window.REPORT_FRAGMENTS={};s.src=src;s.onload=()=>{let value=window.REPORT_FRAGMENTS[key];delete window.REPORT_FRAGMENTS[key];Array.isArray(value)||value&&typeof value==='object'?ok(value):no(new Error('fragment did not define '+key+': '+src))};s.onerror=()=>no(new Error('cannot load: '+src));document.head.appendChild(s)})}async function fragments(srcs,key){let out=[];for(const src of srcs)out.push(await fragment(src,key));return out}function pointRange(){return range(REPORT_MANIFEST.start,REPORT_MANIFEST.end)}async function draw(){let root=$('charts'),r=pointRange();save();root.textContent='Loading selected access-count data…';try{let width=$('width').value,ds=await fragments(REPORT_MANIFEST.workers.map(w=>'data/'+w.id+'/fragments/'+width+'_access_count.js'),'access_count');root.innerHTML='';ds.forEach((raw,i)=>{let a=raw.filter(p=>p[0]>=r[0]&&p[0]<=r[1]),w=REPORT_MANIFEST.workers[i],box=document.createElement('section');box.className='chart';box.innerHTML='<h3>'+w.name+' <small>('+a.length+' points)</small></h3><div class="plot"></div><div class="stats"></div><a href="workers/'+w.id+'.html">Open detail</a>';root.append(box);Plotly.newPlot(box.querySelector('.plot'),[{x:a.map(p=>new Date(p[0]*1000)),y:a.map(p=>p[1]),customdata:a.map(p=>p[2]),mode:'lines+markers',line:{color:COL[0],width:1.5},marker:{color:'#111827',size:4},hovertemplate:hover('accessNum','',true)}],{template:'plotly_white',margin:{t:10,l:55,r:20,b:45},hovermode:'x unified',xaxis:{title:'time',range:r.map(x=>new Date(x*1000)),rangeslider:{visible:true}},yaxis:{title:'accessNum'},dragmode:'zoom'});box.querySelector('.stats').textContent=stats(a.map(p=>p[1]),'accessNum')})}catch(e){root.innerHTML='<p class="error">Load failed: '+e.message+'</p>';console.error(e)}}restore(REPORT_MANIFEST.start,REPORT_MANIFEST.end);$('query').onclick=draw;$('meta').textContent='API: '+REPORT_MANIFEST.api+' | objects: '+REPORT_MANIFEST.workers.length;</script></body></html>'''.replace('COMMON',COMMON_JS)
 (out/'index.html').write_text(page_head()+html,encoding='utf8')
def make_details_v2(out,manifest):
 for w in manifest['workers']:
  html='''<body><div class="top"><h2>__TITLE__ — detail</h2></div><div class="controls"><label>Bucket<select id="width"><option value="1000">1ms</option><option value="10000">10ms</option><option value="100000">100ms</option><option value="1000000">1s</option></select></label><label>Start<input id="start"></label><label>End<input id="end"></label><button id="query">Query</button></div><main class="wrap"><div class="checks"><label><input type="checkbox" value="access">Access latency (µs)</label><label><input type="checkbox" value="post">Post count</label><label><input type="checkbox" value="cost">URMA post-to-completion latency (µs)</label><label><input type="checkbox" value="chip">sendChipId count</label><label><input type="checkbox" value="inflight">Post/Poll inflight</label><label><input type="checkbox" value="active_jetty">Active Jetty</label><label><input type="checkbox" value="numa">Chip balance</label></div><div id="charts"><p>Select metrics and click Query.</p></div></main><script src="../data/manifest.js"></script><script>COMMON
const D={start:REPORT_MANIFEST.start,end:REPORT_MANIFEST.end},WID='__WID__';let plots=[],busy=false;const NAMES={src1:'src chip 1 cumulative writes',src2:'src chip 2 cumulative writes',dst1:'dst chip 1 cumulative writes',dst2:'dst chip 2 cumulative writes',src1_src2:'source switch 1→2',src2_src1:'source switch 2→1'};function fragment(metric){return new Promise((ok,no)=>{let s=document.createElement('script');window.REPORT_FRAGMENTS={};s.src='../data/'+WID+'/fragments/'+$('width').value+'_'+metric+'.js';s.onload=()=>{let value=window.REPORT_FRAGMENTS[metric];delete window.REPORT_FRAGMENTS[metric];Array.isArray(value)||value&&typeof value==='object'?ok(value):no(new Error('fragment did not define '+metric))};s.onerror=()=>no(new Error('cannot load: '+metric));document.head.append(s)})}async function fragments(metrics){let out=[];for(const metric of metrics)out.push(await fragment(metric));return out}function unit(key){return key==='access'||key==='cost'?' µs':''}function count(key){return key==='post'||key==='chip'||key==='numa'}function traces(raw,r,key){let u=unit(key),c=count(key);if(Array.isArray(raw)){let a=raw.filter(p=>p[0]>=r[0]&&p[0]<=r[1]);return [{name:key,x:a.map(p=>new Date(p[0]*1000)),y:a.map(p=>p[1]),customdata:a.map(p=>p[2]),mode:'lines+markers',line:{color:COL[0],width:1.5},marker:{color:'#111827',size:4},hovertemplate:hover(key,u,c)}]}return Object.entries(raw).map(([n,a],i)=>{a=a.filter(p=>p[0]>=r[0]&&p[0]<=r[1]);return {name:NAMES[n]||n,x:a.map(p=>new Date(p[0]*1000)),y:a.map(p=>p[1]),customdata:a.map(p=>p[2]),mode:'lines+markers',line:{color:COL[i%COL.length],width:1.5},marker:{size:4},hovertemplate:hover(NAMES[n]||n,u,c)}})}async function draw(){let r=range(D.start,D.end),root=$('charts'),selected=[...document.querySelectorAll('.checks input:checked')];save();if(!selected.length){root.innerHTML='<p>Select metrics and click Query.</p>';return}root.textContent='Loading selected metric data…';try{let raw=await fragments(selected.map(x=>x.value));root.innerHTML='';plots=[];selected.forEach((cb,i)=>{let ts=traces(raw[i],r,cb.value),u=unit(cb.value),c=count(cb.value);if(!ts.some(t=>t.y.length)){root.insertAdjacentHTML('beforeend','<p class="error">No '+cb.value+' data in the selected time range.</p>');return}let box=document.createElement('section');box.className='chart';box.innerHTML='<h3>'+cb.parentNode.textContent+'</h3><div class="plot"></div><div class="stats"></div>';root.append(box);let p=box.querySelector('.plot');plots.push(p);Plotly.newPlot(p,ts,{template:'plotly_white',margin:{t:10,l:55,r:20,b:45},hovermode:'x unified',xaxis:{title:'time',range:r.map(x=>new Date(x*1000)),rangeslider:{visible:true}},yaxis:{title:u?'latency (µs)':c?'count':'value'},dragmode:'zoom'}).then(()=>p.on('plotly_relayout',e=>{let a=e['xaxis.range[0]'],z=e['xaxis.range[1]'];if(busy||a===undefined)return;busy=true;plots.filter(x=>x!==p).forEach(x=>Plotly.relayout(x,{'xaxis.range':[a,z]}));setTimeout(()=>busy=false,20)}));box.querySelector('.stats').textContent=c?'':ts.map(t=>stats(t.y,t.name,u)).join('\\n')})}catch(e){root.innerHTML='<p class="error">Load failed: '+e.message+'</p>';console.error(e)}}restore(D.start,D.end);$('query').onclick=draw;</script></body></html>'''.replace('COMMON',COMMON_JS).replace('__WID__',w['id']).replace('__TITLE__',w['name'])
  p=out/'workers'/(w['id']+'.html');p.parent.mkdir(parents=True,exist_ok=True);p.write_text(page_head(1)+html,encoding='utf8')
def main():
 started=time.perf_counter()
 p=argparse.ArgumentParser(description='Generate a lazy-loaded client/worker performance report.',formatter_class=argparse.RawDescriptionHelpFormatter,epilog='''Input layout:\n  Standard: <rootDir>/collected/... and <rootDir>/collected_worker_logs/...\n  Mixed:    <rootDir>/jingpai*/client*/ds_client_access*.log[.gz]\n            <rootDir>/jingpai*/worker*/access*.log[.gz]\n  With -p, only second-level directories whose names start with that prefix\n  are scanned. Client and worker directories are validated from access-log filenames.\n\nExamples:\n  %(prog)s -f /logs -o report -t worker --api DS_POSIX_QUERY_AND_GET\n  %(prog)s -f /logs -p jingpai -o report -t worker --api DS_POSIX_QUERY_AND_GET''')
 p.add_argument('-f','--dir',required=True,help='root directory containing standard or mixed collections');p.add_argument('-p','--prefix',help='only scan second-level mixed collection directories with this prefix');p.add_argument('--cross-node',action='store_true',help='fetch the root from Kubernetes nodes before local processing');p.add_argument('-o','--output',default='trace_performance_report');p.add_argument('-t','--type',choices=('client','worker'),default='client',help='log source type used for worker selection');p.add_argument('--api',help='API name to analyze (defaults by source type)');p.add_argument('-j','--jobs',type=int,default=100,help='parallel scan jobs (default: 100)');p.add_argument('-s','--serach',choices=('rg','grep'),default='rg',help='log search command (default: rg; rg uses -j 2 per scan job)');a=p.parse_args();logging.basicConfig(level=logging.INFO,format='%(asctime)s [%(levelname)s] %(message)s')
 if a.serach=='rg' and shutil.which('rg') is None:p.error('rg is not installed; install ripgrep or rerun with --serach grep')
 out=Path(a.output);out.mkdir(parents=True,exist_ok=True)
 api=a.api or ('DS_KV_CLIENT_GET' if a.type=='client' else 'DS_POSIX_QUERY_AND_GET');root=Path(a.dir)
 if a.cross_node:
  try:root=stage_cross_node(root,a.jobs,a.prefix,[api])
  except Exception as exc:p.error(f'cross-node staging failed: {exc}')
 try:client_roots,worker_roots=discover_log_roots(root,a.prefix)
 except ValueError as exc:p.error(str(exc))
 dirs=access_log_dirs(client_roots if a.type=='client' else worker_roots,a.type)
 if not dirs:p.error(f'no {a.type} log directories were discovered')
 LOG.info('start dir=%s output=%s type=%s api=%s jobs=%d directories=%d',root,out,a.type,api,a.jobs,len(dirs));ensure_plotly(out)
 all={};score=[]
 for index,d in enumerate(dirs):all[f'{d.name}_{index}' if sum(x.name==d.name for x in dirs)>1 else d.name]=[x for x in d.iterdir() if x.is_file()]
 first_jobs=[(n,eligible(fs,False)) for n,fs in all.items()]
 with ProcessPoolExecutor(max_workers=max(1,a.jobs)) as pool:
  first_scores=dict(pool.map(scan_qps_job, [(n,fs,api,a.serach) for n,fs in first_jobs]))
 for n in all:score.append((first_scores.get(n,0),n))
 LOG.info('first-pass workers=%d elapsed=%s',len(all),elapsed(started))
 score.sort();names=[]
 # Top5 means the highest average QPS; Last5 means the lowest average QPS.
 for _,n in score[-5:][::-1]+score[:5]:
  if n not in names:names.append(n)
 m={'api':api,'type':a.type,'workers':[]}
 LOG.info('selected workers=%s',names)
 detail_jobs=[]
 for n in names:
  fs=eligible(all[n],True)
  # Split a worker's files into independent read jobs so -j also helps when
  # there are fewer workers than available threads.
  detail_jobs.extend((n,chunk) for chunk in ([p] for p in fs))
 with ProcessPoolExecutor(max_workers=max(1,a.jobs)) as pool:
  grouped=defaultdict(list)
  jobs=[(chunk,api,True,a.serach) for _,chunk in detail_jobs]
  for (n,_),part in zip(detail_jobs,pool.map(scan_job,jobs)): grouped[n].append(part)
 records={n:merge_records(parts) for n,parts in grouped.items()}
 global AGGREGATE_RECORDS
 AGGREGATE_RECORDS=records
 for n in names:
  wid='worker_'+re.sub('[^A-Za-z0-9_.-]+','_',n);d=records[n];start=min((t for k in d for t,*_ in d[k]),default=None);end=max((t for k in d for t,*_ in d[k]),default=None);m['workers'].append({'id':wid,'name':n,'start':start,'end':end})
 aggregate_jobs=[(w['name'],width,str(out),w['id']) for w in m['workers'] for width in WIDTHS]
 with ProcessPoolExecutor(max_workers=min(50,len(aggregate_jobs))) as pool:list(pool.map(aggregate_width_job,aggregate_jobs))
 m['start']=min(w['start'] for w in m['workers']);m['end']=max(w['end'] for w in m['workers']);write_js(out/'data/manifest.js','REPORT_MANIFEST',m)
 phase=time.perf_counter();make_index_v2(out,m);LOG.info('index/data manifest generation elapsed=%s',elapsed(phase))
 phase=time.perf_counter();make_details_v2(out,m);LOG.info('detail pages generation elapsed=%s',elapsed(phase))
 phase=time.perf_counter()
 archive=Path(str(out).rstrip('/')+'.tar.gz');subprocess.run(['tar','-zcvf',str(archive),out.name],cwd=out.parent,check=True)
 LOG.info('tar generation elapsed=%s',elapsed(phase));LOG.info('finished total_elapsed=%s',elapsed(started));print('generated',out,'workers',len(names))
if __name__=='__main__':main()
