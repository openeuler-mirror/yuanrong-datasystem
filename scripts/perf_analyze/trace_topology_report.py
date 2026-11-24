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

# Description: Visualize client and worker request relationships from error or high-latency access records.
# Usage: python3 scripts/perf_analyze/trace_topology_report.py -f logs -t core; use -t time --base 5000 for
#   slow requests; extract the generated archive to view the report.
"""Build a single-file interactive client/worker topology from access logs.

Examples:
  python3 trace_topology_report.py -f /path/to/rootDir -t core
  python3 trace_topology_report.py -f /path/to/rootDir -p jingpai -t time --base 5000 --max-trace 50

Input layout:
  Standard: <rootDir>/collected/... and <rootDir>/collected_worker_logs/...
  Mixed:    <rootDir>/jingpai*/client*/ds_client_access*.log[.gz]
            <rootDir>/jingpai*/worker*/access*.log[.gz]
  With -p, only second-level directories whose names start with that prefix
  are scanned. Client and worker directories are validated from their access
  log filenames, not only their names.

``core`` analyses non-zero client access records; ``time`` analyses client
records whose field 10 exceeds ``--base`` (5000 means 5ms). The output is a
``.tar.gz`` containing an HTML file and its on-demand data directory. Extract
the archive and open the HTML directly; no HTTP service is required.
"""

import argparse
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor, as_completed
from functools import partial
import gzip
import html
import json
import os
from pathlib import Path
import random
import re
import subprocess
import tempfile
import time
import io
import atexit
import tarfile
import shutil
import shlex
import base64
from concurrent.futures import ThreadPoolExecutor
from typing import Iterable

LINE_RE = re.compile(r'^(?P<prefix>.*?:\d+:)?(?P<body>\d{4}-\d\d-\d\dT[^ ]+\s*\|.*)$')
TRACE_RE = re.compile(r'traceId\s*[:=]\s*([^,\s|]+)', re.I)
IP_RE = re.compile(r'\d{1,3}(?:\.\d{1,3}){3}')
HOST_RE = re.compile(r'\bHOST\b(?P<value>.*)', re.I)
DEFAULT_JOBS = 8
_TARGET_TRACES = set()


def cross_log(message):
    print(f'[{time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())}.{int(time.time() * 1000) % 1000:03d}] [cross-node] {message}', flush=True)


def tree_bytes(root):
    return sum(path.stat().st_size for path in Path(root).rglob('*') if path.is_file())


def archive_command(root, prefix=None):
    find_expr = "-type f \\( -name '*.log' -o -name '*.log.*' -o -name '*.gz' -o -iname '*INFO*' -o -iname '*metrics*' \\)"
    scope = (f"find {shlex.quote(root.name)} -mindepth 1 -maxdepth 1 -type d -name {shlex.quote(prefix + '*')} -exec find {{}} {find_expr} -print0 \\;" if prefix else
             f"find {shlex.quote(root.name)} \\( -path '*/collected' -o -path '*/collected_worker_logs' \\) -type d -exec find {{}} {find_expr} -print0 \\;")
    return (f"cd -- {shlex.quote(str(root.parent))} && {scope} | "
            "tar --null --files-from=- -I 'gzip -1' -cf -")

def filtered_archive_command(root, prefix, needles):
    script='''import gzip,json,pathlib,sys,tarfile,tempfile,shutil
r=pathlib.Path(sys.argv[1]);p=sys.argv[2] or None;n=json.loads(sys.argv[3]);s=[x for x in r.iterdir() if x.is_dir() and (p is None or x.name.startswith(p))];s=([x for x in (r/"collected",r/"collected_worker_logs") if x.is_dir()] or s) if p is None else s
def rd(x):
 o=gzip.open if x.name.endswith(".gz") else open
 with o(x,"rt",encoding="utf-8",errors="replace") as f: yield from f
files=[x for d in s for x in d.rglob("*.log*") if x.is_file() and (x.name.lower().startswith("access") or x.name.lower().startswith("ds_client_access"))];out=pathlib.Path(tempfile.mkdtemp(prefix="topology_filter_"))/r.name;(out/"collected").mkdir(parents=True);(out/"collected_worker_logs").mkdir(parents=True)
for x in files:
 t=out/"collected" if x.name.lower().startswith("ds_client") else out/"collected_worker_logs";z=t/x.name;z.parent.mkdir(parents=True,exist_ok=True)
 with z.open("w") as q:
  for l in rd(x):
   if any(v in l for v in n):q.write(l)
with tarfile.open(fileobj=sys.stdout.buffer,mode="w|gz",compresslevel=1) as a:a.add(out,arcname=r.name)
shutil.rmtree(out.parent,ignore_errors=True)'''
    enc=base64.b64encode(script.encode()).decode();return 'python3 -c "import base64;exec(base64.b64decode({}))" {} {} {}'.format(shlex.quote(repr(enc)),shlex.quote(str(root)),shlex.quote(prefix or ''),shlex.quote(json.dumps(list(needles))))


def stage_cross_node(root: Path, jobs: int, prefix=None, needles=None) -> Path:
    root = root.expanduser().resolve()
    started = time.monotonic()
    cross_log(f'phase=discover_nodes enter root={root} jobs={jobs}')
    payload = json.loads(subprocess.check_output(['kubectl', 'get', 'nodes', '-o', 'json'], text=True))
    targets = []
    for item in payload.get('items', []):
        ip = next((entry.get('address') for entry in item.get('status', {}).get('addresses', [])
                   if entry.get('type') == 'InternalIP'), None)
        if ip:
            targets.append((item.get('metadata', {}).get('name') or ip, ip))
    if not targets:
        raise RuntimeError('kubectl returned no nodes with InternalIP')
    cross_log(f'phase=discover_nodes leave nodes={len(targets)} elapsed={time.monotonic() - started:.3f}s targets={targets}')
    stage = Path(tempfile.mkdtemp(prefix='perf_analyze_cross_node_'))
    atexit.register(shutil.rmtree, stage, ignore_errors=True)
    cross_log(f'phase=stage_created path={stage} staged_bytes={tree_bytes(stage)}')
    marker = stage / '.cross_node_roots'
    marker.write_text('[]', encoding='utf-8')
    def fetch(target):
        node, ip = target
        fetch_started = time.monotonic()
        cross_log(f'node={node} phase=ssh_tar enter ip={ip}')
        command = filtered_archive_command(root, prefix, needles) if needles else archive_command(root, prefix)
        result = subprocess.run(['ssh', '-o', 'BatchMode=yes', '-o', 'ConnectTimeout=10', f'root@{ip}', command],
                                stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=False)
        if result.returncode:
            return node, False, result.stderr.decode(errors='replace').strip(), time.monotonic() - fetch_started
        destination = stage / re.sub(r'[^A-Za-z0-9_.-]+', '_', node)
        destination.mkdir(parents=True, exist_ok=True)
        with tarfile.open(fileobj=io.BytesIO(result.stdout), mode='r:gz') as archive:
            archive.extractall(destination, filter='data')
        return node, True, str(destination), len(result.stdout), tree_bytes(destination), time.monotonic() - fetch_started
    failures = []
    with ThreadPoolExecutor(max_workers=max(1, min(jobs, len(targets)))) as pool:
        for node, ok, value, *extra in pool.map(fetch, targets):
            if not ok:
                failures.append(f'{node}: {value}')
                cross_log(f'node={node} phase=ssh_tar leave status=failed elapsed={extra[0]:.3f}s error={value}')
            else:
                cross_log(f'node={node} phase=ssh_tar leave status=ok elapsed={extra[2]:.3f}s transfer_bytes={extra[0]} staged_bytes={extra[1]} stage={value}')
                roots = json.loads(marker.read_text(encoding='utf-8'))
                roots.append(str(Path(value) / root.name))
                marker.write_text(json.dumps(roots), encoding='utf-8')
    if len(failures) == len(targets):
        raise RuntimeError('all cross-node fetches failed')
    cross_log(f'phase=stage leave status=ok elapsed={time.monotonic() - started:.3f}s stage={stage} staged_bytes={tree_bytes(stage)} succeeded={len(targets) - len(failures)} failed={len(failures)}')
    return stage


def discover_log_roots(root: Path, prefix: str | None = None):
    """Return client and worker scan roots below one collection root.

    Standard collected directories take precedence unless a prefix explicitly
    selects one or more second-level mixed collections.
    """
    root = root.expanduser().resolve()
    if not root.is_dir():
        raise ValueError(f'input directory does not exist: {root}')
    marker = root / '.cross_node_roots'
    if marker.is_file():
        clients, workers = [], []
        for node_root in json.loads(marker.read_text(encoding='utf-8')):
            node_clients, node_workers = discover_log_roots(Path(node_root), prefix)
            clients.extend(node_clients)
            workers.extend(node_workers)
        return sorted(set(clients)), sorted(set(workers))
    if prefix is None:
        client_root, worker_root = root / 'collected', root / 'collected_worker_logs'
        if client_root.is_dir() or worker_root.is_dir():
            return ([client_root] if client_root.is_dir() else [],
                    [worker_root] if worker_root.is_dir() else [])
    scopes = sorted((path for path in root.iterdir() if path.is_dir() and
                     (prefix is None or path.name.startswith(prefix))), key=lambda path: str(path))
    if prefix is not None and not scopes:
        raise ValueError(f'no second-level directory under {root} starts with {prefix!r}')
    clients, workers = set(), set()
    for scope in scopes:
        for directory, _, filenames in os.walk(scope, onerror=lambda _: None):
            names = [name.lower() for name in filenames]
            has_client = any(name.startswith('ds_client_access') and
                             name.endswith(('.log', '.log.gz')) for name in names)
            has_worker = any(name.startswith('access') and not name.startswith('ds_client_access') and
                             name.endswith(('.log', '.log.gz')) for name in names)
            if has_client:
                clients.add(Path(directory))
            if has_worker:
                workers.add(Path(directory))
    return sorted(clients), sorted(workers)


def _read_path(path: Path) -> Iterable[str]:
    opener = gzip.open if path.suffix == '.gz' else open
    try:
        with opener(path, 'rt', encoding='utf-8', errors='replace') as stream:
            yield from stream
    except (OSError, UnicodeError, PermissionError):
        return


def _log_paths(root: Path, access_only=False, client=False):
    for directory, _, filenames in os.walk(root, onerror=lambda _: None):
        for filename in sorted(filenames):
            if access_only and not (filename.endswith('.log') or filename.endswith('.log.gz')):
                continue
            if access_only:
                if client and not filename.startswith('ds_client_access'):
                    continue
                if not client and not filename.startswith('access'):
                    continue
            yield Path(directory, filename)


def _parts(line: str):
    match = LINE_RE.match(line.rstrip('\n'))
    if not match:
        return None
    parts = [part.strip() for part in match.group('body').split('|')]
    return parts if len(parts) >= 9 else None


def _trace(parts):
    message = '|'.join(parts[7:]) if len(parts) > 7 else ''
    match = TRACE_RE.search(message)
    value = parts[5] or (match.group(1) if match else '')
    return '' if value in {'', '-', 'null', 'None'} else value


def _client_record(line: str, mode: str, base: int):
    parts = _parts(line)
    if not parts or not parts[8].startswith('DS_KV_CLIENT_'):
        return None
    trace = _trace(parts)
    if not trace:
        return None
    client = parts[3] if IP_RE.fullmatch(parts[3]) else 'unknown'
    if mode == 'core':
        try:
            code = int(parts[7])
        except ValueError:
            return None
        if code == 0:
            return None
        return {'trace': trace, 'client': client, 'code': str(code)}
    if len(parts) < 10:
        return None
    try:
        latency = float(parts[9])
    except ValueError:
        return None
    return {'trace': trace, 'client': client, 'latency': latency} if latency > base else None


def _worker_record(line: str):
    parts = _parts(line)
    if not parts or parts[7] != '0' or not parts[8].startswith('DS_POSIX_'):
        return None
    trace = _trace(parts)
    return (trace, parts[3]) if trace and IP_RE.fullmatch(parts[3]) else None


def _scan_client(path: str, mode: str, base: int):
    records = []
    for line in _read_path(Path(path)):
        record = _client_record(line, mode, base)
        if record:
            record['directory'] = str(Path(path).parent)
            records.append(record)
    return records


def _init_worker_scan(trace_ids):
    global _TARGET_TRACES
    _TARGET_TRACES = set(trace_ids)


def _scan_worker(path: str):
    records = []
    for line in _read_path(Path(path)):
        record = _worker_record(line)
        if record and record[0] in _TARGET_TRACES:
            records.append((record[0], record[1], str(Path(path).parent)))
    return records


def _collect(paths, scan, jobs, label, initializer=None, initargs=()):
    if not paths:
        print(f'  No {label} access logs found')
        return []
    print(f'  Scanning {len(paths)} {label} access logs with {min(jobs, len(paths))} processes...')
    records, started = [], time.monotonic()
    with ProcessPoolExecutor(max_workers=jobs, initializer=initializer, initargs=initargs) as executor:
        futures = {executor.submit(scan, str(path)): path for path in paths}
        for finished, future in enumerate(as_completed(futures), 1):
            records.extend(future.result())
            print(f'  {label} progress: {finished}/{len(paths)} | {futures[future].name} | '
                  f'elapsed {time.monotonic() - started:.1f}s', flush=True)
    return records


def _sample(values, maximum):
    return random.sample(values, maximum) if len(values) > maximum else list(values)


def _find_code_examples(choices, traces):
    """Find full log lines in only the chosen trace's client and worker directories."""
    examples = {code: {'trace_id': trace_id, 'lines': []} for code, trace_id in choices.items()}
    trace_codes = {trace_id: code for code, trace_id in choices.items()}
    directory_traces = defaultdict(set)
    for code, trace_id in choices.items():
        state = traces[trace_id]
        for directory in {state['access']['directory']} | state['worker_dirs']:
            directory_traces[directory].add(trace_id)
    for directory, wanted in directory_traces.items():
        for path in _log_paths(Path(directory)):
            for line in _read_path(path):
                parts = _parts(line)
                trace_id = _trace(parts) if parts else ''
                if trace_id in wanted:
                    examples[trace_codes[trace_id]]['lines'].append(line.rstrip('\n'))
    return examples


def _host_ip_from_directory(directory):
    """Read the first HOST record from the lexicographically earliest plain INFO log."""
    try:
        info_files = sorted(path for path in Path(directory).iterdir()
                            if path.is_file() and '.INFO.' in path.name.upper() and path.suffix != '.gz')
    except OSError:
        return 'unknown'
    if not info_files:
        return 'unknown'
    for line in _read_path(info_files[0]):
        host = HOST_RE.search(line)
        if host:
            address = IP_RE.search(host.group('value'))
            return address.group(0) if address else 'unknown'
    return 'unknown'


def _host_pages(client_directories, worker_directories):
    """Group pod IPs by their earliest INFO log's HOST value, preserving 20-row pages."""
    groups = defaultdict(lambda: {'clients': set(), 'workers': set()})
    for pod_ip, directories in client_directories.items():
        host = _host_ip_from_directory(min(directories)) if directories else 'unknown'
        groups[host]['clients'].add(pod_ip)
    for pod_ip, directories in worker_directories.items():
        host = _host_ip_from_directory(min(directories)) if directories else 'unknown'
        groups[host]['workers'].add(pod_ip)
    rows = []
    for host in sorted(groups, key=lambda value: (value == 'unknown', value)):
        clients = sorted(groups[host]['clients']) or ['']
        workers = sorted(groups[host]['workers']) or ['']
        size = max(len(clients), len(workers))
        for offset in range(0, size, 20):
            chunk_size = min(20, size - offset)
            for index in range(chunk_size):
                position = offset + index
                rows.append({'host': host if index == 0 else '', 'rowspan': chunk_size if index == 0 else 0,
                             'client': clients[position] if position < len(clients) else '',
                             'worker': workers[position] if position < len(workers) else ''})
    return {str(index + 1): rows[index * 20:(index + 1) * 20]
            for index in range((len(rows) + 19) // 20)}


def build_report(root: Path, mode: str, jobs: int, base: int, max_trace: int, prefix: str | None = None):
    client_roots, worker_roots = discover_log_roots(root, prefix)
    client_paths = [path for log_root in client_roots
                    for path in _log_paths(log_root, access_only=True, client=True)]
    worker_paths = [path for log_root in worker_roots
                    for path in _log_paths(log_root, access_only=True, client=False)]
    print(f'  Client access logs: {len(client_paths)}; worker access logs: {len(worker_paths)}')
    if mode == 'time':
        print(f'  Latency baseline: {base} (5000 = 5ms)')
    records = _collect(client_paths, partial(_scan_client, mode=mode, base=base), jobs, 'client')
    traces = {}
    client_directories = defaultdict(set)
    for record in records:
        client_directories[record['client']].add(record['directory'])
        current = traces.get(record['trace'])
        if current is None or (mode == 'time' and record['latency'] > current['access']['latency']):
            traces[record['trace']] = {'access': record, 'workers': set(), 'worker_dirs': set()}
    print(f'  Found {len(traces)} {"slow" if mode == "time" else "error"} client traces')
    worker_records = _collect(worker_paths, _scan_worker, jobs, 'worker', _init_worker_scan, (tuple(traces),))
    worker_directories = defaultdict(set)
    for trace_id, worker, directory in worker_records:
        traces[trace_id]['workers'].add(worker)
        traces[trace_id]['worker_dirs'].add(directory)
        worker_directories[worker].add(directory)

    edges, code_samples = {}, defaultdict(list)
    for trace_id, state in traces.items():
        access = state['access']
        workers = state['workers'] or {'unknown'}
        if mode == 'core':
            code_samples[access['code']].append(trace_id)
        for worker in workers:
            key = (access['client'], worker) if mode == 'time' else (access['client'], worker, access['code'])
            edge = edges.setdefault(key, {'id': len(edges), 'client': access['client'], 'worker': worker,
                'code': access.get('code'), 'count': 0, 'trace_ids': [], 'latency_sum': 0.0, 'max_latency': 0.0})
            edge['count'] += 1
            edge['trace_ids'].append(trace_id)
            if mode == 'time':
                edge['latency_sum'] += access['latency']
                edge['max_latency'] = max(edge['max_latency'], access['latency'])

    details, output_edges, worker_totals = {}, [], defaultdict(int)
    for edge in edges.values():
        details[str(edge['id'])] = {'trace_ids': _sample(edge.pop('trace_ids'), max_trace)}
        worker_totals[edge['worker']] += edge['count']
        if mode == 'time':
            edge['avg_latency'] = edge.pop('latency_sum') / edge['count']
        else:
            edge.pop('latency_sum')
            edge.pop('max_latency')
        output_edges.append(edge)
    output_edges.sort(key=lambda edge: (-worker_totals[edge['worker']], edge['worker'], edge['id']))
    worker_ranks = {worker: rank for rank, worker in enumerate(
        sorted((worker for worker in worker_totals if worker != 'unknown'),
               key=lambda worker: (-worker_totals[worker], worker)), 1)}

    link_totals = defaultdict(int)
    for edge in output_edges:
        link_totals[(edge['client'], edge['worker'])] += edge['count']
    denominator = len(traces)
    rows = [{'link': f'{client} → {worker}', 'count': count,
             'ratio': count / denominator * 100 if denominator else 0}
            for (client, worker), count in link_totals.items()]
    rows.sort(key=lambda row: (-row['count'], row['link']))
    def make_pages(items):
        return {str(index + 1): items[index * 20:(index + 1) * 20]
                for index in range((len(items) + 19) // 20)}
    client_totals = defaultdict(int)
    worker_totals_full = defaultdict(int)
    for (client, worker), count in link_totals.items():
        client_totals[client] += count
        worker_totals_full[worker] += count
    client_rows = [{'link': client, 'count': count, 'ratio': count / denominator * 100 if denominator else 0}
                   for client, count in client_totals.items()]
    worker_rows = [{'link': worker, 'count': count, 'ratio': count / denominator * 100 if denominator else 0}
                   for worker, count in worker_totals_full.items()]
    client_rows.sort(key=lambda row: (-row['count'], row['link']))
    worker_rows.sort(key=lambda row: (-row['count'], row['link']))
    pages = {'link': make_pages(rows), 'client': make_pages(client_rows), 'worker': make_pages(worker_rows),
             'host': _host_pages(client_directories, worker_directories)}
    examples = _find_code_examples(
        {code: random.choice(trace_ids) for code, trace_ids in code_samples.items()}, traces) if mode == 'core' else {}
    return {'mode': mode, 'edges': output_edges, 'details': details, 'pages': pages, 'examples': examples,
            'summary': {'trace_count': len(traces), 'edge_count': len(output_edges),
                        'client_count': len({edge['client'] for edge in output_edges}),
                        'worker_count': len(worker_totals), 'worker_totals': dict(worker_totals),
                        'worker_ranks': worker_ranks,
                        'page_counts': {kind: len(value) for kind, value in pages.items()}}}


def render_html(report, source='', data_dir=None):
    data_dir_name = Path(data_dir).name if data_dir else ''
    index = {'mode': report['mode'], 'data_dir': data_dir_name, 'edges': report['edges'], 'summary': report['summary'],
             'example_codes': sorted(report['examples'])}
    blocks = {f'detail-{key}': value for key, value in report['details'].items()}
    for kind, pages in report['pages'].items():
        blocks.update({f'{kind}-page-{key}': value for key, value in pages.items()})
    blocks.update({f'example-{code}': value for code, value in report['examples'].items()})
    if data_dir:
        Path(data_dir).mkdir(parents=True, exist_ok=True)
        for key, value in blocks.items():
            (Path(data_dir) / f'{key}.js').write_text(
                f'window.__TRACE_TOPOLOGY_BLOCK__={json.dumps(value, ensure_ascii=False, separators=(",", ":"))};',
                encoding='utf-8')
    encoded = ''
    payload = json.dumps(index, ensure_ascii=False).replace('</', '<\\/')
    title = html.escape(f'Trace {report["mode"].title()} Topology - {source}')
    template = r'''<!doctype html><html lang="zh-CN"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>__TITLE__</title><style>
*{box-sizing:border-box}body{margin:0;background:#0f172a;color:#e2e8f0;font:14px system-ui,sans-serif;overflow:hidden}.bar{height:58px;padding:12px 20px;background:#111c35;border-bottom:1px solid #263554;display:flex;align-items:center;gap:12px}.bar h1{font-size:18px;margin:0 10px 0 0}.stats,.hint{color:#94a3b8}button,select{background:#2563eb;border:0;color:#fff;padding:7px 10px;border-radius:6px;cursor:pointer}input{width:210px;background:#0b1224;color:#e2e8f0;border:1px solid #475569;border-radius:6px;padding:8px 10px}#viewport{position:absolute;inset:58px 360px 280px 0;overflow:hidden;background:#0b1224;cursor:grab}canvas{display:block;width:100%;height:100%}#drawer{position:absolute;right:0;top:58px;bottom:0;width:360px;background:#111827;border-left:1px solid #263554;padding:20px;overflow:auto}#bottom{position:absolute;left:0;right:360px;bottom:0;height:280px;padding:14px 20px;background:#111827;border-top:1px solid #263554;overflow:auto}h2,h3{margin:0 0 10px}.trace,.code{border-top:1px solid #263554;padding:8px 0}.code button{background:none;padding:0;color:#93c5fd}.pager{display:flex;align-items:center;gap:8px;margin-top:10px;flex-wrap:wrap}.pager input{width:70px;padding:7px}table{width:100%;border-collapse:collapse}th,td{text-align:left;padding:7px;border-bottom:1px solid #263554}th{color:#94a3b8}pre{white-space:pre-wrap;word-break:break-all;background:#0b1224;padding:10px;height:150px;overflow:auto}#examples{margin-top:16px;min-height:190px}
</style></head><body><header class="bar"><h1>Trace 拓扑</h1><span class="stats" id="stats"></span><input id="search" placeholder="搜索 client / worker IP"><button id="fit">适配全图</button><button id="reset">重置视图</button><button id="export">导出 PNG</button></header><main id="viewport"><canvas id="topology"></canvas></main><aside id="drawer"><h2>链路详情</h2><p class="hint">点击 Worker 或连线，按需加载随机 trace 样本。</p></aside><section id="bottom"><div class="tabs"><button data-sheet="link">Client → Worker</button><button data-sheet="client">Client 排行</button><button data-sheet="worker">Worker 排行</button></div><div id="tableContent"></div></section>__BLOCKS__<script>
const DATA=__PAYLOAD__,MODE=DATA.mode,canvas=document.querySelector('#topology'),ctx=canvas.getContext('2d'),viewport=document.querySelector('#viewport'),drawer=document.querySelector('#drawer'),tableContent=document.querySelector('#tableContent');document.querySelector('.tabs').insertAdjacentHTML('beforeend','<button data-sheet="host">Host → Pod</button>');let decoded=null;
async function loadBlock(id){decoded=null;window.__TRACE_TOPOLOGY_BLOCK__=null;const script=document.createElement('script');script.src=DATA.data_dir+'/'+id+'.js';document.head.append(script);return await new Promise((resolve,reject)=>{script.onload=()=>{const value=window.__TRACE_TOPOLOGY_BLOCK__;script.remove();window.__TRACE_TOPOLOGY_BLOCK__=null;resolve(value)};script.onerror=()=>{script.remove();reject(new Error('无法加载数据文件: '+script.src))}})}
const clients=[...new Set(DATA.edges.map(e=>e.client))].sort(),workers=[...new Set(DATA.edges.map(e=>e.worker))].sort((a,b)=>sum(b)-sum(a)||a.localeCompare(b)),W=Math.max(1600,clients.length*180+workers.length*220+500),H=Math.max(900,Math.max(clients.length,workers.length)*96+180),pos={},hits=[];let view={x:0,y:0,w:W,h:H},drag=null,frame=0,query='';function sum(w){return DATA.summary.worker_totals[w]||0}document.querySelector('#stats').textContent=`${MODE} · trace ${DATA.summary.trace_count} · client ${clients.length} · worker ${workers.length} · edge ${DATA.edges.length}`;clients.forEach((ip,i)=>pos['c'+ip]={x:150,y:90+i*96,kind:'client',ip});workers.forEach((ip,i)=>pos['w'+ip]={x:W-210,y:90+i*96,kind:'worker',ip});
const colors=['#60a5fa','#a78bfa','#34d399','#fbbf24','#fb7185','#f472b6','#c084fc'];function resize(){const r=viewport.getBoundingClientRect(),d=devicePixelRatio||1;canvas.width=r.width*d;canvas.height=r.height*d;ctx.setTransform(d,0,0,d,0,0);draw()}function screen(p){const r=canvas.getBoundingClientRect();return{x:(p.x-view.x)*r.width/view.w,y:(p.y-view.y)*r.height/view.h}}function world(e){const r=canvas.getBoundingClientRect();return{x:view.x+(e.clientX-r.left)*view.w/r.width,y:view.y+(e.clientY-r.top)*view.h/r.height}}function box(x,y,w,h,r){ctx.beginPath();ctx.roundRect(x,y,w,h,r);ctx.fill();ctx.stroke()}function draw(){frame=0;const r=canvas.getBoundingClientRect();ctx.clearRect(0,0,r.width,r.height);hits.length=0;const z=r.width/view.w;DATA.edges.forEach((e,i)=>{const a=screen(pos['c'+e.client]),b=screen(pos['w'+e.worker]),off=MODE==='time'?0:(i%7-3)*8*z,mid=(a.x+b.x)/2,active=!query||e.client.includes(query)||e.worker.includes(query);ctx.globalAlpha=active?.84:.12;ctx.strokeStyle=colors[i%colors.length];ctx.lineWidth=active?2.2:1;ctx.beginPath();ctx.moveTo(a.x+99*z,a.y+off);ctx.bezierCurveTo(mid-180*z,a.y+off,mid+180*z,b.y+off,b.x-99*z,b.y+off);ctx.stroke();if(z>.26){const label=MODE==='time'?`${e.count} slow · max ${e.max_latency}`:`${e.code} × ${e.count}`;ctx.font=`${Math.max(10,11*z)}px system-ui`;const tw=ctx.measureText(label).width+10;ctx.fillStyle='#0b1224';ctx.fillRect(mid-tw/2,(a.y+b.y)/2-13,tw,18);ctx.fillStyle='#f8fafc';ctx.globalAlpha=active?1:.2;ctx.fillText(label,mid-tw/2+5,(a.y+b.y)/2);hits.push({type:'edge',edge:e,x:mid-tw/2,y:(a.y+b.y)/2-15,w:tw,h:22})}});Object.values(pos).forEach(n=>{const p=screen(n),active=!query||n.ip.includes(query),w=196*z,h=54*z;ctx.globalAlpha=active?1:.28;ctx.fillStyle=n.kind==='worker'?'#164e63':'#172554';ctx.strokeStyle=n.kind==='worker'?'#22d3ee':'#60a5fa';ctx.lineWidth=active?2:1;box(p.x-w/2,p.y-h/2,w,h,9*z);ctx.textAlign='center';ctx.fillStyle='#93c5fd';ctx.font=`700 ${Math.max(8,10*z)}px system-ui`;ctx.fillText(n.kind.toUpperCase(),p.x,p.y-6*z);ctx.fillStyle='#f8fafc';ctx.font=`600 ${Math.max(10,15*z)}px system-ui`;ctx.fillText(n.kind==='worker'&&n.ip!=='unknown'?n.ip+' #'+DATA.summary.worker_ranks[n.ip]:n.ip,p.x,p.y+14*z);hits.push({type:n.kind,ip:n.ip,x:p.x-w/2,y:p.y-h/2,w,h})});ctx.globalAlpha=1;ctx.textAlign='start'}function schedule(){if(!frame)frame=requestAnimationFrame(draw)}function fit(){view={x:-90,y:-90,w:W+180,h:H+180};schedule()}function esc(v){return String(v).replaceAll('&','&amp;').replaceAll('<','&lt;')}
async function showEdge(e){drawer.innerHTML='<h2>'+esc(e.client)+' → '+esc(e.worker)+'</h2><p class="hint">正在加载 trace 样本…</p>';const d=await loadBlock('detail-'+e.id);drawer.innerHTML='<h2>'+esc(e.client)+' → '+esc(e.worker)+'</h2><p>'+(MODE==='time'?'慢请求':'错误')+'总数：<b>'+e.count+'</b>'+(MODE==='core'?' · code '+esc(e.code):' · 最大 '+e.max_latency+' · 平均 '+e.avg_latency.toFixed(1))+'；随机样本 '+d.trace_ids.length+' 条</p>'+d.trace_ids.map(t=>'<div class="trace"><b>'+esc(t)+'</b></div>').join('')}
function showWorker(w){const rows=DATA.edges.filter(e=>e.worker===w).sort((a,b)=>b.count-a.count);drawer.innerHTML='<h2>WORKER '+esc(w)+'</h2><p class="hint">总数：'+sum(w)+'；点击条目加载样本。</p>'+rows.map(e=>'<div class="code"><button data-id="'+e.id+'">'+(MODE==='time'?esc(e.client):esc(e.client)+' · '+esc(e.code))+' × '+e.count+'</button></div>').join('');drawer.querySelectorAll('[data-id]').forEach(b=>b.onclick=()=>showEdge(DATA.edges.find(e=>e.id===Number(b.dataset.id))))}
let sheet='link';async function showPage(n){n=Math.max(1,Math.min(DATA.summary.page_counts[sheet],Number(n)||1));tableContent.innerHTML='<p class="hint">正在加载 '+sheet+' 第 '+n+' 页…</p>';const rows=await loadBlock(sheet+'-page-'+n);if(sheet==='host'){tableContent.innerHTML='<table><thead><tr><th>Host IP</th><th>Client Pod IP</th><th>Worker Pod IP</th></tr></thead><tbody>'+rows.map(r=>'<tr>'+(r.host?'<td rowspan="'+r.rowspan+'">'+esc(r.host)+'</td>':'')+'<td>'+esc(r.client)+'</td><td>'+esc(r.worker)+'</td></tr>').join('')+'</tbody></table>'}else{tableContent.innerHTML='<table><thead><tr><th>#</th><th>'+ (sheet==='link'?'链路':sheet==='client'?'Client':'Worker') +'</th><th>'+(MODE==='time'?'慢请求':'错误')+'总数</th><th>全量占比</th></tr></thead><tbody>'+rows.map((r,i)=>'<tr><td>'+((n-1)*20+i+1)+'</td><td>'+esc(r.link)+'</td><td>'+r.count+'</td><td>'+r.ratio.toFixed(2)+'%</td></tr>').join('')+'</tbody></table>'}tableContent.innerHTML+='<div class="pager"><button data-prev>上一页</button><span>第 '+n+' / '+DATA.summary.page_counts[sheet]+' 页</span><button data-next>下一页</button><label>跳转 <input id="pageInput" type="number" min="1" max="'+DATA.summary.page_counts[sheet]+'" value="'+n+'"></label><button data-go>确定</button></div>';tableContent.querySelector('[data-prev]').onclick=()=>showPage(n-1);tableContent.querySelector('[data-next]').onclick=()=>showPage(n+1);tableContent.querySelector('[data-go]').onclick=()=>showPage(tableContent.querySelector('#pageInput').value)}document.querySelectorAll('[data-sheet]').forEach(b=>b.onclick=()=>{sheet=b.dataset.sheet;tableContent.replaceChildren();showPage(1)})
viewport.addEventListener('wheel',e=>{e.preventDefault();const p=world(e),factor=e.deltaY<0?.82:1.22,nw=Math.max(260,Math.min(W*2,view.w*factor)),nh=nw*view.h/view.w;view.x=p.x-(p.x-view.x)*nw/view.w;view.y=p.y-(p.y-view.y)*nh/view.h;view.w=nw;view.h=nh;schedule()},{passive:false});viewport.addEventListener('mousedown',e=>{const h=hits.findLast(h=>e.offsetX>=h.x&&e.offsetX<=h.x+h.w&&e.offsetY>=h.y&&e.offsetY<=h.y+h.h);if(h){if(h.type==='worker')showWorker(h.ip);else if(h.type==='edge')showEdge(h.edge);return}drag={x:e.clientX,y:e.clientY,vx:view.x,vy:view.y}});window.addEventListener('mouseup',()=>drag=null);window.addEventListener('mousemove',e=>{if(!drag)return;const r=canvas.getBoundingClientRect();view.x=drag.vx-(e.clientX-drag.x)*view.w/r.width;view.y=drag.vy-(e.clientY-drag.y)*view.h/r.height;schedule()});document.querySelector('#fit').onclick=fit;document.querySelector('#reset').onclick=()=>{view={x:0,y:0,w:W,h:H};schedule()};document.querySelector('#search').oninput=e=>{query=e.target.value.trim();schedule()};document.querySelector('#export').onclick=()=>{const a=document.createElement('a');a.href=canvas.toDataURL('image/png');a.download='trace-topology.png';a.click()};new ResizeObserver(resize).observe(viewport);fit();
showPage(1);
if(MODE==='core'){const box=document.createElement('div');box.id='examples';box.innerHTML='<h3>错误码参考日志</h3><select id="codeSelect"><option value="">请选择 code</option>'+DATA.example_codes.map(c=>'<option value="'+esc(c)+'">'+esc(c)+'</option>').join('')+'</select><pre id="codeLog">选择错误码后按需加载一条完整 trace。\n\n\n\n\n\n\n\n\n\n</pre>';document.querySelector('#bottom').append(box);const sel=document.querySelector('#codeSelect');sel.onchange=async()=>{if(!sel.value)return;document.querySelector('#codeLog').textContent='正在加载…';const x=await loadBlock('example-'+sel.value);document.querySelector('#codeLog').textContent='trace '+x.trace_id+'\n\n'+x.lines.join('\n')}}
</script></body></html>'''
    return template.replace('__TITLE__', title).replace('__PAYLOAD__', payload).replace('__BLOCKS__', encoded)


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('-f', '--dir', dest='input_dir', type=Path,
                        help='root directory containing standard or mixed collections')
    parser.add_argument('legacy_input_dir', type=Path, nargs='?',
                        help=argparse.SUPPRESS)
    parser.add_argument('-p', '--prefix', help='only scan second-level mixed collection directories with this prefix')
    parser.add_argument('--cross-node', action='store_true', help='fetch the root from Kubernetes nodes before local processing')
    parser.add_argument('-t', '--type', choices=('core', 'time'), default='core',
                        help='core: non-zero errors; time: slow requests (default: core)')
    parser.add_argument('--base', type=int, default=5000, help='time baseline; 5000 = 5ms')
    parser.add_argument('-j', '--jobs', type=int, default=DEFAULT_JOBS,
                        help=f'access-log scan processes (default: {DEFAULT_JOBS})')
    parser.add_argument('--max-trace', type=int, default=50,
                        help='random trace IDs retained per drawer item; totals remain full (default: 50)')
    parser.add_argument('-o', '--output', type=Path, default=Path('trace_topology_report.html'),
                        help='HTML name inside the generated .tar.gz (default: trace_topology_report.html)')
    args = parser.parse_args()
    if args.input_dir is None:
        args.input_dir = args.legacy_input_dir
    elif args.legacy_input_dir is not None:
        parser.error('use either -f/--dir or the legacy positional input directory, not both')
    if args.input_dir is None:
        parser.error('-f/--dir is required')
    if args.jobs < 1 or args.max_trace < 1 or args.base < 0:
        parser.error('--jobs and --max-trace must be positive; --base must be non-negative')
    if args.cross_node:
        try:
            args.input_dir = stage_cross_node(args.input_dir, args.jobs, args.prefix, ['|'] if args.type in ('core', 'time') else None)
        except Exception as exc:
            parser.error(f'cross-node staging failed: {exc}')
    try:
        report = build_report(args.input_dir, args.type, args.jobs, args.base, args.max_trace, args.prefix)
    except ValueError as exc:
        parser.error(str(exc))
    output = args.output.resolve()
    archive = output.with_suffix('.tar.gz')
    archive.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix=f'.{output.stem}-', dir=archive.parent) as staging:
        staging_dir = Path(staging)
        html_path = staging_dir / output.name
        data_dir = staging_dir / f'{output.stem}_data'
        html_path.write_text(render_html(report, str(args.input_dir), data_dir), encoding='utf-8')
        subprocess.run(['tar', '-czf', str(archive), '-C', str(staging_dir),
                        html_path.name, data_dir.name], check=True)
    print(f'Wrote {archive} ({report["summary"]["edge_count"]} edges, '
          f'{report["summary"]["trace_count"]} traces); extract it and open {output.name}')


if __name__ == '__main__':
    main()
