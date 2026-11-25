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

# Description: Extract error or slow-request trace IDs and collect their related client and worker log
#   evidence.
# Usage: python3 scripts/perf_analyze/trace_collector.py -f logs --type all-core; use --help for core,
#   percentile, and time-buckets modes.

"""
trace_collector.py - Unified trace collection tool for scripts/perf_analyze.

Supports four modes:
  1. core: Extract traces from access logs by search string, then search in parallel.
  2. time-buckets: Extract multiple operation/latency buckets in one pass and search only related logs.
  3. percentile: Extract high-latency traces by percentile, then search in parallel.
  4. all-core: Extract traces for every non-zero access-log status code, then search in parallel.

Directory layout:
  Pass the collection root with -f. Standard collected/ and
  collected_worker_logs/ layouts are discovered automatically. For mixed
  second-level collections, use -p to select directories by prefix.

  /parent/                         # -f root directory
  |-- unique_traces_*.txt           # Generated trace files
  |-- collected/                    # Client logs used to extract traces
  |   |-- client-worker/
  |   |   |-- ds_client_access.log  # Trace source
  |   |   |-- ds_client.INFO.log    # Detailed log search source
  |   |   `-- ds_client.INFO.log.gz # Compressed detailed log search source
  |   `-- ...
  |-- collected_worker_logs/        # Worker logs used only for searching
  |   |-- worker/
  |   |   `-- kvcache.INFO.log
  |   `-- ...
  `-- trace_collect/                # Output root (default)
      |-- core/                     # Core-mode results
      |-- time/                     # Time-mode results
      `-- percentile/               # Percentile-mode results

Examples:
  # mixed second-level collections selected by prefix
  python3 trace_collector.py -f /path/to/rootDir -p jingpai --type all-core

  # core mode: extract traces by search string
  python3 trace_collector.py --type core "| 1001 | DS"
  python3 trace_collector.py --type core "| 1001 | DS:| 1002 | DS"

  # all-core mode: collect every non-zero access-log status code
  python3 trace_collector.py --type all-core

  # time-buckets mode: scan access logs once for multiple operations and ranges
  python3 trace_collector.py --type time-buckets \
      --ops DS_KV_CLIENT_GET,DS_KV_CLIENT_SET \
      --ranges 5000,7000 7000,10000 10000,20000 20000

  # percentile mode: extract high-latency traces by percentile
  python3 trace_collector.py --type percentile DS_KV_CLIENT_GET P99
  python3 trace_collector.py --type percentile DS_KV_CLIENT_GET P99.9
  python3 trace_collector.py --type percentile "DS_KV_CLIENT_GET:DS_KV_CLIENT_PUT" P99.99
"""

import os
import sys
import subprocess
import re
import random
import math
import argparse
import gzip
import multiprocessing
import time
import shlex
import json
import io
import tarfile
import tempfile
import atexit
import shutil
import shlex
import base64
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed


def cross_log(message):
    print(f'[{time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())}.{int(time.time() * 1000) % 1000:03d}] [cross-node] {message}', file=sys.stderr, flush=True)


def tree_bytes(root):
    return sum(os.path.getsize(os.path.join(directory, name))
               for directory, _, names in os.walk(root) for name in names)


def archive_command(root, prefix=None):
    parent, name = os.path.dirname(root), os.path.basename(root)
    find_expr = "-type f \\( -name '*.log' -o -name '*.log.*' -o -name '*.gz' -o -iname '*INFO*' -o -iname '*metrics*' \\)"
    scope = (f"find {shlex.quote(name)} -mindepth 1 -maxdepth 1 -type d -name {shlex.quote(prefix + '*')} -exec find {{}} {find_expr} -print0 \\;" if prefix else
             f"find {shlex.quote(name)} \\( -path '*/collected' -o -path '*/collected_worker_logs' \\) -type d -exec find {{}} {find_expr} -print0 \\;")
    return (f"cd -- {shlex.quote(parent)} && {scope} | "
            "tar --null --files-from=- -I 'gzip -1' -cf -")


def prefilter_archive_command(root, prefix, patterns):
    """Build a remote command that filters client traces before transfer."""
    script = '''import gzip,json,pathlib,sys,tempfile,tarfile,shutil
root=pathlib.Path(sys.argv[1]).resolve(); prefix=sys.argv[2] or None; needles=json.loads(sys.argv[3])
scopes=[x for x in root.iterdir() if x.is_dir() and (prefix is None or x.name.startswith(prefix))]
if prefix is None:
    scopes=[x for x in (root/'collected',root/'collected_worker_logs') if x.is_dir()] or scopes
def read(path):
    opener=gzip.open if path.name.endswith('.gz') else open
    with opener(path,'rt',encoding='utf-8',errors='replace') as stream:
        yield from stream
clients=[]; workers=[]
for scope in scopes:
    for path in scope.rglob('*.log*'):
        if path.is_file():
            name=path.name.lower()
            if name.startswith('ds_client_access'): clients.append(path)
            elif name.startswith('access') and not name.startswith('ds_client_access'): workers.append(path)
traces=set()
for path in clients:
    for line in read(path):
        if any(x in line for x in needles):
            fields=line.split('|')
            if len(fields)>5 and fields[5].strip(): traces.add(fields[5].strip())
out=pathlib.Path(tempfile.mkdtemp(prefix='perf_trace_bundle_'))/root.name
(out/'collected'/'client').mkdir(parents=True); (out/'collected_worker_logs'/'worker').mkdir(parents=True)
def write_matches(paths,target):
    with target.open('w',encoding='utf-8') as stream:
        for path in paths:
            for line in read(path):
                if any(trace in line for trace in traces): stream.write(line)
write_matches(clients,out/'collected'/'client'/'ds_client_access.log')
write_matches(workers,out/'collected_worker_logs'/'worker'/'access.log')
write_matches(clients+workers,out/'collected'/'client'/'matched.INFO.log')
write_matches(clients+workers,out/'collected_worker_logs'/'worker'/'matched.INFO.log')
with tarfile.open(fileobj=sys.stdout.buffer,mode='w|gz',compresslevel=1) as archive: archive.add(out,arcname=root.name)
shutil.rmtree(out.parent,ignore_errors=True)
'''
    encoded = base64.b64encode(script.encode()).decode()
    return 'python3 -c "import base64;exec(base64.b64decode({}))" {} {} {}'.format(
        shlex.quote(repr(encoded)), shlex.quote(os.path.abspath(root)), shlex.quote(prefix or ''),
        shlex.quote(json.dumps(list(patterns), separators=(',', ':'))))


def stage_cross_node(root_dir: str, jobs: int, prefix=None, prefilter_patterns=None) -> str:
    root = os.path.abspath(os.path.expanduser(root_dir))
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
    stage = tempfile.mkdtemp(prefix='perf_analyze_cross_node_')
    atexit.register(shutil.rmtree, stage, ignore_errors=True)
    cross_log(f'phase=stage_created path={stage} staged_bytes={tree_bytes(stage)}')
    marker = os.path.join(stage, '.cross_node_roots')
    with open(marker, 'w', encoding='utf-8') as stream:
        json.dump([], stream)
    def fetch(target):
        node, ip = target
        fetch_started = time.monotonic()
        cross_log(f'node={node} phase=ssh_tar enter ip={ip}')
        command = (prefilter_archive_command(root, prefix, prefilter_patterns)
                   if prefilter_patterns else archive_command(root, prefix))
        result = subprocess.run(['ssh', '-o', 'BatchMode=yes', '-o', 'ConnectTimeout=10', f'root@{ip}', command],
                                stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=False)
        if result.returncode:
            return node, False, result.stderr.decode(errors='replace').strip(), time.monotonic() - fetch_started
        destination = os.path.join(stage, re.sub(r'[^A-Za-z0-9_.-]+', '_', node))
        os.makedirs(destination, exist_ok=True)
        with tarfile.open(fileobj=io.BytesIO(result.stdout), mode='r:gz') as archive:
            archive.extractall(destination, filter='data')
        return node, True, destination, len(result.stdout), tree_bytes(destination), time.monotonic() - fetch_started
    failures = 0
    with ThreadPoolExecutor(max_workers=max(1, min(jobs, len(targets)))) as pool:
        for future in as_completed([pool.submit(fetch, target) for target in targets]):
            node, ok, value, *extra = future.result()
            if not ok:
                failures += 1
                cross_log(f'node={node} phase=ssh_tar leave status=failed elapsed={extra[0]:.3f}s error={value}')
            else:
                cross_log(f'node={node} phase=ssh_tar leave status=ok elapsed={extra[2]:.3f}s transfer_bytes={extra[0]} staged_bytes={extra[1]} stage={value}')
                with open(marker, 'r+', encoding='utf-8') as stream:
                    roots = json.load(stream)
                    roots.append(os.path.join(value, name))
                    stream.seek(0)
                    json.dump(roots, stream)
                    stream.truncate()
    if failures == len(targets):
        raise RuntimeError('all cross-node fetches failed')
    cross_log(f'phase=stage leave status=ok elapsed={time.monotonic() - started:.3f}s stage={stage} staged_bytes={tree_bytes(stage)} succeeded={len(targets) - failures} failed={failures}')
    return stage


DEFAULT_MAX_TRACES = 96
DEFAULT_JOBS = 32
ACCESS_STATUS_CODE_RE = re.compile(r'\|\s*(\d+)\s*\|\s*DS(?:_|\b)')
CLIENT_ACCESS_LOG_RE = re.compile(r'^ds_client_access(?:_.*)?\.log(?:[.-].*)?$')
GREP_LINE_RE = re.compile(r'^(.*):(\d+):(.*)$')


def sanitize_filename(name: str) -> str:
    """Convert a trace string to a valid filename."""
    name = name.strip()
    invalid_chars = '/\\:*?"<>|\n\r\t'
    for ch in invalid_chars:
        name = name.replace(ch, '_')
    if not name:
        name = "empty_trace"
    return name


def sanitize_dirname(name: str) -> str:
    """Convert a string to a valid directory name."""
    name = name.strip()
    invalid_chars = '/\\:*?"<>|\n\r\t| '
    for ch in invalid_chars:
        name = name.replace(ch, '_')
    while '__' in name:
        name = name.replace('__', '_')
    name = name.strip('_')
    if not name:
        name = "default"
    return name


def mode_output_dir(output_root: str, mode: str) -> str:
    """Return the dedicated output directory for one collection mode."""
    return os.path.join(output_root, mode)


def extract_trace_id(line: str) -> str:
    """Extract the trace ID from the structured access-log trace column."""
    parts = line.split('|')
    if len(parts) <= 5:
        return ""
    return parts[5].strip()


def run_grep_and_zgrep(trace: str, search_dirs: list, gz_files_by_dir: dict, output_path: str,
                       debug_search: bool = False) -> dict:
    """Search one trace across directories and write results to a file."""
    trace = trace.strip()
    if not trace:
        return {"trace": trace, "status": "skipped"}

    results = []

    for search_dir in search_dirs:
        dir_name = os.path.basename(search_dir)

        # grep -raFn: recursive, binary-safe fixed-string matching with line numbers.
        try:
            command = ["grep", "-raFn", trace, search_dir]
            if debug_search:
                print(f"  [DEBUG] exec: {' '.join(command)}", flush=True)
            result = subprocess.run(
                command,
                capture_output=True, text=True,
                encoding='utf-8', errors='replace',
                timeout=300
            )
            if result.stdout:
                results.append(f"=== grep -raFn '{trace}' {search_dir} ({dir_name}) ===\n")
                results.append(result.stdout)
            if result.stderr:
                results.append(f"\n[STDERR grep {dir_name}]\n{result.stderr}\n")
        except subprocess.TimeoutExpired:
            results.append(f"\n[TIMEOUT] grep timed out ({dir_name}): {trace}\n")
        except Exception as e:
            results.append(f"\n[ERROR grep {dir_name}] {e}\n")

        # zgrep -aFn: binary-safe fixed-string matching with line numbers.
        try:
            gz_files = gz_files_by_dir[search_dir]

            if gz_files:
                command = ["zgrep", "-aFn", trace] + gz_files
                if debug_search:
                    print(f"  [DEBUG] exec: {' '.join(command)}", flush=True)
                result = subprocess.run(
                    command,
                    capture_output=True, text=True,
                    encoding='utf-8', errors='replace',
                    timeout=300
                )
                if result.stdout:
                    results.append(f"\n=== zgrep -aFn '{trace}' {search_dir}/**/*.gz ({dir_name}) ===\n")
                    results.append(result.stdout)
                if result.stderr:
                    results.append(f"\n[STDERR zgrep {dir_name}]\n{result.stderr}\n")
            else:
                results.append(f"\n[INFO] No .gz files found in {dir_name}\n")
        except subprocess.TimeoutExpired:
            results.append(f"\n[TIMEOUT] zgrep timed out ({dir_name}): {trace}\n")
        except Exception as e:
            results.append(f"\n[ERROR zgrep {dir_name}] {e}\n")

    # Write merged results for this trace.
    try:
        with open(output_path, 'w', encoding='utf-8') as f:
            if results:
                f.write(''.join(results))
            else:
                f.write(f"[NO MATCH] No matches found for trace '{trace}'\n")
    except Exception as e:
        return {"trace": trace, "status": "error", "error": f"Failed to write file: {str(e)}"}

    match_count = sum(1 for r in results if r.startswith("==="))
    return {"trace": trace, "status": "success", "sections": match_count}


def search_traces(traces: list, search_dirs: list, output_dir: str, jobs: int,
                  debug_search: bool = False) -> None:
    """Search all traces in parallel and write results to the output directory."""
    total = len(traces)
    if total == 0:
        print("  No traces to search")
        return

    gz_files_by_dir = {}
    for d in search_dirs:
        gz_files = []
        for root, _, files in os.walk(d):
            gz_files.extend(
                os.path.join(root, filename)
                for filename in files
                if filename.endswith('.gz')
            )
        gz_files_by_dir[d] = gz_files
    total_gz = sum(len(gz_files) for gz_files in gz_files_by_dir.values())
    print(f"  Found {total_gz} .gz files")
    print(f"  Processing {total} traces in parallel...")

    print(f"  Using {jobs} threads")

    os.makedirs(output_dir, exist_ok=True)

    completed = 0
    success_count = 0

    with ThreadPoolExecutor(max_workers=jobs) as executor:
        future_to_trace = {}
        for trace in traces:
            filename = sanitize_filename(trace)
            output_path = os.path.join(output_dir, filename)

            counter = 1
            orig = output_path
            while os.path.exists(output_path):
                output_path = f"{orig}_{counter}"
                counter += 1

            future = executor.submit(
                run_grep_and_zgrep,
                trace,
                search_dirs,
                gz_files_by_dir,
                output_path,
                debug_search,
            )
            future_to_trace[future] = trace

        for future in as_completed(future_to_trace):
            result = future.result()
            completed += 1

            if result["status"] == "success":
                success_count += 1
                print(f"  [{completed}/{total}] OK {result['trace'][:50]}... -> {result['sections']} sections")
            elif result["status"] == "skipped":
                print(f"  [{completed}/{total}] SKIPPED empty trace")
            else:
                print(f"  [{completed}/{total}] FAILED {result['trace'][:50]}... -> {result['status']}")

    print(f"  Search complete: {success_count}/{total} succeeded")


def limit_traces(traces: list, max_traces: int) -> list:
    """Randomly retain at most max_traces unique traces."""
    total = len(traces)
    if total > max_traces:
        sampled = random.sample(traces, max_traces)
        print(f"  Trace limit: {total} > {max_traces}; sampled {max_traces}")
        return sorted(sampled)
    else:
        print(f"  Trace count {total} <= {max_traces}; retaining all")
        return sorted(traces)


def find_access_log_files(collected_dir: str) -> list:
    """Return current and rotated client access logs under collected_dir."""
    access_files = []
    roots = collected_dir if isinstance(collected_dir, (list, tuple, set)) else [collected_dir]
    for scan_root in roots:
        for root, _, files in os.walk(scan_root):
            for filename in files:
                if CLIENT_ACCESS_LOG_RE.fullmatch(filename):
                    access_files.append(os.path.join(root, filename))
    return sorted(access_files)


def open_access_log(filepath: str):
    """Open a plain or gzip-compressed access log as replacement-decoded text."""
    opener = gzip.open if filepath.endswith('.gz') else open
    return opener(filepath, 'rt', encoding='utf-8', errors='replace')


def grep_line_content(line: str) -> str:
    """Extract content from grep's file:line:content output without losing colons."""
    match = GREP_LINE_RE.match(line.rstrip('\n'))
    return match.group(3) if match else ''


def find_files(root_dir: str, predicate) -> list:
    """Return files below root_dir matching predicate, in stable order."""
    found = []
    roots = root_dir if isinstance(root_dir, (list, tuple, set)) else [root_dir]
    for scan_root in roots:
        for root, _, files in os.walk(scan_root):
            found.extend(os.path.join(root, name) for name in files if predicate(name))
    return sorted(found)


def discover_log_roots(root_dir: str, prefix: str = None) -> tuple:
    """Return client and worker log directories below one collection root."""
    base = os.path.abspath(os.path.expanduser(root_dir))
    if not os.path.isdir(base):
        raise ValueError(f"input directory does not exist: {base}")
    marker = os.path.join(base, '.cross_node_roots')
    if os.path.isfile(marker):
        clients, workers = [], []
        with open(marker, encoding='utf-8') as stream:
            roots = json.load(stream)
        for node_root in roots:
            node_clients, node_workers = discover_log_roots(node_root, prefix)
            clients.extend(node_clients)
            workers.extend(node_workers)
        return sorted(set(clients)), sorted(set(workers))
    if prefix is None:
        client_root = os.path.join(base, 'collected')
        worker_root = os.path.join(base, 'collected_worker_logs')
        if os.path.isdir(client_root) or os.path.isdir(worker_root):
            return ([client_root] if os.path.isdir(client_root) else [],
                    [worker_root] if os.path.isdir(worker_root) else [])
    scopes = sorted(os.path.join(base, name) for name in os.listdir(base)
                    if os.path.isdir(os.path.join(base, name)) and
                    (prefix is None or name.startswith(prefix)))
    if prefix is not None and not scopes:
        raise ValueError(f"no second-level directory under {base} starts with {prefix!r}")
    clients, workers = set(), set()
    for scope in scopes:
        for directory, _, filenames in os.walk(scope):
            names = [name.lower() for name in filenames]
            if any(name.startswith('ds_client_access') and name.endswith(('.log', '.log.gz')) for name in names):
                clients.add(directory)
            if any(name.startswith('access') and not name.startswith('ds_client_access') and
                   name.endswith(('.log', '.log.gz')) for name in names):
                workers.add(directory)
    return sorted(clients), sorted(workers)


def _scan_bucket_access_file(filepath: str, ops: list, ranges: list) -> tuple:
    bucket_traces = {(op, label): set() for op in ops for _, label, _ in ranges}
    trace_client_dirs = defaultdict(set)
    access_lines = defaultdict(list)
    try:
        with open_access_log(filepath) as stream:
            source_dir = os.path.dirname(filepath)
            for line_no, line in enumerate(stream, 1):
                op = next((candidate for candidate in ops if candidate in line), None)
                if op is None:
                    continue
                parts = line.split('|')
                if len(parts) < 10:
                    continue
                try:
                    value = float(parts[9].strip())
                except ValueError:
                    continue
                trace = extract_trace_id(line)
                if not trace:
                    continue
                for _, label, time_range in ranges:
                    if check_time_value(value, time_range):
                        bucket_traces[(op, label)].add(trace)
                        trace_client_dirs[trace].add(source_dir)
                        access_lines[trace].append(f"{filepath}:{line_no}:{line}")
    except Exception as exc:
        return filepath, None, None, None, str(exc)
    return filepath, bucket_traces, trace_client_dirs, access_lines, None


def _scan_status_access_file(filepath: str) -> tuple:
    traces_by_code = defaultdict(set)
    trace_client_dirs = defaultdict(set)
    access_lines = defaultdict(list)
    try:
        with open_access_log(filepath) as stream:
            for line_no, line in enumerate(stream, 1):
                match = ACCESS_STATUS_CODE_RE.search(line)
                if match is None:
                    continue
                code = match.group(1)
                if int(code) == 0:
                    continue
                trace = extract_trace_id(line)
                if trace:
                    traces_by_code[code].add(trace)
                    trace_client_dirs[trace].add(os.path.dirname(filepath))
                    access_lines[trace].append(f"{filepath}:{line_no}:{line}")
    except Exception as exc:
        return filepath, None, None, None, str(exc)
    return filepath, traces_by_code, trace_client_dirs, access_lines, None


def _merge_access_scan_results(results: list) -> tuple:
    first_success = next((result for result in results if result[4] is None), None)
    bucket_traces = {key: set() for key in first_success[1]} if first_success else {}
    trace_client_dirs = defaultdict(set)
    access_lines = defaultdict(list)
    for filepath, buckets, dirs, lines, error in results:
        if error:
            print(f"  [SKIPPED] {os.path.basename(filepath)}: {error}", flush=True)
            continue
        for key, traces in buckets.items():
            bucket_traces.setdefault(key, set()).update(traces)
        for trace, values in dirs.items():
            trace_client_dirs[trace].update(values)
        for trace, values in lines.items():
            access_lines[trace].extend(values)
    return bucket_traces, trace_client_dirs, access_lines


def _collect_access_files(access_files: list, scan_function, scan_args: tuple, jobs: int) -> list:
    if not access_files:
        return []
    workers = min(jobs, len(access_files))
    print(f"  Scanning {len(access_files)} access logs with up to {workers} processes", flush=True)
    results = []
    started = time.monotonic()
    with ProcessPoolExecutor(max_workers=workers, mp_context=multiprocessing.get_context('fork')) as executor:
        futures = [executor.submit(scan_function, filepath, *scan_args) for filepath in access_files]
        for index, future in enumerate(as_completed(futures), 1):
            results.append(future.result())
            if index == 1 or index % 10 == 0 or index == len(access_files):
                print(f"  [{time.strftime('%H:%M:%S')}] access logs: {index}/{len(access_files)} files, "
                      f"{time.monotonic() - started:.1f}s", flush=True)
    return results


def collect_bucket_traces(ops: list, ranges: list, collected_dir: str, jobs: int = 1) -> dict:
    """Read client access logs in parallel by file and merge their results."""
    access_files = find_access_log_files(collected_dir)
    if not access_files:
        return {(op, label): set() for op in ops for _, label, _ in ranges}, defaultdict(set), defaultdict(list)
    results = _collect_access_files(access_files, _scan_bucket_access_file, (ops, ranges), jobs)
    bucket_traces, trace_client_dirs, access_lines = _merge_access_scan_results(results)
    return bucket_traces, trace_client_dirs, access_lines


def locate_worker_dirs(traces: set, logs_dir: str, jobs: int, debug_search: bool = False) -> tuple:
    """Scan worker access logs once and return matched directories and original lines."""
    if not traces:
        return set(), defaultdict(list)
    worker_access = find_files(logs_dir, lambda n: ('access' in n.lower()) and n.endswith(('.log', '.log.gz')))
    worker_dirs = set()
    access_lines = defaultdict(list)
    patterns = ''.join(trace + '\n' for trace in traces)
    started = time.monotonic()
    def scan(path):
        try:
            # -Hn makes the persisted evidence consistently file:line:content,
            # including when grep receives exactly one input file.
            command = ['zgrep' if path.endswith('.gz') else 'grep', '-aFHn', '-f', '-', path]
            if debug_search:
                print(f"  [{time.strftime('%H:%M:%S')}] [DEBUG] exec: {' '.join(shlex.quote(x) for x in command)}", flush=True)
            result = subprocess.run(command, input=patterns, capture_output=True, text=True,
                                    encoding='utf-8', errors='replace', timeout=300)
            return result.returncode, result.stdout, result.stderr
        except Exception as exc:
            return None, '', str(exc)
    with ThreadPoolExecutor(max_workers=min(jobs, len(worker_access))) as executor:
        futures = {executor.submit(scan, path): path for path in worker_access}
        for index, future in enumerate(as_completed(futures), 1):
            path = futures[future]
            result = future.result()
            returncode, stdout, stderr = result
            if returncode == 0:
                worker_dirs.add(os.path.dirname(path))
                for line in stdout.splitlines(keepends=True):
                    content = grep_line_content(line)
                    for trace in traces:
                        if trace in content:
                            # Keep grep's file:line:content evidence verbatim.  Splitting
                            # on ':' corrupts ISO-8601 timestamps in the content.
                            access_lines[trace].append(line)
            elif returncode not in (1,):
                print(f"  [SKIPPED] {os.path.basename(path)}: {stderr}")
            if index == 1 or index % 50 == 0 or index == len(worker_access):
                print(f"  [{time.strftime('%H:%M:%S')}] worker access: {index}/{len(worker_access)} files, {len(worker_dirs)} matched dirs, {time.monotonic()-started:.1f}s")
    return worker_dirs, access_lines


def _scan_log_file(filepath: str, patterns: str, debug_search: bool = False) -> list:
    matches = []
    try:
        # -Hn provides a stable file:line:content prefix for both plain and gzip logs.
        command = ['zgrep' if filepath.endswith('.gz') else 'grep', '-aFHn', '-f', '-', filepath]
        if debug_search:
            print(f"  [{time.strftime('%H:%M:%S')}] [DEBUG] exec: {' '.join(shlex.quote(x) for x in command)}", flush=True)
        result = subprocess.run(command, input=patterns, capture_output=True, text=True,
                                encoding='utf-8', errors='replace', timeout=300)
        if result.returncode not in (0, 1):
            return [(None, f"[ERROR] {' '.join(command)}: {result.stderr}\n")]
        for line in result.stdout.splitlines(keepends=True):
            # Do not parse the prefix by ':': paths and ISO-8601 timestamps both
            # legitimately contain colons.  The trace is unique enough to associate
            # this complete source line with its output.
            content = grep_line_content(line)
            for trace in patterns.splitlines():
                if trace in content:
                    matches.append((trace, line))
    except Exception as exc:
        return [(None, f"[SKIPPED] {os.path.basename(filepath)}: {exc}\n")]
    return matches


def search_traces_once(traces: set, search_dirs: set, output_by_trace: dict, jobs: int,
                       debug_search: bool = False) -> None:
    """Scan each relevant log once and append matching lines to trace outputs."""
    if not traces or not search_dirs:
        return
    patterns = ''.join(trace + '\n' for trace in traces)
    files = []
    for directory in sorted(search_dirs):
        files.extend(find_files(directory, lambda n: n.endswith(('.log', '.log.gz')) and 'access' not in n.lower()))
    if not files:
        return
    if debug_search:
        print(f"  [DEBUG] targeted search dirs ({len(search_dirs)}):", flush=True)
        for directory in sorted(search_dirs):
            print(f"    {directory}", flush=True)
        print(f"  [DEBUG] targeted detail files ({len(files)}):", flush=True)
        for filepath in files:
            print(f"    {filepath}", flush=True)
        print(f"  [DEBUG] multi-pattern search: {'grep/zgrep -aFn -f - <targeted-file>'} ({len(traces)} patterns)", flush=True)
    started = time.monotonic()
    with ThreadPoolExecutor(max_workers=min(jobs, len(files))) as executor:
        futures = {executor.submit(_scan_log_file, path, patterns, debug_search): path for path in files}
        for index, future in enumerate(as_completed(futures), 1):
            matches = future.result()
            for trace, line in matches:
                if trace is None:
                    print(line, end='')
                else:
                    output_by_trace[trace].append(line)
            if index == 1 or index % 10 == 0 or index == len(files):
                print(f"  [{time.strftime('%H:%M:%S')}] detail logs: {index}/{len(files)} files, {len(output_by_trace)} traces matched, {time.monotonic()-started:.1f}s", flush=True)


def parse_bucket_specs(range_values: list) -> list:
    """Parse ranges and retain a stable, human-readable bucket label."""
    parsed = []
    for value in range_values:
        parsed.append((value, value, parse_time_range(value)))
    return parsed


def process_time_buckets(ops: list, range_values: list, collected_dir: str, logs_dir: str,
                         base_output_dir: str, trace_file_base: str, max_traces: int, jobs: int,
                         debug_search: bool = False) -> None:
    """Collect multiple latency buckets with one access and one targeted log scan."""
    ranges = parse_bucket_specs(range_values)
    bucket_traces, trace_client_dirs, access_lines = collect_bucket_traces(ops, ranges, collected_dir, jobs)
    selected = {}
    for key, traces in bucket_traces.items():
        selected[key] = limit_traces(sorted(traces), max_traces)
    all_traces = set(trace for traces in selected.values() for trace in traces)
    worker_dirs, worker_access_lines = locate_worker_dirs(all_traces, logs_dir, jobs, debug_search)
    if debug_search:
        print(f"  [DEBUG] selected traces: {len(all_traces)}; worker dirs: {len(worker_dirs)}", flush=True)
    search_dirs = set(trace_dir for trace in all_traces for trace_dir in trace_client_dirs[trace]) | worker_dirs
    output_by_trace = defaultdict(list)
    search_traces_once(all_traces, search_dirs, output_by_trace, jobs, debug_search)
    for (op, label), traces in selected.items():
        bucket_dir = os.path.join(base_output_dir, sanitize_dirname(f"{op}_{label}"))
        os.makedirs(bucket_dir, exist_ok=True)
        trace_file = os.path.join(base_output_dir, f"{trace_file_base}_{sanitize_dirname(f'{op}_{label}')}.txt")
        with open(trace_file, 'w', encoding='utf-8') as stream:
            stream.write('\n'.join(traces) + ('\n' if traces else ''))
        for trace in traces:
            output_path = os.path.join(bucket_dir, sanitize_filename(trace))
            with open(output_path, 'w', encoding='utf-8') as stream:
                stream.write("=== client access ===\n")
                stream.write(''.join(access_lines[trace]))
                stream.write("\n=== worker access ===\n")
                stream.write(''.join(worker_access_lines[trace]))
                stream.write("\n=== client and worker detailed logs ===\n")
                stream.write(''.join(output_by_trace[trace]) or f"[NO MATCH] No detailed-log matches found for trace '{trace}'\n")
    print(f"  Targeted scan complete: {len(all_traces)} traces, {len(search_dirs)} directories")


# ==================== Core mode ====================

def extract_traces_core(search_core: str, collected_dir: str) -> list:
    """
    Extract trace UUIDs from collected/ ds_client_access_*.log files.
    Workers without access logs are skipped.
    """
    print(f"  Search string: '{search_core}'")

    access_files = find_access_log_files(collected_dir)

    if not access_files:
        print("  [WARNING] No access logs found")
        return []

    print(f"  Found {len(access_files)} access logs")

    traces = set()

    for filepath in access_files:
        try:
            with open_access_log(filepath) as f:
                for line_no, line in enumerate(f, 1):
                    if search_core in line:
                        trace = extract_trace_id(line)
                        if trace:
                            traces.add(trace)
        except Exception as e:
            print(f"  [SKIPPED] {os.path.basename(filepath)}: {e}")

    all_traces = sorted(traces)
    print(f"  Extracted {len(all_traces)} unique traces")
    return all_traces


def process_core(search_core: str, collected_dir: str, logs_dir: str, base_output_dir: str,
                 trace_file_base: str, max_traces: int, jobs: int, debug_search: bool = False) -> None:
    """Process one search string in core mode."""
    core_dirname = sanitize_dirname(search_core)
    output_dir = os.path.join(base_output_dir, core_dirname)
    trace_file = os.path.join(base_output_dir, f"{trace_file_base}_{core_dirname}.txt")

    os.makedirs(base_output_dir, exist_ok=True)

    print(f"\n{'='*60}")
    print(f"[core mode] Processing search string: '{search_core}'")
    print(f"  Output subdirectory: {core_dirname}")
    print(f"{'='*60}")

    # Step 1: Extract traces.
    all_traces = extract_traces_core(search_core, collected_dir)

    # Step 2: Limit traces.
    traces = limit_traces(all_traces, max_traces)

    # Write trace file.
    with open(trace_file, 'w', encoding='utf-8') as f:
        for t in traces:
            f.write(t + '\n')
    print(f"  Wrote trace file: {trace_file}")

    # Step 3: Search in parallel.
    search_dirs = list(collected_dir) + list(logs_dir)
    search_traces(traces, search_dirs, output_dir, jobs, debug_search)

    print(f"  Search string '{search_core}' complete, results: {output_dir}/")


def extract_traces_by_status_code(collected_dir: str, jobs: int = 1) -> dict:
    """Extract status-code traces, client directories, and original access lines in one pass."""
    access_files = find_access_log_files(collected_dir)
    if not access_files:
        print("  [WARNING] No access logs found")
        return {}, defaultdict(set), defaultdict(list)

    print(f"  Found {len(access_files)} access logs")
    results = _collect_access_files(access_files, _scan_status_access_file, (), jobs)
    traces_by_code, trace_client_dirs, access_lines = _merge_access_scan_results(results)

    print(f"  Extracted {len(traces_by_code)} non-zero status codes")
    return traces_by_code, trace_client_dirs, access_lines


def process_all_core(collected_dir: str, logs_dir: str, base_output_dir: str, trace_file_base: str,
                     max_traces: int, jobs: int, debug_search: bool = False) -> None:
    """Collect traces for every access-log error code with an independent trace limit."""
    print(f"\n{'='*60}")
    print("[all-core mode] Extracting all non-zero access-log status codes")
    print(f"{'='*60}")
    os.makedirs(base_output_dir, exist_ok=True)
    traces_by_code, trace_client_dirs, access_lines = extract_traces_by_status_code(collected_dir, jobs)
    if not traces_by_code:
        return

    selected_by_code = {}
    for code in sorted(traces_by_code, key=int):
        all_traces = sorted(traces_by_code[code])
        code_dir = os.path.join(base_output_dir, code)
        trace_file = os.path.join(base_output_dir, f"{trace_file_base}_{code}.txt")
        print(f"\n{'='*60}")
        print(f"[all-core mode] Processing status code: {code}")
        print(f"  Extracted {len(all_traces)} unique traces")
        print(f"  Output subdirectory: {code}")
        print(f"{'='*60}")

        traces = limit_traces(all_traces, max_traces)
        with open(trace_file, 'w', encoding='utf-8') as f:
            f.write(f"# Status code: {code}\n")
            f.write(f"# Total traces: {len(all_traces)}\n")
            f.write(f"# Final traces: {len(traces)}\n")
            for trace in traces:
                f.write(trace + '\n')
        print(f"  Wrote trace file: {trace_file}")
        selected_by_code[code] = (traces, code_dir)

    all_traces = set(trace for traces, _ in selected_by_code.values() for trace in traces)
    worker_dirs, worker_access_lines = locate_worker_dirs(all_traces, logs_dir, jobs, debug_search)
    search_dirs = set(directory for trace in all_traces for directory in trace_client_dirs[trace]) | worker_dirs
    output_by_trace = defaultdict(list)
    search_traces_once(all_traces, search_dirs, output_by_trace, jobs, debug_search)
    for code, (traces, code_dir) in selected_by_code.items():
        os.makedirs(code_dir, exist_ok=True)
        for trace in traces:
            with open(os.path.join(code_dir, sanitize_filename(trace)), 'w', encoding='utf-8') as f:
                f.write("=== client access ===\n")
                f.write(''.join(access_lines[trace]))
                f.write("\n=== worker access ===\n")
                f.write(''.join(worker_access_lines[trace]))
                f.write("\n=== client and worker detailed logs ===\n")
                f.write(''.join(output_by_trace[trace]) or f"[NO MATCH] No detailed-log matches found for trace '{trace}'\n")
        print(f"  Status code {code} complete: {len(traces)} traces")


# ==================== Time mode ====================

def parse_time_range(time_str: str) -> tuple:
    """
    Parse a latency range string.
    "1000,2000" -> (1000, 2000), greater than 1000 and less than 2000.
    "1000"      -> (1000, None), greater than 1000.
    ",2000"     -> (None, 2000), less than 2000.
    ""          -> (None, None), no limit.
    """
    if not time_str or time_str.strip() == '':
        return (None, None)

    parts = time_str.strip().split(',')
    if len(parts) == 1:
        return (float(parts[0].strip()), None)
    elif len(parts) == 2:
        lower = parts[0].strip()
        upper = parts[1].strip()
        return (
            float(lower) if lower else None,
            float(upper) if upper else None
        )
    else:
        raise ValueError(f"Invalid latency range format: {time_str}")


def check_time_value(time_val: float, time_range: tuple) -> bool:
    """Return whether a latency value is within the requested range."""
    lower, upper = time_range
    if lower is not None and time_val <= lower:
        return False
    if upper is not None and time_val >= upper:
        return False
    return True


def extract_traces_time(op_type: str, time_range: tuple, collected_dir: str) -> list:
    """
    Extract traces from collected/ access logs by operation type and latency range.
    """
    print(f"  Operation type: {op_type}")
    print("  Latency range: ", end="")
    lower, upper = time_range
    if lower is not None and upper is not None:
        print(f"{lower} < time < {upper}")
    elif lower is not None:
        print(f"time > {lower}")
    elif upper is not None:
        print(f"time < {upper}")
    else:
        print("unlimited")

    access_files = find_access_log_files(collected_dir)

    if not access_files:
        print("  [WARNING] No access logs found")
        return []

    print(f"  Found {len(access_files)} access logs")

    traces = set()

    for filepath in access_files:
        try:
            with open_access_log(filepath) as f:
                match_count = 0
                for line in f:
                    if op_type not in line:
                        continue

                    parts = line.split('|')
                    if len(parts) < 10:
                        continue

                    time_str = parts[9].strip()
                    try:
                        time_val = float(time_str)
                    except ValueError:
                        continue

                    if not check_time_value(time_val, time_range):
                        continue

                    trace = extract_trace_id(line)
                    if trace:
                        traces.add(trace)
                        match_count += 1

                if match_count > 0:
                    print(f"  OK {os.path.basename(filepath)}: {match_count} traces")

        except Exception as e:
            print(f"  [SKIPPED] {os.path.basename(filepath)}: {e}")

    all_traces = sorted(traces)
    print(f"  Extracted {len(all_traces)} unique traces")
    return all_traces


def process_time(op_type: str, time_range: tuple, collected_dir: str, logs_dir: str,
                 base_output_dir: str, trace_file_base: str, max_traces: int, jobs: int) -> None:
    """Process one operation type in time mode."""
    lower_bound = int(time_range[0]) if time_range[0] else ''
    upper_bound = int(time_range[1]) if time_range[1] else ''
    core_dirname = sanitize_dirname(f"{op_type}_{lower_bound}_{upper_bound}")
    output_dir = os.path.join(base_output_dir, core_dirname)
    trace_file = os.path.join(base_output_dir, f"{trace_file_base}_{core_dirname}.txt")

    os.makedirs(base_output_dir, exist_ok=True)

    print(f"\n{'='*60}")
    print(f"[time mode] Processing operation type: {op_type}")
    print(f"  Output subdirectory: {core_dirname}")
    print(f"{'='*60}")

    # Step 1: Extract traces.
    all_traces = extract_traces_time(op_type, time_range, collected_dir)

    # Step 2: Limit traces.
    traces = limit_traces(all_traces, max_traces)

    # Write trace file.
    with open(trace_file, 'w', encoding='utf-8') as f:
        for t in traces:
            f.write(t + '\n')
    print(f"  Wrote trace file: {trace_file}")

    # Step 3: Search in parallel.
    search_dirs = list(collected_dir) + list(logs_dir)
    search_traces(traces, search_dirs, output_dir, jobs)

    print(f"  Operation '{op_type}' complete, results: {output_dir}/")


# ==================== Percentile mode ====================

def parse_percentile(p_str: str) -> float:
    """
    Parse a percentile string.
    P99 -> 0.99, P99.9 -> 0.999, P99.99 -> 0.9999
    """
    p_str = p_str.strip().upper()
    if not p_str.startswith('P'):
        raise ValueError(f"Invalid percentile format: {p_str}; expected P99/P99.9/P99.99")

    num_str = p_str[1:]
    try:
        num = float(num_str)
    except ValueError:
        raise ValueError(f"Invalid percentile format: {p_str}")

    if not (0 < num < 100):
        raise ValueError(f"Percentile must be between 0 and 100: {num}")

    return num / 100.0


def calculate_percentile(values: list, percentile: float) -> float:
    """Calculate a percentile using linear interpolation."""
    if not values:
        return 0.0

    sorted_values = sorted(values)
    n = len(sorted_values)

    index = (n - 1) * percentile
    lower_idx = int(math.floor(index))
    upper_idx = int(math.ceil(index))

    if lower_idx == upper_idx:
        return sorted_values[lower_idx]

    weight = index - lower_idx
    return sorted_values[lower_idx] * (1 - weight) + sorted_values[upper_idx] * weight


def extract_times_and_traces(op_type: str, collected_dir: str) -> tuple:
    """
    Extract all latency values and associated traces from access logs.
    Returns: (times_list, time_trace_pairs).
    """
    print(f"  Operation type: {op_type}")

    access_files = find_access_log_files(collected_dir)

    if not access_files:
        print("  [WARNING] No access logs found")
        return [], []

    print(f"  Found {len(access_files)} access logs")

    times = []
    time_trace_pairs = []

    for filepath in access_files:
        try:
            with open_access_log(filepath) as f:
                file_times = 0
                for line in f:
                    if op_type not in line:
                        continue

                    parts = line.split('|')
                    if len(parts) < 10:
                        continue

                    time_str = parts[9].strip()
                    try:
                        time_val = float(time_str)
                    except ValueError:
                        continue

                    trace = extract_trace_id(line)
                    if trace:
                        times.append(time_val)
                        time_trace_pairs.append((time_val, trace))
                        file_times += 1

                if file_times > 0:
                    print(f"  OK {os.path.basename(filepath)}: {file_times} records")

        except Exception as e:
            print(f"  [SKIPPED] {os.path.basename(filepath)}: {e}")

    print(f"  Extracted {len(times)} records")
    return times, time_trace_pairs


def filter_traces_by_percentile(times: list, time_trace_pairs: list, percentile: float) -> tuple:
    """
    Filter traces by percentile.
    Returns: (threshold, traces).
    """
    if not times:
        return 0.0, []

    threshold = calculate_percentile(times, percentile)
    p_str = f"P{percentile * 100}".replace('.0', '')

    print("\n  Latency statistics:")
    print(f"    Min: {min(times):.2f}")
    print(f"    Max: {max(times):.2f}")
    print(f"    Average: {sum(times)/len(times):.2f}")
    print(f"    Median: {calculate_percentile(times, 0.5):.2f}")
    print(f"    {p_str}: {threshold:.2f}")

    seen = set()
    filtered_traces = []
    for time_val, trace in time_trace_pairs:
        if time_val >= threshold and trace not in seen:
            seen.add(trace)
            filtered_traces.append(trace)

    print(f"  Traces >= {p_str} ({threshold:.2f}): {len(filtered_traces)}")

    return threshold, filtered_traces


def process_percentile(op_type: str, percentile: float, collected_dir: str, logs_dir: str,
                       base_output_dir: str, trace_file_base: str, max_traces: int, jobs: int) -> None:
    """Process one operation type in percentile mode."""
    p_str = f"P{percentile * 100}".replace('.0', '')
    core_dirname = sanitize_dirname(f"{op_type}_{p_str}")
    output_dir = os.path.join(base_output_dir, core_dirname)
    trace_file = os.path.join(base_output_dir, f"{trace_file_base}_{core_dirname}.txt")

    os.makedirs(base_output_dir, exist_ok=True)

    print(f"\n{'='*60}")
    print(f"[percentile mode] Processing operation type: {op_type}")
    print(f"  Percentile: {p_str}")
    print(f"  Output subdirectory: {core_dirname}")
    print(f"{'='*60}")

    # Step 1: Extract all latency values and traces.
    times, time_trace_pairs = extract_times_and_traces(op_type, collected_dir)

    if not times:
        print("  [WARNING] No data extracted; skipping")
        return

    # Step 2: Filter by percentile.
    threshold, filtered_traces = filter_traces_by_percentile(times, time_trace_pairs, percentile)

    # Step 3: Limit traces.
    traces = limit_traces(filtered_traces, max_traces)

    # Write trace file.
    with open(trace_file, 'w', encoding='utf-8') as f:
        f.write(f"# Operation type: {op_type}\n")
        f.write(f"# Percentile: {p_str}\n")
        f.write(f"# Threshold: {threshold:.2f}\n")
        f.write(f"# Total records: {len(times)}\n")
        f.write(f"# Filtered traces: {len(filtered_traces)}\n")
        f.write(f"# Final traces: {len(traces)}\n")
        f.write("#" + "=" * 50 + "\n")
        for t in traces:
            f.write(t + '\n')
    print(f"  Wrote trace file: {trace_file}")

    # Step 4: Search in parallel.
    search_dirs = list(collected_dir) + list(logs_dir)
    search_traces(traces, search_dirs, output_dir, jobs)

    print(f"  Operation '{op_type}' complete, results: {output_dir}/")


# ==================== Main entry point ====================

def main():
    # Append the directory layout and mode details to the standard help text.
    class CustomHelpFormatter(argparse.RawDescriptionHelpFormatter):
        def format_help(self):
            help_text = super().format_help()
            dir_structure = """

Directory layout:
  Pass the collection root with -f. Standard collected/ and
  collected_worker_logs/ layouts are discovered automatically. For mixed
  second-level collections, use -p to select directories by prefix.

  /parent/                         # -f root directory
  |-- unique_traces_*.txt           # Generated trace files
  |-- collected/                    # Client logs used to extract traces
  |   |-- client-worker/
  |   |   |-- ds_client_access.log  # Trace source
  |   |   |-- ds_client.INFO.log    # Detailed log search source
  |   |   `-- ds_client.INFO.log.gz # Compressed detailed log search source
  |   `-- ...
  |-- collected_worker_logs/        # Worker logs used only for searching
  |   |-- worker/
  |   |   `-- kvcache.INFO.log
  |   `-- ...
  `-- [output_dir]/                 # Output subdirectories created by mode
      |-- core_xxx/                 # Core mode output
      |-- all-core/                 # All error-code output
      |   |-- 1001/                 # One status code per subdirectory
      |-- time_xxx/                 # Time mode output
      `-- percentile_xxx/           # Percentile mode output

Modes:

  [core mode]
    Extract traces from access logs by search string, then search all logs in parallel.
    Use when a known log marker, such as "| 1001 | DS", identifies the target traces.

    Examples:
      python3 trace_collector.py -f /path/to/rootDir -p jingpai --type all-core
      python3 trace_collector.py --type core "| 1001 | DS"
      python3 trace_collector.py --type core "| 1001 | DS:| 1002 | DS"

  [all-core mode]
    Extract every non-zero status code from client access logs. Each status code gets an
    independent --max-traces limit and a separate output subdirectory. No positional value is used.

    Example:
      python3 trace_collector.py --type all-core

  [time-buckets mode]
    Extract multiple operation/latency buckets in one client access-log pass. The collector then
    scans worker access logs once to identify relevant workers and scans only the matching client
    and worker detailed-log directories once. Use this mode for predefined report buckets.

    Example:
      python3 trace_collector.py --type time-buckets \\
          --ops DS_KV_CLIENT_GET,DS_KV_CLIENT_SET \\
          --ranges 5000,7000 7000,10000 10000,20000 20000

  [percentile mode]
    Calculate P99/P99.9/P99.99 by operation type, extract traces at or above the threshold, then search in parallel.
    Use when locating tail-latency traces for an operation.

    Examples:
      python3 trace_collector.py --type percentile DS_KV_CLIENT_GET P99
      python3 trace_collector.py --type percentile DS_KV_CLIENT_GET P99.9
      python3 trace_collector.py --type percentile "GET:PUT" P99.99

Common options:
  -f, --dir         Collection root; standard or mixed layouts are discovered.
  -p, --prefix      Optional second-level mixed-collection directory prefix.
  --output-dir      Output root directory (default depends on mode)
  --trace-file      Trace file prefix (default: unique_traces)
"""
            return help_text + dir_structure

    parser = argparse.ArgumentParser(
        description="Unified trace collection tool supporting core, all-core, time-buckets, and percentile modes",
        formatter_class=CustomHelpFormatter,
        epilog="""
Examples:
  # mixed second-level collections selected by prefix
  python3 trace_collector.py -f /path/to/rootDir -p jingpai --type all-core

  # core mode
  python3 trace_collector.py --type core "| 1001 | DS"

  # all-core mode
  python3 trace_collector.py --type all-core

  # time-buckets mode: GET and SET, four latency buckets, one command
  python3 trace_collector.py --type time-buckets \\
      --ops DS_KV_CLIENT_GET,DS_KV_CLIENT_SET \\
      --ranges 5000,7000 7000,10000 10000,20000 20000

  # percentile mode
  python3 trace_collector.py --type percentile DS_KV_CLIENT_GET P99.9
        """
    )

    parser.add_argument(
        "--type",
        required=True,
        choices=["core", "all-core", "all_core", "time-buckets", "percentile"],
        help=("Mode: core (search string), all-core (all non-zero status codes), "
              "time-buckets (latency ranges), or percentile (latency percentile)")
    )
    parser.add_argument(
        "values",
        nargs='*',
        help="Positional arguments: core search strings or percentile operation and percentile"
    )
    parser.add_argument('-f', '--dir',
                        help='root directory containing standard or mixed collections (default: script directory)')
    parser.add_argument('-p', '--prefix',
                        help='only scan second-level mixed collection directories with this prefix')
    parser.add_argument('--cross-node', action='store_true',
                        help='fetch the root from Kubernetes nodes before local processing')
    parser.add_argument(
        "--output-dir",
        default="trace_collect",
        help="Output root directory; results are placed in a mode-specific subdirectory (default: trace_collect)"
    )
    parser.add_argument(
        "--trace-file",
        default="unique_traces",
        help="Trace file prefix (default: unique_traces)"
    )
    parser.add_argument(
        "--max-traces",
        type=int,
        default=DEFAULT_MAX_TRACES,
        help=f"Randomly retain at most this many traces per filter (default: {DEFAULT_MAX_TRACES})"
    )
    parser.add_argument(
        "--jobs",
        type=int,
        default=DEFAULT_JOBS,
        help=f"Parallel trace-search workers (default: {DEFAULT_JOBS})"
    )
    parser.add_argument(
        "--ops", default="",
        help="Comma-separated operation types for time-buckets mode"
    )
    parser.add_argument(
        "--ranges", nargs="*", default=[],
        help="Latency ranges for time-buckets mode, e.g. 5000,7000 7000,10000 20000"
    )
    parser.add_argument(
        "--debug-search", action="store_true",
        help="Print grep/zgrep commands and targeted search files for debugging"
    )

    args = parser.parse_args()
    if args.type == "all_core":
        args.type = "all-core"

    if args.max_traces < 1:
        parser.error("--max-traces must be at least 1")
    if args.jobs < 1:
        parser.error("--jobs must be at least 1")

    exec_dir = os.path.dirname(os.path.abspath(__file__)) or os.getcwd()
    input_root = args.dir or exec_dir
    if args.cross_node:
        try:
            prefilter = None
            if args.type == 'core':
                prefilter = [value.strip() for value in ':'.join(args.values).split(':') if value.strip()]
            elif args.type == 'percentile' and args.values:
                prefilter = [value.strip() for value in args.values[0].split(':') if value.strip()]
            elif args.type == 'time-buckets':
                prefilter = [value for value in args.ops.split(',') if value]
            elif args.type == 'all-core':
                prefilter = ['| DS', '|DS']
            input_root = stage_cross_node(input_root, args.jobs, args.prefix, prefilter)
        except Exception as exc:
            parser.error(f'cross-node staging failed: {exc}')
    try:
        collected_dirs, worker_dirs = discover_log_roots(input_root, args.prefix)
    except ValueError as exc:
        parser.error(str(exc))
    if not collected_dirs:
        parser.error('no client log directories were discovered')
    if not worker_dirs:
        parser.error('no worker log directories were discovered')
    output_root = os.path.join(os.path.abspath(input_root), args.output_dir)
    output_dir = mode_output_dir(output_root, args.type)

    print(f"\n{'#'*60}")
    print(f"Mode: {args.type}")
    print(f"Script directory: {exec_dir}")
    print(f"Client log directories: {len(collected_dirs)}")
    print(f"Worker log directories: {len(worker_dirs)}")
    print(f"Output directory: {output_dir}")
    print(f"Trace limit per filter: {args.max_traces}")
    print(f"Parallel workers: {args.jobs}")
    print(f"{'#'*60}")

    # Dispatch processing by mode.
    if args.type == "core":
        # Core mode accepts one or more ':'-separated search strings.
        search_cores = [c.strip() for c in ':'.join(args.values).split(':') if c.strip()]
        if not search_cores:
            print("ERROR: core mode requires at least one search string")
            sys.exit(1)

        print(f"\nSearch strings: {len(search_cores)}")
        for i, c in enumerate(search_cores, 1):
            print(f"  {i}. '{c}'")

        for core in search_cores:
            process_core(core, collected_dirs, worker_dirs, output_dir, args.trace_file, args.max_traces, args.jobs, args.debug_search)

    elif args.type == "all-core":
        if args.values:
            parser.error("all-core mode does not accept positional values")
        process_all_core(collected_dirs, worker_dirs, output_dir, args.trace_file, args.max_traces, args.jobs, args.debug_search)

    elif args.type == "time-buckets":
        if args.values:
            parser.error("time-buckets mode uses --ops and --ranges, not positional values")
        ops = [op.strip() for op in args.ops.split(',') if op.strip()]
        ranges = args.ranges
        if not ops or not ranges:
            parser.error("time batch mode requires --ops and at least one --ranges value")
        try:
            for value in ranges:
                parse_time_range(value)
        except ValueError as exc:
            parser.error(str(exc))
        process_time_buckets(ops, ranges, collected_dirs, worker_dirs, output_dir,
                             args.trace_file, args.max_traces, args.jobs, args.debug_search)

    elif args.type == "percentile":
        # Percentile mode requires an operation type and a percentile.
        if len(args.values) < 2:
            print("ERROR: percentile mode requires an operation type and percentile")
            print("  Example: python3 trace_collector.py --type percentile DS_KV_CLIENT_GET P99.9")
            sys.exit(1)

        percentile_str = args.values[-1]
        op_types_str = ' '.join(args.values[:-1])

        try:
            percentile = parse_percentile(percentile_str)
        except ValueError as e:
            print(f"ERROR: {e}")
            sys.exit(1)

        op_types = [op.strip() for op in op_types_str.split(':') if op.strip()]
        if not op_types:
            print("ERROR: provide at least one operation type")
            sys.exit(1)

        print(f"\nOperation types: {len(op_types)}")
        for i, op in enumerate(op_types, 1):
            print(f"  {i}. '{op}'")
        print(f"Percentile: {percentile_str} ({percentile})")

        for op_type in op_types:
            process_percentile(op_type, percentile, collected_dirs, worker_dirs, output_dir,
                               args.trace_file, args.max_traces, args.jobs)

    print(f"\n{'#'*60}")
    print("All processing complete")
    print(f"Output root directory: {os.path.abspath(output_dir)}/")
    print(f"{'#'*60}")


if __name__ == "__main__":
    main()
