#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
KVCache Trace Log Latency Analysis Tool - High Performance Version

Optimized for processing tens of millions of log records:
- Multi-process parallel log parsing
- TraceId-based disk sharding (constant memory footprint)
- Zero-regex parsing using str.split()
- Streaming output, no in-memory result accumulation
- Two-phase architecture: Map (parse+shard) -> Reduce (merge+output)

Expected collected-log layout (directories may have extra nesting):
  <task>/collected/clients/.../ds_client_access*.log[.gz]
  <task>/collected/clients/.../ds_client*INFO*.log[.gz]
  <task>/collected_worker_logs/workers/.../access*.log[.gz]
  <task>/collected_worker_logs/workers/.../kvcache*INFO*.log[.gz]

Use this tool for offline GET/SET latency diagnosis in meta-owner,
same-node, and local-cache deployments.  Client and worker evidence is
correlated by the trace-id field: any non-empty value except '-', 'null',
or 'None' is accepted; it is not restricted to UUIDs.

Scenario and field semantics:
  auto recognizes meta-owner from Query metadata/CreateMeta evidence and
  local-cache from local-cache-hit evidence; otherwise it reports same-node.
  Absence of a metadata or URMA stage means it was not observed, not zero.
  End-to-end latency comes from client ds_client_access records in microseconds.
  URMA transfer is the maximum URMA_ELAPSED_TOTAL total cost per trace.
  RPC non-server time is max(0, e2e_us - server_exec_us); RPC server time is
  server_exec_us.  Stage distributions are independent diagnostic components:
  retries, overlaps, missing logs, and uninstrumented work make them non-additive.

Performance, outputs, and failure interpretation:
  Input logs stream through a multi-process disk-sharded map/reduce flow.
  setandget collects/correlates once, then produces GET and SET reports without
  traversing the logs again.  Temporary JSONL/.info shards are process-local and
  removed on success and failure.  -qps false avoids 10ms QPS aggregation and its
  HTML payload, reducing runtime memory and browser render cost for long runs.
  A report with zero requests usually means ds_client_access records were absent;
  a missing worker stage means no correlated worker evidence was found.

Arguments not shown in the examples:
  --scenario records auto/meta-owner/same-node/local-cache as an operator hint.
  --shard-bits selects 2^N temporary trace shards (default 8).
  --top limits slow traces in each report (default 200).
  --window-ms selects trend aggregation width (default 1000ms).
  --from-time and --to-time restrict timestamps; --latency-threshold filters
  requests by latency; --error-only retains only errors; --sleep-threshold sets
  the .info sleep threshold in microseconds (default 250).

Input constraints:
  -f names exactly one task directory; -list supplies one task per non-comment line.
  Relative list entries use --client-path/--worker-path when provided, otherwise cwd.
  -j, --shard-bits, and --window-ms must be positive.
  -qps accepts true/false (also 1/0, yes/no).  false skips 10ms QPS aggregation
  and omits its HTML/JavaScript payload for large, high-QPS data sets.

Examples:
  # One task directory. Client and Worker logs are under the same directory.
  python3 kvcache_trace_report.py -f /path/to/task -type setandget -j 32

  # One task directory with separate client and Worker roots.
  python3 kvcache_trace_report.py -f case-a --client-path /logs/client \
      --worker-path /logs/worker -type get -j 32

  # Multiple task directories from html.txt, one relative directory per line.
  python3 kvcache_trace_report.py -list html.txt --client-path /logs/client \
      --worker-path /logs/worker --output-dir report -type setandget -j 32

  # Display all supported options.
  python3 kvcache_trace_report.py -h
"""

import argparse
from pathlib import Path
import gc
import glob
import gzip
import json
import multiprocessing as mp
import os
import shutil
import sys
import tempfile
import time
from typing import Any, Dict, List, Optional, Tuple

# =============================================================================
# CONFIG
# =============================================================================

CLIENT_ACCESS_PATTERNS = ['ds_client_access*.log*']
CLIENT_INFO_PATTERNS = ['ds_client*INFO*.log*']
WORKER_ACCESS_PATTERNS = ['access*.log*']
WORKER_INFO_PATTERNS = ['kvcache*INFO*.log*', 'kvcache*INFO*']

MARKER_ZMQ_SLOW = b'[ZMQ_RPC_FRAMEWORK_SLOW]'
MARKER_BRPC_SLOW = b'[BRPC_RPC_FRAMEWORK_SLOW]'
MARKER_QUERY_MASTER = b'Query metadata from master:'
MARKER_CREATEMETA = b'CreateMeta'
MARKER_CREATE_META_TO_MASTER = b'Create meta to master'
MARKER_GET_DONE = b'[Get] Done'
MARKER_SET_DONE = b'[Set] Done'
MARKER_GET_RECV = b'[Get] Receive'
MARKER_REMOTE_PULL = b'Remote Pull'
MARKER_REMOTE_DONE = b'[Get] Remote done'
MARKER_URMA = b'[URMA_ELAPSED_TOTAL]'
MARKER_POLL_GAP = b'[URMA_ELAPSED_THREAD_SHED]'
MARKER_URMA_PERF = b'[URMA_PERF]'
MARKER_WORKER_CREATE = b'Worker Create'
MARKER_PUBLISH_DONE = b'Publish done'


# =============================================================================
# UTILITIES
# =============================================================================

def parse_kv(field: bytes) -> Dict[str, str]:
    """Parse {k:v,k2:v2} quickly."""
    result = {}
    f = field.strip()
    if f.startswith(b'{'):
        f = f[1:]
    if f.endswith(b'}'):
        f = f[:-1]
    for p in f.split(b','):
        p = p.strip()
        if not p:
            continue
        idx = p.find(b':')
        if idx == -1:
            continue
        key = p[:idx].strip().decode('utf-8', errors='replace')
        value = p[idx + 1:].strip().decode('utf-8', errors='replace')
        result[key] = value
    return result


def ts_to_us(ts: bytes) -> int:
    """ISO timestamp -> microseconds since epoch (UTC)."""
    try:
        s = ts.strip().decode('ascii')
        from calendar import timegm
        epoch = timegm((int(s[0:4]), int(s[5:7]), int(s[8:10]),
                        int(s[11:13]), int(s[14:16]), int(s[17:19]),
                        0, 0, 0))
        return epoch * 1_000_000 + int(s[20:26])
    except Exception:
        return 0


def shard_id(tid: bytes, bits: int) -> int:
    try:
        return (int(tid[:8], 16) >> (32 - bits)) & ((1 << bits) - 1)
    except ValueError:
        import hashlib
        h = hashlib.md5(tid).digest()
        return (int.from_bytes(h[:4], 'big') >> (32 - bits)) & ((1 << bits) - 1)


def find_files(dirs: List[str], patterns: List[str]) -> List[str]:
    seen = set()
    out = []
    for d in dirs:
        if not os.path.isdir(d):
            continue
        for root, _, names in os.walk(d):
            for pat in patterns:
                for n in names:
                    if glob.fnmatch.fnmatch(n, pat):
                        fp = os.path.join(root, n)
                        if fp not in seen:
                            seen.add(fp)
                            out.append(fp)
    return sorted(out)


def expand(paths: List[str]) -> List[str]:
    d = set()
    for p in paths:
        if '*' in p or '?' in p:
            for m in glob.glob(p, recursive=True):
                if os.path.isdir(m):
                    d.add(os.path.abspath(m))
        elif os.path.isdir(p):
            d.add(os.path.abspath(p))
    return sorted(d)


def extract_ip(fp: str) -> Optional[str]:
    import re
    m = re.search(r'(?:SDK_)?(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})', fp)
    return m.group(1) if m else None


def extract_int_b(line: bytes, marker: bytes) -> Optional[int]:
    s = line.find(marker)
    if s == -1:
        return None
    s += len(marker)
    while s < len(line) and line[s:s + 1] in (b':', b' ', b'\t'):
        s += 1
    e = s
    while e < len(line) and line[e:e + 1].isdigit():
        e += 1
    try:
        return int(line[s:e]) if e > s else None
    except ValueError:
        return None


def extract_float_b(line: bytes, marker: bytes) -> Optional[float]:
    s = line.find(marker)
    if s == -1:
        return None
    s += len(marker)
    e = line.find(b'ms', s)
    if e == -1:
        e = line.find(b',', s)
    if e == -1:
        e = len(line)
    try:
        return float(line[s:e].strip())
    except ValueError:
        return None


# =============================================================================
# LINE PARSERS - Phase 1 extracts ALL fields (no data loss)
# =============================================================================

def parse_access_line(line: bytes, fp: str, is_client: bool) -> Optional[Tuple[bytes, bytes]]:
    """
    Parse access log line. Returns (trace_id, json_bytes) or None.
    Format: timestamp | L | file:line | logger | tid:pid | trace_id | tenant | status | req_type | latency | size | {params} | [error]
    """
    parts = line.split(b'|')
    if len(parts) < 8:
        return None

    tid = parts[5].strip() if len(parts) > 5 else b''
    status_idx = 7
    # Old access logs use ``trace_id | tenant | status``. Current kvtest emits
    # a non-UUID trace id in the same fixed field, so do not require UUID shape.
    if not tid or tid in (b'-', b'null', b'None'):
        tid = None

    if tid is None or status_idx + 4 >= len(parts):
        return None

    try:
        ts_bytes = parts[0].strip()[:26]
        status_code = int(parts[status_idx].strip())
        req_type = parts[status_idx + 1].strip().decode('utf-8', errors='replace')
        latency = int(parts[status_idx + 2].strip())
        data_size = int(parts[status_idx + 3].strip())
        params = parse_kv(parts[status_idx + 4].strip())

        # Error: only extract error info when status_code indicates failure
        error = ""
        if status_code != 0 and len(parts) > status_idx + 5:
            err_b = parts[-1].strip()
            if err_b:
                error = err_b.decode('utf-8', errors='replace')

        data = {
            '_cat': 'ca' if is_client else 'wa',
            'timestamp': ts_bytes.decode('ascii', errors='replace').strip(),
            'timestamp_us': ts_to_us(ts_bytes),
            'status_code': status_code,
            'request_type': req_type,
            'latency_us': latency,
            'data_size': data_size,
            'params': params,
            'ip': (parts[3].strip().decode('utf-8', errors='replace')
                   if len(parts) > 3 and parts[3].strip() else extract_ip(fp)),
            'error': error,
        }
        return (tid, json.dumps(data, ensure_ascii=False, separators=(',', ':')).encode())
    except (ValueError, IndexError):
        return None


def parse_client_info_line(line: bytes, fp: str) -> Optional[Tuple[bytes, bytes]]:
    """Parse RPC_FRAMEWORK_SLOW (ZMQ or BRPC) from client info logs. Extract ALL fields."""
    is_zmq = MARKER_ZMQ_SLOW in line
    is_brpc = MARKER_BRPC_SLOW in line
    if not is_zmq and not is_brpc and MARKER_URMA not in line:
        return None

    tid_marker = b'trace_id='
    tid_start = line.find(tid_marker)
    if tid_start != -1:
        tid_start += len(tid_marker)
        tid_end = line.find(b' ', tid_start)
        if tid_end == -1:
            tid_end = len(line)
        trace_id = line[tid_start:tid_end]
    else:
        # Generic UrmaManager logs use the normal pipe-delimited log format,
        # where the trace id is field 6, unlike framework slow logs.
        parts = line.split(b'|')
        trace_id = parts[5].strip() if len(parts) > 5 else b''
    if not trace_id or trace_id in (b'-', b'null', b'None'):
        return None

    # Extract timestamp from beginning of line
    ts_end = line.find(b' ')
    ts_str = line[:ts_end].decode('ascii', errors='replace').strip() if ts_end > 0 else ''

    def extract_int(marker: bytes) -> Optional[int]:
        return extract_int_b(line, marker)

    def extract_float(marker: bytes) -> Optional[float]:
        return extract_float_b(line, marker)

    # Client-side UB/URMA completion.  Keep this trace-correlated just like the
    # worker-side record; the same UrmaManager marker is emitted by both roles.
    if MARKER_URMA in line:
        data = {
            '_cat': 'ci', '_sub': 'ur',
            'timestamp': ts_str,
            'timestamp_us': ts_to_us(line[:ts_end]) if ts_end > 0 else 0,
            'urma_total_ms': extract_float(b'cost '),
            'urma_inflight': extract_int(b'urma_inflight_wr_count:'),
            'data_size': extract_int(b'dataSize:'),
            'urma_sched_us': (extract_int(b'wakeSchedLatencyUs:')
                              or extract_int(b'urmaWriteWakeSchedLatencyUs:')
                              or extract_int(b'firstUrmaWriteWakeSchedLatencyUs:')
                              or extract_int(b'secondUrmaWriteWakeSchedLatencyUs:')),
        }
        return (trace_id, json.dumps(data, ensure_ascii=False, separators=(',', ':')).encode())

    # Extract ALL latency fields (complete data preservation)
    data = {
        '_cat': 'cr',
        'timestamp': ts_str,
        'timestamp_us': ts_to_us(line[:ts_end]) if ts_end > 0 else 0,
        'rpc_type': 'zmq' if is_zmq else 'brpc',
        'method': (line[line.find(b'method=') + len(b'method='):].split(b' ', 1)[0]
                   .decode('utf-8', errors='replace') if b'method=' in line else ''),
        'framework_us': extract_int(b'framework_us='),
        'e2e_us': extract_int(b'e2e_us='),
        'client_req_framework_us': extract_int(b'client_req_framework_us='),
        'remote_processing_us': extract_int(b'remote_processing_us='),
        'client_rsp_framework_us': extract_int(b'client_rsp_framework_us='),
        'server_req_queue_us': extract_int(b'server_req_queue_us='),
        'server_exec_us': extract_int(b'server_exec_us='),
        'server_rsp_queue_us': extract_int(b'server_rsp_queue_us='),
        'network_residual_us': extract_int(b'network_residual_us='),
    }
    return (trace_id, json.dumps(data, ensure_ascii=False, separators=(',', ':')).encode())


def parse_worker_info_line(line: bytes, fp: str) -> Optional[Tuple[bytes, bytes]]:
    """Parse worker info logs. Extract ALL fields, preserve timestamp."""
    parts = line.split(b'|')
    trace_id = parts[5].strip() if len(parts) > 5 else None
    if trace_id in (b'', b'-', b'null', b'None'):
        trace_id = None
    if trace_id is None:
        return None

    # Extract timestamp from first field
    ts_bytes = parts[0].strip()[:26] if parts else b''
    ts_str = ts_bytes.decode('ascii', errors='replace').strip() if ts_bytes else ''
    ts_us = ts_to_us(ts_bytes.strip()) if ts_bytes else 0
    local_ip = (parts[3].strip().decode('utf-8', errors='replace')
                if len(parts) > 3 and parts[3].strip() else extract_ip(fp))

    def extract_int(marker: bytes) -> Optional[int]:
        return extract_int_b(line, marker)

    def extract_float(marker: bytes) -> Optional[float]:
        return extract_float_b(line, marker)

    # Check for RPC FRAMEWORK SLOW (ZMQ or BRPC) - has all latency fields
    is_rpc_zmq = MARKER_ZMQ_SLOW in line
    is_rpc_brpc = MARKER_BRPC_SLOW in line
    if is_rpc_zmq or is_rpc_brpc:
        data = {
            '_cat': 'wi', '_sub': 'rpc',
            'timestamp': ts_str,
            'timestamp_us': ts_us,
            'local_ip': local_ip,
            'rpc_type': 'zmq' if is_rpc_zmq else 'brpc',
            'framework_us': extract_int(b'framework_us='),
            'e2e_us': extract_int(b'e2e_us='),
            'client_req_framework_us': extract_int(b'client_req_framework_us='),
            'remote_processing_us': extract_int(b'remote_processing_us='),
            'client_rsp_framework_us': extract_int(b'client_rsp_framework_us='),
            'server_req_framework_us': extract_int(b'server_req_framework_us='),
            'client_req_network_us': extract_int(b'client_req_network_us='),
            'client_rsp_network_us': extract_int(b'client_rsp_network_us='),
            'server_req_queue_us': extract_int(b'server_req_queue_us='),
            'server_exec_us': extract_int(b'server_exec_us='),
            'server_rsp_queue_us': extract_int(b'server_rsp_queue_us='),
            'network_residual_us': extract_int(b'network_residual_us='),
        }
        return (trace_id, json.dumps(data, ensure_ascii=False, separators=(',', ':')).encode())

    # Query metadata from master (GET)
    if MARKER_QUERY_MASTER in line:
        start = line.find(MARKER_QUERY_MASTER) + len(MARKER_QUERY_MASTER)
        rest = line[start:].strip()
        end = rest.find(b',')
        if end == -1:
            end = len(rest)
        master_ip = rest[:end].strip().decode('utf-8', errors='replace')
        data = {
            '_cat': 'wi', '_sub': 'mq',
            'timestamp': ts_str, 'timestamp_us': ts_us,
            'local_ip': local_ip,
            'master_ip': master_ip,
        }
        return (trace_id, json.dumps(data, ensure_ascii=False, separators=(',', ':')).encode())

    # CreateMeta (SET) - indicates master RPC for SET operations
    if MARKER_CREATEMETA in line or MARKER_CREATE_META_TO_MASTER in line:
        data = {
            '_cat': 'wi', '_sub': 'cm',
            'timestamp': ts_str, 'timestamp_us': ts_us,
            'local_ip': local_ip,
        }
        return (trace_id, json.dumps(data, ensure_ascii=False, separators=(',', ':')).encode())

    # [Get] Done
    if MARKER_GET_DONE in line:
        data = {
            '_cat': 'wi', '_sub': 'gd',
            'timestamp': ts_str, 'timestamp_us': ts_us,
            'local_ip': local_ip,
            'total_cost_ms': extract_float(b'totalCost:'),
        }
        return (trace_id, json.dumps(data, ensure_ascii=False, separators=(',', ':')).encode())

    # [Set] Done
    if MARKER_SET_DONE in line:
        data = {
            '_cat': 'wi', '_sub': 'sd',
            'timestamp': ts_str, 'timestamp_us': ts_us,
            'local_ip': local_ip,
            'total_cost_ms': extract_float(b'totalCost:'),
        }
        return (trace_id, json.dumps(data, ensure_ascii=False, separators=(',', ':')).encode())

    # [Get] Receive
    if MARKER_GET_RECV in line:
        data = {
            '_cat': 'wi', '_sub': 'gr',
            'timestamp': ts_str, 'timestamp_us': ts_us,
            'local_ip': local_ip,
        }
        return (trace_id, json.dumps(data, ensure_ascii=False, separators=(',', ':')).encode())

    # Remote Pull
    if MARKER_REMOTE_PULL in line:
        data = {
            '_cat': 'wi', '_sub': 'rp',
            'timestamp': ts_str, 'timestamp_us': ts_us,
            'local_ip': local_ip,
            'elapsed_ms': extract_float(b'elapsed:'),
        }
        return (trace_id, json.dumps(data, ensure_ascii=False, separators=(',', ':')).encode())

    # [Get] Remote done
    if MARKER_REMOTE_DONE in line:
        data = {
            '_cat': 'wi', '_sub': 'rd',
            'timestamp': ts_str, 'timestamp_us': ts_us,
            'local_ip': local_ip,
        }
        return (trace_id, json.dumps(data, ensure_ascii=False, separators=(',', ':')).encode())

    # URMA elapsed
    if MARKER_URMA in line:
        # Extract src/target addresses for network path analysis
        src_addr = None
        tgt_addr = None
        src_start = line.find(b'src address:')
        if src_start != -1:
            src_start += len(b'src address:')
            src_end = line.find(b',', src_start)
            if src_end == -1:
                src_end = line.find(b' ', src_start)
            if src_end != -1:
                src_addr = line[src_start:src_end].strip().decode('utf-8', errors='replace')
        tgt_start = line.find(b'target address:')
        if tgt_start != -1:
            tgt_start += len(b'target address:')
            tgt_end = line.find(b',', tgt_start)
            if tgt_end == -1:
                tgt_end = line.find(b' ', tgt_start)
            if tgt_end != -1:
                tgt_addr = line[tgt_start:tgt_end].strip().decode('utf-8', errors='replace')

        data = {
            '_cat': 'wi', '_sub': 'ur',
            'timestamp': ts_str, 'timestamp_us': ts_us,
            'local_ip': local_ip,
            'urma_total_ms': extract_float(b'cost '),
            'urma_inflight': extract_int(b'urma_inflight_wr_count:'),
            'data_size': extract_int(b'dataSize:'),
            'src_address': src_addr,
            'target_address': tgt_addr,
            'urma_sched_us': (extract_int(b'wakeSchedLatencyUs:')
                              or extract_int(b'urmaWriteWakeSchedLatencyUs:')
                              or extract_int(b'firstUrmaWriteWakeSchedLatencyUs:')
                              or extract_int(b'secondUrmaWriteWakeSchedLatencyUs:')),
        }
        return (trace_id, json.dumps(data, ensure_ascii=False, separators=(',', ':')).encode())

    # Worker Create
    if MARKER_WORKER_CREATE in line:
        data = {
            '_cat': 'wi', '_sub': 'wc',
            'timestamp': ts_str, 'timestamp_us': ts_us,
            'local_ip': local_ip,
            'elapsed_ms': extract_float(b'elapsed:'),
        }
        return (trace_id, json.dumps(data, ensure_ascii=False, separators=(',', ':')).encode())

    # Publish done
    if MARKER_PUBLISH_DONE in line:
        data = {
            '_cat': 'wi', '_sub': 'pd',
            'timestamp': ts_str, 'timestamp_us': ts_us,
            'local_ip': local_ip,
            'elapsed_ms': extract_float(b'elapsed:'),
        }
        return (trace_id, json.dumps(data, ensure_ascii=False, separators=(',', ':')).encode())

    return None


# =============================================================================
# GLOBAL INFO PARSERS (no traceId)
# =============================================================================

def _parse_poll_gap(line: bytes) -> Optional[bytes]:
    """Parse POLL GAP or SLEEP WAKEUP log into info format: ts\ttype:...\tkey:val..."""
    ts_end = line.find(b' ')
    ts_str = line[:ts_end].decode('ascii', errors='replace').strip() if ts_end > 0 else ''

    if b'lastPollEndToThisPollStart' in line:
        fields = [ts_str.encode(), b'type:poll_gap']
        v1 = extract_int_b(line, b'lastPollEndToThisPollStart')
        v2 = extract_int_b(line, b'lastPollStartToThisPollStart')
        cpuid = extract_int_b(line, b'cpuid:')
        if v1 is not None:
            fields.append(f'lastPollEndToThisPollStart:{v1}'.encode())
        if v2 is not None:
            fields.append(f'lastPollStartToThisPollStart:{v2}'.encode())
        if cpuid is not None:
            fields.append(f'cpuid:{cpuid}'.encode())
        return b'\t'.join(fields)

    if b'nanosleep' in line:
        fields = [ts_str.encode(), b'type:sleep_wakeup']
        cost = None
        cost_start = line.find(b'cost ')
        if cost_start != -1:
            cost_start += len(b'cost ')
            cost_end = line.find(b'us', cost_start)
            if cost_end != -1:
                try:
                    cost = float(line[cost_start:cost_end].strip())
                except ValueError:
                    cost = None
        cpuid = extract_int_b(line, b'cpuid:')
        if cost is not None:
            fields.append(f'nanosleep_cost_us:{cost}'.encode())
        if cpuid is not None:
            fields.append(f'cpuid:{cpuid}'.encode())
        return b'\t'.join(fields)

    return None


def _parse_urma_perf_lines(perf_lines: List[bytes]) -> Optional[bytes]:
    """Parse URMA PERF multi-line block into info format: type:urma_perf\tkey:val...
    Extracts retry_count, avg/max/p99/p9999 (ns->us) for target types."""
    retry_count = None
    # {type_name: {'avg': x, 'max': x, 'p99': x, 'p9999': x}}
    metrics = {}
    target_types = [b'UB_JETTY_POST_SEND', b'BOND_JETTY_POST_SEND',
                    b'UB_POLL_JFC', b'BOND_POLL_JFC']

    for line in perf_lines:
        line = line.strip()
        if not line or line.startswith(b'+') or b'Type' in line:
            continue
        if b'retry_count:' in line:
            retry_count = extract_int_b(line, b'retry_count:')
            continue
        for type_key in target_types:
            if type_key in line:
                parts = line.split(b'|')
                if len(parts) >= 8:
                    try:
                        prefix = type_key.decode('ascii')
                        metrics[prefix] = {
                            'avg': round(int(parts[2].strip()) / 1000, 2),
                            'max': round(int(parts[4].strip()) / 1000, 2),
                            'p99': round(int(parts[6].strip()) / 1000, 2),
                            'p9999': round(int(parts[7].strip()) / 1000, 2),
                        }
                    except (ValueError, IndexError):
                        pass
                break

    if retry_count is None and not metrics:
        return None

    fields = [b'type:urma_perf']
    if retry_count is not None:
        fields.append(f'retry_count:{retry_count}'.encode())
    for name in target_types:
        name_str = name.decode('ascii')
        if name_str in metrics:
            m = metrics[name_str]
            fields.append(f'{name_str}_avg:{m["avg"]}'.encode())
            fields.append(f'{name_str}_max:{m["max"]}'.encode())
            fields.append(f'{name_str}_p99:{m["p99"]}'.encode())
            fields.append(f'{name_str}_p9999:{m["p9999"]}'.encode())

    return b'\t'.join(fields)


# =============================================================================
# PHASE 1: PARSE & SHARD
# =============================================================================

def parse_one_file(args):
    """Parse a single file, write records to shard files and optional info file.

    Returns (trace_count, info_count).
    """
    filepath, shard_dir, shard_bits, ptype, info_dir = args
    handles = {}
    cnt = 0
    info_cnt = 0
    info_fh = None
    if info_dir and ptype == 3:
        os.makedirs(info_dir, exist_ok=True)
        info_path = os.path.join(info_dir, f'gi_{os.getpid()}_{os.path.basename(filepath)}.tmp')
        info_fh = open(info_path, 'ab')

    def get_fh(sid):
        if sid not in handles:
            handles[sid] = open(os.path.join(shard_dir, f's_{sid}.tmp'), 'ab')
        return handles[sid]

    def is_timestamp_line(line: bytes) -> bool:
        try:
            return (line[0:1] == b'2' and line[4:5] == b'-' and
                    line[7:8] == b'-' and line[10:11] == b'T')
        except (IndexError, ValueError):
            return False

    try:
        opener = gzip.open if filepath.endswith('.gz') else open
        with opener(filepath, 'rb') as f:
            if ptype == 0:
                def parse_client_access(line, filepath):
                    return parse_access_line(line, filepath, True)

                parser = parse_client_access
            elif ptype == 1:
                parser = parse_client_info_line
            elif ptype == 2:
                def parse_worker_access(line, filepath):
                    return parse_access_line(line, filepath, False)

                parser = parse_worker_access
            else:
                parser = parse_worker_info_line

            if ptype == 3 and info_dir:
                # Worker info with global info extraction
                perf_header = None
                perf_buffer = []
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    if is_timestamp_line(line):
                        # Flush pending URMA PERF
                        if perf_header is not None:
                            ts_end = perf_header.find(b' ')
                            ts_perf = perf_header[:ts_end].decode('ascii', errors='replace').strip() if ts_end > 0 else ''
                            info_body = _parse_urma_perf_lines(perf_buffer)
                            if info_body and info_fh:
                                info_fh.write(ts_perf.encode() + b'\t' + info_body + b'\n')
                                info_fh.flush()
                                info_cnt += 1
                            perf_header = None
                            perf_buffer = []
                        # Check global info markers
                        if MARKER_POLL_GAP in line:
                            info = _parse_poll_gap(line)
                            if info and info_fh:
                                info_fh.write(info + b'\n')
                                info_fh.flush()
                                info_cnt += 1
                            continue
                        if MARKER_URMA_PERF in line:
                            perf_header = line
                            continue
                        # Normal worker info trace log
                        r = parser(line, filepath)
                        if r is not None:
                            tid, jbytes = r
                            sid = shard_id(tid, shard_bits)
                            fh = get_fh(sid)
                            fh.write(tid + b'\t' + jbytes + b'\n')
                            cnt += 1
                    else:
                        # Non-timestamp: URMA PERF continuation
                        if perf_header is not None:
                            perf_buffer.append(line)
                # Flush final pending PERF
                if perf_header is not None:
                    ts_end = perf_header.find(b' ')
                    ts_perf = perf_header[:ts_end].decode('ascii', errors='replace').strip() if ts_end > 0 else ''
                    info_body = _parse_urma_perf_lines(perf_buffer)
                    if info_body and info_fh:
                        info_fh.write(ts_perf.encode() + b'\t' + info_body + b'\n')
                        info_fh.flush()
                        info_cnt += 1
            else:
                # Normal streaming parse
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    r = parser(line, filepath)
                    if r is None:
                        continue
                    tid, jbytes = r
                    sid = shard_id(tid, shard_bits)
                    fh = get_fh(sid)
                    fh.write(tid + b'\t' + jbytes + b'\n')
                    cnt += 1
    except Exception as e:
        import traceback
        print(f"[ERROR] parse_one_file {filepath}: {e}", file=sys.stderr)
        traceback.print_exc()
    finally:
        for h in handles.values():
            h.close()
        if info_fh:
            info_fh.close()
    return cnt, info_cnt, ptype


def phase1(client_dirs, worker_dirs, shard_dir, shard_bits, nworkers, info_dir=None):
    print("[Phase 1] Parsing & sharding...")
    all_files = []
    ca = find_files(client_dirs, CLIENT_ACCESS_PATTERNS)
    ci = find_files(client_dirs, CLIENT_INFO_PATTERNS)
    wa = find_files(worker_dirs, WORKER_ACCESS_PATTERNS)
    wi = find_files(worker_dirs, WORKER_INFO_PATTERNS)
    for f in ca:
        all_files.append((f, shard_dir, shard_bits, 0, info_dir))
    for f in ci:
        all_files.append((f, shard_dir, shard_bits, 1, info_dir))
    for f in wa:
        all_files.append((f, shard_dir, shard_bits, 2, info_dir))
    for f in wi:
        all_files.append((f, shard_dir, shard_bits, 3, info_dir))

    print(f"  Files: ca={len(ca)} ci={len(ci)} wa={len(wa)} wi={len(wi)} total={len(all_files)}")
    if wi:
        for f in wi[:5]:
            print(f"    wi: {os.path.basename(f)}")
        if len(wi) > 5:
            print(f"    ... and {len(wi)-5} more")
        print(f"  Info extraction: {'enabled' if info_dir else 'disabled'} (info_dir={info_dir})")
    if not all_files:
        return 0, 0

    total_recs = 0
    total_info = 0
    category_records = [0, 0, 0, 0]
    # Use explicit close/join instead of 'with' to avoid terminate() hang.
    # maxtasksperchild: restart worker after N tasks to prevent memory bloat.
    if nworkers == 1:
        iterator = map(parse_one_file, all_files)
        pool = None
    else:
        pool = mp.Pool(nworkers, maxtasksperchild=4)
        chunksize = max(1, len(all_files) // (nworkers * 4))
        iterator = pool.imap_unordered(parse_one_file, all_files, chunksize=chunksize)
    try:
        for i, (c, ic, ptype) in enumerate(iterator):
            total_recs += c
            total_info += ic
            category_records[ptype] += c
            if (i + 1) % max(1, len(all_files) // 20) == 0:
                print(f"  {i+1}/{len(all_files)} files, {total_recs} records, {total_info} info")
        if pool:
            pool.close()
            pool.join()
    except Exception:
        if pool:
            pool.terminate()
            pool.join()
        raise
    print(f"  Done: {total_recs} records -> {1 << shard_bits} shards, {total_info} info lines")
    print(f"  Parsed records: client-access={category_records[0]} client-info={category_records[1]} "
          f"worker-access={category_records[2]} worker-info={category_records[3]}")
    if category_records[0] == 0:
        print("  [WARN] client-access=0: no usable client access trace was parsed. "
              "Expected pipe field 6 to contain a non-empty trace id and field 9 to be DS_KV_CLIENT_GET/SET.")
    return total_recs, total_info


# =============================================================================
# PHASE 2: MERGE & OUTPUT
# =============================================================================

def rpc_lat(rpc):
    """Compute RPC latency breakdown."""
    if not rpc:
        return {'total_us': None, 'network_us': None, 'framework_us': None}
    e2e = rpc.get('e2e_us')
    ex = rpc.get('server_exec_us')
    nr = rpc.get('network_residual_us')
    t = None
    if e2e is not None and ex is not None:
        t = max(0, e2e - ex)
    fw = None
    if t is not None and nr is not None:
        fw = max(0, t - nr)
    return {'total_us': t, 'network_us': nr, 'framework_us': fw}


def build_seg(ca, wa_list, cr_list, mr, rr, urma_list, client_urma_list=None):
    """Build latency segmentation.

    Supports two modes:
    - Mode A (Local Worker): Client -> Local Worker -> Master -> Remote Worker
      Has worker_access logs and local_worker RPC entries.
    - Mode B (Client Direct): Client -> Master -> Remote Worker
      No worker_access, no local_worker RPC. Client has 2+ RPC entries.
    """
    total = ca.get('latency_us', 0)

    # Sum of all worker_access latencies (filter None)
    worker_latency_sum = sum((wa.get('latency_us') or 0) for wa in wa_list) if wa_list else 0

    # Sum of all SDK RPC e2e values (filter None)
    sdk_rpc_e2e_sum = sum((rpc.get('e2e_us') or 0) for rpc in cr_list) if cr_list else 0

    # Detect mode: Mode B = Client has 2 RPC entries (direct to master + remote worker)
    is_client_direct = len(cr_list) >= 2

    # Common: URMA processing (max across all URMA operations for this trace)
    urma_proc = None
    urma_sched = None
    if urma_list:
        vals = [u.get('urma_total_ms', 0) for u in urma_list if u.get('urma_total_ms') is not None]
        if vals:
            urma_proc = int(max(vals) * 1000)
        sched_vals = [u.get('urma_sched_us') for u in urma_list if u.get('urma_sched_us') is not None]
        if sched_vals:
            urma_sched = max(sched_vals)

    client_urma_proc = None
    if client_urma_list:
        vals = [u.get('urma_total_ms', 0) for u in client_urma_list
                if u.get('urma_total_ms') is not None]
        if vals:
            client_urma_proc = int(max(vals) * 1000)

    # Common: remote_worker_processing and remote_worker_internal
    rem_proc = None
    if not is_client_direct and rr:
        rem_proc = rr.get('server_exec_us')
    elif is_client_direct and len(cr_list) >= 2:
        rem_proc = cr_list[1].get('server_exec_us')

    rem_int = None
    if rem_proc is not None and urma_proc is not None:
        rem_int = max(0, rem_proc - urma_proc)

    # SDK processing (common logic)
    sdk_proc = None
    if cr_list and sdk_rpc_e2e_sum > 0:
        sdk_proc = max(0, total - sdk_rpc_e2e_sum)
    elif client_urma_proc is not None:
        # SET can have client-side UB without a sampled RPC slow log.  The
        # worker CREATE/PUBLISH access records are not the client critical
        # path, so subtract client UB rather than treating it as SDK time.
        sdk_proc = max(0, total - client_urma_proc)
    elif wa_list:
        sdk_proc = max(0, total - worker_latency_sum)

    if is_client_direct:
        # Mode B: Client Direct
        cr_master = cr_list[0]  # Client <-> Master
        cr_remote = cr_list[1]  # Client <-> Remote Worker

        return {
            'sdk_total_us': total,
            # Mode B fields
            'client_master_rpc': rpc_lat(cr_master),
            'client_remote_rpc': rpc_lat(cr_remote),
            'master_processing_us': cr_master.get('server_exec_us'),
            'remote_worker_processing_us': cr_remote.get('server_exec_us'),
            'urma_processing_us': urma_proc,
            'client_urma_processing_us': client_urma_proc,
            'urma_sched_us': urma_sched,
            'remote_worker_internal_us': rem_int,
            'sdk_processing_us': sdk_proc,
            # Mode A fields (empty)
            'sdk_rpc': {'total_us': None, 'network_us': None, 'framework_us': None},
            'master_rpc': {'total_us': None, 'network_us': None, 'framework_us': None},
            'remote_worker_rpc': {'total_us': None, 'network_us': None, 'framework_us': None},
            'local_worker_internal_us': None,
        }
    else:
        # Mode A: Local Worker
        sdk_rpc = rpc_lat(cr_list[0]) if cr_list else {'total_us': None, 'network_us': None, 'framework_us': None}
        mas_rpc = rpc_lat(mr)
        mas_proc = mr.get('server_exec_us') if mr else None
        rem_rpc = rpc_lat(rr)

        # Local worker internal
        loc_int = None
        me = mr.get('e2e_us') if mr else None
        re = rr.get('e2e_us') if rr else None
        if wa_list:
            loc_int = worker_latency_sum
            if me is not None:
                loc_int = max(0, loc_int - me)
            if re is not None:
                loc_int = max(0, loc_int - re)

        return {
            'sdk_total_us': total,
            # Mode A fields
            'sdk_rpc': sdk_rpc,
            'master_rpc': mas_rpc,
            'master_processing_us': mas_proc,
            'remote_worker_rpc': rem_rpc,
            'remote_worker_processing_us': rem_proc,
            'urma_processing_us': urma_proc,
            'client_urma_processing_us': client_urma_proc,
            'urma_sched_us': urma_sched,
            'remote_worker_internal_us': rem_int,
            'local_worker_internal_us': loc_int,
            'sdk_processing_us': sdk_proc,
            # Mode B fields (empty)
            'client_master_rpc': {'total_us': None, 'network_us': None, 'framework_us': None},
            'client_remote_rpc': {'total_us': None, 'network_us': None, 'framework_us': None},
        }


def merge_shard(args):
    """Merge one shard file -> one temp output file. Returns trace count."""
    shard_file, out_temp, filters = args

    traces = {}
    with open(shard_file, 'rb') as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            tab = line.find(b'\t')
            if tab == -1:
                continue
            tid = line[:tab]
            try:
                rec = json.loads(line[tab+1:])
            except json.JSONDecodeError:
                continue

            cat = rec.pop('_cat', None)
            if cat is None:
                continue
            if tid not in traces:
                traces[tid] = {}

            if cat == 'ca':
                if filters.get('request_types') and rec.get('request_type') not in filters['request_types']:
                    traces[tid]['_skip'] = True
                    continue
                if filters.get('has_error') is not None:
                    has_err = rec.get('status_code', 0) != 0 or bool(rec.get('error', ''))
                    if has_err != filters['has_error']:
                        traces[tid]['_skip'] = True
                        continue
                traces[tid]['ca'] = rec
            elif cat == 'cr':
                if 'cr' not in traces[tid]:
                    traces[tid]['cr'] = []
                traces[tid]['cr'].append(rec)
            elif cat == 'ci':
                if 'ci' not in traces[tid]:
                    traces[tid]['ci'] = []
                traces[tid]['ci'].append(rec)
            elif cat == 'wa':
                if 'wa' not in traces[tid]:
                    traces[tid]['wa'] = []
                traces[tid]['wa'].append(rec)
            elif cat == 'wi':
                if 'wi' not in traces[tid]:
                    traces[tid]['wi'] = []
                traces[tid]['wi'].append(rec)

    cnt = 0
    with open(out_temp, 'w', encoding='utf-8') as out:
        for tid, d in traces.items():
            if d.get('_skip'):
                continue
            ca = d.get('ca')
            if not ca:
                continue

            wa_list = d.get('wa', [])  # worker_access list
            cr_list = d.get('cr', [])  # client_rpc list
            wi_list = d.get('wi', [])
            ci_list = d.get('ci', [])

            master_ip = None
            mr = rr = None
            gd = gr = rp = rd = sd = wc = pd = None
            urma = []
            client_urma = [
                {
                    'timestamp_us': rec.get('timestamp_us', 0),
                    'urma_total_ms': rec.get('urma_total_ms'),
                    'urma_inflight': rec.get('urma_inflight'),
                    'data_size': rec.get('data_size'),
                    'urma_sched_us': rec.get('urma_sched_us'),
                }
                for rec in ci_list if rec.get('_sub') == 'ur'
            ]
            # local_ip: prefer worker_access ip, fallback to worker_info local_ip
            worker_access_ips = set(wa.get('ip') for wa in wa_list if wa.get('ip'))
            local_ip = None
            for wa in wa_list:
                if wa.get('ip'):
                    local_ip = wa['ip']
                    break
            # has_meta: whether we've seen Query metadata (GET) or CreateMeta (SET) log
            has_meta = False

            # Determine request type for RPC classification
            is_set = 'SET' in ca.get('request_type', '')

            for rec in wi_list:
                sub = rec.pop('_sub', None)
                lip = rec.pop('local_ip', None)
                if lip and not local_ip:
                    local_ip = lip

                if sub == 'rpc':
                    if mr is None:
                        mr = rec
                    else:
                        rr = rec
                elif sub == 'mq':
                    master_ip = rec.get('master_ip')
                    has_meta = True
                elif sub == 'cm':
                    has_meta = True
                elif sub == 'gd':
                    gd = {'timestamp_us': rec.get('timestamp_us', 0),
                          'total_cost_ms': rec.get('total_cost_ms')}
                elif sub == 'sd':
                    sd = {'timestamp_us': rec.get('timestamp_us', 0),
                          'total_cost_ms': rec.get('total_cost_ms')}
                elif sub == 'gr':
                    gr = {'timestamp_us': rec.get('timestamp_us', 0)}
                elif sub == 'rp':
                    rp = {'timestamp_us': rec.get('timestamp_us', 0),
                          'elapsed_ms': rec.get('elapsed_ms')}
                elif sub == 'rd':
                    rd = {'timestamp_us': rec.get('timestamp_us', 0)}
                elif sub == 'ur':
                    urma.append({
                        'timestamp_us': rec.get('timestamp_us', 0),
                        'urma_total_ms': rec.get('urma_total_ms'),
                        'urma_inflight': rec.get('urma_inflight'),
                        'data_size': rec.get('data_size'),
                        'src_address': rec.get('src_address'),
                        'target_address': rec.get('target_address'),
                        'urma_sched_us': rec.get('urma_sched_us'),
                    })
                elif sub == 'wc':
                    wc = {'timestamp_us': rec.get('timestamp_us', 0),
                          'elapsed_ms': rec.get('elapsed_ms')}
                elif sub == 'pd':
                    pd = {'timestamp_us': rec.get('timestamp_us', 0),
                          'elapsed_ms': rec.get('elapsed_ms')}

            # Classify single RPC:
            # SET default -> master_rpc; GET -> check Query metadata
            if mr and not rr:
                if not is_set and not has_meta:
                    rr = mr
                    mr = None

            # Detect mode: Client Direct = 2+ client RPC entries
            is_client_direct = len(cr_list) >= 2
            is_get = 'GET' in ca.get('request_type', '')
            # URMA_ELAPSED_TOTAL is emitted for both GET and SET transfers;
            # do not drop worker-side UB samples merely because this is SET.
            seg = build_seg(ca, wa_list, cr_list, mr, rr, urma, client_urma)

            # QueryAndGet is identified by the worker access API.  Client slow
            # logs are sampled and therefore cannot be the classification key.
            path_role = 'remote_worker' if is_client_direct else 'local_worker'
            query_get_wa = next((wa for wa in wa_list
                                 if wa.get('request_type') == 'DS_POSIX_QUERY_AND_GET'), None)
            if query_get_wa is not None:
                transport = str((query_get_wa.get('params') or {}).get('transportType', '')).upper()
                path_role = 'remote_meta_worker' if transport == 'UB' else 'local_meta_worker'
                if cr_list:
                    qrpc = rpc_lat(cr_list[0])
                    seg['query_get_local_rpc' if path_role == 'local_meta_worker' else 'query_get_remote_rpc'] = qrpc
                    seg['query_get_server_exec_us'] = cr_list[0].get('server_exec_us')
                # QueryAndGet has its own client->meta-worker RPC.  Do not
                # also present it under the legacy SDK RPC name.
                seg['sdk_rpc'] = {'total_us': None, 'network_us': None, 'framework_us': None}

            # Clean worker_access: remove empty error fields
            for wa in wa_list:
                if not wa.get('error'):
                    wa.pop('error', None)

            # Build local_worker: None for Client Direct mode
            local_worker = None
            if not is_client_direct:
                local_worker = {
                    'master_ip': master_ip,
                    'master_rpc': mr,
                    'remote_worker_rpc': rr,
                    'get_done': gd,
                    'get_receive': gr,
                    'remote_pull': rp,
                    'remote_done': rd,
                    'set_done': sd,
                    'worker_create': wc,
                    'publish_done': pd,
                    'local_ip': local_ip,
                }

            result = {
                'trace_id': tid.decode('ascii'),
                'request_type': ca['request_type'],
                'timestamp': ca.get('timestamp', ''),
                'status_code': ca['status_code'],
                'latency_us': ca['latency_us'],
                'data_size': ca.get('data_size', 0),
                'params': ca.get('params', {}),
                'client_ip': ca.get('ip'),
                'error': ca.get('error', ''),
                'sdk_worker_rpc': cr_list if cr_list else None,
                'worker_access': wa_list if wa_list else None,
                'local_worker': local_worker,
                'urma': urma,
                'client_urma': client_urma,
                'path_role': path_role,
                'latency_segmentation': seg,
            }
            out.write(json.dumps(result, ensure_ascii=False, separators=(',', ':')) + '\n')
            cnt += 1
    # Force GC before returning to reduce memory bloat in reused worker processes
    gc.collect()
    return cnt


def merge_info_files(info_dir: str, output_path: str) -> int:
    """Collect all .info temp files, sort by timestamp, write to output. Returns line count."""
    if not os.path.isdir(info_dir):
        print(f"  [merge_info] info_dir not found: {info_dir}")
        return 0
    all_files = os.listdir(info_dir)
    tmp_files = [f for f in all_files if f.endswith('.tmp')]
    print(f"  [merge_info] dir={info_dir}, total files={len(all_files)}, .tmp files={len(tmp_files)}")
    temp_files = []
    for f in tmp_files:
        fp = os.path.join(info_dir, f)
        sz = os.path.getsize(fp)
        print(f"    {f}: {sz} bytes")
        if sz > 0:
            temp_files.append(fp)
    if not temp_files:
        print(f"  [merge_info] no non-empty .tmp files")
        return 0

    lines = []
    for fp in temp_files:
        with open(fp, 'rb') as f:
            for line in f:
                line = line.strip()
                if line:
                    lines.append(line)

    print(f"  [merge_info] collected {len(lines)} lines from {len(temp_files)} files")

    # Sort by timestamp (first tab-separated field)
    def sort_key(line: bytes):
        tab = line.find(b'\t')
        return line[:tab] if tab != -1 else line

    lines.sort(key=sort_key)

    # Ensure output directory exists
    out_dir = os.path.dirname(output_path)
    if out_dir and not os.path.isdir(out_dir):
        os.makedirs(out_dir, exist_ok=True)

    cnt = 0
    with open(output_path, 'wb') as out:
        for line in lines:
            out.write(line + b'\n')
            cnt += 1

    print(f"  [merge_info] wrote {cnt} lines -> {output_path}")
    return cnt


def safe_get(d, path, default=0):
    if not d:
        return default
    for p in path.split('.'):
        if isinstance(d, dict) and p in d:
            d = d[p]
        else:
            return default
    return d if d is not None else default



def phase2(shard_dir, output_file, nworkers, filters):
    print("\n[Phase 2] Merging shards...")
    sfiles = sorted([
        os.path.join(shard_dir, f) for f in os.listdir(shard_dir)
        if f.endswith('.tmp') and os.path.getsize(os.path.join(shard_dir, f)) > 0
    ])
    print(f"  Active shards: {len(sfiles)}")
    if not sfiles:
        return 0

    tmp_dir = os.path.dirname(output_file) or '.'
    tmps = [os.path.join(tmp_dir, f'.kv_{os.getpid()}_{i}.jl') for i in range(len(sfiles))]

    total = 0
    try:
        # Explicit pool management: close/join for graceful shutdown.
        # maxtasksperchild=4: restart worker after 4 shards to prevent memory bloat
        # from merge_shard's in-memory trace aggregation.
        if nworkers == 1:
            iterator = map(merge_shard, [(sf, tf, filters) for sf, tf in zip(sfiles, tmps)])
            pool = None
        else:
            pool = mp.Pool(nworkers, maxtasksperchild=4)
            chunksize = max(1, len(sfiles) // (nworkers * 4))
            iterator = pool.imap_unordered(merge_shard, [(sf, tf, filters) for sf, tf in zip(sfiles, tmps)], chunksize=chunksize)
        try:
            args = [(sf, tf, filters) for sf, tf in zip(sfiles, tmps)]
            for i, c in enumerate(iterator):
                total += c
                if (i + 1) % max(1, len(sfiles) // 10) == 0:
                    print(f"  {i+1}/{len(sfiles)} shards, {total} traces")
            if pool:
                pool.close()
                pool.join()
        except Exception:
            if pool:
                pool.terminate()
                pool.join()
            raise

        print(f"  Concatenating...")
        with open(output_file, 'wb') as outf:
            for tf in tmps:
                if os.path.exists(tf) and os.path.getsize(tf) > 0:
                    with open(tf, 'rb') as inf:
                        shutil.copyfileobj(inf, outf)
        print(f"  Done: {total} traces")
    finally:
        for tf in tmps:
            if os.path.exists(tf):
                os.remove(tf)

    return total


# =============================================================================

# ===== Legacy HTML analyzer (kept intact) =====
import gzip
import heapq
import json
import math
import os
import re
import sys
from array import array
from collections import defaultdict
from datetime import datetime

# \u5C1D\u8BD5\u4F7F\u7528 orjson \u52A0\u901F\u89E3\u6790\uFF08\u53EF\u9009\uFF09
try:
    import orjson

    def json_loads(s):
        if isinstance(s, str):
            s = s.encode('utf-8')
        return orjson.loads(s)
    def json_dumps(obj):
        return orjson.dumps(obj).decode('utf-8')

    HAS_ORJSON = True
except ImportError:

    def json_loads(s):
        return json.loads(s)

    def json_dumps(obj):
        return json.dumps(obj, ensure_ascii=False)

    HAS_ORJSON = False


def parse_timestamp(ts_str):
    """\u89E3\u6790 ISO \u683C\u5F0F\u65F6\u95F4\u6233"""
    if not ts_str:
        return None
    try:
        return datetime.fromisoformat(ts_str.replace('Z', '+00:00'))
    except Exception:
        # Fallback for older Python versions with limited fromisoformat support
        for fmt in ('%Y-%m-%dT%H:%M:%S', '%Y-%m-%d %H:%M:%S',
                     '%Y-%m-%dT%H:%M:%S.%f', '%Y-%m-%d %H:%M:%S.%f'):
            try:
                return datetime.strptime(ts_str[:26].rstrip('Z'), fmt)
            except ValueError:
                continue
        return None


def get_nested_value(data, path, default=0, keep_none=False):
    """\u5B89\u5168\u83B7\u53D6\u5D4C\u5957\u5B57\u5178\u503C"""
    keys = path.split('.')
    val = data
    for k in keys:
        if isinstance(val, dict) and k in val:
            val = val[k]
        else:
            return default
    if keep_none and val is None:
        return None
    return val if isinstance(val, (int, float)) else default


def compute_percentile(sorted_arr, p):
    """\u8BA1\u7B97\u767E\u5206\u4F4D\u6570\uFF08\u7EBF\u6027\u63D2\u503C\uFF09"""
    if not sorted_arr:
        return 0
    n = len(sorted_arr)
    if n == 1:
        return sorted_arr[0]
    idx = (n - 1) * p / 100.0
    lower = int(math.floor(idx))
    upper = int(math.ceil(idx))
    if lower == upper:
        return sorted_arr[lower]
    frac = idx - lower
    return sorted_arr[lower] * (1 - frac) + sorted_arr[upper] * frac


def format_us(val, field_name=None):
    """\u683C\u5F0F\u5316\u5FAE\u79D2\u503C\uFF0C\u975E\u65F6\u5EF6\u6307\u6807\u663E\u793A\u4E3A\u4E2A\u6570"""
    if field_name == 'URMA\u5E76\u53D1\u6570':
        return f"{val:.1f} \u4E2A"
    if val >= 1000:
        return f"{val/1000:.2f} ms"
    return f"{val:.0f} \u00B5s"


class KVCacheAnalyzer:
    # \u6240\u6709\u5206\u6BB5\u65F6\u5EF6\u5B57\u6BB5\u5B9A\u4E49\uFF08\u5B57\u6BB5\u8DEF\u5F84, \u663E\u793A\u540D\u79F0\uFF09
    # \u672C\u5730\u6A21\u5F0F\uFF1Asdk_rpc \u6709\u503C
    # \u8FDC\u7A0B\u6A21\u5F0F\uFF1Asdk_rpc \u65E0\u503C\uFF0Cclient_master_rpc / client_remote_rpc \u6709\u503C
    SEGMENT_FIELDS = [
        # \u5171\u7528\u5B57\u6BB5
        ("sdk_processing_us", "SDK\u5904\u7406"),
        ("query_get_local_rpc.network_us", "QueryGetLocal\u7f51\u7edc"),
        ("query_get_local_rpc.framework_us", "QueryGetLocal\u6846\u67b6"),
        ("query_get_remote_rpc.network_us", "QueryGetRemote\u7f51\u7edc"),
        ("query_get_remote_rpc.framework_us", "QueryGetRemote\u6846\u67b6"),
        ("query_get_server_exec_us", "QueryGet Worker\u6267\u884C"),
        ("master_processing_us", "Master\u5904\u7406"),
        ("worker_access_latency_us", "Worker Access\u65F6\u5EF6"),
        # \u672C\u5730\u6A21\u5F0F\u5B57\u6BB5
        ("remote_worker_internal_us", "Remote Worker\u5185\u90E8"),
        ("local_worker_internal_us", "Local Worker\u5185\u90E8"),
        ("local_worker_internal_active_us", "Local Worker\u5185\u90E8\u65F6\u95F42"),
        ("sdk_rpc.network_us", "SDK RPC\u7F51\u7EDC"),
        ("sdk_rpc.framework_us", "SDK RPC\u6846\u67B6"),
        ("sdk_rpc.total_us", "SDK RPC\u603B\u65F6\u5EF6"),
        ("master_rpc.network_us", "Master RPC\u7F51\u7EDC"),
        ("master_rpc.framework_us", "Master RPC\u6846\u67B6"),
        ("master_rpc.total_us", "Master RPC\u603B\u65F6\u5EF6"),
        ("remote_worker_rpc.network_us", "Remote Worker RPC\u7F51\u7EDC"),
        ("remote_worker_rpc.framework_us", "Remote Worker RPC\u6846\u67B6"),
        ("remote_worker_rpc.total_us", "Remote Worker RPC\u603B\u65F6\u5EF6"),
        ("urma_sched_us", "\u8C03\u5EA6\u65F6\u95F4"),
        ("client_urma_processing_us", "Client UB\u901A\u4FE1"),
        ("urma_processing_us", "Worker UB\u901A\u4FE1"),
        ("urma_inflight_max", "URMA\u5E76\u53D1\u6570"),
        # urma info \u72EC\u7ACB\u5B57\u6BB5
        ("urma_ub_jetty_post_send", "UB_JETTY_POST_SEND"),
        ("urma_bond_jetty_post_send", "BOND_JETTY_POST_SEND"),
        ("urma_ub_poll_jfc", "UB_POLL_JFC"),
        ("urma_bond_poll_jfc", "BOND_POLL_JFC"),
        # \u8FDC\u7A0B\u6A21\u5F0F\u5B57\u6BB5
        ("remote_worker_processing_us", "Remote Worker\u5904\u7406"),
        ("client_master_rpc.network_us", "Client Master RPC\u7F51\u7EDC"),
        ("client_master_rpc.framework_us", "Client Master RPC\u6846\u67B6"),
        ("client_master_rpc.total_us", "Client Master RPC\u603B\u65F6\u5EF6"),
        ("client_remote_rpc.network_us", "Client Remote RPC\u7F51\u7EDC"),
        ("client_remote_rpc.framework_us", "Client Remote RPC\u6846\u67B6"),
        ("client_remote_rpc.total_us", "Client Remote RPC\u603B\u65F6\u5EF6"),
    ]

    # \u8BE6\u7EC6\u5C55\u793A\u4E2D\u6392\u9664\u7684\u5B57\u6BB5\uFF08\u4EC5\u7528\u4E8E\u7EDF\u8BA1\uFF09
    HIDDEN_FIELDS = {
        'sdk_rpc.total_us',
        'master_rpc.total_us',
        'remote_worker_rpc.total_us',
        'client_master_rpc.total_us',
        'client_remote_rpc.total_us',
        'local_worker_internal_active_us',
    }

    NON_TIME_FIELDS = {'urma_inflight_max'}
    NON_TIME_FIELDS_LABELS = {'URMA\u5E76\u53D1\u6570'}

    # urma info 4\u4E2A\u72EC\u7ACB\u6307\u6807\u5B57\u6BB5\uFF08\u5185\u90E8\u5B57\u6BB5key, \u65E5\u5FD7\u6307\u6807\u540D\uFF09
    URMA_INFO_FIELDS = [
        ('urma_ub_jetty_post_send', 'UB_JETTY_POST_SEND'),
        ('urma_bond_jetty_post_send', 'BOND_JETTY_POST_SEND'),
        ('urma_ub_poll_jfc', 'UB_POLL_JFC'),
        ('urma_bond_poll_jfc', 'BOND_POLL_JFC'),
    ]
    # \u65E5\u5FD7\u4E2D\u6BCF\u4E2A\u6307\u6807\u643A\u5E26\u7684\u7EDF\u8BA1\u540E\u7F00\uFF1A{\u6307\u6807\u540D}_{\u540E\u7F00}
    URMA_INFO_STATS = ['avg', 'max', 'p99', 'p9999']
    # \u5404\u7EDF\u8BA1\u91CF\u7684\u5019\u9009\u65E5\u5FD7\u952E\u540E\u7F00\uFF08\u517C\u5BB9\u4E0D\u540C\u7248\u672C\u57CB\u70B9\u547D\u540D\uFF0C\u6309\u4F18\u5148\u7EA7\u53D6\u9996\u4E2A\u547D\u4E2D\uFF09
    # \u5982 avg \u53EF\u80FD\u5199\u4F5C UB_JETTY_POST_SEND_avg / _argv / _avg_us / _argv_us
    URMA_STAT_KEY_ALIASES = {
        'avg':   ['avg', 'argv', 'avg_us', 'argv_us'],
        'max':   ['max', 'pmax', 'max_us', 'pmax_us'],
        'p99':   ['p99', 'p99_us'],
        'p9999': ['p9999', 'p9999_us'],
    }

    # .info \u91C7\u6837\u5B57\u6BB5\uFF1A\u5468\u671F\u4E0A\u62A5\uFF08\u7EA6\u6BCF 10s \u4E00\u6761\uFF09\uFF0C\u7A97\u53E3\u7EC6\u4E8E\u4E0A\u62A5\u5468\u671F\u65F6\u5BF9\u7A7A\u7A97\u53E3\u505A\u524D\u503C\u586B\u5145
    INFO_SAMPLED_FIELDS = ['sleep_time_us'] + [fp for fp, _ in URMA_INFO_FIELDS]

    # \u52A8\u6001\u751F\u6210 DISPLAY_FIELDS
    DISPLAY_FIELDS = []
    for _fp, _name in SEGMENT_FIELDS:
        if _fp not in HIDDEN_FIELDS:
            DISPLAY_FIELDS.append((_fp, _name))

    # Fields reported by urma_perf are periodic runtime samples, not one sample
    # per request.  They must not be plotted alongside request-level latency.
    REQUEST_TREND_FIELDS = [
        (fp, name) for fp, name in SEGMENT_FIELDS
        if fp not in {'urma_ub_jetty_post_send', 'urma_bond_jetty_post_send',
                      'urma_ub_poll_jfc', 'urma_bond_poll_jfc'}
    ]
    REQUEST_TREND_FIELD_PATHS = {fp for fp, _ in REQUEST_TREND_FIELDS}

    def __init__(self, filepath, filters=None, top_n=200, window_ms=1000, collect_qps=True):
        self.filepath = filepath
        self.filters = filters or {}
        self.top_n = top_n
        self.window_ms = window_ms
        self.collect_qps = collect_qps
        self.sleep_threshold_us = self.filters.get('sleep_threshold_us', 250)

        self.total_count = 0
        self.filtered_count = 0
        self.error_count = 0
        self.local_mode_count = 0
        self.remote_mode_count = 0

        # \u6D3B\u8DC3\u7A97\u53E3\u539F\u59CB\u6570\u636E
        self.active_windows = defaultdict(lambda: defaultdict(list))
        self.active_windows_total = defaultdict(list)

        # \u7A97\u53E3\u7EDF\u8BA1\u7ED3\u679C
        self.window_stats = {}

        # Top N \u6700\u5C0F\u5806
        self.top_heap = []
        self._heap_counter = 0

        # \u5168\u5C40\u603B\u65F6\u5EF6\u5217\u8868
        self.all_latencies = array('d')

        # \u5168\u5C40\u5404\u5B57\u6BB5\u503C\u5217\u8868
        self.global_field_values = {fp: array('d') for fp, _ in self.SEGMENT_FIELDS}

        # QPS \u7EDF\u8BA1\uFF1A10ms\u7A97\u53E3 -> {request_type -> count}
        self.qps_windows = defaultdict(lambda: defaultdict(int)) if collect_qps else None

        # .info \u6587\u4EF6\u7EDF\u8BA1\uFF1A\u805A\u5408\u7A97\u53E3 -> {field -> [values]}
        self.info_sleep_values = defaultdict(list)  # sleep_wakeup records
        # urma_perf \u539F\u59CB\u8BB0\u5F55\uFF1A\u7A97\u53E3 -> [ {UB_JETTY_POST_SEND_avg: v, ..., BOND_POLL_JFC_p9999: v}, ... ]
        # \u6BCF\u6761\u8BB0\u5F55\u4FDD\u7559 4\u6307\u6807 \u00D7 4\u7EDF\u8BA1\u91CF\uFF08avg/max/p99/p9999\uFF09\u517116\u4E2A\u539F\u59CB\u503C
        self.info_urma_records = defaultdict(list)
        # Real URMA_PERF samples for the standalone runtime chart.  These are
        # intentionally separate from request aggregation windows.
        self.urma_perf_samples = []
        self.info_ub_count = defaultdict(int)         # \u6BCF\u4E2A\u7A97\u53E3 UB\u901A\u4FE1 \u7684 count

    def _window_key(self, dt):
        """\u6309\u805A\u5408\u7A97\u53E3\u5BF9\u9F50\u65F6\u95F4\u6233\uFF1A\u4EE5\u5F53\u65E5 0 \u70B9\u4E3A\u57FA\u51C6\uFF0C\u6309\u7A97\u53E3\u6BEB\u79D2\u6570\u5411\u4E0B\u53D6\u6574"""
        day_ms = ((dt.hour * 60 + dt.minute) * 60 + dt.second) * 1000 + dt.microsecond // 1000
        aligned_ms = day_ms // self.window_ms * self.window_ms
        aligned = datetime(
            dt.year, dt.month, dt.day,
            aligned_ms // 3600000, aligned_ms // 60000 % 60,
            aligned_ms // 1000 % 60, aligned_ms % 1000 * 1000
        )
        if self.window_ms < 1000:
            return aligned.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
        return aligned.strftime('%Y-%m-%d %H:%M:%S')

    def _window_label(self):
        if self.window_ms % 1000 == 0:
            return f'{self.window_ms // 1000}\u79D2'
        return f'{self.window_ms}\u6BEB\u79D2'

    def _record_request_segments(self, window_key, segments):
        """Use every request as the denominator for request-stage trends."""
        for field_path in self.REQUEST_TREND_FIELD_PATHS:
            value = segments.get(field_path)
            self.active_windows[window_key][field_path].append(
                value if value is not None and value > 0 else 0)

    @staticmethod
    def _detect_mode(segs):
        """\u68C0\u6D4B\u8BB0\u5F55\u6A21\u5F0F\uFF1Alocal\uFF08\u672C\u5730Worker\uFF09\u6216 remote\uFF08\u76F4\u8FDE\uFF09"""
        sdk_rpc_total = get_nested_value(segs, 'sdk_rpc.total_us', 0)
        if sdk_rpc_total and sdk_rpc_total > 0:
            return 'local'
        client_master_total = get_nested_value(segs, 'client_master_rpc.total_us', 0)
        if client_master_total and client_master_total > 0:
            return 'remote'
        return 'unknown'

    @staticmethod
    def _detect_path_role(segs, has_meta=False):
        """Return a user-facing path role; RPC shape alone is not locality."""
        if has_meta:
            return 'meta_worker'
        if get_nested_value(segs, 'client_master_rpc.total_us', 0) > 0:
            return 'client_direct'
        if get_nested_value(segs, 'sdk_rpc.total_us', 0) > 0:
            return 'worker_entry'
        return 'unknown'

    @staticmethod
    def _fallback_total_latency(segments):
        """Estimate total from mutually exclusive outer windows when needed."""
        sdk = max(0, segments.get('sdk_processing_us') or 0)
        worker = max(0, segments.get('worker_access_latency_us') or 0)
        if worker:
            return sdk + worker
        remote = max(0, segments.get('remote_worker_processing_us') or 0)
        master = max(0, segments.get('master_processing_us') or 0)
        return sdk + max(remote, master)

    @staticmethod
    def _build_exclusive_segments(segments, total_latency):
        """Build non-overlapping bars while retaining nested values in details."""
        # Accept both the flattened representation used by the analyzer and
        # nested RPC dictionaries emitted by older merged trace files.
        def segment_value(prefix, field):
            flat = segments.get(prefix + '.' + field)
            if flat is not None:
                return flat
            nested = segments.get(prefix)
            return nested.get(field) if isinstance(nested, dict) else None

        result = []
        query_prefix = ('query_get_local_rpc' if any((segment_value('query_get_local_rpc', f) or 0) > 0
                                                    for f in ('network_us', 'framework_us'))
                        else 'query_get_remote_rpc' if any((segment_value('query_get_remote_rpc', f) or 0) > 0
                                                          for f in ('network_us', 'framework_us')) else None)
        query_rpc = query_prefix is not None
        if query_rpc:
            query_parts = 0
            for field, name in (('network_us', 'QueryGet\u7f51\u7edc'),
                                 ('framework_us', 'QueryGet\u6846\u67b6')):
                value = max(0, segment_value(query_prefix, field) or 0)
                if value:
                    result.append({'name': name, 'field': 'query_get_' + field, 'value': value})
                    query_parts += value
            server_exec = max(0, segments.get('query_get_server_exec_us') or 0)
            urma = max(0, segments.get('urma_processing_us') or 0)
            worker_internal = max(0, server_exec - urma)
            if worker_internal:
                result.append({'name': 'QueryGet Worker\u5185\u90E8',
                               'field': 'query_get_worker_internal_us', 'value': worker_internal})
                query_parts += worker_internal
            if urma:
                result.append({'name': 'Worker UB\u901A\u4FE1', 'field': 'urma_processing_us', 'value': urma})
                query_parts += urma
            client_urma = max(0, segments.get('client_urma_processing_us') or 0)
            if client_urma:
                result.append({'name': 'Client UB\u901a\u4FE1', 'field': 'client_urma_processing_us', 'value': client_urma})
                query_parts += client_urma
            sdk = max(0, (total_latency or 0) - query_parts)
            if sdk:
                result.insert(0, {'name': 'SDK\u5904\u7406', 'field': 'sdk_processing_us', 'value': sdk})
            return result
        sdk = max(0, segments.get('sdk_processing_us') or 0)
        if sdk:
            result.append({'name': 'SDK处理', 'field': 'sdk_processing_us', 'value': sdk})
        outer = 0
        if not outer:
            outer = max(0, segments.get('worker_access_latency_us') or 0)
        if not outer:
            outer = max(0, segments.get('remote_worker_processing_us') or 0)
        urma = max(0, segments.get('urma_processing_us') or 0)
        residual = max(0, outer - urma)
        if residual:
            result.append({'name': 'Worker/Meta内部（扣除UB）',
                           'field': 'worker_access_residual_us', 'value': residual})
        if urma:
            result.append({'name': 'Worker UB通信', 'field': 'urma_processing_us', 'value': urma})
        client_urma = max(0, segments.get('client_urma_processing_us') or 0)
        if client_urma:
            result.append({'name': 'Client UB\u901a\u4FE1', 'field': 'client_urma_processing_us', 'value': client_urma})
        used = sum(item['value'] for item in result)
        residual = max(0, (total_latency or 0) - used)
        if residual:
            label = 'QueryGet客户端处理' if query_rpc else '其他'
            field = 'query_get_client_residual_us' if query_rpc else '__other__'
            result.append({'name': label, 'field': field, 'value': residual})
        return result

    def _extract_record(self, item):
        """\u4ECE\u539F\u59CB item \u63D0\u53D6\u5E76\u8FC7\u6EE4\uFF0C\u8FD4\u56DE\u7CBE\u7B80\u8BB0\u5F55\u6216 None"""
        self.total_count += 1

        trace_id = item.get('trace_id', '')
        request_type = item.get('request_type', '')
        ts_str = item.get('timestamp', '')
        status_code = item.get('status_code', 0)
        latency_us = item.get('latency_us', 0)
        error = item.get('error', '')

        dt = parse_timestamp(ts_str)
        if not dt:
            return None

        # \u8FC7\u6EE4\u6761\u4EF6
        req_types = self.filters.get('request_types', [])
        if req_types and request_type not in req_types:
            return None

        time_from = self.filters.get('time_from')
        time_to = self.filters.get('time_to')
        if time_from and dt < time_from:
            return None
        if time_to and dt > time_to:
            return None

        latency_threshold = self.filters.get('latency_threshold', 0)
        if latency_threshold > 0 and latency_us < latency_threshold:
            return None

        has_error_only = self.filters.get('has_error_only', False)
        if has_error_only and not error and status_code == 0:
            return None

        segs = item.get('latency_segmentation', {})

        # \u5206\u6BB5\u65F6\u5EF6\u9608\u503C\u8FC7\u6EE4\uFF08None \u89C6\u4E3A 0\uFF09
        segment_thresholds = self.filters.get('segment_thresholds', {})
        for field_path, threshold in segment_thresholds.items():
            val = get_nested_value(segs, field_path, 0)
            if (val or 0) < threshold:
                return None

        # \u68C0\u6D4B\u6A21\u5F0F
        mode = self._detect_mode(segs)
        if mode == 'local':
            self.local_mode_count += 1
        elif mode == 'remote':
            self.remote_mode_count += 1

        # \u63D0\u53D6\u6240\u6709\u5206\u6BB5\u65F6\u5EF6\uFF08None \u8868\u793A\u65E0\u503C\uFF09
        segments = {}
        for field_path, _ in self.SEGMENT_FIELDS:
            segments[field_path] = get_nested_value(segs, field_path, 0, keep_none=True)

        # Local Worker\u5185\u90E8\u65F6\u95F42\uFF1A\u4EC5\u5F53 master_rpc \u6216 remote_worker_rpc \u6709\u503C\u65F6\u751F\u6548
        if mode == 'local':
            master_rpc_total = get_nested_value(segs, 'master_rpc.total_us', 0)
            remote_rpc_total = get_nested_value(segs, 'remote_worker_rpc.total_us', 0)
            if master_rpc_total > 0 or remote_rpc_total > 0:
                segments['local_worker_internal_active_us'] = segments['local_worker_internal_us']
            else:
                segments['local_worker_internal_active_us'] = None
        else:
            segments['local_worker_internal_active_us'] = None

        # Worker Access \u65F6\u5EF6\uFF1A\u53D6 worker_access \u6570\u7EC4\u4E2D latency_us \u7684\u6700\u5927\u503C
        worker_access_list = item.get('worker_access', [])
        if worker_access_list and isinstance(worker_access_list, list):
            max_wa_lat = max(w.get('latency_us', 0) or 0 for w in worker_access_list)
            segments['worker_access_latency_us'] = max_wa_lat if max_wa_lat > 0 else None
        else:
            segments['worker_access_latency_us'] = None

        # URMA \u5E76\u53D1\u6570\uFF1A\u53D6 urma \u6570\u7EC4\u4E2D urma_inflight \u7684\u6700\u5927\u503C
        urma_list = item.get('urma', [])
        if urma_list and isinstance(urma_list, list):
            max_inflight = max(u.get('urma_inflight', 0) or 0 for u in urma_list)
            segments['urma_inflight_max'] = max_inflight if max_inflight > 0 else None
        else:
            segments['urma_inflight_max'] = None

        # \u8BA1\u7B97\u603B\u65F6\u5EF6
        computed_total = self._fallback_total_latency(segments)
        total_latency = latency_us if latency_us > 0 else computed_total

        if status_code != 0 or error:
            self.error_count += 1

        self.filtered_count += 1

        # \u805A\u5408\u7A97\u53E3\u5BF9\u9F50\uFF08\u9ED8\u8BA4 1 \u79D2\uFF09
        window_key = self._window_key(dt)

        return {
            'trace_id': trace_id,
            'request_type': request_type,
            'timestamp': dt,
            'window_key': window_key,
            'status_code': status_code,
            'error': error,
            'latency_us': total_latency,
            'mode': mode,
            'path_role': item.get('path_role') or self._detect_path_role(segs),
            'segments': segments,
            'data_size': item.get('data_size', 0),
            'client_ip': item.get('client_ip', ''),
        }

    def _load_info_file(self):
        """\u8BFB\u53D6\u540C\u540D\u7684 .info \u6587\u4EF6\uFF0C\u89E3\u6790 sleep_wakeup \u548C urma_perf \u6570\u636E"""
        # \u5019\u9009\u8DEF\u5F84\uFF1Ainput.json.info / input.info\uFF08\u4EFB\u610F\u6269\u5C55\u540D\u5747\u53EF\uFF0C\u5982 .log/.txt\uFF09\uFF1B
        # \u7EDD\u4E0D\u63A5\u53D7\u8F93\u5165\u6587\u4EF6\u672C\u8EAB\uFF0C\u907F\u514D\u628A\u6570\u636E\u6587\u4EF6\u8BEF\u5F53 .info \u89E3\u6790
        candidates = [self.filepath + '.info']
        root = os.path.splitext(self.filepath)[0]
        candidates.append(root + '.info')
        if self.filepath.endswith('.gz'):
            candidates.append(os.path.splitext(root)[0] + '.info')
        info_path = next((p for p in candidates if p != self.filepath and os.path.exists(p)), None)
        if not info_path:
            print(f"  \u672A\u627E\u5230 .info \u6587\u4EF6\uFF08\u5DF2\u5C1D\u8BD5: {', '.join(candidates)}\uFF09")
            return

        print(f"  \u8BFB\u53D6 .info \u6587\u4EF6: {info_path}")
        line_count = 0
        urma_perf_seen = 0          # \u89C1\u5230\u7684 urma_perf \u884C\u6570\uFF08\u65E0\u8BBA\u662F\u5426\u89E3\u6790\u51FA\u5B57\u6BB5\uFF09
        urma_perf_sample_keys = []  # \u9996\u6761 urma_perf \u7684\u5B57\u6BB5\u540D\uFF08\u8BCA\u65AD\u7528\uFF09
        urma_metrics = [m for _, m in self.URMA_INFO_FIELDS]
        stat_types = self.URMA_INFO_STATS  # ['avg', 'max', 'p99', 'p9999']

        with open(info_path, 'rt', encoding='utf-8', errors='ignore') as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                line_count += 1
                # \u884C\u9996\u4E3A\u65F6\u95F4\u6233\uFF0C\u5176\u4F59\u4E3A key:value \u5B57\u6BB5
                # \u517C\u5BB9 Tab/\u7A7A\u683C\u5206\u9694\u3001\u5192\u53F7\u540E\u5E26\u7A7A\u683C\uFF08key: value\uFF09\u7B49\u53D8\u4F53
                m = re.match(r'^(\S+)\s+(.*)$', line)
                if not m:
                    continue
                dt = parse_timestamp(m.group(1))
                if not dt:
                    continue
                fields = dict(re.findall(r'(\w+)\s*:\s*(\S*)', m.group(2)))

                rec_type = fields.get('type', '')
                wk = self._window_key(dt)

                if rec_type == 'sleep_wakeup':
                    try:
                        cost = float(fields.get('nanosleep_cost_us', 0) or 0)
                    except (ValueError, TypeError):
                        continue
                    if cost > self.sleep_threshold_us:
                        self.info_sleep_values[wk].append(cost)
                elif rec_type == 'urma_perf':
                    urma_perf_seen += 1
                    if not urma_perf_sample_keys:
                        urma_perf_sample_keys = sorted(k for k in fields if k not in ('type', 'retry_count'))
                    # \u63D0\u53D6\u683C\u5F0F\uFF1A{\u6307\u6807}_{\u7EDF\u8BA1\u540E\u7F00}\uFF0C\u540E\u7F00\u517C\u5BB9 avg/argv\u3001max/pmax \u53CA _us \u53D8\u4F53
                    urma_rec = {}
                    has_value = False
                    for metric in urma_metrics:
                        for st in stat_types:
                            val = 0.0
                            for suffix in self.URMA_STAT_KEY_ALIASES[st]:
                                raw = fields.get(f"{metric}_{suffix}")
                                if raw is not None and raw != '':
                                    try:
                                        val = float(raw)
                                    except (ValueError, TypeError):
                                        val = 0.0
                                    break
                            urma_rec[f"{metric}_{st}"] = val
                            if val > 0:
                                has_value = True
                    if has_value:
                        self.info_urma_records[wk].append(urma_rec)
                        self.urma_perf_samples.append({
                            'timestamp': dt.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3],
                            'metrics': urma_rec,
                        })

        # \u8BA1\u7B97\u6BCF\u4E2A\u7A97\u53E3\u7684 poll \u64CD\u4F5C\u57FA\u6570\uFF08\u7528\u4E8E sleep P99 \u57FA\u6570\uFF09
        # \u4F18\u5148\u4F7F\u7528 URMA count\uFF08\u6BCF URMA \u5BF9\u5E94 2 \u6B21 poll\uFF09\uFF0C\u7F3A\u5931\u65F6\u7528\u8BF7\u6C42\u603B\u6570\u4F5C\u4E3A\u8FD1\u4F3C
        for wk in self.active_windows:
            ub_count = len(self.active_windows[wk].get('urma_processing_us', []))
            if ub_count == 0:
                ub_count = len(self.active_windows_total.get(wk, []))
            self.info_ub_count[wk] = ub_count

        total_urma_records = sum(len(v) for v in self.info_urma_records.values())
        print(f"  .info \u6587\u4EF6\u89E3\u6790\u5B8C\u6210: {line_count} \u884C, sleep\u8BB0\u5F55 {sum(len(v) for v in self.info_sleep_values.values())} \u6761, urma_perf\u8BB0\u5F55 {total_urma_records} \u6761")
        if urma_perf_seen > 0 and total_urma_records == 0:
            print(f"  \u26A0\uFE0F \u68C0\u6D4B\u5230 {urma_perf_seen} \u6761 urma_perf \u8BB0\u5F55\u4F46\u672A\u89E3\u6790\u5230\u6709\u6548\u5B57\u6BB5\uFF0C"
                  f"\u5B57\u6BB5\u547D\u540D\u53EF\u80FD\u4E0D\u5728\u517C\u5BB9\u5217\u8868\u5185\u3002\u9996\u6761\u8BB0\u5F55\u5B57\u6BB5\u540D: {urma_perf_sample_keys}")

        # \u8BCA\u65AD\uFF1Aurma_perf \u6240\u5728\u7A97\u53E3\u4E0E\u8BF7\u6C42\u65E5\u5FD7\u7A97\u53E3\u7684\u4EA4\u96C6\uFF08\u65E0\u4EA4\u96C6\u65F6\u8D8B\u52BF\u56FE urma \u5B57\u6BB5\u5168\u4E3A0\uFF09
        if self.info_urma_records:
            req_wks = set(self.active_windows.keys())
            urma_wks = set(self.info_urma_records.keys())
            inter = urma_wks & req_wks
            print(f"  urma_perf \u8986\u76D6 {len(urma_wks)} \u4E2A{self._window_label()}\u7A97\u53E3\uFF0C\u4E0E\u8BF7\u6C42\u7A97\u53E3\u4EA4\u96C6 {len(inter)} \u4E2A")
            if req_wks and not inter:
                print(f"  \u26A0\uFE0F urma_perf \u65F6\u95F4\u8303\u56F4\uFF08{min(urma_wks)} ~ {max(urma_wks)}\uFF09\u4E0E\u8BF7\u6C42\u65E5\u5FD7"
                      f"\uFF08{min(req_wks)} ~ {max(req_wks)}\uFF09\u65E0\u7A97\u53E3\u4EA4\u96C6\uFF0C\u8D8B\u52BF\u56FE urma \u5B57\u6BB5\u5C06\u663E\u793A\u4E3A0\uFF0C"
                      f"\u8BF7\u68C0\u67E5\u4E24\u4FA7\u65F6\u949F/\u65F6\u533A\u662F\u5426\u4E00\u81F4")

    def _finalize_window(self, wk, window_values, window_total_latencies):
        """\u8BA1\u7B97\u5E76\u4FDD\u5B58\u5355\u4E2A\u7A97\u53E3\u7684\u7EDF\u8BA1\u7ED3\u679C"""
        stats = {}
        for fp, _ in self.SEGMENT_FIELDS:
            vals = window_values.get(fp, [])
            if vals:
                vals_sorted = sorted(vals)
                stats[fp] = {
                    'avg': round(sum(vals_sorted) / len(vals_sorted), 2),
                    'p99': round(compute_percentile(vals_sorted, 99), 2),
                    'p9999': round(compute_percentile(vals_sorted, 99.99), 2),
                    'pmax': round(max(vals_sorted), 2),
                    'count': len(vals_sorted),
                    'coverage': sum(1 for value in vals_sorted if value > 0)
                }
        if window_total_latencies:
            vals_sorted = sorted(window_total_latencies)
            stats['__total_latency__'] = {
                'avg': round(sum(vals_sorted) / len(vals_sorted), 2),
                'p99': round(compute_percentile(vals_sorted, 99), 2),
                'p9999': round(compute_percentile(vals_sorted, 99.99), 2),
                'pmax': round(max(vals_sorted), 2),
                'count': len(vals_sorted)
            }

        # \u6DFB\u52A0 .info \u6587\u4EF6\u7EDF\u8BA1
        # sleep \u65F6\u95F4\uFF1A\u4EE5 2 * UB\u901A\u4FE1_count \u4E3A\u57FA\u6570\u8BA1\u7B97 P99
        sleep_vals = self.info_sleep_values.get(wk, [])
        ub_count = self.info_ub_count.get(wk, 0)
        sleep_base = ub_count * 2
        if sleep_base > 0:
            # \u6784\u5EFA\u5B8C\u6574\u6570\u7EC4\uFF1A\u5B9E\u9645\u503C + \u88650
            full_sleep = sorted(sleep_vals + [0] * (sleep_base - len(sleep_vals)))
            stats['sleep_time_us'] = {
                'avg': round(sum(full_sleep) / sleep_base, 2),
                'p99': round(compute_percentile(full_sleep, 99), 2),
                'p9999': round(compute_percentile(full_sleep, 99.99), 2),
                'pmax': round(max(full_sleep) if full_sleep else 0, 2),
                'count': len(sleep_vals),  # \u663E\u793A\u771F\u5B9E\u6837\u672C\u6570
                'base_count': sleep_base   # \u57FA\u6570\u7528\u4E8EP99\u8BA1\u7B97
            }

        # urma info 4\u4E2A\u72EC\u7ACB\u5B57\u6BB5\uFF08\u8D8B\u52BF\u56FE\u7528\uFF09\uFF1A
        # avg/p99/p9999/pmax \u5206\u522B\u53D6\u7A97\u53E3\u5185\u5404\u8BB0\u5F55\u5BF9\u5E94\u7EDF\u8BA1\u91CF\u7684\u6700\u5927\u503C
        urma_records = self.info_urma_records.get(wk, [])
        for field_key, metric in self.URMA_INFO_FIELDS:
            if urma_records:
                stats[field_key] = {
                    'avg': round(max(r[f'{metric}_avg'] for r in urma_records), 2),
                    'p99': round(max(r[f'{metric}_p99'] for r in urma_records), 2),
                    'p9999': round(max(r[f'{metric}_p9999'] for r in urma_records), 2),
                    'pmax': round(max(r[f'{metric}_max'] for r in urma_records), 2),
                    'count': len(urma_records)
                }

        self.window_stats[wk] = stats

    def _forward_fill_info_stats(self):
        """.info \u91C7\u6837\u5B57\u6BB5\uFF08sleep/urma\uFF09\u4E3A\u7A7A\u7A97\u53E3\u65F6\u6CBF\u7528\u6700\u8FD1\u4E00\u6B21\u975E\u7A7A\u7EDF\u8BA1"""
        last = {}
        for wk in sorted(self.window_stats.keys()):
            stats = self.window_stats[wk]
            for fp in self.INFO_SAMPLED_FIELDS:
                if fp in stats:
                    last[fp] = stats[fp]
                elif fp in last:
                    stats[fp] = last[fp]

    @staticmethod
    def _calc_urma_time_stats(urma_records):
        """urma \u65F6\u95F4\u7EDF\u8BA1\uFF1A\u5148\u6309\u8BB0\u5F55\u6C42 BOND_JETTY_POST_SEND_x + BOND_POLL_JFC_x \u4E4B\u548C\uFF0C
        \u518D\u5BF9\u548C\u503C\u5E8F\u5217\u6C42 avg / p99 / p9999 / pmax"""
        sum_avg = sorted(r['BOND_JETTY_POST_SEND_avg'] + r['BOND_POLL_JFC_avg'] for r in urma_records)
        sum_p99 = sorted(r['BOND_JETTY_POST_SEND_p99'] + r['BOND_POLL_JFC_p99'] for r in urma_records)
        sum_p9999 = sorted(r['BOND_JETTY_POST_SEND_p9999'] + r['BOND_POLL_JFC_p9999'] for r in urma_records)
        sum_max = sorted(r['BOND_JETTY_POST_SEND_max'] + r['BOND_POLL_JFC_max'] for r in urma_records)
        return {
            'avg': round(sum(sum_avg) / len(sum_avg), 2),
            'p99': round(compute_percentile(sum_p99, 99), 2),
            'p9999': round(compute_percentile(sum_p9999, 99.99), 2),
            'pmax': round(max(sum_max), 2),
            'count': len(urma_records)
        }

    def _build_urma_perf_chart_data(self):
        """Return real URMA_PERF samples without request-window fill or interpolation."""
        rows = []
        for sample in sorted(self.urma_perf_samples, key=lambda item: item['timestamp']):
            row = {'timestamp': sample['timestamp']}
            metrics = sample['metrics']
            for field_key, metric in self.URMA_INFO_FIELDS:
                row[field_key] = {
                    'avg': metrics.get(f'{metric}_avg', 0),
                    'p99': metrics.get(f'{metric}_p99', 0),
                    'p9999': metrics.get(f'{metric}_p9999', 0),
                    'pmax': metrics.get(f'{metric}_max', 0),
                }
            rows.append(row)
        return rows

    def _update_top_heap(self, rec):
        """\u7EF4\u62A4 Top N \u6700\u5C0F\u5806"""
        self._heap_counter += 1
        entry = (rec['latency_us'], self._heap_counter, rec)
        if len(self.top_heap) < self.top_n:
            heapq.heappush(self.top_heap, entry)
        elif rec['latency_us'] > self.top_heap[0][0]:
            heapq.heapreplace(self.top_heap, entry)

    def load_data(self):
        """\u6D41\u5F0F\u52A0\u8F7D\u6570\u636E"""
        open_fn = gzip.open if self.filepath.endswith('.gz') else open
        file_size = os.path.getsize(self.filepath)

        progress_interval = 100000

        with open_fn(self.filepath, 'rt', encoding='utf-8', errors='ignore') as f:
            first_line = f.readline().strip()
            f.seek(0)

            if first_line.startswith('['):
                if file_size > 100 * 1024 * 1024:
                    print("\u8B66\u544A\uFF1AJSON \u6570\u7EC4\u683C\u5F0F\u7684\u5927\u6587\u4EF6\u4F1A\u5360\u7528\u5927\u91CF\u5185\u5B58\uFF0C\u5EFA\u8BAE\u8F6C\u6362\u4E3A JSON Lines")
                data = json.load(f)
                iterator = enumerate(data, 1)
            else:
                iterator = enumerate(f, 1)

            for line_no, line in iterator:
                if not first_line.startswith('['):
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        item = json_loads(line)
                    except Exception:
                        continue
                else:
                    item = line

                rec = self._extract_record(item)
                if rec is None:
                    continue

                wk = rec['window_key']

                # Request-stage trends use the full request count as denominator.
                self._record_request_segments(wk, rec['segments'])
                # Global summaries retain only stage-present samples.
                for fp, _ in self.SEGMENT_FIELDS:
                    val = rec['segments'][fp]
                    if val is not None and val > 0:
                        self.global_field_values[fp].append(val)

                self.active_windows_total[wk].append(rec['latency_us'])
                self.all_latencies.append(rec['latency_us'])

                # QPS \u7EDF\u8BA1\uFF1A10ms\u7A97\u53E3\uFF08\u5173\u95ED\u65F6\u8FDE\u805A\u5408\u5B57\u5178\u90FD\u4E0D\u521B\u5EFA\uFF09
                if self.collect_qps:
                    qps_ts = datetime(
                        rec['timestamp'].year, rec['timestamp'].month, rec['timestamp'].day,
                        rec['timestamp'].hour, rec['timestamp'].minute, rec['timestamp'].second,
                        (rec['timestamp'].microsecond // 10000) * 10000  # \u5BF9\u9F50\u523010ms
                    )
                    qps_key = qps_ts.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]  # \u4FDD\u7559\u5230\u6BEB\u79D2
                    self.qps_windows[qps_key][rec['request_type']] += 1

                # Top N \u5806
                self._update_top_heap(rec)

                if line_no % progress_interval == 0:
                    mem_mb = self._get_memory_mb()
                    active_windows = len(self.active_windows)
                    print(f"  \u5DF2\u5904\u7406 {line_no:,} \u884C | \u8FC7\u6EE4\u540E {self.filtered_count:,} \u6761 | "
                          f"\u672C\u5730\u6A21\u5F0F {self.local_mode_count} | \u8FDC\u7A0B\u6A21\u5F0F {self.remote_mode_count} | "
                          f"\u6D3B\u8DC3\u7A97\u53E3 {active_windows} | \u5185\u5B58 {mem_mb:.1f} MB")

            # \u8BFB\u53D6 .info \u6587\u4EF6
            self._load_info_file()

            print(f"  \u5904\u7406\u5B8C\u6210\uFF0C\u5171 {len(self.active_windows)} \u4E2A\u7A97\u53E3\uFF0C\u5F00\u59CB\u805A\u5408\u7EDF\u8BA1...")
            for wk in self.active_windows:
                self._finalize_window(wk, self.active_windows[wk], self.active_windows_total[wk])

            self._forward_fill_info_stats()

    @staticmethod
    def _get_memory_mb():
        try:
            import psutil
            import os
            process = psutil.Process(os.getpid())
            return process.memory_info().rss / 1024 / 1024
        except Exception:
            return 0.0

    def get_top_slow(self):
        return [rec for _, _, rec in sorted(self.top_heap, key=lambda x: x[0], reverse=True)]

    def generate_html(self, output_path):
        self.load_data()

        if not self.window_stats:
            print("\u8B66\u544A\uFF1A\u8FC7\u6EE4\u540E\u6CA1\u6709\u6570\u636E\u8BB0\u5F55")
            return

        time_windows = sorted(self.window_stats.keys())
        agg_json = self.window_stats
        urma_perf_chart_data = self._build_urma_perf_chart_data()

        # Top Slow \u6570\u636E
        top_slow_by_latency = self.get_top_slow()
        top_slow_by_time = sorted(top_slow_by_latency, key=lambda r: r['timestamp'])

        # \u56FE\u8868\u6570\u636E\uFF1A\u6309\u65F6\u95F4\u5148\u540E\u987A\u5E8F
        top_slow_chart_data = []
        for rec in top_slow_by_time:
            seg_list = self._build_exclusive_segments(rec['segments'], rec['latency_us'])
            top_slow_chart_data.append({
                'trace_id': rec['trace_id'], 'request_type': rec['request_type'],
                'timestamp': rec['timestamp'].strftime('%Y-%m-%d %H:%M:%S.%f')[:-3],
                'latency_us': rec['latency_us'], 'status_code': rec['status_code'],
                'error': rec['error'], 'client_ip': rec['client_ip'],
                'data_size': rec['data_size'], 'segments': seg_list, 'mode': rec['mode'], 'path_role': rec['path_role']
            })

        # \u8868\u683C\u6570\u636E\uFF1A\u6309\u65F6\u5EF6\u6392\u5E8F
        top_slow_table_data = []
        for rec in top_slow_by_latency:
            seg_list = self._build_exclusive_segments(rec['segments'], rec['latency_us'])
            # \u6DFB\u52A0\u975E\u65F6\u5EF6\u6307\u6807
            for fp, name in self.SEGMENT_FIELDS:
                if fp in self.NON_TIME_FIELDS:
                    val = rec['segments'].get(fp)
                    if val is not None:
                        seg_list.append({'name': name, 'field': fp, 'value': val})
            top_slow_table_data.append({
                'trace_id': rec['trace_id'], 'request_type': rec['request_type'],
                'timestamp': rec['timestamp'].strftime('%Y-%m-%d %H:%M:%S.%f')[:-3],
                'latency_us': rec['latency_us'], 'status_code': rec['status_code'],
                'error': rec['error'], 'client_ip': rec['client_ip'],
                'data_size': rec['data_size'], 'segments': seg_list, 'mode': rec['mode'], 'path_role': rec['path_role']
            })

        # QPS \u6570\u636E\u805A\u5408\uFF1A\u5173\u95ED\u65F6\u4E0D\u904D\u5386\u3001\u4E0D\u5E8F\u5217\u5316\u3001\u4E0D\u5199\u5165 HTML
        qps_time_keys, qps_request_types, qps_data = [], [], {}
        if self.collect_qps:
            qps_time_keys = sorted(self.qps_windows.keys())
            qps_request_types = sorted(set(rt for w in self.qps_windows.values() for rt in w.keys()))
            for tk in qps_time_keys:
                qps_data[tk] = {}
                for rt in qps_request_types:
                    count = self.qps_windows[tk].get(rt, 0)
                    qps_data[tk][rt] = count * 100  # 10ms\u7A97\u53E3\u5185\u8BF7\u6C42\u6570 \u00D7 100 = QPS\uFF08\u6BCF\u79D2\u8BF7\u6C42\u6570\uFF09

        # \u5168\u5C40\u7EDF\u8BA1
        all_lat = sorted(self.all_latencies)
        summary = {
            'total': self.total_count, 'filtered': self.filtered_count,
            'errors': self.error_count, 'local_mode': self.local_mode_count,
            'remote_mode': self.remote_mode_count,
            'avg_latency': round(sum(all_lat) / len(all_lat), 2) if all_lat else 0,
            'p99_latency': round(compute_percentile(all_lat, 99), 2) if all_lat else 0,
            'p9999_latency': round(compute_percentile(all_lat, 99.99), 2) if all_lat else 0,
            'max_latency': round(max(all_lat), 2) if all_lat else 0,
            'time_range': f"{time_windows[0]} ~ {time_windows[-1]}" if time_windows else "N/A"
        }

        # \u5168\u9636\u6BB5\u5168\u5C40\u7EDF\u8BA1
        global_stats = []
        total_avg = summary['avg_latency']
        global_stats.append({
            'field': '__e2e_total__', 'name': '\u7AEF\u5230\u7AEF\u603B\u65F6\u5EF6',
            'avg': round(sum(all_lat) / len(all_lat), 2) if all_lat else 0,
            'p99': round(compute_percentile(all_lat, 99), 2) if all_lat else 0,
            'p9999': round(compute_percentile(all_lat, 99.99), 2) if all_lat else 0,
            'pmax': round(max(all_lat), 2) if all_lat else 0,
            'ratio': 100.0,
            'count': len(all_lat)
        })

        # .info \u6587\u4EF6\u5168\u5C40\u7EDF\u8BA1\uFF1Asleep \u65F6\u95F4
        all_sleep = []
        for wk in self.info_sleep_values:
            ub_count = self.info_ub_count.get(wk, 0)
            if ub_count == 0:
                ub_count = len(self.active_windows_total.get(wk, []))
            sleep_base = ub_count * 2
            if sleep_base > 0:
                sleep_vals = self.info_sleep_values[wk]
                full_sleep = sorted(sleep_vals + [0] * (sleep_base - len(sleep_vals)))
                all_sleep.extend(full_sleep)
        if all_sleep:
            all_sleep_sorted = sorted(all_sleep)
            # \u8BA1\u7B97\u771F\u5B9E\u6837\u672C\u6570\uFF08\u975E0\u503C\uFF09
            actual_count = sum(1 for v in all_sleep_sorted if v > 0)
            global_stats.append({
                'field': 'sleep_time_us', 'name': 'sleep\u65F6\u95F4',
                'avg': round(sum(all_sleep_sorted) / len(all_sleep_sorted), 2),
                'p99': round(compute_percentile(all_sleep_sorted, 99), 2),
                'p9999': round(compute_percentile(all_sleep_sorted, 99.99), 2),
                'pmax': round(max(all_sleep_sorted), 2),
                'ratio': 0.0,
                'count': actual_count  # \u663E\u793A\u771F\u5B9E\u6837\u672C\u6570\uFF08>250\u00B5s\u7684\u8BB0\u5F55\u6570\uFF09
            })

        # .info \u6587\u4EF6\u5168\u5C40\u7EDF\u8BA1\uFF1Aurma \u65F6\u95F4
        # \u8BA1\u7B97\u65B9\u5F0F\uFF1A\u6BCF\u6761 urma_perf \u8BB0\u5F55\u5148\u6C42 BOND_JETTY_POST_SEND_x + BOND_POLL_JFC_x \u4E4B\u548C\uFF0C
        # \u518D\u5BF9\u548C\u503C\u5E8F\u5217\u5206\u522B\u6C42 avg / p99 / p9999 / pmax
        all_urma_records = [r for wk in self.info_urma_records for r in self.info_urma_records[wk]]
        if all_urma_records:
            urma_time = self._calc_urma_time_stats(all_urma_records)
            global_stats.append({
                'field': 'urma_info_us', 'name': 'URMA\u8FD0\u884C\u65F6\u91C7\u6837\uFF08\u975E\u8BF7\u6C42\u65F6\u5EF6\uFF09',
                'avg': urma_time['avg'], 'p99': urma_time['p99'],
                'p9999': urma_time['p9999'], 'pmax': urma_time['pmax'],
                'ratio': 0.0, 'count': urma_time['count']
            })

        for fp, name in self.SEGMENT_FIELDS:
            if fp in {field for field, _ in self.URMA_INFO_FIELDS}:
                continue
            vals = sorted(self.global_field_values[fp])
            if not vals:
                continue
            avg = round(sum(vals) / len(vals), 2)
            p99 = round(compute_percentile(vals, 99), 2)
            p9999 = round(compute_percentile(vals, 99.99), 2)
            pmax = round(max(vals), 2)
            ratio = round(avg / total_avg * 100, 1) if total_avg > 0 else 0
            global_stats.append({
                'field': fp, 'name': name, 'avg': avg, 'p99': p99,
                'p9999': p9999, 'pmax': pmax, 'ratio': ratio,
                'count': len(vals)
            })

        field_names = {fp: name for fp, name in self.SEGMENT_FIELDS}

        # \u751F\u6210\u8868\u683C HTML
        overview_rows = []
        for gs in global_stats:
            is_non_time = gs['name'] in self.NON_TIME_FIELDS_LABELS
            overview_rows.append(
                '<tr>' +
                '<td><strong>' + gs['name'] + '</strong></td>' +
                '<td>' + format_us(gs['avg'], gs['name'] if is_non_time else None) + '</td>' +
                '<td>' + format_us(gs['p99'], gs['name'] if is_non_time else None) + '</td>' +
                '<td>' + format_us(gs['p9999'], gs['name'] if is_non_time else None) + '</td>' +
                '<td class="latency">' + format_us(gs['pmax'], gs['name'] if is_non_time else None) + '</td>' +
                '<td>' + str(gs['ratio']) + '%</td>' +
                '<td>' + str(gs.get('count', 0)) + '</td>' +
                '</tr>'
            )

        # HTML \u6A21\u677F
        html_template = self._html_template()

        top_rows = []
        for index, rec in enumerate(top_slow_by_latency, 1):
            segment_text = ', '.join(
                f"{seg['name']}:{format_us(seg['value'], seg['name'])}"
                for seg in top_slow_table_data[index - 1]['segments'] if seg['value'] > 0
            ) or '-'
            status = '\u2713 OK' if rec['status_code'] == 0 and not rec['error'] else f"\u2717 {rec['status_code']}"
            mode = {'local_worker': 'local worker', 'remote_worker': 'remote worker',
                    'local_meta_worker': 'localMetaWorker', 'remote_meta_worker': 'remoteMetaWorker',
                    'unknown': '\u672A\u77E5'}.get(rec.get('path_role'), '\u672A\u77E5')
            top_rows.append(
                '<tr><td><strong>#' + str(index) + '</strong></td><td class="trace-id">' +
                rec['trace_id'] + '</td><td><span class="mode-badge mode-' + rec['mode'] + '">' + mode +
                '</span></td><td>' + rec['request_type'] + '</td><td>' +
                rec['timestamp'].strftime('%Y-%m-%d %H:%M:%S.%f')[:-3] + '</td><td class="latency">' +
                format_us(rec['latency_us']) + '</td><td>' + status + '</td><td>' +
                (rec['client_ip'] or '-') + '</td><td>' +
                (f"{rec['data_size'] / 1024 / 1024:.2f} MB" if rec['data_size'] else '-') +
                '</td><td>' + segment_text + '</td><td class="error-text">' + (rec['error'] or '-') + '</td></tr>'
            )

        replacements = {
            '{{TITLE}}': 'KVCache \u6027\u80FD\u5206\u6790\u62A5\u544A',
            '{{DATETIME}}': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            '{{FILENAME}}': os.path.basename(self.filepath),
            '{{REQ_TYPES}}': str(self.filters.get('request_types') or '\u5168\u90E8'),
            '{{TIME_FROM}}': (self.filters.get('time_from').strftime('%Y-%m-%d %H:%M:%S') if self.filters.get('time_from') else '\u4E0D\u9650'),
            '{{TIME_TO}}': (self.filters.get('time_to').strftime('%Y-%m-%d %H:%M:%S') if self.filters.get('time_to') else '\u4E0D\u9650'),
            '{{LATENCY_TH}}': str(self.filters.get('latency_threshold', '\u65E0')),
            '{{SEGMENT_TH}}': (', '.join([f"{k}\u2265{v}\u00B5s" for k, v in self.filters.get('segment_thresholds', {}).items()]) if self.filters.get('segment_thresholds') else '\u65E0'),
            '{{ERROR_ONLY}}': str(self.filters.get('has_error_only', False)),
            '{{TOTAL}}': f"{summary['total']:,}",
            '{{FILTERED}}': f"{summary['filtered']:,}",
            '{{ERRORS}}': f"{summary['errors']:,}",
            '{{LOCAL_MODE}}': f"{summary['local_mode']:,}",
            '{{REMOTE_MODE}}': f"{summary['remote_mode']:,}",
            '{{ERR_PCT}}': f"{summary['errors'] / summary['filtered'] * 100 if summary['filtered'] > 0 else 0:.1f}",
            '{{AVG_LAT}}': format_us(summary['avg_latency']),
            '{{P99_LAT}}': format_us(summary['p99_latency']),
            '{{P9999_LAT}}': format_us(summary['p9999_latency']),
            '{{MAX_LAT}}': format_us(summary['max_latency']),
            '{{TIME_RANGE}}': summary['time_range'],
            '{{TOP_N}}': str(self.top_n),
            '{{WINDOW_LABEL}}': self._window_label(),
            '{{AGG_DATA}}': json_dumps(agg_json),
            '{{URMA_PERF_CHART_DATA}}': json_dumps(urma_perf_chart_data),
            '{{URMA_PERF_SECTION}}': self._urma_perf_section() if urma_perf_chart_data else '',
            '{{URMA_PERF_SCRIPT}}': self._urma_perf_script() if urma_perf_chart_data else '',
            '{{TOP_SLOW_CHART_DATA}}': json_dumps(top_slow_chart_data),
            '{{TOP_SLOW_TABLE_DATA}}': json_dumps(top_slow_table_data),
            '{{FIELD_NAMES}}': json_dumps(field_names),
            '{{FIELD_PATHS}}': json_dumps([fp for fp, _ in self.SEGMENT_FIELDS]),
            '{{DISPLAY_FIELDS}}': json_dumps(self.DISPLAY_FIELDS),
            '{{REQUEST_TREND_FIELDS}}': json_dumps(self.REQUEST_TREND_FIELDS),
            '{{COLORS}}': json_dumps([
                '#5470c6', '#91cc75', '#fac858', '#ee6666', '#73c0de',
                '#3ba272', '#fc8452', '#9a60b4', '#ea7ccc', '#ff9f7f', '#ffdb5c',
                '#c23531', '#2f4554', '#61a0a8', '#bda29a', '#6e7074',
                '#749f83', '#ca8622', '#bda29a', '#6e7074', '#546570',
                '#c4ccd3', '#f05b72', '#d53a35', '#f4e001', '#2b821d'
            ]),
            '{{SEGMENT_FIELDS}}': json_dumps(self.SEGMENT_FIELDS),
            '{{QPS_SECTION}}': self._qps_section() if self.collect_qps else '',
            '{{QPS_SCRIPT}}': self._qps_script() if self.collect_qps else '',
            '{{QPS_DATA}}': json_dumps(qps_data),
            '{{QPS_REQUEST_TYPES}}': json_dumps(qps_request_types),
            '{{QPS_TIME_KEYS}}': json_dumps(qps_time_keys),
            '{{OVERVIEW_ROWS}}': '\n'.join(overview_rows),
            '{{TOP_TABLE_ROWS}}': '\n'.join(top_rows) or '<tr><td colspan="11">\u6CA1\u6709\u5339\u914D\u7684\u8BF7\u6C42\u8BB0\u5F55</td></tr>',
        }

        html = html_template
        for key, val in replacements.items():
            html = html.replace(key, val)

        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(html)

        mem_mb = self._get_memory_mb()
        print(f"\u2705 \u62A5\u544A\u5DF2\u751F\u6210\uFF1A{os.path.abspath(output_path)}")
        print(f"   \u603B\u8BB0\u5F55\uFF1A{self.total_count:,} | \u8FC7\u6EE4\u540E\uFF1A{self.filtered_count:,} | \u9519\u8BEF\uFF1A{self.error_count:,}")
        print(f"   \u672C\u5730\u6A21\u5F0F\uFF1A{self.local_mode_count:,} | \u8FDC\u7A0B\u6A21\u5F0F\uFF1A{self.remote_mode_count:,}")
        print(f"   \u65F6\u95F4\u7A97\u53E3\u6570\uFF1A{len(time_windows)} | Top\u5C55\u793A\uFF1A{len(top_slow_by_latency)} \u6761")
        print(f"   \u5CF0\u503C\u5185\u5B58\uFF1A{mem_mb:.1f} MB")
        if not HAS_ORJSON:
            print("   \u63D0\u793A\uFF1Apip install orjson \u53EF\u8FDB\u4E00\u6B65\u52A0\u901F JSON \u89E3\u6790")

    @staticmethod
    def _qps_section():
        return '''        <div class="chart-section">
            <h2>\U0001F4C8 \u5404\u8BF7\u6C42\u7C7B\u578B QPS\uFF0810ms\u7A97\u53E3\uFF0C\u6BCF\u79D2\u8BF7\u6C42\u6570\uFF09</h2>
            <div class="legend-grid" id="qpsLegend"></div>
            <div id="qpsChart" class="chart-container"></div>
        </div>
'''

    @staticmethod
    def _urma_perf_section():
        return '''        <div class="chart-section">
            <h2>\U0001F9ED URMA_PERF \u8FD0\u884C\u65F6\u5468\u671F\u91C7\u6837\uFF08\u975E\u8BF7\u6C42\u7EA7\u65F6\u5EF6\uFF09</h2>
            <div class="filters-info">\u4EC5\u5C55\u793A\u65E5\u5FD7\u4E2D\u7684\u771F\u5B9E URMA_PERF \u91C7\u6837\u65F6\u95F4\uFF1B\u4E0D\u6309\u8BF7\u6C42\u7A97\u53E3\u8865\u96F6\u3001\u4E0D\u505A\u524D\u5411\u586B\u5145\u6216\u63D2\u503C\u3002</div>
            <div class="metric-tabs" id="urmaPerfMetricTabs">
                <div class="metric-tab active" data-metric="avg">\u5E73\u5747\u503C</div>
                <div class="metric-tab" data-metric="p99">P99</div>
                <div class="metric-tab" data-metric="p9999">P99.99</div>
                <div class="metric-tab" data-metric="pmax">\u6700\u5927\u503C</div>
            </div>
            <div class="legend-grid" id="urmaPerfLegend"></div>
            <div id="urmaPerfChart" class="chart-container"></div>
        </div>
'''

    @staticmethod
    def _urma_perf_script():
        return '''
        const urmaPerfFields = [
            ['urma_ub_jetty_post_send', 'UB_JETTY_POST_SEND', '#5470c6'],
            ['urma_bond_jetty_post_send', 'BOND_JETTY_POST_SEND', '#91cc75'],
            ['urma_ub_poll_jfc', 'UB_POLL_JFC', '#fac858'],
            ['urma_bond_poll_jfc', 'BOND_POLL_JFC', '#ee6666']
        ];
        const urmaPerfChart = echarts.init(document.getElementById('urmaPerfChart'));
        const urmaPerfTimes = urmaPerfChartData.map(item => item.timestamp);
        const urmaPerfLegend = document.getElementById('urmaPerfLegend');
        urmaPerfFields.forEach(([, name, color]) => {
            const item = document.createElement('div');
            item.className = 'legend-item';
            item.innerHTML = '<div class="legend-color" style="background:' + color + '"></div><span>' + name + '</span>';
            urmaPerfLegend.appendChild(item);
        });
        function getUrmaPerfOption(metric) {
            return {
                tooltip: { trigger: 'axis', backgroundColor: 'rgba(26, 26, 46, 0.95)', borderColor: 'transparent', textStyle: { color: '#fff', fontSize: 12 }, padding: 12,
                    formatter: function(params) {
                        let html = '<div style="font-weight:600;margin-bottom:8px;">' + params[0].axisValue + '</div>';
                        params.forEach(p => {
                            const val = p.value >= 1000 ? (p.value / 1000).toFixed(2) + ' ms' : p.value.toFixed(0) + ' \\u00b5s';
                            html += '<div style="display:flex;justify-content:space-between;gap:20px;margin:3px 0;"><span><span style="display:inline-block;width:8px;height:8px;border-radius:50%;background:' + p.color + ';margin-right:6px;"></span>' + p.seriesName + '</span><span style="font-weight:600;">' + val + '</span></div>';
                        });
                        return html;
                    }
                },
                legend: { type: 'scroll', bottom: 0, textStyle: { fontSize: 11 } },
                grid: { left: 60, right: 40, top: 40, bottom: 60 },
                xAxis: { type: 'category', data: urmaPerfTimes, axisLabel: { fontSize: 11, rotate: 30, formatter: v => v.substring(11) }, axisLine: { lineStyle: { color: '#e2e8f0' } } },
                yAxis: { type: 'value', name: metric === 'avg' ? '\\u5e73\\u5747\\u65f6\\u5ef6 (\\u00b5s)' : metric === 'p99' ? 'P99 (\\u00b5s)' : metric === 'p9999' ? 'P99.99 (\\u00b5s)' : '\\u6700\\u5927\\u503c (\\u00b5s)', nameTextStyle: { fontSize: 12 }, axisLabel: { fontSize: 11 }, splitLine: { lineStyle: { color: '#f1f5f9' } } },
                series: urmaPerfFields.map(([field, name, color]) => ({
                    name: name, type: 'line', smooth: false, connectNulls: false, symbol: 'circle', symbolSize: 4,
                    lineStyle: { width: 1.5 }, itemStyle: { color: color }, emphasis: { focus: 'series' },
                    data: urmaPerfChartData.map(item => item[field][metric])
                })),
                dataZoom: [{ type: 'inside', start: 0, end: 100 }, { type: 'slider', start: 0, end: 100, bottom: 30, height: 20 }]
            };
        }
        urmaPerfChart.setOption(getUrmaPerfOption('avg'));
        document.getElementById('urmaPerfMetricTabs').addEventListener('click', e => {
            if (e.target.classList.contains('metric-tab')) {
                document.querySelectorAll('#urmaPerfMetricTabs .metric-tab').forEach(t => t.classList.remove('active'));
                e.target.classList.add('active');
                urmaPerfChart.setOption(getUrmaPerfOption(e.target.dataset.metric), true);
            }
        });
        window.addEventListener('resize', () => { urmaPerfChart.resize(); });
'''

    @staticmethod
    def _qps_script():
        return '''
        // QPS \u56FE\u8868
        const qpsData = {{QPS_DATA}};
        const qpsRequestTypes = {{QPS_REQUEST_TYPES}};
        const qpsTimeKeys = {{QPS_TIME_KEYS}};
        const qpsColors = ['#5470c6', '#91cc75', '#fac858', '#ee6666', '#73c0de', '#3ba272', '#fc8452', '#9a60b4', '#ea7ccc', '#ff9f7f'];
        const qpsChart = echarts.init(document.getElementById('qpsChart'));
        const qpsSeries = qpsRequestTypes.map((rt, idx) => ({
            name: rt, type: 'line', smooth: true, symbol: 'none', lineStyle: { width: 2 },
            itemStyle: { color: qpsColors[idx % qpsColors.length] }, emphasis: { focus: 'series' },
            data: qpsTimeKeys.map(tk => qpsData[tk][rt] || 0)
        }));
        qpsChart.setOption({
            tooltip: { trigger: 'axis', backgroundColor: 'rgba(26, 26, 46, 0.95)', borderColor: 'transparent', textStyle: { color: '#fff', fontSize: 12 }, padding: 12,
                formatter: function(params) {
                    let html = '<div style="font-weight:600;margin-bottom:8px;">' + params[0].axisValue + '</div>';
                    params.forEach(p => { html += '<div style="display:flex;justify-content:space-between;gap:20px;margin:3px 0;">' +
                        '<span><span style="display:inline-block;width:8px;height:8px;border-radius:50%;background:' + p.color + ';margin-right:6px;"></span>' + p.seriesName + '</span>' +
                        '<span style="font-weight:600;">' + p.value.toFixed(0) + ' req/s</span></div>'; });
                    return html;
                }
            },
            legend: { type: 'scroll', bottom: 0, textStyle: { fontSize: 11 } },
            grid: { left: 60, right: 40, top: 40, bottom: 60 },
            xAxis: { type: 'category', data: qpsTimeKeys, axisLabel: { fontSize: 10, rotate: 30 }, axisLine: { lineStyle: { color: '#e2e8f0' } } },
            yAxis: { type: 'value', name: 'QPS (req/s)', nameTextStyle: { fontSize: 12 }, axisLabel: { fontSize: 11 }, splitLine: { lineStyle: { color: '#f1f5f9' } } },
            series: qpsSeries,
            dataZoom: [{ type: 'inside', start: 0, end: 100 }, { type: 'slider', start: 0, end: 100, bottom: 30, height: 20 }]
        });
        const qpsLegendContainer = document.getElementById('qpsLegend');
        qpsRequestTypes.forEach((rt, idx) => {
            const item = document.createElement('div');
            item.className = 'legend-item';
            item.innerHTML = '<div class="legend-color" style="background:' + qpsColors[idx % qpsColors.length] + '"></div><span>' + rt + '</span>';
            qpsLegendContainer.appendChild(item);
        });
        window.addEventListener('resize', () => { qpsChart.resize(); });
'''

    @staticmethod
    def _html_template():
        return """<!DOCTYPE html>
<html lang="zh-CN">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>{{TITLE}}</title>
    <script src="https://cdn.jsdelivr.net/npm/echarts@5.4.3/dist/echarts.min.js"></script>
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', 'PingFang SC', 'Hiragino Sans GB', 'Microsoft YaHei', sans-serif; background: #f5f7fa; color: #333; line-height: 1.6; }
        .header { background: linear-gradient(135deg, #1a1a2e 0%, #16213e 100%); color: white; padding: 30px 40px; box-shadow: 0 2px 10px rgba(0,0,0,0.1); }
        .header h1 { font-size: 28px; font-weight: 600; margin-bottom: 8px; }
        .header .subtitle { color: #8892b0; font-size: 14px; }
        .container { max-width: 1600px; margin: 0 auto; padding: 20px 40px; }
        .summary-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 16px; margin: 24px 0; }
        .summary-card { background: white; border-radius: 12px; padding: 20px; box-shadow: 0 2px 8px rgba(0,0,0,0.06); border-left: 4px solid #5470c6; transition: transform 0.2s; }
        .summary-card:hover { transform: translateY(-2px); }
        .summary-card .label { font-size: 13px; color: #8892b0; margin-bottom: 6px; text-transform: uppercase; letter-spacing: 0.5px; }
        .summary-card .value { font-size: 24px; font-weight: 700; color: #1a1a2e; }
        .summary-card .sub { font-size: 12px; color: #666; margin-top: 4px; }
        .summary-card.error { border-left-color: #ee6666; }
        .summary-card.error .value { color: #ee6666; }
        .summary-card.warning { border-left-color: #fac858; }
        .summary-card.success { border-left-color: #91cc75; }
        .summary-card.info { border-left-color: #73c0de; }
        .chart-section { background: white; border-radius: 12px; padding: 24px; margin: 20px 0; box-shadow: 0 2px 8px rgba(0,0,0,0.06); }
        .chart-section h2 { font-size: 18px; font-weight: 600; margin-bottom: 16px; color: #1a1a2e; display: flex; align-items: center; gap: 8px; }
        .chart-section h2::before { content: ''; display: inline-block; width: 4px; height: 20px; background: #5470c6; border-radius: 2px; }
        .chart-container { width: 100%; height: 500px; }
        .chart-container.tall { height: 600px; }
        .metric-tabs { display: flex; gap: 8px; margin-bottom: 16px; flex-wrap: wrap; }
        .metric-tab { padding: 6px 16px; border-radius: 20px; border: 1px solid #e0e6ed; background: white; cursor: pointer; font-size: 13px; transition: all 0.2s; user-select: none; }
        .metric-tab:hover { background: #f5f7fa; }
        .metric-tab.active { background: #5470c6; color: white; border-color: #5470c6; }
        .top-table { width: 100%; border-collapse: collapse; font-size: 13px; margin-top: 16px; }
        .top-table th { background: #f8fafc; padding: 12px; text-align: left; font-weight: 600; color: #475569; border-bottom: 2px solid #e2e8f0; position: sticky; top: 0; }
        .top-table td { padding: 10px 12px; border-bottom: 1px solid #f1f5f9; vertical-align: top; }
        .top-table tr:hover { background: #f8fafc; }
        .top-table .trace-id { font-family: 'Courier New', monospace; font-size: 12px; color: #5470c6; }
        .top-table .latency { font-weight: 600; color: #ee6666; }
        .top-table .error-text { color: #ee6666; font-size: 12px; max-width: 300px; overflow: hidden; text-overflow: ellipsis; }
        .top-table .seg-bar { display: flex; height: 20px; border-radius: 4px; overflow: hidden; min-width: 200px; }
        .top-table .seg-bar div { height: 100%; }
        .legend-grid { display: grid; grid-template-columns: repeat(auto-fill, minmax(180px, 1fr)); gap: 8px; margin: 12px 0; font-size: 12px; }
        .legend-item { display: flex; align-items: center; gap: 6px; }
        .legend-color { width: 12px; height: 12px; border-radius: 2px; }
        .filters-info { background: #f0f4ff; border-radius: 8px; padding: 12px 16px; margin: 16px 0; font-size: 13px; color: #4c5fd7; border: 1px solid #dbe4ff; }
        .mode-badge { display: inline-block; padding: 2px 8px; border-radius: 4px; font-size: 11px; font-weight: 600; }
        .mode-local { background: #e6f7ff; color: #1890ff; }
        .mode-remote { background: #f6ffed; color: #52c41a; }
        @media (max-width: 768px) { .container { padding: 16px; } .header { padding: 20px; } .chart-container { height: 350px; } }
    </style>
</head>
<body>
    <div class="header">
        <h1>\U0001F50D {{TITLE}}</h1>
        <div class="subtitle">\u751F\u6210\u65F6\u95F4\uFF1A{{DATETIME}} | \u6570\u636E\u6E90\uFF1A{{FILENAME}}</div>
    </div>
    <div class="container">
        <div class="filters-info">
            <strong>\u8FC7\u6EE4\u6761\u4EF6\uFF1A</strong>
            \u8BF7\u6C42\u7C7B\u578B\uFF1A{{REQ_TYPES}} |
            \u65F6\u95F4\u8303\u56F4\uFF1A{{TIME_FROM}} ~ {{TIME_TO}} |
            \u65F6\u5EF6\u9608\u503C\uFF1A{{LATENCY_TH}} \u00B5s |
            \u5206\u6BB5\u9608\u503C\uFF1A{{SEGMENT_TH}} |
            \u4EC5\u9519\u8BEF\uFF1A{{ERROR_ONLY}} |
            \u805A\u5408\u7A97\u53E3\uFF1A{{WINDOW_LABEL}}
        </div>
        <div class="summary-grid">
            <div class="summary-card">
                <div class="label">\u603B\u8BF7\u6C42\u6570</div>
                <div class="value">{{TOTAL}}</div>
                <div class="sub">\u539F\u59CB\u6570\u636E\u603B\u91CF</div>
            </div>
            <div class="summary-card">
                <div class="label">\u5339\u914D\u8BB0\u5F55</div>
                <div class="value">{{FILTERED}}</div>
                <div class="sub">\u8FC7\u6EE4\u540E\u53C2\u4E0E\u5206\u6790</div>
            </div>
            <div class="summary-card error">
                <div class="label">\u9519\u8BEF\u8BF7\u6C42</div>
                <div class="value">{{ERRORS}}</div>
                <div class="sub">\u5360\u6BD4 {{ERR_PCT}}%\uFF08\u8FC7\u6EE4\u540E\uFF09</div>
            </div>
            <div class="summary-card info">
                <div class="label">\u672C\u5730\u6A21\u5F0F</div>
                <div class="value">{{LOCAL_MODE}}</div>
                <div class="sub">SDK\u2192\u672C\u5730Worker</div>
            </div>
            <div class="summary-card info">
                <div class="label">\u8FDC\u7A0B\u6A21\u5F0F</div>
                <div class="value">{{REMOTE_MODE}}</div>
                <div class="sub">SDK\u76F4\u8FDEMaster/Remote</div>
            </div>
            <div class="summary-card success">
                <div class="label">\u5E73\u5747\u65F6\u5EF6</div>
                <div class="value">{{AVG_LAT}}</div>
                <div class="sub">\u8FC7\u6EE4\u540E\u6570\u636E</div>
            </div>
            <div class="summary-card warning">
                <div class="label">P99 \u65F6\u5EF6</div>
                <div class="value">{{P99_LAT}}</div>
                <div class="sub">99\u5206\u4F4D</div>
            </div>
            <div class="summary-card">
                <div class="label">P99.99 \u65F6\u5EF6</div>
                <div class="value">{{P9999_LAT}}</div>
                <div class="sub">\u5C3E\u90E8\u6781\u7AEF\u503C</div>
            </div>
            <div class="summary-card error">
                <div class="label">\u6700\u5927\u65F6\u5EF6</div>
                <div class="value">{{MAX_LAT}}</div>
                <div class="sub">\u5355\u8BF7\u6C42\u5CF0\u503C</div>
            </div>
            <div class="summary-card">
                <div class="label">\u65F6\u95F4\u8DE8\u5EA6</div>
                <div class="value" style="font-size: 14px;">{{TIME_RANGE}}</div>
                <div class="sub">{{WINDOW_LABEL}}\u805A\u5408\u7A97\u53E3</div>
            </div>
        </div>
        <div class="chart-section">
            <h2>\U0001F4C8 \u5168\u9636\u6BB5\u65F6\u5EF6\u7EDF\u8BA1\u603B\u89C8</h2>
            <div style="overflow-x: auto;">
                <table class="top-table" style="margin-top: 0;">
                    <thead>
                        <tr><th>\u9636\u6BB5</th><th>\u5E73\u5747\u503C</th><th>P99</th><th>P99.99</th><th>\u6700\u5927\u503C</th><th>\u5360\u603B\u65F6\u5EF6\u6BD4</th><th>\u6837\u672C\u6570</th></tr>
                    </thead>
                    <tbody>{{OVERVIEW_ROWS}}</tbody>
                </table>
            </div>
        </div>
        <div class="chart-section">
            <h2>\U0001F4CA \u5404\u9636\u6BB5\u65F6\u5EF6\u8D8B\u52BF\uFF08{{WINDOW_LABEL}}\u805A\u5408\u7A97\u53E3\uFF09</h2>
            <div class="metric-tabs" id="metricTabs">
                <div class="metric-tab active" data-metric="avg">\u5E73\u5747\u503C</div>
                <div class="metric-tab" data-metric="p99">P99</div>
                <div class="metric-tab" data-metric="p9999">P99.99</div>
                <div class="metric-tab" data-metric="pmax">\u6700\u5927\u503C</div>
            </div>
            <div class="legend-grid" id="mainLegend"></div>
            <div id="trendChart" class="chart-container"></div>
        </div>
{{URMA_PERF_SECTION}}
{{QPS_SECTION}}
        <div class="chart-section">
            <h2>\U0001F422 Top {{TOP_N}} \u6700\u6162\u8BF7\u6C42\u65F6\u5E8F\u5206\u89E3</h2>
            <div class="legend-grid" id="topLegend"></div>
            <div id="topSlowChart" class="chart-container tall"></div>
        </div>
        <div class="chart-section">
            <h2>\U0001F4CB Top {{TOP_N}} \u6700\u6162\u8BF7\u6C42\u8BE6\u60C5</h2>
            <div style="overflow-x: auto;">
                <table class="top-table" id="topTable">
                    <thead>
                        <tr>
                            <th>\u6392\u540D</th><th>Trace ID</th><th>\u6A21\u5F0F</th><th>\u8BF7\u6C42\u7C7B\u578B</th><th>\u65F6\u95F4\u6233</th>
                            <th>\u603B\u65F6\u5EF6</th><th>\u72B6\u6001</th><th>\u5BA2\u6237\u7AEFIP</th><th>\u6570\u636E\u5927\u5C0F</th>
                            <th>\u65F6\u5EF6\u5206\u89E3</th><th>\u9519\u8BEF\u4FE1\u606F</th>
                        </tr>
                    </thead>
                    <tbody id="topTableBody">{{TOP_TABLE_ROWS}}</tbody>
                </table>
            </div>
        </div>
    </div>
    <script>
        const aggData = {{AGG_DATA}};
        const urmaPerfChartData = {{URMA_PERF_CHART_DATA}};
        const topSlowChartData = {{TOP_SLOW_CHART_DATA}};
        const topSlowTableData = {{TOP_SLOW_TABLE_DATA}};
        const fieldNames = {{FIELD_NAMES}};
        const fieldPaths = {{FIELD_PATHS}};
        const displayFields = {{DISPLAY_FIELDS}};
        const requestTrendFields = {{REQUEST_TREND_FIELDS}};
        const colors = {{COLORS}};
        const SEGMENT_FIELDS = {{SEGMENT_FIELDS}};
        const timeWindows = Object.keys(aggData).sort();

        function generateLegend(containerId, fieldList) {
            const container = document.getElementById(containerId);
            fieldList.forEach(([fp, name]) => {
                const segIdx = SEGMENT_FIELDS.findIndex(([f, n]) => f === fp);
                const item = document.createElement('div');
                item.className = 'legend-item';
                const fallback = {query_get_network_us: '#fac858', query_get_framework_us: '#73c0de', query_get_worker_internal_us: '#3ba272', urma_processing_us: '#9a60b4', worker_access_residual_us: '#91cc75', __other__: '#6e7074'};
                item.innerHTML = '<div class="legend-color" style="background:' + (segIdx >= 0 ? colors[segIdx] : fallback[fp]) + '"></div><span>' + name + '</span>';
                container.appendChild(item);
            });
            const totalItem = document.createElement('div');
            totalItem.className = 'legend-item';
            totalItem.innerHTML = '<div class="legend-color" style="background:#d32f2f"></div><span><strong>\u603B\u65F6\u5EF6</strong></span>';
            container.appendChild(totalItem);
        }
        generateLegend('mainLegend', requestTrendFields);
        // \u6DFB\u52A0 .info \u5B57\u6BB5\u56FE\u4F8B
        const mainLegendContainer = document.getElementById('mainLegend');
        [{ name: 'sleep\u65F6\u95F4', color: '#ff6b6b' }].forEach(info => {
            const item = document.createElement('div');
            item.className = 'legend-item';
            item.innerHTML = '<div class="legend-color" style="background:' + info.color + '"></div><span>' + info.name + '</span>';
            mainLegendContainer.appendChild(item);
        });
        const topExclusiveFields = [
            ['sdk_processing_us', 'SDK\u5904\u7406'],
            ['query_get_network_us', 'QueryGet\u7f51\u7edc'],
            ['query_get_framework_us', 'QueryGet\u6846\u67b6'],
            ['query_get_worker_internal_us', 'QueryGet Worker\u5185\u90e8'],
            ['urma_processing_us', 'Worker UB\u901a\u4fe1'],
            ['client_urma_processing_us', 'Client UB\u901a\u4fe1'],
            ['worker_access_residual_us', 'Worker/Meta\u5185\u90E8\uff08\u6263\u9664UB\uff09'],
            ['__other__', '\u5176\u4ed6']
        ];
        generateLegend('topLegend', topExclusiveFields);

        const trendChart = echarts.init(document.getElementById('trendChart'));
        let currentMetric = 'avg';

        function getTrendOption(metric) {
            const series = requestTrendFields.map(([fp, name]) => {
                const idx = SEGMENT_FIELDS.findIndex(([f]) => f === fp);
                return ({
                name: name, type: 'line', smooth: true, symbol: 'circle', symbolSize: 3,
                lineStyle: { width: 1.5 }, itemStyle: { color: colors[idx] },
                emphasis: { focus: 'series' },
                data: timeWindows.map(tw => { const v = aggData[tw][fp]; return v ? v[metric] : 0; })
                });
            });
            // \u6DFB\u52A0 .info \u6587\u4EF6\u7EDF\u8BA1\u5B57\u6BB5\uFF1Asleep \u65F6\u95F4
            const infoFields = [
                { fp: 'sleep_time_us', name: 'sleep\u65F6\u95F4', color: '#ff6b6b' }
            ];
            infoFields.forEach(info => {
                series.push({
                    name: info.name, type: 'line', smooth: true, symbol: 'triangle', symbolSize: 5,
                    lineStyle: { width: 2, type: 'dashed' }, itemStyle: { color: info.color },
                    emphasis: { focus: 'series' },
                    data: timeWindows.map(tw => { const v = aggData[tw][info.fp]; return v ? v[metric] : 0; })
                });
            });
            // urma info 4\u4E2A\u72EC\u7ACB\u5B57\u6BB5\uFF08UB_JETTY_POST_SEND / BOND_JETTY_POST_SEND / UB_POLL_JFC / BOND_POLL_JFC\uFF09
            // \u5DF2\u5305\u542B\u5728 SEGMENT_FIELDS \u4E3B\u7CFB\u5217\u4E2D\uFF1A\u7A97\u53E3\u5185\u5404\u7EDF\u8BA1\u91CF(avg/p99/p9999/pmax)\u53D6\u6700\u5927\u503C\uFF0C\u968F metric \u5207\u6362
            series.push({
                name: '\u603B\u65F6\u5EF6', type: 'line', smooth: true, symbol: 'diamond', symbolSize: 6,
                lineStyle: { width: 3, type: 'solid' }, itemStyle: { color: '#d32f2f' },
                emphasis: { focus: 'series', lineStyle: { width: 4 } }, z: 10,
                data: timeWindows.map(tw => { const v = aggData[tw]['__total_latency__']; return v ? v[metric] : 0; })
            });
            return {
                tooltip: {
                    trigger: 'axis', backgroundColor: 'rgba(26, 26, 46, 0.95)', borderColor: 'transparent',
                    textStyle: { color: '#fff', fontSize: 12 }, padding: 12,
                    formatter: function(params) {
                        let html = '<div style="font-weight:600;margin-bottom:8px;">' + params[0].axisValue + '</div>';
                        params.forEach(p => {
                            let val;
                            if (p.seriesName === 'URMA\u5E76\u53D1\u6570') { val = p.value.toFixed(1) + ' \u4E2A'; }
                            else { val = p.value >= 1000 ? (p.value/1000).toFixed(2) + ' ms' : p.value.toFixed(0) + ' \u00B5s'; }
                            html += '<div style="display:flex;justify-content:space-between;gap:20px;margin:3px 0;">' +
                                '<span><span style="display:inline-block;width:8px;height:8px;border-radius:50%;background:' + p.color + ';margin-right:6px;"></span>' + p.seriesName + '</span>' +
                                '<span style="font-weight:600;">' + val + '</span></div>';
                        });
                        return html;
                    }
                },
                legend: { type: 'scroll', bottom: 0, textStyle: { fontSize: 11 } },
                grid: { left: 60, right: 40, top: 40, bottom: 60 },
                xAxis: { type: 'category', data: timeWindows, axisLabel: { fontSize: 11, rotate: 30, formatter: v => v.substring(11) }, axisLine: { lineStyle: { color: '#e2e8f0' } } },
                yAxis: { type: 'value', name: metric === 'avg' ? '\u5E73\u5747\u65F6\u5EF6 (\u00B5s)' : metric === 'p99' ? 'P99 (\u00B5s)' : metric === 'p9999' ? 'P99.99 (\u00B5s)' : '\u6700\u5927\u503C (\u00B5s)', nameTextStyle: { fontSize: 12 }, axisLabel: { fontSize: 11 }, splitLine: { lineStyle: { color: '#f1f5f9' } } },
                series: series,
                dataZoom: [{ type: 'inside', start: 0, end: 100 }, { type: 'slider', start: 0, end: 100, bottom: 30, height: 20 }]
            };
        }
        trendChart.setOption(getTrendOption('avg'));
        document.getElementById('metricTabs').addEventListener('click', e => {
            if (e.target.classList.contains('metric-tab')) {
                document.querySelectorAll('.metric-tab').forEach(t => t.classList.remove('active'));
                e.target.classList.add('active');
                currentMetric = e.target.dataset.metric;
                trendChart.setOption(getTrendOption(currentMetric), true);
            }
        });

        const topSlowChart = echarts.init(document.getElementById('topSlowChart'));
        const topXData = topSlowChartData.map(d => d.timestamp + '\\n' + d.trace_id.substring(0, 8));
        const topSeries = topExclusiveFields.map(([fp, name], index) => {
            const idx = SEGMENT_FIELDS.findIndex(([f, n]) => f === fp);
            return { name: name, type: 'bar', stack: 'total', itemStyle: { color: idx >= 0 ? colors[idx] : ['#5470c6', '#fac858', '#73c0de', '#3ba272', '#9a60b4', '#ee6666', '#91cc75', '#6e7074'][index] }, emphasis: { focus: 'series' },
                data: topSlowChartData.map(d => { const seg = d.segments.find(s => s.field === fp); return seg ? seg.value : 0; }) };
        });
        topSlowChart.setOption({
            tooltip: { trigger: 'axis', axisPointer: { type: 'shadow' }, backgroundColor: 'rgba(26, 26, 46, 0.95)', borderColor: 'transparent', textStyle: { color: '#fff', fontSize: 12 }, padding: 12,
                formatter: function(params) {
                    const idx = params[0].dataIndex;
                    const rec = topSlowChartData[idx];
                    let html = '<div style="font-weight:600;margin-bottom:8px;">' + rec.timestamp + '</div>';
                    html += '<div style="font-size:11px;color:#8892b0;margin-bottom:8px;">' + rec.trace_id + ' | ' + rec.request_type + ' | <span class="mode-' + rec.mode + '">' + ({local_worker: 'local worker', remote_worker: 'remote worker', local_meta_worker: 'localMetaWorker', remote_meta_worker: 'remoteMetaWorker', unknown: '\u672a\u77e5'}[rec.path_role] || '\u672a\u77e5') + '</span></div>';
                    html += '<div style="margin-bottom:6px;font-weight:600;">\u603B\u65F6\u5EF6: ' + (rec.latency_us >= 1000 ? (rec.latency_us/1000).toFixed(2)+' ms' : rec.latency_us+' \u00B5s') + '</div>';
                    params.forEach(p => {
                        if (p.value > 0) {
                            let val = p.value >= 1000 ? (p.value/1000).toFixed(2) + ' ms' : p.value.toFixed(0) + ' \u00B5s';
                            if (p.seriesName === 'URMA\u5E76\u53D1\u6570') val = p.value.toFixed(1) + ' \u4E2A';
                            html += '<div style="display:flex;justify-content:space-between;gap:20px;margin:2px 0;font-size:11px;">' +
                                '<span><span style="display:inline-block;width:8px;height:8px;border-radius:2px;background:' + p.color + ';margin-right:6px;"></span>' + p.seriesName + '</span>' +
                                '<span>' + val + '</span></div>';
                        }
                    });
                    return html;
                }
            },
            legend: { type: 'scroll', bottom: 0, textStyle: { fontSize: 11 } },
            grid: { left: 80, right: 40, top: 30, bottom: 100 },
            xAxis: { type: 'category', data: topXData, axisLabel: { fontSize: 10, rotate: 45, interval: 0 }, axisLine: { lineStyle: { color: '#e2e8f0' } } },
            yAxis: { type: 'value', name: '\u65F6\u5EF6 (\u00B5s)', nameTextStyle: { fontSize: 12 }, axisLabel: { fontSize: 11 }, splitLine: { lineStyle: { color: '#f1f5f9' } } },
            series: topSeries,
            dataZoom: [{ type: 'inside', start: 0, end: Math.min(100, 50 * 100 / topSlowChartData.length) }, { type: 'slider', start: 0, end: Math.min(100, 50 * 100 / topSlowChartData.length), bottom: 60, height: 20 }]
        });

        const tbody = document.getElementById('topTableBody');
        tbody.innerHTML = '';
        topSlowTableData.forEach((rec, idx) => {
            const tr = document.createElement('tr');
            const statusHtml = rec.status_code !== 0 || rec.error
                ? '<span style="color:#ee6666;font-weight:600;">\u2717 ' + rec.status_code + '</span>'
                : '<span style="color:#91cc75;font-weight:600;">\u2713 OK</span>';
            const modeBadge = '<span class="mode-badge mode-' + rec.mode + '">' + ({local_worker: 'local worker', remote_worker: 'remote worker', local_meta_worker: 'localMetaWorker', remote_meta_worker: 'remoteMetaWorker', unknown: '\u672a\u77e5'}[rec.path_role] || '\u672a\u77e5') + '</span>';
            let barHtml = '<div class="seg-bar">';
            rec.segments.forEach((seg, sidx) => {
                if (seg.value > 0 && seg.field !== 'urma_inflight_max') {
                    const pct = (seg.value / rec.latency_us * 100).toFixed(1);
                    const colorIdx = SEGMENT_FIELDS.findIndex(([fp, n]) => n === seg.name);
                    const fallback = { 'QueryGet\u7f51\u7edc': '#fac858', 'QueryGet\u6846\u67b6': '#73c0de', 'QueryGet Worker\u5185\u90E8': '#3ba272', 'Worker UB\u901a\u4fe1': '#9a60b4', 'Worker/Meta\u5185\u90E8\uff08\u6263\u9664UB\uff09': '#91cc75', '\u5176\u4ed6': '#6e7074' };
                    barHtml += '<div style="width:' + pct + '%;background:' + (colorIdx >= 0 ? colors[colorIdx] : fallback[seg.name]) + ';" title="' + seg.name + ': ' + seg.value + (seg.name === 'URMA\u5E76\u53D1\u6570' ? '\u4E2A' : '\u00B5s') + '"></div>';
                }
            });
            barHtml += '</div>';
            const segText = rec.segments.map(s => s.name + ':' + s.value + (s.name === 'URMA\u5E76\u53D1\u6570' ? '\u4E2A' : '')).join(', ');
            tr.innerHTML =
                '<td><strong>#' + (idx + 1) + '</strong></td>' +
                '<td class="trace-id">' + rec.trace_id + '</td>' +
                '<td>' + modeBadge + '</td>' +
                '<td>' + rec.request_type + '</td>' +
                '<td>' + rec.timestamp + '</td>' +
                '<td class="latency">' + (rec.latency_us >= 1000 ? (rec.latency_us/1000).toFixed(2)+' ms' : rec.latency_us+' \u00B5s') + '</td>' +
                '<td>' + statusHtml + '</td>' +
                '<td>' + rec.client_ip + '</td>' +
                '<td>' + (rec.data_size ? (rec.data_size/1024/1024).toFixed(2)+' MB' : '-') + '</td>' +
                '<td>' + barHtml + '<div style="font-size:10px;color:#666;margin-top:2px;max-width:400px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;" title="' + segText + '">' + segText + '</div></td>' +
                '<td class="error-text">' + (rec.error || '-') + '</td>';
            tbody.appendChild(tr);
        });

{{QPS_SCRIPT}}
{{URMA_PERF_SCRIPT}}
        window.addEventListener('resize', () => { trendChart.resize(); topSlowChart.resize(); });
    </script>
</body>
</html>"""


def _read_task_list(path):
    return [Path(line.strip()) for line in Path(path).read_text(encoding='utf-8').splitlines()
            if line.strip() and not line.lstrip().startswith('#')]


def _task_paths(task, client_root, worker_root):
    task_path = Path(task)
    if task_path.is_absolute():
        return str(task_path), str(task_path)
    cwd = Path.cwd()
    return (str((Path(client_root) / task_path) if client_root else (cwd / task_path)),
            str((Path(worker_root) / task_path) if worker_root else (cwd / task_path)))


def _filters_for(request_type, args):
    return {
        'request_types': [request_type], 'time_from': parse_timestamp(args.from_time) if args.from_time else None,
        'time_to': parse_timestamp(args.to_time) if args.to_time else None,
        'latency_threshold': args.latency_threshold, 'has_error_only': args.error_only,
        'segment_thresholds': {}, 'sleep_threshold_us': args.sleep_threshold,
    }


def parse_bool(value):
    normalized = str(value).strip().lower()
    if normalized in ('true', '1', 'yes', 'y', 'on'):
        return True
    if normalized in ('false', '0', 'no', 'n', 'off'):
        return False
    raise argparse.ArgumentTypeError('expected true or false')


def generate_task_reports(client_dir, worker_dir, output_dir, args):
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    temp_dir = tempfile.mkdtemp(prefix='kvcache_trace_report_')
    trace_file = os.path.join(temp_dir, 'traces.jsonl')
    info_file = trace_file + '.info'
    print(f'\n[Task] client={client_dir} worker={worker_dir}')
    print(f'[Task] scenario hint={args.scenario}; auto detection remains evidence-based per trace.')
    print(f'[Task] temporary correlation directory: {temp_dir}')
    try:
        client_dirs, worker_dirs = expand([client_dir]), expand([worker_dir])
        if not client_dirs:
            print(f'[WARN] client directory does not exist or is not a directory: {client_dir}')
        if not worker_dirs:
            print(f'[WARN] worker directory does not exist or is not a directory: {worker_dir}')
        records, info_records = phase1(client_dirs, worker_dirs, temp_dir, args.shard_bits, args.jobs,
                                       os.path.join(temp_dir, 'info'))
        if records == 0:
            print('[WARN] no trace-bearing log records were collected; no report was generated.')
            return []
        traces = phase2(temp_dir, trace_file, args.jobs, {})
        if traces == 0:
            print('[WARN] access traces could not be correlated; verify client access logs and trace_id sampling.')
            return []
        if info_records:
            merge_info_files(os.path.join(temp_dir, 'info'), info_file)
        reports = []
        report_types = ('get', 'set') if args.report_type == 'setandget' else (args.report_type,)
        if args.qps:
            print('[Report] QPS: enabled (10ms aggregation and chart payload).')
        else:
            print('[Report] QPS: disabled; skipped 10ms aggregation and QPS HTML payload.')
        for request_type in report_types:
            output = output_dir / f'kvcache.{request_type}.html'
            print(f'[Report] generating {request_type.upper()} HTML: {output}')
            analyzer = KVCacheAnalyzer(trace_file, filters=_filters_for(
                'DS_KV_CLIENT_GET' if request_type == 'get' else 'DS_KV_CLIENT_SET', args),
                top_n=args.top, window_ms=args.window_ms, collect_qps=args.qps)
            analyzer.generate_html(str(output))
            if output.exists() and output.stat().st_size > 0:
                reports.append(str(output))
                print(f'[Report] generated {output} ({output.stat().st_size:,} bytes)')
            else:
                print(f'[ERROR] HTML output was not created: {output}')
        return reports
    except Exception as error:
        print(f'[ERROR] report generation failed: {type(error).__name__}: {error}', file=sys.stderr)
        raise
    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)
        print('[Cleanup] temporary correlation data removed.')


def main():
    parser = argparse.ArgumentParser(
        description='KVCache trace parser and original-layout HTML report generator',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''Expected collected-log layout (extra nesting is allowed):
  <task>/collected/clients/.../ds_client_access*.log[.gz]
  <task>/collected/clients/.../ds_client*INFO*.log[.gz]
  <task>/collected_worker_logs/workers/.../access*.log[.gz]
  <task>/collected_worker_logs/workers/.../kvcache*INFO*.log[.gz]

Use case: offline GET/SET latency diagnosis in meta-owner, same-node, and
local-cache deployments. Client/worker records correlate on any non-empty
trace-id field except '-', 'null', or 'None'; UUID format is not required.

Constraints: -f is one task; -list contains one task per non-comment line.
Relative list entries use --client-path/--worker-path or the current directory.
-j, --shard-bits, and --window-ms must be positive. -qps false avoids the
10ms QPS aggregation and its large HTML payload for high-QPS/long-running logs.

Examples:
  python3 kvcache_trace_report.py -f /path/to/task -type setandget -j 32
  python3 kvcache_trace_report.py -f /path/to/task -type get -qps false
  python3 kvcache_trace_report.py -list html.txt --client-path /logs/client \\
      --worker-path /logs/worker --output-dir report -type get -j 32
  python3 kvcache_trace_report.py -h''',
    )
    input_group = parser.add_mutually_exclusive_group()
    input_group.add_argument('-f', '--folder', default='.', help='one task directory (default: current directory)')
    input_group.add_argument('-list', '--list', dest='list_file', help='one task directory per line')
    parser.add_argument('-type', '--type', dest='report_type', choices=('set', 'get', 'setandget'), default='setandget')
    parser.add_argument('-j', '--jobs', type=int, default=32, help='parser worker processes (default: 32)')
    parser.add_argument('-qps', type=parse_bool, default=True, metavar='{true,false}',
                        help='include 10ms QPS aggregation/chart; false reduces memory and HTML size (default: true)')
    parser.add_argument('--client-path', help='client root for relative list entries')
    parser.add_argument('--worker-path', help='worker root for relative list entries')
    parser.add_argument('--scenario', choices=('auto', 'meta-owner', 'same-node', 'local-cache'), default='auto',
                        help='record the expected deployment path in progress logs; parsing remains log-evidence based')
    parser.add_argument('--output-dir', default='report', help='report root (default: ./report)')
    parser.add_argument('--shard-bits', type=int, default=8, help='temporary trace shards: 2^N (default: 256)')
    parser.add_argument('--top', type=int, default=200, help='slow traces shown in each legacy report')
    parser.add_argument('--window-ms', type=int, default=1000, help='trend aggregation window in milliseconds')
    parser.add_argument('--from-time')
    parser.add_argument('--to-time')
    parser.add_argument('--latency-threshold', type=int, default=0)
    parser.add_argument('--error-only', action='store_true')
    parser.add_argument('--sleep-threshold', type=int, default=250)
    args = parser.parse_args()
    if args.jobs < 1 or args.shard_bits < 1 or args.window_ms < 1:
        parser.error('--jobs, --shard-bits, and --window-ms must be positive')
    tasks = _read_task_list(args.list_file) if args.list_file else [Path(args.folder)]
    print('=' * 72)
    print(f'KVCache trace report | tasks={len(tasks)} | jobs={args.jobs} | type={args.report_type}')
    print('=' * 72)
    for task in tasks:
        client_dir, worker_dir = _task_paths(task, args.client_path, args.worker_path)
        target = Path(args.output_dir) / task.name if len(tasks) > 1 else Path(args.output_dir)
        generate_task_reports(client_dir, worker_dir, target, args)


if __name__ == '__main__':
    main()
