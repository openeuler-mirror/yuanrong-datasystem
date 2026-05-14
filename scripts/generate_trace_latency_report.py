#!/usr/bin/env python3
"""KVCache Trace-Level Latency Segmentation Analysis.

Correlates SDK client access logs with Worker logs (access + info) by traceId,
computes per-request latency segmentation, and generates an interactive HTML report
with ECharts charts.

Segmentation Model:

  GET:
    SDK Total
    +-- Request Delay (SDK start → Worker start, via timestamps)
    +-- Queue Wait (Worker thread pool queue wait, [Get] Receive elapsed)
    +-- Worker Processing
        +-- Worker->Master QueryMeta
        |   +-- RPC Network (ZMQ network_residual)
        |   +-- Master Processing (req_queue + exec + rsp_queue)
        +-- Remote Get ([Get] Remote done)
        |   +-- RPC Network (Remote done - Remote Pull)
        |   +-- Remote Worker Processing (Remote Pull)
        |       +-- Remote Worker Self (Remote Pull - URMA)
        |       +-- URMA Write
        +-- Other Worker (RPC retries / error cleanup)

  SET:
    SDK Total
    +-- Request Delay (SDK start → Worker start)
    +-- Worker Create (local OC)
    +-- Worker Publish
        +-- Worker->Master CreateMeta
        |   +-- RPC Network
        |   +-- Master Processing
        +-- Other Publish

Usage:
    # Explicit SDK + Worker log files
    python3 generate_trace_latency_report.py <sdk_log> <worker_log> -o report.html

    # With time filtering
    python3 generate_trace_latency_report.py <sdk_log> <worker_log> \\
        --since "2026-05-10T02:00:00" --until "2026-05-10T03:00:00" \\
        -o report.html --open

    # Auto-detect from directory (looks for ds_client_access*.log and worker log)
    python3 generate_trace_latency_report.py <log_dir> -o report.html

Log Formats:
    SDK access log:     pipe-separated, source contains 'access_recorder',
                        operation names like DS_KV_CLIENT_GET, DS_KV_CLIENT_SET
    Worker access log:  same pipe format with 'jingpai' field
    Worker info log:    pipe-separated with 'jingpai' field, contains [Get] Done,
                        Publish done, ZMQ_RPC_FRAMEWORK_SLOW, etc.

Report Sections:
    1. Summary              GET/SET count, avg, P50, P90, P99, P99.99, Max, Min
    2. GET Per-Request      SDK latency per request + running P50/P99/Max lines
    3. GET Segmentation     Stacked bar: Client RPC / QMeta Net / Master Proc /
                            Remote Net / Remote Self / URMA / Other Worker
    4. GET Seg Summary      Per-phase Avg/P99/Max table
    5. SET Per-Request      SDK latency per request
    6. SET Segmentation     Stacked bar: Client RPC / Create / CMeta Net /
                            Master Proc / Other Publish
    7. SET Seg Summary      Per-phase Avg/P99/Max table
    8. Top N Traces         Highest latency traces with full segmentation
                            and retry/error details

Requirements:
    - Python 3.8+
    - Internet access for ECharts CDN (jsdelivr.net)
"""

import argparse
import gzip
import html
import json
import os
import re
import subprocess
import sys
from collections import defaultdict
from datetime import datetime, timedelta

# ---------------------------------------------------------------------------
# Parsing helpers
# ---------------------------------------------------------------------------

PIPE_RE = re.compile(r"\s*\|\s*")


def _ts(s):
    return datetime.fromisoformat(s.strip())


def _pipe(line):
    parts = PIPE_RE.split(line.strip())
    return parts if len(parts) >= 6 else None


def _open_log(path):
    """Open a log file, handling both plain .log and gzip .log.gz."""
    if path.endswith(".gz"):
        return gzip.open(path, "rt", encoding="utf-8", errors="replace")
    return open(path, encoding="utf-8", errors="replace")


def _access(parts):
    try:
        return {
            "ts": _ts(parts[0]),
            "host": parts[3].strip(),
            "pidtid": parts[4].strip(),
            "trace_id": parts[5].strip(),
            "status": int(parts[7].strip()),
            "operation": parts[8].strip(),
            "duration_us": int(parts[9].strip()),
            "size": int(parts[10].strip()),
        }
    except (ValueError, IndexError):
        return None


# ---------------------------------------------------------------------------
# Regexes for worker info log segmentation
# ---------------------------------------------------------------------------

_RE_GET_DONE = re.compile(
    r"\[Get\] Done.*?totalCost:\s+([\d.]+)ms"
)
_RE_PUB_DONE = re.compile(
    r"Publish done.*?cost:\s+([\d.]+)ms"
)
_RE_EXCEED_BLK = re.compile(r"exceed 3ms:\s+\{(.+?)\}")
_RE_EXCEED = re.compile(r"(\w[\w\s]*?):\s+(\d+)\s*ms")
_RE_MASTER_DONE = re.compile(r"\[Get\] Master query done.*?cost:\s+([\d.]+)ms")
_RE_REMOTE_DONE = re.compile(r"\[Get\] Remote done.*?cost:\s+([\d.]+)ms")
_RE_REMOTE_PULL = re.compile(r"\[Get/RemotePull\] finish.*?cost:\s+([\d.]+)ms")
_RE_URMA = re.compile(r"\[URMA_ELAPSED_TOTAL\].*?cost\s+([\d.]+)ms")
_RE_ZMQ = re.compile(
    r"\[ZMQ_RPC_FRAMEWORK_SLOW\].*?e2e_us=(\d+)\s+"
    r"client_req_framework_us=(\d+)\s+remote_processing_us=(\d+)\s+"
    r"client_rsp_framework_us=(\d+)\s+"
    r"server_req_queue_us=(\d+)\s+server_exec_us=(\d+)\s+"
    r"server_rsp_queue_us=(\d+)\s+network_residual_us=(\d+)"
)
_RE_GET_RECV = re.compile(r"\[Get\] Receive.*?elapsed:\s+([\d.]+)ms")
_RE_RPC_TIMEOUT = re.compile(r"RPC timeout\.\s+time elapsed\s+(\d+)")


def _extract(entries):
    """Extract segmentation metrics from worker info log entries."""
    s = {}
    for e in entries:
        msg = e["message"]
        m = _RE_GET_DONE.search(msg)
        if m:
            s.setdefault("get_costs", []).append(float(m.group(1)))
            eb = _RE_EXCEED_BLK.search(msg)
            if eb:
                for sm in _RE_EXCEED.finditer(eb.group(1)):
                    key = f"x_{sm.group(1).strip()}"
                    s[key] = s.get(key, 0) + int(sm.group(2))
        m = _RE_PUB_DONE.search(msg)
        if m:
            s.setdefault("pub_costs", []).append(float(m.group(1)))
            eb = _RE_EXCEED_BLK.search(msg)
            if eb:
                for sm in _RE_EXCEED.finditer(eb.group(1)):
                    key = f"x_{sm.group(1).strip()}"
                    s[key] = s.get(key, 0) + int(sm.group(2))
        m = _RE_MASTER_DONE.search(msg)
        if m:
            val = float(m.group(1))
            s["master_q_ms"] = max(s.get("master_q_ms", 0), val)
        m = _RE_REMOTE_DONE.search(msg)
        if m:
            s.setdefault("rdone", []).append(float(m.group(1)))
        m = _RE_REMOTE_PULL.search(msg)
        if m:
            s.setdefault("rpull", []).append(float(m.group(1)))
        m = _RE_URMA.search(msg)
        if m:
            s.setdefault("urma", []).append(float(m.group(1)))
        m = _RE_ZMQ.search(msg)
        if m:
            s["zmq_e2e"] = int(m.group(1))
            s["zmq_client_req_fw"] = int(m.group(2))
            s["zmq_remote_proc"] = int(m.group(3))
            s["zmq_client_rsp_fw"] = int(m.group(4))
            s["zmq_sreqq"] = int(m.group(5))
            s["zmq_sexec"] = int(m.group(6))
            s["zmq_srspq"] = int(m.group(7))
            s["zmq_netres"] = int(m.group(8))
            s["master_proc_us"] = int(m.group(5)) + int(m.group(6)) + int(m.group(7))
        m = _RE_GET_RECV.search(msg)
        if m:
            s.setdefault("get_recv_elapsed", []).append(float(m.group(1)))
        m = _RE_RPC_TIMEOUT.search(msg)
        if m:
            s["rpc_timeout_elapsed"] = int(m.group(1))
    for k in ("get_costs", "pub_costs", "rdone", "rpull", "urma", "get_recv_elapsed"):
        if k in s:
            v = s[k]
            s[f"{k}_max"] = max(v)
            s[f"{k}_cnt"] = len(v)
    return s


# ---------------------------------------------------------------------------
# Error / retry counters
# ---------------------------------------------------------------------------


def _extract_errors(entries):
    er = {
        "wa_cnt": 0, "get_done_cnt": 0, "pub_done_cnt": 0, "rpc_retry": 0,
        "ret_timeout": False, "remote_fail": 0, "remove_fail": 0,
        "master_fail": 0, "pub_fail": 0, "cant_find": False,
        "errors": [], "batch_fail": 0, "cleanup_delete": 0,
        "local_lookup": 0, "rpc_timeout_wait": 0,
    }
    for e in entries:
        msg = e["message"]
        src = e.get("source", "")
        if "access_recorder" in src:
            er["wa_cnt"] += 1
        elif "[Get] Done" in msg:
            er["get_done_cnt"] += 1
        elif "Publish done" in msg:
            er["pub_done_cnt"] += 1
        elif "[RPC Retry]" in msg:
            er["rpc_retry"] += 1
        elif "ReturnFromGetRequest timeout" in msg:
            er["ret_timeout"] = True
        elif "RPC timeout" in msg and "time elapsed" in msg:
            er["rpc_timeout_wait"] += 1
        elif "Get from remote failed" in msg:
            er["remote_fail"] += 1
        elif "BatchGetObjectFromRemoteOnLock failed" in msg:
            er["batch_fail"] += 1
        elif "Remove location failed" in msg:
            er["remove_fail"] += 1
        elif "delete unacked" in msg:
            er["cleanup_delete"] += 1
        elif "Publish failed" in msg:
            er["pub_fail"] += 1
        elif "RequestingToMaster failed" in msg:
            er["master_fail"] += 1
        elif "Can't find object" in msg:
            er["cant_find"] = True
        elif "[Get] Receive" in msg:
            er["local_lookup"] += 1
        elif "URMA operation failed" in msg or "Failed to wait for URMA" in msg:
            er["errors"].append("URMA_FAIL")
        elif "TCP fallback payload rejected" in msg:
            er["errors"].append("TCP_FALLBACK_REJECTED")
    return er


# ---------------------------------------------------------------------------
# Log parsing
# ---------------------------------------------------------------------------


def _find_log_files(log_dir):
    """Auto-detect SDK and Worker log files in a directory.

    Returns (sdk_paths, worker_paths) lists.
    SDK: files matching 'ds_client_access' in name.
    Worker: files matching 'access', 'resource', or 'worker' in name (not ds_client_*).
    """
    sdk_paths = []
    worker_paths = []
    for root, _dirs, files in os.walk(log_dir, followlinks=False):
        for f in sorted(files):
            full = os.path.join(root, f)
            if not (f.endswith(".log") or f.endswith(".log.gz")):
                continue
            if "ds_client_access" in f:
                sdk_paths.append(full)
            elif any(p in f for p in ("access", "worker", "info", "resource")) and "ds_client" not in f:
                worker_paths.append(full)
    return sdk_paths, worker_paths


def parse_logs(sdk_path, worker_path, since=None, until=None):
    """Parse SDK and Worker log files.

    Args:
        sdk_path: path to SDK client access log (or list of paths)
        worker_path: path to Worker log (access+info, or list of paths)
        since: datetime, inclusive start filter
        until: datetime, exclusive end filter

    Returns:
        (sdk_records, wa_records, worker_info_by_traceid, worker_err_by_traceid)
    """
    sdk_paths = sdk_path if isinstance(sdk_path, list) else [sdk_path]
    worker_paths = worker_path if isinstance(worker_path, list) else [worker_path]

    sdk = []
    wa = []
    wi = defaultdict(list)
    we = defaultdict(list)
    stats = {"sdk_lines": 0, "wa_lines": 0, "wi_lines": 0, "skip_parse": 0}

    def _time_ok(ts):
        if since and ts < since:
            return False
        if until and ts >= until:
            return False
        return True

    # Parse SDK access logs
    total_lines = 0
    for path in sdk_paths:
        print(f"  Parsing SDK: {os.path.basename(path)}")
        with _open_log(path) as f:
            for line in f:
                total_lines += 1
                if total_lines % 500000 == 0:
                    print(f"  ... {total_lines:,} lines")
                p = _pipe(line)
                if not p or "access_recorder" not in p[2]:
                    continue
                r = _access(p)
                if r and _time_ok(r["ts"]):
                    sdk.append(r)
                stats["sdk_lines"] += 1

    # Parse Worker logs (access + info)
    for path in worker_paths:
        print(f"  Parsing Worker: {os.path.basename(path)}")
        with _open_log(path) as f:
            for line in f:
                total_lines += 1
                if total_lines % 500000 == 0:
                    print(f"  ... {total_lines:,} lines")
                p = _pipe(line)
                if not p:
                    continue
                src = p[2].strip() if len(p) > 2 else ""
                tid = p[5].strip() if len(p) > 5 else ""

                if "access_recorder" in src:
                    r = _access(p)
                    if r and _time_ok(r["ts"]):
                        wa.append(r)
                    stats["wa_lines"] += 1
                elif tid:
                    try:
                        ts = _ts(p[0])
                        if not _time_ok(ts):
                            continue
                        m = re.search(r"\|\s+jingpai\s+\|\s+(.*)", line.strip())
                        msg = m.group(1).strip() if m else (p[7].strip() if len(p) > 7 else "")
                        entry = {"ts": ts, "source": src, "message": msg}
                        wi[tid].append(entry)
                        we[tid].append(entry)
                        stats["wi_lines"] += 1
                    except (ValueError, IndexError):
                        stats["skip_parse"] += 1

    print(f"  SDK: {stats['sdk_lines']} lines -> {len(sdk)} records")
    print(f"  Worker access: {stats['wa_lines']} lines -> {len(wa)} records")
    print(f"  Worker info: {stats['wi_lines']} lines in {len(wi)} traces")
    if stats["skip_parse"]:
        print(f"  Skipped: {stats['skip_parse']} malformed lines")
    return sdk, wa, wi, we


# ---------------------------------------------------------------------------
# Trace building
# ---------------------------------------------------------------------------


def build_traces(sdk, wa, wi, we):
    """Correlate SDK and Worker records by traceId."""
    traces = {}
    for r in sdk:
        traces[r["trace_id"]] = {
            "sdk": r, "sdk_op": r["operation"], "sdk_ms": r["duration_us"] / 1000,
            "sdk_status": r["status"], "sdk_ts": r["ts"], "sdk_size": r["size"],
        }
    for r in wa:
        tid = r["trace_id"]
        if tid not in traces:
            continue
        traces[tid].setdefault("wa_list", []).append(r)
    for tid, entries in wi.items():
        if tid in traces:
            traces[tid]["seg"] = _extract(entries)
    for tid, entries in we.items():
        if tid in traces:
            traces[tid]["err"] = _extract_errors(entries)

    for tid, t in traces.items():
        seg = t.get("seg", {})
        er = t.get("err", {})
        is_get = "GET" in t.get("sdk_op", "")
        is_set = "SET" in t.get("sdk_op", "")
        wal = t.get("wa_list", [])
        t["wa_count"] = len(wal)

        if is_get:
            gets = [w for w in wal if "GET" in w["operation"]]
            t["wa_max_ms"] = (
                max((w["duration_us"] for w in gets), default=0) / 1000
                if gets else None
            )
        elif is_set:
            creates = [w for w in wal if "CREATE" in w["operation"]]
            publishes = [w for w in wal if "PUBLISH" in w["operation"]]
            t["create_ms"] = round(max((w["duration_us"] for w in creates), default=0) / 1000, 3) if creates else 0
            t["publish_max_ms"] = round(max((w["duration_us"] for w in publishes), default=0) / 1000, 3) if publishes else 0
            t["wa_max_ms"] = (t["create_ms"] or 0) + (t["publish_max_ms"] or 0)

        # Compute request/response delay using timestamps
        if wal:
            sdk_end = t["sdk_ts"]
            sdk_start = sdk_end - timedelta(microseconds=t["sdk"]["duration_us"])
            wa_starts = []
            wa_ends = []
            for w in wal:
                w_end = w["ts"]
                w_start = w_end - timedelta(microseconds=w["duration_us"])
                wa_starts.append(w_start)
                wa_ends.append(w_end)
            t["req_delay_ms"] = round(
                max((min(wa_starts) - sdk_start).total_seconds() * 1000, 0), 3
            )
            if is_get:
                # GET: single operation, timestamp-based rsp_delay balances exactly
                t["rsp_delay_ms"] = round(
                    max((sdk_end - max(wa_ends)).total_seconds() * 1000, 0), 3
                )
            elif is_set:
                # SET: create+publish may have gap, use residual for balance
                t["rsp_delay_ms"] = round(
                    max(t["sdk_ms"] - t["req_delay_ms"]
                        - (t.get("wa_max_ms") or 0), 0), 3
                )
        else:
            t["req_delay_ms"] = None
            t["rsp_delay_ms"] = None

        if is_get:
            _fill_get(t, seg, er)
        elif is_set:
            _fill_set(t, seg, er)

        t["retry_summary"] = _retry_tag(er, is_get)

    return traces


def _fill_get(t, seg, er):
    # Prefer exceed breakdown (integer ms), fall back to master_q_ms (float ms)
    qm_exceed = seg.get("x_Worker to master rpc QueryMeta")
    qm_master = seg.get("master_q_ms")
    t["query_meta_ms"] = qm_exceed if qm_exceed is not None else qm_master
    mp = seg.get("master_proc_us")
    t["master_proc_ms"] = round(mp / 1000, 3) if mp else None
    t["query_meta_net_ms"] = (
        round(max((t["query_meta_ms"] or 0) - (t["master_proc_ms"] or 0), 0), 3)
        if t["query_meta_ms"] and mp else None
    )

    t["remote_done_max_ms"] = seg.get("rdone_max")
    t["remote_pull_max_ms"] = seg.get("rpull_max")
    rd = seg.get("rdone_max")
    rp = seg.get("rpull_max")
    t["remote_rpc_net_ms"] = round(max(rd - rp, 0), 3) if rd and rp else None
    t["urma_ms"] = seg.get("urma_max")
    ur = seg.get("urma_max")
    if rp and ur:
        t["remote_self_ms"] = round(max(rp - ur, 0), 3)
    elif rp:
        t["remote_self_ms"] = round(rp, 3)
    else:
        t["remote_self_ms"] = None

    t["worker_cost_ms"] = seg.get("get_costs_max")
    t["queue_wait_ms"] = seg.get("get_recv_elapsed_max")
    wc = seg.get("get_costs_max")
    qw = seg.get("get_recv_elapsed_max") or 0
    qm = t.get("query_meta_ms") or 0
    rd2 = seg.get("rdone_max") or 0
    t["other_worker_ms"] = round(max(wc - qm - rd2 - qw, 0), 3) if wc else None

    t["zmq_e2e_us"] = seg.get("zmq_e2e")
    t["zmq_sreqq_us"] = seg.get("zmq_sreqq")
    t["zmq_sexec_us"] = seg.get("zmq_sexec")
    t["zmq_netres_us"] = seg.get("zmq_netres")

    crfw = seg.get("zmq_client_req_fw")
    sreqq = seg.get("zmq_sreqq")
    netres = seg.get("zmq_netres")
    t["client_queuing_ms"] = round(crfw / 1000, 3) if crfw else None
    t["server_queue_wait_ms"] = round(sreqq / 1000, 3) if sreqq else None
    t["network_ms"] = round(netres / 1000, 3) if netres else None


def _fill_set(t, seg, er):
    t["create_meta_ms"] = seg.get("x_Worker to master rpc CreateMeta")
    mp = seg.get("master_proc_us")
    t["master_proc_ms"] = round(mp / 1000, 3) if mp else None
    t["create_meta_net_ms"] = (
        round(max((t["create_meta_ms"] or 0) - (t["master_proc_ms"] or 0), 0), 3)
        if t["create_meta_ms"] and mp else None
    )

    t["publish_cost_ms"] = seg.get("pub_costs_max")
    pc = seg.get("pub_costs_max")
    cm = t.get("create_meta_ms") or 0
    t["other_publish_ms"] = round(max(pc - cm, 0), 3) if pc else None

    t["zmq_e2e_us"] = seg.get("zmq_e2e")
    t["zmq_sexec_us"] = seg.get("zmq_sexec")

    crfw = seg.get("zmq_client_req_fw")
    sreqq = seg.get("zmq_sreqq")
    netres = seg.get("zmq_netres")
    t["client_queuing_ms"] = round(crfw / 1000, 3) if crfw else None
    t["server_queue_wait_ms"] = round(sreqq / 1000, 3) if sreqq else None
    t["network_ms"] = round(netres / 1000, 3) if netres else None


def _retry_tag(er, is_get):
    if not er:
        return ""
    parts = []
    wa = er.get("wa_cnt", 0)
    if wa > 1:
        parts.append(f"{wa}次attempt")
    if is_get:
        done = er.get("get_done_cnt", 0)
        if done > 1:
            parts.append(f"{done}次GetDone")
        mf = er.get("master_fail", 0)
        if mf:
            parts.append(f"QueryMeta失败x{mf}")
        rf = er.get("remote_fail", 0)
        if rf:
            parts.append(f"RemoteGet失败x{rf}")
        bf = er.get("batch_fail", 0)
        if bf:
            parts.append(f"BatchFailx{bf}")
        rmf = er.get("remove_fail", 0)
        if rmf:
            parts.append(f"RemoveMetax{rmf}")
        cd = er.get("cleanup_delete", 0)
        if cd:
            parts.append(f"清理unackedx{cd}")
        retry = er.get("rpc_retry", 0)
        if retry:
            parts.append(f"RPCRetryx{retry}")
        if er.get("ret_timeout"):
            parts.append("Worker超时返回")
        if er.get("cant_find"):
            parts.append("Can't find object")
    else:
        pf = er.get("pub_fail", 0)
        if pf:
            parts.append(f"Publish失败x{pf}")
        mf = er.get("master_fail", 0)
        if mf:
            parts.append(f"Master失败x{mf}")
    errs = er.get("errors", [])
    for e in errs:
        if e not in parts:
            parts.append(e)
    return " | ".join(parts) if parts else ""


# ---------------------------------------------------------------------------
# Statistics helpers
# ---------------------------------------------------------------------------


def _running_pct(data, p):
    r, b = [], []
    for v in data:
        b.append(v)
        s = sorted(b)
        r.append(s[min(int(len(s) * p), len(s) - 1)])
    return r


def _running_max(data):
    m = -float("inf")
    for v in data:
        if v is not None and v > m:
            m = v
        yield m if m != -float("inf") else None


def _overall_pct(d):
    if not d:
        return {}
    s = sorted(d)
    return {
        "count": len(s), "avg": sum(s) / len(s),
        "p50": s[int(len(s) * .5)], "p90": s[int(len(s) * .9)],
        "p99": s[int(len(s) * .99)],
        "p9999": s[min(int(len(s) * .9999), len(s) - 1)],
        "max": s[-1], "min": s[0],
    }


def _seg_stat(name, data, key):
    vals = [t[key] for t in data if t.get(key) is not None]
    if not vals:
        return None
    sv = sorted(vals)
    return {
        "name": name, "count": len(vals), "avg": sum(vals) / len(vals),
        "p99": sv[min(int(len(sv) * .99), len(sv) - 1)], "max": sv[-1],
    }


_E = html.escape


# ---------------------------------------------------------------------------
# Other Worker / Other Publish tooltip builder
# ---------------------------------------------------------------------------


def _other_worker_tip(er, seg=None):
    if not er and not seg:
        return ""
    p = []
    if seg:
        gc = seg.get("get_costs", [])
        if len(gc) > 1:
            p.append(f"{len(gc)}次attempt costs=[{','.join(f'{g:.1f}' for g in gc)}]ms")
    if er:
        if er.get("rpc_retry"):
            p.append(f"RPCRetryx{er['rpc_retry']}")
        if er.get("rpc_timeout_wait"):
            p.append(f"RPCTimeoutWaitx{er['rpc_timeout_wait']}")
        if er.get("ret_timeout"):
            p.append("Worker超时")
        if er.get("master_fail"):
            p.append(f"QueryMeta重试x{er['master_fail']}")
        if er.get("remote_fail"):
            p.append(f"RemoteGet失败x{er['remote_fail']}")
        if er.get("batch_fail"):
            p.append(f"BatchFailx{er['batch_fail']}")
        if er.get("local_lookup"):
            p.append(f"LocalLookupx{er['local_lookup']}")
        if er.get("cant_find"):
            p.append("Obj未找到")
        if er.get("cleanup_delete"):
            p.append(f"清理unackedx{er['cleanup_delete']}")
        if er.get("remove_fail"):
            p.append(f"RemoveMetax{er['remove_fail']}")
    return " | ".join(p) if p else ""


def _other_publish_tip(er, seg):
    p = []
    pc = seg.get("pub_costs_max")
    cm = seg.get("x_Worker to master rpc CreateMeta") or 0
    if pc:
        p.append(f"PublishCost={pc:.1f}ms")
    if cm:
        p.append(f"CreateMeta={cm:.1f}ms")
    if er.get("pub_fail"):
        p.append(f"Publish失败x{er['pub_fail']}")
    if er.get("master_fail"):
        p.append(f"Master失败x{er['master_fail']}")
    if er.get("rpc_retry"):
        p.append(f"RPCRetryx{er['rpc_retry']}")
    if er.get("cleanup_delete"):
        p.append(f"清理x{er['cleanup_delete']}")
    return " | ".join(p) if p else ""


# ---------------------------------------------------------------------------
# Report generation
# ---------------------------------------------------------------------------


def generate_report(sdk_path, worker_path, output_path, since=None, until=None, top_n=50):
    print(f"Parsing SDK: {sdk_path}")
    print(f"Parsing Worker: {worker_path}")
    sdk_r, wa_r, wi, we = parse_logs(sdk_path, worker_path, since, until)
    traces = build_traces(sdk_r, wa_r, wi, we)
    print(f"Traces: {len(traces)} total (SDK:{len(sdk_r)} WorkerAccess:{len(wa_r)})")

    if not traces:
        print("No correlated traces found between SDK and Worker logs", file=sys.stderr)
        print("Ensure SDK and Worker logs overlap in time range", file=sys.stderr)
        sys.exit(1)

    gets = sorted(
        [t for t in traces.values() if "GET" in t.get("sdk_op", "")],
        key=lambda x: x["sdk_ts"],
    )
    sets = sorted(
        [t for t in traces.values() if "SET" in t.get("sdk_op", "")],
        key=lambda x: x["sdk_ts"],
    )
    print(f"  GET: {len(gets)}, SET: {len(sets)}")

    # -- GET chart arrays --
    g_lbl = [t["sdk_ts"].strftime("%H:%M:%S.%f")[:11] for t in gets]
    g_sdk = [t["sdk_ms"] for t in gets]
    g_st = [t["sdk_status"] for t in gets]
    g_crpc = [t.get("req_delay_ms") or 0 for t in gets]
    g_rsp = [t.get("rsp_delay_ms") or 0 for t in gets]
    # Req Delay decomposition: ZMQ ticks when available, else raw req_delay
    # When ZMQ data exists: req_delay=0, show 3 sub-bars; otherwise: show raw req_delay
    g_cliq = [t.get("client_queuing_ms") or 0 for t in gets]
    g_net = [t.get("network_ms") or 0 for t in gets]
    g_sqw = [t.get("server_queue_wait_ms") or 0 for t in gets]
    g_has_zmq = [t.get("client_queuing_ms") is not None for t in gets]
    # Adjust: when ZMQ data exists, zero out the raw req_delay to avoid double-counting
    g_crpc_adj = [
        0 if t.get("client_queuing_ms") is not None else (t.get("req_delay_ms") or 0)
        for t in gets
    ]
    g_qw = [t.get("queue_wait_ms") or 0 for t in gets]
    g_qmn = [
        t.get("query_meta_net_ms") if t.get("query_meta_net_ms") is not None else 0
        for t in gets
    ]
    g_mp = [t.get("master_proc_ms") or 0 for t in gets]
    g_rn = [
        t.get("remote_rpc_net_ms") if t.get("remote_rpc_net_ms") is not None else 0
        for t in gets
    ]
    g_rs = [t.get("remote_self_ms") or 0 for t in gets]
    g_ur = [t.get("urma_ms") or 0 for t in gets]
    g_oth = [t.get("other_worker_ms") or 0 for t in gets]
    g_oth_tip = [_other_worker_tip(t.get("err", {}), t.get("seg", {})) for t in gets]

    g_p50 = _running_pct(g_sdk, .5)
    g_p99 = _running_pct(g_sdk, .99)
    g_rm = list(_running_max(g_sdk))
    get_st = _overall_pct(g_sdk)

    # -- SET chart arrays --
    s_lbl = [t["sdk_ts"].strftime("%H:%M:%S.%f")[:11] for t in sets]
    s_sdk = [t["sdk_ms"] for t in sets]
    s_st = [t["sdk_status"] for t in sets]
    s_crpc = [t.get("req_delay_ms") or 0 for t in sets]
    s_rsp = [t.get("rsp_delay_ms") or 0 for t in sets]
    s_cliq = [t.get("client_queuing_ms") or 0 for t in sets]
    s_net = [t.get("network_ms") or 0 for t in sets]
    s_sqw = [t.get("server_queue_wait_ms") or 0 for t in sets]
    s_has_zmq = [t.get("client_queuing_ms") is not None for t in sets]
    s_crpc_adj = [
        0 if t.get("client_queuing_ms") is not None else (t.get("req_delay_ms") or 0)
        for t in sets
    ]
    s_cr = [t.get("create_ms") or 0 for t in sets]
    s_cmn = [
        t.get("create_meta_net_ms") if t.get("create_meta_net_ms") is not None else 0
        for t in sets
    ]
    s_mp = [t.get("master_proc_ms") or 0 for t in sets]
    s_opub = [t.get("other_publish_ms") or 0 for t in sets]
    s_opub_tip = [_other_publish_tip(t.get("err", {}), t.get("seg", {})) for t in sets]
    set_st = _overall_pct(s_sdk)

    # -- Segmentation summary --
    g_segs = [s for s in [
        _seg_stat("Request Delay (SDK→Worker)", gets, "req_delay_ms"),
        _seg_stat("  Client Queuing (SDK ZMQ框架)", gets, "client_queuing_ms"),
        _seg_stat("  Network (网络传输残差)", gets, "network_ms"),
        _seg_stat("  Server Queue Wait (Worker线程池排队)", gets, "server_queue_wait_ms"),
        _seg_stat("Response Delay (Worker→SDK)", gets, "rsp_delay_ms"),
        _seg_stat("Queue Wait (线程池排队)", gets, "queue_wait_ms"),
        _seg_stat("QueryMeta: RPC网络", gets, "query_meta_net_ms"),
        _seg_stat("QueryMeta: Master处理", gets, "master_proc_ms"),
        _seg_stat("QueryMeta: 总计", gets, "query_meta_ms"),
        _seg_stat("Remote Get: RPC网络", gets, "remote_rpc_net_ms"),
        _seg_stat("Remote Get: 远端Worker自身", gets, "remote_self_ms"),
        _seg_stat("Remote Get: URMA Write", gets, "urma_ms"),
        _seg_stat("Remote Get: e2e总计", gets, "remote_done_max_ms"),
        _seg_stat("Other Worker (RPC重试+错误清理)", gets, "other_worker_ms"),
    ] if s]
    s_segs = [s for s in [
        _seg_stat("Request Delay (SDK→Worker)", sets, "req_delay_ms"),
        _seg_stat("  Client Queuing (SDK ZMQ框架)", sets, "client_queuing_ms"),
        _seg_stat("  Network (网络传输残差)", sets, "network_ms"),
        _seg_stat("  Server Queue Wait (Worker线程池排队)", sets, "server_queue_wait_ms"),
        _seg_stat("Response Delay (Worker→SDK)", sets, "rsp_delay_ms"),
        _seg_stat("Worker Create (本地OC)", sets, "create_ms"),
        _seg_stat("CreateMeta: RPC网络", sets, "create_meta_net_ms"),
        _seg_stat("CreateMeta: Master处理", sets, "master_proc_ms"),
        _seg_stat("CreateMeta: 总计", sets, "create_meta_ms"),
        _seg_stat("Other Publish", sets, "other_publish_ms"),
    ] if s]

    flat = sorted(
        [t for t in traces.values() if "sdk" in t],
        key=lambda x: x["sdk_ms"],
        reverse=True,
    )

    _write_html(output_path, {
        "get_st": get_st, "set_st": set_st, "top_n": top_n,
        "g_lbl": json.dumps(g_lbl), "g_sdk": json.dumps(g_sdk),
        "g_st": json.dumps(g_st), "g_crpc": json.dumps(g_crpc_adj),
        "g_rsp": json.dumps(g_rsp),
        "g_qw": json.dumps(g_qw),
        "g_cliq": json.dumps(g_cliq), "g_net": json.dumps(g_net),
        "g_sqw": json.dumps(g_sqw), "g_has_zmq": json.dumps(g_has_zmq),
        "g_qmn": json.dumps(g_qmn), "g_mp": json.dumps(g_mp),
        "g_rn": json.dumps(g_rn), "g_rs": json.dumps(g_rs),
        "g_ur": json.dumps(g_ur), "g_oth": json.dumps(g_oth),
        "g_oth_tip": json.dumps(g_oth_tip, ensure_ascii=False),
        "g_p50": json.dumps(g_p50), "g_p99": json.dumps(g_p99),
        "g_rm": json.dumps(g_rm),
        "s_lbl": json.dumps(s_lbl), "s_sdk": json.dumps(s_sdk),
        "s_st": json.dumps(s_st), "s_crpc": json.dumps(s_crpc_adj),
        "s_rsp": json.dumps(s_rsp),
        "s_cliq": json.dumps(s_cliq), "s_net": json.dumps(s_net),
        "s_sqw": json.dumps(s_sqw), "s_has_zmq": json.dumps(s_has_zmq),
        "s_cr": json.dumps(s_cr), "s_cmn": json.dumps(s_cmn),
        "s_mp": json.dumps(s_mp), "s_opub": json.dumps(s_opub),
        "s_opub_tip": json.dumps(s_opub_tip, ensure_ascii=False),
        "g_segs": g_segs, "s_segs": s_segs, "flat": flat,
        "total": len(traces), "sdk_n": len(sdk_r), "wa_n": len(wa_r),
    })


# ---------------------------------------------------------------------------
# HTML generation
# ---------------------------------------------------------------------------


def _write_html(path, D):
    gs, ss, top_n = D["get_st"], D["set_st"], D["top_n"]

    def card(label, s, color):
        if not s:
            return ""
        items = "".join(
            f'<div class="sv">{s[k]:.2f}<span class="sl">{l2}</span></div>'
            for k, l2 in [("count", "Count"), ("avg", "Avg"), ("p50", "P50"),
                          ("p90", "P90"), ("p99", "P99"), ("p9999", "P99.99"),
                          ("max", "Max"), ("min", "Min")]
        )
        return f'<div class="card"><h3 style="color:{color}">{label}</h3><div class="sg">{items}</div></div>'

    def seg_table(segs):
        return "".join(
            f'<tr><td style="white-space:nowrap">{_E(s["name"])}</td><td class="n">{s["count"]}</td>'
            f'<td class="n">{s["avg"]:.3f}</td><td class="n">{s["p99"]:.3f}</td>'
            f'<td class="n">{s["max"]:.3f}</td></tr>'
            for s in segs
        )

    # -- Top N table rows --
    top_rows = ""
    for i, t in enumerate(D["flat"][:top_n]):
        bg = "#ffebee" if t["sdk_status"] else "#e8f5e9"
        tag = (f'<span class="er">{t["sdk_status"]}</span>'
               if t["sdk_status"] else '<span class="ok">0</span>')
        is_get = "GET" in t.get("sdk_op", "")
        req_d = f'{t["req_delay_ms"]}' if t.get("req_delay_ms") is not None else "-"
        rsp_d = f'{t["rsp_delay_ms"]}' if t.get("rsp_delay_ms") is not None else "-"
        # Req Delay decomposition columns
        cq = t.get("client_queuing_ms")
        nt = t.get("network_ms")
        sq = t.get("server_queue_wait_ms")
        if cq is not None:
            rq_cols = f'<td class="n">{cq:.2f}</td><td class="n">{nt:.2f}</td><td class="n">{sq:.2f}</td>'
        else:
            rq_cols = '<td class="n">-</td><td class="n">-</td><td class="n">-</td>'

        if is_get:
            qw_val = t.get("queue_wait_ms")
            qw_show = f'{qw_val:.1f}' if qw_val else '-'
            qmn = t.get("query_meta_net_ms")
            mp = t.get("master_proc_ms")
            qm = t.get("query_meta_ms")
            qm_show = f'{qmn:.1f}' if qmn is not None else (f'{qm}' if qm else '-')
            mp_show = f'{mp:.2f}' if mp else '-'
            rn = t.get("remote_rpc_net_ms")
            rd = t.get("remote_done_max_ms")
            rn_show = f'{rn:.1f}' if rn is not None else (f'{rd:.1f}' if rd else '-')
            rs = t.get("remote_self_ms") or '-'
            ur = t.get("urma_ms") or '-'
            ow_val = t.get("other_worker_ms")
            ow_tip = _other_worker_tip(t.get("err", {}), t.get("seg", {}))
            if ow_tip:
                ow_show = f'{ow_val:.1f}ms<br><span style="font-size:10px;color:#666">{_E(ow_tip)}</span>' if ow_val is not None else '-'
            elif ow_val is not None:
                ow_show = f'{ow_val:.1f}ms'
            else:
                ow_show = '-'
            seg_cols = (
                f'<td class="n">{qw_show}</td><td class="n">{qm_show}</td><td class="n">{mp_show}</td>'
                f'<td class="n">{rn_show}</td><td class="n">{rs}</td>'
                f'<td class="n">{ur}</td><td class="nd">{ow_show}</td>'
            )
        else:
            cr = t.get("create_ms") or '-'
            cmn = t.get("create_meta_net_ms")
            cmp_ = t.get("master_proc_ms")
            cm = t.get("create_meta_ms")
            cmn_show = f'{cmn:.1f}' if cmn is not None else (f'{cm}' if cm else '-')
            cmp_show = f'{cmp_:.2f}' if cmp_ else '-'
            op = t.get("other_publish_ms") or '-'
            seg_cols = (
                f'<td class="n">{cr}</td><td class="n">{cmn_show}</td>'
                f'<td class="n">{cmp_show}</td><td class="n">-</td>'
                f'<td class="n">-</td><td class="n">{op}</td>'
            )

        retry = t.get("retry_summary", "")
        retry_cell = f'<td class="retry">{_E(retry)}</td>' if retry else '<td>-</td>'

        top_rows += (
            f'<tr style="background:{bg}">'
            f'<td>{i+1}</td>'
            f'<td class="tid" title="{_E(t["sdk"]["trace_id"])}">{_E(t["sdk"]["trace_id"][:16])}...</td>'
            f'<td>{t["sdk_ts"].strftime("%H:%M:%S.%f")[:11]}</td>'
            f'<td class="nb">{t["sdk_ms"]:.3f}</td>'
            f'<td class="n">{req_d}</td>{rq_cols}<td class="n">{rsp_d}</td>{seg_cols}{retry_cell}<td>{tag}</td></tr>'
        )

    # -- Section diagnostic guides (collapsible help panels) --
    _G1 = '''<details class="guide"><summary>诊断指南 — Summary 关键词与阈值</summary>
<div class="guide-body">
<h4>关键词</h4>
<ul>
<li><span class="tag">Count</span> 时间范围内的请求总数</li>
<li><span class="tag">Avg</span> 平均延迟, 所有请求的算术平均</li>
<li><span class="tag">P50/P90/P99/P99.99</span> 百分位延迟, 分别表示 50%/90%/99%/99.99% 的请求在此值以下</li>
<li><span class="tag">Max/Min</span> 最大/最小延迟</li>
</ul>
<h4>正常值参考</h4>
<table>
<tr><th>指标</th><th>GET 正常</th><th>SET 正常</th><th>需关注</th></tr>
<tr><td>Avg</td><td class="good">&lt; 3ms</td><td class="good">&lt; 5ms</td><td class="warn">&gt; 10ms</td></tr>
<tr><td>P99</td><td class="good">&lt; 10ms</td><td class="good">&lt; 15ms</td><td class="warn">&gt; 20ms</td></tr>
<tr><td>P99.99</td><td class="good">&lt; 20ms</td><td class="good">&lt; 30ms</td><td class="warn">接近 RPC timeout(~20ms)</td></tr>
</table>
<h4>异常信号</h4>
<ul>
<li><b>P99 持续 &gt; 20ms</b>: 系统性瓶颈, 需排查瓶颈阶段</li>
<li><b>Max 远大于 P99.99</b>: 个别尖刺, 可能是超时/重试导致</li>
<li><b>GET 和 SET 同时异常</b>: 共同瓶颈 (网络 / Worker 线程池)</li>
</ul>
<div class="next"><b>下一步:</b> P99 高 &rarr; 看 &sect;3/&sect;6 分段图定位瓶颈阶段; Max 异常 &rarr; 看 &sect;8 TopN 定位具体 trace</div>
</div></details>'''

    _G2 = '''<details class="guide"><summary>诊断指南 — GET 延迟趋势</summary>
<div class="guide-body">
<h4>关键词</h4>
<ul>
<li><span class="tag">绿色柱</span> SDK status=0, 请求成功</li>
<li><span class="tag">红色柱</span> SDK status&ne;0, 请求失败 (常见 1002=RPC 超时)</li>
<li><span class="tag">P50 线 (蓝实)</span> 实时 P50, 反映多数请求的延迟水平</li>
<li><span class="tag">P99 线 (黄虚)</span> 实时 P99, 反映尾部延迟趋势</li>
<li><span class="tag">Max 线 (红实)</span> 实时最大值, 追踪尖刺</li>
</ul>
<h4>正常 vs 异常</h4>
<table>
<tr><th>模式</th><th>正常</th><th>异常</th></tr>
<tr><td>柱体分布</td><td class="good">集中在 &lt; 5ms</td><td class="warn">散布到 10ms+ 或大量红柱</td></tr>
<tr><td>P50 趋势</td><td class="good">平稳 &lt; 1ms</td><td class="warn">持续上升</td></tr>
<tr><td>P99 趋势</td><td class="good">平稳 &lt; 10ms</td><td class="warn">突增或持续上升</td></tr>
<tr><td>红色柱比例</td><td class="good">&lt; 0.1%</td><td class="warn">&gt; 1% (Worker 不可达或过载)</td></tr>
</table>
<h4>常见异常模式</h4>
<ul>
<li><b>延迟聚集在 ~20ms</b>: SDK RPC timeout, Worker 响应不及时</li>
<li><b>延迟聚集在 ~8-12ms 且 size 较大</b>: SHM 大对象拷贝, 属于正常物理开销</li>
<li><b>延迟周期性波动</b>: 可能与 GC/后台任务/扩缩容相关</li>
</ul>
<div class="next"><b>下一步:</b> 红柱多 &rarr; &sect;8 查异常信息; 高延迟 &rarr; &sect;3 看分段找瓶颈阶段; 集中在 20ms &rarr; 检查 Worker 线程池(resource report)</div>
</div></details>'''

    _G3 = '''<details class="guide"><summary>诊断指南 — GET 分段含义与阈值</summary>
<div class="guide-body">
<h4>各阶段含义</h4>
<table>
<tr><th>阶段</th><th>含义</th><th>正常</th><th>需关注</th></tr>
<tr><td><b>Req Delay</b></td><td>SDK 发出 &rarr; Worker 开始处理 (无 ZMQ 明细时的整体值)</td><td class="good">&lt; 1ms(本地) &lt; 3ms(跨节点)</td><td class="warn">&gt; 5ms: 线程池饱和或网络延迟</td></tr>
<tr><td><b>Client Queuing</b></td><td>SDK ZMQ 框架开销: 序列化+发送队列+Stub发送 (client_req_framework_us)</td><td class="good">&lt; 0.1ms</td><td class="warn">&gt; 1ms: SDK 发送队列积压 (ZMQ HWM 水位)</td></tr>
<tr><td><b>Network</b></td><td>网络传输残差: ZMQ socket 发送 &rarr; Worker proxy 收到 (network_residual_us)</td><td class="good">&lt; 0.5ms</td><td class="warn">&gt; 2ms: 网络拥塞或跨子网</td></tr>
<tr><td><b>Server Queue Wait</b></td><td>Worker 线程池排队: proxy 收到 &rarr; 线程取出 (server_req_queue_us)</td><td class="good">&lt; 0.5ms</td><td class="warn">&gt; 2ms: Worker 线程池饱和</td></tr>
<tr><td><b>Queue Wait</b></td><td>Worker 接收请求 &rarr; 线程池开始执行的排队等待</td><td class="good">&lt; 0.5ms</td><td class="warn">&gt; 2ms: 线程池严重积压</td></tr>
<tr><td><b>QMeta Net</b></td><td>Worker&rarr;Master QueryMeta RPC 网络耗时 (= query_meta_ms - master_proc_ms)</td><td class="good">&lt; 0.5ms</td><td class="warn">&gt; 2ms: 见下方详细诊断</td></tr>
<tr><td><b>Master Proc</b></td><td>Master 处理 QueryMeta 耗时 (req_queue+exec+rsp_queue)</td><td class="good">&lt; 0.5ms</td><td class="warn">&gt; 3ms: Master 瓶颈 (锁竞争/etcd 慢/内存压力)</td></tr>
<tr><td><b>Remote Net</b></td><td>Worker&rarr;远端 Worker 获取数据的 RPC 网络</td><td class="good">&lt; 1ms</td><td class="warn">&gt; 3ms: 跨节点网络问题</td></tr>
<tr><td><b>Remote Self</b></td><td>远端 Worker 本地处理 (不含 URMA)</td><td class="good">&lt; 3ms</td><td class="warn">&gt; 10ms: 远端负载高 / OC Miss</td></tr>
<tr><td><b>URMA</b></td><td>URMA 远程写耗时 (RDMA/UB)</td><td class="good">&lt; 2ms</td><td class="warn">&gt; 5ms: RDMA/UB 链路异常</td></tr>
<tr><td><b>Other Worker</b></td><td>无法归入以上阶段的时间: RPC 重试/错误清理/多次 attempt</td><td class="good">&lt; 1ms</td><td class="warn">&gt; 5ms: 存在重试或错误 (hover 看子项)</td></tr>
<tr><td><b>Rsp Delay</b></td><td>Worker 完成 &rarr; SDK 收到响应</td><td class="good">&lt; 1ms</td><td class="warn">&gt; 3ms: SDK 侧 RPC 响应处理慢</td></tr>
</table>
<h4>Other Worker hover 子项说明</h4>
<ul>
<li><span class="tag">RPCRetry</span> RPC 内部重试次数, 常见原因: 网络抖动 / 对端繁忙</li>
<li><span class="tag">RPCTimeoutWait</span> 等待 RPC 超时返回的耗时</li>
<li><span class="tag">QueryMeta 重试</span> Master 请求失败后重试</li>
<li><span class="tag">RemoteGet 失败</span> 远端 Worker 获取数据失败</li>
<li><span class="tag">LocalLookup</span> 本地 OC 查找次数</li>
<li><span class="tag">清理 unacked</span> 清理未确认的 Publish 记录</li>
<li><span class="tag">N 次 attempt</span> 单个 SDK 请求触发了多次 Worker attempt (重试导致)</li>
</ul>
<h4>QMeta Net 详细诊断 (Worker&rarr;Master QueryMeta 网络耗时)</h4>
<p style="color:#666;font-size:11px">QMeta Net = query_meta_ms (Worker发出RPC&rarr;收到响应的总时间) - master_proc_ms (Master端 ZMQ处理: 排队+执行+响应排队)</p>
<table>
<tr><th>模式</th><th>query_meta_ms</th><th>master_proc_ms</th><th>QMeta Net</th><th>根因</th></tr>
<tr><td>网络延迟高</td><td class="n">5-15ms</td><td class="n">&lt; 1ms</td><td class="n">&asymp; query_meta_ms</td><td>Worker 和 Master 跨机/跨AZ部署, 网络 RTT 高</td></tr>
<tr><td>无 ZMQ SLOW 日志</td><td class="n">5-15ms</td><td class="n">null</td><td class="n">= query_meta_ms</td><td><span class="er">master_proc_ms 未采集</span>, QMeta Net 被高估为全部值</td></tr>
<tr><td>Master 锁竞争</td><td class="n">5-10ms</td><td class="n">3-8ms</td><td class="n">正常</td><td>metaTableMutex_ 写锁阻塞读 (Publish 占写锁)</td></tr>
<tr><td>重试放大</td><td class="n">20ms+</td><td class="n">&lt; 1ms</td><td class="n">20ms+</td><td>QueryMeta RPC 失败后 RetryOnErrorRepent 重试, 耗时叠加</td></tr>
<tr><td>大 payload</td><td class="n">10-30ms</td><td class="n">5-20ms</td><td class="n">小</td><td>单次 QueryMeta 查询大量 key, protobuf 序列化耗时</td></tr>
</table>
<h5>定位步骤</h5>
<ol>
<li><b>确认 master_proc_ms 是否有值</b>: Summary 表或 TopN 表中看 Master Proc 列 &mdash; 空值/0 表示无 ZMQ_RPC_FRAMEWORK_SLOW 日志, QMeta Net 被高估</li>
<li><b>查 Master 日志锁竞争</b>: <code>grep "QueryMeta get lock" &lt;master_log&gt; | grep -v "0 ms"</code> 非零值表示 metaTableMutex_ 竞争, 根因通常是并发 Publish 持写锁</li>
<li><b>查重试</b>: TopN 表异常信息列出现 <span class="tag">QueryMeta失败xN</span> 表示有重试, 检查 Master 日志中的 K_TRY_AGAIN 错误</li>
<li><b>查网络 RTT</b>: <code>ping -c 10 &lt;master_ip&gt;</code> 同机房应 &lt; 0.5ms, &gt; 2ms 则跨子网/跨 AZ</li>
<li><b>查 key 数量</b>: access log 中 QueryMeta 的 ids 数量, 批量 Get 100+ key 时序列化耗时会增加</li>
</ol>
<h5>相关调优参数</h5>
<table>
<tr><th>参数</th><th>默认值</th><th>作用</th></tr>
<tr><td><code>rpc_thread_num</code> (Master)</td><td>16</td><td>Master RPC 线程池, 影响 server_req_queue_us</td></tr>
<tr><td><code>enable_redirect</code></td><td>true</td><td>开启 redirect 可减少扩缩容时的 QueryMeta 调用</td></tr>
<tr><td><code>requestTimeoutMs</code> (ConnectOptions)</td><td>0</td><td>SDK 请求超时, 影响 QueryMeta 重试的剩余时间</td></tr>
</table>
<h4>定位瓶颈决策树</h4>
<ul>
<li>Req Delay + Rsp Delay 同时高 &rarr; <b>网络问题</b> (检查 RDMA/UB 链路质量)</li>
<li>仅 Req Delay 高 &rarr; <b>Worker 线程池饱和</b> (检查 resource report 的线程池利用率)</li>
<li>QMeta Net 高 + Master Proc 低 &rarr; <b>网络延迟或 ZMQ 日志缺失</b> (见上方 QMeta Net 详细诊断)</li>
<li>QMeta Net 高 + Master Proc 高 &rarr; <b>Master 锁竞争</b> (grep "QueryMeta get lock" 检查非零值)</li>
<li>Master Proc 高 + QMeta Net 低 &rarr; <b>Master 处理瓶颈</b> (检查 etcd 延迟 / Master 内存)</li>
<li>Remote Self 高 &rarr; <b>远端 Worker 负载高</b> (检查远端 Worker 资源指标)</li>
<li>Other Worker 高 &rarr; <b>RPC 不稳定</b> (hover 看具体重试原因)</li>
<li>所有阶段都很低但 SDK 总延迟高 &rarr; <b>SDK 侧问题</b> (检查 async_release_buffer 线程池)</li>
</ul>
<div class="next"><b>下一步:</b> 找到最高的色块 &rarr; 对应上表的阈值 &rarr; 按决策树定位根因。QMeta Net 高时先确认 Master Proc 是否有值。hover Other Worker 色块查看详细子项。</div>
</div></details>'''

    _G4 = '''<details class="guide"><summary>诊断指南 — GET 分段统计表</summary>
<div class="guide-body">
<h4>关键词</h4>
<ul>
<li><span class="tag">Cnt</span> 有该阶段数据的 trace 数量 (小于总数 = 部分缺少日志)</li>
<li><span class="tag">Avg</span> 该阶段在所有有效 trace 中的平均值</li>
<li><span class="tag">P99</span> 该阶段的 99 分位值</li>
<li><span class="tag">Max</span> 该阶段的最大值</li>
</ul>
<h4>快速判断</h4>
<ul>
<li>某阶段 Max &gt;&gt; P99 &rarr; <b>少数尖刺</b>, 看 &sect;8 TopN 定位具体 trace</li>
<li>某阶段 P99 已经很高 &rarr; <b>系统性问题</b>, 需优化该阶段</li>
<li>QueryMeta 总计 P99 &gt; 3ms &rarr; Master 侧需排查</li>
<li>Other Worker Avg &gt; 1ms &rarr; 检查重试率和错误率</li>
<li>Cnt 远小于总数 &rarr; 该阶段仅在有 exceed 3ms 日志时才记录, 属于正常采样偏差</li>
</ul>
<div class="next"><b>下一步:</b> 用 P99 列定位主要瓶颈阶段, 再到 &sect;3 找对应 trace 验证</div>
</div></details>'''

    _G5 = '''<details class="guide"><summary>诊断指南 — SET 延迟趋势</summary>
<div class="guide-body">
<h4>与 GET 的区别</h4>
<ul>
<li>SET 通常比 GET 慢 (需要 Create 本地 OC + Publish 到 Master)</li>
<li>SET 的红色柱表示 Publish 失败或 Master 不可达</li>
<li>SET 延迟聚集在固定值可能是 Create/Publish 的固定开销</li>
</ul>
<h4>正常 vs 异常</h4>
<table>
<tr><th>模式</th><th>正常</th><th>异常</th></tr>
<tr><td>柱体分布</td><td class="good">大多数 &lt; 10ms</td><td class="warn">&gt; 20ms 或大量红柱</td></tr>
<tr><td>红色柱</td><td class="good">&lt; 0.1%</td><td class="warn">&gt; 1% (Master 不可达)</td></tr>
</table>
<div class="next"><b>下一步:</b> 高延迟 &rarr; 看 &sect;6 分段找瓶颈; 红柱 &rarr; &sect;8 看异常信息列</div>
</div></details>'''

    _G6 = '''<details class="guide"><summary>诊断指南 — SET 分段含义与阈值</summary>
<div class="guide-body">
<h4>各阶段含义</h4>
<table>
<tr><th>阶段</th><th>含义</th><th>正常</th><th>需关注</th></tr>
<tr><td><b>Req Delay</b></td><td>SDK 发出 &rarr; Worker 开始处理 (无 ZMQ 明细时的整体值)</td><td class="good">&lt; 1ms(本地) &lt; 3ms(跨节点)</td><td class="warn">&gt; 5ms: 线程池饱和或网络延迟</td></tr>
<tr><td><b>Client Queuing</b></td><td>SDK ZMQ 框架开销 (client_req_framework_us)</td><td class="good">&lt; 0.1ms</td><td class="warn">&gt; 1ms: SDK 发送队列积压</td></tr>
<tr><td><b>Network</b></td><td>网络传输残差 (network_residual_us)</td><td class="good">&lt; 0.5ms</td><td class="warn">&gt; 2ms: 网络拥塞或跨子网</td></tr>
<tr><td><b>Server Queue Wait</b></td><td>Worker 线程池排队 (server_req_queue_us)</td><td class="good">&lt; 0.5ms</td><td class="warn">&gt; 2ms: Worker 线程池饱和</td></tr>
<tr><td><b>Worker Create</b></td><td>Worker 在本地 Object Cache 中创建对象</td><td class="good">&lt; 0.1ms</td><td class="warn">&gt; 1ms: OC 写入慢 (内存不足/锁竞争)</td></tr>
<tr><td><b>CMeta Net</b></td><td>Worker&rarr;Master CreateMeta RPC 网络耗时 (= create_meta_ms - master_proc_ms)</td><td class="good">&lt; 1ms</td><td class="warn">&gt; 3ms: 参见 GET 的 QMeta Net 诊断方法</td></tr>
<tr><td><b>Master Proc</b></td><td>Master 处理 CreateMeta 耗时 (含 metaTableMutex_ 锁等待)</td><td class="good">&lt; 1ms</td><td class="warn">&gt; 5ms: Master 瓶颈 (锁竞争/etcd 慢)</td></tr>
<tr><td><b>Other Publish</b></td><td>Publish 中除 CreateMeta 外的时间: 重试/失败/多次 attempt</td><td class="good">&lt; 1ms</td><td class="warn">&gt; 5ms: Publish 失败重试 (hover 看详情)</td></tr>
<tr><td><b>Rsp Delay</b></td><td>残差 = SDK 总时间 - Req Delay - Worker 处理时间, 可能含 SDK 等待 Publish 确认</td><td class="good">&lt; 2ms</td><td class="warn">&gt; 5ms: SDK 超时或 Publish 等待</td></tr>
</table>
<h4>Other Publish hover 子项</h4>
<ul>
<li><span class="tag">Publish 失败 xN</span> Publish 请求失败次数</li>
<li><span class="tag">Master 失败 xN</span> Master 请求失败次数</li>
<li><span class="tag">RPCRetry xN</span> RPC 内部重试次数</li>
</ul>
<h4>SET 瓶颈定位</h4>
<ul>
<li>Create 高 &rarr; <b>Worker 本地 OC 问题</b> (检查内存 / resource report)</li>
<li>CMeta Net 高 + Master Proc 低 &rarr; <b>网络延迟或 ZMQ 日志缺失</b> (诊断方法同 GET QMeta Net)</li>
<li>CMeta Net 高 + Master Proc 高 &rarr; <b>Master 锁竞争</b> (CreateMeta 写 metaTableMutex_ 占写锁)</li>
<li>Master Proc 高 + CMeta Net 低 &rarr; <b>Master 处理瓶颈</b> (检查 etcd / Master 内存)</li>
<li>Other Publish 高 &rarr; <b>Publish 失败重试</b> (hover 看详情)</li>
<li>Rsp Delay 高 &rarr; <b>SDK 等待</b> (可能 Master 响应慢或 SDK 超时)</li>
</ul>
<div class="next"><b>下一步:</b> 找到最高的色块 &rarr; 对照上表阈值 &rarr; 按定位建议排查</div>
</div></details>'''

    _G7 = '''<details class="guide"><summary>诊断指南 — SET 分段统计表</summary>
<div class="guide-body">
<h4>快速判断</h4>
<ul>
<li>Worker Create 正常值极小 (&lt; 0.1ms), 如果 P99 &gt; 0.5ms 则异常</li>
<li>CreateMeta 总计 P99 &gt; 5ms &rarr; Master 侧需排查</li>
<li>Other Publish P99 &gt; 2ms &rarr; 有重试或失败, 需关注</li>
<li>某阶段 Max &gt;&gt; P99 &rarr; 少数尖刺, 看 &sect;8 TopN</li>
</ul>
<div class="next"><b>下一步:</b> 用 P99 列定位 SET 主要瓶颈, 再到 &sect;6 找对应 trace 验证</div>
</div></details>'''

    _G8 = '''<details class="guide"><summary>诊断指南 — TopN 表格解读</summary>
<div class="guide-body">
<h4>列说明</h4>
<table>
<tr><th>列</th><th>含义</th></tr>
<tr><td><b>TraceId</b></td><td>前 16 位, hover 显示完整 ID, 可用于 grep 日志</td></tr>
<tr><td><b>SDK ms</b></td><td>SDK 端总延迟 (降序, 最慢的排最前)</td></tr>
<tr><td><b>Req/Rsp Delay</b></td><td>请求/响应 RPC 延迟 (无 ZMQ 数据时显示整体值)</td></tr>
<tr><td><b>CliQ / Net / SrvQ</b></td><td>Req Delay 的三段拆解 (仅当有 ZMQ_RPC_FRAMEWORK_SLOW 日志时显示):<br><b>CliQ</b>=Client Queuing (SDK ZMQ框架排队)<br><b>Net</b>=Network (网络传输残差)<br><b>SrvQ</b>=Server Queue Wait (Worker线程池排队)<br>无数据时显示 <code>-</code></td></tr>
<tr><td><b>QMeta Net</b></td><td>QueryMeta RPC 网络耗时 (若无 ZMQ 明细则显示 QueryMeta 总耗时)</td></tr>
<tr><td><b>Master Proc</b></td><td>Master 处理耗时 (含 req_queue + exec + rsp_queue)</td></tr>
<tr><td><b>Remote Net/Self/URMA</b></td><td>GET 远程获取的三个子阶段</td></tr>
<tr><td><b>Other (含详情)</b></td><td>无法归入以上阶段的时间, hover 看子项 (重试/失败/清理)</td></tr>
<tr><td><b>异常信息</b></td><td>重试/失败/错误的关键事件汇总</td></tr>
<tr><td><b>St</b></td><td>状态码: <span class="ok">0=成功</span> <span class="er">1002=RPC 超时</span></td></tr>
</table>
<h4>颜色含义</h4>
<ul>
<li><span style="background:#e8f5e9;padding:1px 4px">绿底</span> 成功请求 (status=0)</li>
<li><span style="background:#ffebee;padding:1px 4px">红底</span> 失败请求 (status&ne;0, 通常 1002)</li>
</ul>
<h4>异常信息关键词</h4>
<ul>
<li><span class="tag">N 次 attempt</span> SDK 请求触发了多次 Worker attempt (SDK 重试)</li>
<li><span class="tag">N 次 GetDone</span> Worker 执行了多次 Get Done (内部重试)</li>
<li><span class="tag">Publish 失败 xN</span> Publish 请求失败 (SET 专用)</li>
<li><span class="tag">QueryMeta 失败 xN</span> Master 查询失败后重试</li>
<li><span class="tag">RPCRetry xN</span> RPC 框架内部重试</li>
<li><span class="tag">Worker 超时</span> Worker 侧处理超时</li>
<li><span class="tag">Can't find object</span> 对象在所有 Worker 上都未找到</li>
</ul>
<h4>诊断步骤</h4>
<ol>
<li>先看 <span style="background:#ffebee;padding:1px 2px">红底行</span> &rarr; 失败请求的原因</li>
<li>看 SDK ms 最高的行 &rarr; 最大延迟尖刺</li>
<li>看哪个阶段数值最大 &rarr; 瓶颈阶段</li>
<li>看异常信息列 &rarr; 是否有重试/失败</li>
<li>用 TraceId 去 SDK 和 Worker 日志中 <code>grep</code> 完整上下文</li>
</ol>
<div class="next"><b>下一步:</b> 用 TraceId 去 SDK 日志中 <code>grep -n &lt;TraceId&gt; sdk.log</code> 和 Worker 日志中 <code>grep -n &lt;TraceId&gt; worker.log</code> 查看完整请求链路</div>
</div></details>'''

    content = f'''<!DOCTYPE html>
<html lang="zh-CN"><head><meta charset="UTF-8">
<title>Trace Latency Segmentation</title>
<script src="https://cdn.jsdelivr.net/npm/echarts@5/dist/echarts.min.js"></script>
<style>
*{{box-sizing:border-box}}
body{{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;margin:0;padding:14px;background:#f5f5f5;font-size:13px}}
h1{{text-align:center;color:#333;margin:0 0 8px}} h2{{color:#333;border-bottom:2px solid #5470c6;padding-bottom:6px;margin-top:22px;font-size:16px}}
.cards{{display:flex;gap:14px;flex-wrap:wrap;justify-content:center;margin:10px 0}}
.card{{background:#fff;border-radius:8px;padding:10px 14px;box-shadow:0 2px 6px rgba(0,0,0,.1);min-width:300px}}
.sg{{display:grid;grid-template-columns:repeat(4,1fr);gap:4px}}
.sv{{text-align:center;font-size:13px;font-weight:bold}} .sl{{display:block;font-size:10px;color:#999;font-weight:normal}}
.chart{{background:#fff;border-radius:8px;padding:8px;margin:10px 0;box-shadow:0 2px 6px rgba(0,0,0,.1)}}
table{{width:100%;border-collapse:collapse;background:#fff;border-radius:8px;overflow:hidden;box-shadow:0 2px 6px rgba(0,0,0,.1);font-size:12px}}
th{{background:#5470c6;color:#fff;padding:6px 5px;text-align:left;position:sticky;top:0;z-index:1;font-size:11px;white-space:nowrap}} th.n{{text-align:right}}
td{{padding:4px 5px;border-bottom:1px solid #eee}} td.n{{text-align:right;font-family:monospace;font-size:11px}}
td.nb{{text-align:right;font-family:monospace;font-weight:bold;color:#c62828;font-size:12px}}
td.tid{{font-family:monospace;font-size:10px}}
td.retry{{font-family:monospace;font-size:10px;color:#c62828;max-width:250px;white-space:normal;word-break:break-word;line-height:1.3}}
td.nd{{text-align:right;font-family:monospace;font-size:11px;line-height:1.4}}
.tw{{max-height:500px;overflow-y:auto;border-radius:8px;margin:10px 0}}
.er{{background:#ffebee;color:#c62828;padding:1px 4px;border-radius:3px;font-size:10px;font-weight:bold}}
.ok{{background:#e8f5e9;color:#2e7d32;padding:1px 4px;border-radius:3px;font-size:10px}}
.bar{{background:#fff;border-radius:8px;padding:10px 14px;margin:10px 0;box-shadow:0 2px 6px rgba(0,0,0,.1)}}
.lg{{display:flex;gap:12px;flex-wrap:wrap;font-size:11px;margin:4px 0}}
.lg i{{display:inline-block;width:12px;height:12px;border-radius:2px;margin-right:3px;vertical-align:middle}}
.mdl{{background:#fff;border-radius:8px;padding:12px 16px;margin:10px 0;box-shadow:0 2px 6px rgba(0,0,0,.1);font-size:12px}}
.mdl pre{{margin:4px 0;padding:8px;background:#f8f9fa;border-radius:4px;font-size:11px;overflow-x:auto;line-height:1.6}}
.guide{{background:#fff;border-radius:8px;margin:6px 0;box-shadow:0 1px 4px rgba(0,0,0,.08)}}
.guide summary{{cursor:pointer;font-weight:bold;color:#5470c6;padding:6px 10px;font-size:12px;list-style:none}}
.guide summary::before{{content:'\\25B6 '}}
.guide[open] summary::before{{content:'\\25BC '}}
.guide summary:hover{{color:#ee6666}}
.guide-body{{padding:2px 10px 10px;line-height:1.7}}
.guide-body h4{{margin:6px 0 2px;color:#444;font-size:12px}}
.guide-body table{{font-size:11px;margin:4px 0;box-shadow:none;border:1px solid #e0e0e0}}
.guide-body th,.guide-body td{{padding:3px 6px;border:1px solid #eee;font-size:11px}}
.guide-body th{{background:#f0f4ff;color:#333}}
.guide .tag{{display:inline-block;background:#e3f2fd;color:#1565c0;padding:1px 5px;border-radius:3px;font-size:10px;margin:1px}}
.guide .next{{background:#fff8e1;border-left:3px solid #ff9800;padding:6px 10px;margin:4px 0;border-radius:0 4px 4px 0;font-size:11px}}
</style></head><body>
<h1>Trace Latency Segmentation Analysis</h1>

<div class="bar"><strong>Data:</strong> {D['total']} traces | {D['sdk_n']} SDK | {D['wa_n']} WA |
<span class="ok">0=成功</span> <span class="er">1002=RPC超时</span></div>

<div class="mdl">
<pre>
<b>GET 分段模型:</b>
SDK Total (端到端)
+-- Request Delay (SDK发出 → Worker接收, 含RPC排队+网络)
+-- Queue Wait (Worker线程池排队等待, [Get] Receive elapsed)
+-- Worker Processing
    +-- Worker->Master QueryMeta
    |   +-- RPC Network (ZMQ network_residual)
    |   +-- <b>Master Processing</b> (req_queue + exec + rsp_queue)
    +-- Remote Get (Worker->远端Worker获取数据)
    |   +-- RPC Network (Remote done - Remote Pull)
    |   +-- Remote Worker Processing
    |       +-- <b>Remote Worker Self</b> (Remote Pull - URMA)
    |       +-- URMA Write
    +-- Other Worker (RPC重试 / 错误处理 / 清理)
+-- Response Delay (Worker完成 → SDK收到响应)

<b>SET 分段模型:</b>
SDK Total
+-- Request Delay (SDK发出 → Worker接收)
+-- Worker Create (本地OC写入, ~0.03ms)
+-- Worker Publish
    +-- Worker->Master CreateMeta
    |   +-- RPC Network
    |   +-- <b>Master Processing</b>
    +-- Other Publish
+-- Response Delay (Worker完成 → SDK收到响应)
</pre></div>

<h2>1. Summary</h2>
{_G1}
<div class="cards">{card("GET",gs,"#5470c6")}{card("SET",ss,"#91cc75")}</div>

<h2>2. GET Per-Request + Running Percentiles</h2>
{_G2}
<div class="chart" id="c1" style="height:360px"></div>

<h2>3. GET Segmentation (Per Request)</h2>
{_G3}
<div class="lg">
<i style="background:#fc8452"></i>Req Delay (无ZMQ明细时整体)
<i style="background:#ffe0b2"></i>Client Queuing (SDK框架)
<i style="background:#80cbc4"></i>Network (网络传输)
<i style="background:#ff8a65"></i>Server Queue Wait (Worker线程池)
<i style="background:#9b59b6"></i>Rsp Delay (Worker→SDK)
<i style="background:#fac858"></i>Queue Wait (线程池排队)
<i style="background:#73c0de"></i>QueryMeta: Network
<i style="background:#5470c6"></i><b>Master Processing</b>
<i style="background:#3ba272"></i>Remote: Network
<i style="background:#ee6666"></i><b>Remote Worker Self</b>
<i style="background:#ea7ccc"></i>URMA Write
<i style="background:#999"></i>Other Worker
</div>
<div class="chart" id="c2" style="height:360px"></div>

<h2>4. GET Segmentation Summary</h2>
{_G4}
<div class="tw" style="max-height:320px"><table>
<tr><th>Phase</th><th class="n">Cnt</th><th class="n">Avg ms</th><th class="n">P99 ms</th><th class="n">Max ms</th></tr>
{seg_table(D['g_segs'])}</table></div>

<h2>5. SET Per-Request</h2>
{_G5}
<div class="chart" id="c3" style="height:300px"></div>

<h2>6. SET Segmentation (Per Request)</h2>
{_G6}
<div class="lg">
<i style="background:#fc8452"></i>Req Delay (无ZMQ明细时整体)
<i style="background:#ffe0b2"></i>Client Queuing (SDK框架)
<i style="background:#80cbc4"></i>Network (网络传输)
<i style="background:#ff8a65"></i>Server Queue Wait (Worker线程池)
<i style="background:#9b59b6"></i>Rsp Delay (Worker→SDK)
<i style="background:#fac858"></i>Worker Create
<i style="background:#73c0de"></i>CreateMeta: Network
<i style="background:#5470c6"></i><b>Master Processing</b>
<i style="background:#999"></i>Other Publish
</div>
<div class="chart" id="c4" style="height:300px"></div>

<h2>7. SET Segmentation Summary</h2>
{_G7}
<div class="tw" style="max-height:280px"><table>
<tr><th>Phase</th><th class="n">Cnt</th><th class="n">Avg ms</th><th class="n">P99 ms</th><th class="n">Max ms</th></tr>
{seg_table(D['s_segs'])}</table></div>

<h2>8. Top {top_n} (含异常信息)</h2>
{_G8}
<div class="tw" style="max-height:600px"><table>
<tr><th>#</th><th>TraceId</th><th>Time</th><th class="n">SDK ms</th><th class="n">Req Delay</th><th class="n">CliQ</th><th class="n">Net</th><th class="n">SrvQ</th><th class="n">Rsp Delay</th>
<th class="n">Queue Wait</th><th class="n">QMeta Net</th><th class="n"><b>Master Proc</b></th><th class="n">Remote Net</th><th class="n"><b>Remote Self</b></th><th class="n">URMA</th><th class="n">Other (含详情)</th>
<th>异常信息</th><th>St</th></tr>
{top_rows}</table></div>

<script>
var c1=echarts.init(document.getElementById('c1'));
c1.setOption({{
  tooltip:{{trigger:'axis'}},
  legend:{{data:['SDK','P50','P99','Max'],top:0}},
  grid:{{left:58,right:12,bottom:50,top:32}},dataZoom:[{{type:'slider'}}],
  xAxis:{{type:'category',data:{D['g_lbl']},axisLabel:{{rotate:45,fontSize:9}}}},
  yAxis:{{type:'value',name:'ms',scale:true,min:0}},
  series:[
    {{name:'SDK',type:'bar',data:{D['g_sdk']},barMaxWidth:5,itemStyle:{{color:function(p){{var s={D['g_st']};return s[p.dataIndex]===0?'#91cc75':'#ee6666'}}}}}},
    {{name:'P50',type:'line',data:{D['g_p50']},smooth:true,lineStyle:{{width:2}},symbol:'none',z:10}},
    {{name:'P99',type:'line',data:{D['g_p99']},lineStyle:{{width:2,type:'dashed',color:'#fac858'}},symbol:'none',z:10}},
    {{name:'Max',type:'line',data:{D['g_rm']},lineStyle:{{width:2,color:'#c62828'}},symbol:'none',z:10}}
  ]
}});

var c2=echarts.init(document.getElementById('c2'));
var gOthTip={D['g_oth_tip']};
c2.setOption({{
  tooltip:{{trigger:'axis',formatter:function(ps){{
    var h=ps[0].axisValue+'<br>';
    var idx=ps[0].dataIndex;
    ps.forEach(function(p){{if(p.value>0.01){{
      h+=p.marker+' '+p.seriesName+': '+p.value.toFixed(2)+'ms';
      if(p.seriesName==='Other'&&gOthTip[idx])h+='<br><span style="color:#888;font-size:11px;margin-left:18px">'+gOthTip[idx]+'</span>';
      h+='<br>';
    }}}});
    return h;
  }}}},
  legend:{{data:['Req Delay','Client Queuing','Network','Server Queue','Rsp Delay','Queue Wait','QMeta Net','Master Proc','Remote Net','Remote Self','URMA','Other'],top:0}},
  grid:{{left:58,right:12,bottom:50,top:32}},dataZoom:[{{type:'slider'}}],
  xAxis:{{type:'category',data:{D['g_lbl']},axisLabel:{{rotate:45,fontSize:9}}}},
  yAxis:{{type:'value',name:'ms',scale:true,min:0}},
  series:[
    {{name:'Req Delay',type:'bar',stack:'s',data:{D['g_crpc']},itemStyle:{{color:'#fc8452'}},barMaxWidth:8}},
    {{name:'Client Queuing',type:'bar',stack:'s',data:{D['g_cliq']},itemStyle:{{color:'#ffe0b2'}},barMaxWidth:8}},
    {{name:'Network',type:'bar',stack:'s',data:{D['g_net']},itemStyle:{{color:'#80cbc4'}},barMaxWidth:8}},
    {{name:'Server Queue',type:'bar',stack:'s',data:{D['g_sqw']},itemStyle:{{color:'#ff8a65'}},barMaxWidth:8}},
    {{name:'Rsp Delay',type:'bar',stack:'s',data:{D['g_rsp']},itemStyle:{{color:'#9b59b6'}},barMaxWidth:8}},
    {{name:'Queue Wait',type:'bar',stack:'s',data:{D['g_qw']},itemStyle:{{color:'#fac858'}},barMaxWidth:8}},
    {{name:'QMeta Net',type:'bar',stack:'s',data:{D['g_qmn']},itemStyle:{{color:'#73c0de'}},barMaxWidth:8}},
    {{name:'Master Proc',type:'bar',stack:'s',data:{D['g_mp']},itemStyle:{{color:'#5470c6'}},barMaxWidth:8}},
    {{name:'Remote Net',type:'bar',stack:'s',data:{D['g_rn']},itemStyle:{{color:'#3ba272'}},barMaxWidth:8}},
    {{name:'Remote Self',type:'bar',stack:'s',data:{D['g_rs']},itemStyle:{{color:'#ee6666'}},barMaxWidth:8}},
    {{name:'URMA',type:'bar',stack:'s',data:{D['g_ur']},itemStyle:{{color:'#ea7ccc'}},barMaxWidth:8}},
    {{name:'Other',type:'bar',stack:'s',data:{D['g_oth']},itemStyle:{{color:'#999'}},barMaxWidth:8}}
  ]
}});

var c3=echarts.init(document.getElementById('c3'));
c3.setOption({{
  tooltip:{{trigger:'axis'}},
  grid:{{left:58,right:12,bottom:50,top:24}},dataZoom:[{{type:'slider'}}],
  xAxis:{{type:'category',data:{D['s_lbl']},axisLabel:{{rotate:45,fontSize:9}}}},
  yAxis:{{type:'value',name:'ms',scale:true,min:0}},
  series:[{{type:'bar',data:{D['s_sdk']},barMaxWidth:10,itemStyle:{{color:function(p){{var s={D['s_st']};return s[p.dataIndex]===0?'#91cc75':'#ee6666'}}}}}}]
}});

var c4=echarts.init(document.getElementById('c4'));
var sOpubTip={D['s_opub_tip']};
c4.setOption({{
  tooltip:{{trigger:'axis',formatter:function(ps){{
    var h=ps[0].axisValue+'<br>';
    var idx=ps[0].dataIndex;
    ps.forEach(function(p){{if(p.value>0.01){{
      h+=p.marker+' '+p.seriesName+': '+p.value.toFixed(2)+'ms';
      if(p.seriesName==='Other Pub'&&sOpubTip[idx])h+='<br><span style="color:#888;font-size:11px;margin-left:18px">'+sOpubTip[idx]+'</span>';
      h+='<br>';
    }}}});
    return h;
  }}}},
  legend:{{data:['Req Delay','Client Queuing','Network','Server Queue','Rsp Delay','Create','CMeta Net','Master Proc','Other Pub'],top:0}},
  grid:{{left:58,right:12,bottom:50,top:32}},dataZoom:[{{type:'slider'}}],
  xAxis:{{type:'category',data:{D['s_lbl']},axisLabel:{{rotate:45,fontSize:9}}}},
  yAxis:{{type:'value',name:'ms',scale:true,min:0}},
  series:[
    {{name:'Req Delay',type:'bar',stack:'s',data:{D['s_crpc']},itemStyle:{{color:'#fc8452'}},barMaxWidth:10}},
    {{name:'Client Queuing',type:'bar',stack:'s',data:{D['s_cliq']},itemStyle:{{color:'#ffe0b2'}},barMaxWidth:10}},
    {{name:'Network',type:'bar',stack:'s',data:{D['s_net']},itemStyle:{{color:'#80cbc4'}},barMaxWidth:10}},
    {{name:'Server Queue',type:'bar',stack:'s',data:{D['s_sqw']},itemStyle:{{color:'#ff8a65'}},barMaxWidth:10}},
    {{name:'Rsp Delay',type:'bar',stack:'s',data:{D['s_rsp']},itemStyle:{{color:'#9b59b6'}},barMaxWidth:10}},
    {{name:'Create',type:'bar',stack:'s',data:{D['s_cr']},itemStyle:{{color:'#fac858'}},barMaxWidth:10}},
    {{name:'CMeta Net',type:'bar',stack:'s',data:{D['s_cmn']},itemStyle:{{color:'#73c0de'}},barMaxWidth:10}},
    {{name:'Master Proc',type:'bar',stack:'s',data:{D['s_mp']},itemStyle:{{color:'#5470c6'}},barMaxWidth:10}},
    {{name:'Other Pub',type:'bar',stack:'s',data:{D['s_opub']},itemStyle:{{color:'#999'}},barMaxWidth:10}}
  ]
}});

window.addEventListener('resize',function(){{c1.resize();c2.resize();c3.resize();c4.resize();}});
</script></body></html>'''

    os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        f.write(content)
    kb = os.path.getsize(path) / 1024
    print(f"Report: {path} ({kb:.1f} KB)")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _open_browser(path):
    abs_path = os.path.abspath(path)
    try:
        # WSL: open in Windows Edge
        result = subprocess.run(
            ["cmd.exe", "/c", "start", "ms-edge", abs_path],
            capture_output=True, timeout=5,
        )
        if result.returncode == 0:
            return
    except (FileNotFoundError, subprocess.TimeoutExpired):
        pass
    try:
        subprocess.run(["xdg-open", abs_path], capture_output=True, timeout=5)
    except (FileNotFoundError, subprocess.TimeoutExpired):
        print(f"  Please open manually: {abs_path}")


def _parse_time(s):
    return datetime.fromisoformat(s)


def main():
    parser = argparse.ArgumentParser(
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description="KVCache Trace-Level Latency Segmentation Analysis",
        epilog="""\
Examples:
  # Explicit SDK + Worker log files
  python3 %(prog)s sdk.log worker.log -o report.html

  # With time filtering
  python3 %(prog)s sdk.log worker.log \\
      --since "2026-05-10T02:00:00" --until "2026-05-10T03:00:00" \\
      -o report.html

  # Auto-detect from directory
  python3 %(prog)s /path/to/log_dir -o report.html

  # Open in browser (WSL Edge supported)
  python3 %(prog)s sdk.log worker.log -o /mnt/d/html/report.html --open
""",
    )
    parser.add_argument(
        "logs", nargs="+",
        help="SDK log file + Worker log file, OR a single directory to auto-detect",
    )
    parser.add_argument(
        "--since",
        type=_parse_time,
        help="Start time (inclusive), format: YYYY-MM-DDTHH:MM:SS",
    )
    parser.add_argument(
        "--until",
        type=_parse_time,
        help="End time (exclusive), format: YYYY-MM-DDTHH:MM:SS",
    )
    parser.add_argument(
        "-o", "--output",
        default="trace_latency_report.html",
        help="Output HTML path (default: trace_latency_report.html)",
    )
    parser.add_argument(
        "--top-n",
        type=int,
        default=50,
        help="Number of top-latency traces to show (default: 50)",
    )
    parser.add_argument(
        "--open",
        action="store_true",
        help="Open report in browser after generation",
    )

    args = parser.parse_args()

    # Determine input mode
    if len(args.logs) == 1 and os.path.isdir(args.logs[0]):
        log_dir = args.logs[0]
        sdk_paths, worker_paths = _find_log_files(log_dir)
        if not sdk_paths:
            print(f"Error: no SDK log files (ds_client_access*) found in {log_dir}", file=sys.stderr)
            sys.exit(1)
        if not worker_paths:
            print(f"Error: no Worker log files found in {log_dir}", file=sys.stderr)
            sys.exit(1)
        print(f"Auto-detected from {log_dir}:")
        print(f"  SDK: {len(sdk_paths)} file(s)")
        print(f"  Worker: {len(worker_paths)} file(s)")
        sdk_input = sdk_paths
        worker_input = worker_paths
    elif len(args.logs) == 2:
        sdk_input = args.logs[0]
        worker_input = args.logs[1]
        for p in args.logs:
            if not os.path.exists(p):
                print(f"Error: {p} not found", file=sys.stderr)
                sys.exit(1)
    else:
        parser.error("Provide either 1 directory or 2 log files (SDK + Worker)")

    generate_report(sdk_input, worker_input, args.output, args.since, args.until, args.top_n)

    if args.open:
        _open_browser(args.output)


if __name__ == "__main__":
    main()
