#!/usr/bin/env python3
"""Post-process ds-trace-triage and bottleneck outputs for NUMA/chip analysis."""

from __future__ import annotations

import argparse
import json
import re
import tarfile
from collections import Counter
from pathlib import Path
from typing import Any


URMA_WAIT_TIMEOUT_RE = re.compile(
    r"(?:URMA(?:[_ -]WAIT[_ -]TIMEOUT)|Timed out waiting for urma_request_id)",
    re.IGNORECASE,
)


def _cohort_from_member(name: str) -> str | None:
    parts = Path(name).parts
    if "timeCollect" in parts:
        root = parts.index("timeCollect")
        if len(parts) <= root + 2:
            return None
        return f"time/{parts[root + 1]}"
    try:
        root = parts.index("trace_collect")
    except ValueError:
        return None
    if len(parts) <= root + 2 or parts[root + 1] not in {"core", "time"}:
        return None
    if parts[root + 2].startswith("unique_traces_"):
        return None
    return f"{parts[root + 1]}/{parts[root + 2]}"


def build_cohort_index(archive_path: Path) -> dict[str, set[str]]:
    """Map a Trace ID to every collection cohort containing it."""
    cohorts: dict[str, set[str]] = {}
    with tarfile.open(archive_path, "r:gz") as archive:
        for member in archive.getmembers():
            if not member.isfile():
                continue
            cohort = _cohort_from_member(member.name)
            if cohort is None:
                continue
            trace_id = re.sub(r"_\d+$", "", Path(member.name).name)
            cohorts.setdefault(trace_id, set()).add(cohort)
    return cohorts


def count_archive_trace_files(archive_path: Path) -> int:
    with tarfile.open(archive_path, "r:gz") as archive:
        return sum(member.isfile() and _cohort_from_member(member.name) is not None for member in archive)


def _metric_max(value: Any) -> float | None:
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, dict):
        maximum = value.get("max")
        return float(maximum) if isinstance(maximum, (int, float)) else None
    return None


def _client_ms(trace: dict, row: dict | None) -> float | None:
    if row is not None and isinstance(row.get("client_ms"), (int, float)):
        return float(row["client_ms"])
    client = (trace.get("access_latency_ms_by_role") or {}).get("client")
    return _metric_max(client)


def _operation(trace: dict) -> str:
    flows = trace.get("flows") or {}
    for name in flows:
        upper = str(name).upper()
        if "GET" in upper:
            return "GET"
        if any(token in upper for token in ("PUT", "SET", "CREATE", "PUBLISH")):
            return "PUT"
    return "未明确"


def _status(trace: dict, row: dict | None) -> int | None:
    if row is not None and isinstance(row.get("status"), int):
        return row["status"]
    codes = []
    for value in (trace.get("access_statuses") or {}):
        try:
            codes.append(int(value))
        except (TypeError, ValueError):
            continue
    for key in (trace.get("errors") or {}):
        match = re.fullmatch(r"status=(\d+)", str(key))
        if match:
            codes.append(int(match.group(1)))
    nonzero = [code for code in codes if code]
    return nonzero[0] if nonzero else (0 if codes else None)


def _evidence_raw(trace: dict, row: dict | None) -> list[str]:
    result: list[str] = []
    for item in trace.get("evidence") or []:
        raw = (item.get("raw") or item.get("text")) if isinstance(item, dict) else item
        if raw and str(raw) not in result:
            result.append(str(raw))
    if row:
        for item in row.get("evidence") or []:
            raw = item.get("raw") if isinstance(item, dict) else item
            if raw and str(raw) not in result:
                result.append(str(raw))
    return result


def _timeout_elapsed_ms(evidence: list[str]) -> float | None:
    values = []
    for line in evidence:
        if "TIMEOUT" not in line.upper() and "TIMED OUT" not in line.upper():
            continue
        match = re.search(r"elapsedMs\s*[=:]\s*([0-9]+(?:\.[0-9]+)?)", line, re.IGNORECASE)
        if match:
            values.append(float(match.group(1)))
    return max(values) if values else None


def _chip_values(event: dict) -> tuple[int, int] | None:
    values = event.get("src_chip_inflight")
    if isinstance(values, str):
        values = {
            match.group(1): int(match.group(2))
            for match in re.finditer(r"(\d+)\s*:\s*(\d+)", values)
        }
    if not isinstance(values, dict):
        return None
    chip1 = values.get("1", values.get(1, 0))
    chip2 = values.get("2", values.get(2, 0))
    if not isinstance(chip1, (int, float)) or not isinstance(chip2, (int, float)):
        return None
    return int(chip1), int(chip2)


def build_trace_records(
    summary: dict, bottleneck: dict, cohorts: dict[str, set[str]]
) -> list[dict]:
    """Build one record per unique Trace while preserving multi-cohort membership."""
    summary_traces = summary.get("traces") or {}
    rows = {row["trace_id"]: row for row in bottleneck.get("traces") or []}
    trace_ids = sorted(set(summary_traces) | set(rows) | set(cohorts))
    records: list[dict] = []
    for trace_id in trace_ids:
        trace = summary_traces.get(trace_id) or {}
        row = rows.get(trace_id)
        events = []
        event_keys = set()
        for event in trace.get("ub_events") or []:
            if event.get("event_type") not in {"total", "urma_total"} or not isinstance(
                event.get("cost_ms"), (int, float)
            ):
                continue
            key = event.get("raw") or (
                event.get("request_id"), event.get("timestamp"), event.get("cost_ms"), event.get("worker")
            )
            if key in event_keys:
                continue
            event_keys.add(key)
            events.append(event)
        chip_pairs = []
        for event in events:
            pair = _chip_values(event)
            if pair is not None:
                chip_pairs.append(pair)
        chip1_total = sum(pair[0] for pair in chip_pairs)
        chip2_total = sum(pair[1] for pair in chip_pairs)
        chip1_peak = max((pair[0] for pair in chip_pairs), default=None)
        chip2_peak = max((pair[1] for pair in chip_pairs), default=None)
        if not chip_pairs:
            chip_mode = "未观测"
            chip_skew = None
        elif chip1_total > 0 and chip2_total > 0:
            chip_mode = "双 chip"
            chip_skew = abs(chip1_total - chip2_total) / (chip1_total + chip2_total)
        elif chip1_total > 0:
            chip_mode = "仅 chip 1"
            chip_skew = 1.0
        elif chip2_total > 0:
            chip_mode = "仅 chip 2"
            chip_skew = 1.0
        else:
            chip_mode = "已观测但无 inflight"
            chip_skew = None
        requests = [
            {
                "request_id": event.get("request_id"),
                "timestamp": event.get("timestamp"),
                "worker": event.get("worker"),
                "total_ms": float(event["cost_ms"]),
                "is_slow": float(event["cost_ms"]) > 1.5,
                "src_chip_inflight": event.get("src_chip_inflight"),
                "urma_inflight_wr_count": event.get("urma_inflight_wr_count"),
                "raw": event.get("raw"),
            }
            for event in events
        ]
        row_worker = row.get("direct_data_worker") if row else None
        if row_worker in {None, "", "未明确", "unknown"}:
            row_worker = None
        worker = row_worker or next((request["worker"] for request in requests if request["worker"]), None)
        if not worker:
            trace_workers = [name for name in (trace.get("workers") or {}) if name not in {"unknown", "未明确"}]
            worker = next((name for name in trace_workers if "worker-0-worker" in name), None)
            worker = worker or (trace_workers[0] if trace_workers else None)
        evidence = _evidence_raw(trace, row)
        records.append(
            {
                "trace_id": trace_id,
                "cohorts": sorted(cohorts.get(trace_id, set())),
                "operation": _operation(trace),
                "status": _status(trace, row),
                "client_ms": _client_ms(trace, row),
                "primary_problem": row.get("primary_problem") if row else None,
                "transport": row.get("transport") if row else None,
                "worker": worker,
                "timestamp": trace.get("first_ts")
                or next((request["timestamp"] for request in requests if request["timestamp"]), None),
                "failed": bool(row.get("failed")) if row else (_status(trace, row) not in {None, 0}),
                "chip_mode": chip_mode,
                "chip1_total": chip1_total if chip_pairs else None,
                "chip2_total": chip2_total if chip_pairs else None,
                "chip1_peak": chip1_peak,
                "chip2_peak": chip2_peak,
                "chip_skew": chip_skew,
                "urma_requests": requests,
                "slow_wr_count": sum(request["is_slow"] for request in requests),
                "timeout_elapsed_ms": _timeout_elapsed_ms(evidence),
                "evidence": evidence,
            }
        )
    return records


def build_aggregate(records: list[dict], source: dict) -> dict:
    return {
        "unique_trace_count": len(records),
        "chip_mode_counts": dict(Counter(record["chip_mode"] for record in records)),
        "operation_counts": dict(Counter(record["operation"] for record in records)),
        "status_counts": dict(Counter(str(record["status"]) for record in records)),
        "source": source,
    }


def classify_error_chain(record: dict) -> dict:
    """Classify only evidence-backed 1004/1010 chains."""
    joined = "\n".join(record.get("evidence") or [])
    operation = record.get("operation")
    status = record.get("status")
    has_timeout = bool(URMA_WAIT_TIMEOUT_RE.search(joined))
    has_response_shape = "unexpectedly returned TCP payload" in joined or "fallback payload" in joined
    signals: list[str] = []
    if has_timeout:
        signals.append("URMA等待超时")
    if has_response_shape:
        signals.append("UB响应形态异常")
    if status == 1004:
        signals.append("Client状态1004")
    if status == 1010:
        signals.append("Client状态1010")

    if operation == "GET" and status == 1004:
        required = [("URMA等待超时", has_timeout), ("UB响应形态异常", has_response_shape)]
        closed = all(present for _, present in required)
        return {
            "family": "GET URMA超时后上浮1004" if closed else "GET URMA错误1004（链路未闭合）",
            "closed": closed,
            "signals": signals,
            "missing": [name for name, present in required if not present],
        }
    if operation == "PUT" and status == 1010:
        closed = has_timeout and ("op=WRITE" in joined or "WRITE" in joined)
        return {
            "family": "PUT URMA WRITE等待超时1010" if closed else "PUT URMA超时1010（链路未闭合）",
            "closed": closed,
            "signals": signals,
            "missing": [] if closed else ["URMA WRITE等待超时"],
        }
    if status not in {None, 0}:
        return {
            "family": f"其他状态{status}",
            "closed": False,
            "signals": signals,
            "missing": ["可证明的完整错误链"],
        }
    return {"family": "成功", "closed": True, "signals": signals, "missing": []}


def summarize_latency_bands(records: list[dict]) -> list[dict]:
    cohorts = set()
    for record in records:
        for cohort in record.get("cohorts", []):
            if cohort.startswith("time/"):
                cohorts.add(cohort)
    result = []
    for cohort in sorted(cohorts):
        members = [record for record in records if cohort in record.get("cohorts", [])]
        latencies = sorted(
            float(record["client_ms"])
            for record in members
            if isinstance(record.get("client_ms"), (int, float))
        )
        result.append(
            {
                "cohort": cohort,
                "unique_trace_count": len(members),
                "operation_counts": dict(Counter(record["operation"] for record in members)),
                "problem_counts": dict(
                    Counter(record.get("primary_problem") or "未分类" for record in members)
                ),
                "chip_mode_counts": dict(Counter(record["chip_mode"] for record in members)),
                "client_min_ms": latencies[0] if latencies else None,
                "client_max_ms": latencies[-1] if latencies else None,
                "slow_wr_count": sum(record["slow_wr_count"] for record in members),
            }
        )
    return result


def summarize_time_buckets(records: list[dict]) -> list[dict]:
    buckets: dict[str, dict] = {}
    for record in records:
        timestamp = record.get("timestamp")
        if not timestamp:
            continue
        second = str(timestamp)[:19]
        bucket = buckets.setdefault(
            second,
            {"second": second, "trace_count": 0, "error_count": 0, "slow_wr_count": 0, "dual_chip_count": 0},
        )
        bucket["trace_count"] += 1
        bucket["error_count"] += record.get("status") not in {None, 0}
        bucket["slow_wr_count"] += record.get("slow_wr_count") or 0
        bucket["dual_chip_count"] += record.get("chip_mode") == "双 chip"
    return [buckets[key] for key in sorted(buckets)]


def build_insights(records: list[dict], latency_bands: list[dict], time_buckets: list[dict]) -> list[dict]:
    observed = [record for record in records if record.get("chip_mode") != "未观测"]
    dual = [record for record in observed if record.get("chip_mode") == "双 chip"]
    insights = [
        {
            "title": "多 chip 并发已观测",
            "text": (
                f"{len(observed)} 条 Trace 有 srcChipInflight 证据，其中 {len(dual)} 条同时出现 chip 1/2"
                f"（{100 * len(dual) / len(observed):.1f}%）。该字段是发送侧完成时刻的全局并发快照，"
                "可证明两颗 source chip 存在并发 WR；不能证明当前 WR 选中了哪颗 chip、"
                "不能证明队列均衡 override 已执行，也不能单独换算带宽收益。"
                if observed
                else "本批 Trace 未观测到 srcChipInflight，不能判断多 chip 并发或队列均衡是否工作。"
            ),
        }
    ]
    short_bands = [band for band in latency_bands if "5000_7000" in band["cohort"] or "7000_10000" in band["cohort"]]
    short_count = sum(band["unique_trace_count"] for band in short_bands)
    short_problems = Counter()
    for band in short_bands:
        short_problems.update(band["problem_counts"])
    if short_count and short_problems:
        problem, count = short_problems.most_common(1)[0]
        short_text = (
            f"5–10ms 两档共 {short_count} 条 cohort 成员，主导分类为“{problem}” "
            f"{count} 条（{100 * count / short_count:.1f}%）。"
        )
    else:
        short_text = "本批数据没有可用于 5–10ms 汇总的 cohort，短时延档主瓶颈未观测。"
    insights.append({"title": "短时延档主瓶颈", "text": short_text})

    status_1004 = [record for record in records if record.get("status") == 1004]
    timeout_1004 = [record for record in status_1004 if record.get("timeout_elapsed_ms") is not None]
    insights.append(
        {
            "title": "10–20ms错误链",
            "text": (
                f"状态 1004 共 {len(status_1004)} 条，其中 {len(timeout_1004)} 条保留可量化的 URMA timeout elapsedMs。"
                "超时 WR 没有完成态 URMA_ELAPSED_TOTAL，因此阶段图中的“数据父窗口/未细分”不能解释为非 URMA。"
            ),
        }
    )
    peak = max(time_buckets, key=lambda item: item["error_count"], default=None)
    insights.append(
        {
            "title": "错误时间集中度",
            "text": (
                f"错误峰值位于 {peak['second']}，该秒 {peak['error_count']} 条错误、{peak['trace_count']} 条 Trace。"
                "时间集中支持短时突发/并发拥塞假设，但仅凭 Trace 不能把它定性为 incast。"
                if peak and peak["error_count"]
                else "未观测到可定位到秒级时间戳的错误。"
            ),
        }
    )
    return insights


def build_source_chain(source: dict) -> list[dict]:
    head = source.get("head") or "unknown"
    chain = [
        {
            "stage": "arena配置",
            "source": "src/datasystem/common/rdma/urma_manager.cpp:SetClientTransportArenaConfig",
            "judgment": "读取 DATASYSTEM_UB_TRANSPORT_ARENA_NUM 并设置 ub_transport_arena_num",
            "source_ref": head,
        },
        {
            "stage": "NUMA内存绑定",
            "source": "src/datasystem/common/rdma/urma_manager.cpp:BindClientTransportMemory",
            "judgment": "等大 arena range 按已发现 NUMA node 轮转 mbind",
            "source_ref": head,
        },
        {
            "stage": "多arena分配",
            "source": "src/datasystem/common/shared_memory/arena.cpp:ArenaManager::CreateArenaGroup",
            "judgment": "UB_TRANSPORT pool 拆为多个 arena，ArenaGroup 轮转选择 arena",
            "source_ref": head,
        },
        {
            "stage": "chip信息传播",
            "source": "src/datasystem/client/object_cache/transport/data_plane/ub_transporter.cpp:NumaIdToChipId",
            "judgment": "接收 buffer NUMA id 转为传输描述中的 chip id",
            "source_ref": head,
        },
        {
            "stage": "发送端chip选择",
            "source": "src/datasystem/common/rdma/urma_manager.cpp:GetAffinitySrcChipIdForPost",
            "judgment": "发送端按 transmitted chip 与 NUMA affinity 策略选择 source chip",
            "source_ref": head,
        },
        {
            "stage": "Trace观测",
            "source": "src/datasystem/common/rdma/urma_manager.cpp:GetSrcChipInflightWrCountsString",
            "judgment": "URMA_ELAPSED_TOTAL 输出完成时刻的全局 srcChipInflight 快照；不等于当前 WR 的选片决策",
            "source_ref": head,
        },
    ]
    if source.get("pr") == 2095:
        chain.insert(
            -1,
            {
                "stage": "inflight均衡",
                "source": "src/datasystem/common/rdma/urma_manager.cpp:GetAffinitySrcChipId",
                "judgment": "PR 2095 仅在 chip1/chip2 inflight 差值严格大于阈值时覆盖 RR 候选；Trace 快照不是选择时刻",
                "source_ref": head,
            },
        )
    return chain


def normalize_runtime_config(runtime_config: dict | None) -> dict:
    config = runtime_config or {}
    qps_per_node = config.get("qps_per_node")
    client_count = config.get("client_count")
    threads_per_client = config.get("threads_per_client")
    workers_per_node = config.get("workers_per_node")
    return {
        "qps_per_node": qps_per_node,
        "client_count": client_count,
        "threads_per_client": threads_per_client,
        "workers_per_node": workers_per_node,
        "qps_per_client": (
            qps_per_node / client_count
            if isinstance(qps_per_node, (int, float))
            and isinstance(client_count, int)
            and client_count > 0
            else None
        ),
        "client_threads_per_node": (
            client_count * threads_per_client
            if isinstance(client_count, int)
            and client_count > 0
            and isinstance(threads_per_client, int)
            and threads_per_client > 0
            else None
        ),
    }


def build_analysis(
    run_dir: Path,
    bottleneck_path: Path,
    archive_path: Path,
    source: dict,
    runtime_config: dict | None = None,
) -> dict:
    summary = json.loads((run_dir / "summary.json").read_text(encoding="utf-8"))
    manifest_path = run_dir / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8")) if manifest_path.exists() else {}
    bottleneck = json.loads(bottleneck_path.read_text(encoding="utf-8"))
    cohorts = build_cohort_index(archive_path)
    records = build_trace_records(summary, bottleneck, cohorts)
    for record in records:
        record["error_chain"] = classify_error_chain(record)
        if not record.get("worker"):
            record["worker"] = next(
                (request["worker"] for request in record["urma_requests"] if request.get("worker")), None
            )
    aggregate = build_aggregate(records, source)
    aggregate.update(
        {
            "archive_member_trace_files": count_archive_trace_files(archive_path),
            "cohort_membership_count": sum(len(values) for values in cohorts.values()),
            "cohort_counts": dict(Counter(cohort for values in cohorts.values() for cohort in values)),
            "overlap_trace_count": sum(len(values) > 1 for values in cohorts.values()),
            "error_family_counts": dict(Counter(record["error_chain"]["family"] for record in records)),
            "slow_wr_count": sum(record["slow_wr_count"] for record in records),
            "dual_chip_trace_count": sum(record["chip_mode"] == "双 chip" for record in records),
            "chip_observed_trace_count": sum(record["chip_mode"] != "未观测" for record in records),
        }
    )
    latency_bands = summarize_latency_bands(records)
    time_buckets = summarize_time_buckets(records)
    return {
        "schema_version": 1,
        "metadata": {
            "case": manifest.get("case_name") or manifest.get("case") or "pr2081-numa",
            "run_dir": run_dir.name,
            "archive": archive_path.name,
            "source": source,
            "runtime_config": normalize_runtime_config(runtime_config),
        },
        "aggregate": aggregate,
        "latency_bands": latency_bands,
        "time_buckets": time_buckets,
        "insights": build_insights(records, latency_bands, time_buckets),
        "source_chain": build_source_chain(source),
        "limitations": [
            "同一 Trace 在 Core 与时延档目录重复出现时只计一次，cohort 标签保留多值。",
            "srcChipInflight 是发送侧完成时刻的全局 inflight 快照；它不能证明当前 WR 的选片结果，也不能单独证明接收端带宽或端到端吞吐收益。",
            "缺失的 RPC、URMA、chip、CPU、锁或调度字段保持未观测，不按 0 处理。",
            f"当前单包没有修改前同配置基线，不计算 PR {source.get('pr', '未指定')} 的性能提升百分比。",
        ],
        "traces": records,
    }


HTML_TEMPLATE = r'''<!doctype html><html lang="zh-CN"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>PR2081 多NUMA Trace专项分析</title><style>
:root{--bg:#f4f7fb;--card:#fff;--ink:#19233a;--muted:#68738a;--line:#dfe6f1;--blue:#2778ff;--red:#d94352;--amber:#e99b24;--green:#26a269;--nav:238px}*{box-sizing:border-box}html{scroll-behavior:smooth}body{margin:0;background:var(--bg);color:var(--ink);font:14px/1.55 system-ui,-apple-system,"Segoe UI","Microsoft YaHei",sans-serif}nav{position:fixed;inset:0 auto 0 0;width:var(--nav);padding:22px 14px;background:#13213d;color:#dbe7ff;overflow:auto}nav h3{color:white;margin:0 8px 14px}nav a{display:block;color:#dbe7ff;text-decoration:none;padding:8px 10px;border-radius:8px}nav a:hover{background:#263d68}.main{margin-left:var(--nav);padding:18px;max-width:1700px}.hero{padding:24px 28px;border-radius:16px;background:linear-gradient(135deg,#13213d,#2360bd);color:#fff}.hero h1{margin:0 0 8px}.report-links{display:flex;gap:9px;flex-wrap:wrap;margin-top:15px}.report-links a{padding:8px 13px;border:1px solid #87b5ff;border-radius:9px;color:#fff;text-decoration:none;background:#ffffff14}.report-links a.active,.report-links a:hover{background:#fff;color:#173a70}.load-strip{display:grid;grid-template-columns:repeat(6,minmax(110px,1fr));gap:8px;margin-top:15px}.load-item{padding:9px;border-radius:9px;background:#ffffff16;border:1px solid #ffffff30;text-align:center}.load-item b{display:block;font-size:18px}.card{margin:16px 0;padding:18px;background:var(--card);border:1px solid var(--line);border-radius:14px}.card h2,.chart-title{text-align:center;margin:0 0 14px}.judgment{border-left:5px solid var(--blue);background:#f0f6ff}.warning{border-left:5px solid var(--amber);background:#fff8ea}.kpis{display:grid;grid-template-columns:repeat(6,minmax(110px,1fr));gap:10px}.kpi{padding:14px;border:1px solid var(--line);border-radius:10px;background:#fbfdff;text-align:center}.grid2{display:grid;grid-template-columns:1fr 1fr;gap:16px}.chart{width:100%;height:400px}.filters{display:flex;gap:8px;flex-wrap:wrap;margin:10px 0}.filters select,.filters input,.filters button{max-width:100%;padding:8px;border:1px solid var(--line);border-radius:8px;background:white}.filters button{cursor:pointer;color:white;background:var(--blue)}.table-wrap{width:100%;overflow-x:auto}table{width:100%;table-layout:fixed;border-collapse:collapse;font-size:12px}th,td{padding:7px 4px;border-bottom:1px solid var(--line);text-align:center;overflow-wrap:anywhere}th{background:#f0f4fb;cursor:pointer}td:first-child,th:first-child{text-align:left}.pager{display:flex;justify-content:center;gap:12px;align-items:center;margin-top:10px}.pager button{padding:6px 12px}.bad{color:var(--red);font-weight:700}.good{color:var(--green)}.muted{color:var(--muted)}pre{white-space:pre-wrap;word-break:break-word;background:#101829;color:#dce8ff;border-radius:9px;padding:12px;max-height:520px;overflow:auto}.evidence-line{display:block;padding:2px 5px}.evidence-line.bad{background:#4a1d29;color:#ffd9df}.source-step{padding:10px 12px;margin:8px 0;border-left:4px solid var(--green);background:#f2fbf7}.caption{color:var(--muted);text-align:center;margin-top:-6px}.chip1{color:#2878ff}.chip2{color:#8b5cf6}@media(max-width:1050px){nav{position:static;width:auto}nav a{display:inline-block}.main{margin:0;padding:8px}.grid2{grid-template-columns:1fr}.kpis,.load-strip{grid-template-columns:repeat(3,1fr)}.chart{height:350px}table{font-size:11px}th,td{padding:5px 2px}}@media(max-width:620px){.kpis,.load-strip{grid-template-columns:repeat(2,1fr)}.optional{display:none}}
</style></head><body><nav><h3>PR2081 NUMA诊断</h3><a href="#judgment">1. 核心判断</a><a href="#sample">2. 样本范围</a><a href="#errors">3. Core错误族</a><a href="#latency">4. GET/PUT时延档</a><a href="#chips">5. chip 1/2</a><a href="#worker">6. Worker与时间</a><a href="#traces">7. Top Trace</a><a href="#detail">8. Trace明细</a><a href="#source">9. PR 2081源码链</a><a href="#limits">10. 证据边界</a></nav><main class="main"><section class="hero"><h1>PR 2081 · 多 NUMA / chip Trace 专项分析</h1><div id="source-meta"></div><div class="report-links"><a class="active" href="index.html">NUMA专项</a><a href="bottleneck.html">Bottleneck分析</a><a href="triage.html">Trace Triage</a></div><div id="runtime-config" class="load-strip"></div></section>
<section class="card judgment" id="judgment"><h2>1. 核心判断</h2><div id="judgment-text"></div></section>
<section class="card" id="sample"><h2>2. 样本范围</h2><div class="kpis" id="kpis"></div><div id="cohort-overlap" class="caption"></div></section>
<section class="grid2" id="errors"><section class="card"><h2 class="chart-title">3.1 Core错误族</h2><div id="error-chart" class="chart"></div><p class="caption">回答 1004/1010 各有多少唯一 Trace，以及错误链是否闭合。</p></section><section class="card"><h2 class="chart-title">3.2 错误与操作</h2><div id="error-op-chart" class="chart"></div><p class="caption">区分 GET 上浮 1004 与 PUT WRITE 等待 1010。</p></section></section>
<section class="card" id="latency"><h2 class="chart-title">4. GET/PUT时延档与主瓶颈</h2><div id="latency-chart" class="chart"></div><p class="caption">每个采集档显示唯一 Trace 数及 bottleneck 主问题；目录数量不是线上发生率。</p></section>
<section class="grid2" id="chips"><section class="card"><h2 class="chart-title">5.1 chip 1/2 使用模式</h2><div id="chip-mode-chart" class="chart"></div></section><section class="card"><h2 class="chart-title">5.2 chip inflight与URMA</h2><div id="chip-load-chart" class="chart"></div></section></section>
<section class="card" id="worker"><h2>6. Worker与时间关联</h2><div><h3 class="chart-title">6.1 Worker集中度</h3><div id="worker-chart" class="chart"></div></div><div><h3 class="chart-title">6.2 秒级时间序列</h3><div id="time-chart" class="chart"></div></div><p class="caption">两张图各占一行，展示慢 WR/错误在 Worker 和时间上的集中；相关不等于因果。</p></section>
<section class="card" id="traces"><h2>7. Top Trace</h2><div class="filters"><select id="f-cohort"><option value="">全部cohort</option></select><select id="f-op"><option value="">全部操作</option></select><select id="f-status"><option value="">全部状态</option></select><select id="f-worker"><option value="">全部Worker</option></select><select id="f-chip"><option value="">全部chip模式</option></select><input id="f-min" type="number" step="0.1" placeholder="最小时延ms"><input id="f-max" type="number" step="0.1" placeholder="最大时延ms"><button id="download-filtered">下载筛选Trace</button></div><div class="table-wrap"><table id="trace-table"><thead><tr><th data-key="trace_id">Trace</th><th data-key="operation">操作</th><th data-key="status">状态</th><th data-key="client_ms">总时延</th><th data-key="primary_problem">主瓶颈</th><th data-key="chip_mode">chip模式</th><th data-key="chip1_peak">chip1峰值</th><th data-key="chip2_peak">chip2峰值</th><th data-key="slow_wr_count">慢WR</th><th data-key="worker">Worker</th></tr></thead><tbody></tbody></table></div><div class="pager"><button id="prev">上一页</button><span id="page-label"></span><button id="next">下一页</button></div></section>
<section class="card" id="detail"><h2>8. Trace明细</h2><div id="detail-summary"></div><pre id="detail-log"></pre></section>
<section class="card" id="source"><h2>9. PR 2081源码链</h2><div id="source-chain"></div></section>
<section class="card warning" id="limits"><h2>10. 证据边界</h2><ul id="limitations"></ul></section></main>
<script>__ECHARTS_SOURCE__</script><script>const DATA=__DATA_JSON__;const $=id=>document.getElementById(id);const esc=v=>String(v??'未观测').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));const entries=o=>Object.entries(o||{});let filtered=[...DATA.traces],page=1,sortKey='client_ms',sortDir=-1;const pageSize=8;const source=DATA.metadata.source;$('source-meta').textContent=`PR ${source.pr??'未指定'} · head ${source.head} · base ${source.base}`;const runtime=DATA.metadata.runtime_config||{};const fmt=v=>v==null?'未配置':Number.isInteger(v)?String(v):Number(v).toFixed(3).replace(/0+$/,'').replace(/\.$/,'');$('runtime-config').innerHTML=[['节点QPS',runtime.qps_per_node],['Client数',runtime.client_count],['线程/Client',runtime.threads_per_client],['Worker/节点',runtime.workers_per_node],['QPS/Client',runtime.qps_per_client],['Client线程/节点',runtime.client_threads_per_node]].map(x=>`<div class="load-item"><b>${fmt(x[1])}</b>${x[0]}</div>`).join('');const A=DATA.aggregate;$('judgment-text').innerHTML=DATA.insights.map(x=>`<p><b>${esc(x.title)}：</b>${esc(x.text)}</p>`).join('');$('kpis').innerHTML=[['唯一Trace',A.unique_trace_count],['归档Trace文件',A.archive_member_trace_files],['跨目录重复',A.overlap_trace_count],['Core错误',DATA.traces.filter(x=>x.status&&x.status!==0).length],['已观测chip',A.chip_observed_trace_count],['慢WR >1.5ms',A.slow_wr_count]].map(x=>`<div class="kpi"><b>${x[1]}</b>${x[0]}</div>`).join('');$('cohort-overlap').textContent=`cohort成员：${entries(A.cohort_counts).map(x=>x.join('=')).join(' · ')}`;$('limitations').innerHTML=DATA.limitations.map(x=>`<li>${esc(x)}</li>`).join('');$('source-chain').innerHTML=DATA.source_chain.map(x=>`<div class="source-step"><b>${esc(x.stage)}</b> · <code>${esc(x.source)}</code><br>${esc(x.judgment)}</div>`).join('');
function chart(id,opt){const dom=$(id),old=echarts.getInstanceByDom?.(dom);if(old)old.dispose();const c=echarts.init(dom);c.setOption(opt);return c}const err=entries(A.error_family_counts);chart('error-chart',{tooltip:{},xAxis:{type:'category',data:err.map(x=>x[0]),axisLabel:{rotate:20}},yAxis:{type:'value'},series:[{type:'bar',data:err.map(x=>x[1]),itemStyle:{color:'#d94352'}}]});const ops=entries(A.operation_counts);chart('error-op-chart',{tooltip:{},series:[{type:'pie',radius:['35%','68%'],data:ops.map(x=>({name:x[0],value:x[1]}))}]});const bands=DATA.latency_bands;chart('latency-chart',{tooltip:{trigger:'axis'},legend:{},grid:{left:50,right:20,bottom:100},xAxis:{type:'category',data:bands.map(x=>x.cohort.replace('time/','')),axisLabel:{rotate:20}},yAxis:{type:'value'},series:[{name:'唯一Trace',type:'bar',data:bands.map(x=>x.unique_trace_count)},{name:'慢WR',type:'bar',data:bands.map(x=>x.slow_wr_count)}]});const modes=entries(A.chip_mode_counts);chart('chip-mode-chart',{tooltip:{},series:[{type:'pie',radius:['35%','68%'],data:modes.map(x=>({name:x[0],value:x[1]}))}]});const chipRows=DATA.traces.filter(x=>x.chip_mode!=='未观测').sort((a,b)=>(b.client_ms||0)-(a.client_ms||0)).slice(0,50);chart('chip-load-chart',{tooltip:{trigger:'axis'},legend:{},grid:{left:50,right:20,bottom:100},xAxis:{type:'category',data:chipRows.map(x=>x.trace_id),axisLabel:{show:false}},yAxis:{type:'value'},series:[{name:'chip1峰值',type:'bar',stack:'chip',data:chipRows.map(x=>x.chip1_peak||0),itemStyle:{color:'#2878ff'}},{name:'chip2峰值',type:'bar',stack:'chip',data:chipRows.map(x=>x.chip2_peak||0),itemStyle:{color:'#8b5cf6'}},{name:'Client ms',type:'line',data:chipRows.map(x=>x.client_ms)}]});const workerCounts={};DATA.traces.forEach(x=>{const w=x.worker||'未明确';workerCounts[w]??={total:0,slow:0,error:0};workerCounts[w].total++;workerCounts[w].slow+=x.slow_wr_count;workerCounts[w].error+=x.status&&x.status!==0?1:0});const workers=entries(workerCounts).sort((a,b)=>b[1].total-a[1].total);chart('worker-chart',{tooltip:{trigger:'axis'},legend:{},grid:{left:50,right:20,bottom:120},xAxis:{type:'category',data:workers.map(x=>x[0]),axisLabel:{rotate:35}},yAxis:{type:'value'},series:[{name:'Trace',type:'bar',data:workers.map(x=>x[1].total)},{name:'慢WR',type:'bar',data:workers.map(x=>x[1].slow)},{name:'错误',type:'bar',data:workers.map(x=>x[1].error)}]});const tb=DATA.time_buckets;chart('time-chart',{tooltip:{trigger:'axis'},legend:{},grid:{left:50,right:20,bottom:100},xAxis:{type:'category',data:tb.map(x=>x.second.slice(11)),axisLabel:{rotate:35}},yAxis:{type:'value'},series:[{name:'Trace',type:'line',data:tb.map(x=>x.trace_count)},{name:'错误',type:'bar',data:tb.map(x=>x.error_count),itemStyle:{color:'#d94352'}},{name:'慢WR',type:'bar',data:tb.map(x=>x.slow_wr_count),itemStyle:{color:'#e99b24'}},{name:'双chip Trace',type:'line',data:tb.map(x=>x.dual_chip_count),itemStyle:{color:'#26a269'}}]});
function options(id,values){$(id).innerHTML+= [...new Set(values.filter(x=>x!==null&&x!==undefined&&x!==''))].sort().map(v=>`<option value="${esc(v)}">${esc(v)}</option>`).join('')}options('f-cohort',DATA.traces.flatMap(x=>x.cohorts));options('f-op',DATA.traces.map(x=>x.operation));options('f-status',DATA.traces.map(x=>String(x.status)));options('f-worker',DATA.traces.map(x=>x.worker));options('f-chip',DATA.traces.map(x=>x.chip_mode));function apply(){const cohort=$('f-cohort').value,op=$('f-op').value,status=$('f-status').value,worker=$('f-worker').value,chip=$('f-chip').value,min=parseFloat($('f-min').value),max=parseFloat($('f-max').value);filtered=DATA.traces.filter(x=>(!cohort||x.cohorts.includes(cohort))&&(!op||x.operation===op)&&(!status||String(x.status)===status)&&(!worker||x.worker===worker)&&(!chip||x.chip_mode===chip)&&(Number.isNaN(min)||(x.client_ms??-Infinity)>=min)&&(Number.isNaN(max)||(x.client_ms??Infinity)<=max));page=1;renderTable()}function cmp(a,b){const av=a[sortKey],bv=b[sortKey];if(av==null)return 1;if(bv==null)return -1;return (typeof av==='number'?av-bv:String(av).localeCompare(String(bv)))*sortDir}function renderTable(){filtered.sort(cmp);const pages=Math.max(1,Math.ceil(filtered.length/pageSize));page=Math.min(page,pages);const rows=filtered.slice((page-1)*pageSize,page*pageSize);$('trace-table').querySelector('tbody').innerHTML=rows.map(x=>`<tr data-id="${esc(x.trace_id)}"><td>${esc(x.trace_id)}</td><td>${esc(x.operation)}</td><td class="${x.status&&x.status!==0?'bad':'good'}">${esc(x.status)}</td><td class="${(x.client_ms||0)>10?'bad':''}">${x.client_ms==null?'未观测':x.client_ms.toFixed(3)+'ms'}</td><td>${esc(x.primary_problem)}</td><td>${esc(x.chip_mode)}</td><td class="chip1">${esc(x.chip1_peak)}</td><td class="chip2">${esc(x.chip2_peak)}</td><td>${x.slow_wr_count}</td><td>${esc(x.worker)}</td></tr>`).join('');$('page-label').textContent=`${page}/${pages} · ${filtered.length}条`;[...$('trace-table').querySelectorAll('tbody tr')].forEach(tr=>tr.onclick=()=>showDetail(tr.dataset.id));if(rows[0])showDetail(rows[0].trace_id)}function showDetail(id){const x=DATA.traces.find(r=>r.trace_id===id);if(!x)return;const timeout=x.timeout_elapsed_ms==null?'timeout未量化':`timeout ${x.timeout_elapsed_ms.toFixed(3)}ms`;$('detail-summary').innerHTML=`<b>${esc(x.trace_id)}</b> · ${esc(x.operation)} · 状态 <span class="${x.status?'bad':'good'}">${esc(x.status)}</span> · ${x.client_ms==null?'总时延未观测':x.client_ms.toFixed(3)+'ms'} · ${esc(x.error_chain.family)}<br>cohort: ${x.cohorts.map(esc).join(' / ')} · chip: ${esc(x.chip_mode)} (${esc(x.chip1_peak)} / ${esc(x.chip2_peak)}) · ${timeout}`;$('detail-log').innerHTML=x.evidence.map(line=>`<span class="evidence-line ${/TIMEOUT|failed|unexpectedly|\| E \|/i.test(line)?'bad':''}">${esc(line)}</span>`).join('')||'<span class="muted">未保留证据日志</span>'}['f-cohort','f-op','f-status','f-worker','f-chip','f-min','f-max'].forEach(id=>$(id).addEventListener('change',apply));$('prev').onclick=()=>{if(page>1){page--;renderTable()}};$('next').onclick=()=>{if(page*pageSize<filtered.length){page++;renderTable()}};[...$('trace-table').querySelectorAll('th[data-key]')].forEach(th=>th.onclick=()=>{if(sortKey===th.dataset.key)sortDir*=-1;else{sortKey=th.dataset.key;sortDir=1}renderTable()});$('download-filtered').onclick=()=>{const text=filtered.map(x=>`Trace ID: ${x.trace_id}\n${x.evidence.join('\n')}`).join('\n\n');const a=document.createElement('a');a.href=URL.createObjectURL(new Blob([text],{type:'text/plain;charset=utf-8'}));a.download='filtered-traces.txt';a.click();URL.revokeObjectURL(a.href)};renderTable();addEventListener('resize',()=>document.querySelectorAll('.chart').forEach(dom=>echarts.getInstanceByDom?.(dom)?.resize()));</script></body></html>'''


def _safe_json(value: object) -> str:
    return (
        json.dumps(value, ensure_ascii=False, separators=(",", ":"))
        .replace("<", "\\u003c")
        .replace(">", "\\u003e")
        .replace("&", "\\u0026")
    )


def render_html(analysis: dict, echarts_source: str) -> str:
    source = analysis.get("metadata", {}).get("source") or {}
    pr = source.get("pr") or "未指定"
    template = HTML_TEMPLATE.replace("PR2081", f"PR{pr}").replace("PR 2081", f"PR {pr}")
    template = template.replace("__ECHARTS_SOURCE__", echarts_source, 1)
    return template.replace("__DATA_JSON__", _safe_json(analysis), 1)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-dir", type=Path, required=True)
    parser.add_argument("--bottleneck-analysis", type=Path, required=True)
    parser.add_argument("--archive", type=Path, required=True)
    parser.add_argument("--source-head", required=True)
    parser.add_argument("--source-base", required=True)
    parser.add_argument("--pr", type=int, default=2081)
    parser.add_argument("--qps-per-node", type=float)
    parser.add_argument("--client-count", type=int)
    parser.add_argument("--threads-per-client", type=int)
    parser.add_argument("--workers-per-node", type=int)
    parser.add_argument("--echarts", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--analysis-json", type=Path, required=True)
    parser.add_argument("--force", action="store_true")
    args = parser.parse_args(argv)
    for target in (args.output, args.analysis_json):
        if target.exists() and not args.force:
            parser.error(f"refusing to overwrite {target}; pass --force")
    echarts_path = (
        args.echarts
        or Path(__file__).resolve().parents[1]
        / ".skills/ds-trace-triage/assets/echarts-5.5.1.min.js"
    )
    analysis = build_analysis(
        args.run_dir,
        args.bottleneck_analysis,
        args.archive,
        {"head": args.source_head, "base": args.source_base, "pr": args.pr},
        {
            "qps_per_node": args.qps_per_node,
            "client_count": args.client_count,
            "threads_per_client": args.threads_per_client,
            "workers_per_node": args.workers_per_node,
        },
    )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.analysis_json.parent.mkdir(parents=True, exist_ok=True)
    args.analysis_json.write_text(json.dumps(analysis, ensure_ascii=False, indent=2), encoding="utf-8")
    args.output.write_text(render_html(analysis, echarts_path.read_text(encoding="utf-8")), encoding="utf-8")
    print(args.output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
