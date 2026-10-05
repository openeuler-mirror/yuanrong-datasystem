"""Read trace, latency, RPC and URMA summary models for reporting."""

from __future__ import annotations

import collections
import re

from .contracts import (
    CATEGORY_CLIENT_RPC,
    CATEGORY_REMOTE,
    CATEGORY_WORKER,
    FOCUS_STAGE_NAMES,
    NON_TRANSPORT_CATEGORIES,
    PROBLEM_NAMES,
    STAGE_NAMES,
)
from .correlation import _build_worker_correlation
from ..evidence.rpc import (
    QUERY_AND_GET_METHOD_RE,
    _is_query_and_get_method,
    _rpc_fields,
)
from ..evidence.urma import SLOW_WR_THRESHOLD_MS
from .stats import (
    _metric_summary,
    _pearson,
    _percentile,
)


def _aggregate_urma(rows: list[dict]) -> dict:
    urma_rows = [row for row in rows if row.get("urma_requests")]
    requests = [
        {**request, "trace_id": row["trace_id"], "client_ms": row["client_ms"]}
        for row in urma_rows
        for request in row["urma_requests"]
    ]
    total_values = [item["total_ms"] for item in requests]
    inflight_values = [
        item["urma_inflight_wr_count"]
        for item in requests
        if item["urma_inflight_wr_count"] is not None
    ]
    wait_values = [item["wait_completion_ms"] for item in requests if item["wait_completion_ms"] is not None]
    inflight_total_pairs = [
        (float(item["urma_inflight_wr_count"]), float(item["total_ms"]))
        for item in requests
        if item["urma_inflight_wr_count"] is not None
    ]

    time_groups: dict[str, list[dict]] = collections.defaultdict(list)
    source_groups: dict[str, list[dict]] = collections.defaultdict(list)
    edge_groups: dict[tuple[str, str], list[dict]] = collections.defaultdict(list)
    for item in requests:
        time_groups[item["timestamp"][:16]].append(item)
        source_groups[item["source_worker"]].append(item)
        edge_groups[(item["source_worker"], item["target_worker"])].append(item)

    def group_summary(name: str, selected: list[dict]) -> dict:
        totals = [item["total_ms"] for item in selected]
        inflights = [item["urma_inflight_wr_count"] for item in selected if item["urma_inflight_wr_count"] is not None]
        waits = [item["wait_completion_ms"] for item in selected if item["wait_completion_ms"] is not None]
        return {
            "name": name,
            "trace_count": len({item["trace_id"] for item in selected}),
            "request_count": len(selected),
            "slow_request_count": sum(item["is_slow"] for item in selected),
            "total_p50_ms": round(_percentile(totals, 0.50), 3),
            "total_p90_ms": round(_percentile(totals, 0.90), 3),
            "total_max_ms": round(max(totals, default=0), 3),
            "inflight_p90": round(_percentile(inflights, 0.90), 1),
            "inflight_max": round(max(inflights, default=0), 1),
            "wait_p90_ms": round(_percentile(waits, 0.90), 3),
        }

    time_buckets = []
    for minute, selected in sorted(time_groups.items()):
        item = group_summary(minute, selected)
        item["minute"] = minute
        time_buckets.append(item)
    source_workers = [
        group_summary(worker, selected) | {"worker": worker}
        for worker, selected in source_groups.items()
    ]
    source_workers.sort(key=lambda item: (-item["slow_request_count"], -item["total_p90_ms"], item["worker"]))
    worker_edges = [
        group_summary(f"{source} → {target}", selected) | {"source_worker": source, "target_worker": target}
        for (source, target), selected in edge_groups.items()
    ]
    worker_edges.sort(key=lambda item: (-item["slow_request_count"], -item["total_p90_ms"], item["name"]))
    highest = max(requests, key=lambda item: item["total_ms"], default=None)
    correlation = _pearson(inflight_total_pairs)
    logical_writes = [write for row in urma_rows for write in row.get("urma_logical_writes", [])]
    complete_logical_writes = [write for write in logical_writes if write["complete"]]
    return {
        "trace_count": len(urma_rows),
        "request_count": len(requests),
        "wr_count": len(requests),
        "logical_write_count": len(logical_writes),
        "confirmed_logical_write_count": len(complete_logical_writes),
        "logical_write_wall_ms": _metric_summary(
            [write["wall_clock_ms"] for write in complete_logical_writes]
        ),
        "logical_write_slowest_wr_ms": _metric_summary(
            [write["slowest_wr_ms"] for write in logical_writes]
        ),
        "slow_threshold_ms": SLOW_WR_THRESHOLD_MS,
        "slow_request_count": sum(item["is_slow"] for item in requests),
        "request_total_ms": _metric_summary(total_values),
        "inflight_wr": _metric_summary(inflight_values),
        "wait_completion_ms": _metric_summary(wait_values),
        "inflight_total_correlation": round(correlation, 3) if correlation is not None else None,
        "time_buckets": time_buckets,
        "source_workers": source_workers,
        "worker_edges": worker_edges,
        "highest_request": highest,
    }


def _aggregate_non_transport(rows: list[dict]) -> dict:
    selected = [row for row in rows if row.get("non_transport_analysis")]
    categories = []
    for category in NON_TRANSPORT_CATEGORIES:
        items = [row for row in selected if row["non_transport_analysis"]["deep_category"] == category]
        latencies = [row["client_ms"] for row in items]
        observed = [row["non_transport_analysis"]["observed_ms"] for row in items]
        categories.append(
            {
                "category": category,
                "trace_count": len(items),
                "failed_count": sum(row["failed"] for row in items),
                "client_p50_ms": round(_percentile(latencies, 0.50), 3),
                "client_p90_ms": round(_percentile(latencies, 0.90), 3),
                "client_max_ms": round(max(latencies, default=0), 3),
                "observed_p50_ms": round(_percentile(observed, 0.50), 3),
            }
        )
    worker_groups: dict[str, list[dict]] = collections.defaultdict(list)
    for row in selected:
        worker_groups[row["direct_data_worker"]].append(row)
    workers = []
    for worker, items in worker_groups.items():
        counts = collections.Counter(item["non_transport_analysis"]["deep_category"] for item in items)
        latencies = [item["client_ms"] for item in items]
        workers.append(
            {
                "worker": worker,
                "trace_count": len(items),
                "failed_count": sum(item["failed"] for item in items),
                "client_p90_ms": round(_percentile(latencies, 0.90), 3),
                "categories": {category: counts[category] for category in NON_TRANSPORT_CATEGORIES},
            }
        )
    workers.sort(key=lambda item: (-item["trace_count"], item["worker"]))
    return {
        "trace_count": len(selected),
        "failed_count": sum(row["failed"] for row in selected),
        "categories": categories,
        "workers": workers,
    }


def _build_latency_segments(rows: list[dict]) -> list[dict]:
    """Group TopN rows by Client-latency bands; optional 2–5ms bands appear only when populated."""

    bands = (
        ("2–3ms", lambda value: 2 <= value < 3),
        ("3–4ms", lambda value: 3 <= value < 4),
        ("4–5ms", lambda value: 4 <= value < 5),
        ("5–6ms", lambda value: 5 <= value < 6),
        ("6–7ms", lambda value: 6 <= value < 7),
        ("7–10ms", lambda value: 7 <= value < 10),
        ("10–20ms", lambda value: 10 <= value <= 20),
        (">20ms", lambda value: value > 20),
    )
    segments = []
    for index, (label, matches) in enumerate(bands):
        selected = sorted(
            (row for row in rows if matches(float(row["client_ms"]))),
            key=lambda row: (row.get("timestamp") or "", row["trace_id"]),
        )
        counts = collections.Counter(
            row.get("focus_primary_problem", row["primary_problem"]) for row in selected
        )
        problem_order = list(
            dict.fromkeys((*FOCUS_STAGE_NAMES, "URMA超时", *PROBLEM_NAMES, *sorted(counts)))
        )
        dominant_problem = (
            max(problem_order, key=lambda problem: (counts[problem], -problem_order.index(problem)))
            if selected
            else "无Trace"
        )
        latencies = [row["client_ms"] for row in selected]
        segments.append(
            {
                "segment_id": index + 1,
                "label": label,
                "start_ts": (selected[0].get("timestamp") or "") if selected else "",
                "end_ts": (selected[-1].get("timestamp") or "") if selected else "",
                "trace_count": len(selected),
                "failed_count": sum(bool(row["failed"]) for row in selected),
                "client_p50_ms": round(_percentile(latencies, 0.50), 3),
                "client_p90_ms": round(_percentile(latencies, 0.90), 3),
                "dominant_problem": dominant_problem,
                "problem_counts": {
                    problem: counts[problem] for problem in problem_order if counts[problem]
                },
                "trace_ids": [row["trace_id"] for row in selected],
            }
        )
    return [item for item in segments if item["trace_count"] or item["label"] not in {"2–3ms", "3–4ms", "4–5ms"}]


def _aggregate_query_meta(rows: list[dict]) -> dict:
    selected = []
    for row in rows:
        query_meta_problem = row.get("primary_problem") == "QueryMeta"
        query_meta_deadline = row.get("failure_reason") == "QueryMeta RPC deadline"
        if query_meta_problem or query_meta_deadline:
            selected.append(row)
    second_groups: dict[str, list[dict]] = collections.defaultdict(list)
    initiator_groups: dict[str, list[dict]] = collections.defaultdict(list)
    target_groups: collections.Counter[str] = collections.Counter()
    target_observed = 0
    for row in selected:
        second_groups[row["timestamp"][:19] or "时间未记录"].append(row)
        initiator = row.get("client_observer") or "未明确"
        if initiator == "未明确":
            for record in row.get("evidence_records", []):
                if QUERY_AND_GET_METHOD_RE.search(record.get("text", "")):
                    initiator = record.get("worker") or "未明确"
                    break
        initiator_groups[initiator].append(row)
        targets = []
        for record in row.get("evidence_records", []):
            match = re.search(
                r"(?:meta owner|targetAddress|target address|peer)\s*[:=]\s*([^,\s]+)",
                record.get("text", ""),
                re.I,
            )
            if match:
                targets.append(match.group(1))
        if targets:
            target_observed += 1
            target_groups.update(set(targets))

    def summary(name: str, items: list[dict], key: str) -> dict:
        latencies = [row["query_meta_ms"] for row in items]
        failed_count = sum(row["failed"] for row in items)
        return {
            key: name,
            "trace_count": len(items),
            "failed_count": failed_count,
            "failure_rate_pct": round(failed_count / len(items) * 100, 1) if items else 0,
            "p50_ms": round(_percentile(latencies, 0.50), 3),
            "p90_ms": round(_percentile(latencies, 0.90), 3),
            "max_ms": round(max(latencies, default=0), 3),
        }

    time_buckets = [summary(second, items, "second") for second, items in sorted(second_groups.items())]
    initiators = [summary(name, items, "initiator") for name, items in initiator_groups.items()]
    initiators.sort(key=lambda item: (-item["failed_count"], -item["trace_count"], item["initiator"]))
    details = [row["query_meta_detail"] for row in selected if row.get("query_meta_detail")]
    timeout_rows = [row for row in selected if row.get("failure_reason") == "QueryMeta RPC deadline"]
    timeout_seconds = collections.Counter(row["timestamp"][:19] for row in timeout_rows)
    timeout_initiators: set[str] = set()
    timeout_targets: set[str] = set()
    empty_response_count = 0
    server_timing_unavailable_count = 0
    new_channel_count = 0
    for row in timeout_rows:
        initiator = row.get("client_observer") or "未明确"
        if initiator == "未明确":
            for record in row.get("evidence_records", []):
                if QUERY_AND_GET_METHOD_RE.search(record.get("text", "")):
                    initiator = record.get("worker") or "未明确"
                    break
        if initiator != "未明确":
            timeout_initiators.add(initiator)
        query_entries = []
        for text in row.get("evidence", []):
            method, fields = _rpc_fields(text)
            if method and _is_query_and_get_method(method):
                query_entries.append(fields)
            target = re.search(
                r"(?:meta owner|targetAddress|target address|peer)\s*[:=]\s*([^,\s]+)",
                text,
                re.I,
            )
            if target:
                timeout_targets.add(target.group(1))
        if any(
            QUERY_AND_GET_METHOD_RE.search(text) and re.search(r"\bresp_attachment_bytes=0\b", text)
            for text in row.get("evidence", [])
        ):
            empty_response_count += 1
        if any(
            (entry.get("cntl_failed") or entry.get("cntl_error_code"))
            and entry.get("server_req_queue", 0) == 0
            and entry.get("server_exec", 0) == 0
            and entry.get("network_residual", 0) == 0
            for entry in query_entries
        ):
            server_timing_unavailable_count += 1
        if any("BrpcChannel created:" in text for text in row.get("evidence", [])):
            new_channel_count += 1
    dominant_second, dominant_count = timeout_seconds.most_common(1)[0] if timeout_seconds else ("", 0)
    timeout_flow = {
        "timeout_count": len(timeout_rows),
        "full_window_count": sum(
            row.get("query_meta_detail", {}).get("category") == "QueryAndGet超时·服务端明细未闭合"
            for row in timeout_rows
        ),
        "retry_budget_count": sum(
            row.get("query_meta_detail", {}).get("category") == "QueryAndGet超时·重试累计窗口"
            for row in timeout_rows
        ),
        "empty_response_count": empty_response_count,
        "server_timing_unavailable_count": server_timing_unavailable_count,
        "urma_not_observed_count": sum(not row.get("urma_observed") for row in timeout_rows),
        "new_channel_count": new_channel_count,
        "distinct_initiator_count": len(timeout_initiators),
        "distinct_target_count": len(timeout_targets),
        "dominant_second": {"second": dominant_second, "trace_count": dominant_count},
        "confirmed_flow": (
            "Client ObjectReadFlow::Resolve → ObjectMetadataClient::QueryAndGet/QueryWithRetry → "
            "WorkerRpcClient::InvokeQueryAndGet → metadata-affine WorkerOCService.QueryAndGet；"
            "超时发生在该RPC返回前，尚未进入后续独立Data Worker GetObjectRemote阶段。"
        ),
        "likely_common_mechanism": (
            "同秒跨多个Client与Meta Owner集中爆发，且部分Trace在调用前新建BrpcChannel。"
            "当前源码中Channel::Init只创建channel、不主动建连，因此首次RPC懒建连可能放大请求/响应交付尾延迟；"
            "但并非每条超时都新建channel，不能作为唯一根因。"
        ),
        "root_cause_status": (
            "已确认卡在Client等待QueryAndGet RPC返回；失败请求缺少server trailer/Meta Owner阶段日志，"
            "不能确认是Client→Meta Owner连接/发送、Meta Owner排队/执行（含TryGet）、还是响应返回。"
        ),
        "ruled_out": (
            "未观测到同Trace URMA completion/URMA_WAIT_TIMEOUT，不能归为已确认URMA未返回；"
            "server_exec、network_residual的0是不可用占位，不是实测0ms。"
        ),
        "next_evidence": (
            "补齐同Trace的Meta Owner收包、QueryAndGet进入/退出、TryGet/URMA、响应发送时间戳，"
            "以及bRPC连接建立/复用信息，才能把最终根因压到连接、服务端处理或回包之一。"
        ),
    }
    return {
        "trace_count": len(selected),
        "failed_count": sum(row["failed"] for row in selected),
        "slow_success_count": sum(not row["failed"] and row["query_meta_ms"] >= 5 for row in selected),
        "detail_counts": dict(collections.Counter(detail["category"] for detail in details)),
        "try_get_urma_observed_count": sum(detail["try_get_urma_observed"] for detail in details),
        "try_get_slow_urma_count": sum(detail["slow_urma"] for detail in details),
        "failure_reasons": dict(
            collections.Counter(row["failure_reason"] for row in selected if row["failed"])
        ),
        "latency_ms": _metric_summary([row["query_meta_ms"] for row in selected]),
        "time_buckets": time_buckets,
        "initiators": initiators,
        "meta_targets": [
            {"target": target, "trace_count": count}
            for target, count in target_groups.most_common()
        ],
        "meta_target_coverage": "present" if target_observed else "missing",
        "meta_target_observed_count": target_observed,
        "timeout_flow": timeout_flow,
        "root_cause_boundary": (
            "QueryAndGet deadline confirms the Client-side wait endpoint. Failed RPCs without a server trailer "
            "do not separate Meta Owner execution, response send, network delivery, and Client deadline observation."
        ),
    }


def aggregate(rows: list[dict]) -> dict:
    categories: dict[str, dict[str, int]] = {}
    for category in (CATEGORY_REMOTE, CATEGORY_WORKER, CATEGORY_CLIENT_RPC):
        selected = [row for row in rows if row["category"] == category]
        categories[category] = {
            "total": len(selected),
            "success": sum(not row["failed"] for row in selected),
            "failed": sum(row["failed"] for row in selected),
        }
    latencies = [row["client_ms"] for row in rows]
    stage_totals = {}
    for stage in STAGE_NAMES:
        success_ms = sum(row["attribution_ms"][stage] for row in rows if not row["failed"])
        failed_ms = sum(row["attribution_ms"][stage] for row in rows if row["failed"])
        stage_totals[stage] = {
            "success_ms": round(success_ms, 3),
            "failed_ms": round(failed_ms, 3),
            "total_ms": round(success_ms + failed_ms, 3),
        }
    focus_stage_totals = {}
    for stage in FOCUS_STAGE_NAMES:
        success_ms = sum(row["focus_breakdown_ms"][stage] for row in rows if not row["failed"])
        failed_ms = sum(row["focus_breakdown_ms"][stage] for row in rows if row["failed"])
        focus_stage_totals[stage] = {
            "success_ms": round(success_ms, 3),
            "failed_ms": round(failed_ms, 3),
            "total_ms": round(success_ms + failed_ms, 3),
        }
    problem_summary = {}
    guidance_actions = {
        "RPC网络": "Worker 处理相对较快时，优先排查 bRPC 网络、调度、响应通知和 framework residual。",
        "RPC排队": "排查服务端请求队列、执行线程池饱和与 handler 调度；该阶段不等同于网络传输或业务执行。",
        "QueryMeta": "排查 Meta Worker 响应、元数据锁竞争、路由刷新与 metadata RPC。",
        "URMA": "排查 URMA completion、poll/notify 唤醒、线程调度、inflight 和大对象分块写。",
        "远端供数处理": "远端 server 父窗口较高；结合 URMA 观测边界，排查对象查找、buffer 准备、重试与远端 Worker 调度。",
        "数据访问父窗口/未细分": "数据访问父窗口扣除已知子阶段后仍较高；仅有明确 Local processing / remoteObjects=0 证据时才判为本地处理，否则保留未细分。",
        "未解释残差": "Client 总时延未被现有阶段覆盖，优先补齐 direct query/data、框架排队与 deadline 前后的观测。",
        "URMA超时": (
            "已观测到 URMA_WAIT_TIMEOUT；优先检查 completion、send lane、pending WR "
            "和错误上浮链。没有完成态时不把缺失的 URMA_ELAPSED_TOTAL 当作 0。"
        ),
    }
    for problem in PROBLEM_NAMES:
        selected = [row for row in rows if row["primary_problem"] == problem]
        if problem == "URMA超时":
            stage_values = [
                row["urma_timeout_max_ms"]
                for row in selected
                if row["urma_timeout_max_ms"] is not None
            ]
            metric_name = "URMA timeout elapsedMs"
        else:
            stage_values = [row["attribution_ms"][row["primary_stage"]] for row in selected]
            metric_name = "主阶段耗时"
        client_values = [row["client_ms"] for row in selected]
        problem_summary[problem] = {
            "trace_count": len(selected),
            "success_count": sum(not row["failed"] for row in selected),
            "failed_count": sum(row["failed"] for row in selected),
            "stage_p50_ms": round(_percentile(stage_values, 0.50), 3),
            "stage_p90_ms": round(_percentile(stage_values, 0.90), 3),
            "stage_max_ms": round(max(stage_values, default=0), 3),
            "client_p50_ms": round(_percentile(client_values, 0.50), 3),
            "client_p90_ms": round(_percentile(client_values, 0.90), 3),
            "metric_name": metric_name,
            "action": guidance_actions[problem],
        }
    focus_problem_summary = {}
    focus_problem_names = list(FOCUS_STAGE_NAMES) + sorted(
        {
            row["focus_primary_problem"]
            for row in rows
            if row["focus_primary_problem"] not in FOCUS_STAGE_NAMES
        }
    )
    for problem in focus_problem_names:
        selected = [row for row in rows if row["focus_primary_problem"] == problem]
        if problem == "URMA超时":
            stage_values = [
                row["urma_timeout_max_ms"]
                for row in selected
                if row["urma_timeout_max_ms"] is not None
            ]
            metric_name = "URMA timeout elapsedMs"
            action = "该类是错误覆盖层；用 timeout elapsedMs 和上浮链定位，不用互斥主阶段代替超时等待。"
        else:
            stage_values = [
                row["focus_breakdown_ms"][row["focus_primary_stage"]] for row in selected
            ]
            metric_name = "主阶段耗时"
            action = "按该互斥阶段的逐 Trace 明细和原始日志继续定位。"
        client_values = [row["client_ms"] for row in selected]
        focus_problem_summary[problem] = {
            "trace_count": len(selected),
            "success_count": sum(not row["failed"] for row in selected),
            "failed_count": sum(row["failed"] for row in selected),
            "stage_p50_ms": round(_percentile(stage_values, 0.50), 3),
            "stage_p90_ms": round(_percentile(stage_values, 0.90), 3),
            "stage_max_ms": round(max(stage_values, default=0), 3),
            "client_p50_ms": round(_percentile(client_values, 0.50), 3),
            "client_p90_ms": round(_percentile(client_values, 0.90), 3),
            "metric_name": metric_name,
            "action": action,
        }

    direct_groups: dict[str, list[dict]] = collections.defaultdict(list)
    for row in rows:
        direct_groups[row["direct_data_worker"]].append(row)
    direct_data_workers = []
    for worker, selected in direct_groups.items():
        client_values = [row["client_ms"] for row in selected]
        worker_values = [row["worker_process_ms"] for row in selected]
        direct_data_workers.append(
            {
                "worker": worker,
                "trace_count": len(selected),
                "failed_count": sum(row["failed"] for row in selected),
                "client_p50_ms": round(_percentile(client_values, 0.50), 3),
                "client_p90_ms": round(_percentile(client_values, 0.90), 3),
                "client_max_ms": round(max(client_values, default=0), 3),
                "worker_p50_ms": round(_percentile(worker_values, 0.50), 3),
            }
        )
    direct_data_workers.sort(key=lambda item: (-item["trace_count"], item["worker"]))

    source_groups: dict[str, list[float]] = collections.defaultdict(list)
    for row in rows:
        for worker, cost in row["urma_source_costs"].items():
            source_groups[worker].append(cost)
    urma_source_workers = []
    for worker, values in source_groups.items():
        urma_source_workers.append(
            {
                "worker": worker,
                "trace_count": len(values),
                "urma_p50_ms": round(_percentile(values, 0.50), 3),
                "urma_p90_ms": round(_percentile(values, 0.90), 3),
                "urma_max_ms": round(max(values, default=0), 3),
            }
        )
    urma_source_workers.sort(key=lambda item: (-item["trace_count"], item["worker"]))

    minute_groups: dict[str, list[dict]] = collections.defaultdict(list)
    for row in rows:
        minute_groups[row["timestamp"][:16] or "时间未记录"].append(row)
    busiest = max(minute_groups.items(), key=lambda item: (len(item[1]), item[0]), default=None)
    failure_hot = max(
        minute_groups.items(),
        key=lambda item: (sum(row["failed"] for row in item[1]), len(item[1]), item[0]),
        default=None,
    )
    latency_hot = max(
        minute_groups.items(),
        key=lambda item: (_percentile([row["client_ms"] for row in item[1]], 0.90), len(item[1]), item[0]),
        default=None,
    )
    time_findings = []
    if busiest:
        minute, selected = busiest
        time_findings.append(
            f"最密集的 Worker 本地分钟为 {minute}：{len(selected)} 条 Trace，"
            f"其中 {sum(row['failed'] for row in selected)} 条超时。"
        )
    if failure_hot:
        minute, selected = failure_hot
        failures = [row for row in selected if row["failed"]]
        time_findings.append(
            f"超时最集中的分钟为 {minute}：{len(failures)} 条；"
            f"主问题分布为 "
            f"{dict(collections.Counter(row['focus_primary_problem'] for row in failures)) or '无超时'}。"
        )
    if latency_hot:
        minute, selected = latency_hot
        p90 = _percentile([row["client_ms"] for row in selected], 0.90)
        time_findings.append(
            f"Client p90 最高的分钟为 {minute}：{p90:.3f}ms；"
            f"其中 {sum(bool(row['urma_requests']) for row in selected)} 条带 URMA 证据。"
        )
    return {
        "trace_count": len(rows),
        "failed_count": sum(row["failed"] for row in rows),
        "transport": dict(collections.Counter(row["transport"] for row in rows)),
        "access_locations": dict(collections.Counter(row["access_location"] for row in rows)),
        "data_affinity": dict(collections.Counter(row["data_affinity"] for row in rows)),
        "categories": categories,
        "stage_totals": stage_totals,
        "problem_summary": problem_summary,
        "focus_stage_totals": focus_stage_totals,
        "focus_problem_summary": focus_problem_summary,
        "error_summary": dict(collections.Counter(row["error_family"] for row in rows if row["error_family"])),
        "error_detail_summary": {
            "subcategories": dict(
                collections.Counter(
                    row["error_subcategory"] for row in rows if row["error_subcategory"]
                )
            ),
            "chains": dict(
                collections.Counter(
                    row["error_chain_category"] for row in rows if row["error_chain_category"]
                )
            ),
        },
        "latency": {
            "p50": round(_percentile(latencies, 0.50), 3),
            "p90": round(_percentile(latencies, 0.90), 3),
            "p99": round(_percentile(latencies, 0.99), 3),
            "max": round(max(latencies, default=0), 3),
        },
        "time_findings": time_findings,
        "latency_segments": _build_latency_segments(rows),
        "direct_data_workers": direct_data_workers,
        "urma_source_workers": urma_source_workers,
        "urma_analysis": _aggregate_urma(rows),
        "query_meta_analysis": _aggregate_query_meta(rows),
        "worker_correlation": _build_worker_correlation(rows),
        "non_transport_analysis": _aggregate_non_transport(rows),
    }
