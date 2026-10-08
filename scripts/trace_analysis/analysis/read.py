"""Read-request attribution rules and diagnostic stage refinements."""

from __future__ import annotations

import collections
import datetime as dt

from .budget import (
    _max_rpc_framework,
    _replace_rpc_network_budget,
    _rpc_framework_ms,
    _take_focus_budget,
    _urma_scheduling_detail,
)
from .contracts import (
    FOCUS_STAGE_NAMES,
    STAGE_NAMES,
)
from ..evidence.observations import facts_for, _max_evidence_ms
from ..evidence.rpc import (
    _is_query_and_get_method,
    _timestamp_value,
)
from ..evidence.urma import SLOW_WR_THRESHOLD_MS
from ..evidence.urma import _group_urma_logical_writes


def _query_and_get_breakdown(analysis: dict, worker_calls: list[dict]) -> dict:
    rpc = []
    for call in analysis["calls"]:
        if call["method"].rsplit(".", 1)[-1] != "QueryAndGet" or call.get("owner") != analysis["client_process"]:
            continue
        fields = call.get("fields_us", {})
        total = fields.get("e2e_us")
        parts = None
        names = ("network_residual_us", "server_req_queue_us", "server_exec_us")
        if call["network_ms"] is not None and all(fields.get(name, -1) >= 0 for name in names):
            subtotal = sum(fields[name] for name in names)
            if total is not None and subtotal <= total:
                parts = {"network": fields[names[0]] / 1000, "queue": fields[names[1]] / 1000,
                         "server": fields[names[2]] / 1000, "framework": (total - subtotal) / 1000}
        rpc.append({**call, "total_ms": total / 1000 if total is not None else None, "breakdown_ms": parts})
    detailed_rpc = list(rpc)
    for window in analysis.get("summary_windows", []):
        if window["stage_key"] != "client.rpc.direct_query_and_get":
            continue
        if any(call.get("owner") == window["owner"] and call.get("total_ms") is not None for call in detailed_rpc):
            continue
        rpc.append({**window, "method": "QueryAndGet", "breakdown_ms": {"summary": window["total_ms"]}})
    worker = []
    for call in worker_calls:
        phases, total = call.get("phases_ms", {}), call.get("total_ms")
        missing = [name for name in ("preprocess", "localRead", "metadata", "delivery") if name not in phases]
        complete = not missing and total is not None
        valid = complete and total >= 0 and all(value >= 0 for value in phases.values())
        delta = total - sum(phases.values()) if valid else None
        reason = ("missing_phases" if missing else "missing_total" if total is None else
                  "invalid_duration" if not valid else "phase_sum_exceeds_total" if delta < -0.005 else None)
        worker.append({**call, "stackable": reason is None, "phase_delta_ms": delta,
                       "exclusion_reason": reason, "missing_phases": missing})
    return {"rpc": rpc, "worker": worker}


def _apply_focus_breakdown(row: dict) -> None:
    legacy = row["attribution_ms"]
    focus = {
        "URMA建链": 0.0,
        "URMA通信": legacy["URMA"] + legacy["URMA超时等待"],
        "URMA调度/线程开销": 0.0,
        "QueryAndGet其他业务": legacy["QueryMeta"],
        "Get其他业务": legacy["远端供数处理"] + legacy["数据访问父窗口/未细分"],
        "其他调度/线程开销": legacy["RPC排队"],
        "RPC网络相关": legacy["RPC网络"],
        "RPC框架": 0.0,
        "未解释残差": legacy["未解释残差"],
    }
    slowest_urma_ms = (row.get("urma_trace") or {}).get("slowest_total_ms")
    if slowest_urma_ms is not None:
        slowest_urma_ms = max(slowest_urma_ms, row.get("query_urma_timeout_ms") or 0.0)
    if slowest_urma_ms is not None and focus["URMA通信"] > slowest_urma_ms:
        focus["未解释残差"] += focus["URMA通信"] - slowest_urma_ms
        focus["URMA通信"] = slowest_urma_ms
    facts = facts_for(row)
    phase_maps = facts["transport_phase_maps"]
    leaf_connect_ms = max(
        (
            sum(phases.get(name, 0) for name in ("urma_connect_info_exchange", "urma_connection_finalize"))
            / 1000.0
            for phases in phase_maps
        ),
        default=0.0,
    )
    outer_connect_ms = max(
        (
            max(
                (
                    value
                    for name, value in phases.items()
                    if name in {"connection_acquire", "connection_rebuild", "ub_fallback_connection"}
                ),
                default=0,
            )
            / 1000.0
            for phases in phase_maps
        ),
        default=0.0,
    )
    finalize_ms = max(
        (phases.get("urma_connection_finalize", 0) / 1000.0 for phases in phase_maps),
        default=0.0,
    )
    rpc_entries = [(item["method"], item["fields"]) for item in facts["rpc_entries"]]
    connect_entries = [
        fields for method, fields in rpc_entries if "ExchangeUrmaConnectInfo" in method
    ]
    connect_fields = max(connect_entries, key=lambda fields: fields.get("e2e", 0), default=None)
    connect_rpc_ms = (connect_fields.get("e2e", 0) / 1000.0) if connect_fields else 0.0
    connect_total_ms = max(leaf_connect_ms, outer_connect_ms, connect_rpc_ms + finalize_ms)
    connect_network_ms = (connect_fields.get("network_residual", 0) / 1000.0) if connect_fields else 0.0
    connect_queue_ms = (connect_fields.get("server_req_queue", 0) / 1000.0) if connect_fields else 0.0
    connect_framework_ms = _rpc_framework_ms(connect_fields) if connect_fields else None
    if connect_framework_ms is None:
        connect_network_ms = 0.0
        connect_queue_ms = 0.0
        connect_framework_ms = 0.0
    connect_business_ms = max(
        0.0, connect_total_ms - connect_network_ms - connect_queue_ms - connect_framework_ms
    )
    moved_connect_ms = _take_focus_budget(
        focus, ("Get其他业务", "未解释残差"), connect_total_ms
    )
    if connect_total_ms > 0 and moved_connect_ms > 0:
        scale = moved_connect_ms / connect_total_ms
        focus["URMA建链"] += connect_business_ms * scale
        focus["RPC网络相关"] += connect_network_ms * scale
        focus["其他调度/线程开销"] += connect_queue_ms * scale
        focus["RPC框架"] += connect_framework_ms * scale

    lock_wait_ms = max(
        (
            sum(value for name, value in phases.items() if name.endswith("_lock_wait")) / 1000.0
            for phases in phase_maps
        ),
        default=0.0,
    )
    focus["其他调度/线程开销"] += _take_focus_budget(
        focus, ("Get其他业务", "未解释残差"), lock_wait_ms
    )

    slowest_request_id = (row.get("urma_trace") or {}).get("slowest_request_id")
    urma_sched_detail = _urma_scheduling_detail(
        row.get("urma_requests", []), slowest_request_id
    )
    urma_sched_ms = max(
        (value for value in urma_sched_detail.values() if value is not None), default=0.0
    )
    moved_urma_sched_ms = min(focus["URMA通信"], urma_sched_ms)
    focus["URMA通信"] -= moved_urma_sched_ms
    focus["URMA调度/线程开销"] += moved_urma_sched_ms
    row["urma_scheduling_detail_ms"] = {
        name: round(value, 6) if value is not None else None
        for name, value in urma_sched_detail.items()
    }
    row["urma_scheduling_request_id"] = slowest_request_id

    query_entries = [fields for method, fields in rpc_entries if _is_query_and_get_method(method)]
    query_framework_ms = _max_rpc_framework(query_entries)
    outer_get_entries = [
        fields
        for method, fields in rpc_entries
        if method.endswith("WorkerOCService.Get") and "GetObjectRemote" not in method
    ]
    data_entries = []
    for method, fields in rpc_entries:
        is_outer_get = method.endswith("WorkerOCService.Get") and "GetObjectRemote" not in method
        if not _is_query_and_get_method(method) and "ExchangeUrmaConnectInfo" not in method:
            if not is_outer_get:
                data_entries.append(fields)
    outer_framework_ms = _max_rpc_framework(outer_get_entries)
    data_framework_ms = _max_rpc_framework(data_entries)
    data_queue_ms = max(
        (fields.get("server_req_queue", 0) / 1000.0 for fields in data_entries),
        default=0.0,
    )
    focus["其他调度/线程开销"] += _take_focus_budget(
        focus, ("Get其他业务",), data_queue_ms
    )
    focus["RPC框架"] += _take_focus_budget(
        focus, ("QueryAndGet其他业务",), query_framework_ms
    )
    focus["RPC框架"] += _take_focus_budget(
        focus, ("Get其他业务",), data_framework_ms
    )
    focus["RPC框架"] += _take_focus_budget(
        focus, ("未解释残差",), outer_framework_ms
    )

    _replace_rpc_network_budget(focus, row.get("rpc_analysis", {}),
                                ("未解释残差", "QueryAndGet其他业务", "Get其他业务", "URMA建链"))
    total_before = sum(legacy.values())
    rounded = {name: round(max(0.0, focus[name]), 6) for name in FOCUS_STAGE_NAMES}
    rounding_delta = round(total_before - sum(rounded.values()), 6)
    rounded["未解释残差"] = round(max(0.0, rounded["未解释残差"] + rounding_delta), 6)
    row["focus_breakdown_ms"] = rounded
    row["focus_primary_stage"] = max(FOCUS_STAGE_NAMES, key=lambda stage: rounded[stage])
    row["focus_primary_problem"] = (
        row.get("error_family")
        if row.get("error_family") and row.get("error_family") != "RPC截止超时"
        else row["focus_primary_stage"]
    )
    row["focus_breakdown_observed"] = {
        "urma_connect": connect_total_ms > 0,
        "urma_sched": urma_sched_ms > 0,
        "transport_lock_wait": lock_wait_ms > 0,
        "rpc_framework": (
            query_framework_ms > 0
            or data_framework_ms > 0
            or outer_framework_ms > 0
            or connect_framework_ms > 0
        ),
    }


def _non_transport_analysis(row: dict, topology: dict[str, object]) -> dict | None:
    if row["primary_problem"] not in {"远端供数处理", "数据访问父窗口/未细分", "未解释残差"}:
        return None

    facts = facts_for(row)
    batch_attempts = [item["fields"] for item in facts["rpc_entries"]
                      if "BatchGetObjectRemote" in item["method"]]
    batch_timeouts = [item for item in batch_attempts if item.get("cntl_error_code") == 1008]
    batch_successes = [item for item in batch_attempts if item.get("cntl_error_code", 0) == 0]
    local = facts["local_processing"]
    remote_lock_ms = facts["remote_lock_ms"]

    common = {
        "client_ms": row["client_ms"],
        "worker_process_ms": row["worker_process_ms"],
        "query_meta_ms": row["query_meta_ms"],
        "batch_e2e_ms": row["batch_e2e_ms"],
        "batch_network_ms": row["batch_network_ms"],
        "batch_server_ms": row["batch_server_ms"],
        "urma_ms": row["urma_ms"],
        "urma_observed": row["urma_observed"],
        "rpc_observed": row["rpc_observed"],
        "unexplained_ms": row["attribution_ms"]["未解释残差"],
    }

    if row.get("error_family") == "Client UB接收缓冲分配失败":
        return common | {
            "deep_category": "Client UB接收缓冲分配失败",
            "confidence": "高",
            "observed_ms": row["attribution_ms"]["未解释残差"],
            "conclusion": (
                f"Client 在 {row['client_ms']:.3f}ms 内为 UB 接收准备内存时，"
                "arena 报 fresh_extent_unavailable / Out of memory 并上浮1004。"
                "故障点在 Client 接收缓冲分配，不是已完成 WR 变慢。"
            ),
            "evidence_points": [
                "Receive buffer preparation failed",
                "fresh_extent_unavailable / Out of memory",
                "Client状态1004",
            ],
            "next_action": "检查 Client arena 按 NUMA 的容量、fresh extent 补充/回收和同时到达的 8MiB 接收缓冲需求。",
        }

    if row["primary_problem"] == "未解释残差":
        rpc_window = (
            f"已记录 server_exec/network residual 均未覆盖该窗口"
            if row["rpc_observed"]
            else "未观测到可关联的 RPC server_exec/network residual"
        )
        parent_name = "Client direct_get_data" if row["direct_read_observed"] else "Worker ProcessGet"
        conclusion = (
            f"Client 可见窗口为 {row['client_ms']:.3f}ms，但{rpc_window}；"
            f"{parent_name} 父窗口为 {row['worker_process_ms']:.3f}ms。该证据不能证明网络耗时，也不能"
            "定位请求进入 handler 前的传输、框架调度或跨端时间差，结论是 Client/Worker 观测未闭合。"
        )
        return common | {
            "deep_category": "Client/Worker观测未闭合",
            "confidence": "中",
            "observed_ms": row["attribution_ms"]["未解释残差"],
            "conclusion": conclusion,
            "evidence_points": [
                f"Client总时延 {row['client_ms']:.3f}ms，状态={row['status']}",
                (
                    f"Client RPC server_exec {row['client_rpc_server_ms']:.3f}ms / "
                    f"network residual {row['client_rpc_network_ms']:.3f}ms"
                    if row["rpc_observed"]
                    else "Client RPC breakdown 未观测"
                ),
                f"{parent_name}父窗口 {row['worker_process_ms']:.3f}ms",
            ],
            "next_action": "补齐 Client发送、Worker收包、进入handler、响应发送四点同源时间戳与队列等待埋点。",
        }

    if row.get("data_access_scope") == "Data Worker供数处理慢":
        breakdown = row["access_path_breakdown"]
        pull_ms = breakdown.get("provider_pull_ms")
        finish_ms = breakdown.get("provider_finish_ms")
        logical_urma_ms = breakdown.get("logical_urma_write_ms")
        observed_ms = max(value for value in (pull_ms, finish_ms, 0.0) if value is not None)
        pull_text = f"{pull_ms:.3f}ms" if pull_ms is not None else "未观测"
        finish_text = f"{finish_ms:.3f}ms" if finish_ms is not None else "未观测"
        urma_text = f"{logical_urma_ms:.3f}ms" if logical_urma_ms is not None else "未观测"
        return common | {
            "deep_category": "Data Worker供数处理慢",
            "confidence": "高",
            "observed_ms": observed_ms,
            "conclusion": (
                f"Processing pull {pull_text}；GetObjectRemote finish {finish_text}；"
                f"逻辑 URMA Write {urma_text}。供数端处理覆盖数据窗口主体，"
                "而 URMA 完成较快，卡点位于 Data Worker 供数处理，不是 RPC 网络或 URMA completion。"
            ),
            "evidence_points": [
                f"Processing pull {pull_text}",
                f"GetObjectRemote finish {finish_text}",
                f"逻辑 URMA Write {urma_text}",
            ],
            "next_action": "在 GetObjectRemoteHandler 内细分对象查找、buffer准备、URMA post 前等待和响应构建。",
        }

    if batch_timeouts:
        first_timeout_ms = batch_timeouts[0].get("e2e", 0) / 1000.0
        success_ms = batch_successes[-1].get("e2e", 0) / 1000.0 if batch_successes else 0.0
        urma_evidence = (
            f"URMA 已观测最大 {row['urma_ms']:.3f}ms"
            if row["urma_observed"]
            else "未观测到可关联的 URMA 证据"
        )
        batch_path = str(topology["batch_get_path"])
        if batch_successes:
            conclusion = (
                f"第一次 {batch_path} BatchGet 约 {first_timeout_ms:.3f}ms、命中请求截止点后超时，"
                f"第二次约 {success_ms:.3f}ms 成功；{urma_evidence}。"
                "已确认卡点是首轮 BatchGet 超时及重试窗口；仅在有 URMA 观测时才能比较 UB 执行时延。"
            )
        else:
            conclusion = (
                f"{batch_path} BatchGet 在约 {first_timeout_ms:.3f}ms 的尝试中超时，"
                f"整段远端获取父窗口达到 {row['batch_e2e_ms']:.3f}ms；日志带重试证据，{urma_evidence}。"
            )
        return common | {
            "deep_category": "BatchGet超时/重试",
            "confidence": "高",
            "observed_ms": row["batch_e2e_ms"],
            "conclusion": conclusion,
            "evidence_points": [
                f"BatchGet超时尝试 {len(batch_timeouts)} 次，首次 {first_timeout_ms:.3f}ms",
                f"成功尝试 {len(batch_successes)} 次" + (f"，最后 {success_ms:.3f}ms" if batch_successes else ""),
                urma_evidence,
            ],
            "next_action": "关联 BatchGet 每次 Data Worker、deadline 预算和 Retry detail，检查首轮响应为何未在既定预算内完成。",
        }

    if row["primary_problem"] == "远端供数处理":
        urma_boundary = (
            f"URMA 已观测最大 {row['urma_ms']:.3f}ms"
            if row["urma_observed"]
            else "URMA 证据未观测，server_exec 可能仍包含未分离的 UB 子阶段"
        )
        if remote_lock_ms is not None:
            lock_ms = remote_lock_ms
            conclusion = (
                f"远端 BatchGet server_exec {row['batch_server_ms']:.3f}ms，占 BatchGet "
                f"{row['batch_e2e_ms']:.3f}ms 的主体；RemotePull 明确记录 RemoteLockEntry {lock_ms:.3f}ms，"
                f"{urma_boundary}；已确认供数端 RemoteLockEntry 是其中的显著窗口。"
            )
            evidence = f"RemoteLockEntry {lock_ms:.3f}ms"
            confidence = "高"
        else:
            conclusion = (
                f"远端 BatchGet server_exec {row['batch_server_ms']:.3f}ms，占 BatchGet "
                f"{row['batch_e2e_ms']:.3f}ms 的主体，而网络 residual 仅 {row['batch_network_ms']:.3f}ms、"
                f"{urma_boundary}；可确定为供数端 handler 父窗口，但现有日志未继续细分内部阶段。"
            )
            evidence = "RemotePull内部子阶段未记录"
            confidence = "中"
        return common | {
            "deep_category": "Data Worker服务端处理",
            "confidence": confidence,
            "observed_ms": row["batch_server_ms"],
            "conclusion": conclusion,
            "evidence_points": [
                f"BatchGet server_exec {row['batch_server_ms']:.3f}ms / e2e {row['batch_e2e_ms']:.3f}ms",
                f"BatchGet network residual {row['batch_network_ms']:.3f}ms",
                urma_boundary,
                evidence,
            ],
            "next_action": "在远端 BatchGet handler 内细分锁等待、对象查找、buffer准备和响应序列化。",
        }

    if local is not None and local["remote_objects"] == 0:
        local_ms = local['cost_us'] / 1000.0
        return common | {
            "deep_category": "明确本地ProcessGet耗时",
            "confidence": "高",
            "observed_ms": local_ms,
            "conclusion": (
                f"该 Data Worker 明确记录 Local processing {local_ms:.3f}ms、remoteObjects=0，"
                "该 Trace 没有远端 BatchGet/URMA 子请求证据；主要可观测窗口在本地 ProcessGet。"
            ),
            "evidence_points": [
                f"Local processing costUs={local['cost_us']}",
                "remoteObjects=0",
                f"Worker ProcessGet父窗口 {row['worker_process_ms']:.3f}ms",
            ],
            "next_action": "在本地 ProcessGet 内细分对象锁、内存查找、数据准备、拷贝和响应附件构建。",
        }

    known_data_ms = max(row["batch_e2e_ms"], row.get("data_rpc_e2e_ms") or 0)
    known_ms = row["query_meta_ms"] + known_data_ms
    internal_gap_ms = max(0.0, row["worker_process_ms"] - known_ms)
    if row["direct_read_observed"]:
        return common | {
            "deep_category": "Client数据获取窗口未细分",
            "confidence": "中",
            "observed_ms": internal_gap_ms,
            "conclusion": (
                f"Client direct_get_data 父窗口 {row['worker_process_ms']:.3f}ms，而已知 QueryMeta+Data RPC "
                f"仅 {known_ms:.3f}ms，剩余约 {internal_gap_ms:.3f}ms 未细分。该窗口在 Client 侧观测，"
                "不能写成 Data Worker 本地处理，也不能在缺少 RPC trailer 时写成网络耗时。"
            ),
            "evidence_points": [
                f"Client direct_get_data父窗口 {row['worker_process_ms']:.3f}ms",
                f"QueryMeta {row['query_meta_ms']:.3f}ms + Data RPC {known_data_ms:.3f}ms",
                f"Client数据获取未细分约 {internal_gap_ms:.3f}ms",
            ],
            "next_action": "补齐 Client direct_get_data 内路由、RPC发起/完成、URMA等待与materialize子阶段。",
        }
    return common | {
        "deep_category": "ProcessGet内部未细分",
        "confidence": "中",
        "observed_ms": internal_gap_ms,
        "conclusion": (
            f"ProcessGet父窗口 {row['worker_process_ms']:.3f}ms，而已知 QueryMeta+BatchGet 仅 "
            f"{known_ms:.3f}ms，剩余约 {internal_gap_ms:.3f}ms 没有子阶段。结论是 ProcessGet 内部观测盲区，"
            "不能直接等价为本地 CPU、锁等待或调度。"
        ),
        "evidence_points": [
            f"Worker ProcessGet父窗口 {row['worker_process_ms']:.3f}ms",
            f"QueryMeta {row['query_meta_ms']:.3f}ms + BatchGet {row['batch_e2e_ms']:.3f}ms",
            f"内部未细分约 {internal_gap_ms:.3f}ms",
        ],
        "next_action": "补齐 ProcessGetObjectRequest 的锁、查找、等待远端future、buffer/attachment和调度子阶段。",
    }


def _urma_critical_path(logical_writes: list[dict]) -> tuple[float, str]:
    candidates = [
        (item["slowest_wr_ms"], "最慢WR")
        for item in logical_writes
    ]
    critical_ms, basis = max(candidates, key=lambda item: item[0])
    return critical_ms, basis


def _sequential_urma_path(logical_writes: list[dict]) -> tuple[float, str]:
    durations = [item["slowest_wr_ms"] for item in logical_writes]
    basis = f"{len(logical_writes)}个串行逻辑Write的最慢WR之和"
    return sum(durations), basis


def _apply_inline_query_urma_attribution(row: dict) -> None:
    row["inline_query_urma_ms"] = None
    row["inline_query_urma_basis"] = None
    row["query_and_get_parent_ms"] = None
    row["query_and_get_exclusive_ms"] = None
    row["query_meta_exclusive_ms"] = row["attribution_ms"]["QueryMeta"]
    inline_candidates = []
    facts = facts_for(row)
    for item in facts["inline_attempts"]:
        worker = item["worker"]
        if worker in {None, "", "unknown", "未明确"}:
            return
        end = _timestamp_value(item["timestamp"])
        if end is None:
            return
        total_ms = item["total_ms"]
        inline_candidates.append({"worker": worker, "start": end - dt.timedelta(milliseconds=total_ms),
                                  "end": end, "total_ms": total_ms})
    if not inline_candidates:
        for item in facts["query_access_attempts"]:
            end, worker = _timestamp_value(item["timestamp"]), item["worker"]
            if end is None or worker in {None, "", "unknown", "未明确"}:
                continue
            total_ms = item["total_ms"]
            inline_candidates.append({"worker": worker, "start": end - dt.timedelta(milliseconds=total_ms),
                                      "end": end, "total_ms": total_ms})
    if not inline_candidates or not row.get("client_query_and_get_ms"):
        return

    requests_by_attempt = [[] for _ in inline_candidates]
    tolerance = dt.timedelta(milliseconds=1)
    inline_workers = {attempt["worker"] for attempt in inline_candidates}
    for request in row.get("urma_requests", []):
        request_time = _timestamp_value(str(request.get("timestamp") or ""))
        if request_time is None:
            if request.get("source_worker") in inline_workers:
                return
            continue
        matches = []
        for index, attempt in enumerate(inline_candidates):
            same_worker = request.get("source_worker") == attempt["worker"]
            inside_window = (
                attempt["start"] - tolerance <= request_time <= attempt["end"] + tolerance
            )
            if same_worker and inside_window:
                matches.append(index)
        if len(matches) > 1:
            return
        if len(matches) == 1:
            requests_by_attempt[matches[0]].append(request)

    paths_by_worker = collections.defaultdict(list)
    for attempt, inline_requests in zip(inline_candidates, requests_by_attempt):
        if inline_requests:
            logical_writes = _group_urma_logical_writes(inline_requests)
            path_ms, basis = _sequential_urma_path(logical_writes)
            clamped_ms = min(attempt["total_ms"], path_ms)
            if clamped_ms < path_ms:
                basis += "·QueryAndGet父窗口clamp"
            paths_by_worker[attempt["worker"]].append((clamped_ms, basis))
    if not paths_by_worker:
        return

    worker_paths = max(paths_by_worker.values(), key=lambda paths: sum(item[0] for item in paths))
    inline_ms = sum(item[0] for item in worker_paths)
    basis = (
        worker_paths[0][1]
        if len(worker_paths) == 1
        else f"{len(worker_paths)}次QueryAndGet尝试关键路径之和"
    )
    query_ms = row["attribution_ms"]["QueryMeta"]
    moved_from_query = min(query_ms, inline_ms)
    current_urma_ms = row["attribution_ms"]["URMA"]
    added_to_urma = min(moved_from_query, max(0.0, inline_ms - current_urma_ms))
    row["attribution_ms"]["QueryMeta"] = round(query_ms - moved_from_query, 6)
    row["attribution_ms"]["URMA"] = round(current_urma_ms + added_to_urma, 6)
    row["attribution_ms"]["数据访问父窗口/未细分"] = round(
        row["attribution_ms"]["数据访问父窗口/未细分"]
        + moved_from_query
        - added_to_urma,
        6,
    )
    row["inline_query_urma_ms"] = round(inline_ms, 6)
    row["inline_query_urma_basis"] = basis
    row["query_meta_exclusive_ms"] = row["attribution_ms"]["QueryMeta"]
    row["query_and_get_parent_ms"] = row["client_query_and_get_ms"]
    row["query_and_get_exclusive_ms"] = row["query_meta_exclusive_ms"]
    row["primary_stage"] = max(STAGE_NAMES, key=lambda stage: row["attribution_ms"][stage])
    if not row.get("error_family") or row.get("error_family") == "RPC截止超时":
        row["primary_problem"] = row["primary_stage"]


def _apply_query_rpc_attribution(row: dict) -> None:
    row["query_rpc_breakdown_observed"] = False
    row["query_rpc_network_ms"] = None
    row["query_rpc_queue_ms"] = None
    entries = [item["fields"] for item in facts_for(row)["rpc_entries"]
               if _is_query_and_get_method(item["method"])]
    if len(entries) != 1:
        return
    fields = entries[0]
    if fields.get("cntl_failed") or fields.get("cntl_error_code"):
        return
    if "network_residual" not in fields and "server_req_queue" not in fields:
        return

    query_ms = row["attribution_ms"]["QueryMeta"]
    network_ms = fields.get("network_residual", 0) / 1000.0
    queue_ms = fields.get("server_req_queue", 0) / 1000.0
    moved_network_ms = min(query_ms, network_ms)
    remaining_ms = query_ms - moved_network_ms
    moved_queue_ms = min(remaining_ms, queue_ms)
    remaining_ms -= moved_queue_ms
    row["attribution_ms"]["RPC网络"] = round(
        row["attribution_ms"]["RPC网络"] + moved_network_ms, 6
    )
    row["attribution_ms"]["RPC排队"] = round(
        row["attribution_ms"]["RPC排队"] + moved_queue_ms, 6
    )
    row["attribution_ms"]["QueryMeta"] = round(remaining_ms, 6)
    row["query_meta_exclusive_ms"] = row["attribution_ms"]["QueryMeta"]
    row["query_rpc_breakdown_observed"] = True
    row["query_rpc_network_ms"] = round(moved_network_ms, 6)
    row["query_rpc_queue_ms"] = round(moved_queue_ms, 6)
    row["primary_stage"] = max(STAGE_NAMES, key=lambda stage: row["attribution_ms"][stage])
    if not row.get("error_family") or row.get("error_family") == "RPC截止超时":
        row["primary_problem"] = row["primary_stage"]


def _apply_query_urma_timeout_attribution(row: dict) -> None:
    row["query_urma_timeout_ms"] = None
    row["query_urma_timeout_basis"] = None
    row["query_urma_timeout_parent_ms"] = None
    if not row.get("urma_timeout_observed"):
        return

    query_attempts = []
    facts = facts_for(row)
    for item in facts["query_attempts"]:
        end, worker = _timestamp_value(item["timestamp"]), item["worker"]
        if end is None or worker in {None, "", "unknown", "未明确"}:
            continue
        total_ms = item["total_ms"]
        query_attempts.append({"worker": worker, "start": end - dt.timedelta(milliseconds=total_ms),
                               "end": end, "total_ms": total_ms})

    timeout_events = {}
    for item in facts["timeout_events"]:
        timestamp, worker = _timestamp_value(item["timestamp"]), item["worker"]
        if timestamp is None or worker in {None, "", "unknown", "未明确"}:
            continue
        identity = (worker, item["request_id"] or timestamp.isoformat())
        event = {"worker": worker, "timestamp": timestamp, "elapsed_ms": item["elapsed_ms"]}
        current = timeout_events.get(identity)
        if current is None or timestamp < current["timestamp"]:
            timeout_events[identity] = event

    tolerance = dt.timedelta(milliseconds=1)
    nested_groups = []
    for attempt in query_attempts:
        nested = []
        for event in timeout_events.values():
            same_worker = event["worker"] == attempt["worker"]
            inside_window = (
                attempt["start"] - tolerance <= event["timestamp"] <= attempt["end"] + tolerance
            )
            if same_worker and inside_window:
                nested.append(event)
        if nested:
            nested_groups.append((attempt, nested))
    if len(nested_groups) != 1 or len(nested_groups[0][1]) != 1:
        return

    attempt, nested = nested_groups[0]
    timeout = nested[0]
    query_ms = row["attribution_ms"]["QueryMeta"]
    moved_ms = min(query_ms, timeout["elapsed_ms"])
    if moved_ms <= 0:
        return
    row["attribution_ms"]["QueryMeta"] = round(query_ms - moved_ms, 6)
    row["attribution_ms"]["URMA超时等待"] = round(moved_ms, 6)
    row["query_meta_exclusive_ms"] = row["attribution_ms"]["QueryMeta"]
    row["query_urma_timeout_ms"] = round(moved_ms, 6)
    row["query_urma_timeout_basis"] = "同Worker QueryAndGet父窗口内唯一URMA_WAIT_TIMEOUT"
    row["query_urma_timeout_parent_ms"] = round(attempt["total_ms"], 6)
    row["primary_stage"] = max(STAGE_NAMES, key=lambda stage: row["attribution_ms"][stage])
    if row["primary_stage"] == "URMA超时等待":
        row["primary_problem"] = "URMA超时"


def _query_meta_detail(row: dict) -> dict | None:
    """Return one exclusive QueryAndGet diagnosis plus orthogonal TryGet evidence."""

    facts = facts_for(row)
    worker_query_done_observed = facts["worker_query_done_observed"]
    local_read_ms = facts["durations_ms"]["local_read"]
    entries = [item["fields"] for item in facts_for(row)["rpc_entries"]
               if _is_query_and_get_method(item["method"])]
    if not entries and not row.get("query_meta_ms"):
        return None

    query_total_ms = float(row.get("query_meta_ms") or 0.0)
    rpc_e2e_ms = max((entry.get("e2e", 0) for entry in entries), default=0) / 1000.0
    rpc_network_ms = max((entry.get("network_residual", 0) for entry in entries), default=0) / 1000.0
    rpc_server_ms = max((entry.get("server_exec", 0) for entry in entries), default=0) / 1000.0
    rpc_queue_ms = max((entry.get("server_req_queue", 0) for entry in entries), default=0) / 1000.0
    failed_attempt_observed = any(
        entry.get("cntl_failed") or entry.get("cntl_error_code") for entry in entries
    )
    query_rpc_failed = row.get("failure_reason") == "QueryMeta RPC deadline"
    retry_observed = len(entries) > 1 or (
        rpc_e2e_ms and query_total_ms and rpc_e2e_ms < query_total_ms * 0.5
    )
    legacy_try_get_urma_observed = bool(row.get("urma_requests")) and facts["legacy_pull_src_sentinel"]
    try_get_urma_observed = (
        row.get("inline_query_urma_ms") is not None or legacy_try_get_urma_observed
    )
    if row.get("inline_query_urma_ms") is not None:
        slow_urma = row["inline_query_urma_ms"] > SLOW_WR_THRESHOLD_MS
    else:
        slow_urma = legacy_try_get_urma_observed and any(
            float(request.get("total_ms", 0) or 0) > SLOW_WR_THRESHOLD_MS
            for request in row.get("urma_requests", [])
        )
    urma_max_ms = max(
        (float(request.get("total_ms", 0) or 0) for request in row.get("urma_requests", [])),
        default=None,
    )
    local_read_dominates = local_read_ms is not None and local_read_ms >= max(
        1.0, query_total_ms * 0.5
    )

    if row.get("failure_reason") == "Data URMA建链截止超时" and not query_rpc_failed:
        category = "QueryAndGet成功·后续URMA建链失败"
        boundary = "QueryAndGet已成功；最终失败点是后续WorkerWorkerExchangeUrmaConnectInfo，不计为QueryMeta超时"
    elif row.get("failure_reason") == "Data RPC deadline" and not query_rpc_failed:
        category = "QueryAndGet成功·后续Data RPC失败"
        boundary = "QueryAndGet已成功；最终失败点是后续GetObjectRemote，不计为QueryMeta超时"
    elif query_rpc_failed:
        if retry_observed:
            category = "QueryAndGet超时·重试累计窗口"
            boundary = "末次失败RPC仅覆盖总QueryAndGet窗口的一部分；其余为前序尝试/退避，服务端明细未闭合"
        else:
            category = "QueryAndGet超时·服务端明细未闭合"
            boundary = "失败RPC无完整server trailer；0值不是实测，不能区分Meta Owner执行、响应与网络"
    elif slow_urma:
        category = "QueryAndGet TryGet·URMA慢"
        boundary = "同Trace本地TryGet产生慢WR，严格按URMA_ELAPSED_TOTAL >1.5ms"
    elif worker_query_done_observed and not try_get_urma_observed and local_read_dominates:
        category = "QueryAndGet localRead慢·URMA未观测"
        boundary = (
            "Worker QueryAndGet localRead覆盖父窗口主体，但同Trace未保留URMA完成明细；"
            "只能定位到localRead/EncodeLocalHit父窗口，不能确认慢WR"
        )
    elif not entries and not worker_query_done_observed:
        category = "QueryAndGet父窗口·服务端未观测"
        boundary = (
            "仅观测到Client QueryAndGet父窗口，缺少RPC trailer和Worker QueryAndGet done；"
            "不能区分通信残差、Worker排队/处理或inline URMA"
        )
    elif retry_observed:
        category = "QueryAndGet成功·重试/多次尝试累计"
        boundary = "已保留的成功RPC不足以覆盖QueryAndGet总窗口；差值归入前序尝试/退避，不归网络"
    elif rpc_e2e_ms and rpc_network_ms >= rpc_e2e_ms * 0.8:
        category = "QueryAndGet成功·RPC residual主导"
        boundary = "network_residual为RPC未被queue/server解释的残差；不等同于已证明物理网络慢"
    elif rpc_e2e_ms and rpc_queue_ms >= rpc_e2e_ms * 0.5:
        category = "QueryAndGet成功·Meta Owner排队主导"
        boundary = "server_req_queue覆盖RPC主体，定位到Meta Owner进入handler前"
    elif rpc_e2e_ms and rpc_server_ms >= rpc_e2e_ms * 0.5:
        category = "QueryAndGet成功·Meta Owner处理主导"
        boundary = "server_exec覆盖RPC主体；可继续结合QueryAndGet内部TryGet与元数据子阶段"
    else:
        category = "QueryAndGet成功·内部未细分"
        boundary = "现有RPC字段与TryGet证据不足以闭合QueryAndGet内部窗口"

    return {
        "category": category,
        "query_total_ms": round(query_total_ms, 6),
        "query_exclusive_ms": row.get("query_and_get_exclusive_ms"),
        "inline_urma_ms": row.get("inline_query_urma_ms"),
        "inline_urma_basis": row.get("inline_query_urma_basis"),
        "rpc_e2e_ms": round(rpc_e2e_ms, 6),
        "rpc_network_residual_ms": round(rpc_network_ms, 6),
        "rpc_server_ms": round(rpc_server_ms, 6),
        "rpc_queue_ms": round(rpc_queue_ms, 6),
        "query_rpc_failed": bool(query_rpc_failed),
        "failed_attempt_observed": bool(failed_attempt_observed),
        "rpc_attempt_count": len(entries),
        "try_get_urma_observed": try_get_urma_observed,
        "slow_urma": slow_urma,
        "worker_query_done_observed": worker_query_done_observed,
        "local_read_ms": round(local_read_ms, 6) if local_read_ms is not None else None,
        "urma_max_ms": round(urma_max_ms, 6) if urma_max_ms is not None else None,
        "boundary": boundary,
    }


def _refine_data_access_scope(row: dict) -> None:
    """Replace broad parent-window labels when trace-local evidence closes the path."""

    durations = facts_for(row)["durations_ms"]
    client_transfer_ms = durations["client_transfer"]
    provider_pull_ms = durations["provider_pull"]
    provider_finish_ms = durations["provider_finish"]
    data_parent_ms = row.get("direct_get_data_ms") or row.get("worker_process_ms") or 0.0
    logical_write_ms = row.get("urma_critical_path_ms")
    closure_candidates = [
        value
        for value in (client_transfer_ms, provider_finish_ms, provider_pull_ms, logical_write_ms)
        if value is not None
    ]
    closed_ms = max(closure_candidates, default=0.0)
    closure_ratio = closed_ms / data_parent_ms * 100 if data_parent_ms else None
    row["access_path_breakdown"] = {
        "data_parent_ms": round(data_parent_ms, 6),
        "client_data_transfer_ms": round(client_transfer_ms, 6) if client_transfer_ms is not None else None,
        "provider_pull_ms": round(provider_pull_ms, 6) if provider_pull_ms is not None else None,
        "provider_finish_ms": round(provider_finish_ms, 6) if provider_finish_ms is not None else None,
        "logical_urma_write_ms": round(logical_write_ms, 6) if logical_write_ms is not None else None,
        "closure_ratio_pct": round(closure_ratio, 3) if closure_ratio is not None else None,
    }

    if row.get("urma_timeout_observed"):
        row["data_access_scope"] = "URMA等待超时"
        row["data_access_evidence"] = (
            f"已观测 URMA_WAIT_TIMEOUT，timeout elapsedMs "
            f"{row['urma_timeout_max_ms']:.3f}ms"
            if row.get("urma_timeout_max_ms") is not None
            else "已观测 URMA_WAIT_TIMEOUT；完成态耗时未观测"
        )
        return
    inline_urma_ms = row.get("inline_query_urma_ms")
    if inline_urma_ms is not None:
        parent_ms = row.get("query_and_get_parent_ms") or 0.0
        exclusive_ms = row.get("query_and_get_exclusive_ms") or 0.0
        if inline_urma_ms > exclusive_ms:
            row["data_access_scope"] = "QueryAndGet inline URMA"
            row["data_access_evidence"] = (
                f"同 Worker/同 attempt 唯一匹配；QueryAndGet父窗口 {parent_ms:.3f}ms，"
                f"inline URMA关键路径 {inline_urma_ms:.3f}ms，独占 {exclusive_ms:.3f}ms；"
                "单次逻辑Write的WR分片取最慢URMA Elapsed Time，不求和"
            )
            return
        query_rpc_network_ms = row.get("query_rpc_network_ms") or 0.0
        query_server_exclusive_ms = row.get("query_meta_exclusive_ms") or 0.0
        if query_rpc_network_ms >= max(1.0, query_server_exclusive_ms):
            row["data_access_scope"] = "QueryAndGet RPC通信残差慢"
            row["data_access_evidence"] = (
                f"QueryAndGet父窗口 {parent_ms:.3f}ms，inline URMA {inline_urma_ms:.3f}ms，"
                f"RPC通信残差 {query_rpc_network_ms:.3f}ms，排队 "
                f"{(row.get('query_rpc_queue_ms') or 0.0):.3f}ms；"
                "通信残差包含网络与RPC框架，不能直接定责物理网络"
            )
            return
        row["data_access_scope"] = "QueryAndGet独占窗口"
        row["data_access_evidence"] = (
            f"QueryAndGet父窗口 {parent_ms:.3f}ms，已剥离 inline URMA {inline_urma_ms:.3f}ms，"
            f"剩余独占 {exclusive_ms:.3f}ms；独占窗口大于 inline URMA"
        )
        return
    if row.get("failure_reason") in {
        "Data RPC deadline",
        "Data URMA建链截止超时",
        "QueryMeta RPC deadline",
    }:
        return

    query_rpc_network_ms = row.get("query_rpc_network_ms") or 0.0
    if query_rpc_network_ms >= max(1.0, row.get("query_meta_exclusive_ms") or 0.0):
        row["data_access_scope"] = "QueryAndGet RPC通信残差慢"
        row["data_access_evidence"] = (
            f"RPC通信残差 {query_rpc_network_ms:.3f}ms，排队 "
            f"{(row.get('query_rpc_queue_ms') or 0.0):.3f}ms；"
            "已从QueryAndGet父窗口互斥剥离，但仍包含网络与RPC框架，不能直接定责物理网络"
        )
        return

    queue_ms = row.get("client_rpc_queue_ms") or 0.0
    outer_e2e_ms = row.get("client_rpc_e2e_ms") or 0.0
    if queue_ms >= 1.0 and outer_e2e_ms and queue_ms >= outer_e2e_ms * 0.5:
        row["data_access_scope"] = "Client→Worker RPC排队慢"
        row["data_access_evidence"] = (
            f"外层 Get e2e {outer_e2e_ms:.3f}ms，server_req_queue {queue_ms:.3f}ms，"
            f"server_exec {row['client_rpc_server_ms']:.3f}ms；主要耗时在服务端进入 handler 前，"
            "不是 SHM拷贝耗时"
        )
        return

    outer_network_ms = row.get("client_rpc_network_ms") or 0.0
    if outer_network_ms >= max(1.0, row.get("client_rpc_server_ms") or 0.0):
        row["data_access_scope"] = "Client→Worker RPC网络慢"
        delivery_note = "；最终SHM交付与外层RPC网络是两个不同窗口" if row.get("transport") == "SHM" else ""
        row["data_access_evidence"] = (
            f"外层 Get e2e {outer_e2e_ms:.3f}ms，network residual {outer_network_ms:.3f}ms，"
            f"server_req_queue {queue_ms:.3f}ms，server_exec {row['client_rpc_server_ms']:.3f}ms"
            f"{delivery_note}"
        )
        return

    if row.get("query_meta_ms", 0.0) >= max(1.0, data_parent_ms * 0.5):
        row["data_access_scope"] = "QueryMeta慢"
        row["data_access_evidence"] = (
            f"QueryMeta {row['query_meta_ms']:.3f}ms，占数据访问父窗口主体；"
            "定位到元数据请求窗口，不归入数据传输"
        )
        return

    provider_ms = max(value for value in (provider_pull_ms, provider_finish_ms, 0.0) if value is not None)
    urma_ms = logical_write_ms or 0.0
    if provider_ms >= 1.0 and urma_ms < provider_ms * 0.5:
        parent_bucket = row["attribution_ms"]["数据访问父窗口/未细分"]
        movable = min(parent_bucket, max(0.0, provider_ms - urma_ms))
        row["attribution_ms"]["数据访问父窗口/未细分"] = round(parent_bucket - movable, 6)
        row["attribution_ms"]["远端供数处理"] = round(
            row["attribution_ms"]["远端供数处理"] + movable, 6
        )
        row["primary_stage"] = max(STAGE_NAMES, key=lambda stage: row["attribution_ms"][stage])
        row["primary_problem"] = row["primary_stage"]
        row["data_access_scope"] = "Data Worker供数处理慢"
        pull_text = f"{provider_pull_ms:.3f}ms" if provider_pull_ms is not None else "未观测"
        finish_text = f"{provider_finish_ms:.3f}ms" if provider_finish_ms is not None else "未观测"
        urma_text = f"{logical_write_ms:.3f}ms" if logical_write_ms is not None else "未观测"
        row["data_access_evidence"] = (
            f"Processing pull {pull_text}；GetObjectRemote finish {finish_text}；"
            f"逻辑 URMA Write {urma_text}。"
            "供数处理窗口明显大于 URMA 完成窗口"
        )
        return

    if logical_write_ms is not None and logical_write_ms > SLOW_WR_THRESHOLD_MS:
        ratio = logical_write_ms / data_parent_ms * 100 if data_parent_ms else 0.0
        if ratio >= 70.0:
            row["data_access_scope"] = "URMA慢完成"
            row["data_access_evidence"] = (
                f"最慢URMA Elapsed Time {logical_write_ms:.3f}ms，占数据窗口 {ratio:.1f}%；"
                + (
                    f"Client data_transfer {client_transfer_ms:.3f}ms，"
                    if client_transfer_ms is not None
                    else "Client data_transfer未观测，"
                )
                + "WR阈值严格按 >1.5ms"
            )
            return

    if row.get("data_access_scope") in {
        "Client数据获取父窗口未闭合",
        "Worker ProcessGet父窗口未细分",
        "Client/Worker观测未闭合",
    }:
        row["data_access_scope"] = "证据不足·数据访问窗口未闭合"
        row["data_access_evidence"] = (
            "现有 Trace 未观测到足以闭合父窗口的 QueryMeta、RPC queue/network/server、"
            "Data Worker Processing pull 或完整逻辑 URMA Write；不确定具体卡点"
        )


def _apply_explicit_rpc_errors(row: dict) -> None:
    errors = []
    for call in row.get("rpc_analysis", {}).get("calls", []):
        if not (call.get("cntl_failed") or call.get("cntl_error_code")):
            continue
        method = call["method"]
        kind = "RPC截止超时" if call.get("cntl_error_code") == 1008 else "RPC失败"
        label = ("URMA建链握手 " if "ExchangeUrmaConnectInfo" in method else "") + method + " " + kind
        if label not in errors:
            errors.append(label)
    row["explicit_rpc_errors"] = errors
    if errors and (not row.get("error_family") or row.get("error_subcategory") == "RPC deadline·方法未细分"):
        row["error_family"] = "RPC报错"
        row["error_subcategory"] = "；".join(errors)
        row["failure_reason"] = row["error_subcategory"]
        row["error_failure_point"] = row["error_subcategory"]
        row["error_root_cause_boundary"] = "已定位到报错方法和controller错误；底层原因需结合该调用时序核验。"
        row["data_access_scope"] = row["error_subcategory"]
        row["data_access_evidence"] = "结构化RPC错误记录；内部调用报错与Client最终状态分别统计。"
