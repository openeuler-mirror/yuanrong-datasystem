#!/usr/bin/env python3
"""Build a reusable TopN bottleneck report from a ds-trace-triage run directory."""

from __future__ import annotations

import argparse
import collections
import datetime as dt
import html
import json
import math
import re
import shutil
from pathlib import Path


CATEGORY_REMOTE = "明确远端数据阶段"
CATEGORY_WORKER = "数据访问处理未细分"
CATEGORY_CLIENT_RPC = "Client→Data Worker RPC residual"
STAGE_NAMES = (
    "RPC网络",
    "RPC排队",
    "QueryMeta",
    "URMA超时等待",
    "URMA",
    "远端供数处理",
    "数据访问父窗口/未细分",
    "未解释残差",
)
FOCUS_STAGE_NAMES = (
    "URMA建链",
    "URMA通信",
    "URMA调度/线程开销",
    "QueryAndGet其他业务",
    "Get其他业务",
    "其他调度/线程开销",
    "RPC网络相关",
    "RPC框架",
    "未解释残差",
)
WRITE_STAGE_NAMES = (
    "Create RPC其他",
    "写入MemoryCopy",
    "写入URMA通信",
    "写入URMA调度/线程开销",
    "Publish RPC其他",
    "Worker Publish/元数据",
    "其他调度/线程开销",
    "RPC网络相关",
    "RPC框架",
    "未解释残差",
)
WRITE_CLIENT_FLOWS = frozenset(
    {
        "DS_KV_CLIENT_SET",
        "DS_KV_CLIENT_MSET",
        "DS_KV_CLIENT_CREATE",
        "DS_KV_CLIENT_PUBLISH",
    }
)
WRITE_CLIENT_OPERATION_RE = "|".join(
    re.escape(name.removeprefix("DS_KV_CLIENT_")) for name in sorted(WRITE_CLIENT_FLOWS)
)
PROBLEM_NAMES = tuple(stage for stage in STAGE_NAMES if stage != "URMA超时等待") + ("URMA超时",)
NON_TRANSPORT_CATEGORIES = (
    "Client UB接收缓冲分配失败",
    "Client/Worker观测未闭合",
    "BatchGet超时/重试",
    "Data Worker服务端处理",
    "明确本地ProcessGet耗时",
    "Client数据获取窗口未细分",
    "ProcessGet内部未细分",
)
SLOW_WR_THRESHOLD_MS = 1.5
URMA_WAIT_TIMEOUT_RE = re.compile(
    r"(?:URMA(?:[_ -]WAIT[_ -]TIMEOUT)|Timed out waiting for urma_request_id)", re.I
)
QUERY_AND_GET_METHOD_RE = re.compile(r"(?:Master|Worker)OCService\.QueryAndGet", re.I)


class InputContractError(ValueError):
    """Raised when a triage run directory does not satisfy the input contract."""


def _topology_contract(local_cache: bool | None, read_path: str | None = None) -> dict[str, object]:
    if read_path == "legacy-worker-pull":
        return {
            "kind": "legacy_worker_pull",
            "local_cache": local_cache,
            "label": "历史运行证据 · Worker 中转取数",
            "path": "Client → 接入 Worker → Meta Owner / Data Worker",
            "batch_get_path": "Worker→Data Worker",
            "urma_path": "Data Worker→请求 Worker",
            "urma_target": None,
        }
    if local_cache is False:
        return {
            "kind": "client_direct",
            "local_cache": False,
            "label": "local_cache=false · Client 直连数据面",
            "path": "Client → Meta Owner → Data Worker",
            "batch_get_path": "Client→Data Worker",
            "urma_path": "Data Worker→Client",
            "urma_target": "Client",
        }
    if local_cache is True:
        return {
            "kind": "bound_worker",
            "local_cache": True,
            "label": "local_cache=true · 绑定 Worker 模式",
            "path": "Client → 绑定 Worker；远端取数仅按明确证据判定",
            "batch_get_path": "绑定 Worker 侧",
            "urma_path": "Data Worker→绑定 Worker（接收端需证据确认）",
            "urma_target": None,
        }
    return {
        "kind": "unknown",
        "local_cache": None,
        "label": "local cache 模式未知",
        "path": "调用拓扑未确认",
        "batch_get_path": "调用方未确认的",
        "urma_path": "URMA接收端未确认",
        "urma_target": "未确认",
    }


def _access_location(transport: str) -> tuple[str, str]:
    """Classify one Client GET from its recorded actual Client-to-Worker transport."""

    normalized = str(transport or "").upper()
    if normalized == "SHM":
        return "本节点SHM", "DS_KV_CLIENT_GET transportType:SHM"
    if normalized == "UB":
        return "远端Data Worker", "DS_KV_CLIENT_GET transportType:UB"
    if normalized == "TCP":
        return "位置不确定（TCP）", "transportType:TCP 不能区分远端访问与同节点SHM失败回退"
    return "未确认", "DS_KV_CLIENT_GET transportType 未观测"


def _canonical_text(text: str) -> str:
    return text.split(" | ", 1)[1] if " | " in text else text


def _share_safe_input(value: object) -> str:
    """Keep report provenance useful without embedding a local directory."""

    normalized = str(value).replace("\\", "/").rstrip("/")
    return normalized.rsplit("/", 1)[-1] or "未命名输入"


def _metric_max(value: object) -> float:
    """Read the current scalar summary shape and tolerate legacy percentiles."""

    if isinstance(value, dict):
        return float(value.get("max", 0) or 0)
    return float(value or 0)


def _percentile(values: list[float], quantile: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    index = (len(ordered) - 1) * quantile
    lower = math.floor(index)
    upper = math.ceil(index)
    if lower == upper:
        return ordered[lower]
    return ordered[lower] + (ordered[upper] - ordered[lower]) * (index - lower)


def _latency_summary(text: str) -> dict[str, int]:
    result: dict[str, int] = {}
    for body in re.findall(r"latencySummary:\{([^}]*)\}", text):
        for key, value in re.findall(r"([A-Za-z0-9_.]+):(-?\d+)", body):
            result[key] = int(value)
    return result


def _rpc_fields(text: str) -> tuple[str | None, dict[str, int]]:
    method_match = re.search(r"method=(\S+)", text)
    if not method_match or "e2e_us=" not in text:
        return None, {}
    fields = {key: int(value) for key, value in re.findall(r"([a-z0-9_]+)_us=(-?\d+)", text)}
    for key, value in re.findall(r"(cntl_error_code|cntl_failed)=(-?\d+)", text):
        fields[key] = int(value)
    return method_match.group(1), fields


def _transport_phase_maps(evidence: list[str]) -> list[dict[str, int]]:
    result = []
    for text in evidence:
        for body in re.findall(r"phasesUs=\{([^}]*)\}", text):
            result.append(
                {name: int(value) for name, value in re.findall(r"([A-Za-z0-9_]+):(-?\d+)", body)}
            )
    return result


def _rpc_framework_ms(fields: dict[str, int]) -> float | None:
    required = {"e2e", "server_req_queue", "server_exec", "network_residual"}
    if not required.issubset(fields):
        return None
    if fields.get("cntl_failed") or fields.get("cntl_error_code"):
        if not any(fields.get(name, 0) for name in ("server_req_queue", "server_exec", "network_residual")):
            return None
    explained_us = sum(fields.get(name, 0) for name in ("server_req_queue", "server_exec", "network_residual"))
    return max(0.0, (fields["e2e"] - explained_us) / 1000.0)


def _take_focus_budget(focus: dict[str, float], donors: tuple[str, ...], amount_ms: float) -> float:
    remaining = max(0.0, amount_ms)
    moved = 0.0
    for donor in donors:
        take = min(focus[donor], remaining)
        focus[donor] -= take
        remaining -= take
        moved += take
        if remaining <= 0:
            break
    return moved


def _urma_scheduling_detail(
    requests: list[dict], slowest_request_id: str | None = None
) -> dict[str, float | None]:
    selected = requests
    if slowest_request_id:
        matched = [
            request for request in requests if request.get("request_id") == slowest_request_id
        ]
        if matched:
            selected = matched
    fields = {
        "wake_sched_latency": "wake_sched_latency_ms",
        "thread_sched": "thread_sched_ms",
        "notify_to_awake": "notify_to_awake_ms",
        "poll_jfc": "poll_jfc_ms",
        "notify": "notify_ms",
    }
    detail = {}
    for label, field in fields.items():
        observed = [float(request[field]) for request in selected if request.get(field) is not None]
        detail[label] = max(observed) if observed else None
    return detail


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
    if slowest_urma_ms is not None and focus["URMA通信"] > slowest_urma_ms:
        focus["未解释残差"] += focus["URMA通信"] - slowest_urma_ms
        focus["URMA通信"] = slowest_urma_ms
    evidence = row.get("evidence", [])
    phase_maps = _transport_phase_maps(evidence)
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
    rpc_entries = []
    for text in evidence:
        method, fields = _rpc_fields(text)
        if method:
            rpc_entries.append((method, fields))
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

    query_framework_ms = max(
        (
            value
            for method, fields in rpc_entries
            if _is_query_and_get_method(method)
            for value in [_rpc_framework_ms(fields)]
            if value is not None
        ),
        default=0.0,
    )
    outer_get_entries = [
        fields
        for method, fields in rpc_entries
        if method.endswith("WorkerOCService.Get") and "GetObjectRemote" not in method
    ]
    data_entries = [
        fields
        for method, fields in rpc_entries
        if not _is_query_and_get_method(method)
        and "ExchangeUrmaConnectInfo" not in method
        and not (method.endswith("WorkerOCService.Get") and "GetObjectRemote" not in method)
    ]
    outer_framework_ms = max(
        (
            value
            for fields in outer_get_entries
            for value in [_rpc_framework_ms(fields)]
            if value is not None
        ),
        default=0.0,
    )
    data_framework_ms = max(
        (
            value
            for fields in data_entries
            for value in [_rpc_framework_ms(fields)]
            if value is not None
        ),
        default=0.0,
    )
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


def _is_write_flow(trace: dict) -> bool:
    return bool(WRITE_CLIENT_FLOWS.intersection(trace.get("flows", {})))


def _write_rpc_group(evidence: list[str], operation: str) -> list[dict[str, int]]:
    result = []
    for text in evidence:
        method, fields = _rpc_fields(text)
        if not method:
            continue
        leaf = method.rsplit(".", 1)[-1].lower()
        if operation == "create" and "create" in leaf and "meta" not in leaf:
            result.append(fields)
        if operation == "publish" and "publish" in leaf and "meta" not in leaf:
            result.append(fields)
    return result


def _write_rpc_split(parent_ms: float, entries: list[dict[str, int]]) -> dict[str, float]:
    split = {"other": parent_ms, "queue": 0.0, "network": 0.0, "framework": 0.0}
    complete = [
        fields
        for fields in entries
        if not fields.get("cntl_failed")
        and not fields.get("cntl_error_code")
        and _rpc_framework_ms(fields) is not None
    ]
    if not complete or parent_ms <= 0:
        return split
    fields = max(complete, key=lambda item: item.get("e2e", 0))
    remaining = parent_ms
    network = min(remaining, fields.get("network_residual", 0) / 1000.0)
    remaining -= network
    queue = min(remaining, fields.get("server_req_queue", 0) / 1000.0)
    remaining -= queue
    framework = min(remaining, _rpc_framework_ms(fields) or 0.0)
    remaining -= framework
    return {"other": remaining, "queue": queue, "network": network, "framework": framework}


def _scaled_write_rpc_split(parent_ms: float, budget_ms: float, entries: list[dict[str, int]]) -> dict[str, float]:
    split = _write_rpc_split(parent_ms, entries)
    scale = budget_ms / parent_ms if parent_ms > 0 else 0.0
    return {name: value * scale for name, value in split.items()}


def _build_write_row(base: dict, trace: dict) -> dict:
    summary = {name: int(value or 0) for name, value in trace.get("latency_summary_us", {}).items()}
    evidence = base.get("evidence", [])
    client_ms = float(base.get("client_ms", 0) or 0)
    status = int(base.get("status", 0) or 0)
    size_bytes = int(base.get("size_bytes", 0) or 0)
    for text in evidence:
        match = re.search(
            rf"\| (-?\d+) \| DS_KV_CLIENT_(?:{WRITE_CLIENT_OPERATION_RE}) \| (\d+) \| (\d+) \|",
            text,
        )
        if match:
            status = int(match.group(1))
            client_ms = int(match.group(2)) / 1000.0
            size_bytes = int(match.group(3))
            break

    create_us = summary.get("client.rpc.create_total") or summary.get("client.rpc.create", 0)
    publish_us = summary.get("client.rpc.publish_total") or summary.get("client.rpc.publish", 0)
    memory_ms = summary.get("client.process.memory_copy", 0) / 1000.0
    summary_urma_ms = summary.get("client.urma.ub_transfer", 0) / 1000.0
    observed_urma_ms = float(base.get("urma_critical_path_ms") or 0.0)
    urma_ms = summary_urma_ms or observed_urma_ms
    data_parent_ms = max(memory_ms, urma_ms)
    create_ms = create_us / 1000.0
    publish_ms = publish_us / 1000.0

    remaining = client_ms
    create_budget = min(remaining, create_ms)
    remaining -= create_budget
    data_budget = min(remaining, data_parent_ms)
    remaining -= data_budget
    publish_budget = min(remaining, publish_ms)
    remaining -= publish_budget
    create_split = _scaled_write_rpc_split(
        create_ms, create_budget, _write_rpc_group(evidence, "create")
    )
    publish_split = _scaled_write_rpc_split(
        publish_ms, publish_budget, _write_rpc_group(evidence, "publish")
    )
    data_scale = data_budget / data_parent_ms if data_parent_ms > 0 else 0.0
    urma_budget = min(data_parent_ms, urma_ms) * data_scale
    memory_budget = max(0.0, data_budget - urma_budget)
    meta_ms = max(
        summary.get("worker.rpc.create_meta", 0),
        summary.get("worker.rpc.update_meta", 0),
    ) / 1000.0
    worker_publish_ms = summary.get("worker.process.publish", 0) / 1000.0
    worker_nested_ms = min(publish_split["other"], worker_publish_ms + meta_ms)
    publish_split["other"] -= worker_nested_ms

    slowest_request_id = (base.get("urma_trace") or {}).get("slowest_request_id")
    urma_sched_detail = _urma_scheduling_detail(
        base.get("urma_requests", []), slowest_request_id
    )
    urma_sched_ms = max(
        (value for value in urma_sched_detail.values() if value is not None), default=0.0
    )
    moved_urma_sched_ms = min(urma_budget, urma_sched_ms)
    urma_budget -= moved_urma_sched_ms
    other_scheduling_ms = create_split["queue"] + publish_split["queue"]
    breakdown = {
        "Create RPC其他": create_split["other"],
        "写入MemoryCopy": memory_budget,
        "写入URMA通信": urma_budget,
        "写入URMA调度/线程开销": moved_urma_sched_ms,
        "Publish RPC其他": publish_split["other"],
        "Worker Publish/元数据": worker_nested_ms,
        "其他调度/线程开销": other_scheduling_ms,
        "RPC网络相关": create_split["network"] + publish_split["network"],
        "RPC框架": create_split["framework"] + publish_split["framework"],
        "未解释残差": remaining,
    }
    rounded = {name: round(max(0.0, breakdown[name]), 6) for name in WRITE_STAGE_NAMES}
    delta = round(client_ms - sum(rounded.values()), 6)
    rounded["未解释残差"] = round(max(0.0, rounded["未解释残差"] + delta), 6)
    primary = max(WRITE_STAGE_NAMES, key=lambda name: rounded[name])
    return {
        "trace_id": base["trace_id"],
        "timestamp": base.get("timestamp", ""),
        "last_ts": base.get("last_ts", ""),
        "client_ms": round(client_ms, 6),
        "failed": status != 0 or bool(trace.get("errors")),
        "status": status,
        "size_bytes": size_bytes,
        "create_rpc_ms": round(create_ms, 6),
        "publish_rpc_ms": round(publish_ms, 6),
        "write_data_ms": round(data_parent_ms, 6),
        "memory_copy_ms": round(memory_ms, 6),
        "write_urma_ms": round(urma_ms, 6) if urma_ms else None,
        "write_data_basis": (
            "client.urma.ub_transfer"
            if summary_urma_ms
            else "URMA logical Write"
            if observed_urma_ms
            else "client.process.memory_copy"
            if memory_ms
            else "未观测"
        ),
        "urma_scheduling_detail_ms": {
            name: round(value, 6) if value is not None else None
            for name, value in urma_sched_detail.items()
        },
        "urma_scheduling_request_id": slowest_request_id,
        "worker_publish_ms": round(worker_publish_ms, 6),
        "metadata_rpc_ms": round(meta_ms, 6),
        "write_breakdown_ms": rounded,
        "write_primary_stage": primary,
        "evidence": evidence,
        "dropped_evidence": int(trace.get("dropped_evidence", 0) or 0),
    }


def _aggregate_write(rows: list[dict]) -> dict:
    latencies = [row["client_ms"] for row in rows]
    return {
        "trace_count": len(rows),
        "failed_count": sum(row["failed"] for row in rows),
        "latency": {
            "p50": round(_percentile(latencies, 0.50), 3),
            "p90": round(_percentile(latencies, 0.90), 3),
            "p99": round(_percentile(latencies, 0.99), 3),
            "max": round(max(latencies, default=0.0), 3),
        },
        "stage_totals": {
            name: round(sum(row["write_breakdown_ms"][name] for row in rows), 3)
            for name in WRITE_STAGE_NAMES
        },
        "problem_counts": dict(collections.Counter(row["write_primary_stage"] for row in rows)),
    }


def _is_query_and_get_method(method: str) -> bool:
    return bool(QUERY_AND_GET_METHOD_RE.search(method))


def _max_rpc(rpcs: dict[str, list[dict[str, int]]], method_part: str, field: str) -> int:
    values = []
    for method, entries in rpcs.items():
        if method_part not in method:
            continue
        for entry in entries:
            values.append(entry.get(field, 0))
    return max(values, default=0)


def _max_single_data_rpc(rpcs: dict[str, list[dict[str, int]]], field: str) -> int | None:
    values = []
    for method, entries in rpcs.items():
        if not method.endswith("GetObjectRemote") or "BatchGetObjectRemote" in method:
            continue
        for entry in entries:
            if field in entry:
                values.append(entry[field])
    return max(values, default=None)


def _raw_float(text: str, pattern: str) -> float | None:
    match = re.search(pattern, text, re.I)
    return float(match.group(1)) if match else None


def _urma_timeout_evidence(trace: dict, evidence: list[str]) -> tuple[bool, float | None]:
    """Return timeout presence and max elapsedMs without inventing a completed WR duration."""

    error_observed = any(
        count and URMA_WAIT_TIMEOUT_RE.search(str(name))
        for name, count in (trace.get("errors") or {}).items()
    )
    matched = [text for text in evidence if URMA_WAIT_TIMEOUT_RE.search(text)]
    elapsed = []
    for text in matched:
        match = re.search(r"\belapsedMs\s*[=:]\s*([\d.]+)", text, re.I)
        if match:
            elapsed.append(float(match.group(1)))
    return error_observed or bool(matched), (max(elapsed) if elapsed else None)


def _classify_urma_timeout_detail(status: int, evidence: list[str]) -> dict[str, str | int | None]:
    """Describe the observed failure point and upward chain without guessing a hardware root cause."""

    joined = "\n".join(evidence)
    pending = []
    for value in re.findall(
        r"URMA_SEND_LANE_(?:TIMEOUT_OBSERVED|FORCE_RELEASE)[^\n]*\bpendingWrs=(\d+)",
        joined,
        re.I,
    ):
        pending.append(int(value))
    pending_wrs = max(pending) if pending else None
    if pending_wrs is None:
        subcategory = "URMA completion超时·pending未观测"
    elif pending_wrs > 1:
        subcategory = "URMA completion超时·多pending WR"
    else:
        subcategory = "URMA completion超时·单pending WR"

    unexpected_payload = bool(re.search(r"Unexpected TCP payload|fallback payload", joined, re.I))
    rpc_deadline = bool(
        re.search(r"RPC timed out|RPC deadline exceeded|cntl_error_code\s*[=:]\s*1008", joined, re.I)
    )
    if status == 1004 and unexpected_payload:
        chain = "URMA超时→UB异常响应→1004"
    elif status == 1001 and rpc_deadline:
        chain = "URMA超时→外层RPC deadline→1001"
    elif status:
        chain = f"URMA超时→状态{status}（上浮细节未闭合）"
    else:
        chain = "URMA超时（上浮状态未观测）"

    recovery = []
    if "URMA_SEND_LANE_TIMEOUT_OBSERVED" in joined:
        recovery.append("send lane已封存")
    if "URMA_SEND_LANE_FORCE_RELEASE" in joined:
        recovery.append("send lane已强制回收")
    return {
        "error_subcategory": subcategory,
        "error_chain_category": chain,
        "error_failure_point": "URMA WRITE completion在等待窗口内未返回",
        "error_root_cause_boundary": (
            "当前证据不能继续区分接收端未完成、链路/设备丢失、CQ/JFC轮询或线程唤醒异常"
        ),
        "error_recovery_action": "、".join(recovery) if recovery else "恢复动作未观测",
        "error_pending_wrs": pending_wrs,
    }


def _classify_rpc_deadline_detail(status: int, evidence: list[str]) -> dict[str, str | int | None]:
    """Classify an observed RPC deadline while keeping its unobserved interval explicit."""

    joined = "\n".join(evidence)
    receive_buffer_failure = re.search(r"Receive buffer preparation failed", joined, re.I)
    arena_oom = re.search(r"Out of memory|no space in arena|fresh_extent_unavailable", joined, re.I)
    if status == 1004 and receive_buffer_failure and arena_oom:
        return {
            "error_family": "Client UB接收缓冲分配失败",
            "error_subcategory": "Client arena fresh extent不足",
            "error_chain_category": "Client接收缓冲分配失败→1004",
            "error_failure_point": "Client为 UB 接收准备内存时 arena 分配失败",
            "error_root_cause_boundary": (
                "日志已闭合到 Client arena fresh extent 不足；"
                "这不是 URMA completion 超时，也不能由此推断已完成 WR 变慢"
            ),
            "error_recovery_action": "TransportGet终止数据读取并上浮1004",
            "error_pending_wrs": None,
        }
    if not status or not re.search(
        r"RPC timed out|RPC deadline exceeded|cntl_error_code\s*[=:]\s*1008", joined, re.I
    ):
        return {}
    failed_methods = []
    for text in evidence:
        method, fields = _rpc_fields(text)
        if method and (fields.get("cntl_failed") or fields.get("cntl_error_code")):
            failed_methods.append(method)
    data_timeout = any(
        method.endswith("GetObjectRemote") and "BatchGetObjectRemote" not in method
        for method in failed_methods
    ) or bool(re.search(r"GetObjectRemote->[^\n]*RPC deadline exceeded", joined, re.I))
    urma_connect_timeout = bool(
        re.search(
            r"(?:WorkerWorkerExchangeUrmaConnectInfo->|UB establish failed:)[^\n]*RPC deadline exceeded",
            joined,
            re.I,
        )
    )
    query_timeout = any(_is_query_and_get_method(method) for method in failed_methods) or bool(
        re.search(r"(?:Master|Worker)OCService\.QueryAndGet[^\n]*RPC deadline exceeded", joined, re.I)
    )
    # Method-specific failure evidence wins over the mere presence of another
    # successful RPC in the same Trace. QueryAndGet may succeed before a later
    # GetObjectRemote consumes the remaining API deadline.
    if urma_connect_timeout:
        subcategory = "Data URMA建链截止超时"
        chain = f"URMA建链超时→数据访问失败→{status}"
        failure_point = "WorkerWorkerExchangeUrmaConnectInfo未在剩余deadline内完成"
    elif data_timeout:
        subcategory = "Data RPC deadline"
        chain = f"Data RPC超时→TransportGet失败→{status}"
        failure_point = "GetObjectRemote未在deadline内返回"
    elif query_timeout:
        subcategory = "QueryMeta RPC deadline"
        chain = f"QueryMeta RPC超时→TransportGet失败→{status}"
        failure_point = "WorkerOCService.QueryAndGet未在deadline内返回"
    elif re.search(r"(?:WorkerWorkerOCService\.)?GetObjectRemote", joined, re.I) and not re.search(
        r"(?:Master|Worker)OCService\.QueryAndGet", joined, re.I
    ):
        subcategory = "Data RPC deadline"
        chain = f"Data RPC超时→TransportGet失败→{status}"
        failure_point = "GetObjectRemote未在deadline内返回"
    elif QUERY_AND_GET_METHOD_RE.search(joined):
        subcategory = "QueryMeta RPC deadline"
        chain = f"QueryMeta RPC超时→TransportGet失败→{status}"
        failure_point = "WorkerOCService.QueryAndGet未在deadline内返回"
    else:
        subcategory = "RPC deadline·方法未细分"
        chain = f"RPC超时→TransportGet失败→{status}"
        failure_point = "RPC未在deadline内返回"
    return {
        "error_family": "RPC截止超时",
        "error_subcategory": subcategory,
        "error_chain_category": chain,
        "error_failure_point": failure_point,
        "error_root_cause_boundary": (
            "失败RPC缺少完整闭环时，服务端执行、响应发送、网络交付和客户端截止观察之间仍不可区分"
        ),
        "error_recovery_action": "TransportGet终止/上浮失败",
        "error_pending_wrs": None,
    }


def _trace_us(text: str) -> dict[str, int]:
    # Older runtime lines end after the trace_us payload without a closing
    # brace.  Consume until the brace when present, otherwise to end-of-line.
    match = re.search(r"trace_us:\{([^}]*)", text)
    if not match:
        return {}
    return {key: int(value) for key, value in re.findall(r"([a-z_]+):(-?\d+)", match.group(1))}


def _delta_ms(trace_us: dict[str, int], start: str, end: str) -> float | None:
    if start not in trace_us or end not in trace_us:
        return None
    delta = trace_us[end] - trace_us[start]
    return round(delta / 1000.0, 6) if delta >= 0 else None


def _request_from_event(
    event: dict,
    remote_get_wr_count: int,
    ip_to_worker: dict[str, str],
    local_cache: bool | None,
    read_path: str | None = None,
) -> dict:
    raw = event.get("raw", "")
    request_match = re.search(r"(?:urma_request_id|request id)[:=]\s*(\d+)", raw, re.I)
    trace_us = _trace_us(raw)
    src_addr = event.get("src_addr") or ""
    target_addr = event.get("target_addr") or ""
    wake_us = event.get("wake_sched_latency_us")
    total_ms = event.get("cost_ms")
    return {
        "request_id": request_match.group(1) if request_match else "",
        "timestamp": event.get("timestamp") or "",
        "source_worker": event.get("worker") or "未明确",
        "target_worker": (
            "Client"
            if local_cache is False and read_path != "legacy-worker-pull"
            else (
                ip_to_worker.get(target_addr.split(":", 1)[0], "未映射")
                if local_cache is True
                else "未确认"
            )
        ),
        "src_addr": src_addr,
        "target_addr": target_addr,
        "data_size": event.get("data_size"),
        "cpuid": event.get("cpuid"),
        "status": event.get("status") or "未记录",
        "src_chip_inflight": event.get("src_chip_inflight") or "未记录",
        "urma_inflight_wr_count": event.get("urma_inflight_wr_count"),
        "remote_get_wr_count": remote_get_wr_count,
        "total_ms": total_ms,
        "is_slow": _is_slow_wr(total_ms),
        "wait_completion_ms": _raw_float(
            raw, r"wait bthread completion time\([^)]*\):\s*([\d.]+)ms"
        ),
        "wake_sched_latency_ms": round(float(wake_us) / 1000.0, 6) if wake_us is not None else None,
        "poll_jfc_ms": event.get("poll_jfc_ms"),
        "notify_ms": event.get("notify_ms"),
        "thread_sched_ms": event.get("thread_sched_ms"),
        "trace_us": trace_us,
        "write_chunk_index": int(
            event.get("write_chunk_index")
            or _raw_float(raw, r"writeChunkIndex\s*:\s*(\d+)")
            or 0
        ),
        "write_chunk_count": int(
            event.get("write_chunk_count")
            or _raw_float(raw, r"writeChunkCount\s*:\s*(\d+)")
            or 0
        ),
        "post_to_wait_ms": _delta_ms(trace_us, "post", "wait"),
        "wait_to_poll_ms": _delta_ms(trace_us, "wait", "poll_begin"),
        "poll_call_ms": _delta_ms(trace_us, "poll_begin", "poll_end"),
        "notify_to_awake_ms": _delta_ms(trace_us, "notify", "awake"),
        "awake_to_observed_ms": _delta_ms(trace_us, "awake", "observed"),
    }


def _is_slow_wr(total_ms: float | None) -> bool:
    return total_ms is not None and total_ms > SLOW_WR_THRESHOLD_MS


def _pearson(pairs: list[tuple[float, float]]) -> float | None:
    if len(pairs) < 2:
        return None
    xs = [item[0] for item in pairs]
    ys = [item[1] for item in pairs]
    mean_x = sum(xs) / len(xs)
    mean_y = sum(ys) / len(ys)
    numerator = sum((x - mean_x) * (y - mean_y) for x, y in pairs)
    denominator = math.sqrt(
        sum((x - mean_x) ** 2 for x in xs) * sum((y - mean_y) ** 2 for y in ys)
    )
    return numerator / denominator if denominator else None


def _metric_summary(values: list[float]) -> dict[str, float | int]:
    return {
        "count": len(values),
        "p50": round(_percentile(values, 0.50), 3),
        "p90": round(_percentile(values, 0.90), 3),
        "p99": round(_percentile(values, 0.99), 3),
        "max": round(max(values, default=0), 3),
    }


def _evidence_timestamp(text: str) -> str:
    match = re.search(r"\b(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?)\b", text)
    return match.group(1) if match else ""


def _timestamp_value(timestamp: str) -> dt.datetime | None:
    if not timestamp:
        return None
    try:
        return dt.datetime.fromisoformat(timestamp)
    except ValueError:
        return None


def _worker_roles(row: dict, worker: str, dimension: str, kind: str) -> list[str]:
    roles: set[str] = set()
    if worker == row.get("direct_data_worker"):
        roles.add("get_worker")
    if worker in row.get("urma_source_workers", []):
        roles.add("urma_source_worker")
    if dimension == "rpc":
        roles.add("rpc_emitter")
    if dimension == "metadata":
        roles.add("metadata_rpc_emitter")
    if dimension == "data" and kind == "remote_get":
        roles.add("data_access_emitter")
    if dimension == "data" and kind == "local_processing":
        roles.add("local_processing_worker")
    return sorted(roles) or ["evidence_worker"]


def _worker_event_views(trace_id: str, index: int, record: dict) -> list[dict]:
    text = str(record.get("text") or "")
    worker = str(record.get("worker") or "未明确")
    timestamp = _evidence_timestamp(text)
    method, fields = _rpc_fields(text)
    views: list[dict] = []

    if method:
        source_event_id = f"{trace_id}:{index}"

        def optional_ms(field: str) -> float | None:
            return round(fields[field] / 1000.0, 6) if field in fields else None

        base = {
            "event_id": source_event_id,
            "source_event_id": source_event_id,
            "trace_id": trace_id,
            "timestamp": timestamp,
            "worker": worker,
            "method": method,
            "failed": bool(fields.get("cntl_error_code") or fields.get("cntl_failed")),
            "is_slow": bool(
                re.search(r"\[(?:(?:ZMQ|BRPC)_)?RPC_FRAMEWORK_SLOW\]", text)
            ),
            "latency_ms": optional_ms("e2e"),
            "network_ms": optional_ms("network_residual"),
            "server_ms": optional_ms("server_exec"),
            "queue_ms": optional_ms("server_req_queue"),
            "retry": "retry" in text.lower(),
        }
        views.append(base | {"event_id": f"{base['event_id']}:rpc", "dimension": "rpc", "kind": "rpc"})
        if "QueryMeta" in method or "QueryAndGet" in method:
            views.append(
                base
                | {
                    "event_id": f"{base['event_id']}:metadata",
                    "dimension": "metadata",
                    "kind": "query_meta",
                    "component_scope": (
                        "Client发起QueryMeta；Meta Owner目标未观测"
                        if "QueryAndGet" in method
                        else "Worker发起QueryMeta；Meta Owner目标未观测"
                    ),
                }
            )
        if "GetObjectRemote" in method:
            views.append(
                base | {"event_id": f"{base['event_id']}:data", "dimension": "data", "kind": "remote_get"}
            )

    local_match = re.search(
        r"Local processing done.*?remoteObjects:\s*(\d+).*?costUs:\s*(\d+)", text, re.I
    )
    if local_match:
        views.append(
            {
                "event_id": f"{trace_id}:{index}:data-local",
                "source_event_id": f"{trace_id}:{index}",
                "trace_id": trace_id,
                "timestamp": timestamp,
                "worker": worker,
                "method": "Local processing",
                "dimension": "data",
                "kind": "local_processing",
                "failed": "rc: code: [OK]" not in text,
                "latency_ms": round(int(local_match.group(2)) / 1000.0, 6),
                "network_ms": None,
                "server_ms": None,
                "queue_ms": None,
                "retry": False,
                "remote_objects": int(local_match.group(1)),
            }
        )
    elif "[Get] Remote done" in text or "[Get/RemotePull]" in text:
        cost = _raw_float(text, r"\bcost:\s*([\d.]+)ms")
        views.append(
            {
                "event_id": f"{trace_id}:{index}:data-remote",
                "source_event_id": f"{trace_id}:{index}",
                "trace_id": trace_id,
                "timestamp": timestamp,
                "worker": worker,
                "method": "RemoteGet",
                "dimension": "data",
                "kind": "remote_get",
                "failed": bool(re.search(r"failed|timed out|deadline exceeded", text, re.I)),
                "latency_ms": cost,
                "network_ms": None,
                "server_ms": None,
                "queue_ms": None,
                "retry": "retry" in text.lower(),
            }
        )
    return views


def _group_metric(values: list[float]) -> dict[str, float | int]:
    return {
        "count": len(values),
        "p90": round(_percentile(values, 0.90), 3) if values else 0.0,
        "max": round(max(values), 3) if values else 0.0,
    }


def _build_worker_correlation(rows: list[dict]) -> dict:
    events: list[dict] = []
    unassigned = 0
    untimed = 0
    for row in rows:
        for index, record in enumerate(row.get("evidence_records", [])):
            views = _worker_event_views(row["trace_id"], index, record)
            if not views:
                continue
            if views[0]["worker"] in {"", "未明确"}:
                unassigned += 1
            if _timestamp_value(views[0]["timestamp"]) is None:
                untimed += 1
            for event in views:
                event["worker_roles"] = _worker_roles(
                    row, event["worker"], event["dimension"], event["kind"]
                )
                event["client_ms"] = row["client_ms"]
                event["failure_reason"] = row.get("failure_reason")
                event.setdefault("is_slow", False)
                event.setdefault("component_scope", "日志所在组件；对端目标未观测")
                event["companions"] = None
                events.append(event)
        for index, request in enumerate(row.get("urma_requests", [])):
            worker = str(request.get("source_worker") or "未明确")
            timestamp = str(request.get("timestamp") or "")
            status = str(request.get("status") or "")
            status_observed = status not in {"", "未记录"}
            if worker in {"", "未明确"}:
                unassigned += 1
            if _timestamp_value(timestamp) is None:
                untimed += 1
            events.append(
                {
                    "event_id": f"{row['trace_id']}:urma:{index}",
                    "source_event_id": f"{row['trace_id']}:urma:{index}",
                    "trace_id": row["trace_id"],
                    "timestamp": timestamp,
                    "worker": worker,
                    "worker_roles": _worker_roles(row, worker, "ub", "urma_wr"),
                    "method": "URMA_ELAPSED_TOTAL",
                    "dimension": "ub",
                    "kind": "urma_wr",
                    "failed": status_observed and "[ok]" not in status.lower() and status.lower() != "ok",
                    "latency_ms": request.get("total_ms"),
                    "network_ms": None,
                    "server_ms": None,
                    "queue_ms": None,
                    "retry": False,
                    "is_slow": bool(request.get("is_slow")),
                    "wait_completion_ms": request.get("wait_completion_ms"),
                    "inflight_wr": request.get("urma_inflight_wr_count"),
                    "companions": None,
                    "component_scope": "Data Worker发起URMA WR；目标按日志证据展示",
                    "client_ms": row["client_ms"],
                    "failure_reason": row.get("failure_reason"),
                }
            )

    valid_events = [
        event
        for event in events
        if event["worker"] not in {"", "未明确"} and _timestamp_value(event["timestamp"]) is not None
    ]
    for event in valid_events:
        if event["kind"] not in {"query_meta", "remote_get"} or not event["failed"]:
            continue
        event_time = _timestamp_value(event["timestamp"])
        nearby = []
        for candidate in valid_events:
            candidate_time = _timestamp_value(candidate["timestamp"])
            same_worker = candidate["worker"] == event["worker"]
            within_window = abs((candidate_time - event_time).total_seconds()) <= 1
            different_source = candidate["source_event_id"] != event["source_event_id"]
            if same_worker and within_window and different_source:
                nearby.append(candidate)
        problem_companions = [candidate for candidate in nearby if candidate["failed"] or candidate["is_slow"]]
        slow_wrs = [candidate for candidate in nearby if candidate["kind"] == "urma_wr" and candidate["is_slow"]]
        same_trace_problem_sources = {
            candidate["source_event_id"]
            for candidate in problem_companions
            if candidate["trace_id"] == event["trace_id"]
        }
        event["companions"] = {
            "slow_wr_count": len(slow_wrs),
            "slow_wr_max_ms": round(max((item["latency_ms"] for item in slow_wrs), default=0), 3),
            "rpc_failure_count": sum(item["dimension"] == "rpc" and item["failed"] for item in nearby),
            "query_meta_failure_count": sum(item["kind"] == "query_meta" and item["failed"] for item in nearby),
            "remote_get_failure_count": sum(item["kind"] == "remote_get" and item["failed"] for item in nearby),
            "same_trace_event_count": len(same_trace_problem_sources),
            "other_worker_event_count": 0,
            "relation": (
                "direct_same_trace"
                if same_trace_problem_sources
                else "concurrent_companion" if problem_companions else "no_companion_evidence"
            ),
        }

    bucket_groups: dict[tuple[str, str], list[dict]] = collections.defaultdict(list)
    for event in valid_events:
        bucket_groups[(event["worker"], event["timestamp"][:19])].append(event)
    time_buckets = []
    for (worker, second), selected in sorted(bucket_groups.items()):
        by_dimension = {
            name: [item for item in selected if item["dimension"] == name]
            for name in ("rpc", "ub", "metadata", "data")
        }
        rpc = by_dimension["rpc"]
        ub = by_dimension["ub"]
        metadata = by_dimension["metadata"]
        data = by_dimension["data"]
        time_buckets.append(
            {
                "worker": worker,
                "second": second,
                "trace_count": len({item["trace_id"] for item in selected}),
                "rpc": {
                    "request_count": len(rpc),
                    "failure_count": sum(item["failed"] for item in rpc),
                    "network_ms": _group_metric([item["network_ms"] for item in rpc if item["network_ms"] is not None]),
                    "server_ms": _group_metric([item["server_ms"] for item in rpc if item["server_ms"] is not None]),
                    "queue_ms": _group_metric([item["queue_ms"] for item in rpc if item["queue_ms"] is not None]),
                },
                "ub": {
                    "wr_count": len(ub),
                    "slow_wr_count": sum(item["is_slow"] for item in ub),
                    "total_ms": _group_metric([item["latency_ms"] for item in ub if item["latency_ms"] is not None]),
                    "wait_ms": _group_metric(
                        [
                            item["wait_completion_ms"]
                            for item in ub
                            if item.get("wait_completion_ms") is not None
                        ]
                    ),
                    "inflight": _group_metric(
                        [item["inflight_wr"] for item in ub if item.get("inflight_wr") is not None]
                    ),
                },
                "metadata": {
                    "request_count": len(metadata),
                    "failure_count": sum(item["failed"] for item in metadata),
                    "latency_ms": _group_metric(
                        [
                            item["latency_ms"]
                            for item in metadata
                            if item["latency_ms"] is not None
                        ]
                    ),
                },
                "data": {
                    "local_count": sum(item["kind"] == "local_processing" for item in data),
                    "remote_count": sum(item["kind"] == "remote_get" for item in data),
                    "failure_count": sum(item["failed"] for item in data),
                    "retry_count": sum(item["retry"] for item in data),
                    "latency_ms": _group_metric(
                        [item["latency_ms"] for item in data if item["latency_ms"] is not None]
                    ),
                },
            }
        )

    worker_groups: dict[str, list[dict]] = collections.defaultdict(list)
    for event in valid_events:
        worker_groups[event["worker"]].append(event)
    workers = []
    for worker, selected in worker_groups.items():
        workers.append(
            {
                "worker": worker,
                "roles": sorted({role for event in selected for role in event["worker_roles"]}),
                "event_count": len(selected),
                "trace_count": len({event["trace_id"] for event in selected}),
                "failure_count": sum(event["failed"] for event in selected),
                "slow_wr_count": sum(event["kind"] == "urma_wr" and event["is_slow"] for event in selected),
            }
        )
    workers.sort(key=lambda item: (-item["event_count"], item["worker"]))
    events.sort(key=lambda item: (item["timestamp"], item["worker"], item["event_id"]))
    return {
        "slow_wr_threshold_ms": SLOW_WR_THRESHOLD_MS,
        "bucket_seconds": 1,
        "neighbor_window_seconds": 1,
        "workers": workers,
        "time_buckets": time_buckets,
        "events": events,
        "summaries": {},
        "unassigned_event_count": unassigned,
        "untimed_event_count": untimed,
    }


def _extract_trace(trace_id: str, trace: dict) -> dict:
    texts: list[str] = []
    evidence_items: list[tuple[str, str]] = []
    seen: set[str] = set()
    for evidence in trace.get("evidence", []):
        text = evidence.get("text", "")
        canonical = _canonical_text(text)
        if canonical in seen:
            continue
        seen.add(canonical)
        texts.append(text)
        evidence_items.append((text, evidence.get("worker", "") or "未明确"))

    client_us = 0
    worker_us = 0
    status = 0
    size_bytes = 0
    transport = "未知"
    client_summary: dict[str, int] = {}
    worker_summary: dict[str, int] = {}
    rpcs: dict[str, list[dict[str, int]]] = collections.defaultdict(list)
    urma_values: list[float] = []
    direct_data_worker = "未明确"
    client_observer = "未明确"
    urma_source_costs: dict[str, float] = {}
    explicit_remote = False

    for text, evidence_worker in evidence_items:
        client_match = re.search(r"\| (\d+) \| DS_KV_CLIENT_GET \| (\d+) \| (\d+) \|", text)
        if client_match:
            status, client_us, size_bytes = map(int, client_match.groups())
            client_observer = evidence_worker
            transport_match = re.search(r"transportType:(\w+)", text)
            if transport_match:
                transport = transport_match.group(1)
            client_summary.update(_latency_summary(text))

        worker_match = re.search(r"\| \d+ \| DS_POSIX_GET \| (\d+) \|", text)
        if worker_match:
            worker_us = int(worker_match.group(1))
            worker_summary.update(_latency_summary(text))
            direct_data_worker = evidence_worker

        method, fields = _rpc_fields(text)
        if method:
            rpcs[method].append(fields)
            if "BatchGetObjectRemote" in method:
                explicit_remote = True

        if "[Get] Remote done" in text or "[Get/RemotePull]" in text:
            explicit_remote = True

        urma_match = re.search(r"URMA_ELAPSED_TOTAL.*?total cost ([\d.]+)ms", text)
        if not urma_match:
            urma_match = re.search(r"URMA_ELAPSED_TOTAL.*?cost\s+([\d.]+)ms", text)
        if urma_match:
            urma_cost = float(urma_match.group(1))
            urma_values.append(urma_cost)
            urma_source_costs[evidence_worker] = max(urma_source_costs.get(evidence_worker, 0.0), urma_cost)

    if not client_us:
        client_role = trace.get("access_latency_ms_by_role", {}).get("client", {})
        client_us = round(float(client_role.get("max", 0)) * 1000)
    if not worker_us:
        worker_role = trace.get("access_latency_ms_by_role", {}).get("worker", {})
        worker_us = round(float(worker_role.get("max", 0)) * 1000)

    summary = dict(trace.get("latency_summary_us", {}))
    summary.update(worker_summary)
    summary.update(client_summary)
    direct_read_keys = (
        "client.process.direct_route",
        "client.rpc.direct_query_and_get",
        "client.rpc.direct_get_data",
        "client.process.direct_materialize",
        "client.process.get",
    )
    direct_read_us = sum(int(summary.get(key, 0) or 0) for key in direct_read_keys)
    worker_process_us = summary.get("worker.process.get", 0)
    if not worker_process_us:
        worker_process_us = round(_metric_max(trace.get("breakdown_ms", {}).get("worker.process.get")) * 1000)
    worker_budget_us = max(worker_process_us, worker_us, direct_read_us)
    client_query_and_get_us = summary.get("client.rpc.direct_query_and_get", 0)
    query_meta_us = (
        client_query_and_get_us
        or summary.get("worker.rpc.query_meta", 0)
        or _max_rpc(rpcs, "QueryMeta", "e2e")
    )
    if not query_meta_us:
        query_meta_us = round(
            max(
                (
                    float(item.get("duration_ms", 0))
                    for item in trace.get("stage_breakdown", [])
                    if item.get("stage") == "read.entry_to_meta_worker"
                ),
                default=0,
            )
            * 1000
        )

    client_rpc_e2e_us = _max_rpc(rpcs, "datasystem.WorkerOCService.Get", "e2e")
    client_rpc_network_us = _max_rpc(rpcs, "datasystem.WorkerOCService.Get", "network_residual")
    client_rpc_queue_us = _max_rpc(rpcs, "datasystem.WorkerOCService.Get", "server_req_queue")
    client_rpc_server_us = _max_rpc(rpcs, "datasystem.WorkerOCService.Get", "server_exec")
    batch_e2e_us = _max_rpc(rpcs, "BatchGetObjectRemote", "e2e")
    batch_network_us = _max_rpc(rpcs, "BatchGetObjectRemote", "network_residual")
    batch_server_us = _max_rpc(rpcs, "BatchGetObjectRemote", "server_exec")
    data_rpc_e2e_us = _max_single_data_rpc(rpcs, "e2e")
    data_rpc_network_us = _max_single_data_rpc(rpcs, "network_residual")
    data_rpc_server_us = _max_single_data_rpc(rpcs, "server_exec")
    rpc_slow_methods = trace.get("rpc_slow", {})
    has_outer_get_slow = len(rpc_slow_methods) == 1 and any(
        method.endswith("WorkerOCService.Get") and "BatchGetObjectRemote" not in method
        for method in rpc_slow_methods
    )
    if not client_rpc_network_us and has_outer_get_slow:
        client_rpc_network_us = round(
            float(trace.get("rpc_slow_fields_us", {}).get("network_residual_us", {}).get("max", 0))
        )
    if not urma_values:
        normalized_urma = float(trace.get("urma_elapsed_ms", {}).get("total", {}).get("max", 0) or 0)
        if normalized_urma:
            urma_values.append(normalized_urma)
            normalized_sources = {
                event.get("worker")
                for event in trace.get("ub_events", [])
                if event.get("worker") and event.get("event_type") in {"total", "urma_total"}
            }
            normalized_source = next(iter(normalized_sources)) if len(normalized_sources) == 1 else "未明确"
            urma_source_costs[normalized_source] = normalized_urma
    urma_ms = max(urma_values, default=0.0)
    urma_observed = (
        int(trace.get("urma_elapsed_ms", {}).get("total", {}).get("count", 0)) > 0
        or any(
            event.get("event_type") in {"total", "urma_total"}
            for event in trace.get("ub_events", [])
        )
        or any("URMA_ELAPSED_TOTAL" in text for text in texts)
    )
    outer_rpc_observed = has_outer_get_slow or any(
        method.endswith("WorkerOCService.Get") and "BatchGetObjectRemote" not in method
        for method in rpcs
    )
    data_rpc_observed = any(
        method.endswith("GetObjectRemote") and "BatchGetObjectRemote" not in method
        for method in rpcs
    )
    rpc_observed = outer_rpc_observed or data_rpc_observed

    if status == 0:
        for error_name, count in trace.get("errors", {}).items():
            status_match = re.fullmatch(r"status=(-?\d+)", error_name)
            if status_match and count and int(status_match.group(1)) != 0:
                status = int(status_match.group(1))
                break
    failed = status != 0
    worker_dominant = worker_budget_us >= max(1000, client_us * 0.5) or (failed and worker_us >= 5000)
    if worker_dominant and explicit_remote:
        category = CATEGORY_REMOTE
    elif worker_dominant:
        category = CATEGORY_WORKER
    else:
        category = CATEGORY_CLIENT_RPC

    client_ms = client_us / 1000.0
    remaining = client_ms
    client_network_ms = min(remaining, client_rpc_network_us / 1000.0)
    remaining -= client_network_ms
    batch_network_ms = min(remaining, batch_network_us / 1000.0)
    remaining -= batch_network_ms
    data_rpc_network_ms = min(remaining, (data_rpc_network_us or 0) / 1000.0)
    remaining -= data_rpc_network_ms
    rpc_network_ms = client_network_ms + batch_network_ms + data_rpc_network_ms
    rpc_queue_ms = min(remaining, client_rpc_queue_us / 1000.0)
    remaining -= rpc_queue_ms
    # BatchGet network is nested in the Worker parent window but belongs to the
    # exclusive RPC bucket. Remove it once from the Worker non-network budget.
    worker_non_network_ms = max(
        0.0,
        worker_budget_us / 1000.0 - batch_network_ms - data_rpc_network_ms - rpc_queue_ms,
    )
    worker_cap_ms = min(remaining, worker_non_network_ms)
    query_ms = min(worker_cap_ms, query_meta_us / 1000.0)
    after_query = worker_cap_ms - query_ms
    urma_attribution_ms = min(after_query, urma_ms)
    after_urma_ms = max(0.0, after_query - urma_attribution_ms)
    remote_non_urma_cap_ms = max(
        0.0, max(batch_server_us, data_rpc_server_us or 0) / 1000.0 - urma_attribution_ms
    )
    remote_non_urma_ms = min(after_urma_ms, remote_non_urma_cap_ms)
    direct_worker_other_ms = max(0.0, after_urma_ms - remote_non_urma_ms)
    remaining -= worker_cap_ms
    unexplained_ms = max(0.0, remaining)

    attribution = {
        "RPC网络": round(rpc_network_ms, 6),
        "RPC排队": round(rpc_queue_ms, 6),
        "QueryMeta": round(query_ms, 6),
        "URMA超时等待": 0.0,
        "URMA": round(urma_attribution_ms, 6),
        "远端供数处理": round(remote_non_urma_ms, 6),
        "数据访问父窗口/未细分": round(direct_worker_other_ms, 6),
        "未解释残差": round(unexplained_ms, 6),
    }
    primary_stage = max(STAGE_NAMES, key=lambda stage: attribution[stage])
    urma_timeout_observed, urma_timeout_max_ms = _urma_timeout_evidence(trace, texts)
    if urma_timeout_observed:
        error_family = "URMA超时"
        error_detail = _classify_urma_timeout_detail(status, texts)
    else:
        error_detail = _classify_rpc_deadline_detail(status, texts)
        error_family = error_detail.get("error_family")
    primary_problem = error_family if failed and error_family else primary_stage
    if error_family == "RPC截止超时":
        primary_problem = primary_stage
    if error_family == "Client UB接收缓冲分配失败":
        primary_problem = primary_stage
    access_location, access_location_evidence = _access_location(transport)
    failure_reason = error_detail.get("error_subcategory") or (
        f"状态{status}·原因未细分" if failed else "成功"
    )
    if failure_reason == "QueryMeta RPC deadline":
        data_access_scope = "Client等待Meta Owner QueryAndGet超时"
        data_access_evidence = "Client发起QueryAndGet命中RPC deadline；Meta Owner服务端阶段未闭合"
    elif failure_reason == "Data URMA建链截止超时":
        data_access_scope = "Client→Data Worker URMA建链截止超时"
        data_access_evidence = "QueryAndGet已返回；后续数据访问建立URMA连接时API剩余deadline耗尽"
    elif failure_reason == "Data RPC deadline":
        data_access_scope = "Client→Data Worker RPC截止超时"
        data_access_evidence = "Client发起GetObjectRemote命中RPC deadline；失败RPC缺少完整server trailer"
    elif data_rpc_network_us is not None and data_rpc_network_ms >= max(
        (data_rpc_server_us or 0) / 1000.0, 1.0
    ):
        data_access_scope = "Client→Data Worker RPC网络慢"
        data_access_evidence = (
            f"Client发起GetObjectRemote：network residual {data_rpc_network_ms:.3f}ms，"
            + (
                f"Data Worker server_exec {data_rpc_server_us / 1000.0:.3f}ms"
                if data_rpc_server_us is not None
                else "Data Worker server_exec未观测"
            )
        )
    elif (data_rpc_server_us or 0) > 0:
        data_access_scope = "Data Worker GetObjectRemote服务端处理"
        data_access_evidence = (
            f"GetObjectRemote server_exec {data_rpc_server_us / 1000.0:.3f}ms，"
            + (
                f"network residual {data_rpc_network_ms:.3f}ms"
                if data_rpc_network_us is not None
                else "network residual未观测"
            )
        )
    elif transport == "SHM" and direct_read_us:
        data_access_scope = "Client本节点SHM数据访问窗口"
        data_access_evidence = "Client direct_get_data使用SHM；缺少子阶段时不等价为Data Worker CPU"
    elif direct_read_us:
        data_access_scope = "Client数据获取父窗口未闭合"
        data_access_evidence = "Client direct_get_data有耗时，但RPC/SHM/URMA子阶段未完整覆盖"
    elif worker_process_us or worker_us:
        data_access_scope = "Worker ProcessGet父窗口未细分"
        data_access_evidence = "Worker ProcessGet有父窗口证据，内部锁/查找/等待阶段未完整覆盖"
    else:
        data_access_scope = "Client/Worker观测未闭合"
        data_access_evidence = "只有Client总窗口，未观测到可定位的RPC或Worker子阶段"

    return {
        "trace_id": trace_id,
        "timestamp": trace.get("first_ts") or "",
        "last_ts": trace.get("last_ts") or "",
        "category": category,
        "failed": failed,
        "status": status,
        "client_ms": round(client_ms, 6),
        "worker_ms": round(worker_us / 1000.0, 6),
        "worker_process_ms": round(worker_budget_us / 1000.0, 6),
        "client_rpc_e2e_ms": round(client_rpc_e2e_us / 1000.0, 6),
        "client_rpc_network_ms": round(client_rpc_network_us / 1000.0, 6),
        "client_rpc_queue_ms": round(client_rpc_queue_us / 1000.0, 6),
        "client_rpc_server_ms": round(client_rpc_server_us / 1000.0, 6),
        "batch_e2e_ms": round(batch_e2e_us / 1000.0, 6),
        "batch_network_ms": round(batch_network_us / 1000.0, 6),
        "batch_server_ms": round(batch_server_us / 1000.0, 6),
        "data_rpc_e2e_ms": round(data_rpc_e2e_us / 1000.0, 6) if data_rpc_e2e_us is not None else None,
        "data_rpc_network_ms": (
            round(data_rpc_network_us / 1000.0, 6) if data_rpc_network_us is not None else None
        ),
        "data_rpc_server_ms": (
            round(data_rpc_server_us / 1000.0, 6) if data_rpc_server_us is not None else None
        ),
        "data_rpc_observed": data_rpc_observed,
        "query_meta_ms": round(query_meta_us / 1000.0, 6),
        "client_query_and_get_ms": round(client_query_and_get_us / 1000.0, 6),
        "urma_ms": round(urma_ms, 6),
        "urma_observed": urma_observed,
        "urma_timeout_observed": urma_timeout_observed,
        "urma_timeout_max_ms": round(urma_timeout_max_ms, 6) if urma_timeout_max_ms is not None else None,
        "error_family": error_family,
        "error_subcategory": error_detail.get("error_subcategory"),
        "error_chain_category": error_detail.get("error_chain_category"),
        "error_failure_point": error_detail.get("error_failure_point"),
        "error_root_cause_boundary": error_detail.get("error_root_cause_boundary"),
        "error_recovery_action": error_detail.get("error_recovery_action"),
        "error_pending_wrs": error_detail.get("error_pending_wrs"),
        "failure_reason": failure_reason,
        "rpc_observed": rpc_observed,
        "direct_read_observed": bool(direct_read_us),
        "direct_get_data_ms": round(float(summary.get("client.rpc.direct_get_data", 0) or 0) / 1000.0, 6),
        "size_bytes": size_bytes,
        "transport": transport,
        "delivery_affinity": f"{transport}交付" if transport != "未知" else "交付方式未明确",
        "access_location": access_location,
        "access_location_evidence": access_location_evidence,
        "data_affinity": access_location,
        "direct_data_worker": direct_data_worker,
        "client_observer": client_observer,
        "data_access_scope": data_access_scope,
        "data_access_evidence": data_access_evidence,
        "urma_source_workers": sorted(urma_source_costs),
        "urma_source_costs": {worker: round(cost, 6) for worker, cost in sorted(urma_source_costs.items())},
        "attribution_ms": attribution,
        "primary_problem": primary_problem,
        "primary_stage": primary_stage,
        "client_observed": bool(client_us),
        "get_observed": bool(trace.get("flows", {}).get("DS_KV_CLIENT_GET")),
        "dropped_evidence": int(trace.get("dropped_evidence", 0) or 0),
        "evidence": texts,
        # Correlation consumes every upstream evidence record.  Display text is
        # still canonicalized above, but retries that differ only by timestamp
        # must remain distinct for the worker/time model.
        "evidence_records": [
            {
                "text": evidence.get("text", ""),
                "worker": evidence.get("worker", "") or "未明确",
                "source": evidence.get("source", ""),
                "member": evidence.get("member", ""),
                "line": evidence.get("line"),
            }
            for evidence in trace.get("evidence", [])
        ],
    }


def _non_transport_analysis(row: dict, topology: dict[str, object]) -> dict | None:
    if row["primary_problem"] not in {"远端供数处理", "数据访问父窗口/未细分", "未解释残差"}:
        return None

    joined = "\n".join(row["evidence"])
    batch_attempts = []
    for text in row["evidence"]:
        method, fields = _rpc_fields(text)
        if method and "BatchGetObjectRemote" in method:
            batch_attempts.append(fields)
    batch_timeouts = [item for item in batch_attempts if item.get("cntl_error_code") == 1008]
    batch_successes = [item for item in batch_attempts if item.get("cntl_error_code", 0) == 0]
    local_match = re.search(
        r"Local processing done.*?remoteObjects:\s*(\d+).*?costUs:\s*(\d+)", joined, re.I
    )
    remote_lock_match = re.search(r"RemoteLockEntry:\s*([\d.]+)\s*ms", joined, re.I)

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
        if remote_lock_match:
            lock_ms = float(remote_lock_match.group(1))
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

    if local_match and int(local_match.group(1)) == 0:
        local_ms = int(local_match.group(2)) / 1000.0
        return common | {
            "deep_category": "明确本地ProcessGet耗时",
            "confidence": "高",
            "observed_ms": local_ms,
            "conclusion": (
                f"该 Data Worker 明确记录 Local processing {local_ms:.3f}ms、remoteObjects=0，"
                "该 Trace 没有远端 BatchGet/URMA 子请求证据；主要可观测窗口在本地 ProcessGet。"
            ),
            "evidence_points": [
                f"Local processing costUs={int(local_match.group(2))}",
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


def _group_urma_logical_writes(requests: list[dict]) -> list[dict]:
    """Group explicitly indexed WR chunks and compute non-additive wall-clock spans."""

    groups: list[list[dict]] = []
    current: list[dict] = []
    for request in requests:
        chunk_index = int(request.get("write_chunk_index") or 0)
        chunk_count = int(request.get("write_chunk_count") or 0)
        current_count = int(current[0].get("write_chunk_count") or 0) if current else 0
        current_indexes = {int(item.get("write_chunk_index") or 0) for item in current}
        starts_group = (
            not current
            or not chunk_count
            or current_count != chunk_count
            or chunk_index in current_indexes
            or len(current) >= chunk_count
        )
        if starts_group and current:
            groups.append(current)
            current = []
        current.append(request)
    if current:
        groups.append(current)

    result = []
    for write_index, selected in enumerate(groups, start=1):
        expected = int(selected[0].get("write_chunk_count") or 0)
        indexes = [int(item.get("write_chunk_index") or 0) for item in selected]
        complete_chunks = expected > 0 and sorted(indexes) == list(range(1, expected + 1))
        posts = [item.get("trace_us", {}).get("post") for item in selected]
        observed = [item.get("trace_us", {}).get("observed") for item in selected]
        complete_clock = complete_chunks and all(value is not None for value in posts + observed)
        wall_clock_ms = None
        if complete_clock:
            delta_us = max(observed) - min(posts)
            wall_clock_ms = round(delta_us / 1000.0, 6) if delta_us >= 0 else None
        totals = [float(item["total_ms"]) for item in selected]
        result.append(
            {
                "write_index": write_index,
                "wr_count": len(selected),
                "expected_wr_count": expected or None,
                "complete": bool(complete_clock and wall_clock_ms is not None),
                "wall_clock_ms": wall_clock_ms,
                "slowest_wr_ms": round(max(totals), 6),
                "sum_wr_ms": round(sum(totals), 6),
                "request_ids": [item["request_id"] for item in selected],
                "data_size": sum(int(item.get("data_size") or 0) for item in selected),
                "grouping_basis": (
                    "完整chunkIndex/count+trace_us" if complete_clock else "分片或trace_us未闭合"
                ),
            }
        )
    return result


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
    inline_pattern = re.compile(
        r"QueryAndGet done,.*?inlineHits:\s*(\d+).*?transport:\s*UB\b.*?total:\s*([\d.]+)ms",
        re.I,
    )
    for item in row.get("evidence_records", []):
        match = inline_pattern.search(item.get("text", ""))
        if not match or int(match.group(1)) <= 0:
            continue
        worker = item.get("worker")
        if worker in {None, "", "unknown", "未明确"}:
            return
        end = _timestamp_value(_evidence_timestamp(item.get("text", "")))
        if end is None:
            return
        total_ms = float(match.group(2))
        inline_candidates.append(
            {
                "worker": worker,
                "start": end - dt.timedelta(milliseconds=total_ms),
                "end": end,
                "total_ms": total_ms,
            }
        )
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
    entries = []
    for text in row.get("evidence", []):
        method, fields = _rpc_fields(text)
        if method and _is_query_and_get_method(method):
            entries.append(fields)
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
    query_pattern = re.compile(
        r"QueryAndGet done,.*?localRead:\s*([\d.]+)ms.*?total:\s*([\d.]+)ms",
        re.I,
    )
    timeout_pattern = re.compile(
        r"\[URMA(?:[_ -]WAIT[_ -]TIMEOUT)\].*?elapsedMs\s*[=:]\s*([\d.]+)",
        re.I,
    )
    request_pattern = re.compile(r"urma_request_id[:_]?(\d+)", re.I)
    for item in row.get("evidence_records", []):
        text = item.get("text", "")
        match = query_pattern.search(text)
        end = _timestamp_value(_evidence_timestamp(text))
        worker = item.get("worker")
        if not match or end is None or worker in {None, "", "unknown", "未明确"}:
            continue
        total_ms = float(match.group(2))
        query_attempts.append(
            {
                "worker": worker,
                "start": end - dt.timedelta(milliseconds=total_ms),
                "end": end,
                "total_ms": total_ms,
            }
        )

    timeout_events = {}
    for item in row.get("evidence_records", []):
        text = item.get("text", "")
        match = timeout_pattern.search(text)
        timestamp = _timestamp_value(_evidence_timestamp(text))
        worker = item.get("worker")
        if not match or timestamp is None or worker in {None, "", "unknown", "未明确"}:
            continue
        request_match = request_pattern.search(text)
        identity = (
            worker,
            request_match.group(1) if request_match else timestamp.isoformat(),
        )
        event = {
            "worker": worker,
            "timestamp": timestamp,
            "elapsed_ms": float(match.group(1)),
        }
        current = timeout_events.get(identity)
        if current is None or timestamp < current["timestamp"]:
            timeout_events[identity] = event

    tolerance = dt.timedelta(milliseconds=1)
    nested_groups = []
    for attempt in query_attempts:
        nested = [
            event
            for event in timeout_events.values()
            if event["worker"] == attempt["worker"]
            and attempt["start"] - tolerance <= event["timestamp"] <= attempt["end"] + tolerance
        ]
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


def _max_evidence_ms(evidence: list[str], pattern: str, *, divisor: float = 1.0) -> float | None:
    values = []
    for text in evidence:
        match = re.search(pattern, text, re.I)
        if match:
            values.append(float(match.group(1)) / divisor)
    return max(values, default=None)


def _query_meta_detail(row: dict) -> dict | None:
    """Return one exclusive QueryAndGet diagnosis plus orthogonal TryGet evidence."""

    worker_query_done_observed = any(
        re.search(r"QueryAndGet done,", text, re.I) for text in row.get("evidence", [])
    )
    local_read_ms = _max_evidence_ms(
        row.get("evidence", []), r"QueryAndGet done,.*?\blocalRead:\s*([\d.]+)ms"
    )
    entries = []
    for text in row.get("evidence", []):
        method, fields = _rpc_fields(text)
        if method and _is_query_and_get_method(method):
            entries.append(fields)
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
    legacy_try_get_urma_observed = bool(row.get("urma_requests")) and any(
        re.search(r"Processing pull object.*\bsrc=:-1", text, re.I)
        for text in row.get("evidence", [])
    )
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
    elif (
        worker_query_done_observed
        and not try_get_urma_observed
        and local_read_ms is not None
        and local_read_ms >= max(1.0, query_total_ms * 0.5)
    ):
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

    evidence = row["evidence"]
    client_transfer_ms = _max_evidence_ms(
        evidence, r"\[TransportGet\].*?phasesUs=\{[^}]*\bdata_transfer:(\d+)", divisor=1000.0
    )
    provider_pull_ms = _max_evidence_ms(
        evidence, r"Processing pull object.*?\bcost:\s*([\d.]+)ms"
    )
    provider_finish_ms = _max_evidence_ms(
        evidence, r"\[GetObjectRemote\]\s+finish.*?\bcost:\s*([\d.]+)ms"
    )
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


def build_trace_rows(
    summary: dict, local_cache: bool | None = None, read_path: str | None = None
) -> list[dict]:
    topology = _topology_contract(local_cache, read_path)
    rows = [_extract_trace(trace_id, trace) for trace_id, trace in summary.get("traces", {}).items()]
    by_id = {row["trace_id"]: row for row in rows}
    mapping = summary.get("dimensions", {}).get("worker_ip_mapping", [])
    ip_to_worker = (
        dict(mapping)
        if isinstance(mapping, dict)
        else {
            item.get("pod_ip", ""): item.get("worker_full_name", "未映射")
            for item in mapping
            if isinstance(item, dict)
        }
    )
    all_requests: list[dict] = []
    for trace_id, trace in summary.get("traces", {}).items():
        remote_get_wr_count = max(
            (int(event.get("inflight_remote_get") or 0) for event in trace.get("ub_events", [])),
            default=0,
        )
        requests = [
            _request_from_event(event, remote_get_wr_count, ip_to_worker, local_cache, read_path)
            for event in trace.get("ub_events", [])
            if event.get("event_type") in {"total", "urma_total"} and event.get("cost_ms") is not None
        ]
        requests.sort(key=lambda item: (item["timestamp"], item["request_id"]))
        by_id[trace_id]["urma_requests"] = requests
        by_id[trace_id]["urma_logical_writes"] = _group_urma_logical_writes(requests)
        all_requests.extend(requests)

    inflight_threshold = _percentile(
        [item["urma_inflight_wr_count"] for item in all_requests if item["urma_inflight_wr_count"] is not None],
        0.90,
    )
    for row in rows:
        requests = row["urma_requests"]
        if not requests:
            row["urma_trace"] = None
            row["urma_logical_writes"] = []
            row["urma_critical_path_ms"] = None
            continue
        slowest = max(requests, key=lambda item: item["total_ms"])
        logical_writes = row["urma_logical_writes"]
        complete_writes = [item for item in logical_writes if item["complete"]]
        critical_path_ms, latency_basis = _urma_critical_path(logical_writes)
        row["urma_critical_path_ms"] = round(critical_path_ms, 6)
        old_urma = row["attribution_ms"]["URMA"]
        extra_urma = max(0.0, critical_path_ms - old_urma)
        if extra_urma:
            moved = min(extra_urma, row["attribution_ms"]["数据访问父窗口/未细分"])
            row["attribution_ms"]["URMA"] = round(old_urma + moved, 6)
            row["attribution_ms"]["数据访问父窗口/未细分"] = round(
                row["attribution_ms"]["数据访问父窗口/未细分"] - moved, 6
            )
            row["primary_stage"] = max(STAGE_NAMES, key=lambda stage: row["attribution_ms"][stage])
            if not row.get("error_family") or row.get("error_family") == "RPC截止超时":
                row["primary_problem"] = row["primary_stage"]
        max_inflight = max(
            (item["urma_inflight_wr_count"] for item in requests if item["urma_inflight_wr_count"] is not None),
            default=0,
        )
        max_wake = max(
            (item["wake_sched_latency_ms"] for item in requests if item["wake_sched_latency_ms"] is not None),
            default=None,
        )
        wait_ms = slowest.get("wait_completion_ms")
        wait_ratio = wait_ms / slowest["total_ms"] * 100 if wait_ms is not None and slowest["total_ms"] else None
        urma_ratio = slowest["total_ms"] / row["client_ms"] * 100 if row["client_ms"] else None
        labels = []
        if slowest["is_slow"]:
            labels.append("URMA尾延迟")
        if max_inflight >= inflight_threshold:
            labels.append("高Inflight伴随")
        if wait_ratio is not None and wait_ratio >= 70:
            labels.append("completion等待主导")
        if max_wake is not None and max_wake < 0.1:
            labels.append("wake调度正常")
        if urma_ratio is not None and urma_ratio >= 70:
            labels.append("URMA占比高")
        if row["attribution_ms"]["RPC网络"] >= 1:
            labels.append("RPC残差伴随")
        direction = f"{slowest['source_worker']} → {slowest['target_worker']}"
        row["urma_trace"] = {
            "request_count": len(requests),
            "wr_count": len(requests),
            "logical_write_count": len(logical_writes),
            "confirmed_logical_write_count": len(complete_writes),
            "critical_path_ms": round(critical_path_ms, 6),
            "latency_basis": latency_basis,
            "slowest_request_id": slowest["request_id"],
            "slowest_total_ms": round(slowest["total_ms"], 6),
            "wait_completion_ms": wait_ms,
            "wait_ratio_pct": round(wait_ratio, 3) if wait_ratio is not None else None,
            "urma_client_ratio_pct": round(urma_ratio, 3) if urma_ratio is not None else None,
            "max_inflight_wr": max_inflight,
            "max_remote_get_wr": max(item["remote_get_wr_count"] for item in requests),
            "max_wake_sched_ms": max_wake,
            "source_worker": slowest["source_worker"],
            "target_worker": slowest["target_worker"],
            "direction": direction,
            "labels": labels,
            "conclusion": (
                f"{latency_basis} {critical_path_ms:.3f}ms；最慢 WR {slowest['total_ms']:.3f}ms"
                f"（request {slowest['request_id'] or '日志未携带'}，"
                f"{direction}），URMA/Client {urma_ratio:.1f}%"
                + (f"，completion wait {wait_ms:.3f}ms（{wait_ratio:.1f}%）" if wait_ms is not None else "")
                + f"，Inflight WR 最大 {max_inflight}；证据标签：{'、'.join(labels) or '无突出标签'}。"
            ),
        }
    for row in rows:
        _apply_inline_query_urma_attribution(row)
        _apply_query_rpc_attribution(row)
        _apply_query_urma_timeout_attribution(row)
        row["query_meta_detail"] = _query_meta_detail(row)
        _refine_data_access_scope(row)
        row["non_transport_analysis"] = _non_transport_analysis(row, topology)
        _apply_focus_breakdown(row)
    return sorted(rows, key=lambda row: (row["timestamp"], row["trace_id"]))


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
    """Group TopN rows by the report's five Client-latency reference bands."""

    bands = (
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
    return segments


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


CORRELATION_STYLE = r'''
.correlation-grid{display:grid;grid-template-columns:minmax(0,1fr);gap:14px}
.correlation-grid>div{min-width:0;border:1px solid var(--line);border-radius:9px;padding:12px}
.correlation-summary{display:grid;grid-template-columns:repeat(4,minmax(140px,1fr));gap:10px;margin:12px 0}
.correlation-summary .metric{background:#f9fbfe}
table{width:100%;table-layout:fixed}
.table-wrap{max-height:560px;overflow-y:auto;overflow-x:hidden}
.worker-table-wrap{overflow-y:auto;overflow-x:hidden}
.panel th,.panel td{overflow-wrap:anywhere;word-break:break-word}
.panel code{white-space:normal;word-break:break-all}
.panel .badge{white-space:normal}
.worker-name{max-width:none;overflow:visible;text-overflow:clip;white-space:normal}
.conclusion-cell{min-width:0}
.nowrap{white-space:normal}
.controls>*{max-width:100%}
#trace-table th:nth-child(1){width:8%}#trace-table th:nth-child(2){width:7%}#trace-table th:nth-child(3){width:12%}#trace-table th:nth-child(4){width:7%}#trace-table th:nth-child(5){width:9%}#trace-table th:nth-child(6){width:12%}#trace-table th:nth-child(7),#trace-table th:nth-child(8),#trace-table th:nth-child(9),#trace-table th:nth-child(10){width:7%}#trace-table th:nth-child(11),#trace-table th:nth-child(12){width:6%}#trace-table th:nth-child(13){width:5%}
#urma-trace-table th:nth-child(1){width:9%}#urma-trace-table th:nth-child(2){width:7%}#urma-trace-table th:nth-child(3){width:14%}#urma-trace-table th:nth-child(4),#urma-trace-table th:nth-child(5){width:8%}#urma-trace-table th:nth-child(6){width:6%}#urma-trace-table th:nth-child(7){width:5%}#urma-trace-table th:nth-child(8),#urma-trace-table th:nth-child(9){width:7%}#urma-trace-table th:nth-child(10){width:29%}
#non-transport-table th:nth-child(1){width:7%}#non-transport-table th:nth-child(2){width:6%}#non-transport-table th:nth-child(3){width:11%}#non-transport-table th:nth-child(4){width:15%}#non-transport-table th:nth-child(5){width:10%}#non-transport-table th:nth-child(6),#non-transport-table th:nth-child(7),#non-transport-table th:nth-child(8),#non-transport-table th:nth-child(9),#non-transport-table th:nth-child(10){width:6%}#non-transport-table th:nth-child(11){width:21%}
#worker-correlation-table th:nth-child(1){width:10%}#worker-correlation-table th:nth-child(2){width:7%}#worker-correlation-table th:nth-child(3){width:13%}#worker-correlation-table th:nth-child(4){width:6%}#worker-correlation-table th:nth-child(5){width:14%}#worker-correlation-table th:nth-child(6){width:7%}#worker-correlation-table th:nth-child(7){width:6%}#worker-correlation-table th:nth-child(8),#worker-correlation-table th:nth-child(9){width:7%}#worker-correlation-table th:nth-child(10){width:23%}
#direct-worker-table th:first-child,#urma-source-table th:first-child{width:52%}#direct-worker-table th:not(:first-child),#urma-source-table th:not(:first-child){width:12%}
#write-trace-table th:nth-child(1){width:11%}#write-trace-table th:nth-child(2){width:12%}#write-trace-table th:nth-child(3){width:8%}#write-trace-table th:nth-child(4){width:16%}#write-trace-table th:nth-child(5),#write-trace-table th:nth-child(6),#write-trace-table th:nth-child(7){width:9%}#write-trace-table th:nth-child(8){width:15%}#write-trace-table th:nth-child(9){width:11%}
.time-segment-controls{display:flex;gap:8px;flex-wrap:wrap;margin:12px 0}
.time-segment-button{height:34px;border:1px solid #cfd8e6;border-radius:999px;background:#fff;padding:0 14px;cursor:pointer;color:var(--ink)}
.time-segment-button:hover{border-color:var(--blue);color:var(--blue)}
.time-segment-button.active{border-color:var(--blue);background:var(--blue);color:#fff;font-weight:700}
.time-segment-scope{min-height:24px;color:var(--muted);font-size:12px;line-height:1.6}
@media(max-width:1050px){
  .correlation-summary{grid-template-columns:repeat(3,1fr)}
  .controls input{min-width:0;flex:1 1 220px}
  #trace-table th:nth-child(6),#trace-table td:nth-child(6),#trace-table th:nth-child(9),#trace-table td:nth-child(9),#trace-table th:nth-child(11),#trace-table td:nth-child(11),#trace-table th:nth-child(12),#trace-table td:nth-child(12),#trace-table th:nth-child(13),#trace-table td:nth-child(13){display:none}
  #non-transport-table th:nth-child(4),#non-transport-table td:nth-child(4),#non-transport-table th:nth-child(5),#non-transport-table td:nth-child(5),#non-transport-table th:nth-child(7),#non-transport-table td:nth-child(7),#non-transport-table th:nth-child(8),#non-transport-table td:nth-child(8),#non-transport-table th:nth-child(10),#non-transport-table td:nth-child(10){display:none}
  #worker-correlation-table th:nth-child(2),#worker-correlation-table td:nth-child(2),#worker-correlation-table th:nth-child(8),#worker-correlation-table td:nth-child(8),#worker-correlation-table th:nth-child(9),#worker-correlation-table td:nth-child(9){display:none}
  #urma-trace-table th:nth-child(3),#urma-trace-table td:nth-child(3),#urma-trace-table th:nth-child(6),#urma-trace-table td:nth-child(6),#urma-trace-table th:nth-child(7),#urma-trace-table td:nth-child(7),#urma-trace-table th:nth-child(8),#urma-trace-table td:nth-child(8),#urma-trace-table th:nth-child(9),#urma-trace-table td:nth-child(9){display:none}
  #write-trace-table th:nth-child(1),#write-trace-table td:nth-child(1),#write-trace-table th:nth-child(5),#write-trace-table td:nth-child(5),#write-trace-table th:nth-child(7),#write-trace-table td:nth-child(7){display:none}
  .urma-request-table th:nth-child(n+6):nth-child(-n+12),.urma-request-table td:nth-child(n+6):nth-child(-n+12),.urma-request-table th:nth-child(14),.urma-request-table td:nth-child(14),.urma-request-table th:nth-child(15),.urma-request-table td:nth-child(15),.urma-request-table th:nth-child(16),.urma-request-table td:nth-child(16),.urma-request-table th:nth-child(17),.urma-request-table td:nth-child(17){display:none}
}
@media(max-width:650px){
  .correlation-summary{grid-template-columns:repeat(2,1fr)}
  .panel table{font-size:10px}.panel th,.panel td{padding:6px 3px}
  #trace-table th:nth-child(1),#trace-table td:nth-child(1),#trace-table th:nth-child(4),#trace-table td:nth-child(4),#trace-table th:nth-child(5),#trace-table td:nth-child(5),#trace-table th:nth-child(16),#trace-table td:nth-child(16){display:none}
  #worker-correlation-table th:nth-child(3),#worker-correlation-table td:nth-child(3),#worker-correlation-table th:nth-child(7),#worker-correlation-table td:nth-child(7){display:none}
  #write-trace-table th:nth-child(9),#write-trace-table td:nth-child(9){display:none}
  #urma-time-table th:nth-child(2),#urma-time-table td:nth-child(2),#urma-time-table th:nth-child(3),#urma-time-table td:nth-child(3),#urma-time-table th:nth-child(6),#urma-time-table td:nth-child(6),#urma-time-table th:nth-child(7),#urma-time-table td:nth-child(7){display:none}
  #urma-edge-table th:nth-child(2),#urma-edge-table td:nth-child(2),#urma-edge-table th:nth-child(3),#urma-edge-table td:nth-child(3),#urma-edge-table th:nth-child(6),#urma-edge-table td:nth-child(6),#urma-edge-table th:nth-child(7),#urma-edge-table td:nth-child(7){display:none}
  #direct-worker-table th:nth-child(3),#direct-worker-table td:nth-child(3),#direct-worker-table th:nth-child(4),#direct-worker-table td:nth-child(4),#direct-worker-table th:nth-child(5),#direct-worker-table td:nth-child(5){display:none}
  #urma-source-table th:nth-child(3),#urma-source-table td:nth-child(3){display:none}
}
'''

WRITE_SECTION = r'''
<section class="panel" id="write-analysis">
<h2>9. 写入瓶颈分析</h2>
<div class="notice"><b>独立口径：</b>写入不复用读取 QueryAndGet/Get 阶段。以 Client Set 总窗口为边界，分别展示 Create RPC、MemoryCopy、Client→Worker URMA通信、URMA调度/线程开销、Publish RPC、Worker Publish/元数据、其他调度、RPC网络和RPC框架。RPC timing 不闭合时不强拆 handler/网络/框架。</div>
<div id="write-summary" class="finding-grid"></div>
<h3 class="chart-title">图 9-1 写入 TopN 互斥阶段</h3>
<div id="write-timeline-chart" class="chart" style="height:430px"></div>
<h3>表 9-1 写入 Trace</h3>
<div class="worker-table-wrap"><table id="write-trace-table"><thead><tr><th>时间</th><th>Trace</th><th>总时延</th><th>主问题</th><th>Create RPC</th><th>写入数据</th><th>URMA通信/调度</th><th>Publish RPC</th><th>Worker Publish/元数据</th><th>RPC网络/框架</th></tr></thead><tbody></tbody></table></div>
<div id="write-trace-pager" class="pager"></div>
</section>
'''

WRITE_SCRIPT = r'''
const WRITE_STAGE_COLORS={'Create RPC其他':'#2563eb','写入MemoryCopy':'#0ea5a4','写入URMA通信':'#f59e0b','写入URMA调度/线程开销':'#c026d3','Publish RPC其他':'#7c3aed','Worker Publish/元数据':'#16a34a','其他调度/线程开销':'#9333ea','RPC网络相关':'#dc2626','RPC框架':'#64748b','未解释残差':'#9aa4b2'};
let writePage=1;const WRITE_PAGE_SIZE=8;
function renderWriteAnalysis(){
  const summary=$('write-summary'),body=$('write-trace-table').querySelector('tbody'),pager=$('write-trace-pager');
  if(!WRITE_ROWS.length){summary.innerHTML='<div class="empty">本批未采集 Client 写入 Trace</div>';body.innerHTML='<tr><td colspan="10" class="empty">0条/未采集</td></tr>';pager.innerHTML='';correlationChart('write-timeline-chart',false,{});return}
  const problems=Object.entries(WRITE_AGG.problem_counts||{}).sort((a,b)=>b[1]-a[1]),top=problems[0]||['未解释残差',0];
  summary.innerHTML=`<div class="finding-card"><b>写入 Trace</b><br>${WRITE_AGG.trace_count}条，失败 ${WRITE_AGG.failed_count}条；Client p90 ${fmt(WRITE_AGG.latency.p90)}，max ${fmt(WRITE_AGG.latency.max)}。</div><div class="finding-card"><b>最多主问题</b><br>${esc(top[0])} ${top[1]}条。主问题按每条 Trace 最大互斥阶段计数。</div><div class="finding-card"><b>URMA / RPC 口径</b><br>URMA通信与明确的 URMA 调度分开；wait→poll/completion wait 不整体算调度。Create/Publish RPC其他在完整 trailer 缺失时保持未细分。</div>`;
  const labels=WRITE_ROWS.map((row,index)=>`${String(index+1).padStart(3,'0')} ${row.timestamp.slice(11,19)}`),stages=Object.keys(WRITE_STAGE_COLORS),series=stages.map(name=>({name,type:'bar',stack:'write',barMaxWidth:12,data:WRITE_ROWS.map(row=>row.write_breakdown_ms[name]),itemStyle:{color:WRITE_STAGE_COLORS[name]}}));
  const chart=chartAt('write-timeline-chart');chart.setOption({animation:false,legend:{top:0,data:stages},grid:{left:48,right:20,top:68,bottom:72},tooltip:{trigger:'axis',axisPointer:{type:'shadow'},formatter:params=>{const row=WRITE_ROWS[params[0]?.dataIndex||0];return `<b>${esc(row.trace_id)}</b><br>Client ${fmt(row.client_ms)}<br>Create ${fmt(row.create_rpc_ms)} / Publish ${fmt(row.publish_rpc_ms)}<br>${stages.map(name=>`${esc(name)}: ${fmt(row.write_breakdown_ms[name])}`).join('<br>')}`}},xAxis:{type:'category',data:labels,axisLabel:{interval:9,rotate:35,fontSize:10}},yAxis:{type:'value',name:'耗时 (ms)'},dataZoom:[{type:'inside',start:0,end:100},{type:'slider',height:18,bottom:10,start:0,end:100}],series});
  const pages=Math.max(1,Math.ceil(WRITE_ROWS.length/WRITE_PAGE_SIZE));writePage=Math.min(writePage,pages);const selected=WRITE_ROWS.slice((writePage-1)*WRITE_PAGE_SIZE,writePage*WRITE_PAGE_SIZE);
  body.innerHTML=selected.map(row=>`<tr><td class="nowrap">${esc(row.timestamp.slice(11,23))}</td><td><code>${esc(row.trace_id)}</code></td><td>${latencyValue(row.client_ms)}</td><td><span class="badge" style="background:${WRITE_STAGE_COLORS[row.write_primary_stage]}20;color:${WRITE_STAGE_COLORS[row.write_primary_stage]}">${esc(row.write_primary_stage)}</span></td><td>${latencyValue(row.create_rpc_ms)}</td><td>${latencyValue(row.write_data_ms)}<div class="caption">${esc(row.write_data_basis)}</div></td><td>${latencyValue(row.write_breakdown_ms['写入URMA通信'])} / ${latencyValue(row.write_breakdown_ms['写入URMA调度/线程开销'])}</td><td>${latencyValue(row.publish_rpc_ms)}</td><td>${latencyValue(row.write_breakdown_ms['Worker Publish/元数据'])}</td><td>${latencyValue(row.write_breakdown_ms['RPC网络相关'])} / ${latencyValue(row.write_breakdown_ms['RPC框架'])}</td></tr>`).join('');
  pager.innerHTML=`<button id="write-prev" ${writePage<=1?'disabled':''}>上一页</button><span>${writePage}/${pages} · ${WRITE_ROWS.length}条</span><button id="write-next" ${writePage>=pages?'disabled':''}>下一页</button>`;$('write-prev').onclick=()=>{if(writePage>1){writePage--;renderWriteAnalysis()}};$('write-next').onclick=()=>{if(writePage<pages){writePage++;renderWriteAnalysis()}};
}
'''


CORRELATION_SECTION = r'''
<section class="panel" id="query-meta-analysis">
<h2>4-A. QueryMeta 根因分析</h2>
<div class="notice"><b>定界口径：</b>当前源码为 <code>WorkerOCService.QueryAndGet</code>（分析器同时兼容历史 <code>MasterOCService.QueryAndGet</code>）。它不只查元数据：携带 <code>data_request</code> 时，metadata-affine Worker 还会准备本地数据响应，可通过 UB/URMA 内联返回数据。Worker 日志明确 <code>inlineHits &gt; 0</code>、<code>transport: UB</code>，且 URMA source Worker、Trace 和 attempt 时间窗唯一匹配时，逻辑 URMA Write 关键路径会从 QueryAndGet 父窗口剝离；同 Worker 的 QueryAndGet 父窗口内若唯一匹配到带 <code>elapsedMs</code> 的 <code>URMA_WAIT_TIMEOUT</code>，Stacked Bars 进一步拆为 QueryMeta/QueryAndGet 独占与 <b>URMA超时等待窗口</b>。该窗口是等待到超时的证据，不冒充完成态 WR 耗时。WR 分片不求和。失败且只有 <code>cntl_error_code=1008</code> 时，只能确认 Client 等待到截止点；缺少 server trailer 时，Worker 执行、响应发送、RPC residual 与 Client 截止观察仍未闭合。</div>
<div id="query-meta-summary" class="finding-grid"></div>
<h3>QueryAndGet 超时流程定界</h3>
<div id="query-meta-timeout-flow" class="finding-grid"></div>
<div class="correlation-grid">
<div><h3>图 4-A-1 QueryAndGet 互斥细类</h3><div id="query-meta-detail-chart" class="chart"></div></div>
<div><h3>图 4-A-2 QueryMeta 时间分布</h3><div id="query-meta-time-chart" class="chart"></div></div>
<div><h3>图 4-A-3 QueryMeta 发起节点分布</h3><div id="query-meta-worker-chart" class="chart"></div></div>
<div><h3>图 4-A-4 Meta Owner 目标分布</h3><div id="query-meta-target-chart" class="chart"></div></div>
</div>
</section>
<section class="panel" id="worker-correlation">
<h2>4-B. 同 Worker 时间关联分析</h2>
<div class="notice"><b>口径：</b>本章始终使用全量 TopN，不受总览五段筛选影响。只在同一日志 Worker 的本地时间内按 1 秒桶聚合，并查看 QueryMeta / RemoteGet 失败前后各 1 秒。RPC、UB、元数据、数据访问是四个独立观察维度，不把父子窗口相加；<code>URMA_ELAPSED_TOTAL &gt; 1.5ms</code> 才是慢 WR，<code>transferPath: UB</code> 本身不是 UB 耗时证据。同期出现只表示伴随关系，不证明因果。</div>
<div class="controls">
<select id="correlation-worker-filter"><option value="">全部有证据 Worker</option></select>
<select id="correlation-category-filter"><option value="">全部类别</option><option value="query_meta">QueryMeta</option><option value="remote_get">RemoteGet</option><option value="urma_wr">URMA WR</option><option value="rpc">RPC</option><option value="local_processing">本地处理</option></select>
<select id="correlation-status-filter"><option value="problem">失败/慢事件</option><option value="failed">仅失败</option><option value="slow">仅慢事件</option><option value="normal">仅正常</option><option value="all">全部状态</option></select>
<select id="correlation-relation-filter"><option value="">全部关联</option><option value="direct_same_trace">同Trace直接证据</option><option value="concurrent_companion">同Worker同期伴随</option><option value="no_companion_evidence">无伴随证据</option></select>
<select id="correlation-latency-band-filter"><option value="">全部Client时延</option><option value="5-6">5–6ms</option><option value="6-7">6–7ms</option><option value="7-10">7–10ms</option><option value="10-20">10–20ms</option><option value="20+">≥20ms</option></select>
<input id="correlation-time-start" type="datetime-local" step="0.001" title="Worker本地开始时间">
<input id="correlation-time-end" type="datetime-local" step="0.001" title="Worker本地结束时间">
<button id="correlation-reset-filter">清空筛选</button>
</div>
<div id="worker-correlation-summary" class="correlation-summary"></div>
<div class="correlation-grid">
<div><h3>图 4-B-1 RPC 通信残差 / 服务端 / 排队</h3><div class="caption">“RPC网络”桶沿用历史字段名，实际表示 bRPC 未被服务端执行和排队解释的通信残差（物理网络 + RPC framework），不能单独证明物理网络慢。</div><div id="worker-correlation-chart-rpc" class="chart"></div></div>
<div><h3>图 4-B-2 UB WR / completion wait / Inflight</h3><div id="worker-correlation-chart-ub" class="chart"></div></div>
<div><h3>图 4-B-3 元数据 QueryMeta</h3><div id="worker-correlation-chart-metadata" class="chart"></div></div>
<div><h3>图 4-B-4 数据访问 Local / RemoteGet</h3><div id="worker-correlation-chart-data" class="chart"></div></div>
</div>
<div class="worker-section"><h3>表 5-1 关联事件明细</h3><div class="table-wrap"><table id="worker-correlation-table"><thead><tr><th>Worker本地时间</th><th>Trace</th><th>Worker</th><th>维度</th><th>事件</th><th>耗时</th><th>失败</th><th>±1s慢WR</th><th>±1s RPC失败</th><th>关联判断</th></tr></thead><tbody></tbody></table></div><div id="worker-correlation-pager" class="pager"></div></div>
</section>
'''


CORRELATION_SCRIPT = r'''
function renderQueryMetaAnalysis(){
  const a=AGG.query_meta_analysis,node=$('query-meta-summary'),flowNode=$('query-meta-timeout-flow');
  if(!a||!a.trace_count){node.innerHTML='<div class="empty">TopN 未观测到 QueryMeta 证据</div>';flowNode.innerHTML='<div class="empty">没有 QueryAndGet 超时证据</div>';correlationChart('query-meta-detail-chart',false,{});correlationChart('query-meta-time-chart',false,{});correlationChart('query-meta-worker-chart',false,{});correlationChart('query-meta-target-chart',false,{});return}
  const burst=[...a.time_buckets].sort((x,y)=>y.trace_count-x.trace_count)[0],reasons=Object.entries(a.failure_reasons||{}).sort((x,y)=>y[1]-x[1]).map(([k,v])=>`${k||'原因未细分'} ${v}条`).join('；')||'无失败',details=Object.entries(a.detail_counts||{}).sort((x,y)=>y[1]-x[1]).map(([k,v])=>`${k} ${v}条`).join('；')||'未细分';
  const targets=(a.meta_targets||[]).slice(0,4).map(x=>`${x.target} ${x.trace_count}条`).join('；');node.innerHTML=`<div class="finding-card"><b>规模与失败</b><div>${a.trace_count}条，失败 ${a.failed_count}条；慢成功 ${a.slow_success_count}条</div></div><div class="finding-card"><b>互斥细类</b><div>${esc(details)}</div></div><div class="finding-card"><b>TryGet / URMA</b><div>显式本地TryGet+URMA ${a.try_get_urma_observed_count||0}条；慢WR（严格 &gt;1.5ms）${a.try_get_slow_urma_count||0}条。该标签不重复计入细类。</div></div><div class="finding-card"><b>最密集秒</b><div>${esc(burst?.second||'未观测')} · ${burst?.trace_count||0}条 / 失败${burst?.failed_count||0}条</div></div><div class="finding-card"><b>失败原因</b><div>${esc(reasons)}</div></div><div class="finding-card"><b>Meta Owner覆盖</b><div>${a.meta_target_coverage==='present'?`${a.meta_target_observed_count}条携带目标证据；${esc(targets)}`:'目标地址未观测；Worker仅为发起节点'}</div></div><div class="finding-card"><b>根因边界</b><div>${esc(a.root_cause_boundary)}</div></div>`;
  const f=a.timeout_flow||{};flowNode.innerHTML=!f.timeout_count?'<div class="empty">没有 QueryAndGet 超时证据</div>':`<div class="finding-card"><b>已确认超时流程</b><div>${esc(f.confirmed_flow)}</div></div><div class="finding-card"><b>请求形态</b><div>${f.timeout_count}条：单次/末次RPC吃满窗口 ${f.full_window_count}条，重试/退避累计 ${f.retry_budget_count}条；空响应 ${f.empty_response_count}条，server timing不可用 ${f.server_timing_unavailable_count}条。</div></div><div class="finding-card"><b>时间与节点</b><div>最密集 ${esc(f.dominant_second?.second||'未观测')}：${f.dominant_second?.trace_count||0}条；覆盖 ${f.distinct_initiator_count}个Client发起节点、${f.distinct_target_count}个Meta Owner目标。</div></div><div class="finding-card"><b>高概率共同机制</b><div>${esc(f.likely_common_mechanism)} 同Trace新建channel ${f.new_channel_count}/${f.timeout_count}条。</div></div><div class="finding-card"><b>当前根因结论</b><div>${esc(f.root_cause_status)}</div></div><div class="finding-card"><b>已排除的误判</b><div>${esc(f.ruled_out)} 未观测URMA total/timeout ${f.urma_not_observed_count}/${f.timeout_count}条。</div></div><div class="finding-card"><b>闭环所需证据</b><div>${esc(f.next_evidence)}</div></div>`;
  let detailRows=Object.entries(a.detail_counts||{}).sort((x,y)=>y[1]-x[1]),labels=detailRows.map(x=>x[0]);let option=correlationBase(labels);option.grid.bottom=95;option.xAxis.axisLabel={interval:0,rotate:25,fontSize:10};option.series=[{name:'Trace',type:'bar',data:detailRows.map(x=>x[1]),label:{show:true,position:'top'}}];correlationChart('query-meta-detail-chart',detailRows.length>0,option);
  labels=a.time_buckets.map(x=>x.second.slice(11));option=correlationBase(labels);option.series=[{name:'Trace',type:'bar',data:a.time_buckets.map(x=>x.trace_count)},{name:'失败',type:'bar',data:a.time_buckets.map(x=>x.failed_count)},{name:'QueryMeta p90',type:'line',yAxisIndex:1,showSymbol:false,data:a.time_buckets.map(x=>x.p90_ms)}];correlationChart('query-meta-time-chart',true,option);
  labels=a.initiators.map(x=>shortWorker(x.initiator));option=correlationBase(labels);option.series=[{name:'Trace',type:'bar',data:a.initiators.map(x=>x.trace_count)},{name:'失败',type:'bar',data:a.initiators.map(x=>x.failed_count)},{name:'QueryMeta p90',type:'line',yAxisIndex:1,data:a.initiators.map(x=>x.p90_ms)}];correlationChart('query-meta-worker-chart',true,option);
  labels=(a.meta_targets||[]).map(x=>x.target);option=correlationBase(labels);option.series=[{name:'Trace',type:'bar',data:(a.meta_targets||[]).map(x=>x.trace_count)}];correlationChart('query-meta-target-chart',labels.length>0,option)
}
function latencyBandMatches(value,band){if(!band)return true;const n=Number(value);if(band==='5-6')return n>=5&&n<6;if(band==='6-7')return n>=6&&n<7;if(band==='7-10')return n>=7&&n<10;if(band==='10-20')return n>=10&&n<20;return band==='20+'?n>=20:true}
function filteredCorrelationEvents(){
  const worker=$('correlation-worker-filter').value,category=$('correlation-category-filter').value,status=$('correlation-status-filter').value,relation=$('correlation-relation-filter').value,band=$('correlation-latency-band-filter').value,start=$('correlation-time-start').value,end=$('correlation-time-end').value;
  return AGG.worker_correlation.events.filter(event=>(!worker||event.worker===worker)&&(!category||event.kind===category)&&(!relation||event.companions?.relation===relation)&&latencyBandMatches(event.client_ms,band)&&(!start||event.timestamp>=start)&&(!end||event.timestamp<=end)&&(status==='all'||status==='failed'&&event.failed||status==='slow'&&event.is_slow||status==='normal'&&!event.failed&&!event.is_slow||status==='problem'&&(event.failed||event.is_slow)))
}
function metricFor(values){const clean=values.filter(x=>x!==null&&x!==undefined).map(Number).sort((a,b)=>a-b);return{count:clean.length,p90:clean.length?clean[Math.ceil(clean.length*.9)-1]:0,max:clean.length?clean[clean.length-1]:0}}
function buildCorrelationBuckets(events){
  const groups=new Map();events.filter(e=>e.timestamp&&e.worker&&e.worker!=='未明确').forEach(e=>{const key=`${e.worker}\n${e.timestamp.slice(0,19)}`;if(!groups.has(key))groups.set(key,[]);groups.get(key).push(e)});
  return [...groups.entries()].sort((a,b)=>a[0].localeCompare(b[0])).map(([key,items])=>{const [worker,second]=key.split('\n'),by=d=>items.filter(x=>x.dimension===d),rpc=by('rpc'),ub=by('ub'),metadata=by('metadata'),data=by('data');return{worker,second,trace_count:new Set(items.map(x=>x.trace_id)).size,rpc:{request_count:rpc.length,failure_count:rpc.filter(x=>x.failed).length,network_ms:metricFor(rpc.map(x=>x.network_ms)),server_ms:metricFor(rpc.map(x=>x.server_ms)),queue_ms:metricFor(rpc.map(x=>x.queue_ms))},ub:{wr_count:ub.length,slow_wr_count:ub.filter(x=>x.is_slow).length,total_ms:metricFor(ub.map(x=>x.latency_ms)),wait_ms:metricFor(ub.map(x=>x.wait_completion_ms)),inflight:metricFor(ub.map(x=>x.inflight_wr))},metadata:{request_count:metadata.length,failure_count:metadata.filter(x=>x.failed).length,latency_ms:metricFor(metadata.map(x=>x.latency_ms))},data:{local_count:data.filter(x=>x.kind==='local_processing').length,remote_count:data.filter(x=>x.kind==='remote_get').length,failure_count:data.filter(x=>x.failed).length,retry_count:data.filter(x=>x.retry).length,latency_ms:metricFor(data.map(x=>x.latency_ms))}}})
}
function correlationRows(){
  return sortRows('worker-correlation-table',filteredCorrelationEvents())
}
function correlationChart(id,hasEvidence,option){
  const node=$(id),old=echarts.getInstanceByDom(node);if(old)old.dispose();node.innerHTML='';
  if(!hasEvidence){node.innerHTML='<div class="empty">未观测到对应证据</div>';return}
  const chart=echarts.init(node,null,{renderer:'canvas'});charts.push(chart);chart.setOption(option)
}
function correlationAxis(buckets){return buckets.map(item=>correlationWorker?item.second.slice(11):`${item.second.slice(11)} · ${shortWorker(item.worker)}`)}
function correlationBase(labels){return{animation:false,tooltip:{trigger:'axis'},legend:{top:0},grid:{left:48,right:45,top:54,bottom:labels.length>24?70:45},xAxis:{type:'category',data:labels,axisLabel:{fontSize:10,rotate:labels.length>12?35:0}},yAxis:[{type:'value',name:'事件数',minInterval:1},{type:'value',name:'耗时 / Inflight'}],dataZoom:labels.length>24?[{type:'inside'},{type:'slider',bottom:5,height:18}]:[]}}
function renderWorkerCorrelationCharts(buckets){
  const labels=correlationAxis(buckets),rpc=buckets.some(x=>x.rpc.request_count),ub=buckets.some(x=>x.ub.wr_count),metadata=buckets.some(x=>x.metadata.request_count),data=buckets.some(x=>x.data.local_count+x.data.remote_count);
  let option=correlationBase(labels);option.series=[{name:'RPC失败',type:'bar',data:buckets.map(x=>x.rpc.failure_count)},{name:'network p90',type:'line',yAxisIndex:1,showSymbol:false,data:buckets.map(x=>x.rpc.network_ms.count?x.rpc.network_ms.p90:null)},{name:'server p90',type:'line',yAxisIndex:1,showSymbol:false,data:buckets.map(x=>x.rpc.server_ms.count?x.rpc.server_ms.p90:null)},{name:'queue p90',type:'line',yAxisIndex:1,showSymbol:false,data:buckets.map(x=>x.rpc.queue_ms.count?x.rpc.queue_ms.p90:null)}];correlationChart('worker-correlation-chart-rpc',rpc,option);
  option=correlationBase(labels);option.series=[{name:'WR',type:'bar',data:buckets.map(x=>x.ub.wr_count)},{name:'慢WR (>1.5ms)',type:'bar',data:buckets.map(x=>x.ub.slow_wr_count)},{name:'total p90',type:'line',yAxisIndex:1,showSymbol:false,data:buckets.map(x=>x.ub.total_ms.count?x.ub.total_ms.p90:null),markLine:{silent:true,lineStyle:{color:'#dc3545',type:'dashed'},label:{formatter:'慢WR阈值 1.5ms'},data:[{yAxis:AGG.worker_correlation.slow_wr_threshold_ms}]}},{name:'wait p90',type:'line',yAxisIndex:1,showSymbol:false,data:buckets.map(x=>x.ub.wait_ms.count?x.ub.wait_ms.p90:null)},{name:'Inflight p90',type:'line',yAxisIndex:1,showSymbol:false,data:buckets.map(x=>x.ub.inflight.count?x.ub.inflight.p90:null)}];correlationChart('worker-correlation-chart-ub',ub,option);
  option=correlationBase(labels);option.series=[{name:'QueryMeta请求',type:'bar',data:buckets.map(x=>x.metadata.request_count)},{name:'QueryMeta失败',type:'bar',data:buckets.map(x=>x.metadata.failure_count)},{name:'e2e p90',type:'line',yAxisIndex:1,showSymbol:false,data:buckets.map(x=>x.metadata.latency_ms.count?x.metadata.latency_ms.p90:null)}];correlationChart('worker-correlation-chart-metadata',metadata,option);
  option=correlationBase(labels);option.series=[{name:'Local',type:'bar',data:buckets.map(x=>x.data.local_count)},{name:'RemoteGet',type:'bar',data:buckets.map(x=>x.data.remote_count)},{name:'失败',type:'bar',data:buckets.map(x=>x.data.failure_count)},{name:'重试',type:'bar',data:buckets.map(x=>x.data.retry_count)},{name:'耗时 p90',type:'line',yAxisIndex:1,showSymbol:false,data:buckets.map(x=>x.data.latency_ms.count?x.data.latency_ms.p90:null)}];correlationChart('worker-correlation-chart-data',data,option)
}
function correlationRelation(event){const relation=event.companions?.relation;if(relation==='direct_same_trace')return'同 Trace 直接证据';if(relation==='concurrent_companion')return'同 Worker 同期伴随';if(relation==='no_companion_evidence')return'±1秒未观测到对应证据';return event.is_slow?'慢 WR 事件':'独立事件'}
function renderWorkerCorrelationTable(){
  const all=correlationRows(),pages=Math.max(1,Math.ceil(all.length/PAGE_SIZE));correlationPage=Math.min(correlationPage,pages);const rows=all.slice((correlationPage-1)*PAGE_SIZE,correlationPage*PAGE_SIZE),body=$('worker-correlation-table').querySelector('tbody');
  body.innerHTML=rows.map(event=>`<tr data-id="${esc(event.trace_id)}" class="${event.trace_id===selectedId?'selected ':''}${event.failed?'trace-failed':''}"><td>${esc(event.timestamp||'未观测')}</td><td><code>${esc(short(event.trace_id))}</code></td><td class="worker-name" title="${esc(event.worker)}">${esc(shortWorker(event.worker))}</td><td>${esc({rpc:'RPC',ub:'UB',metadata:'元数据',data:'数据访问'}[event.dimension]||event.dimension)}</td><td>${esc(event.method||event.kind)}</td><td>${event.latency_ms===null||event.latency_ms===undefined?'—':latencyValue(event.latency_ms)}</td><td>${badge(event.failed?'失败':'否',event.failed?'b-fail':'b-ok')}</td><td>${event.companions?.slow_wr_count??'—'}</td><td>${event.companions?.rpc_failure_count??'—'}</td><td>${esc(correlationRelation(event))}</td></tr>`).join('')||'<tr><td colspan="10" class="empty">当前 Worker 未观测到失败或慢事件</td></tr>';
  body.querySelectorAll('tr[data-id]').forEach(tr=>tr.onclick=()=>{selectedId=tr.dataset.id;renderWorkerCorrelationTable();renderTable();renderDetail();$('trace-detail-panel').scrollIntoView({behavior:'smooth'})});
  $('worker-correlation-pager').innerHTML=`<button id="worker-correlation-prev" ${correlationPage<=1?'disabled':''}>上一页</button><span>${correlationPage}/${pages} · ${all.length}条</span><button id="worker-correlation-next" ${correlationPage>=pages?'disabled':''}>下一页</button>`;$('worker-correlation-prev').onclick=()=>{if(correlationPage>1){correlationPage--;renderWorkerCorrelationTable()}};$('worker-correlation-next').onclick=()=>{if(correlationPage<pages){correlationPage++;renderWorkerCorrelationTable()}};updateSortableHeaders('worker-correlation-table')
}
function renderWorkerCorrelation(){
  const a=AGG.worker_correlation,events=filteredCorrelationEvents(),buckets=buildCorrelationBuckets(events),targetFailures=events.filter(event=>event.failed&&(event.kind==='query_meta'||event.kind==='remote_get')),slow=events.filter(event=>event.kind==='urma_wr'&&event.is_slow),direct=targetFailures.filter(event=>event.companions?.relation==='direct_same_trace').length,concurrent=targetFailures.filter(event=>event.companions?.relation==='concurrent_companion').length,missing=['rpc','ub','metadata','data'].filter(dimension=>!events.some(event=>event.dimension===dimension)).map(dimension=>({rpc:'RPC',ub:'UB',metadata:'元数据',data:'数据访问'}[dimension]));
  $('worker-correlation-summary').innerHTML=`<div class="metric"><span>筛选后事件</span><b>${events.length}</b><span>${new Set(events.map(x=>x.worker)).size}个 Worker</span></div><div class="metric"><span>QueryMeta / RemoteGet失败</span><b>${targetFailures.length}</b></div><div class="metric"><span>慢WR（严格 &gt; 1.5ms）</span><b>${slow.length}</b></div><div class="metric"><span>关联判断</span><b>同Trace ${direct} / 同期 ${concurrent}</b><span>${missing.length?`筛选范围未观测：${esc(missing.join('、'))}`:'四维均有证据'}；同期伴随不等于因果。</span></div>`;
  renderWorkerCorrelationCharts(buckets);renderWorkerCorrelationTable()
}
'''


HTML_TEMPLATE = r'''<!doctype html>
<html lang="zh-CN">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>SAME 3x105 QPS · Top100 关键瓶颈</title>
<style>
:root{--bg:#f4f7fb;--panel:#fff;--ink:#172033;--muted:#667085;--line:#dfe6ef;--nav:#0f2747;--blue:#2f6fed;--cyan:#17a2b8;--orange:#f59e0b;--red:#dc3545;--green:#18a36b;--purple:#7c5ce7;--gray:#9aa4b2}
*{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--ink);font-family:Inter,"Segoe UI","Microsoft YaHei",sans-serif;font-size:14px}.layout{display:grid;grid-template-columns:238px minmax(0,1fr);min-height:100vh}aside{position:sticky;top:0;height:100vh;overflow:auto;background:var(--nav);color:#fff;padding:22px 14px}aside h2{font-size:17px;margin:0 6px 16px}aside a{display:block;color:#cbd8e8;text-decoration:none;padding:7px 9px;border-radius:7px;margin:2px 0}aside a:hover,aside a.active{background:#1a3b66;color:#fff}aside a.sub{font-size:12px;padding:5px 9px 5px 22px;color:#9fb4cc}main{padding:26px 30px 60px;min-width:0;max-width:1600px}section[id],div[id]{scroll-margin-top:18px}.hero{background:linear-gradient(125deg,#0f2747,#254f86);color:#fff;border-radius:14px;padding:24px 28px;margin-bottom:18px}.hero h1{margin:0 0 8px;font-size:26px}.hero p{margin:0;color:#d9e5f5}.kpis{display:grid;grid-template-columns:repeat(6,minmax(120px,1fr));gap:12px;margin:16px 0}.kpi,.panel{background:var(--panel);border:1px solid var(--line);border-radius:12px;box-shadow:0 3px 12px rgba(15,39,71,.05)}.kpi{padding:14px}.kpi b{display:block;font-size:23px;margin-top:5px}.kpi span,.muted{color:var(--muted);font-size:12px}.problem-grid{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:14px}.panel{padding:18px;margin-bottom:14px}.panel h2{font-size:18px;margin:0 0 5px}.panel h3{font-size:15px;margin:0 0 10px}.caption{color:var(--muted);font-size:12px;margin-top:8px;line-height:1.6}.chart{width:100%;min-height:300px}.notice{border-left:4px solid var(--orange);background:#fff8e8;padding:12px 14px;border-radius:7px;margin:12px 0;color:#664d03}.controls{display:flex;gap:9px;flex-wrap:wrap;margin:12px 0}.controls select,.controls input,.controls button,.pager button{height:35px;border:1px solid #cfd8e6;border-radius:7px;background:#fff;padding:0 10px;color:var(--ink)}.controls input{min-width:270px}.controls button,.pager button{cursor:pointer}.controls button:hover,.pager button:hover{border-color:var(--blue);color:var(--blue)}.pager button:disabled{cursor:not-allowed;opacity:.45}table{width:100%;border-collapse:collapse;font-size:12px}th,td{padding:9px 8px;border-bottom:1px solid #e8edf4;text-align:left;vertical-align:top}th{background:#f7f9fc;position:sticky;top:0;z-index:1;color:#475467}tbody tr{cursor:pointer}tbody tr:hover,tbody tr.selected{background:#eef5ff}tbody tr.trace-failed td{background:#fff1f2}.badge{display:inline-block;padding:3px 7px;border-radius:999px;font-size:11px;white-space:nowrap}.b-remote{background:#e7f0ff;color:#245abf}.b-worker{background:#fff3d7;color:#9a6200}.b-rpc{background:#efe9ff;color:#6242bd}.b-fail{background:#ffe8ea;color:#bd2430}.b-ok{background:#e5f7ef;color:#147a50}.table-wrap{max-height:560px;overflow:auto;border:1px solid var(--line);border-radius:9px}.pager{display:flex;align-items:center;justify-content:flex-end;gap:9px;margin-top:10px}.worker-section{border-top:1px solid var(--line);padding-top:16px;margin-top:16px}.worker-section:first-of-type{border-top:0;padding-top:4px}.worker-table-wrap{overflow:auto;border:1px solid var(--line);border-radius:8px}.worker-name{max-width:320px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}.conclusion-cell{min-width:340px;line-height:1.55}.evidence-level{font-weight:700}.latency-warn{display:inline-block;background:#ffedd5;color:#9a3412;border-radius:4px;padding:1px 5px;font-weight:700}.latency-hot{display:inline-block;background:#fee2e2;color:#991b1b;border-radius:4px;padding:1px 5px;font-weight:700}.metric-grid{display:grid;grid-template-columns:repeat(5,1fr);gap:8px}.metric{border:1px solid var(--line);border-radius:8px;padding:9px}.metric b{display:block;margin-top:3px}.phase-row{display:grid;grid-template-columns:190px 1fr 90px;gap:8px;align-items:center;margin:8px 0}.phase-track{height:17px;background:#edf1f6;border-radius:5px;overflow:hidden}.phase-fill{height:100%}.trace-panels{margin-top:14px}.trace-log-groups{display:grid;gap:12px}.trace-log-group{border:1px solid var(--line);border-left:4px solid var(--blue);border-radius:8px;overflow:hidden;background:#fff}.trace-log-group h4{display:flex;justify-content:space-between;gap:12px;margin:0;padding:10px 12px;background:#f7f9fc}.trace-log-summary{padding:8px 12px;color:var(--muted);font-size:12px;line-height:1.6}.trace-log-group pre{margin:0;padding:12px;background:#0f172a;color:#dbeafe;white-space:pre-wrap;word-break:break-word;max-height:420px;overflow:auto;font:12px/1.6 "Cascadia Code",Consolas,monospace}.trace-log-group pre .log-line{display:block;border-bottom:1px solid #1f3048;padding:3px 5px}.trace-log-group details{background:#0f172a;border-top:1px solid #334155}.trace-log-group details summary{cursor:pointer;color:#bfdbfe;padding:9px 13px;font-size:12px}.trace-log-group details pre{border-top:1px solid #334155}.log-keyword{display:inline;border-radius:3px;padding:0 3px;font-weight:700}.log-tag-error{background:#fee2e2;color:#991b1b}.log-tag-deadline{background:#ffedd5;color:#9a3412}.log-tag-rpc{background:#dbeafe;color:#1e40af}.log-tag-urma{background:#ede9fe;color:#5b21b6}.log-tag-latency{background:#dcfce7;color:#166534}.log-token{border-radius:3px;padding:0 2px;font-weight:700}.finding-grid{display:grid;grid-template-columns:repeat(3,1fr);gap:10px;margin-top:14px}.finding-card{border:1px solid var(--line);border-top:3px solid var(--blue);border-radius:8px;padding:11px 12px;background:#f9fbfe;line-height:1.65}.guidance-grid{display:grid;grid-template-columns:repeat(3,1fr);gap:10px}.guidance-card{border:1px solid var(--line);border-left:4px solid var(--blue);border-radius:8px;padding:12px;background:#fff;line-height:1.6}.guidance-card b{display:block;margin-bottom:5px}.urma-summary{display:grid;grid-template-columns:repeat(5,minmax(120px,1fr));gap:10px;margin:12px 0}.urma-summary .metric{background:#f9fbfe}.diagnosis-tags{display:flex;gap:4px;flex-wrap:wrap}.diagnosis-tag{display:inline-block;padding:2px 6px;border-radius:999px;background:#fff3d7;color:#8a5700;font-size:10px}.urma-request-table th{position:static}.nowrap{white-space:nowrap}.empty{padding:28px;text-align:center;color:var(--muted)}@media(max-width:1050px){.layout{grid-template-columns:1fr}aside{display:none}main{padding:16px}.kpis{grid-template-columns:repeat(3,1fr)}.problem-grid,.finding-grid,.guidance-grid{grid-template-columns:1fr}.metric-grid{grid-template-columns:repeat(3,1fr)}.urma-summary{grid-template-columns:repeat(3,1fr)}}@media(max-width:650px){.kpis{grid-template-columns:repeat(2,1fr)}.metric-grid,.urma-summary{grid-template-columns:repeat(2,1fr)}}
.hero h1{overflow-wrap:anywhere;word-break:break-word}
</style>
</head>
<body data-deadline-ms="20">
<div class="layout">
<aside id="nav"><h2>Top100 诊断</h2><a href="#overview">1. 总览</a><a class="sub" href="#problem-count-chart">图 1-1 主问题 Trace 数</a><a class="sub" href="#problem-latency-chart">图 1-2 关键证据耗时</a><a class="sub" href="#error-analysis">图 1-3 错误细分</a><a class="sub" href="#stage-share-chart">图 1-4 关键阶段占比</a><a href="#timeline">2. Top100 时间序列</a><a class="sub" href="#timeline-chart">图 2-1 Stacked Bars</a><a href="#urma-analysis">3. URMA 批量分析</a><a class="sub" href="#urma-time-chart">图 3-1 WR 时间序列</a><a class="sub" href="#urma-worker-chart">图 3-2 源 Worker</a><a class="sub" href="#urma-trace-table">表 3-3 全部 URMA Trace</a><a href="#non-transport-analysis">4. 非 RPC 主导深挖</a><a class="sub" href="#non-transport-count-chart">图 4-1 精细分类</a><a class="sub" href="#non-transport-time-chart">图 4-2 时间分布</a><a class="sub" href="#non-transport-worker-chart">图 4-3 Worker 分布</a><a class="sub" href="#non-transport-table">表 4-1 逐 Trace 结论</a><a href="#workers">5. Data Worker 分析</a><a href="#source-logic">6. 最新代码逻辑</a><a href="#traces">7. Trace 查看</a><a class="sub" href="#trace-table">表 7-1 Top100</a><a class="sub" href="#trace-detail-panel">表 7-2 Trace 阶段明细</a><a class="sub" href="#trace-log-panel">日志框 7-3 Trace 证据日志</a></aside>
<main>
<section class="hero" id="overview"><h1>SAME 3x105 QPS · Top100 关键瓶颈</h1><p>100 个唯一 GET trace · triage 数据 ref d897aee1 · 代码逻辑校正 main/master@77fb2d9a</p></section>
<div class="kpis" id="kpis"></div>
<div class="problem-grid">
  <section class="panel"><h2>图 1-1 主问题 Trace 数</h2><div id="problem-count-chart" class="chart"></div><div class="caption">每条 trace 只按最大互斥阶段归入一类，合计 100 条；按成功/20ms 超时堆叠。</div></section>
  <section class="panel"><h2>图 1-2 关键证据耗时</h2><div id="problem-latency-chart" class="chart"></div><div class="caption">普通瓶颈展示主阶段耗时；URMA超时展示日志中的 timeout elapsedMs。两者不相加，鼠标悬停可确认口径。</div></section>
</div>
<section class="panel"><h2>问题类型与治理指导</h2><div id="problem-guidance"></div></section>
<section class="panel" id="error-analysis"><h2>图 1-3 错误分类与根因边界</h2><div class="notice"><b>口径：</b>“completion超时”是已确认故障点；pending WR 数和上浮链来自同 Trace 显式日志。当前证据不能继续区分接收端、链路/设备、CQ/JFC轮询或线程唤醒，不能把恢复动作当成最终根因。</div><div class="problem-grid"><div><h3 class="chart-title">图 1-3a 故障点细分</h3><div id="error-subcategory-chart" class="chart"></div></div><div><h3 class="chart-title">图 1-3b 错误上浮链</h3><div id="error-chain-chart" class="chart"></div></div></div><div id="error-analysis-summary" class="finding-grid"></div></section>
<section class="panel"><h2>图 1-4 关键阶段耗时占比</h2><div id="stage-share-chart" class="chart" style="height:300px"></div><div class="caption">展示 8 个关键互斥阶段及未解释残差的整体耗时占比。RPC 框架已扣除 handler、网络残差和明确调度/排队；用于看时间消耗，不表示 Trace 数。</div></section>
<section class="panel" id="time-segments"><h2>图 1-5 Client 总时延五档问题分布</h2><div class="notice"><b>口径：</b>按 Client 总时延分为 5–6ms、6–7ms、7–10ms、10–20ms、&gt;20ms 五档；边界 20ms 归入 10–20ms，只有 &gt;20ms 归入最后一档。低于 5ms 明确列为未纳入五档。档位筛选仅过滤总览、Stacked Bars 与 Trace 表，URMA/Worker 深挖仍展示全量 TopN。</div><div id="time-segment-controls" class="time-segment-controls"></div><div id="time-segment-scope" class="time-segment-scope"></div><div id="time-segment-chart" class="chart" style="height:340px"></div><div id="time-segment-summary" class="finding-grid"></div></section>
<section class="panel" id="timeline"><h2>2. Top100 时间序列</h2><h3>图 2-1 Stacked Bars</h3><div class="notice">Stacked bars 是裁剪后的互斥阶段耗时；红色菱形“URMA超时标记”是错误标签，不作为耗时再次堆叠。URMA建链、URMA通信、URMA调度/线程开销、QueryAndGet/Get其他业务、其他调度、RPC网络和RPC框架分开展示；证据不闭合的部分保留为未解释残差。</div><div id="timeline-chart" class="chart" style="height:430px"></div><div class="caption">按 trace 首时间从左到右；红色虚线为参考阈值，红色菱形为 URMA 超时，灰色圆点为其他失败。支持缩放并可点击选择 Trace。</div><div id="time-findings" class="finding-grid"></div></section>
<section class="panel" id="urma-analysis"><h2>3. URMA 批量分析</h2><div class="caption">本章始终使用全量 TopN，不受总览五段筛选影响。</div><div class="notice"><b>三级口径：</b><b>Client Get → 逻辑 URMA Write → WR分片</b>。每条 <code>URMA_ELAPSED_TOTAL</code> 对应一个 WR chunk。当前读取实现顺序异步 post 两个 WR、随后统一 reap；同一逻辑 Write 的关键耗时取两个 <b>URMA Elapsed Time 的最大值</b>，<b>不求和</b>，两个 WR 均在明细中展示。Inflight WR 是发送端 URMA manager 的全局在途快照，不是该 Get 的 WR 数。</div><div id="urma-summary-text"></div><div id="urma-summary-kpis" class="urma-summary"></div><div class="worker-section"><h3>图 3-1 WR 时间序列</h3><div class="notice"><b>时间口径：</b>横轴使用 URMA 事件所在 <b>Worker 本地时间</b>。跨 Worker 可能存在时钟偏移，不用不同机器绝对时间戳直接相减；URMA Elapsed Time、completion wait 与 trace_us 均来自同一个 WR 事件。</div><div id="urma-time-chart" class="chart" style="height:430px"></div><div class="worker-table-wrap"><table id="urma-time-table"><thead><tr><th>Worker本地分钟</th><th>Trace</th><th>WR</th><th>慢WR</th><th>Elapsed p90</th><th>wait p90</th><th>Inflight p90</th><th>max</th></tr></thead><tbody></tbody></table></div></div><div class="worker-section"><h3>图 3-2 源 Worker 与源→目标边</h3><div id="urma-worker-chart" class="chart" style="height:390px"></div><div class="worker-table-wrap"><table id="urma-edge-table"><thead><tr><th>源→目标</th><th>Trace</th><th>WR</th><th>慢WR</th><th>Elapsed p90</th><th>completion wait p90</th><th>Inflight WR p90/max</th><th>max</th></tr></thead><tbody></tbody></table></div></div><div class="worker-section"><h3>表 3-3 全部 URMA Trace</h3><div class="controls"><select id="urma-worker-filter"><option value="">全部源 Worker</option></select><select id="urma-label-filter"><option value="">全部诊断标签</option></select><input id="urma-trace-search" placeholder="搜索 Trace / Worker / Request"><button id="urma-reset-filter">清空筛选</button></div><div class="table-wrap"><table id="urma-trace-table"><thead><tr><th>时间</th><th>Trace</th><th>源→目标</th><th>Client总时延</th><th>最慢URMA Elapsed</th><th>URMA占比</th><th>WR数</th><th>Inflight WR max</th><th>RemoteGet WR</th><th>诊断</th></tr></thead><tbody></tbody></table></div><div id="urma-trace-pager" class="pager"></div></div></section>
<section class="panel" id="non-transport-analysis"><h2>4. 非 RPC / 非 UB 深挖</h2><div class="notice"><b>范围：</b>本章始终使用全量 TopN，不受总览五段筛选影响。只分析主问题为“数据访问父窗口/未细分”“远端供数非 URMA”或“未解释残差”的 Trace。五类结论来自显式超时/重试、RemotePull、Local processing、bRPC 字段和 ProcessGet 父窗口；只有明确 Local processing 且 remoteObjects=0 才判为本地，缺少子阶段时保持未细分。</div><div id="non-transport-summary" class="finding-grid"></div><div class="problem-grid"><div><h3>图 4-1 精细分类</h3><div id="non-transport-count-chart" class="chart"></div></div><div><h3>图 4-2 时间分布</h3><div id="non-transport-time-chart" class="chart"></div></div></div><div class="worker-section"><h3>图 4-3 Worker 分布</h3><div id="non-transport-worker-chart" class="chart" style="height:390px"></div></div><div class="worker-section"><h3>表 4-1 逐 Trace 结论</h3><div class="controls"><select id="non-transport-category-filter"><option value="">全部精细分类</option></select><select id="non-transport-confidence-filter"><option value="">全部证据强度</option><option value="高">高</option><option value="中">中</option></select><select id="non-transport-worker-filter"><option value="">全部 Data Worker</option></select><select id="non-transport-status-filter"><option value="">全部状态</option><option value="success">成功</option><option value="failed">20ms超时</option></select><input id="non-transport-search" placeholder="搜索 Trace / 结论 / 日志"><button id="non-transport-reset-filter">清空筛选</button></div><div class="table-wrap"><table id="non-transport-table"><thead><tr><th>时间</th><th>Trace</th><th>精细分类</th><th>证据</th><th>Data Worker</th><th>总时延</th><th>ProcessGet</th><th>BatchGet</th><th>RPC网络</th><th>URMA</th><th>结论</th></tr></thead><tbody></tbody></table></div><div id="non-transport-pager" class="pager"></div></div></section>
<section class="panel" id="workers"><h2>Data Worker 粒度分析</h2><div class="notice"><b>范围：</b>本章始终使用全量 TopN，不受总览五段筛选影响。<b>逐 Trace 位置口径：</b><code>DS_KV_CLIENT_GET transportType</code> 是本次 Client→Worker 实际传输：<b>SHM=本节点共享内存；UB=远端 Data Worker；TCP=位置不确定</b>（可能是远端，也可能是同节点 SHM 失败回退）；缺字段标“未确认”。该结论<b>仅覆盖当前 TopN 输入</b>，不外推整个运行。<code>DS_POSIX_GET</code> 仅用于标识有日志证据的处理 Worker，不能替代访问位置。异常耗时统一按 <span class="latency-warn">≥5ms</span>、<span class="latency-hot">≥20ms</span> 高亮。</div><div class="worker-section"><h3>有处理证据的 Data Worker 负载与尾延迟</h3><div id="direct-worker-chart" class="chart"></div><div class="worker-table-wrap"><table id="direct-worker-table"><thead><tr><th>Worker</th><th>Trace</th><th>超时</th><th>Client p90</th><th>Worker p50</th></tr></thead><tbody></tbody></table></div><div id="direct-worker-pager" class="pager"></div></div><div class="worker-section"><h3>URMA 源 Data Worker</h3><div id="urma-source-chart" class="chart"></div><div class="worker-table-wrap"><table id="urma-source-table"><thead><tr><th>Worker</th><th>Trace</th><th>URMA p50</th><th>URMA p90</th><th>Max</th></tr></thead><tbody></tbody></table></div><div id="urma-source-pager" class="pager"></div></div></section>
<section class="panel" id="source-logic"><h2>最新 main/master 代码逻辑校正</h2><div class="notice"><b><code>enableLocalCache=false</code> 读取主链：</b><code>Get</code> 进入 <code>GetFromTransportLayer</code>；<code>BuildTransportReadRequest</code> 先按 hash ring 选择 metadata owner，获得对象位置后，<code>ReplicaReader::ReadReplicaOnce</code> 直接向该 Data Worker 执行读取。因此这不是“Client→入口 Worker→Data Worker”的固定代理链。Trace 中出现不同的 DS_POSIX_GET Worker 与 URMA 日志 Worker 时，页面仅作“直连请求目标”和“URMA 供数端”的证据分组。</div><div class="caption">代码逻辑校正基线：main/master@77fb2d9a46f7ba9b658f4e1f6eba74c22206f9fe；triage 数据记录的 code ref 为 d897aee13b7f20b58a60f81e1b31e094964c996d。CodeGraph 仅用于定位，结论已对照当前实际源码。</div></section>
<section class="panel" id="traces"><h2>按分类查看 Trace</h2><div class="controls"><select id="category-filter"><option value="">全部主问题</option></select><select id="status-filter"><option value="">全部状态</option><option value="success">成功</option><option value="failed">20ms超时</option></select><select id="access-location-filter"><option value="">全部交付方式</option></select><select id="direct-worker-filter"><option value="">全部有证据 Data Worker</option></select><select id="urma-source-filter"><option value="">全部 URMA 源 Worker</option></select><input id="trace-search" placeholder="搜索 Trace / 失败原因 / 精确卡点 / 日志"><button id="reset-filter">清空筛选</button></div><div class="table-wrap"><table id="trace-table"><thead><tr><th>时间</th><th>Trace</th><th>主问题</th><th>失败原因</th><th>状态</th><th>交付方式</th><th>精确卡点</th><th>总时延</th><th>URMA建链</th><th>URMA通信</th><th>URMA调度/线程开销</th><th>QueryAndGet其他</th><th>Get其他</th><th>其他调度/线程开销</th><th>RPC网络相关</th><th>RPC框架</th><th>未解释残差</th></tr></thead><tbody></tbody></table></div><div class="pager"><button id="prev-page">上一页</button><span id="page-label"></span><button id="next-page">下一页</button></div></section>
<div class="trace-panels"><section class="panel" id="trace-detail-panel"><h2>Trace 阶段明细</h2><div id="trace-detail"></div></section><section class="panel" id="trace-log-panel"><h2>Trace 原始日志</h2><div class="caption">按 ds-trace-triage 证据阅读方式分组；每组默认展示最多 8 行重点，完整原始行按需展开，颜色只用于字段级辅助定位。</div><div id="trace-log-groups" class="trace-log-groups"></div></section></div>
</main></div><div id="tooltip" class="tooltip"></div>
<script>__ECHARTS_SOURCE__</script>
<script>
const ROWS=__ROWS__;
const AGG=__AGG__;
const COLORS={'明确远端数据阶段':'#2f6fed','数据访问处理未细分':'#f59e0b','Client→Data Worker RPC residual':'#7c5ce7'};
const STAGE_COLORS={'URMA建链':'#b45309','URMA通信':'#f59e0b','URMA调度/线程开销':'#c026d3','QueryAndGet其他业务':'#17a2b8','Get其他业务':'#2f6fed','其他调度/线程开销':'#7c3aed','RPC网络相关':'#dc3545','RPC框架':'#64748b','未解释残差':'#9aa4b2'};
const FOCUS_PROBLEMS=AGG.focus_problem_summary||AGG.problem_summary;
const PROBLEM_COLORS={...STAGE_COLORS,'URMA超时':'#b42318'};
const NON_TRANSPORT_COLORS={'Client UB接收缓冲分配失败':'#b42318','Client/Worker观测未闭合':'#dc3545','BatchGet超时/重试':'#f59e0b','Data Worker服务端处理':'#2f6fed','明确本地ProcessGet耗时':'#18a36b','Client数据获取窗口未细分':'#0ea5a4','ProcessGet内部未细分':'#7c5ce7'};
const PAGE_SIZE=8;const WORKER_PAGE_SIZE=8;const URMA_PAGE_SIZE=8;const NON_TRANSPORT_PAGE_SIZE=8;const EDGE_PAGE_SIZE=8;let page=1;let selectedId=ROWS[0]?.trace_id||null;let filtered=[...ROWS];let activeTimeSegment=null;const workerPages={direct:1,urma:1};const URMA_ROWS=ROWS.filter(r=>r.urma_trace);let urmaFiltered=[...URMA_ROWS];let urmaPage=1;let urmaTimePage=1;let urmaEdgePage=1;const NON_TRANSPORT_ROWS=ROWS.filter(r=>r.non_transport_analysis);let nonTransportFiltered=[...NON_TRANSPORT_ROWS];let nonTransportPage=1;
const SORT_CONFIGS={
  'trace-table':[['timestamp',r=>r.timestamp,'text'],['trace_id',r=>r.trace_id,'text'],['primary_problem',r=>r.focus_primary_problem,'text'],['failure_reason',r=>r.failure_reason||'','text'],['failed',r=>Number(r.failed),'number'],['access_location',r=>r.access_location,'text'],['data_access_scope',r=>r.data_access_scope||'','text'],['client_ms',r=>r.client_ms,'number'],...Object.keys(STAGE_COLORS).map(name=>[name,r=>r.focus_breakdown_ms[name],'number'])],
  'non-transport-table':[['timestamp',r=>r.timestamp,'text'],['trace_id',r=>r.trace_id,'text'],['deep_category',r=>r.non_transport_analysis.deep_category,'text'],['confidence',r=>r.non_transport_analysis.confidence,'text'],['direct_data_worker',r=>r.direct_data_worker,'text'],['client_ms',r=>r.client_ms,'number'],['worker_process_ms',r=>r.worker_process_ms,'number'],['batch_e2e_ms',r=>r.batch_e2e_ms,'number'],['rpc_network',r=>r.attribution_ms['RPC网络'],'number'],['urma_ms',r=>r.urma_ms,'number'],['conclusion',r=>r.non_transport_analysis.conclusion,'text']],
  'urma-trace-table':[['timestamp',r=>r.urma_requests[0]?.timestamp||r.timestamp,'text'],['trace_id',r=>r.trace_id,'text'],['direction',r=>r.urma_trace.direction,'text'],['client_ms',r=>r.client_ms,'number'],['slowest_total_ms',r=>r.urma_trace.slowest_total_ms,'number'],['ratio',r=>r.urma_trace.urma_client_ratio_pct,'number'],['request_count',r=>r.urma_trace.request_count,'number'],['max_inflight',r=>r.urma_trace.max_inflight_wr,'number'],['max_remote_get',r=>r.urma_trace.max_remote_get_wr,'number'],['conclusion',r=>r.urma_trace.conclusion,'text']],
  'urma-time-table':[['minute',r=>r.minute,'text'],['trace_count',r=>r.trace_count,'number'],['request_count',r=>r.request_count,'number'],['slow_request_count',r=>r.slow_request_count,'number'],['total_p90_ms',r=>r.total_p90_ms,'number'],['wait_p90_ms',r=>r.wait_p90_ms,'number'],['inflight_p90',r=>r.inflight_p90,'number'],['total_max_ms',r=>r.total_max_ms,'number']],
  'urma-edge-table':[['name',r=>r.name,'text'],['trace_count',r=>r.trace_count,'number'],['request_count',r=>r.request_count,'number'],['slow_request_count',r=>r.slow_request_count,'number'],['total_p90_ms',r=>r.total_p90_ms,'number'],['wait_p90_ms',r=>r.wait_p90_ms,'number'],['inflight_p90',r=>r.inflight_p90,'number'],['total_max_ms',r=>r.total_max_ms,'number']],
  'direct-worker-table':[['worker',r=>r.worker,'text'],['trace_count',r=>r.trace_count,'number'],['failed_count',r=>r.failed_count,'number'],['client_p90_ms',r=>r.client_p90_ms,'number'],['worker_p50_ms',r=>r.worker_p50_ms,'number']],
  'urma-source-table':[['worker',r=>r.worker,'text'],['trace_count',r=>r.trace_count,'number'],['urma_p50_ms',r=>r.urma_p50_ms,'number'],['urma_p90_ms',r=>r.urma_p90_ms,'number'],['urma_max_ms',r=>r.urma_max_ms,'number']],
  'worker-correlation-table':[['timestamp',r=>r.timestamp,'text'],['trace_id',r=>r.trace_id,'text'],['worker',r=>r.worker,'text'],['dimension',r=>r.dimension,'text'],['method',r=>r.method||r.kind,'text'],['latency_ms',r=>r.latency_ms,'number'],['failed',r=>Number(r.failed),'number'],['slow_wr_count',r=>r.companions?.slow_wr_count??0,'number'],['rpc_failure_count',r=>r.companions?.rpc_failure_count??0,'number'],['relation',r=>r.companions?.relation||'','text']],
};
const TABLE_SORTS={'trace-table':{key:'timestamp',direction:'asc'},'non-transport-table':{key:'timestamp',direction:'asc'},'urma-trace-table':{key:'timestamp',direction:'asc'},'urma-time-table':{key:'minute',direction:'asc'},'urma-edge-table':{key:'slow_request_count',direction:'desc'},'direct-worker-table':{key:'trace_count',direction:'desc'},'urma-source-table':{key:'trace_count',direction:'desc'},'worker-correlation-table':{key:'timestamp',direction:'asc'}};
const $=id=>document.getElementById(id);const esc=s=>String(s??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
const fmt=v=>`${Number(v||0).toFixed(3)} ms`;const short=id=>id.slice(0,8);
function sortRows(tableId,rows){const state=TABLE_SORTS[tableId],config=(SORT_CONFIGS[tableId]||[]).find(item=>item[0]===state?.key);if(!state||!config)return[...rows];const getter=config[1],type=config[2],direction=state.direction==='asc'?1:-1;return rows.map((row,index)=>({row,index,value:getter(row)})).sort((a,b)=>{const ae=a.value===null||a.value===undefined||a.value==='',be=b.value===null||b.value===undefined||b.value==='';if(ae||be)return ae===be?a.index-b.index:ae?1:-1;let cmp=type==='number'?Number(a.value)-Number(b.value):String(a.value).localeCompare(String(b.value),'zh-CN',{numeric:true,sensitivity:'base'});if(!Number.isFinite(cmp))cmp=0;return cmp?cmp*direction:a.index-b.index}).map(item=>item.row)}
function updateSortableHeaders(tableId){const table=$(tableId),state=TABLE_SORTS[tableId];if(!table||!state)return;table.querySelectorAll('thead th[data-sort-key]').forEach(th=>{const active=th.dataset.sortKey===state.key;th.textContent=`${th.dataset.sortLabel} ${active?(state.direction==='asc'?'↑':'↓'):'↕'}`;th.setAttribute('aria-sort',active?(state.direction==='asc'?'ascending':'descending'):'none')})}
function bindSortableHeaders(tableId,resetPage,render){const table=$(tableId),config=SORT_CONFIGS[tableId]||[];if(!table)return;table.querySelectorAll('thead th').forEach((th,index)=>{const column=config[index];if(!column)return;th.dataset.sortKey=column[0];th.dataset.sortLabel=th.textContent.trim();th.classList.add('sortable-header');th.tabIndex=0;th.setAttribute('role','button');const activate=()=>{const state=TABLE_SORTS[tableId];if(state.key===column[0])state.direction=state.direction==='asc'?'desc':'asc';else{state.key=column[0];state.direction=column[2]==='number'?'desc':'asc'}resetPage();render();updateSortableHeaders(tableId)};th.onclick=activate;th.onkeydown=event=>{if(event.key==='Enter'||event.key===' '){event.preventDefault();activate()}}});updateSortableHeaders(tableId)}
function badge(text,cls){return `<span class="badge ${cls}">${esc(text)}</span>`}
function latencyClass(value){const n=Number(value||0);return n>=20?'latency-hot':n>=5?'latency-warn':''}
function latencyValue(value){const cls=latencyClass(value);return `<span${cls?` class="${cls}"`:''}>${fmt(value)}</span>`}
function renderKpis(){const k=[['唯一 Trace',AGG.trace_count],['Client p50',`${AGG.latency.p50} ms`],['Client p90',`${AGG.latency.p90} ms`],['Client p99',`${AGG.latency.p99} ms`],['20ms 超时',AGG.failed_count],['本节点 SHM',AGG.access_locations['本节点SHM']||0]];$('kpis').innerHTML=k.map(([a,b])=>`<div class="kpi"><span>${a}</span><b>${b}</b></div>`).join('')}
const charts=[];
function chartAt(id){const node=$(id),old=echarts.getInstanceByDom(node);if(old){old.clear();return old}const chart=echarts.init(node,null,{renderer:'canvas'});charts.push(chart);return chart}
function shortProblem(name){return name==='QueryAndGet其他业务'?'QueryAndGet\n其他业务':name==='URMA调度/线程开销'?'URMA调度/\n线程开销':name==='其他调度/线程开销'?'其他调度/\n线程开销':name==='RPC网络相关'?'RPC网络\n相关':name==='未解释残差'?'未解释\n残差':name}
function scopeRows(){if(activeTimeSegment===null)return ROWS;const segment=AGG.latency_segments.find(item=>item.segment_id===activeTimeSegment);if(!segment)return ROWS;const ids=new Set(segment.trace_ids);return ROWS.filter(row=>ids.has(row.trace_id))}
function percentile(values,q){const ordered=values.map(Number).filter(Number.isFinite).sort((a,b)=>a-b);if(!ordered.length)return 0;const index=(ordered.length-1)*q,lower=Math.floor(index),upper=Math.ceil(index);return ordered[lower]+(ordered[upper]-ordered[lower])*(index-lower)}
function scopedProblemSummary(rows){const result={};Object.keys(PROBLEM_COLORS).forEach(problem=>{const matched=rows.filter(row=>row.focus_primary_problem===problem),stageValues=matched.map(row=>problem==='URMA超时'?row.urma_timeout_max_ms:row.focus_breakdown_ms[row.focus_primary_stage]).filter(value=>value!==null&&value!==undefined),clientValues=matched.map(row=>row.client_ms);result[problem]={trace_count:matched.length,success_count:matched.filter(row=>!row.failed).length,failed_count:matched.filter(row=>row.failed).length,stage_p50_ms:percentile(stageValues,.5),stage_p90_ms:percentile(stageValues,.9),stage_max_ms:stageValues.length?Math.max(...stageValues):0,client_p50_ms:percentile(clientValues,.5),client_p90_ms:percentile(clientValues,.9),metric_name:problem==='URMA超时'?'URMA timeout elapsedMs':'主阶段耗时',action:problem==='URMA超时'?'该类是错误覆盖层；用 timeout elapsedMs 和上浮链定位，不用互斥主阶段代替超时等待。':'按该互斥阶段的逐 Trace 明细和原始日志继续定位。'}});return result}
function scopedStageTotals(rows){const totals={};Object.keys(STAGE_COLORS).forEach(stage=>{totals[stage]=rows.reduce((sum,row)=>sum+Number(row.focus_breakdown_ms[stage]||0),0)});return totals}
function renderTimeSegments(){
  const segments=AGG.latency_segments||[],active=segments.find(item=>item.segment_id===activeTimeSegment),covered=segments.reduce((sum,item)=>sum+item.trace_count,0),outside=ROWS.length-covered,buttons=[`<button class="time-segment-button ${activeTimeSegment===null?'active':''}" data-segment="all">全部 · ${ROWS.length}条</button>`,...segments.map(item=>`<button class="time-segment-button ${activeTimeSegment===item.segment_id?'active':''}" data-segment="${item.segment_id}">${esc(item.label)} · ${item.trace_count}条</button>`)].join('');
  $('time-segment-controls').innerHTML=buttons;$('time-segment-controls').querySelectorAll('button').forEach(button=>button.onclick=()=>selectTimeSegment(button.dataset.segment==='all'?null:Number(button.dataset.segment)));
  $('time-segment-scope').innerHTML=active?`<b>当前范围：</b>Client 总时延 ${esc(active.label)} · ${active.trace_count} 条 · 失败 ${active.failed_count} 条 · 主问题 ${esc(active.dominant_problem)}`:`<b>当前范围：</b>全部 TopN · ${ROWS.length} 条；五档覆盖 ${covered} 条，低于 5ms 未纳入 ${outside} 条。`;
  const chart=chartAt('time-segment-chart'),problems=Object.keys(PROBLEM_COLORS);
  chart.setOption({animation:false,color:problems.map(problem=>PROBLEM_COLORS[problem]),tooltip:{trigger:'axis',axisPointer:{type:'shadow'},formatter:params=>{const item=segments[params[0]?.dataIndex||0];return `<b>Client 总时延 ${esc(item.label)}</b><br>Trace ${item.trace_count} · 失败 ${item.failed_count}<br>Client p50 ${fmt(item.client_p50_ms)} / p90 ${fmt(item.client_p90_ms)}<br>主问题 ${esc(item.dominant_problem)}<br>${problems.map(problem=>`${esc(problem)}: ${item.problem_counts[problem]||0}`).join('<br>')}`}},legend:{top:0,data:problems},grid:{left:48,right:20,top:48,bottom:46},xAxis:{type:'category',data:segments.map(item=>`${item.label}\n${item.trace_count}条`)},yAxis:{type:'value',name:'Trace 数',minInterval:1},series:problems.map(problem=>({name:problem,type:'bar',stack:'trace-count',data:segments.map(item=>item.problem_counts[problem]||0),barMaxWidth:72}))});
  chart.off('click');
  chart.on('click',params=>{const item=segments[params.dataIndex];if(item)selectTimeSegment(item.segment_id)});
  $('time-segment-summary').innerHTML=segments.map(item=>`<div class="finding-card" style="border-top-color:${activeTimeSegment===item.segment_id?'#dc3545':(PROBLEM_COLORS[item.dominant_problem]||'#9aa4b2')}"><b>Client 总时延 ${esc(item.label)}</b><br>${item.trace_count?`${item.trace_count} 条，失败 ${item.failed_count} 条；Client p50 ${fmt(item.client_p50_ms)} / p90 ${fmt(item.client_p90_ms)}；主问题 ${esc(item.dominant_problem)}。`:'当前 TopN 无 Trace。'}</div>`).join('');
}
function selectTimeSegment(segmentId){activeTimeSegment=segmentId;page=1;renderTimeSegments();renderProblemOverview();renderErrorAnalysis();renderStageShare();renderTimeline();applyFilters()}
function renderProblemOverview(){
  const summary=scopedProblemSummary(scopeRows()),names=Object.keys(summary),items=names.map(n=>summary[n]);
  const countChart=chartAt('problem-count-chart');
  countChart.setOption({color:['#2f6fed','#dc3545'],tooltip:{trigger:'axis',axisPointer:{type:'shadow'},formatter:ps=>{const x=items[ps[0].dataIndex];return `<b>${esc(names[ps[0].dataIndex])}</b><br>Trace ${x.trace_count}条<br>成功 ${x.success_count}条 / 超时 ${x.failed_count}条`}},legend:{top:0},grid:{left:42,right:15,top:40,bottom:75},xAxis:{type:'category',data:names,axisLabel:{interval:0,fontSize:10,formatter:shortProblem}},yAxis:{type:'value',name:'trace 数',minInterval:1,splitLine:{lineStyle:{color:'#e6ebf2'}}},series:[{name:'成功',type:'bar',stack:'trace-count',data:items.map(x=>x.success_count),barMaxWidth:48},{name:'20ms超时',type:'bar',stack:'trace-count',data:items.map(x=>x.failed_count),label:{show:true,position:'top',formatter:p=>items[p.dataIndex].trace_count}}]});
  countChart.off('click');countChart.on('click',p=>{if(names.includes(p.name)){$('category-filter').value=p.name;applyFilters();$('traces').scrollIntoView({behavior:'smooth'})}});
  const latencyChart=chartAt('problem-latency-chart');
  latencyChart.setOption({color:['#17a2b8','#f59e0b','#dc3545'],tooltip:{trigger:'axis',axisPointer:{type:'shadow'},formatter:ps=>{const i=ps[0].dataIndex,x=items[i];return `<b>${esc(names[i])}</b><br>${esc(x.metric_name)} p50 ${fmt(x.stage_p50_ms)} / p90 ${fmt(x.stage_p90_ms)} / max ${fmt(x.stage_max_ms)}<br>Client总时延 p50 ${fmt(x.client_p50_ms)} / p90 ${fmt(x.client_p90_ms)}`}},legend:{top:0},grid:{left:48,right:15,top:40,bottom:75},xAxis:{type:'category',data:names,axisLabel:{interval:0,fontSize:10,formatter:shortProblem}},yAxis:{type:'value',name:'关键证据 (ms)',splitLine:{lineStyle:{color:'#e6ebf2'}}},series:[{name:'p50',type:'bar',data:items.map(x=>x.stage_p50_ms),barMaxWidth:22},{name:'p90',type:'bar',data:items.map(x=>x.stage_p90_ms),barMaxWidth:22},{name:'max',type:'bar',data:items.map(x=>x.stage_max_ms),barMaxWidth:22}]});
}
function renderStageShare(){
  const chart=chartAt('stage-share-chart'),totals=scopedStageTotals(scopeRows());
  chart.setOption({color:Object.values(STAGE_COLORS),tooltip:{trigger:'item',formatter:p=>`${esc(p.name)}<br>${Number(p.value).toFixed(3)} ms (${p.percent}%)`},legend:{orient:'vertical',right:'8%',top:'middle',textStyle:{fontSize:11}},series:[{name:'阶段耗时占比',type:'pie',radius:['37%','68%'],center:['38%','50%'],label:{formatter:'{b}\n{d}%'},data:Object.entries(totals).map(([name,value])=>({name,value}))}]})
}
function renderProblemGuidance(){
  const items=Object.entries(FOCUS_PROBLEMS).filter(([,x])=>x.trace_count>0).sort((a,b)=>b[1].failed_count-a[1].failed_count||b[1].trace_count-a[1].trace_count);
  const top=items.slice(0,3).map(([name,x])=>`${name} ${x.trace_count}条/失败${x.failed_count}条`).join('；');
  $('problem-guidance').innerHTML=`<div class="notice"><b>本批治理顺序：</b>${esc(top)}。九个阶段互斥展示；URMA通信与URMA调度/线程开销分开，其他RPC/锁调度单列；证据不足的时间只进入未解释残差。</div><div class="guidance-grid">${items.map(([name,x])=>`<div class="guidance-card" style="border-left-color:${PROBLEM_COLORS[name]||'#667085'}"><b>${esc(name)} · ${x.trace_count}条 · 失败${x.failed_count}条</b><span>${esc(x.metric_name)} p50 ${fmt(x.stage_p50_ms)}，p90 ${fmt(x.stage_p90_ms)}；Client总时延 p90 ${fmt(x.client_p90_ms)}。</span><div class="caption">${esc(x.action)}</div></div>`).join('')}</div>`;
}
function renderErrorAnalysis(){
  const errorRows=scopeRows().filter(row=>row.error_family),timeoutRows=errorRows.filter(row=>row.error_family==='URMA超时'),rpcRows=errorRows.filter(row=>row.error_family==='RPC截止超时'),countBy=key=>errorRows.reduce((result,row)=>{const value=row[key]||'未细分';result[value]=(result[value]||0)+1;return result},{}),sub=Object.entries(countBy('error_subcategory')),chains=Object.entries(countBy('error_chain_category')),subChart=chartAt('error-subcategory-chart'),chainChart=chartAt('error-chain-chart');
  const shortError=value=>String(value).replace('URMA completion超时·','URMA超时\n').replace('QueryMeta RPC超时→TransportGet失败→','QueryMeta RPC→').replace('Data RPC超时→TransportGet失败→','Data RPC→').replace('URMA超时→UB异常响应→','URMA→UB异常→').replace('URMA超时→外层RPC deadline→','URMA→RPC deadline→'),barOption=(items,color)=>({animation:false,tooltip:{trigger:'axis'},grid:{left:55,right:20,top:25,bottom:82},xAxis:{type:'category',data:items.map(x=>x[0]),axisLabel:{interval:0,fontSize:10,formatter:shortError}},yAxis:{type:'value',name:'Trace数',minInterval:1},series:[{type:'bar',data:items.map(x=>x[1]),itemStyle:{color},label:{show:true,position:'top'}}]});
  subChart.setOption(barOption(sub,'#b42318'));chainChart.setOption(barOption(chains,'#7c5ce7'));
  const single=timeoutRows.filter(row=>row.error_pending_wrs===1).length,multiple=timeoutRows.filter(row=>Number(row.error_pending_wrs)>1).length,unknown=timeoutRows.filter(row=>row.error_pending_wrs===null||row.error_pending_wrs===undefined).length;
  $('error-analysis-summary').innerHTML=`<div class="finding-card"><b>URMA故障点</b><br>${timeoutRows.length} 条为 URMA WRITE completion 等待超时；其中单 pending WR ${single} 条，多 pending WR ${multiple} 条，pending 未观测 ${unknown} 条。</div><div class="finding-card"><b>RPC截止超时</b><br>${rpcRows.length} 条；QueryMeta ${rpcRows.filter(row=>row.error_subcategory==='QueryMeta RPC deadline').length} 条，Data RPC ${rpcRows.filter(row=>row.error_subcategory==='Data RPC deadline').length} 条。</div><div class="finding-card"><b>恢复动作</b><br>send lane 封存/强制回收是 timeout 后的保护动作，不作为 timeout 的起因。</div><div class="finding-card"><b>最终根因边界</b><br>缺少接收端完成、设备事件和 CQ/JFC 调度闭环，当前不能区分接收端、链路/设备、poll 或线程唤醒；失败RPC缺少 trailer 时也不能把 0 当实测。</div>`;
}
function shortWorker(worker){const value=String(worker??'未明确');return value.length>28?`${value.slice(0,12)}…${value.slice(-12)}`:value}
function renderTimeFindings(){$('time-findings').innerHTML=AGG.time_findings.map((text,i)=>`<div class="finding-card"><b>${i+1}. 时间结论</b><br>${esc(text)}</div>`).join('')}
function maybeMs(value){return value===null||value===undefined?'—':latencyValue(value)}
function diagnosisTags(labels){return `<div class="diagnosis-tags">${(labels||[]).map(label=>`<span class="diagnosis-tag">${esc(label)}</span>`).join('')}</div>`}
function selectUrmaTrace(traceId){selectedId=traceId;renderTable();renderDetail();$('trace-detail-panel').scrollIntoView({behavior:'smooth'})}
function renderUrmaAnalysis(){
  if(!URMA_ROWS.length){$('urma-summary-text').innerHTML='<div class="notice"><b>本次无 URMA 证据：</b>对应图表和请求表保持为空，不将缺失值按 0 处理。</div>';$('urma-summary-kpis').innerHTML='';return}
  const a=AGG.urma_analysis,corr=a.inflight_total_correlation,corrText=corr===null?'样本不足或无方差，相关性不可计算':`${Math.abs(corr)<.3?'弱相关':Math.abs(corr)<.6?'中等相关':'强相关'}（r=${corr.toFixed(3)}）`,waitRatio=a.request_total_ms.p90>0&&a.wait_completion_ms.count?a.wait_completion_ms.p90/a.request_total_ms.p90:null,waitText=waitRatio===null?'completion wait 未观测，不能判断等待占比':waitRatio>=.7?`completion wait p90 ${fmt(a.wait_completion_ms.p90)}，占 total p90 的 ${(waitRatio*100).toFixed(1)}%，等待窗口主导`:`completion wait p90 ${fmt(a.wait_completion_ms.p90)}，占 total p90 的 ${(waitRatio*100).toFixed(1)}%，不能判为等待主导`,hotTime=[...a.time_buckets].sort((x,y)=>y.slow_request_count-x.slow_request_count||y.total_p90_ms-x.total_p90_ms)[0],hotWorker=[...a.source_workers].sort((x,y)=>y.slow_request_count-x.slow_request_count||y.total_p90_ms-x.total_p90_ms)[0],volumeWorker=[...a.source_workers].sort((x,y)=>y.request_count-x.request_count||y.total_p90_ms-x.total_p90_ms)[0];
  $('urma-summary-text').innerHTML=`<div class="notice"><b>批量结论：</b>${a.trace_count} 条 Trace 共 ${a.request_count} 个 WR；URMA total p90 ${fmt(a.request_total_ms.p90)}、p99 ${fmt(a.request_total_ms.p99)}，固定以 total &gt; ${fmt(a.slow_threshold_ms)} 定义慢 WR，共 ${a.slow_request_count} 个；等于阈值仍为正常。${waitText}。Inflight WR 与 total：${corrText}；只有可计算时才作为伴随压力信号。</div><div class="finding-grid"><div class="finding-card"><b>时间集中</b><br>${esc(hotTime.minute.slice(11))} 有 ${hotTime.slow_request_count}/${hotTime.request_count} 个慢 WR，total p90 ${fmt(hotTime.total_p90_ms)}；慢事件主要集中在这一 Worker 本地分钟。</div><div class="finding-card"><b>尾延迟 Worker</b><br>${esc(hotWorker.worker)} 有 ${hotWorker.slow_request_count}/${hotWorker.request_count} 个慢 WR，total p90 ${fmt(hotWorker.total_p90_ms)}、max ${fmt(hotWorker.total_max_ms)}。</div><div class="finding-card"><b>高负载 Worker</b><br>${esc(volumeWorker.worker)} 共 ${volumeWorker.request_count} 个 WR，Inflight p90 ${volumeWorker.inflight_p90}、total p90 ${fmt(volumeWorker.total_p90_ms)}；需要结合源→目标边继续定位。</div></div>`;
  const kpis=[['URMA Trace',a.trace_count],['WR事件',a.request_count],['慢WR',a.slow_request_count],['total p90',fmt(a.request_total_ms.p90)],['Inflight p90',a.inflight_wr.p90]];
  $('urma-summary-kpis').innerHTML=kpis.map(([name,value])=>`<div class="metric"><span>${name}</span><b>${value}</b></div>`).join('');
  const events=URMA_ROWS.flatMap(row=>row.urma_requests.map(request=>({...request,trace_id:row.trace_id}))).sort((x,y)=>x.timestamp.localeCompare(y.timestamp)||x.trace_id.localeCompare(y.trace_id));
  const eventLabels=events.map((item,index)=>`${String(index+1).padStart(3,'0')} ${item.timestamp.slice(11,23)}`),waits=events.map(item=>item.wait_completion_ms||0),other=events.map((item,index)=>Math.max(0,item.total_ms-waits[index]));
  const timeChart=chartAt('urma-time-chart');
  timeChart.setOption({animation:false,tooltip:{trigger:'axis',axisPointer:{type:'shadow'},formatter:params=>{const item=events[params[0]?.dataIndex||0];return `<b>${esc(item.trace_id)}</b><br>request ${esc(item.request_id||'日志未携带')} · ${esc(item.timestamp)}<br>${esc(item.source_worker)} → ${esc(item.target_worker)}<br>total ${fmt(item.total_ms)} / completion wait ${fmt(item.wait_completion_ms)}<br>Inflight WR ${item.urma_inflight_wr_count} / RemoteGet WR ${item.remote_get_wr_count}<br>srcChipInflight ${esc(item.src_chip_inflight)}`}},legend:{top:0},grid:{left:50,right:54,top:48,bottom:72},xAxis:{type:'category',data:eventLabels,axisLabel:{interval:13,rotate:35,fontSize:10}},yAxis:[{type:'value',name:'URMA ms'},{type:'value',name:'Inflight WR',min:0}],dataZoom:[{type:'inside',start:0,end:100},{type:'slider',height:18,bottom:10}],series:[{name:'completion wait',type:'bar',stack:'urma-total',data:waits,itemStyle:{color:'#7c5ce7'},barMaxWidth:11,markLine:{silent:true,symbol:'none',lineStyle:{color:'#dc3545',type:'dashed'},label:{formatter:`慢WR ${a.slow_threshold_ms}ms`},data:[{yAxis:a.slow_threshold_ms}]}},{name:'total其余窗口',type:'bar',stack:'urma-total',data:other,itemStyle:{color:'#f59e0b'},barMaxWidth:11},{name:'Inflight WR',type:'line',yAxisIndex:1,data:events.map(item=>item.urma_inflight_wr_count),symbolSize:3,lineStyle:{width:1,color:'#2f6fed'},itemStyle:{color:'#2f6fed'}}]});
  timeChart.on('click',params=>{const item=events[params.dataIndex];if(item)selectUrmaTrace(item.trace_id)});
  renderUrmaTimeTable();
  const workers=a.source_workers.slice(0,14),workerChart=chartAt('urma-worker-chart');
  workerChart.setOption({tooltip:{trigger:'axis',axisPointer:{type:'shadow'},formatter:params=>{const item=workers[params[0]?.dataIndex||0];return `<b>${esc(item.worker)}</b><br>Trace ${item.trace_count} / WR ${item.request_count} / 慢WR ${item.slow_request_count}<br>total p90 ${fmt(item.total_p90_ms)} / max ${fmt(item.total_max_ms)}<br>completion wait p90 ${fmt(item.wait_p90_ms)}<br>Inflight p90 ${item.inflight_p90} / max ${item.inflight_max}`}},legend:{top:0},grid:{left:48,right:48,top:48,bottom:90},xAxis:{type:'category',data:workers.map(item=>item.worker),axisLabel:{rotate:40,fontSize:10,formatter:shortWorker}},yAxis:[{type:'value',name:'ms'},{type:'value',name:'慢WR',minInterval:1}],series:[{name:'total p90',type:'bar',data:workers.map(item=>item.total_p90_ms),itemStyle:{color:'#f59e0b'},barMaxWidth:24},{name:'completion wait p90',type:'bar',data:workers.map(item=>item.wait_p90_ms),itemStyle:{color:'#7c5ce7'},barMaxWidth:24},{name:'慢WR数',type:'line',yAxisIndex:1,data:workers.map(item=>item.slow_request_count),itemStyle:{color:'#dc3545'}}]});
  renderUrmaEdgeTable();
  a.source_workers.forEach(item=>$('urma-worker-filter').insertAdjacentHTML('beforeend',`<option value="${esc(item.worker)}">${esc(item.worker)}</option>`));
  [...new Set(URMA_ROWS.flatMap(row=>row.urma_trace.labels))].sort().forEach(label=>$('urma-label-filter').insertAdjacentHTML('beforeend',`<option value="${esc(label)}">${esc(label)}</option>`));
  ['urma-worker-filter','urma-label-filter'].forEach(id=>$(id).onchange=applyUrmaFilters);$('urma-trace-search').oninput=applyUrmaFilters;$('urma-reset-filter').onclick=()=>{$('urma-worker-filter').value='';$('urma-label-filter').value='';$('urma-trace-search').value='';applyUrmaFilters()};
  renderUrmaTraceTable();
}
function renderUrmaTimeTable(){const all=sortRows('urma-time-table',AGG.urma_analysis.time_buckets),pages=Math.max(1,Math.ceil(all.length/URMA_PAGE_SIZE));urmaTimePage=Math.min(urmaTimePage,pages);const rows=all.slice((urmaTimePage-1)*URMA_PAGE_SIZE,urmaTimePage*URMA_PAGE_SIZE),table=$('urma-time-table'),body=table.querySelector('tbody');body.innerHTML=rows.map(item=>`<tr><td>${esc(item.minute)}</td><td>${item.trace_count}</td><td>${item.request_count}</td><td>${item.slow_request_count}</td><td>${latencyValue(item.total_p90_ms)}</td><td>${latencyValue(item.wait_p90_ms)}</td><td>${item.inflight_p90}</td><td>${latencyValue(item.total_max_ms)}</td></tr>`).join('');let pager=$('urma-time-pager');if(!pager){pager=document.createElement('div');pager.id='urma-time-pager';pager.className='pager';table.parentElement.after(pager)}pager.innerHTML=`<button id="urma-time-prev" ${urmaTimePage<=1?'disabled':''}>上一页</button><span>${urmaTimePage}/${pages} · ${all.length}条</span><button id="urma-time-next" ${urmaTimePage>=pages?'disabled':''}>下一页</button>`;$('urma-time-prev').onclick=()=>{if(urmaTimePage>1){urmaTimePage--;renderUrmaTimeTable()}};$('urma-time-next').onclick=()=>{if(urmaTimePage<pages){urmaTimePage++;renderUrmaTimeTable()}};updateSortableHeaders('urma-time-table')}
function renderUrmaEdgeTable(){const all=sortRows('urma-edge-table',AGG.urma_analysis.worker_edges),pages=Math.max(1,Math.ceil(all.length/EDGE_PAGE_SIZE));urmaEdgePage=Math.min(urmaEdgePage,pages);const rows=all.slice((urmaEdgePage-1)*EDGE_PAGE_SIZE,urmaEdgePage*EDGE_PAGE_SIZE),body=$('urma-edge-table').querySelector('tbody');body.innerHTML=rows.map(item=>`<tr><td class="worker-name" title="${esc(item.name)}">${esc(shortWorker(item.source_worker))} → ${esc(shortWorker(item.target_worker))}</td><td>${item.trace_count}</td><td>${item.request_count}</td><td>${item.slow_request_count}</td><td>${latencyValue(item.total_p90_ms)}</td><td>${latencyValue(item.wait_p90_ms)}</td><td>${item.inflight_p90} / ${item.inflight_max}</td><td>${latencyValue(item.total_max_ms)}</td></tr>`).join('');$('urma-edge-pager').innerHTML=`<button id="urma-edge-prev" ${urmaEdgePage<=1?'disabled':''}>上一页</button><span>${urmaEdgePage}/${pages} · ${all.length}条</span><button id="urma-edge-next" ${urmaEdgePage>=pages?'disabled':''}>下一页</button>`;$('urma-edge-prev').onclick=()=>{if(urmaEdgePage>1){urmaEdgePage--;renderUrmaEdgeTable()}};$('urma-edge-next').onclick=()=>{if(urmaEdgePage<pages){urmaEdgePage++;renderUrmaEdgeTable()}};updateSortableHeaders('urma-edge-table')}
function applyUrmaFilters(){const worker=$('urma-worker-filter').value,label=$('urma-label-filter').value,q=$('urma-trace-search').value.trim().toLowerCase();urmaFiltered=URMA_ROWS.filter(row=>(!worker||row.urma_trace.source_worker===worker)&&(!label||row.urma_trace.labels.includes(label))&&(!q||row.trace_id.toLowerCase().includes(q)||row.urma_trace.direction.toLowerCase().includes(q)||row.urma_requests.some(item=>String(item.request_id).includes(q))));urmaPage=1;renderUrmaTraceTable()}
function renderUrmaTraceTable(){const pages=Math.max(1,Math.ceil(urmaFiltered.length/URMA_PAGE_SIZE));urmaPage=Math.min(urmaPage,pages);const selected=urmaFiltered.slice((urmaPage-1)*URMA_PAGE_SIZE,urmaPage*URMA_PAGE_SIZE),body=$('urma-trace-table').querySelector('tbody');body.innerHTML=selected.map(row=>{const u=row.urma_trace;return `<tr data-id="${esc(row.trace_id)}" class="${row.trace_id===selectedId?'selected':''}"><td class="nowrap" title="Worker本地时间">${esc(row.urma_requests[0].timestamp.slice(11,23))}</td><td><code>${esc(short(row.trace_id))}</code></td><td class="worker-name" title="${esc(u.direction)}">${esc(shortWorker(u.source_worker))} → ${esc(shortWorker(u.target_worker))}</td><td>${latencyValue(row.client_ms)}</td><td title="${esc(u.latency_basis)}">${latencyValue(u.critical_path_ms)}</td><td>${u.urma_client_ratio_pct?.toFixed(1)??'—'}%</td><td>${u.request_count}</td><td>${u.max_inflight_wr}</td><td>${u.max_remote_get_wr}</td><td>${diagnosisTags(u.labels)}<div class="caption">${esc(u.conclusion)}</div></td></tr>`}).join('')||'<tr><td colspan="10" class="empty">没有匹配的 URMA Trace</td></tr>';body.querySelectorAll('tr[data-id]').forEach(tr=>tr.onclick=()=>selectUrmaTrace(tr.dataset.id));$('urma-trace-pager').innerHTML=`<button id="urma-trace-prev" ${urmaPage<=1?'disabled':''}>上一页</button><span>${urmaPage}/${pages} · ${urmaFiltered.length}条</span><button id="urma-trace-next" ${urmaPage>=pages?'disabled':''}>下一页</button>`;$('urma-trace-prev').onclick=()=>{if(urmaPage>1){urmaPage--;renderUrmaTraceTable()}};$('urma-trace-next').onclick=()=>{if(urmaPage<pages){urmaPage++;renderUrmaTraceTable()}}}
function renderUrmaRequestSummary(row){
  if(!row.urma_requests?.length)return '<div id="urma-request-summary" class="empty">该 Trace 没有 URMA request 证据</div>';
  const writes=(row.urma_logical_writes||[]).map(w=>`<div class="metric"><span>逻辑Write ${w.write_index}</span><b>${latencyValue(w.slowest_wr_ms)}</b><span>${w.wr_count}/${w.expected_wr_count||'?'} WR；取最慢 URMA Elapsed；WR求和 ${w.sum_wr_ms.toFixed(3)}ms 仅作证据、不可作为关键路径</span></div>`).join('');
  const sched=row.urma_scheduling_detail_ms||{};
  const schedMetrics=[['wake sched',sched.wake_sched_latency],['thread sched',sched.thread_sched],['notify→awake',sched.notify_to_awake],['poll JFC',sched.poll_jfc],['notify',sched.notify]].map(([name,value])=>`<div class="metric"><span>${name}</span><b>${maybeMs(value)}</b></div>`).join('');
  return `<div id="urma-request-summary"><h3 style="margin-top:18px">逻辑 URMA Write / WR 明细</h3><div class="notice">${esc(row.urma_trace.conclusion)} 每个 Request ID 是一个 WR chunk；顺序异步 post、统一 reap 时取两个 <b>URMA Elapsed Time 的最大值</b>，不求和。completion wait / wait→poll 是 reap 等待窗口，不整体等价于线程调度；Inflight WR 是发送端全局快照。</div><h4>URMA 调度/线程显式证据</h4><div class="metric-grid">${schedMetrics}</div><div class="caption">仅取关键路径最慢 WR${row.urma_scheduling_request_id?`（request ${esc(row.urma_scheduling_request_id)}）`:''}的兼容观测最大值，不累加重叠字段；该值从 URMA Elapsed 中剥离。wait→poll 与 completion wait 仅作为等待窗口证据。</div><div class="metric-grid">${writes}</div><div class="worker-table-wrap"><table class="urma-request-table"><thead><tr><th>Worker本地时间</th><th>Request</th><th>Chunk</th><th>源→目标</th><th>URMA Elapsed Time</th><th>completion wait</th><th>wait→poll</th><th>poll call</th><th>notify→awake</th><th>wake sched</th><th>poll_jfc</th><th>notify</th><th>thread sched</th><th>Inflight WR</th><th>RemoteGet WR</th><th>srcChipInflight</th><th>CPU</th><th>数据量</th><th>状态</th></tr></thead><tbody>${row.urma_requests.map(item=>`<tr><td class="nowrap">${esc(item.timestamp.slice(11,23))}</td><td><code>${esc(item.request_id||'日志未携带')}</code></td><td>${item.write_chunk_index&&item.write_chunk_count?`${item.write_chunk_index}/${item.write_chunk_count}`:'—'}</td><td class="worker-name" title="${esc(item.src_addr)} → ${esc(item.target_addr)}">${esc(shortWorker(item.source_worker))} → ${esc(shortWorker(item.target_worker))}</td><td>${latencyValue(item.total_ms)}</td><td>${maybeMs(item.wait_completion_ms)}</td><td>${maybeMs(item.wait_to_poll_ms)}</td><td>${maybeMs(item.poll_call_ms)}</td><td>${maybeMs(item.notify_to_awake_ms)}</td><td>${maybeMs(item.wake_sched_latency_ms)}</td><td>${maybeMs(item.poll_jfc_ms)}</td><td>${maybeMs(item.notify_ms)}</td><td>${maybeMs(item.thread_sched_ms)}</td><td>${item.urma_inflight_wr_count??'—'}</td><td>${item.remote_get_wr_count??'—'}</td><td><code>${esc(item.src_chip_inflight)}</code></td><td>${item.cpuid??'—'}</td><td>${item.data_size?`${(item.data_size/1048576).toFixed(0)} MiB`:'—'}</td><td>${esc(item.status)}</td></tr>`).join('')}</tbody></table></div></div>`
}
function shortDeep(name){return name.replace('BatchGet','BatchGet\n').replace('Data Worker服务端','Data Worker服务端\n').replace('明确本地','明确本地\n').replace('ProcessGet内部','ProcessGet内部\n').replace('截止点','截止点\n')}
function renderNonTransportAnalysis(){
  const a=AGG.non_transport_analysis,categories=a.categories,names=categories.map(item=>item.category);
  const count=name=>categories.find(item=>item.category===name)?.trace_count||0,direct=count('明确本地ProcessGet耗时')+count('BatchGet超时/重试')+count('Data Worker服务端处理'),deadline=count('截止点观测盲区'),blind=count('ProcessGet内部未细分'),examples=categories.map(item=>[item,NON_TRANSPORT_ROWS.filter(row=>row.non_transport_analysis.deep_category===item.category).sort((x,y)=>y.non_transport_analysis.observed_ms-x.non_transport_analysis.observed_ms)[0]]).filter(([,row])=>row);
  $('non-transport-conclusions').innerHTML=`<h3>现有日志可以得出的结论</h3><div class="finding-grid"><div class="finding-card" style="border-top-color:#18a36b"><b>${direct} 条有直接原因证据</b><div>来自明确本地 ProcessGet、BatchGet 超时/重试或 Data Worker 服务端处理。</div></div><div class="finding-card" style="border-top-color:#dc3545"><b>${deadline} 条截止点观测空窗</b><div>现有 server_exec/network residual 未覆盖完整窗口；不能直接归为网络耗时。</div></div><div class="finding-card" style="border-top-color:#7c5ce7"><b>${blind} 条 ProcessGet 内部未细分</b><div>已知子阶段不足以覆盖父窗口；不能武断归因为 CPU、锁或线程调度。</div></div></div><h3 style="margin-top:16px">各类代表 Trace</h3><div class="guidance-grid">${examples.map(([item,row])=>`<div class="guidance-card"><b><code>${esc(short(row.trace_id))}</code> · ${esc(item.category)}</b>Client ${fmt(row.client_ms)}；${esc(row.non_transport_analysis.conclusion)}</div>`).join('')||'<div class="empty">本次没有非 RPC / 非 UB 深挖样本</div>'}</div><div class="notice"><b>需要补充的埋点：</b>本地对象锁/查找/数据准备/response attachment；远端 future 等待与重试 attempt；request register/timer/ReturnToClient；Client/Worker 四点时间戳。</div>`;
  $('non-transport-summary').innerHTML=categories.map(item=>`<div class="finding-card" style="border-top-color:${NON_TRANSPORT_COLORS[item.category]}"><b>${esc(item.category)} · ${item.trace_count}条</b><div>超时 ${item.failed_count}条 · Client p90 ${fmt(item.client_p90_ms)} · 已确认窗口 p50 ${fmt(item.observed_p50_ms)}</div><div class="caption">${esc(NON_TRANSPORT_ROWS.find(row=>row.non_transport_analysis.deep_category===item.category)?.non_transport_analysis.next_action||'')}</div></div>`).join('');
  const countChart=chartAt('non-transport-count-chart');
  countChart.setOption({animation:false,tooltip:{trigger:'axis',axisPointer:{type:'shadow'},formatter:ps=>{const item=categories[ps[0].dataIndex];return `<b>${esc(item.category)}</b><br>Trace ${item.trace_count}条 / 超时 ${item.failed_count}条<br>Client p50 ${fmt(item.client_p50_ms)} / p90 ${fmt(item.client_p90_ms)} / max ${fmt(item.client_max_ms)}<br>已确认窗口 p50 ${fmt(item.observed_p50_ms)}`}},legend:{top:0},grid:{left:45,right:18,top:42,bottom:88},xAxis:{type:'category',data:names,axisLabel:{interval:0,fontSize:10,formatter:shortDeep}},yAxis:{type:'value',name:'Trace数',minInterval:1},series:[{name:'成功',type:'bar',stack:'trace',data:categories.map(item=>item.trace_count-item.failed_count),itemStyle:{color:'#2f6fed'},barMaxWidth:34},{name:'20ms超时',type:'bar',stack:'trace',data:categories.map(item=>item.failed_count),itemStyle:{color:'#dc3545'},barMaxWidth:34}]});
  countChart.on('click',p=>{const category=names[p.dataIndex];if(category){$('non-transport-category-filter').value=category;applyNonTransportFilters()}});
  const timeRows=[...NON_TRANSPORT_ROWS].sort((x,y)=>x.timestamp.localeCompare(y.timestamp)),timeChart=chartAt('non-transport-time-chart');
  timeChart.setOption({animation:false,tooltip:{trigger:'axis',axisPointer:{type:'shadow'},formatter:ps=>{const row=timeRows[ps[0]?.dataIndex||0],d=row.non_transport_analysis;return `<b>${esc(row.trace_id)}</b><br>${esc(row.timestamp)}<br>${esc(d.deep_category)} · 证据${esc(d.confidence)}<br>Client ${fmt(row.client_ms)} / 已确认窗口 ${fmt(d.observed_ms)}<br>${esc(d.conclusion)}`}},grid:{left:48,right:18,top:38,bottom:70},xAxis:{type:'category',data:timeRows.map((row,i)=>`${String(i+1).padStart(2,'0')} ${row.timestamp.slice(11,19)}`),axisLabel:{interval:4,rotate:35,fontSize:10}},yAxis:{type:'value',name:'ms'},dataZoom:[{type:'inside'},{type:'slider',height:17,bottom:8}],series:[{name:'Client总时延',type:'bar',data:timeRows.map(row=>({value:row.client_ms,itemStyle:{color:NON_TRANSPORT_COLORS[row.non_transport_analysis.deep_category]}})),barMaxWidth:14,markLine:{silent:true,symbol:'none',lineStyle:{color:'#dc3545',type:'dashed'},label:{formatter:'20ms deadline'},data:[{yAxis:20}]}},{name:'已确认窗口',type:'line',data:timeRows.map(row=>row.non_transport_analysis.observed_ms),symbolSize:4,lineStyle:{width:1,color:'#172033'},itemStyle:{color:'#172033'}}]});
  timeChart.on('click',p=>{const row=timeRows[p.dataIndex];if(row)selectNonTransportTrace(row.trace_id)});
  const workers=a.workers.slice(0,16),workerChart=chartAt('non-transport-worker-chart');
  const workerSeries=names.map(category=>({name:category,type:'bar',stack:'trace',data:workers.map(item=>item.categories[category]),itemStyle:{color:NON_TRANSPORT_COLORS[category]},barMaxWidth:30}));
  workerSeries.push({name:'Client p90',type:'line',yAxisIndex:1,data:workers.map(item=>item.client_p90_ms),itemStyle:{color:'#172033'}});
  workerChart.setOption({animation:false,tooltip:{trigger:'axis',axisPointer:{type:'shadow'}},legend:{top:0,type:'scroll'},grid:{left:45,right:52,top:58,bottom:92},xAxis:{type:'category',data:workers.map(item=>item.worker),axisLabel:{rotate:40,fontSize:10,formatter:shortWorker}},yAxis:[{type:'value',name:'Trace数',minInterval:1},{type:'value',name:'Client p90(ms)'}],series:workerSeries});
  workerChart.on('click',p=>{const worker=workers[p.dataIndex]?.worker;if(worker){$('non-transport-worker-filter').value=worker;applyNonTransportFilters()}});
  names.forEach(category=>$('non-transport-category-filter').insertAdjacentHTML('beforeend',`<option value="${esc(category)}">${esc(category)}</option>`));
  a.workers.forEach(item=>$('non-transport-worker-filter').insertAdjacentHTML('beforeend',`<option value="${esc(item.worker)}">${esc(item.worker)}</option>`));
  ['non-transport-category-filter','non-transport-confidence-filter','non-transport-worker-filter','non-transport-status-filter'].forEach(id=>$(id).onchange=applyNonTransportFilters);$('non-transport-search').oninput=applyNonTransportFilters;$('non-transport-reset-filter').onclick=()=>{['non-transport-category-filter','non-transport-confidence-filter','non-transport-worker-filter','non-transport-status-filter'].forEach(id=>$(id).value='');$('non-transport-search').value='';applyNonTransportFilters()};
  renderNonTransportTable();
}
function applyNonTransportFilters(){const category=$('non-transport-category-filter').value,confidence=$('non-transport-confidence-filter').value,worker=$('non-transport-worker-filter').value,status=$('non-transport-status-filter').value,q=$('non-transport-search').value.trim().toLowerCase();nonTransportFiltered=NON_TRANSPORT_ROWS.filter(row=>{const d=row.non_transport_analysis;return(!category||d.deep_category===category)&&(!confidence||d.confidence===confidence)&&(!worker||row.direct_data_worker===worker)&&(!status||(status==='failed')===row.failed)&&(!q||row.trace_id.toLowerCase().includes(q)||d.conclusion.toLowerCase().includes(q)||row.evidence.some(text=>text.toLowerCase().includes(q)))});nonTransportPage=1;renderNonTransportTable()}
function renderNonTransportTable(){const pages=Math.max(1,Math.ceil(nonTransportFiltered.length/NON_TRANSPORT_PAGE_SIZE));nonTransportPage=Math.min(nonTransportPage,pages);const rows=nonTransportFiltered.slice((nonTransportPage-1)*NON_TRANSPORT_PAGE_SIZE,nonTransportPage*NON_TRANSPORT_PAGE_SIZE),body=$('non-transport-table').querySelector('tbody');body.innerHTML=rows.map(row=>{const d=row.non_transport_analysis;return `<tr data-id="${esc(row.trace_id)}" class="${row.trace_id===selectedId?'selected ':''}${row.failed?'trace-failed':''}"><td class="nowrap">${esc(row.timestamp.slice(11,23))}</td><td><code>${esc(short(row.trace_id))}</code></td><td><span class="badge" style="background:${NON_TRANSPORT_COLORS[d.deep_category]}20;color:${NON_TRANSPORT_COLORS[d.deep_category]}">${esc(d.deep_category)}</span></td><td><span class="evidence-level">${esc(d.confidence)}</span><div class="caption">${esc(d.evidence_points.join('；'))}</div></td><td class="worker-name" title="${esc(row.direct_data_worker)}">${esc(shortWorker(row.direct_data_worker))}</td><td>${latencyValue(row.client_ms)}</td><td>${latencyValue(row.worker_process_ms)}</td><td>${latencyValue(row.batch_e2e_ms)}</td><td>${latencyValue(row.attribution_ms['RPC网络'])}</td><td>${latencyValue(row.urma_ms)}</td><td class="conclusion-cell">${esc(d.conclusion)}<div class="caption"><b>下一步：</b>${esc(d.next_action)}</div></td></tr>`}).join('')||'<tr><td colspan="11" class="empty">没有匹配的 Trace</td></tr>';body.querySelectorAll('tr[data-id]').forEach(tr=>tr.onclick=()=>selectNonTransportTrace(tr.dataset.id));$('non-transport-pager').innerHTML=`<button id="non-transport-prev" ${nonTransportPage<=1?'disabled':''}>上一页</button><span>${nonTransportPage}/${pages} · ${nonTransportFiltered.length}条</span><button id="non-transport-next" ${nonTransportPage>=pages?'disabled':''}>下一页</button>`;$('non-transport-prev').onclick=()=>{if(nonTransportPage>1){nonTransportPage--;renderNonTransportTable()}};$('non-transport-next').onclick=()=>{if(nonTransportPage<pages){nonTransportPage++;renderNonTransportTable()}}}
function selectNonTransportTrace(traceId){selectedId=traceId;renderNonTransportTable();renderTable();renderDetail();$('trace-detail-panel').scrollIntoView({behavior:'smooth'})}
function initScrollSpy(){const links=[...document.querySelectorAll('#nav a')];const update=()=>{let active=links[0];for(const link of links){const node=$(link.getAttribute('href').slice(1));if(node&&node.getBoundingClientRect().top<120)active=link}links.forEach(link=>link.classList.toggle('active',link===active))};window.addEventListener('scroll',update,{passive:true});update()}
function renderWorkers(){
  const direct=AGG.direct_data_workers.slice(0,12),directChart=chartAt('direct-worker-chart'),directNames=direct.map(x=>x.worker);
  directChart.setOption({tooltip:{trigger:'axis',axisPointer:{type:'shadow'},formatter:ps=>{const x=direct[ps[0].dataIndex];return `<b>${esc(x.worker)}</b><br>Trace ${x.trace_count}条 / 超时 ${x.failed_count}条<br>Client p90 ${fmt(x.client_p90_ms)}<br>Worker p50 ${fmt(x.worker_p50_ms)}`}},legend:{top:0},grid:{left:42,right:48,top:42,bottom:82},xAxis:{type:'category',data:directNames,axisLabel:{rotate:40,fontSize:10,formatter:shortWorker}},yAxis:[{type:'value',name:'trace',minInterval:1},{type:'value',name:'ms'}],series:[{name:'Trace数',type:'bar',data:direct.map(x=>x.trace_count),barMaxWidth:28,itemStyle:{color:'#2f6fed'}},{name:'超时数',type:'bar',data:direct.map(x=>x.failed_count),barMaxWidth:28,itemStyle:{color:'#dc3545'}},{name:'Client p90',type:'line',yAxisIndex:1,data:direct.map(x=>x.client_p90_ms),itemStyle:{color:'#f59e0b'}}]});
  directChart.on('click',p=>{const worker=directNames[p.dataIndex];if(worker){$('direct-worker-filter').value=worker;applyFilters();$('traces').scrollIntoView({behavior:'smooth'})}});

  const sources=AGG.urma_source_workers.slice(0,12),sourceChart=chartAt('urma-source-chart'),sourceNames=sources.map(x=>x.worker);
  sourceChart.setOption({tooltip:{trigger:'axis',axisPointer:{type:'shadow'},formatter:ps=>{const x=sources[ps[0].dataIndex];return `<b>${esc(x.worker)}</b><br>Trace ${x.trace_count}条<br>URMA p50 ${fmt(x.urma_p50_ms)}<br>URMA p90 ${fmt(x.urma_p90_ms)}<br>Max ${fmt(x.urma_max_ms)}`}},legend:{top:0},grid:{left:42,right:48,top:42,bottom:82},xAxis:{type:'category',data:sourceNames,axisLabel:{rotate:40,fontSize:10,formatter:shortWorker}},yAxis:[{type:'value',name:'trace',minInterval:1},{type:'value',name:'ms'}],series:[{name:'Trace数',type:'bar',data:sources.map(x=>x.trace_count),barMaxWidth:30,itemStyle:{color:'#17a2b8'}},{name:'URMA p90',type:'line',yAxisIndex:1,data:sources.map(x=>x.urma_p90_ms),itemStyle:{color:'#f59e0b'}}]});
  sourceChart.on('click',p=>{const worker=sourceNames[p.dataIndex];if(worker){$('urma-source-filter').value=worker;applyFilters();$('traces').scrollIntoView({behavior:'smooth'})}});
  renderWorkerTables();
}
function renderWorkerPager(kind,total){const pages=Math.max(1,Math.ceil(total/WORKER_PAGE_SIZE)),current=Math.min(workerPages[kind],pages);workerPages[kind]=current;const id=kind==='direct'?'direct-worker':'urma-source',label=kind==='direct'?'Data Worker证据':'URMA 源 Data Worker';$(`${id}-pager`).innerHTML=`<button id="${id}-prev" ${current<=1?'disabled':''}>上一页</button><span>${current}/${pages} · ${total}条 ${label}</span><button id="${id}-next" ${current>=pages?'disabled':''}>下一页</button>`;$(`${id}-prev`).onclick=()=>changeWorkerPage(kind,-1);$(`${id}-next`).onclick=()=>changeWorkerPage(kind,1)}
function renderWorkerTables(){
  const directStart=(workerPages.direct-1)*WORKER_PAGE_SIZE,directRows=sortRows('direct-worker-table',AGG.direct_data_workers).slice(directStart,directStart+WORKER_PAGE_SIZE),directBody=$('direct-worker-table').querySelector('tbody');
  directBody.innerHTML=directRows.map(x=>`<tr data-worker="${esc(x.worker)}"><td class="worker-name" title="${esc(x.worker)}">${esc(x.worker)}</td><td>${x.trace_count}</td><td>${x.failed_count}</td><td>${latencyValue(x.client_p90_ms)}</td><td>${latencyValue(x.worker_p50_ms)}</td></tr>`).join('');
  directBody.querySelectorAll('tr[data-worker]').forEach(tr=>tr.onclick=()=>{$('direct-worker-filter').value=tr.dataset.worker;applyFilters()});
  renderWorkerPager('direct',AGG.direct_data_workers.length);
  const urmaStart=(workerPages.urma-1)*WORKER_PAGE_SIZE,urmaRows=sortRows('urma-source-table',AGG.urma_source_workers).slice(urmaStart,urmaStart+WORKER_PAGE_SIZE),urmaBody=$('urma-source-table').querySelector('tbody');
  urmaBody.innerHTML=urmaRows.map(x=>`<tr data-worker="${esc(x.worker)}"><td class="worker-name" title="${esc(x.worker)}">${esc(x.worker)}</td><td>${x.trace_count}</td><td>${latencyValue(x.urma_p50_ms)}</td><td>${latencyValue(x.urma_p90_ms)}</td><td>${latencyValue(x.urma_max_ms)}</td></tr>`).join('');
  urmaBody.querySelectorAll('tr[data-worker]').forEach(tr=>tr.onclick=()=>{$('urma-source-filter').value=tr.dataset.worker;applyFilters()});
  renderWorkerPager('urma',AGG.urma_source_workers.length);
  updateSortableHeaders('direct-worker-table');updateSortableHeaders('urma-source-table');
}
function changeWorkerPage(kind,delta){const total=kind==='direct'?AGG.direct_data_workers.length:AGG.urma_source_workers.length,pages=Math.max(1,Math.ceil(total/WORKER_PAGE_SIZE));workerPages[kind]=Math.min(pages,Math.max(1,workerPages[kind]+delta));renderWorkerTables()}
function renderTimeline(){
  const timelineRows=scopeRows(),chart=chartAt('timeline-chart'),labels=timelineRows.map((r,i)=>`${String(i+1).padStart(3,'0')} ${r.timestamp.slice(11,19)}`),stages=Object.keys(STAGE_COLORS);
  const series=stages.map((name,i)=>({name,type:'bar',stack:'latency',barMaxWidth:11,data:timelineRows.map(r=>r.focus_breakdown_ms[name]),itemStyle:{color:STAGE_COLORS[name]},emphasis:{focus:'series'},markLine:i===0?{silent:true,symbol:'none',lineStyle:{color:'#dc3545',type:'dashed',width:1.5},label:{formatter:'20ms deadline',color:'#dc3545'},data:[{yAxis:20}]}:undefined}));
  series.push({name:'其他失败',type:'scatter',symbol:'circle',symbolSize:7,itemStyle:{color:'#667085'},data:timelineRows.map((r,i)=>r.failed&&!r.urma_timeout_observed?[i,r.client_ms+0.4]:null).filter(Boolean)});
  series.push({name:'URMA超时标记',type:'scatter',symbol:'diamond',symbolSize:12,itemStyle:{color:'#b42318'},data:timelineRows.map((r,i)=>r.urma_timeout_observed?[i,r.client_ms+0.4]:null).filter(Boolean)});
  chart.setOption({animation:false,color:Object.values(STAGE_COLORS),legend:{top:0,data:[...stages,'其他失败','URMA超时标记']},grid:{left:48,right:20,top:48,bottom:72},tooltip:{trigger:'axis',axisPointer:{type:'shadow'},formatter:params=>{const idx=params[0]?.dataIndex??0,r=timelineRows[idx];return `<b>${esc(r.trace_id)}</b><br>${esc(r.timestamp)} · Client ${fmt(r.client_ms)}<br>${stages.map(n=>`${n}: ${fmt(r.focus_breakdown_ms[n])}`).join('<br>')}<br>主问题：${esc(r.focus_primary_problem)} · ${r.failed?'失败':'成功'}${r.error_subcategory?`<br>错误细分：${esc(r.error_subcategory)}<br>上浮链：${esc(r.error_chain_category)}`:''}`}},xAxis:{type:'category',data:labels,axisLabel:{interval:9,rotate:35,fontSize:10}},yAxis:{type:'value',name:'归因耗时 (ms)',splitLine:{lineStyle:{color:'#e6ebf2'}}},dataZoom:[{type:'inside',start:0,end:100},{type:'slider',height:18,bottom:10,start:0,end:100}],series});
  chart.off('click');chart.on('click',p=>{if(Number.isInteger(p.dataIndex)){selectedId=timelineRows[p.dataIndex].trace_id;renderTable();renderDetail();document.getElementById('traces').scrollIntoView({behavior:'smooth'})}})
}
function categoryBadge(r){const color=PROBLEM_COLORS[r.focus_primary_problem]||'#667085';return `<span class="badge" style="background:${color}20;color:${color}">${esc(r.focus_primary_problem)}</span>`}
function applyFilters(){const cat=$('category-filter').value,status=$('status-filter').value,location=$('access-location-filter').value,direct=$('direct-worker-filter').value,source=$('urma-source-filter').value,q=$('trace-search').value.trim().toLowerCase();filtered=scopeRows().filter(r=>(!cat||r.focus_primary_problem===cat)&&(!status||(status==='failed')===r.failed)&&(!location||r.access_location===location)&&(!direct||r.direct_data_worker===direct)&&(!source||r.urma_source_workers.includes(source))&&(!q||r.trace_id.toLowerCase().includes(q)||String(r.failure_reason||'').toLowerCase().includes(q)||String(r.data_access_scope||'').toLowerCase().includes(q)||String(r.error_subcategory||'').toLowerCase().includes(q)||String(r.error_chain_category||'').toLowerCase().includes(q)||r.evidence.some(x=>x.toLowerCase().includes(q))));page=1;if(!filtered.some(r=>r.trace_id===selectedId))selectedId=filtered[0]?.trace_id||null;renderTable();renderDetail()}
function renderTable(){const body=$('trace-table').querySelector('tbody'),pages=Math.max(1,Math.ceil(filtered.length/PAGE_SIZE));page=Math.min(page,pages);const slice=filtered.slice((page-1)*PAGE_SIZE,page*PAGE_SIZE),stages=Object.keys(STAGE_COLORS);body.innerHTML=slice.map(r=>`<tr data-id="${esc(r.trace_id)}" class="${r.trace_id===selectedId?'selected ':''}${r.failed?'trace-failed':''}"><td>${esc(r.timestamp.slice(11,23))}</td><td><code>${esc(short(r.trace_id))}</code></td><td>${categoryBadge(r)}</td><td>${esc(r.failure_reason||r.error_subcategory||'—')}</td><td>${badge(r.failed?'20ms超时':'成功',r.failed?'b-fail':'b-ok')}</td><td title="${esc(r.access_location_evidence)}">${esc(r.access_location)}</td><td title="${esc(r.data_access_evidence||'')}">${esc(r.data_access_scope||'证据不足')}</td><td>${latencyValue(r.client_ms)}</td>${stages.map(name=>`<td>${latencyValue(r.focus_breakdown_ms[name])}</td>`).join('')}</tr>`).join('')||`<tr><td colspan="17" class="empty">没有匹配的 Trace</td></tr>`;body.querySelectorAll('tr[data-id]').forEach(tr=>tr.onclick=()=>{selectedId=tr.dataset.id;renderTable();renderDetail()});$('page-label').textContent=`${page}/${pages} · ${filtered.length}条`;$('prev-page').disabled=page<=1;$('next-page').disabled=page>=pages}
function evidenceMs(value){const number=Number(value);return value===null||value===undefined||!Number.isFinite(number)?'未观测':`${number.toFixed(6)} ms`}
function traceText(rows,title){
  const dropped=rows.reduce((sum,row)=>sum+(row.dropped_evidence||0),0),header=[`# DataSystem Trace Evidence Export`,`# 范围: ${title}`,`# Trace数量: ${rows.length}`,`# 证据范围: ds-trace-triage 保留行；另有 ${dropped} 行被预处理截断，不是原始日志全量。`,`# 口径: URMA建链、URMA通信、URMA调度/线程开销、QueryAndGet/Get其他业务、其他调度/线程开销、RPC网络、RPC框架是互斥阶段；证据不闭合的部分保留为未解释残差。`,`# 生成自 ds-trace-triage 后置关键瓶颈分析页。`];
  const blocks=rows.map((row,index)=>{const d=row.non_transport_analysis,stages=Object.entries(row.focus_breakdown_ms).map(([name,value])=>`${name}: ${Number(value).toFixed(6)} ms`).join('\n'),deep=d?[`精细分类: ${d.deep_category}`,`证据强度: ${d.confidence}`,`结论: ${d.conclusion}`,`下一步: ${d.next_action}`,`证据摘要: ${d.evidence_points.join('；')}`]:[];return [`===== TRACE ${index+1}/${rows.length} =====`,`Trace ID: ${row.trace_id}`,`时间: ${row.timestamp} ~ ${row.last_ts}`,`状态: ${row.failed?'失败':'成功'} (${row.status})`,`主问题: ${row.focus_primary_problem}`,`失败原因: ${row.failure_reason||row.error_subcategory||'无'}`,`错误上浮链: ${row.error_chain_category||'无'}`,`根因边界: ${row.error_root_cause_boundary||'无'}`,`交付方式: ${row.access_location} (${row.access_location_evidence})`,`定位侧/窗口: ${row.data_access_scope} (${row.data_access_evidence})`,`Data Worker证据: ${row.direct_data_worker}`,`URMA 源 Data Worker: ${row.urma_source_workers.join(', ')||'未明确'}`,`Client总时延: ${evidenceMs(row.client_ms)}`,`Client/Worker数据父窗口: ${evidenceMs(row.worker_process_ms)}`,`Data RPC e2e/network/server: ${evidenceMs(row.data_rpc_e2e_ms)} / ${evidenceMs(row.data_rpc_network_ms)} / ${evidenceMs(row.data_rpc_server_ms)}`,`BatchGet e2e: ${evidenceMs(row.batch_e2e_ms)}`,`BatchGet network residual: ${evidenceMs(row.batch_network_ms)}`,`BatchGet server_exec: ${evidenceMs(row.batch_server_ms)}`,`URMA关键路径: ${evidenceMs(row.urma_critical_path_ms)}`,`证据保留: ${row.evidence.length}行；截断: ${row.dropped_evidence}行`,...deep,`--- 互斥阶段 ---`,stages,`--- triage保留证据 ---`,...row.evidence].join('\n')}).join('\n\n');return `${header.join('\n')}\n\n${blocks}\n`}
function safeFilename(value){return String(value||'traces').replace(/[\\/:*?"<>|\s]+/g,'-').replace(/-+/g,'-').replace(/^-|-$/g,'').slice(0,100)||'traces'}
function downloadTraceSet(rows,filename,title){if(!rows.length)return;const blob=new Blob(['\ufeff',traceText(rows,title)],{type:'text/plain;charset=utf-8'}),url=URL.createObjectURL(blob),link=document.createElement('a');link.href=url;link.download=`${safeFilename(filename)}.txt`;document.body.appendChild(link);link.click();link.remove();setTimeout(()=>URL.revokeObjectURL(url),1000)}
function classifyEvidence(text){if(/URMA|UDMA|\bUB\b/i.test(text))return'urma';if(/BatchGetObjectRemote|RemotePull|WorkerWorkerOCService|remote[_ ]?get/i.test(text))return'remote';if(/DS_KV_CLIENT_GET|ds_client|ClientWorkerRemoteApi/i.test(text))return'client';if(/DS_POSIX_GET|worker_oc_service_get_impl|\[Get\]/i.test(text))return'direct';return'other'}
function toLogLatencyMs(key,raw,unit='',kind='field'){const value=Number(raw),u=String(unit).toLowerCase();if(!Number.isFinite(value))return null;if(u.includes('ms'))return value;if(u.includes('us')||kind==='access'||/_us$|costUs/i.test(key)||/^(?:client|worker)\./i.test(key))return value/1000;return value}
function extractLogLatencyTokens(text){const source=String(text),values=[];for(const match of source.matchAll(/\|\s*(?:DS_KV_CLIENT_GET|DS_POSIX_GET)\s*\|\s*(\d+)\s*\|/g)){const ms=toLogLatencyMs('access',match[1],'us','access');if(ms!==null)values.push(ms)}for(const match of source.matchAll(/\b(totalCost|costUs|cost|ProcessGetObjectRequest|QueryMeta|[A-Za-z][A-Za-z0-9_]*_us|(?:client|worker)\.[A-Za-z0-9_.]+)\s*[:=]?\s*(\d+(?:\.\d+)?)\s*(ms|us)?/gi)){const ms=toLogLatencyMs(match[1],match[2],match[3]||'');if(ms!==null)values.push(ms)}return values}
function problemLatencyToken(display,ms,kind){const cls=latencyClass(ms);return cls?`<span class="log-token problem-latency ${cls}" data-latency-ms="${Number(ms).toFixed(3)}" data-latency-kind="${kind}" title="异常耗时 ${Number(ms).toFixed(3)}ms">${display}</span>`:display}
function highlightLogLine(text){let html=esc(text);html=html.replace(/(\|\s*(?:DS_KV_CLIENT_GET|DS_POSIX_GET)\s*\|\s*)(\d+)(\s*\|)/g,(all,prefix,raw,suffix)=>`${prefix}${problemLatencyToken(raw,toLogLatencyMs('access',raw,'us','access'),'access')}${suffix}`);html=html.replace(/\b(totalCost|costUs|cost|ProcessGetObjectRequest|QueryMeta|[A-Za-z][A-Za-z0-9_]*_us|(?:client|worker)\.[A-Za-z0-9_.]+)(\s*[:=]?\s*)(\d+(?:\.\d+)?)(\s*(?:ms|us))?/gi,(all,key,sep,raw,unit='')=>{const kind=/^(?:client|worker)\./i.test(key)?'summary':/_us$/i.test(key)?'rpc':'stage',ms=toLogLatencyMs(key,raw,unit,kind);return `${key}${sep}${problemLatencyToken(`${raw}${unit}`,ms,kind)}`});return html.replace(/(\|\s*[EF]\s*\||\bERROR\b|\bFATAL\b|status[:=]?\s*1001|RPC failed)/gi,'<span class="log-keyword log-tag-error">$1</span>').replace(/(deadline exceeded|RPC timed out|\btimeout\b|20ms deadline)/gi,'<span class="log-keyword log-tag-deadline">$1</span>').replace(/(\[?URMA_ELAPSED_(?:TOTAL|POLL_JFC|NOTIFY|THREAD_SHED)\]?|URMA(?:[_ -]WAIT[_ -]TIMEOUT))/gi,'<span class="log-keyword log-tag-urma">$1</span>').replace(/(\[?(?:(?:ZMQ|BRPC)_)?RPC_FRAMEWORK_SLOW\]?)/gi,'<span class="log-keyword log-tag-rpc">$1</span>').replace(/(latencySummary)/gi,'<span class="log-keyword log-tag-latency">$1</span>').replace(/(BatchGetObjectRemote|RemotePull|DS_KV_CLIENT_GET|DS_POSIX_GET)/gi,'<span class="log-keyword log-tag-rpc">$1</span>').replace(/(urma_request_id|urma_inflight_wr_count|srcChipInflight|inflightRemoteGet|trace_us|dataSize|cpuid|src address|target address)/gi,'<span class="log-keyword log-tag-urma">$1</span>')}
function logGroupSummary(lines){const joined=lines.join('\n'),values=lines.flatMap(extractLogLatencyTokens),max=values.length?Math.max(...values):null,direction=joined.match(/src address\s*:?\s*([^,\s]+).*?target address\s*:?\s*([^,\s]+)/i);const parts=[];if(max!==null)parts.push(`最大可解析耗时 ${fmt(max)}`);if(direction)parts.push(`方向 ${direction[1]} → ${direction[2]}`);if(/deadline|timeout/i.test(joined))parts.push('包含 deadline/timeout');return parts.join(' · ')||'保留原始证据，当前无可解析耗时字段'}
function rankLogLine(text){if(/\|\s*[EF]\s*\||ERROR|FATAL|deadline exceeded|RPC timed out|URMA(?:[_ -]WAIT[_ -]TIMEOUT)|Timed out waiting for urma_request_id/i.test(text))return 0;if(/URMA_ELAPSED_TOTAL/i.test(text))return 1;if(/urma_inflight_wr_count|srcChipInflight|inflightRemoteGet|trace_us/i.test(text))return 2;if(/RPC_FRAMEWORK_SLOW|BatchGetObjectRemote|RemotePull/i.test(text))return 3;if(/DS_KV_CLIENT_GET|DS_POSIX_GET|latencySummary/i.test(text))return 4;if(/src address|target address|dataSize|cpuid/i.test(text))return 5;return 9}
function renderTraceLogGroups(evidence){const labels={client:'Client',direct:'Data Worker处理证据',remote:'远端取数 / RPC',urma:'URMA / UB',other:'其他证据'},groups={client:[],direct:[],remote:[],urma:[],other:[]};(evidence||[]).forEach(text=>groups[classifyEvidence(text)].push(text));const html=Object.entries(groups).filter(([,lines])=>lines.length).map(([kind,lines])=>{const keyLines=lines.map((text,index)=>({text,index,rank:rankLogLine(text)})).sort((a,b)=>a.rank-b.rank||a.index-b.index).slice(0,8).map(item=>item.text),allLines=lines.map(text=>`<span class="log-line">${highlightLogLine(text)}</span>`).join(''),disclosure=lines.length>8?`<details class="log-all-lines"><summary>展开全部 ${lines.length} 行原始日志</summary><pre>${allLines}</pre></details>`:'';return `<section class="trace-log-group trace-log-${kind}"><h4><span>${labels[kind]}</span><span>${lines.length} 行 · 默认 ${keyLines.length} 行重点</span></h4><div class="trace-log-summary">${esc(logGroupSummary(lines))}</div><pre class="log-key-lines">${keyLines.map(text=>`<span class="log-line">${highlightLogLine(text)}</span>`).join('')}</pre>${disclosure}</section>`}).join('');return html||'<div class="empty">没有原始日志证据</div>'}
function renderErrorNote(r){
  if(!r.error_family)return '';
  if(!r.urma_timeout_observed)return `<div class="notice"><b>${esc(r.error_family)}：</b>${esc(r.error_subcategory)}；${esc(r.error_chain_category)}。<br><b>已确认故障点：</b>${esc(r.error_failure_point)}；<b>根因边界：</b>${esc(r.error_root_cause_boundary)}。</div>`;
  const breakdown=r.query_urma_timeout_ms==null?'超时未与 QueryAndGet 父窗口唯一闭合，保留为错误标记，不挪动阶段。':`同 Worker QueryAndGet 父窗口内唯一匹配；从 QueryMeta/QueryAndGet 中剥离 ${fmt(r.query_urma_timeout_ms)} 的等待窗口。`;
  return `<div class="notice"><b>URMA超时细分：</b>${esc(r.error_subcategory)}；${esc(r.error_chain_category)}。<br><b>已确认故障点：</b>${esc(r.error_failure_point)}；<b>恢复动作：</b>${esc(r.error_recovery_action)}。<br><b>根因边界：</b>${esc(r.error_root_cause_boundary)}。<br><b>口径：</b>已观测到 URMA_WAIT_TIMEOUT；失败 WR 没有完成态时不伪造 URMA 耗时。<br><b>Breakdown口径：</b>${breakdown}</div>`;
}
function renderMetricGrid(raw){return raw.map(([name,value])=>`<div class="metric"><span>${name}</span><b>${value===null||value===undefined?'—':latencyValue(value)}</b></div>`).join('')}
function renderAttributionStages(r,max,topStage){return Object.entries(r.focus_breakdown_ms).map(([name,value])=>{const isTop=name===topStage,color=isTop?'#dc3545':STAGE_COLORS[name],weight=isTop?'700':'400',label=isTop?' · TOP 瓶颈':'';return `<div class="phase-row"><span style="color:${isTop?'#dc3545':'inherit'};font-weight:${weight}">${name}${label}</span><div class="phase-track"><div class="phase-fill" style="width:${Math.min(100,value/max*100)}%;background:${color}"></div></div><b>${latencyValue(value)}</b></div>`}).join('')}
function renderDetail(){
  const r=ROWS.find(item=>item.trace_id===selectedId);
  if(!r){$('trace-detail').innerHTML='<div class="empty">请选择 Trace</div>';$('trace-log-groups').innerHTML='';return}
  const max=Math.max(r.client_ms,1),topStage=r.focus_primary_stage;
  const raw=[['总时延',r.client_ms],['Client/Worker数据父窗口',r.worker_process_ms],['Client RPC e2e',r.rpc_observed?r.client_rpc_e2e_ms:null],['Client RPC 网络',r.rpc_observed?r.client_rpc_network_ms:null],['Data RPC e2e',r.data_rpc_observed?r.data_rpc_e2e_ms:null],['Data RPC 网络',r.data_rpc_observed?r.data_rpc_network_ms:null],['Data RPC server',r.data_rpc_observed?r.data_rpc_server_ms:null],['QueryMeta/QueryAndGet原始父窗口',r.query_meta_ms],['URMA超时等待窗口',r.query_urma_timeout_ms],['URMA关键路径',r.urma_critical_path_ms],['URMA timeout最大elapsedMs',r.urma_timeout_max_ms]];
  const status=badge(r.failed?'20ms超时':'成功',r.failed?'b-fail':'b-ok'),errorNote=renderErrorNote(r),metricGrid=renderMetricGrid(raw),stages=renderAttributionStages(r,max,topStage);
  $('trace-detail').innerHTML=`<div><code>${esc(r.trace_id)}</code></div><p>${categoryBadge(r)} ${status}</p>${errorNote}<div class="notice"><b>定位侧/窗口：</b>${esc(r.data_access_scope||'证据不足')}。${esc(r.data_access_evidence||'')}<br><b>失败原因：</b>${esc(r.failure_reason||r.error_subcategory||'无失败')}。</div><p><b>交付方式：</b>${esc(r.access_location)} <span class="caption">${esc(r.access_location_evidence)}</span><br><b>Data Worker证据：</b>${esc(r.direct_data_worker)}<br><b>URMA 源 Data Worker：</b>${esc(r.urma_source_workers.join(', ')||'未明确')}</p><div class="metric-grid">${metricGrid}</div>${renderUrmaRequestSummary(r)}<h3 style="margin-top:16px">互斥归因 Stages</h3>${stages}<div class="caption">红色条为该 Trace 最大互斥阶段。RPC框架已扣除 handler、网络残差和明确的其他调度/排队；URMA通信已扣除明确的 URMA 调度/线程开销。证据不闭合时保留未解释残差，不强行归因。</div>`;
  const truncation=r.dropped_evidence?`<div class="notice"><b>证据已截断：</b>ds-trace-triage 另有 ${r.dropped_evidence} 行未保留；本页和下载均不是原始日志全量。</div>`:'';
  $('trace-log-groups').innerHTML=truncation+renderTraceLogGroups(r.evidence);
}
function init(){renderKpis();renderProblemOverview();renderErrorAnalysis();renderStageShare();renderProblemGuidance();renderTimeFindings();renderUrmaAnalysis();renderNonTransportAnalysis();renderWorkers();renderTimeline();Object.keys(FOCUS_PROBLEMS).forEach(cat=>$('category-filter').insertAdjacentHTML('beforeend',`<option value="${esc(cat)}">${esc(cat)}</option>`));Object.keys(AGG.access_locations).forEach(location=>$('access-location-filter').insertAdjacentHTML('beforeend',`<option value="${esc(location)}">${esc(location)} · ${AGG.access_locations[location]}条</option>`));AGG.direct_data_workers.forEach(x=>$('direct-worker-filter').insertAdjacentHTML('beforeend',`<option value="${esc(x.worker)}">${esc(x.worker)}</option>`));AGG.urma_source_workers.forEach(x=>$('urma-source-filter').insertAdjacentHTML('beforeend',`<option value="${esc(x.worker)}">${esc(x.worker)}</option>`));['category-filter','status-filter','access-location-filter','direct-worker-filter','urma-source-filter'].forEach(id=>$(id).onchange=applyFilters);$('trace-search').oninput=applyFilters;$('reset-filter').onclick=()=>{['category-filter','status-filter','access-location-filter','direct-worker-filter','urma-source-filter'].forEach(id=>$(id).value='');$('trace-search').value='';applyFilters()};$('prev-page').onclick=()=>{if(page>1){page--;renderTable()}};$('next-page').onclick=()=>{if(page*PAGE_SIZE<filtered.length){page++;renderTable()}};$('download-all-traces').onclick=()=>downloadTraceSet(ROWS,'same-3x105qps-8mb-top100-all-100','Top100 全量 Trace');$('download-filtered-traces').onclick=()=>downloadTraceSet(filtered,`same-3x105qps-8mb-filtered-${filtered.length}`,`Top100 当前筛选 ${filtered.length} 条`);$('download-non-transport-category').onclick=()=>{const category=$('non-transport-category-filter').value||'非RPC非UB全部';downloadTraceSet(nonTransportFiltered,`same-3x105qps-8mb-${category}-${nonTransportFiltered.length}`,`${category} ${nonTransportFiltered.length} 条`)};$('download-selected-trace').onclick=()=>{const row=ROWS.find(item=>item.trace_id===selectedId);if(row)downloadTraceSet([row],`trace-${row.trace_id}`,`单条 Trace ${row.trace_id}`)};renderTable();renderDetail();initScrollSpy();window.addEventListener('resize',()=>charts.forEach(chart=>chart.resize()))}init();
const renderDetailBase=renderDetail;
renderDetail=function(){renderDetailBase();const r=ROWS.find(x=>x.trace_id===selectedId);if(!r||r.query_and_get_parent_ms==null)return;const node=$('trace-detail');const queryMetric=[...node.querySelectorAll('.metric span')].find(x=>x.textContent==='QueryMeta');if(queryMetric)queryMetric.textContent='QueryAndGet父窗口（原始）';node.insertAdjacentHTML('afterbegin',`<div class="notice"><b>PR2165 inline 互斥归因：</b>QueryAndGet 父窗口 ${latencyValue(r.query_and_get_parent_ms)}；独占 ${latencyValue(r.query_and_get_exclusive_ms)}；已剝离 inline URMA ${latencyValue(r.inline_query_urma_ms)}（${esc(r.inline_query_urma_basis||'未观测')}）。同 Worker、同 attempt 唯一匹配；WR 分片不求和。</div>`)};
renderDetail();
</script>
</body></html>'''


def _render_dashboard_html(
    rows: list[dict],
    aggregate_data: dict,
    title: str,
    metadata: dict,
    write_rows: list[dict],
    write_aggregate: dict,
) -> str:
    rows_json = json.dumps(rows, ensure_ascii=False, separators=(",", ":")).replace("<", "\\u003c")
    aggregate_json = json.dumps(aggregate_data, ensure_ascii=False, separators=(",", ":")).replace("<", "\\u003c")
    write_rows_json = json.dumps(write_rows, ensure_ascii=False, separators=(",", ":")).replace("<", "\\u003c")
    write_aggregate_json = json.dumps(write_aggregate, ensure_ascii=False, separators=(",", ":")).replace(
        "<", "\\u003c"
    )
    echarts_path = (
        Path(__file__).resolve().parent.parent
        / ".skills"
        / "ds-trace-triage"
        / "assets"
        / "echarts-5.5.1.min.js"
    )
    if not echarts_path.is_file():
        echarts_path = Path(__file__).resolve().parent.parent / "assets" / "echarts-5.5.1.min.js"
    echarts_source = echarts_path.read_text(encoding="utf-8")
    safe_title = html.escape(title)
    code_ref = html.escape(str(metadata.get("code_ref") or "未记录"))
    current_source_ref = html.escape(str(metadata.get("current_source_ref") or "未提供"))
    case = html.escape(str(metadata.get("case") or "未命名"))
    scenario = html.escape(str(metadata.get("scenario") or "未记录"))
    deadline = float(aggregate_data.get("deadline_ms", 20.0))
    deadline_label = "参考阈值" if aggregate_data.get("deadline_is_reference") else "deadline"
    non_transport_count = int(aggregate_data.get("non_transport_analysis", {}).get("trace_count", 0))
    topology = aggregate_data.get("topology") or _topology_contract(metadata.get("local_cache"))
    topology_kind = topology["kind"]
    topology_label = html.escape(str(topology["label"]))
    topology_path = html.escape(str(topology["path"]))
    raw_archives = metadata.get("raw_input_archives") or []
    raw_archive_html = ""
    if raw_archives:
        archive_items = "".join(
            '<li><a href="{path}" download>{name}</a> · {size} bytes · SHA256 <code>{sha}</code></li>'.format(
                path=html.escape(str(item.get("download_path") or ""), quote=True),
                name=html.escape(str(item.get("name") or "")),
                size=int(item.get("size_bytes", 0) or 0),
                sha=html.escape(str(item.get("sha256") or "未记录")),
            )
            for item in raw_archives
        )
        raw_archive_html = (
            '<section class="panel" id="raw-archives"><h2>下载原始 Trace 数据包</h2>'
            '<div class="notice"><b>归档合同：</b>以下文件是 ds-trace-triage 保留的输入原包副本；'
            '专项分享目录再次复制原文件，不从截断后的 evidence 反向拼包。</div>'
            f'<ul>{archive_items}</ul></section>'
        )
    if topology_kind == "client_direct":
        topology_detail = (
            "<code>Get</code> 进入 Client 侧 <code>GetFromTransportLayer</code>；Client 先向 Meta Owner 查询对象位置，"
            "再通过 TCP/URMA 直接访问 Data Worker。<code>BatchGetObjectRemote</code>、"
            "<code>WorkerWorkerOCService</code> 和 <code>RemotePull</code> 是服务/日志命名，单独出现时不代表 Worker→Worker。"
            "本模式下 RPC 网络是 Client↔Data Worker，URMA 是 Data Worker→Client。"
        )
    elif topology_kind == "legacy_worker_pull":
        topology_detail = (
            "该页面按历史运行日志中的调用方、目标地址、<code>WorkerWorkerOCService</code>、"
            "<code>RemotePull</code> 与 <code>worker-&gt;worker</code> 明确信息解释方向："
            "BatchGet 为 Worker→Data Worker，URMA 为 Data Worker→请求 Worker。"
            "这是采集版本的运行时语义；当前源码可能已经改为 Client 直达数据面。"
        )
    elif topology_kind == "bound_worker":
        topology_detail = (
            "Client 通过绑定 Worker 访问；只有 Trace 同时提供调用方、目标 Data Worker 和远端请求证据时，"
            "才把 BatchGet/RemotePull 判定为 Worker→Worker，不能仅凭服务名推断。"
        )
    else:
        topology_detail = (
            "使用者未提供 <code>local_cache</code> 模式。本页保留中性 BatchGet/Data Worker 口径，"
            "不根据 <code>WorkerWorkerOCService</code>、<code>BatchGetObjectRemote</code> 或 "
            "<code>RemotePull</code> 单独推断 Worker→Worker。"
        )
    source_logic_html = (
        '<section class="panel" id="source-logic"><h2>8. 源码与访问拓扑</h2>'
        f'<div class="notice"><b>{topology_label}：</b><code>{topology_path}</code>。{topology_detail}</div>'
        f'<div class="caption">triage code ref：<code>{code_ref}</code>；当前源码校正 ref：'
        f'<code>{current_source_ref}</code>。源码级结论需对照该 ref 的实际源码；'
        "CodeGraph 仅用于定位。字段缺失保持为观测盲区，不推断网络、CPU、锁或线程调度。</div></section>"
    )
    template = (
        HTML_TEMPLATE.replace("SAME 3x105 QPS · Top100 关键瓶颈", safe_title)
        .replace(
            "100 个唯一 GET trace · triage 数据 ref d897aee1 · 代码逻辑校正 main/master@77fb2d9a",
            f"{len(rows)} 个唯一 Trace · case {case} · scenario {scenario} · triage ref {code_ref}",
        )
        .replace(
            '<section class="panel" id="source-logic"><h2>最新 main/master 代码逻辑校正</h2><div class="notice"><b><code>enableLocalCache=false</code> 读取主链：</b><code>Get</code> 进入 <code>GetFromTransportLayer</code>；<code>BuildTransportReadRequest</code> 先按 hash ring 选择 metadata owner，获得对象位置后，<code>ReplicaReader::ReadReplicaOnce</code> 直接向该 Data Worker 执行读取。因此这不是“Client→入口 Worker→Data Worker”的固定代理链。Trace 中出现不同的 DS_POSIX_GET Worker 与 URMA 日志 Worker 时，页面仅作“直连请求目标”和“URMA 供数端”的证据分组。</div><div class="caption">代码逻辑校正基线：main/master@77fb2d9a46f7ba9b658f4e1f6eba74c22206f9fe；triage 数据记录的 code ref 为 d897aee13b7f20b58a60f81e1b31e094964c996d。CodeGraph 仅用于定位，结论已对照当前实际源码。</div></section>',
            raw_archive_html + source_logic_html,
        )
        .replace("Top100 诊断", "TopN 诊断")
        .replace("2. Top100 时间序列", "2. TopN 时间序列")
        .replace("表 7-1 Top100", "表 8-1 TopN")
        .replace("Top100 中", f"Top{len(rows)} 中")
        .replace("合计 100 条", f"合计 {len(rows)} 条")
        .replace("的 40 条 Trace", f"的 {non_transport_count} 条 Trace")
        .replace(
            '<a class="sub" href="#trace-log-panel">日志框 7-3 Trace 证据日志</a></aside>',
            '<a class="sub" href="#trace-log-panel">日志框 7-3 Trace 证据日志</a>'
            '<a href="#write-analysis">9. 写入瓶颈分析</a></aside>',
        )
        .replace('data-deadline-ms="20"', f'data-deadline-ms="{deadline:g}"')
        .replace("20ms deadline", f"{deadline:g}ms {deadline_label}")
        .replace("20ms超时", "失败")
        .replace("20ms 超时", "失败")
        .replace("远端供数非URMA", "远端供数处理")
        .replace("远端供数非 URMA", "远端供数处理")
        .replace("远端供数端非URMA处理", "远端供数端处理")
        .replace("截止点观测盲区", "Client/Worker观测未闭合")
        .replace("截止点观测空窗", "Client/Worker观测空窗")
        .replace("非 RPC / 非 UB 深挖", "非 RPC 主导深挖")
        .replace("Trace 原始日志", "Trace 证据日志")
        .replace("完整原始行按需展开", "triage 保留的证据行按需展开")
        .replace("展开全部 ${lines.length} 行原始日志", "展开 triage 保留的 ${lines.length} 行证据")
        .replace("下载全量 TopN", "下载 TopN 证据")
        .replace("Top100 全量 Trace", "TopN triage 证据")
        .replace("${latencyValue(row.urma_ms)}", "${row.urma_observed?latencyValue(row.urma_ms):'—'}")
        .replace(
            "${Number(row.data_rpc_e2e_ms).toFixed(6)} / "
            "${Number(row.data_rpc_network_ms).toFixed(6)} / "
            "${Number(row.data_rpc_server_ms).toFixed(6)} ms",
            "${row.data_rpc_e2e_ms===null?'未观测':"
            "Number(row.data_rpc_e2e_ms).toFixed(6)+' ms'} / "
            "${row.data_rpc_network_ms===null?'未观测':"
            "Number(row.data_rpc_network_ms).toFixed(6)+' ms'} / "
            "${row.data_rpc_server_ms===null?'未观测':"
            "Number(row.data_rpc_server_ms).toFixed(6)+' ms'}",
        )
        .replace(
            "`--- 原始日志 (${row.evidence.length}行) ---`,...row.evidence",
            "`证据保留: ${row.evidence.length}行；截断: ${row.dropped_evidence}行`,"
            "`--- triage保留证据 ---`,...row.evidence",
        )
        .replace("yAxis:20", "yAxis:DEADLINE_MS")
        .replace("const PAGE_SIZE=8", f"const DEADLINE_MS={deadline:g};const PAGE_SIZE=8")
        .replace("same-3x105qps-8mb", "datasystem-bottleneck")
        .replace(
            "'datasystem-bottleneck-top100-all-100'",
            "`datasystem-bottleneck-top${ROWS.length}-all-${ROWS.length}`",
        )
        .replace("`Top100 当前筛选 ${filtered.length} 条`", "`Top${ROWS.length} 当前筛选 ${filtered.length} 条`")
        .replace("生成自 SAME 3x105 QPS Top100 离线分析页", "生成自 ds-trace-triage 后置关键瓶颈分析页")
        .replace("<b>口径：</b><code>local cache=false</code> 下没有固定“入口 Worker”层。页面将", "<b>口径：</b>页面不假设固定“入口 Worker”层；将")
        .replace('<a href="#non-transport-analysis">4. 非 RPC 主导深挖</a>', '<a href="#query-meta-analysis">4-A. QueryMeta 根因分析</a><a class="sub" href="#query-meta-detail-chart">图 4-A-1 互斥细类</a><a class="sub" href="#query-meta-time-chart">图 4-A-2 时间</a><a class="sub" href="#query-meta-worker-chart">图 4-A-3 发起节点</a><a class="sub" href="#query-meta-target-chart">图 4-A-4 Meta Owner</a><a href="#worker-correlation">4-B. 同 Worker 时间关联</a><a class="sub" href="#worker-correlation-chart-rpc">图 4-B-1 RPC</a><a class="sub" href="#worker-correlation-chart-ub">图 4-B-2 UB</a><a class="sub" href="#worker-correlation-chart-metadata">图 4-B-3 元数据</a><a class="sub" href="#worker-correlation-chart-data">图 4-B-4 数据访问</a><a class="sub" href="#worker-correlation-table">表 4-B-1 关键事件</a><a href="#non-transport-analysis">5. 非 RPC 主导深挖</a>')
        .replace(
            '<a href="#source-logic">7. 最新代码逻辑</a>',
            '<a href="#raw-archives">7. 原始数据包</a>'
            '<a href="#source-logic">8. 最新代码逻辑</a>',
        )
        .replace('<a class="sub" href="#non-transport-count-chart">图 4-1 精细分类</a><a class="sub" href="#non-transport-time-chart">图 4-2 时间分布</a><a class="sub" href="#non-transport-worker-chart">图 4-3 Worker 分布</a><a class="sub" href="#non-transport-table">表 4-1 逐 Trace 结论</a><a href="#workers">5. Data Worker 分析</a><a href="#source-logic">6. 最新代码逻辑</a><a href="#traces">7. Trace 查看</a><a class="sub" href="#trace-table">表 8-1 TopN</a><a class="sub" href="#trace-detail-panel">表 7-2 Trace 阶段明细</a><a class="sub" href="#trace-log-panel">日志框 7-3 Trace 证据日志</a>', '<a class="sub" href="#non-transport-count-chart">图 5-1 精细分类</a><a class="sub" href="#non-transport-time-chart">图 5-2 时间分布</a><a class="sub" href="#non-transport-worker-chart">图 5-3 Worker 分布</a><a class="sub" href="#non-transport-table">表 5-1 逐 Trace 结论</a><a href="#workers">6. Data Worker 分析</a><a href="#source-logic">7. 最新代码逻辑</a><a href="#traces">8. Trace 查看</a><a class="sub" href="#trace-table">表 8-1 TopN</a><a class="sub" href="#trace-detail-panel">表 8-2 Trace 阶段明细</a><a class="sub" href="#trace-log-panel">日志框 8-3 Trace 证据日志</a>')
        .replace(
            '<section class="panel" id="non-transport-analysis">'
            '<h2>4. 非 RPC 主导深挖</h2>',
            CORRELATION_SECTION
            + '<section class="panel" id="non-transport-analysis">'
            '<h2>5. 非 RPC 主导深挖</h2>',
        )
        .replace(
            '<div class="problem-grid"><div><h3>图 4-1 精细分类</h3>',
            '<div class="problem-grid"><div><h3>图 5-1 精细分类</h3>',
        )
        .replace('<div><h3>图 4-2 时间分布</h3>', '<div><h3>图 5-2 时间分布</h3>')
        .replace(
            '<div class="worker-section"><h3>图 4-3 Worker 分布</h3>',
            '<div class="worker-section"><h3>图 5-3 Worker 分布</h3>',
        )
        .replace(
            '<div class="worker-section"><h3>表 4-1 逐 Trace 结论</h3>',
            '<div class="worker-section"><h3>表 5-1 逐 Trace 结论</h3>',
        )
        .replace(
            '<section class="panel" id="workers"><h2>Data Worker 粒度分析</h2>',
            '<section class="panel" id="workers"><h2>6. Data Worker 粒度分析</h2>',
        )
        .replace(
            '<section class="panel" id="source-logic">'
            '<h2>最新 main/master 代码逻辑校正</h2>',
            '<section class="panel" id="source-logic">'
            '<h2>7. 最新 main/master 代码逻辑校正</h2>',
        )
        .replace(
            '<section class="panel" id="traces"><h2>按分类查看 Trace</h2>',
            '<section class="panel" id="traces"><h2>8. Trace 查看</h2><h3>表 8-1 TopN</h3>',
        )
        .replace(
            '<section class="panel" id="trace-detail-panel"><h2>Trace 阶段明细</h2>',
            '<section class="panel" id="trace-detail-panel"><h2>表 8-2 Trace 阶段明细</h2>',
        )
        .replace(
            '<section class="panel" id="trace-log-panel"><h2>Trace 原始日志</h2>',
            '<section class="panel" id="trace-log-panel"><h2>日志框 8-3 Trace 原始日志</h2>',
        )
        .replace(
            '<button id="non-transport-reset-filter">清空筛选</button>',
            '<button id="non-transport-reset-filter">清空筛选</button>'
            '<button id="download-non-transport-category">下载当前精细分类</button>',
        )
        .replace(
            '<div id="non-transport-summary" class="finding-grid"></div>',
            '<div id="non-transport-conclusions"></div>'
            '<h3 style="margin-top:18px">五类分布与治理方向</h3>'
            '<div id="non-transport-summary" class="finding-grid"></div>',
        )
        .replace(
            '<button id="reset-filter">清空筛选</button>',
            '<button id="reset-filter">清空筛选</button>'
            '<button id="download-filtered-traces">下载当前筛选 Trace</button>'
            '<button id="download-all-traces">下载 TopN 证据</button>',
        )
        .replace(
            '<div id="trace-detail"></div>',
            '<div class="controls"><button id="download-selected-trace">'
            '下载当前单条 Trace</button></div><div id="trace-detail"></div>',
        )
        .replace('</main></div><div id="tooltip"', WRITE_SECTION + '</main></div><div id="tooltip"')
        .replace('const AGG=__AGG__;', 'const AGG=__AGG__;const WRITE_ROWS=__WRITE_ROWS__;const WRITE_AGG=__WRITE_AGG__;')
        .replace(
            '</style>',
            CORRELATION_STYLE
            + '.chart-title{text-align:center}'
            'th.sortable-header{cursor:pointer;user-select:none;white-space:nowrap}'
            'th.sortable-header:hover{color:var(--blue);background:#eef5ff}'
            'th.sortable-header:focus{outline:2px solid var(--blue);outline-offset:-2px}'
            '</style>',
        )
        .replace(
            '</thead><tbody></tbody></table></div></div>'
            '<div class="worker-section"><h3>表 3-3',
            '</thead><tbody></tbody></table></div>'
            '<div id="urma-edge-pager" class="pager"></div></div>'
            '<div class="worker-section"><h3>表 3-3',
        )
        .replace(
            "const selected=urmaFiltered.slice(",
            "const selected=sortRows('urma-trace-table',urmaFiltered).slice(",
        )
        .replace(
            "const rows=nonTransportFiltered.slice(",
            "const rows=sortRows('non-transport-table',nonTransportFiltered).slice(",
        )
        .replace("const slice=filtered.slice(", "const slice=sortRows('trace-table',filtered).slice(")
        .replace('let nonTransportPage=1;', 'let nonTransportPage=1;let correlationWorker="";let correlationPage=1;')
        .replace(
            'function renderNonTransportAnalysis(){',
            CORRELATION_SCRIPT + '\nfunction renderNonTransportAnalysis(){',
        )
        .replace('function init(){', WRITE_SCRIPT + '\nfunction init(){')
        .replace('function init(){renderKpis();', "function init(){AGG.worker_correlation.workers.forEach(item=>$('correlation-worker-filter').insertAdjacentHTML('beforeend',`<option value=\"${esc(item.worker)}\">${esc(shortWorker(item.worker))} · ${item.event_count}事件</option>`));['correlation-worker-filter','correlation-category-filter','correlation-status-filter','correlation-relation-filter','correlation-latency-band-filter'].forEach(id=>$(id).onchange=()=>{correlationPage=1;renderWorkerCorrelation()});['correlation-time-start','correlation-time-end'].forEach(id=>$(id).oninput=()=>{correlationPage=1;renderWorkerCorrelation()});$('correlation-reset-filter').onclick=()=>{['correlation-worker-filter','correlation-category-filter','correlation-relation-filter','correlation-latency-band-filter','correlation-time-start','correlation-time-end'].forEach(id=>$(id).value='');$('correlation-status-filter').value='problem';correlationPage=1;renderWorkerCorrelation()};renderKpis();")
        .replace(
            'renderKpis();renderProblemOverview();',
            "renderKpis();document.querySelectorAll('h2,h3').forEach(title=>{"
            "if(/^图\\s*\\d/.test(title.textContent.trim()))"
            "title.classList.add('chart-title')});renderTimeSegments();renderProblemOverview();",
        )
        .replace(
            '<a href="#timeline">2. TopN 时间序列</a>',
            '<a class="sub" href="#time-segments">图 1-5 Client总时延五档</a>'
            '<a href="#timeline">2. TopN 时间序列</a>',
        )
        .replace(
            'renderUrmaAnalysis();renderNonTransportAnalysis();',
            'renderUrmaAnalysis();renderQueryMetaAnalysis();renderWorkerCorrelation();renderWriteAnalysis();'
            'renderNonTransportAnalysis();',
        )
        .replace('renderTable();renderDetail();initScrollSpy();', "renderTable();renderDetail();bindSortableHeaders('trace-table',()=>{page=1},renderTable);bindSortableHeaders('non-transport-table',()=>{nonTransportPage=1},renderNonTransportTable);bindSortableHeaders('urma-trace-table',()=>{urmaPage=1},renderUrmaTraceTable);bindSortableHeaders('urma-time-table',()=>{urmaTimePage=1},renderUrmaTimeTable);bindSortableHeaders('urma-edge-table',()=>{urmaEdgePage=1},renderUrmaEdgeTable);bindSortableHeaders('direct-worker-table',()=>{workerPages.direct=1},renderWorkerTables);bindSortableHeaders('urma-source-table',()=>{workerPages.urma=1},renderWorkerTables);bindSortableHeaders('worker-correlation-table',()=>{correlationPage=1},renderWorkerCorrelationTable);initScrollSpy();")
    )
    if topology_kind == "client_direct":
        template = template.replace(
            "需要结合源→目标边继续定位", "需要结合 Data Worker→Client 请求继续定位"
        ).replace("源 Worker 与源→目标边", "Data Worker→Client URMA").replace(
            "源→目标", "Data Worker→接收端"
        )
    elif topology_kind == "unknown":
        template = template.replace("源 Worker 与源→目标边", "URMA 发送端与接收端（拓扑未确认）").replace(
            "源→目标", "发送端→接收端"
        )
    injections = {
        "__ROWS__": rows_json,
        "__AGG__": aggregate_json,
        "__WRITE_ROWS__": write_rows_json,
        "__WRITE_AGG__": write_aggregate_json,
        "__ECHARTS_SOURCE__": echarts_source,
    }
    return re.sub(
        r"__(?:ROWS|AGG|WRITE_ROWS|WRITE_AGG|ECHARTS_SOURCE)__",
        lambda match: injections[match.group(0)],
        template,
    )


def _read_json(path: Path) -> dict:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise InputContractError(f"cannot read valid JSON from {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise InputContractError(f"expected JSON object in {path}")
    return value


def build_analysis(
    run_dir: Path,
    top_n: int = 100,
    deadline_ms: float | None = None,
    local_cache: bool | None = None,
    read_path: str | None = None,
    source_ref: str | None = None,
) -> dict:
    """Build a deterministic report model from one completed triage run directory."""

    run_dir = Path(run_dir)
    required = [run_dir / name for name in ("manifest.json", "summary.json", "triage.json")]
    missing = [path.name for path in required if not path.is_file()]
    if missing:
        required_names = ", ".join(path.name for path in required)
        raise InputContractError(f"run directory requires {required_names}; missing: {', '.join(missing)}")
    if top_n <= 0:
        raise ValueError("top_n must be positive")
    if local_cache is not None and not isinstance(local_cache, bool):
        raise ValueError("local_cache must be true, false, or omitted")
    if read_path not in {None, "legacy-worker-pull"}:
        raise ValueError("read_path must be legacy-worker-pull or omitted")

    manifest = _read_json(run_dir / "manifest.json")
    summary = _read_json(run_dir / "summary.json")
    _read_json(run_dir / "triage.json")
    topology = _topology_contract(local_cache, read_path)
    all_rows = build_trace_rows(summary, local_cache=local_cache, read_path=read_path)
    trace_inputs = summary.get("traces", {})
    get_rows = [row for row in all_rows if row["get_observed"]]
    client_rows = [row for row in get_rows if row["client_observed"]]
    ranked = sorted(client_rows, key=lambda row: (-row["client_ms"], row["timestamp"], row["trace_id"]))
    rows = ranked[:top_n]
    write_candidates = [
        _build_write_row(row, trace_inputs[row["trace_id"]])
        for row in all_rows
        if row["client_observed"] and _is_write_flow(trace_inputs[row["trace_id"]])
    ]
    write_rows = sorted(
        write_candidates,
        key=lambda row: (-row["client_ms"], row["timestamp"], row["trace_id"]),
    )[:top_n]
    write_aggregate = _aggregate_write(write_rows)
    aggregate_data = aggregate(rows)
    for row in rows:
        row.pop("evidence_records", None)
    coverage = {
        "manifest": "present",
        "summary": "present",
        "triage": "present",
        "parsed_traces": "present" if (run_dir / "parsed_traces.json").is_file() else "missing",
        "events": "present" if (run_dir / "events.jsonl").is_file() else "missing",
    }
    limitations = [
        f"{name} is missing; corresponding drilldown uses summary evidence only"
        for name, state in coverage.items()
        if state == "missing"
    ]
    if local_cache is None:
        limitations.append(
            "local_cache mode is unknown; BatchGet and URMA topology remains unconfirmed"
        )
    if read_path == "legacy-worker-pull":
        limitations.append(
            "historical runtime Worker-pull topology is explicitly supplied from trace "
            "evidence; current source may differ"
        )
    if not source_ref:
        limitations.append(
            "current source ref is not supplied; code-path claims require separate main/master verification"
        )
    excluded_non_get = len(all_rows) - len(get_rows)
    excluded_worker_only = len(get_rows) - len(client_rows)
    if excluded_non_get:
        limitations.append(
            f"{excluded_non_get} non-GET traces are excluded from the read model; "
            f"{len(write_rows)} Client write traces are analyzed in the separate write model"
        )
    if excluded_worker_only:
        limitations.append(
            f"{excluded_worker_only} traces lack a Client latency window and are excluded from Client TopN"
        )
    deadline_is_reference = False
    if deadline_ms is None:
        candidate = manifest.get("deadline_ms") or manifest.get("options", {}).get("deadline_ms")
        if candidate is None:
            deadline_ms = 20.0
            deadline_is_reference = True
            limitations.append("deadline is not recorded; 20ms is a visualization reference, not a configured deadline")
        else:
            deadline_ms = float(candidate)
    if not math.isfinite(deadline_ms) or deadline_ms <= 0:
        raise ValueError("deadline_ms must be a positive finite number")
    aggregate_data["deadline_ms"] = deadline_ms
    aggregate_data["deadline_is_reference"] = deadline_is_reference
    aggregate_data["topology"] = topology
    raw_input_archives = []
    for item in manifest.get("inputs", []):
        preserved_name = str(item.get("preserved_name") or "")
        name = Path(preserved_name).name
        if not name or name != preserved_name:
            continue
        if not (run_dir / "raw" / "inputs" / name).is_file():
            continue
        raw_input_archives.append(
            {
                "name": name,
                "size_bytes": int(item.get("size", 0) or 0),
                "sha256": str(item.get("sha256") or ""),
                "download_path": f"raw-inputs/{name}",
            }
        )
    metadata = {
        "case": manifest.get("case_name") or manifest.get("case") or manifest.get("options", {}).get("case"),
        "scenario": manifest.get("scenario") or manifest.get("options", {}).get("scenario"),
        "code_ref": manifest.get("code_ref") or summary.get("code_ref"),
        "inputs": [_share_safe_input(value) for value in summary.get("inputs", [])],
        "run_dir": run_dir.name,
        "local_cache": local_cache,
        "read_path": read_path,
        "current_source_ref": source_ref,
        "raw_input_archives": raw_input_archives,
    }
    return {
        "schema_version": 1,
        "metadata": metadata,
        "topology": topology,
        "source_trace_count": len(all_rows),
        "excluded_without_client_window": excluded_worker_only,
        "excluded_non_get": excluded_non_get,
        "trace_count": len(rows),
        "write_trace_count": len(write_rows),
        "top_requested": top_n,
        "deadline_ms": deadline_ms,
        "evidence_coverage": coverage,
        "limitations": limitations,
        "problem_summary": {
            name: values["trace_count"] for name, values in aggregate_data["problem_summary"].items()
        },
        "aggregate": aggregate_data,
        "write_aggregate": write_aggregate,
        "traces": rows,
        "write_traces": write_rows,
    }


def render_html(analysis: dict, title: str) -> str:
    """Render one self-contained report from a precomputed analysis model."""

    rows = sorted(analysis["traces"], key=lambda row: (row["timestamp"], row["trace_id"]))
    write_rows = sorted(
        analysis.get("write_traces", []),
        key=lambda row: (row["timestamp"], row["trace_id"]),
    )
    output = _render_dashboard_html(
        rows,
        analysis["aggregate"],
        title,
        analysis["metadata"],
        write_rows,
        analysis.get("write_aggregate", {}),
    )
    return output.replace("Top100 时间序列", "TopN 时间序列").replace(
        "图 2-1 Stacked Bars", "图 2-1 TopN 时间序列 Stacked Bars"
    )


def write_outputs(
    analysis: dict,
    output: Path,
    *,
    title: str,
    force: bool = False,
    analysis_json: Path | None = None,
    source_run_dir: Path | None = None,
) -> tuple[Path, Path]:
    output = Path(output)
    analysis_json = Path(analysis_json) if analysis_json else output.with_name("bottleneck.analysis.json")
    existing = [path for path in (output, analysis_json) if path.exists()]
    if existing and not force:
        raise FileExistsError("refusing to overwrite: " + ", ".join(str(path) for path in existing))
    output.parent.mkdir(parents=True, exist_ok=True)
    analysis_json.parent.mkdir(parents=True, exist_ok=True)
    if source_run_dir is not None:
        source_run_dir = Path(source_run_dir)
        archive_dir = output.parent / "raw-inputs"
        for item in analysis.get("metadata", {}).get("raw_input_archives", []):
            name = Path(str(item.get("name") or "")).name
            if not name or name != str(item.get("name") or ""):
                continue
            source = source_run_dir / "raw" / "inputs" / name
            if source.is_file():
                archive_dir.mkdir(parents=True, exist_ok=True)
                shutil.copy2(source, archive_dir / name)
    output.write_text(render_html(analysis, title), encoding="utf-8")
    analysis_json.write_text(json.dumps(analysis, ensure_ascii=False, indent=2), encoding="utf-8")
    return output.resolve(), analysis_json.resolve()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-dir", required=True, type=Path)
    parser.add_argument("--top", type=int, default=100)
    parser.add_argument("--deadline-ms", type=float)
    parser.add_argument(
        "--local-cache",
        choices=("true", "false"),
        help="Access topology supplied by the user/config; omit when unknown",
    )
    parser.add_argument(
        "--source-ref",
        help="Current main/master ref used to verify code-path claims",
    )
    parser.add_argument(
        "--read-path",
        choices=("legacy-worker-pull",),
        help="Explicit historical runtime read topology; omit for current local-cache-derived topology",
    )
    parser.add_argument("--title")
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--analysis-json", type=Path)
    parser.add_argument("--force", action="store_true")
    args = parser.parse_args(argv)
    local_cache = None if args.local_cache is None else args.local_cache == "true"
    analysis = build_analysis(
        args.run_dir,
        top_n=args.top,
        deadline_ms=args.deadline_ms,
        local_cache=local_cache,
        read_path=args.read_path,
        source_ref=args.source_ref,
    )
    metadata = analysis["metadata"]
    title = args.title or f"{metadata.get('case') or 'DataSystem'} · Top{analysis['trace_count']} 关键瓶颈"
    output, _ = write_outputs(
        analysis,
        args.output,
        title=title,
        force=args.force,
        analysis_json=args.analysis_json,
        source_run_dir=args.run_dir,
    )
    print(output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
