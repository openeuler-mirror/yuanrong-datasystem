"""Shared budget arithmetic and observed RPC/URMA interval accounting."""

from __future__ import annotations

import collections
import datetime as dt

from ..evidence.rpc import _timestamp_value


def _replace_rpc_network_budget(focus: dict, analysis: dict, donors: tuple[str, ...]) -> None:
    target = analysis.get("network_ms")
    if target is None:
        return
    current = focus["RPC网络相关"]
    if target < current:
        focus[donors[0]] += current - target
        focus["RPC网络相关"] = target
    else:
        focus["RPC网络相关"] += _take_focus_budget(focus, donors, target - current)
    analysis["attributed_network_ms"] = round(focus["RPC网络相关"], 6)
    analysis["budget_clipped_ms"] = round(max(0, target - focus["RPC网络相关"]), 6)


def _rpc_framework_ms(fields: dict[str, int]) -> float | None:
    required = {"e2e", "server_req_queue", "server_exec", "network_residual"}
    if not required.issubset(fields):
        return None
    if fields.get("cntl_failed") or fields.get("cntl_error_code"):
        if not any(fields.get(name, 0) for name in ("server_req_queue", "server_exec", "network_residual")):
            return None
    explained_us = sum(fields.get(name, 0) for name in ("server_req_queue", "server_exec", "network_residual"))
    return max(0.0, (fields["e2e"] - explained_us) / 1000.0)


def _valid_write_rpc_fields(fields: dict[str, int]) -> bool:
    components = ("network_residual", "server_req_queue", "server_exec")
    if fields.get("cntl_failed") or fields.get("cntl_error_code"):
        return False
    if _rpc_framework_ms(fields) is None:
        return False
    if any(fields[name] < 0 for name in ("e2e", *components)):
        return False
    return sum(fields[name] for name in components) <= fields["e2e"]


def _max_rpc_framework(entries: list[dict[str, int]]) -> float:
    values = []
    for fields in entries:
        value = _rpc_framework_ms(fields)
        if value is not None:
            values.append(value)
    return max(values, default=0.0)


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


def _urma_timeout_accounting(row: dict, trace: dict, budget: dict, write: bool = False) -> dict:
    """Reconcile log-aligned URMA intervals within the observed transport budget."""
    events = trace.get("urma_timeout_events", [])
    result = {"events": events, "event_count": len(events), "timeout_path_ms": None,
              "urma_path_ms": None, "added_ms": 0.0, "unallocated_ms": None,
              "basis": "原始发出进程内按本地日志时间对齐 elapsed 区间并集；跨进程取最大，不相加"}
    if not events:
        result["basis"] = "未观测结构化超时计时；报错仍独立保留，不补零耗时"
        return result
    owners = {tuple(p) for p in trace.get("client_processes", [])}
    intervals = collections.defaultdict(list)
    timeout_intervals = collections.defaultdict(list)

    def interval(event, duration):
        owner = tuple(event.get("owner", []))
        end = _timestamp_value(event.get("timestamp", ""))
        if len(owner) != 2 or not all(owner):
            return None
        if end is None or duration is None or duration < 0:
            return None
        if write and owner not in owners:
            return None
        return owner, (end - dt.timedelta(milliseconds=duration), end)
    for event in events:
        item = interval(event, event.get("elapsed_ms"))
        if item:
            owner, span = item
            timeout_intervals[owner].append(span)
            intervals[owner].append(span)
    completions = [event for event in row.get("urma_requests", row.get("write_wr_events", []))
                   if tuple(event.get("owner", [])) in timeout_intervals]
    result["completion_observations"] = [
        {key: event.get(key) for key in ("request_id", "owner", "timestamp", "total_ms", "trace_us")}
        for event in completions
    ]

    def path(groups):
        totals = []
        for spans in groups.values():
            merged = []
            for start, end in sorted(set(spans)):
                if merged and start <= merged[-1][1]:
                    merged[-1] = (merged[-1][0], max(merged[-1][1], end))
                else:
                    merged.append((start, end))
            totals.append(sum((end - start).total_seconds() * 1000 for start, end in merged))
        return round(max(totals), 6) if totals else None
    timeout_ms, path_ms = path(timeout_intervals), path(intervals)
    result.update(timeout_path_ms=timeout_ms, urma_path_ms=path_ms)
    if completions:
        result.update(urma_path_ms=None, basis="完成日志在等待返回后打印，total_ms 不含完成观察延迟；"
                      "完成与超时缺少共同可靠时钟锚点，分别保留观测，不混合求并集或重分预算")
        return result
    if path_ms is None:
        result["basis"] = "有超时原始记录，但发出进程未与该 Client 写入匹配"
        return result
    summary = trace.get("latency_summary_us", {})
    parent = (summary.get("client.urma.ub_transfer", 0) / 1000 if write else
              sum(summary.get(k, 0) for k in ("client.rpc.direct_query_and_get", "client.rpc.direct_get_data")) / 1000)
    communication = "写入URMA通信" if write else "URMA通信"
    scheduling = "写入URMA调度/线程开销" if write else "URMA调度/线程开销"
    current = budget.get(communication, 0) + budget.get(scheduling, 0)
    result.update(parent_ms=parent or None, allocated_ms=round(current, 6))
    if not parent or path_ms > parent + 1:
        result["unallocated_ms"] = round(max(0, path_ms - current), 6)
        result["basis"] += "；缺少对应传输父窗口或计时超出其范围，保留观测值，不强行挪动"
        return result
    target = min(path_ms, parent)
    needed = max(0.0, target - current)
    donors = ("未解释残差",) if write else ("未解释残差", "Get其他业务", "QueryAndGet其他业务")
    outside_keys = (("client.process.memory_copy", "client.process.set") if write else
                    ("client.process.direct_route", "client.process.direct_materialize", "client.process.get"))
    outside_ms = sum(summary.get(k, 0) for k in outside_keys) / 1000
    for key in donors:
        available = max(0.0, budget.get(key, 0) - (outside_ms if key == "未解释残差" else 0))
        moved = min(needed, available)
        budget[key] = round(budget.get(key, 0) - moved, 6)
        budget[communication] = round(budget.get(communication, 0) + moved, 6)
        needed -= moved
    result.update(
        allocated_ms=round(budget.get(communication, 0) + budget.get(scheduling, 0), 6),
        added_ms=round(max(0.0, target - current) - needed, 6),
        unallocated_ms=round(
            max(0.0, path_ms - (budget.get(communication, 0) + budget.get(scheduling, 0))), 6
        ),
    )
    return result
