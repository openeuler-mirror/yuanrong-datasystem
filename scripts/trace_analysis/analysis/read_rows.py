"""Derive read bottleneck rows from normalized Trace and Evidence records."""

from __future__ import annotations

from .contracts import STAGE_NAMES
from .read_initial import _extract_trace
from .read import (
    _apply_explicit_rpc_errors, _apply_focus_breakdown,
    _apply_inline_query_urma_attribution, _apply_query_rpc_attribution,
    _apply_query_urma_timeout_attribution, _non_transport_analysis,
    _query_and_get_breakdown, _query_meta_detail, _refine_data_access_scope,
    _urma_critical_path,
)
from .stats import _percentile
from .budget import _urma_timeout_accounting
from ..evidence.normalized import read_observations
from ..evidence.observations import build_evidence_facts
from ..evidence.rpc import _analyze_rpc_calls
from ..evidence.urma import _group_urma_logical_writes, observed_urma_requests, worker_ip_mapping


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


def build_trace_rows(
    summary: dict, local_cache: bool | None = None, read_path: str | None = None,
    evidence_data: dict | None = None,
) -> list[dict]:
    topology = _topology_contract(local_cache, read_path)
    entries = evidence_data["traces"] if evidence_data is not None else None
    rows = [
        _extract_trace(trace_id, trace, read_observations(trace, entries[trace_id]) if entries is not None else None)
        for trace_id, trace in summary.get("traces", {}).items()
    ]
    for row in rows:
        trace = summary["traces"][row["trace_id"]]
        if entries is not None and "write" in entries[row["trace_id"]]:
            row["write_evidence_facts"] = entries[row["trace_id"]]["write"]
        row["rpc_analysis"] = _analyze_rpc_calls(trace)
        row["query_and_get_breakdown"] = _query_and_get_breakdown(
            row["rpc_analysis"], trace.get("query_and_get_calls", []))
    by_id = {row["trace_id"]: row for row in rows}
    ip_to_worker = worker_ip_mapping(summary)
    all_requests: list[dict] = []
    for trace_id, trace in summary.get("traces", {}).items():
        requests = observed_urma_requests(trace, ip_to_worker, local_cache, read_path)
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
        if inflight_threshold and max_inflight >= inflight_threshold:
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
        client_ratio_text = f"{urma_ratio:.1f}%" if urma_ratio is not None else "未观测"
        wait_ratio_text = f"{wait_ratio:.1f}%" if wait_ratio is not None else "未观测"
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
                f"{direction}），URMA/Client {client_ratio_text}"
                + (f"，completion wait {wait_ms:.3f}ms（{wait_ratio_text}）" if wait_ms is not None else "")
                + f"，Inflight WR 最大 {max_inflight}；证据标签：{'、'.join(labels) or '无突出标签'}。"
            ),
        }
    for row in rows:
        row["evidence_facts"] = (entries[row["trace_id"]]["facts"] if entries is not None else
                                 build_evidence_facts(row.get("evidence", []), row.get("evidence_records", [])))
        _apply_inline_query_urma_attribution(row)
        _apply_query_rpc_attribution(row)
        _apply_query_urma_timeout_attribution(row)
        row["query_meta_detail"] = _query_meta_detail(row)
        _refine_data_access_scope(row)
        row["non_transport_analysis"] = _non_transport_analysis(row, topology)
        _apply_focus_breakdown(row)
        row["urma_timeout_accounting"] = _urma_timeout_accounting(
            row, summary["traces"][row["trace_id"]], row["focus_breakdown_ms"]
        )
        row["focus_primary_stage"] = max(row["focus_breakdown_ms"], key=row["focus_breakdown_ms"].get)
        _apply_explicit_rpc_errors(row)
    return sorted(rows, key=lambda row: (row["timestamp"], row["trace_id"]))
