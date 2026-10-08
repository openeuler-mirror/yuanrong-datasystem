"""Initial exclusive GET budget from normalized Trace observations."""

from __future__ import annotations

from .contracts import CATEGORY_REMOTE, CATEGORY_WORKER, CATEGORY_CLIENT_RPC, STAGE_NAMES
from .issues import _classify_urma_timeout_detail, _classify_rpc_deadline_detail
from ..evidence.read import ReadObservations, evidence_records, extract_read_observations


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


def _metric_max(value: object) -> float:
    """Read the current scalar summary shape and tolerate legacy percentiles."""

    if isinstance(value, dict):
        return float(value.get("max", 0) or 0)
    return float(value or 0)


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


def _extract_trace(trace_id: str, trace: dict, observed: ReadObservations | None = None) -> dict:
    observed = observed if observed is not None else extract_read_observations(trace)
    texts = observed.texts
    client_us = observed.client_us
    worker_us = observed.worker_us
    status = observed.status
    size_bytes = observed.size_bytes
    transport = observed.transport
    summary = observed.summary
    rpcs = observed.rpcs
    urma_values = observed.urma_values
    direct_data_worker = observed.direct_data_worker
    client_observer = observed.client_observer
    urma_source_costs = observed.urma_source_costs
    explicit_remote = observed.explicit_remote

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
        or observed.urma_total_text_observed
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
    missing_stages = set(STAGE_NAMES) - attribution.keys()
    if missing_stages:
        raise ValueError(f"read budget lacks stages: {sorted(missing_stages)}")
    primary_stage = max(STAGE_NAMES, key=attribution.get)
    error_observations = observed.error_observations
    urma_timeout_observed = error_observations["urma_timeout"] or observed.urma_timeout_error_observed
    urma_timeout_max_ms = error_observations["urma_timeout_elapsed_ms"]
    if urma_timeout_observed:
        error_family = "URMA超时"
        error_detail = _classify_urma_timeout_detail(status, error_observations)
    else:
        error_detail = _classify_rpc_deadline_detail(status, error_observations)
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
        "evidence_records": evidence_records(trace),
    }
