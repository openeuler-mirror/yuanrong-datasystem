"""Exclusive write-stage budget built from validated write observations."""

from .budget import (
    _rpc_framework_ms,
    _valid_write_rpc_fields,
    _replace_rpc_network_budget,
    _urma_scheduling_detail,
    _urma_timeout_accounting,
)
from ..diagnosis import WRITE_STAGES
from ..evidence.rpc import _analyze_rpc_calls
from ..evidence.write import resolve_write_facts, rpc_group


WRITE_STAGE_NAMES = WRITE_STAGES


def _write_rpc_split(parent_ms: float, entries: list[dict[str, int]]) -> dict[str, float]:
    split = {"other": parent_ms, "queue": 0.0, "network": 0.0, "framework": 0.0}
    complete = []
    for fields in entries:
        if _valid_write_rpc_fields(fields):
            complete.append(fields)
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
    facts = resolve_write_facts(base)
    client = facts["client_summary"]
    if client is not None:
        status = client["status"]
        client_ms = client["client_ms"]
        size_bytes = client["size_bytes"]

    create_us = (summary["client.rpc.create_total"] if "client.rpc.create_total" in summary
                 else summary.get("client.rpc.create", 0))
    publish_us = (summary["client.rpc.publish_total"] if "client.rpc.publish_total" in summary
                  else summary.get("client.rpc.publish", 0))
    create_observed = any(key in summary for key in ("client.rpc.create_total", "client.rpc.create"))
    publish_observed = any(key in summary for key in ("client.rpc.publish_total", "client.rpc.publish"))
    memory_ms = summary.get("client.process.memory_copy", 0) / 1000.0
    copy_observed = "client.process.memory_copy" in summary
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
        create_ms, create_budget, rpc_group(facts, "create")
    )
    publish_split = _scaled_write_rpc_split(
        publish_ms, publish_budget, rpc_group(facts, "publish")
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
    rpc_analysis = _analyze_rpc_calls(trace)
    _replace_rpc_network_budget(breakdown, rpc_analysis,
                                ("未解释残差", "Create RPC其他", "Publish RPC其他"))
    missing_stages = set(WRITE_STAGE_NAMES) - breakdown.keys()
    if missing_stages:
        raise ValueError(f"write budget lacks stages: {sorted(missing_stages)}")
    rounded = {name: round(max(0.0, breakdown.get(name)), 6) for name in WRITE_STAGE_NAMES}
    delta = round(client_ms - sum(rounded.values()), 6)
    rounded["未解释残差"] = round(max(0.0, rounded["未解释残差"] + delta), 6)
    timeout_accounting = _urma_timeout_accounting(base, trace, rounded, write=True)
    primary = max(WRITE_STAGE_NAMES, key=lambda name: rounded[name])
    return {
        "trace_id": base["trace_id"],
        "write_evidence_facts": facts,
        "timestamp": base.get("timestamp", ""),
        "last_ts": base.get("last_ts", ""),
        "client_ms": round(client_ms, 6),
        "failed": status != 0,
        "status": status,
        "size_bytes": size_bytes,
        "create_rpc_ms": round(create_ms, 6),
        "publish_rpc_ms": round(publish_ms, 6),
        "write_phase_observation": {
            "Create": {
                "state": "observed" if create_observed else "unobserved",
                "parent_ms": round(create_ms, 6) if create_observed else None,
                "source": "latency_summary" if create_observed else None,
            },
            "Copy": {
                "state": "observed" if copy_observed else "unobserved",
                "parent_ms": round(memory_ms, 6) if copy_observed else None,
                "source": "latency_summary" if copy_observed else None,
            },
            "Publish": {
                "state": "observed" if publish_observed else "unobserved",
                "parent_ms": round(publish_ms, 6) if publish_observed else None,
                "source": "latency_summary" if publish_observed else None,
            },
        },
        "write_wr_callsite": base.get("write_wr_callsite"),
        "write_wr_callsite_source": base.get("write_wr_callsite_source"),
        "write_data_ms": round(data_parent_ms, 6),
        "write_wr_events": base.get("urma_requests", []),
        "urma_timeout_accounting": timeout_accounting,
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
        "rpc_analysis": rpc_analysis,
        "write_breakdown_ms": rounded,
        "write_primary_stage": primary,
        "evidence": evidence,
        "dropped_evidence": int(trace.get("dropped_evidence", 0) or 0),
    }
