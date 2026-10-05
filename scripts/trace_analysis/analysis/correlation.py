"""Worker-scoped event correlation with explicit evidence boundaries."""

from __future__ import annotations

from bisect import bisect_left
from bisect import bisect_right
import collections
import datetime as dt
import math
import re

from ..evidence.rpc import (
    _evidence_timestamp,
    _rpc_fields,
    _timestamp_value,
)
from ..evidence.urma import SLOW_WR_THRESHOLD_MS
from ..evidence.urma import _raw_float
from .stats import _group_metric


def _worker_roles(row: dict, worker: str, dimension: str, kind: str) -> list[str]:
    roles: set[str] = set()
    if worker == row.get("direct_data_worker"):
        roles.add("get_worker")
    if worker in row.get("urma_source_workers", []):
        roles.add("urma_source_worker")
    if worker == row.get("client_observer"):
        roles.add("client")
    if dimension == "rpc":
        roles.add("worker_handler" if kind == "rpc_server" else "rpc_emitter")
    if dimension == "metadata":
        roles.add("worker_metadata" if kind == "query_metadata" else "metadata_rpc_emitter")
    if dimension == "data" and kind == "remote_get":
        roles.add("data_access_emitter")
    if dimension == "data" and kind in {"local_processing", "query_local_read"}:
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


def _query_worker_event_views(row: dict) -> list[dict]:
    result = []
    for index, call in enumerate((row.get("query_and_get_breakdown") or {}).get("worker", [])):
        owner = call.get("owner") or []
        worker = owner[0] if owner else "未明确"
        base = {"trace_id": row["trace_id"], "timestamp": call.get("timestamp"),
                "worker": worker, "source_event_id": f"{row['trace_id']}:worker-query:{index}",
                "failed": False, "status_observed": False, "is_slow": False,
                "network_ms": None, "server_ms": None, "queue_ms": None, "retry": False,
                "client_ms": row.get("client_ms"), "companions": None,
                "component_scope": "Worker本地阶段；与调用端RPC及URMA窗口不相加"}
        observed = [("rpc", "rpc_server", "Worker QueryAndGet", call.get("total_ms"))]
        phases = call.get("phases_ms") or {}
        observed += [("metadata", "query_metadata", "Worker metadata", phases.get("metadata")),
                     ("data", "query_local_read", "Worker localRead", phases.get("localRead"))]
        for dimension, kind, method, latency in observed:
            if not isinstance(latency, (int, float)) or not math.isfinite(latency) or latency < 0:
                continue
            result.append(base | {"event_id": base["source_event_id"] + ":" + dimension,
                                  "dimension": dimension, "kind": kind, "method": method,
                                  "latency_ms": latency,
                                  "handler_ms": latency if kind == "rpc_server" else None,
                                  "worker_roles": _worker_roles(row, worker, dimension, kind)})
    return result


def _build_worker_correlation(rows: list[dict]) -> dict:
    events: list[dict] = []
    unassigned = 0
    untimed = 0
    for row in rows:
        worker_views = _query_worker_event_views(row)
        source_views = {event["source_event_id"]: event for event in worker_views}.values()
        for event in source_views:
            unassigned += event["worker"] in {"", "未明确"}
            untimed += _timestamp_value(event["timestamp"]) is None
        events.extend(worker_views)
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
                    "status_observed": status_observed,
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
    worker_timeline = collections.defaultdict(list)
    for event in valid_events:
        worker_timeline[event["worker"]].append((_timestamp_value(event["timestamp"]), event))
    worker_times = {}
    for worker, timeline in worker_timeline.items():
        timeline.sort(key=lambda item: item[0])
        worker_times[worker] = [item[0] for item in timeline]
    for event in valid_events:
        if event["kind"] not in {"query_meta", "remote_get"} or not event["failed"]:
            continue
        event_time = _timestamp_value(event["timestamp"])
        times = worker_times[event["worker"]]
        left, right = bisect_left(times, event_time - dt.timedelta(seconds=1)), bisect_right(
            times, event_time + dt.timedelta(seconds=1)
        )
        nearby = [candidate for _, candidate in worker_timeline[event["worker"]][left:right]
                  if candidate["source_event_id"] != event["source_event_id"]]
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
                    "handler_ms": _group_metric(
                        [item["handler_ms"] for item in rpc if item.get("handler_ms") is not None]
                    ),
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
                    "local_count": sum(item["kind"] in {"local_processing", "query_local_read"} for item in data),
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
