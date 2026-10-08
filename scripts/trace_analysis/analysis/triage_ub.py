"""Worker and lifecycle projections for observed URMA events."""

from collections import Counter, defaultdict
import re

from .triage_stats import _percentiles


def _ub_role(event_type):
    if event_type in ("transfer_path", "remote_get_start"):
        return "ub_entry"
    if event_type in ("total", "poll_jfc", "notify", "thread_sched"):
        return "ub_exit"
    return "unknown"


def _build_ub_worker_summary(trace_rows, bucket_ms=1000):
    workers = defaultdict(lambda: {
        "entry_events": 0,
        "exit_events": 0,
        "trace_ids": set(),
        "entry_trace_ids": set(),
        "exit_trace_ids": set(),
        "latencies": [],
        "edges": Counter(),
        "first_ts": None,
        "last_ts": None,
    })
    buckets = defaultdict(lambda: {
        "bucket_start": None,
        "entry_events": 0,
        "exit_events": 0,
        "latencies": [],
        "entry_workers": Counter(),
        "exit_workers": Counter(),
    })
    for trace_id, trace in trace_rows.items():
        for event in trace.get("ub_events", []):
            worker = event.get("worker") or "unknown"
            role = _ub_role(event.get("event_type"))
            if role == "unknown":
                continue
            item = workers[worker]
            item["trace_ids"].add(trace_id)
            if role == "ub_entry":
                item["entry_events"] += 1
                item["entry_trace_ids"].add(trace_id)
            else:
                item["exit_events"] += 1
                item["exit_trace_ids"].add(trace_id)
            if event.get("src_addr") and event.get("target_addr"):
                item["edges"][f"{event['src_addr']} -> {event['target_addr']}"] += 1
            if event.get("cost_ms") is not None:
                item["latencies"].append(event["cost_ms"])
            ts = event.get("timestamp")
            if ts:
                item["first_ts"] = min(filter(None, [item["first_ts"], ts])) if item["first_ts"] else ts
                item["last_ts"] = max(filter(None, [item["last_ts"], ts])) if item["last_ts"] else ts
                bucket_start = ts[:19]
                bucket = buckets[bucket_start]
                bucket["bucket_start"] = bucket_start
                if role == "ub_entry":
                    bucket["entry_events"] += 1
                    bucket["entry_workers"][worker] += 1
                else:
                    bucket["exit_events"] += 1
                    bucket["exit_workers"][worker] += 1
                if event.get("cost_ms") is not None:
                    bucket["latencies"].append(event["cost_ms"])
    worker_rows = {}
    for worker, item in workers.items():
        role = "ub_entry_and_exit" if item["entry_events"] and item["exit_events"] else (
            "ub_entry" if item["entry_events"] else "ub_exit")
        worker_rows[worker] = {
            "role": role,
            "entry_events": item["entry_events"],
            "exit_events": item["exit_events"],
            "trace_count": len(item["trace_ids"]),
            "entry_trace_count": len(item["entry_trace_ids"]),
            "exit_trace_count": len(item["exit_trace_ids"]),
            "latency_ms": _percentiles(item["latencies"]),
            "top_edges": [edge for edge, _ in item["edges"].most_common(5)],
            "first_ts": item["first_ts"],
            "last_ts": item["last_ts"],
        }
    time_rows = []
    for _, item in sorted(buckets.items()):
        time_rows.append({
            "bucket_start": item["bucket_start"],
            "bucket_ms": bucket_ms,
            "entry_events": item["entry_events"],
            "exit_events": item["exit_events"],
            "latency_ms": _percentiles(item["latencies"]),
            "top_entry_workers": [worker for worker, _ in item["entry_workers"].most_common(5)],
            "top_exit_workers": [worker for worker, _ in item["exit_workers"].most_common(5)],
        })
    return {
        "workers": dict(sorted(worker_rows.items(), key=lambda kv: (
            -(kv[1]["latency_ms"].get("max") or 0),
            -(kv[1]["entry_events"] + kv[1]["exit_events"]),
            kv[0],
        ))),
        "time_buckets": time_rows,
    }


def _add_lifecycle_metric(metrics, name, value):
    if value is None:
        return
    try:
        metrics[name].append(float(value))
    except (TypeError, ValueError):
        return


def _parse_chip_inflight(raw):
    chips = {}
    if not raw:
        return chips
    for chip, value in re.findall(r"(\d+)\s*:\s*(\d+)", raw):
        chips[chip] = int(value)
    return chips


def _build_ub_lifecycle_summary(trace_rows):
    metrics = defaultdict(list)
    chip_inflight = defaultdict(list)
    trace_remote_get_wr_counts = defaultdict(list)
    requests = {}

    def request_row(trace_id, event):
        request_id = event.get("request_id")
        if request_id:
            key_parts = [str(part or "") for part in (trace_id, request_id)]
            key = "|".join(key_parts)
        else:
            key_parts = []
            fallback_parts = (
                trace_id,
                event.get("worker"),
                event.get("src_addr"),
                event.get("target_addr"),
                event.get("timestamp"),
            )
            for part in fallback_parts:
                key_parts.append(str(part or ""))
            key = "|".join(key_parts)
        row = requests.setdefault(key, {
            "trace_id": trace_id,
            "request_id": request_id or "",
            "first_ts": event.get("timestamp") or "",
            "last_ts": event.get("timestamp") or "",
            "worker": event.get("worker") or "unknown",
            "src_addr": event.get("src_addr") or "",
            "target_addr": event.get("target_addr") or "",
            "data_size": event.get("data_size"),
            "cpuid": event.get("cpuid"),
            "status": event.get("status") or "",
            "src_chip_inflight": event.get("src_chip_inflight") or "",
            "urma_inflight_wr_count": event.get("urma_inflight_wr_count"),
            "remote_get_wr_count": event.get("inflight_remote_get"),
            "write_chunk_index": event.get("write_chunk_index"),
            "write_chunk_count": event.get("write_chunk_count"),
            "wake_sched_kind": event.get("wake_sched_kind") or "",
            "wake_sched_inherited": event.get("wake_sched_inherited"),
            "waited_for_notification": event.get("waited_for_notification"),
            "pre_completed_before_wait": event.get("pre_completed_before_wait"),
            "total_ms": None,
            "wait_os_sched_ms": None,
            "reported_wake_sched_latency_ms": None,
            "wake_sched_latency_ms": None,
            "completion_observation_latency_ms": None,
            "event_processing_and_wait_latency_ms": None,
            "poll_jfc_ms": None,
            "notify_ms": None,
            "thread_sched_ms": None,
            "poll_loop_gap_ms": None,
            "nanosleep_wake_ms": None,
        })
        if event.get("timestamp"):
            if row["first_ts"]:
                row["first_ts"] = min(filter(None, [row["first_ts"], event["timestamp"]]))
            else:
                row["first_ts"] = event["timestamp"]
            if row["last_ts"]:
                row["last_ts"] = max(filter(None, [row["last_ts"], event["timestamp"]]))
            else:
                row["last_ts"] = event["timestamp"]
        for field in ("worker", "src_addr", "target_addr", "status", "src_chip_inflight", "wake_sched_kind"):
            if event.get(field):
                row[field] = event[field]
        for field in (
            "data_size", "cpuid", "urma_inflight_wr_count", "inflight_remote_get", "write_chunk_index",
            "write_chunk_count",
        ):
            if event.get(field) is not None:
                row["remote_get_wr_count" if field == "inflight_remote_get" else field] = event[field]
        for field in ("wake_sched_inherited", "waited_for_notification", "pre_completed_before_wait"):
            if event.get(field) is not None:
                row[field] = event[field]
        return row

    def update_max(row, field, value):
        if value is None:
            return
        value = float(value)
        row[field] = value if row.get(field) is None else max(row[field], value)

    for trace_id, trace in trace_rows.items():
        for event in trace.get("ub_events", []):
            event_type = event.get("event_type")
            if event.get("inflight_remote_get") is not None:
                _add_lifecycle_metric(metrics, "remote_get_wr_count", event.get("inflight_remote_get"))
                trace_remote_get_wr_counts[trace_id].append(event.get("inflight_remote_get"))
                update_max(request_row(trace_id, event), "remote_get_wr_count", event.get("inflight_remote_get"))
            if event_type == "total":
                _add_lifecycle_metric(metrics, "total_ms", event.get("cost_ms"))
                _add_lifecycle_metric(metrics, "wait_os_sched_ms", event.get("wait_os_sched_ms"))
                _add_lifecycle_metric(metrics, "urma_inflight_wr_count", event.get("urma_inflight_wr_count"))
                for chip, value in _parse_chip_inflight(event.get("src_chip_inflight")).items():
                    chip_inflight[chip].append(value)
                row = request_row(trace_id, event)
                update_max(row, "total_ms", event.get("cost_ms"))
                update_max(row, "wait_os_sched_ms", event.get("wait_os_sched_ms"))
                update_max(row, "urma_inflight_wr_count", event.get("urma_inflight_wr_count"))
                wake_sched_latency_us = event.get("wake_sched_latency_us")
                if wake_sched_latency_us is not None:
                    wake_sched_latency_ms = wake_sched_latency_us / 1000.0
                    update_max(row, "reported_wake_sched_latency_ms", wake_sched_latency_ms)
                    wake_kind = event.get("wake_sched_kind")
                    if wake_kind:
                        _add_lifecycle_metric(metrics, f"{wake_kind}_wake_sched_latency_ms", wake_sched_latency_ms)
                    if event.get("wake_sched_inherited"):
                        _add_lifecycle_metric(metrics, "inherited_wake_sched_latency_ms", wake_sched_latency_ms)
                    if event.get("wake_sched_is_actual", True):
                        _add_lifecycle_metric(metrics, "wake_sched_latency_ms", wake_sched_latency_ms)
                        update_max(row, "wake_sched_latency_ms", wake_sched_latency_ms)
                completion_observation_us = event.get("completion_observation_latency_us")
                if completion_observation_us is not None:
                    completion_observation_ms = completion_observation_us / 1000.0
                    _add_lifecycle_metric(metrics, "completion_observation_latency_ms", completion_observation_ms)
                    update_max(row, "completion_observation_latency_ms", completion_observation_ms)
                event_processing_us = event.get("event_processing_and_wait_latency_us")
                if event_processing_us is not None and event.get("event_processing_and_wait_latency_valid") is True:
                    event_processing_ms = event_processing_us / 1000.0
                    _add_lifecycle_metric(metrics, "event_processing_and_wait_latency_ms", event_processing_ms)
                    update_max(row, "event_processing_and_wait_latency_ms", event_processing_ms)
                continue
            if event_type == "poll_jfc":
                _add_lifecycle_metric(metrics, "poll_jfc_ms", event.get("cost_ms"))
                update_max(request_row(trace_id, event), "poll_jfc_ms", event.get("cost_ms"))
                continue
            if event_type == "notify":
                _add_lifecycle_metric(metrics, "notify_ms", event.get("cost_ms"))
                update_max(request_row(trace_id, event), "notify_ms", event.get("cost_ms"))
                continue
            if event_type == "thread_sched":
                kind = event.get("thread_sched_kind")
                _add_lifecycle_metric(metrics, "thread_sched_ms", event.get("cost_ms"))
                row = request_row(trace_id, event)
                update_max(row, "thread_sched_ms", event.get("cost_ms"))
                if kind == "poll_loop_gap":
                    gap_ms = event.get("last_poll_end_to_start_us", 0) / 1000.0
                    _add_lifecycle_metric(metrics, "poll_loop_gap_ms", gap_ms)
                    update_max(row, "poll_loop_gap_ms", gap_ms)
                elif kind == "nanosleep_wake":
                    _add_lifecycle_metric(metrics, "nanosleep_wake_ms", event.get("cost_ms"))
                    update_max(row, "nanosleep_wake_ms", event.get("cost_ms"))

    request_rows = list(requests.values())
    for row in request_rows:
        if row.get("remote_get_wr_count") is None and trace_remote_get_wr_counts.get(row["trace_id"]):
            row["remote_get_wr_count"] = max(trace_remote_get_wr_counts[row["trace_id"]])
        score_fields = (
            "total_ms",
            "wait_os_sched_ms",
            "completion_observation_latency_ms",
            "event_processing_and_wait_latency_ms",
            "poll_loop_gap_ms",
            "nanosleep_wake_ms",
            "poll_jfc_ms",
            "notify_ms",
        )
        score_values = [float(row.get(field) or 0) for field in score_fields]
        row["score_ms"] = max(score_values)
        for field in (
            "total_ms", "wait_os_sched_ms", "reported_wake_sched_latency_ms", "wake_sched_latency_ms",
            "completion_observation_latency_ms",
            "event_processing_and_wait_latency_ms", "poll_jfc_ms", "notify_ms",
            "thread_sched_ms", "poll_loop_gap_ms", "nanosleep_wake_ms", "remote_get_wr_count",
            "urma_inflight_wr_count", "score_ms",
        ):
            if row.get(field) is not None:
                row[field] = round(float(row[field]), 3)
    request_rows.sort(key=lambda item: (-item["score_ms"], item["trace_id"], item["request_id"]))
    return {
        "metrics": {name: _percentiles(values) for name, values in sorted(metrics.items())},
        "chip_inflight": {chip: _percentiles(values) for chip, values in sorted(chip_inflight.items())},
        "requests": request_rows[:80],
    }
