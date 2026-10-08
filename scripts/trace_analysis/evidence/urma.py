"""URMA timing, chunk identity and timeout evidence shared by report analyses."""

from __future__ import annotations

import re


SLOW_WR_THRESHOLD_MS = 1.5


def worker_ip_mapping(summary):
    mapping = summary.get("dimensions", {}).get("worker_ip_mapping", [])
    if isinstance(mapping, dict):
        return dict(mapping)
    return {
        item.get("pod_ip", ""): item.get("worker_full_name", "未映射")
        for item in mapping
        if isinstance(item, dict)
    }


def observed_urma_requests(trace, ip_to_worker, local_cache, read_path):
    remote_get_wr_count = max(
        (int(event.get("inflight_remote_get") or 0) for event in trace.get("ub_events", [])),
        default=0,
    )
    requests = [
        _request_from_event(event, remote_get_wr_count, ip_to_worker, local_cache, read_path)
        for event in trace.get("ub_events", [])
        if event.get("event_type") in {"total", "urma_total"} and event.get("cost_ms") is not None
    ]
    requests = _dedupe_urma_requests(requests)
    requests.sort(key=lambda item: (item["timestamp"], item["request_id"]))
    return requests

URMA_WAIT_TIMEOUT_RE = re.compile(
    r"(?:URMA(?:[_ -]WAIT[_ -]TIMEOUT)|Timed out waiting for urma_request_id)", re.I
)


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
    raw = event.get("raw", "").replace("**", "")
    columns = raw.split(" | ")
    request_match = re.search(r"(?:urma_request_id|request id)[:=]\s*(\d+)", raw, re.I)
    trace_us = _trace_us(raw)
    src_match = re.search(r"\bsrc (?:addr|address):\s*([^,\s]+)", raw)
    src_addr = event.get("src_addr") or (src_match.group(1) if src_match else "")
    target_match = re.search(r"\b(?:tgt addr|target address):\s*([^,\s]+)", raw)
    target_addr = event.get("target_addr") or (target_match.group(1) if target_match else "")
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
        "target_worker_mapped": ip_to_worker.get(target_addr.split(":", 1)[0]),
        "owner": ([columns[3].strip(), columns[4].strip().split(":")[0]] if len(columns) >= 5 else []),
        "src_addr": src_addr,
        "target_addr": target_addr,
        "data_size": event.get("data_size"),
        "cpuid": event.get("cpuid"),
        "status": event.get("status") or "未记录",
        "src_chip_inflight": event.get("src_chip_inflight") or "未记录",
        "urma_inflight_wr_count": event.get(
            "urma_inflight_wr_count", _raw_float(raw, r"\burma_inflight_wr_(?:cnt|count):\s*(\d+)")
        ),
        "remote_get_wr_count": remote_get_wr_count,
        "total_ms": total_ms,
        "is_slow": _is_slow_wr(total_ms),
        "wait_completion_ms": _raw_float(
            raw, r"(?:wait bthread completion time\([^)]*\)|condition wait):\s*([\d.]+)ms"
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


def _sender_identity(item):
    return (tuple(item.get("owner") or ()), item.get("source_worker"),
            item.get("src_addr"), item.get("target_addr"))


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
            or _sender_identity(request) != _sender_identity(current[0])
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


def _dedupe_urma_requests(requests: list[dict]) -> list[dict]:
    """Remove repeated log observations of the same WR before aggregation."""
    unique: list[dict] = []
    seen: set[tuple] = set()
    for item in requests:
        request_id = str(item.get("request_id") or "")
        if request_id:
            key = (
                "request",
                request_id,
                item.get("timestamp"),
                item.get("source_worker"),
                item.get("write_chunk_index"),
                item.get("write_chunk_count"),
            )
        else:
            key = (
                "observation",
                item.get("timestamp"),
                item.get("source_worker"),
                item.get("target_addr"),
                item.get("total_ms"),
                item.get("write_chunk_index"),
                item.get("write_chunk_count"),
            )
        key += (tuple(item.get("owner") or ()), (item.get("trace_us") or {}).get("post"))
        if key in seen:
            continue
        seen.add(key)
        unique.append(item)
    return unique
