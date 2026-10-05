"""RPC field and timestamp observations independent of report attribution."""

from __future__ import annotations

from bisect import bisect_right
import datetime as dt
import json
import math
import re


QUERY_AND_GET_METHOD_RE = re.compile(r"(?:Master|Worker)OCService\.QueryAndGet", re.I)


def _latency_summary(text: str) -> dict[str, int]:
    result: dict[str, int] = {}
    for body in re.findall(r"latencySummary:\{([^}]*)\}", text):
        for key, value in re.findall(r"([A-Za-z0-9_.]+):(-?\d+)", body):
            result[key] = int(value)
    return result


def _rpc_fields(text: str) -> tuple[str | None, dict[str, int]]:
    if "method=" not in text or "e2e_us=" not in text:
        return None, {}
    method_match = re.search(r"method=(\S+)", text)
    if not method_match:
        return None, {}
    fields = {key: int(value) for key, value in re.findall(r"([a-z0-9_]+)_us=(-?\d+)", text)}
    for key, value in re.findall(r"(cntl_error_code|cntl_failed)=(-?\d+)", text):
        fields[key] = int(value)
    return method_match.group(1), fields


def _rpc_summary_windows(trace: dict) -> list[dict]:
    labels = {"client.rpc.direct_query_and_get": "QueryAndGet", "worker.rpc.query_meta": "QueryMeta"}
    windows = []
    for event in trace.get("rpc_stage_windows", []):
        stage, duration = event["stage_key"], event["duration_us"]
        if not isinstance(duration, (int, float)) or not math.isfinite(duration) or duration < 0:
            raise ValueError("invalid normalized RPC stage duration")
        windows.append({**event, "stage_label": labels.get(stage, stage),
                        "method_hint": labels.get(stage), "total_ms": duration / 1000,
                        "evidence_scope": "access_stage", "network_ms": None,
                        "selection_reason": "summary_only", "selected": False})
    return windows


def _analyze_rpc_calls(trace: dict) -> dict:
    owners = {tuple(owner) for owner in trace.get("client_processes", [])}
    calls, seen = [], set()
    for raw in trace.get("rpc_calls", []):
        identity = json.dumps({k: v for k, v in raw.items() if k != "source"}, sort_keys=True)
        if identity in seen:
            continue
        seen.add(identity)
        call = {**raw, "network_ms": None, "selected": False, "selection_reason": "invalid_timing"}
        fields, clocks = raw.get("fields_us", {}), raw.get("clocks_ns", {})
        start, end = clocks.get("ClientSend", 0), clocks.get("ClientRecv", 0)
        recv, send = clocks.get("ServerRecv", 0), clocks.get("ServerSend", 0)
        network, e2e = fields.get("network_residual_us"), fields.get("e2e_us")
        valid = (network is not None and e2e is not None and 0 <= network <= e2e
                 and 0 < start <= end and 0 < recv <= send
                 and (end - start) / 1000 <= e2e + 2
                 and abs((end - start) / 1000 - fields.get("remote_processing_us", (end - start) / 1000)) <= 2
                 and abs(((end - start) - (send - recv)) / 1000 - network) <= 2)
        if valid:
            call["network_ms"] = network / 1000
            if len(owners) != 1:
                call["selection_reason"] = "ambiguous_client_process"
            elif tuple(raw.get("owner", [])) not in owners:
                call["selection_reason"] = "different_process"
            else:
                call["selection_reason"] = "candidate"
        calls.append(call)
    candidates = sorted((i for i, call in enumerate(calls) if call["selection_reason"] == "candidate"),
                        key=lambda i: (calls[i]["clocks_ns"]["ClientRecv"], calls[i]["clocks_ns"]["ClientSend"], i))
    ends = [calls[i]["clocks_ns"]["ClientRecv"] for i in candidates]
    scores, previous, take = [(0, 0)], [], []
    for position, index in enumerate(candidates):
        call = calls[index]
        before = bisect_right(ends, call["clocks_ns"]["ClientSend"], 0, position)
        proposed = (scores[before][0] + call["fields_us"]["network_residual_us"], scores[before][1] + 1)
        chosen = proposed > scores[-1]
        previous.append(before)
        take.append(chosen)
        scores.append(proposed if chosen else scores[-1])
        call["selection_reason"] = "overlapping_client_interval"
    position = len(candidates)
    while position:
        if take[position - 1]:
            calls[candidates[position - 1]].update(selected=True, selection_reason="serial_client_interval")
            position = previous[position - 1]
        else:
            position -= 1
    return {
        "calls": calls,
        "summary_windows": _rpc_summary_windows(trace),
        "client_process": list(next(iter(owners))) if len(owners) == 1 else None,
        "network_ms": scores[-1][0] / 1000 if candidates else None,
        "coverage": "normalized" if "rpc_calls" in trace else "legacy_summary",
        "overlap_policy": "maximum non-overlapping residual path; excluded calls remain visible",
    }


def _transport_phase_maps(evidence: list[str]) -> list[dict[str, int]]:
    result = []
    for text in evidence:
        for body in re.findall(r"phasesUs=\{([^}]*)\}", text):
            result.append(
                {name: int(value) for name, value in re.findall(r"([A-Za-z0-9_]+):(-?\d+)", body)}
            )
    return result


def _is_query_and_get_method(method: str) -> bool:
    return bool(QUERY_AND_GET_METHOD_RE.search(method))


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
