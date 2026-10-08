"""Extract error-chain observations without assigning a root cause."""

import re

from .rpc import QUERY_AND_GET_METHOD_RE, _rpc_fields, _is_query_and_get_method
from .urma import URMA_WAIT_TIMEOUT_RE


def observe_error_evidence(evidence: list[str]) -> dict:
    joined = "\n".join(evidence)
    lowered = joined.lower()
    has_timeout = "timeout" in lowered or "timed out" in lowered
    has_deadline = "rpc deadline exceeded" in lowered
    has_get = "getobjectremote" in lowered
    has_query = "queryandget" in lowered
    pending = []
    if "urma_send_lane_" in lowered:
        matches = re.findall(
            r"URMA_SEND_LANE_(?:TIMEOUT_OBSERVED|FORCE_RELEASE)[^\n]*\bpendingWrs=(\d+)", joined, re.I)
        pending = [int(value) for value in matches]
    elapsed = []
    urma_elapsed = []
    if has_timeout:
        for line in evidence:
            if "TIMEOUT" not in line.upper() and "TIMED OUT" not in line.upper():
                continue
            match = re.search(r"elapsedMs\s*[=:]\s*([0-9]+(?:\.[0-9]+)?)", line, re.I)
            if match:
                elapsed.append(float(match.group(1)))
            if URMA_WAIT_TIMEOUT_RE.search(line):
                urma_match = re.search(r"\belapsedMs\s*[=:]\s*([\d.]+)", line, re.I)
                if urma_match:
                    urma_elapsed.append(float(urma_match.group(1)))
    rpc_deadline = False
    if "rpc timed out" in lowered or has_deadline or "cntl_error_code" in lowered:
        rpc_deadline = bool(re.search(
            r"RPC timed out|RPC deadline exceeded|cntl_error_code\s*[=:]\s*1008", joined, re.I))
    failed_methods = []
    if rpc_deadline:
        for text in evidence:
            method, fields = _rpc_fields(text)
            if method and (fields.get("cntl_failed") or fields.get("cntl_error_code")):
                failed_methods.append(method)
    arena_markers = ("out of memory", "no space in arena", "fresh_extent_unavailable")
    return {
        "pending_wrs": max(pending) if pending else None,
        "timeout_elapsed_ms": max(elapsed) if elapsed else None,
        "urma_timeout_elapsed_ms": max(urma_elapsed) if urma_elapsed else None,
        "urma_timeout": bool(URMA_WAIT_TIMEOUT_RE.search(joined)) if "urma" in lowered and has_timeout else False,
        "unexpected_payload": bool(re.search(r"Unexpected TCP payload|fallback payload", joined, re.I))
        if "payload" in lowered else False,
        "response_shape": "unexpectedly returned TCP payload" in joined or "fallback payload" in joined,
        "rpc_deadline": rpc_deadline,
        "receive_buffer_failure": "receive buffer preparation failed" in lowered,
        "arena_oom": any(marker in lowered for marker in arena_markers),
        "send_lane_timeout": "URMA_SEND_LANE_TIMEOUT_OBSERVED" in joined,
        "send_lane_release": "URMA_SEND_LANE_FORCE_RELEASE" in joined,
        "data_rpc_deadline": bool(re.search(r"GetObjectRemote->[^\n]*RPC deadline exceeded", joined, re.I))
        if has_deadline and "getobjectremote->" in lowered else False,
        "connect_deadline": bool(re.search(
            r"(?:WorkerWorkerExchangeUrmaConnectInfo->|UB establish failed:)[^\n]*RPC deadline exceeded",
            joined, re.I)) if has_deadline and (
                "workerworkerexchangeurmaconnectinfo->" in lowered or "ub establish failed:" in lowered) else False,
        "query_deadline": bool(re.search(
            r"(?:Master|Worker)OCService\.QueryAndGet[^\n]*RPC deadline exceeded", joined, re.I))
        if has_deadline and has_query else False,
        "get_method": bool(re.search(r"(?:WorkerWorkerOCService\.)?GetObjectRemote", joined, re.I))
        if has_get else False,
        "query_rpc_mention": bool(re.search(r"(?:Master|Worker)OCService\.QueryAndGet", joined, re.I))
        if has_query else False,
        "query_method": bool(QUERY_AND_GET_METHOD_RE.search(joined)) if has_query else False,
        "write_operation": "op=WRITE" in joined or "WRITE" in joined,
        "failed_methods": failed_methods,
    }


def failed_data_rpc(observations: dict) -> bool:
    return observations["data_rpc_deadline"] or any(
        method.endswith("GetObjectRemote") and "BatchGetObjectRemote" not in method
        for method in observations["failed_methods"]
    )


def failed_query_rpc(observations: dict) -> bool:
    return observations["query_deadline"] or any(
        _is_query_and_get_method(method) for method in observations["failed_methods"]
    )
