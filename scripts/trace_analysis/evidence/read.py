"""Observed GET facts extracted from one triaged Trace."""

from __future__ import annotations

import collections
from dataclasses import dataclass
import re

from .rpc import _latency_summary, _rpc_fields
from .errors import observe_error_evidence
from .urma import URMA_WAIT_TIMEOUT_RE


@dataclass
class ReadObservations:
    texts: list[str]
    display_indices: list[int]
    client_us: int
    worker_us: int
    status: int
    size_bytes: int
    transport: str
    summary: dict[str, int]
    rpcs: dict[str, list[dict[str, int]]]
    urma_values: list[float]
    direct_data_worker: str
    client_observer: str
    urma_source_costs: dict[str, float]
    explicit_remote: bool
    urma_total_text_observed: bool
    urma_timeout_error_observed: bool
    error_observations: dict


def display_evidence(trace: dict) -> tuple[list[str], list[tuple[str, str]], list[int]]:
    texts: list[str] = []
    evidence_items: list[tuple[str, str]] = []
    indices: list[int] = []
    seen: set[str] = set()
    for index, evidence in enumerate(trace.get("evidence", [])):
        text = evidence.get("text", "")
        canonical = text.split(" | ", 1)[1] if " | " in text else text
        if canonical in seen:
            continue
        seen.add(canonical)
        texts.append(text)
        evidence_items.append((text, evidence.get("worker", "") or "未明确"))
        indices.append(index)
    return texts, evidence_items, indices


def evidence_records(trace: dict) -> list[dict]:
    return [
        {"text": evidence.get("text", ""), "worker": evidence.get("worker", "") or "未明确",
         "source": evidence.get("source", ""), "member": evidence.get("member", ""),
         "line": evidence.get("line")}
        for evidence in trace.get("evidence", [])
    ]


def extract_read_observations(trace: dict) -> ReadObservations:
    texts, evidence_items, indices = display_evidence(trace)

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

        worker_match = re.search(r"\| \d+ \| DS_POSIX_(?:GET|REMOTE_GET|REMOTE_MGET) \| (\d+) \|", text)
        if worker_match:
            worker_us = int(worker_match.group(1))
            worker_summary.update(_latency_summary(text))
            direct_data_worker = evidence_worker
            if "DS_POSIX_REMOTE_" in text:
                explicit_remote = True

        method, fields = _rpc_fields(text)
        if method:
            rpcs[method].append(fields)
            if "BatchGetObjectRemote" in method:
                explicit_remote = True

        if "[Get] Remote done" in text or "[Get/RemotePull]" in text:
            explicit_remote = True

        urma_match = re.search(
            r"URMA_ELAPSED_TOTAL.*?\bcost\s*:?\s*([\d.]+)\s*(ms|us)\b",
            text.replace("*", ""),
            re.I,
        )
        if urma_match:
            urma_cost = float(urma_match.group(1))
            if urma_match.group(2).lower() == "us":
                urma_cost /= 1000.0
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
    if status == 0:
        for error_name, count in trace.get("errors", {}).items():
            status_match = re.fullmatch(r"status=(-?\d+)", error_name)
            if status_match and count and int(status_match.group(1)) != 0:
                status = int(status_match.group(1))
                break
    return ReadObservations(
        texts=texts,
        display_indices=indices,
        client_us=client_us,
        worker_us=worker_us,
        status=status,
        size_bytes=size_bytes,
        transport=transport,
        summary=summary,
        rpcs=rpcs,
        urma_values=urma_values,
        direct_data_worker=direct_data_worker,
        client_observer=client_observer,
        urma_source_costs=urma_source_costs,
        explicit_remote=explicit_remote,
        urma_total_text_observed=any("URMA_ELAPSED_TOTAL" in text for text in texts),
        urma_timeout_error_observed=any(
            count and URMA_WAIT_TIMEOUT_RE.search(str(name))
            for name, count in (trace.get("errors") or {}).items()
        ),
        error_observations=observe_error_evidence(texts),
    )
