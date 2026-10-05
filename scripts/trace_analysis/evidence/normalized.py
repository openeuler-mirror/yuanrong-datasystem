"""Versioned, compact observations shared by downstream Trace models."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

from .observations import build_evidence_facts
from .read import ReadObservations, evidence_records, extract_read_observations
from .write import build_write_facts, is_write_flow


def summary_digest(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        while True:
            block = source.read(1024 * 1024)
            if not block:
                break
            digest.update(block)
    return digest.hexdigest()


def build_evidence(summary: dict, source_sha256: str) -> dict:
    traces = {}
    coverage = {"trace_count": 0, "client_observed": 0, "rpc_observed": 0,
                "urma_observed": 0, "error_observed": 0}
    for trace_id, trace in summary.get("traces", {}).items():
        observed = extract_read_observations(trace)
        read = vars(observed).copy()
        read.pop("texts")
        facts = build_evidence_facts(observed.texts, evidence_records(trace))
        entry = {"read": read, "facts": facts}
        if is_write_flow(trace):
            entry["write"] = build_write_facts(trace_id, observed.texts)
        traces[trace_id] = entry
        coverage["trace_count"] += 1
        coverage["client_observed"] += bool(observed.client_us)
        coverage["rpc_observed"] += bool(observed.rpcs)
        coverage["urma_observed"] += bool(observed.urma_values)
        coverage["error_observed"] += bool(trace.get("errors"))
    return {"schema_version": 1, "summary_sha256": source_sha256,
            "traces": traces, "coverage": coverage}


def read_observations(trace: dict, entry: dict) -> ReadObservations:
    fields = entry["read"]
    texts = [trace["evidence"][index].get("text", "") for index in fields["display_indices"]]
    return ReadObservations(texts=texts, **fields)


def load_evidence(path: Path) -> dict:
    with path.open(encoding="utf-8") as source:
        data = json.load(source)
    if not isinstance(data, dict) or type(data.get("schema_version")) is not int or data["schema_version"] != 1:
        raise ValueError(f"unsupported evidence model: {path}")
    return data
