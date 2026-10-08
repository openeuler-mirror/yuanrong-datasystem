"""Semantic input projection for legacy NUMA calls using a read model."""
from __future__ import annotations

import copy
import hashlib
import json
from pathlib import Path


NUMA_ROW_FIELDS = (
    "trace_id", "client_ms", "status", "evidence", "direct_data_worker",
    "primary_problem", "transport", "failed",
)


def _rows(analysis: dict, key: str) -> list[dict]:
    rows = analysis.get(key, [])
    if not isinstance(rows, list) or any(not isinstance(row, dict) for row in rows):
        raise ValueError(f"{key} must be a list of row objects")
    return rows


def project_numa_inputs(analysis: dict) -> dict:
    """Retain consumed diagnosis fields and ordering, including duplicate precedence."""
    return _select_numa_inputs(analysis, copy.deepcopy)


def _select_numa_inputs(analysis, copy_value):
    return {
        group: [
            {field: copy_value(row[field]) for field in NUMA_ROW_FIELDS if field in row}
            for row in _rows(analysis, group)
        ]
        for group in ("traces", "write_traces")
    }


def _model_inputs(path: Path, kind: str, project) -> dict[str, str]:
    analysis = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(analysis, dict):
        raise ValueError("bottleneck model must be a JSON object")
    # The parsed model is local: borrowed rows are serialized here and never escape.
    payload = {"projection_version": 1, "kind": kind, "model": project(analysis)}
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"),
                           ensure_ascii=False, allow_nan=False)
    return {kind: hashlib.sha256(canonical.encode("utf-8")).hexdigest()}


def numa_model_inputs(path: Path) -> dict[str, str]:
    """Hash legacy NUMA read-model inputs; the pipeline now hashes Evidence instead."""
    return _model_inputs(path, "numa_base", lambda analysis: _select_numa_inputs(analysis, lambda value: value))
