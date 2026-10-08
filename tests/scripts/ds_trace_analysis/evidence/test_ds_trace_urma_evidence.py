"""Shared WR facts retain identity and timing independently of read/write budgets."""

import ast
import copy
import importlib
from pathlib import Path

import pytest

from test_ds_trace_bottleneck import load_module


def facts():
    load_module()
    return importlib.import_module("trace_analysis.evidence.urma")


def test_urma_facts_have_no_analysis_or_renderer_dependency():
    module = facts()
    tree = ast.parse(Path(module.__file__).read_text(encoding="utf-8"))
    imports = [node for node in tree.body if isinstance(node, (ast.Import, ast.ImportFrom))]
    assert all(
        isinstance(node, ast.Import) and all(alias.name == "re" for alias in node.names)
        or isinstance(node, ast.ImportFrom) and node.module == "__future__"
        for node in imports
    )
    legacy = load_module()
    for name in ("_trace_us", "_delta_ms", "_request_from_event", "_dedupe_urma_requests",
                 "_group_urma_logical_writes", "_urma_timeout_evidence"):
        assert getattr(legacy, name) is getattr(module, name)


@pytest.mark.parametrize("closing", ["", "}"])
def test_trace_us_keeps_missing_and_negative_durations_unobserved(closing):
    module = facts()
    clock = module._trace_us("trace_us:{post:1000, wait:1010, poll_begin:990" + closing)
    assert clock == {"post": 1000, "wait": 1010, "poll_begin": 990}
    assert module._delta_ms(clock, "post", "wait") == 0.01
    assert module._delta_ms(clock, "wait", "poll_begin") is None
    assert module._delta_ms(clock, "post", "observed") is None


def test_wr_identity_dedupes_repeated_observation_not_other_worker_or_chunk():
    module = facts()
    base = {"request_id": "42", "timestamp": "2026-09-20T14:43:48", "source_worker": "worker-a",
            "write_chunk_index": 1, "write_chunk_count": 2, "total_ms": 2.635}
    rows = [base, dict(base), {**base, "source_worker": "worker-b"}, {**base, "write_chunk_index": 2}]
    original = copy.deepcopy(rows)
    assert module._dedupe_urma_requests(rows) == [rows[0], rows[2], rows[3]]
    assert rows == original


def test_timeout_duration_does_not_become_completed_wr_time():
    module = facts()
    assert module._urma_timeout_evidence({}, ["Timed out waiting for urma_request_id 42 elapsedMs=5000"]) == (True, 5000)
    assert module._urma_timeout_evidence({"errors": {"URMA_WAIT_TIMEOUT": 1}}, []) == (True, None)
    assert not module._is_slow_wr(1.5)
    assert module._is_slow_wr(1.500001)


@pytest.mark.parametrize("request_id", ["42", ""])
def test_wr_identity_preserves_process_and_post_clock(request_id):
    module = facts()
    first = {"request_id": request_id, "timestamp": "2026-09-20T14:43:48",
             "source_worker": "worker-a", "owner": ["worker-a", "100"],
             "write_chunk_index": 1, "write_chunk_count": 2, "total_ms": 2.635,
             "trace_us": {"post": 1000}}
    second_process = {**first, "owner": ["worker-a", "200"]}
    later_post = {**first, "trace_us": {"post": 5000}}
    rows = [first, copy.deepcopy(first), second_process, later_post]
    original = copy.deepcopy(rows)
    assert module._dedupe_urma_requests(rows) == [first, second_process, later_post]
    assert rows == original


def test_logical_write_does_not_combine_chunks_from_different_processes():
    module = facts()
    first = {"request_id": "41", "source_worker": "worker-a", "owner": ["worker-a", "100"],
             "write_chunk_index": 1, "write_chunk_count": 2, "total_ms": 1,
             "trace_us": {"post": 1000, "observed": 2000}}
    second = {**first, "request_id": "42", "owner": ["worker-a", "200"],
              "write_chunk_index": 2, "trace_us": {"post": 100000, "observed": 101000}}
    groups = module._group_urma_logical_writes([first, second])
    assert len(groups) == 2
    assert all(not group["complete"] and group["wall_clock_ms"] is None for group in groups)
    second["owner"] = first["owner"]
    second["trace_us"] = {"post": 1500, "observed": 2500}
    groups = module._group_urma_logical_writes([first, second])
    assert len(groups) == 1
    assert groups[0]["complete"] and groups[0]["wall_clock_ms"] == 1.5
