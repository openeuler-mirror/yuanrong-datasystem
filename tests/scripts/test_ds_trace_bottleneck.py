#!/usr/bin/env python3
"""Tests for the post-triage main-bottleneck report."""

from __future__ import annotations

import importlib.util
import json
from pathlib import Path

import pytest


SCRIPT = Path(__file__).resolve().parents[2] / "scripts" / "ds_trace_bottleneck.py"


def load_module():
    spec = importlib.util.spec_from_file_location("ds_trace_bottleneck", SCRIPT)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def metric(value: float) -> dict:
    return {"count": 1, "min": value, "p50": value, "p90": value, "p99": value, "max": value}


def trace(
    trace_id: str,
    client_ms: float,
    worker_ms: float,
    *,
    timestamp: str,
    network_ms: float = 0,
    batch_network_ms: float = 0,
    query_meta_ms: float = 0,
    urma_ms: float = 0,
    local_ms: float = 0,
    worker: str = "worker-a",
    status: int = 0,
) -> dict:
    stages = []
    if query_meta_ms:
        stages.append(
            {
                "stage": "read.entry_to_meta_worker",
                "confidence": "high",
                "source": "worker.rpc.query_meta",
                "duration_ms": query_meta_ms,
            }
        )
    if urma_ms:
        stages.append(
            {
                "stage": "read.data_worker_ub_write",
                "confidence": "high",
                "source": "URMA_ELAPSED_TOTAL",
                "duration_ms": urma_ms,
            }
        )
    return {
        "classification": "rpc_slow" if network_ms else "slow",
        "line_count": 4,
        "workers": {worker: 4},
        "first_ts": timestamp,
        "last_ts": timestamp,
        "flows": {"DS_KV_CLIENT_GET": 1, "DS_POSIX_GET": 1},
        "access_latency_ms": metric(max(client_ms, worker_ms)),
        "access_latency_ms_by_role": {
            "client": metric(client_ms),
            "worker": metric(worker_ms),
        },
        "breakdown_ms": {"worker.process.get": local_ms} if local_ms else {},
        "rpc_slow": {"datasystem.WorkerOCService.Get": 1} if network_ms else {},
        "rpc_slow_fields_us": {"network_residual_us": metric(network_ms * 1000)} if network_ms else {},
        "urma_elapsed_ms": {"total": metric(urma_ms)} if urma_ms else {},
        "urma_perf_ms": {},
        "custom_metrics_ms": {},
        "ub_events": [
            {
                "event_type": "urma_total",
                "timestamp": timestamp,
                "worker": worker,
                "target_addr": "target:1",
                "cost_ms": urma_ms,
                "urma_inflight_wr_count": 7,
                "raw": "URMA_ELAPSED_TOTAL request id: 42 Inflight WR: 7",
            }
        ] if urma_ms else [],
        "latency_summary_us": {},
        "latency_summary_raw": [],
        "errors": {f"status={status}": 1} if status else {},
        "input_sources": ["fixture.log"],
        "source_stats": {},
        "dropped_evidence": 0,
        "triage_flags": [],
        "stage_breakdown": stages,
        "evidence_coverage": {
            "client": "present",
            "entry_worker": "present",
            "meta_worker": "present" if query_meta_ms else "missing",
            "data_worker": "present",
            "urma": "present" if urma_ms else "missing",
        },
        "missing_evidence": [],
        "evidence": [
            {
                "source": "fixture.log",
                "member": trace_id,
                "line": 1,
                "worker": worker,
                "host_ip": "",
                "text": f"{timestamp} | traceId: {trace_id} | client={client_ms}ms worker={worker_ms}ms",
            }
        ] + ([
            {
                "source": "fixture.log",
                "member": trace_id,
                "line": 2,
                "worker": worker,
                "host_ip": "",
                "text": (
                    f"{timestamp} | method=datasystem.WorkerOCService.BatchGetObjectRemote "
                    f"e2e_us={int(batch_network_ms * 1000)} server_exec_us=0 "
                    f"network_residual_us={int(batch_network_ms * 1000)} cntl_error_code=0"
                ),
            }
        ] if batch_network_ms else []),
    }


def urma_event(timestamp: str, worker: str, cost_ms: float, request_id: str) -> dict:
    return {
        "event_type": "urma_total",
        "timestamp": timestamp,
        "worker": worker,
        "target_addr": "target:1",
        "cost_ms": cost_ms,
        "urma_inflight_wr_count": 1,
        "raw": (
            f"{timestamp} [URMA_ELAPSED_TOTAL] urma_request_id:{request_id} "
            f"total cost {cost_ms}ms"
        ),
    }


def chunked_urma_event(
    timestamp: str,
    worker: str,
    cost_ms: float,
    request_id: str,
    chunk_index: int,
    chunk_count: int,
    *,
    post_us: int,
    observed_us: int,
) -> dict:
    return {
        "event_type": "urma_total",
        "timestamp": timestamp,
        "worker": worker,
        "target_addr": "client:1",
        "cost_ms": cost_ms,
        "data_size": 4 * 1024 * 1024,
        "write_chunk_index": chunk_index,
        "write_chunk_count": chunk_count,
        "urma_inflight_wr_count": chunk_count - chunk_index + 1,
        # Production 0813 lines do not close trace_us with a right brace.
        "raw": (
            f"{timestamp} [URMA_ELAPSED_TOTAL] [urma_request_id:{request_id}] "
            f"total cost {cost_ms}ms, writeChunkIndex:{chunk_index}, "
            f"writeChunkCount:{chunk_count}, trace_us:{{post:{post_us}, "
            f"wait:{post_us + 10}, observed:{observed_us}"
        ),
    }


@pytest.fixture
def run_dir(tmp_path: Path) -> Path:
    run = tmp_path / "run"
    run.mkdir()
    traces = {
        "rpc-trace": trace(
            "rpc-trace", 20, 1, timestamp="2026-08-15T10:00:00.000001", network_ms=12, status=1
        ),
        "urma-trace": trace(
            "urma-trace", 15, 10, timestamp="2026-08-15T10:00:01.000001", urma_ms=8, worker="worker-b"
        ),
        "local-trace": trace(
            "local-trace", 10, 9, timestamp="2026-08-15T10:00:02.000001", local_ms=9, worker="worker-c"
        ),
        "small-trace": trace(
            "small-trace", 4, 2, timestamp="2026-08-15T10:00:03.000001", query_meta_ms=1
        ),
    }
    summary = {
        "schema_version": 7,
        "code_ref": "fixture-ref",
        "inputs": ["fixture.log"],
        "trace_count": len(traces),
        "dimensions": {"input_failures": [], "worker_ip_mapping": {}, "coverage": {}},
        "traces": traces,
    }
    manifest = {
        "schema_version": 1,
        "case_name": "fixture",
        "scenario": "test",
        "code_ref": "fixture-ref",
        "time_range": {"first": "2026-08-15T10:00:00", "last": "2026-08-15T10:00:03"},
    }
    (run / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
    (run / "summary.json").write_text(json.dumps(summary), encoding="utf-8")
    (run / "triage.json").write_text(json.dumps({"issues": []}), encoding="utf-8")
    return run


def test_slow_wr_uses_strict_fixed_threshold(run_dir: Path):
    summary_path = run_dir / "summary.json"
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    boundary = trace(
        "wr-boundary",
        8,
        4,
        timestamp="2026-08-15T10:00:04.000001",
        worker="worker-wr",
    )
    boundary["ub_events"] = [
        urma_event("2026-08-15T10:00:04.000001", "worker-wr", 1.500, "1500"),
        urma_event("2026-08-15T10:00:04.100001", "worker-wr", 1.501, "1501"),
    ]
    boundary["urma_elapsed_ms"] = {
        "total": {"count": 2, "min": 1.5, "p50": 1.5005, "p90": 1.5009, "p99": 1.50099, "max": 1.501}
    }
    boundary["evidence_coverage"]["urma"] = "present"
    summary["traces"]["wr-boundary"] = boundary
    summary["trace_count"] += 1
    summary_path.write_text(json.dumps(summary), encoding="utf-8")

    analysis = load_module().build_analysis(run_dir, top_n=100, deadline_ms=20)
    urma = analysis["aggregate"]["urma_analysis"]
    row = next(item for item in analysis["traces"] if item["trace_id"] == "wr-boundary")

    assert urma["slow_threshold_ms"] == 1.5
    assert sum(item["is_slow"] for item in row["urma_requests"]) == 1


def test_explicit_legacy_worker_pull_topology_preserves_historical_direction(run_dir: Path):
    analysis = load_module().build_analysis(
        run_dir,
        top_n=100,
        local_cache=False,
        read_path="legacy-worker-pull",
        source_ref="current-main",
    )

    assert analysis["topology"]["kind"] == "legacy_worker_pull"
    assert analysis["topology"]["batch_get_path"] == "Worker→Data Worker"
    assert analysis["topology"]["urma_path"] == "Data Worker→请求 Worker"
    urma_row = next(item for item in analysis["traces"] if item["trace_id"] == "urma-trace")
    assert urma_row["urma_requests"][0]["target_worker"] != "Client"
    assert any("historical runtime" in item for item in analysis["limitations"])


def test_transfer_path_ub_without_total_does_not_create_wr(run_dir: Path):
    summary_path = run_dir / "summary.json"
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    label_only = trace(
        "ub-label-only",
        7,
        6,
        timestamp="2026-08-15T10:00:05.000001",
        worker="worker-label",
    )
    label_only["evidence"].append(
        {
            "source": "fixture.log",
            "member": "ub-label-only",
            "line": 2,
            "worker": "worker-label",
            "host_ip": "",
            "text": (
                "2026-08-15T10:00:05.000001 [SLOW LOG] [Get] Done, "
                "transferPath: UB, totalCost: 6ms"
            ),
        }
    )
    summary["traces"]["ub-label-only"] = label_only
    summary["trace_count"] += 1
    summary_path.write_text(json.dumps(summary), encoding="utf-8")

    analysis = load_module().build_analysis(run_dir, top_n=100, deadline_ms=20)
    row = next(item for item in analysis["traces"] if item["trace_id"] == "ub-label-only")

    assert row["urma_requests"] == []


def test_worker_events_keep_four_dimensions_separate(run_dir: Path):
    summary_path = run_dir / "summary.json"
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    correlated = trace(
        "correlated-failure",
        18,
        15,
        timestamp="2026-08-15T10:00:10.000000",
        worker="worker-correlation",
    )
    correlated["evidence"].extend(
        [
            {
                "source": "fixture.log",
                "member": "correlated-failure",
                "line": 2,
                "worker": "worker-correlation",
                "host_ip": "",
                "text": (
                    "2026-08-15T10:00:10.000000 method=datasystem.MasterService.QueryMeta "
                    "e2e_us=2100 server_req_queue_us=100 server_exec_us=900 "
                    "network_residual_us=1100 cntl_error_code=1008 cntl_failed=1"
                ),
            },
            {
                "source": "fixture.log",
                "member": "correlated-failure",
                "line": 3,
                "worker": "worker-correlation",
                "host_ip": "",
                "text": (
                    "2026-08-15T10:00:10.100000 method=datasystem.WorkerWorkerOCService.BatchGetObjectRemote "
                    "e2e_us=15000 server_req_queue_us=0 server_exec_us=0 "
                    "network_residual_us=0 cntl_error_code=1008 cntl_failed=1"
                ),
            },
            {
                "source": "fixture.log",
                "member": "correlated-failure",
                "line": 4,
                "worker": "worker-correlation",
                "host_ip": "",
                "text": (
                    "2026-08-15T10:00:10.200000 [Get] Local processing done, objects: 1, "
                    "remoteObjects: 0, costUs: 13442, rc: code: [OK]"
                ),
            },
        ]
    )
    correlated["ub_events"] = [
        urma_event("2026-08-15T10:00:09.100000", "worker-correlation", 1.6, "before"),
        urma_event("2026-08-15T10:00:11.100000", "worker-correlation", 1.7, "after"),
        urma_event("2026-08-15T10:00:10.100000", "worker-other", 9.0, "other-worker"),
    ]
    correlated["urma_elapsed_ms"] = {
        "total": {"count": 3, "min": 1.6, "p50": 1.7, "p90": 7.54, "p99": 8.854, "max": 9.0}
    }
    correlated["evidence_coverage"]["urma"] = "present"
    summary["traces"]["correlated-failure"] = correlated
    summary["trace_count"] += 1
    summary_path.write_text(json.dumps(summary), encoding="utf-8")

    correlation = load_module().build_analysis(run_dir, top_n=100, deadline_ms=20)["aggregate"][
        "worker_correlation"
    ]

    assert {event["dimension"] for event in correlation["events"]} >= {
        "rpc",
        "ub",
        "metadata",
        "data",
    }
    assert any(
        event["kind"] == "local_processing" and event["latency_ms"] == pytest.approx(13.442)
        for event in correlation["events"]
    )
    assert all(
        not event["failed"]
        for event in correlation["events"]
        if event["kind"] == "urma_wr"
    )
    failure = next(
        event
        for event in correlation["events"]
        if event["kind"] == "remote_get" and event["failed"]
    )
    assert failure["companions"]["slow_wr_count"] == 2
    assert failure["companions"]["other_worker_event_count"] == 0


def test_worker_correlation_counts_unassigned_and_untimed_events(run_dir: Path):
    summary_path = run_dir / "summary.json"
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    degraded = trace(
        "degraded-events",
        9,
        6,
        timestamp="2026-08-15T10:00:12.000000",
        worker="worker-degraded",
    )
    degraded["evidence"].extend(
        [
            {
                "source": "fixture.log",
                "member": "degraded-events",
                "line": 2,
                "worker": "未明确",
                "host_ip": "",
                "text": (
                    "2026-08-15T10:00:12.100000 method=datasystem.MasterService.QueryMeta "
                    "e2e_us=3000 server_exec_us=1000 network_residual_us=2000 "
                    "cntl_error_code=1008 cntl_failed=1"
                ),
            },
            {
                "source": "fixture.log",
                "member": "degraded-events",
                "line": 3,
                "worker": "worker-degraded",
                "host_ip": "",
                "text": (
                    "method=datasystem.WorkerWorkerOCService.BatchGetObjectRemote "
                    "e2e_us=3000 server_exec_us=1000 network_residual_us=2000 "
                    "cntl_error_code=1008 cntl_failed=1"
                ),
            },
        ]
    )
    summary["traces"]["degraded-events"] = degraded
    summary["trace_count"] += 1
    summary_path.write_text(json.dumps(summary), encoding="utf-8")

    correlation = load_module().build_analysis(run_dir, top_n=100, deadline_ms=20)["aggregate"][
        "worker_correlation"
    ]

    assert correlation["unassigned_event_count"] == 1
    assert correlation["untimed_event_count"] == 1


def test_rpc_dimension_views_do_not_self_correlate(run_dir: Path):
    summary_path = run_dir / "summary.json"
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    isolated = trace(
        "isolated-query-meta",
        9,
        6,
        timestamp="2026-08-15T10:00:20.000000",
        worker="worker-isolated",
    )
    isolated["evidence"].append(
        {
            "source": "fixture.log",
            "member": "isolated-query-meta",
            "line": 2,
            "worker": "worker-isolated",
            "host_ip": "",
            "text": (
                "2026-08-15T10:00:20.100000 method=datasystem.MasterService.QueryMeta "
                "e2e_us=3000 cntl_error_code=1008 cntl_failed=1"
            ),
        }
    )
    summary["traces"]["isolated-query-meta"] = isolated
    summary["trace_count"] += 1
    summary_path.write_text(json.dumps(summary), encoding="utf-8")

    correlation = load_module().build_analysis(run_dir, top_n=100, deadline_ms=20)["aggregate"][
        "worker_correlation"
    ]
    failure = next(
        event
        for event in correlation["events"]
        if event["trace_id"] == "isolated-query-meta" and event["kind"] == "query_meta"
    )

    assert failure["companions"]["same_trace_event_count"] == 0
    assert failure["companions"]["relation"] == "no_companion_evidence"


def test_successful_nearby_rpc_does_not_become_direct_problem_evidence(run_dir: Path):
    summary_path = run_dir / "summary.json"
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    mixed = trace(
        "query-meta-with-success",
        9,
        6,
        timestamp="2026-08-15T10:00:20.000000",
        worker="worker-mixed",
    )
    mixed["evidence"].extend(
        [
            {
                "source": "fixture.log",
                "member": "query-meta-with-success",
                "line": 2,
                "worker": "worker-mixed",
                "host_ip": "",
                "text": (
                    "2026-08-15T10:00:20.100000 method=datasystem.MasterService.QueryMeta "
                    "e2e_us=3000 cntl_error_code=1008 cntl_failed=1"
                ),
            },
            {
                "source": "fixture.log",
                "member": "query-meta-with-success",
                "line": 3,
                "worker": "worker-mixed",
                "host_ip": "",
                "text": (
                    "2026-08-15T10:00:20.200000 method=datasystem.WorkerOCService.Get "
                    "e2e_us=500 server_exec_us=300 network_residual_us=200 "
                    "cntl_error_code=0 cntl_failed=0"
                ),
            },
        ]
    )
    summary["traces"]["query-meta-with-success"] = mixed
    summary["trace_count"] += 1
    summary_path.write_text(json.dumps(summary), encoding="utf-8")

    correlation = load_module().build_analysis(run_dir, top_n=100, deadline_ms=20)["aggregate"][
        "worker_correlation"
    ]
    failure = next(
        event
        for event in correlation["events"]
        if event["trace_id"] == "query-meta-with-success" and event["kind"] == "query_meta"
    )

    assert failure["companions"]["same_trace_event_count"] == 0
    assert failure["companions"]["relation"] == "no_companion_evidence"


def test_explicit_slow_rpc_marker_is_problem_companion(run_dir: Path):
    summary_path = run_dir / "summary.json"
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    mixed = trace(
        "query-meta-with-slow-rpc",
        12,
        9,
        timestamp="2026-08-15T10:00:24.000000",
        worker="worker-slow-rpc",
    )
    mixed["evidence"].extend(
        [
            {
                "source": "fixture.log",
                "member": "query-meta-with-slow-rpc",
                "line": 2,
                "worker": "worker-slow-rpc",
                "host_ip": "",
                "text": (
                    "2026-08-15T10:00:24.100000 method=datasystem.MasterService.QueryMeta "
                    "e2e_us=3000 cntl_error_code=1008 cntl_failed=1"
                ),
            },
            {
                "source": "fixture.log",
                "member": "query-meta-with-slow-rpc",
                "line": 3,
                "worker": "worker-slow-rpc",
                "host_ip": "",
                "text": (
                    "2026-08-15T10:00:24.200000 [BRPC_RPC_FRAMEWORK_SLOW] "
                    "method=datasystem.WorkerOCService.Get e2e_us=9000 "
                    "server_exec_us=5000 network_residual_us=4000 "
                    "cntl_error_code=0 cntl_failed=0"
                ),
            },
        ]
    )
    summary["traces"]["query-meta-with-slow-rpc"] = mixed
    summary["trace_count"] += 1
    summary_path.write_text(json.dumps(summary), encoding="utf-8")

    correlation = load_module().build_analysis(run_dir, top_n=100, deadline_ms=20)["aggregate"][
        "worker_correlation"
    ]
    failure = next(
        event
        for event in correlation["events"]
        if event["trace_id"] == "query-meta-with-slow-rpc" and event["kind"] == "query_meta"
    )
    slow_rpc = next(
        event
        for event in correlation["events"]
        if event["trace_id"] == "query-meta-with-slow-rpc" and event["kind"] == "rpc"
        and event["method"].endswith("WorkerOCService.Get")
    )

    assert slow_rpc["is_slow"]
    assert failure["companions"]["same_trace_event_count"] == 1
    assert failure["companions"]["relation"] == "direct_same_trace"


def test_missing_rpc_breakdown_fields_remain_unobserved(run_dir: Path):
    summary_path = run_dir / "summary.json"
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    partial = trace(
        "partial-query-meta",
        9,
        6,
        timestamp="2026-08-15T10:00:21.000000",
        worker="worker-partial",
    )
    partial["evidence"].append(
        {
            "source": "fixture.log",
            "member": "partial-query-meta",
            "line": 2,
            "worker": "worker-partial",
            "host_ip": "",
            "text": (
                "2026-08-15T10:00:21.100000 method=datasystem.MasterService.QueryMeta "
                "e2e_us=3000 cntl_error_code=1008 cntl_failed=1"
            ),
        }
    )
    summary["traces"]["partial-query-meta"] = partial
    summary["trace_count"] += 1
    summary_path.write_text(json.dumps(summary), encoding="utf-8")

    correlation = load_module().build_analysis(run_dir, top_n=100, deadline_ms=20)["aggregate"][
        "worker_correlation"
    ]
    event = next(
        item
        for item in correlation["events"]
        if item["trace_id"] == "partial-query-meta" and item["kind"] == "query_meta"
    )
    bucket = next(
        item
        for item in correlation["time_buckets"]
        if item["worker"] == "worker-partial" and item["second"] == "2026-08-15T10:00:21"
    )

    assert event["network_ms"] is None
    assert event["server_ms"] is None
    assert event["queue_ms"] is None
    assert bucket["rpc"]["network_ms"]["count"] == 0
    assert bucket["rpc"]["server_ms"]["count"] == 0
    assert bucket["rpc"]["queue_ms"]["count"] == 0


def test_worker_correlation_preserves_repeated_lines_at_distinct_times(run_dir: Path):
    summary_path = run_dir / "summary.json"
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    retries = trace(
        "repeated-query-meta",
        9,
        6,
        timestamp="2026-08-15T10:00:22.000000",
        worker="worker-repeated",
    )
    for line, timestamp in enumerate(
        ("2026-08-15T10:00:22.100000", "2026-08-15T10:00:23.100000"), start=2
    ):
        retries["evidence"].append(
            {
                "source": "fixture.log",
                "member": "repeated-query-meta",
                "line": line,
                "worker": "worker-repeated",
                "host_ip": "",
                "text": (
                    f"{timestamp} | method=datasystem.MasterService.QueryMeta "
                    "e2e_us=3000 cntl_error_code=1008 cntl_failed=1"
                ),
            }
        )
    summary["traces"]["repeated-query-meta"] = retries
    summary["trace_count"] += 1
    summary_path.write_text(json.dumps(summary), encoding="utf-8")

    correlation = load_module().build_analysis(run_dir, top_n=100, deadline_ms=20)["aggregate"][
        "worker_correlation"
    ]
    events = [
        item
        for item in correlation["events"]
        if item["trace_id"] == "repeated-query-meta" and item["kind"] == "query_meta"
    ]

    assert [item["timestamp"] for item in events] == [
        "2026-08-15T10:00:22.100000",
        "2026-08-15T10:00:23.100000",
    ]


def test_query_and_get_has_time_worker_analysis_and_unclosed_failure_boundary(run_dir: Path):
    summary_path = run_dir / "summary.json"
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    for index, timestamp in enumerate(
        ("2026-08-15T10:00:30.100000", "2026-08-15T10:00:30.200000"), start=1
    ):
        trace_id = f"query-and-get-{index}"
        item = trace(
            trace_id,
            20.4,
            0,
            timestamp=timestamp,
            query_meta_ms=20.2,
            worker=f"client-node-{index}",
            status=1001,
        )
        item["latency_summary_us"] = {
            "client.process.direct_route": 2,
            "client.rpc.direct_query_and_get": 20200,
            "client.rpc.direct_get_data": 0,
            "client.process.direct_materialize": 0,
            "client.process.get": 198,
        }
        item["evidence"].append(
            {
                "source": "client.log",
                "member": trace_id,
                "line": 2,
                "worker": f"client-node-{index}",
                "host_ip": "",
                "text": (
                    f"{timestamp} [BRPC_RPC_FRAMEWORK_SLOW] "
                    "method=datasystem.master.MasterOCService.QueryAndGet "
                    "e2e_us=20150 remote_processing_us=20150 server_req_queue_us=0 "
                    "server_exec_us=0 network_residual_us=0 cntl_error_code=1008 "
                    "cntl_failed=1 resp_attachment_bytes=0"
                ),
            }
        )
        if index == 1:
            item["evidence"].extend(
                [
                    {
                        "source": "client.log",
                        "member": trace_id,
                        "line": 3,
                        "worker": f"client-node-{index}",
                        "host_ip": "",
                        "text": f"{timestamp} BrpcChannel created: 192.0.2.10:31501 timeout=20ms connect_timeout=1000ms",
                    },
                    {
                        "source": "client.log",
                        "member": trace_id,
                        "line": 4,
                        "worker": f"client-node-{index}",
                        "host_ip": "",
                        "text": f"{timestamp} [TransportGet][Metadata] meta owner: 192.0.2.10:31501, status: RPC deadline exceeded",
                    },
                ]
            )
        summary["traces"][trace_id] = item
        summary["trace_count"] += 1
    summary_path.write_text(json.dumps(summary), encoding="utf-8")

    mod = load_module()
    analysis = mod.build_analysis(run_dir, top_n=100, deadline_ms=20, local_cache=False)
    query = analysis["aggregate"]["query_meta_analysis"]
    correlation = analysis["aggregate"]["worker_correlation"]

    assert query["trace_count"] >= 2
    assert query["failed_count"] >= 2
    assert sum(query["detail_counts"].values()) == query["trace_count"]
    assert query["failure_reasons"]["QueryMeta RPC deadline"] >= 2
    second = next(item for item in query["time_buckets"] if item["second"] == "2026-08-15T10:00:30")
    assert second["trace_count"] == 2
    assert second["failed_count"] == 2
    assert {item["initiator"] for item in query["initiators"]} >= {"client-node-1", "client-node-2"}
    assert query["meta_target_coverage"] == "present"
    assert query["meta_targets"] == [{"target": "192.0.2.10:31501", "trace_count": 1}]
    assert "server trailer" in query["root_cause_boundary"]
    timeout_flow = query["timeout_flow"]
    assert timeout_flow["timeout_count"] >= 2
    assert timeout_flow["full_window_count"] >= 2
    assert timeout_flow["retry_budget_count"] == 0
    assert timeout_flow["empty_response_count"] >= 2
    assert timeout_flow["server_timing_unavailable_count"] >= 2
    assert timeout_flow["urma_not_observed_count"] >= 2
    assert timeout_flow["new_channel_count"] >= 1
    assert timeout_flow["distinct_initiator_count"] >= 2
    assert timeout_flow["distinct_target_count"] >= 1
    assert timeout_flow["dominant_second"]["trace_count"] >= 2
    assert "ObjectReadFlow::Resolve" in timeout_flow["confirmed_flow"]
    assert "不能确认" in timeout_flow["root_cause_status"]
    query_events = [event for event in correlation["events"] if event["kind"] == "query_meta"]
    assert any(event["method"].endswith("MasterOCService.QueryAndGet") for event in query_events)
    assert all(event["component_scope"] == "Client发起QueryMeta；Meta Owner目标未观测" for event in query_events)

    html_text = mod.render_html(analysis, "QueryMeta analysis")
    for marker in (
        "QueryMeta 根因分析",
        "query-meta-detail-chart",
        "query-meta-time-chart",
        "query-meta-worker-chart",
        "query-meta-target-chart",
        "QueryAndGet 超时流程定界",
        "高概率共同机制",
        "correlation-category-filter",
        "correlation-status-filter",
        "correlation-relation-filter",
        "correlation-latency-band-filter",
        "correlation-time-start",
        "correlation-time-end",
        "function filteredCorrelationEvents()",
        "function buildCorrelationBuckets(events)",
    ):
        assert marker in html_text


def test_build_analysis_selects_topn_and_bounds_exclusive_stages(run_dir: Path):
    mod = load_module()
    analysis = mod.build_analysis(run_dir, top_n=3, deadline_ms=20.0)
    assert analysis["schema_version"] == 1
    assert [row["trace_id"] for row in analysis["traces"]] == ["rpc-trace", "urma-trace", "local-trace"]
    assert sum(analysis["problem_summary"].values()) == 3
    assert analysis["traces"][0]["primary_problem"] == "RPC网络"
    assert analysis["traces"][1]["primary_problem"] == "URMA"
    assert analysis["traces"][2]["primary_problem"] == "数据访问父窗口/未细分"
    for row in analysis["traces"]:
        assert sum(row["attribution_ms"].values()) == pytest.approx(row["client_ms"], abs=1e-6)
        assert all(value >= 0 for value in row["attribution_ms"].values())


def test_focus_breakdown_separates_connect_communication_business_sched_network_and_framework():
    mod = load_module()
    row = {
        "attribution_ms": {
            "RPC网络": 1.0,
            "RPC排队": 0.2,
            "QueryMeta": 5.0,
            "URMA超时等待": 0.0,
            "URMA": 4.0,
            "远端供数处理": 2.0,
            "数据访问父窗口/未细分": 3.0,
            "未解释残差": 4.8,
        },
        "evidence": [
            "[TransportGet] phasesUs={urma_connect_info_exchange:2000,"
            "urma_connection_finalize:500,connection_write_lock_wait:300}",
            "[BRPC_RPC_FRAMEWORK_SLOW] method=WorkerWorkerExchangeUrmaConnectInfo "
            "e2e_us=2000 server_req_queue_us=100 server_exec_us=800 network_residual_us=500",
            "[BRPC_RPC_FRAMEWORK_SLOW] method=WorkerOCService.QueryAndGet "
            "e2e_us=5000 server_req_queue_us=200 server_exec_us=3000 network_residual_us=1000",
            "[BRPC_RPC_FRAMEWORK_SLOW] method=WorkerWorkerOCService.GetObjectRemote "
            "e2e_us=3000 server_req_queue_us=100 server_exec_us=1500 network_residual_us=700",
        ],
        "urma_requests": [
            {
                "wake_sched_latency_ms": 0.4,
                "thread_sched_ms": 0.3,
                "notify_to_awake_ms": 0.2,
            }
        ],
        "error_family": None,
    }

    mod._apply_focus_breakdown(row)

    assert row["focus_breakdown_ms"] == pytest.approx(
        {
            "URMA建链": 1.3,
            "URMA通信": 3.6,
            "URMA调度/线程开销": 0.4,
            "QueryAndGet其他业务": 4.2,
            "Get其他业务": 1.4,
            "其他调度/线程开销": 0.7,
            "RPC网络相关": 1.5,
            "RPC框架": 2.1,
            "未解释残差": 4.8,
        }
    )
    assert sum(row["focus_breakdown_ms"].values()) == pytest.approx(20.0)
    assert row["focus_primary_stage"] == "未解释残差"


def test_focus_breakdown_does_not_treat_urma_wait_to_poll_as_thread_scheduling():
    mod = load_module()
    row = {
        "attribution_ms": {
            "RPC网络": 0.0,
            "RPC排队": 0.0,
            "QueryMeta": 0.0,
            "URMA超时等待": 0.0,
            "URMA": 13.441,
            "远端供数处理": 0.0,
            "数据访问父窗口/未细分": 0.0,
            "未解释残差": 0.0,
        },
        "evidence": [],
        "urma_requests": [
            {
                "request_id": "slowest-wr",
                "wait_to_poll_ms": 13.393,
                "notify_to_awake_ms": 0.007,
                "wake_sched_latency_ms": 0.007,
                "thread_sched_ms": None,
            },
            {
                "request_id": "faster-wr",
                "wait_to_poll_ms": None,
                "notify_to_awake_ms": 0.5,
                "wake_sched_latency_ms": 0.5,
                "thread_sched_ms": None,
            },
        ],
        "urma_logical_writes": [
            {
                "wall_clock_ms": 13.441,
                "slowest_wr_ms": 13.426,
                "complete": True,
            }
        ],
        "urma_trace": {"slowest_request_id": "slowest-wr", "slowest_total_ms": 13.426},
        "error_family": None,
    }

    mod._apply_focus_breakdown(row)

    assert row["focus_breakdown_ms"]["URMA通信"] == pytest.approx(13.419)
    assert row["focus_breakdown_ms"]["URMA调度/线程开销"] == pytest.approx(0.007)
    assert row["focus_breakdown_ms"]["其他调度/线程开销"] == 0.0
    assert row["urma_scheduling_detail_ms"] == {
        "wake_sched_latency": pytest.approx(0.007),
        "thread_sched": None,
        "notify_to_awake": pytest.approx(0.007),
        "poll_jfc": None,
        "notify": None,
    }
    assert row["focus_breakdown_ms"]["未解释残差"] == pytest.approx(0.015)


def test_focus_breakdown_separates_outer_and_nested_rpc_framework_windows():
    mod = load_module()
    row = {
        "attribution_ms": {
            "RPC网络": 1.5,
            "RPC排队": 0.2,
            "QueryMeta": 0.0,
            "URMA超时等待": 0.0,
            "URMA": 0.0,
            "远端供数处理": 2.0,
            "数据访问父窗口/未细分": 4.0,
            "未解释残差": 3.0,
        },
        "evidence": [
            "[BRPC_RPC_FRAMEWORK_SLOW] method=datasystem.WorkerOCService.Get "
            "e2e_us=10000 server_req_queue_us=200 server_exec_us=8000 "
            "network_residual_us=1000 cntl_error_code=0 cntl_failed=0",
            "[BRPC_RPC_FRAMEWORK_SLOW] method=datasystem.WorkerWorkerOCService.GetObjectRemote "
            "e2e_us=3000 server_req_queue_us=100 server_exec_us=2000 "
            "network_residual_us=500 cntl_error_code=0 cntl_failed=0",
        ],
        "urma_requests": [],
        "error_family": None,
    }

    mod._apply_focus_breakdown(row)

    assert row["focus_breakdown_ms"]["RPC框架"] == pytest.approx(1.2)
    assert row["focus_breakdown_ms"]["其他调度/线程开销"] == pytest.approx(0.3)
    assert row["focus_breakdown_ms"]["Get其他业务"] == pytest.approx(5.5)
    assert row["focus_breakdown_ms"]["未解释残差"] == pytest.approx(2.2)
    assert sum(row["focus_breakdown_ms"].values()) == pytest.approx(10.7)


def test_focus_breakdown_keeps_failed_rpc_without_server_trailer_unexplained():
    mod = load_module()
    row = {
        "attribution_ms": {
            "RPC网络": 0.0,
            "RPC排队": 0.0,
            "QueryMeta": 0.0,
            "URMA超时等待": 0.0,
            "URMA": 0.0,
            "远端供数处理": 0.0,
            "数据访问父窗口/未细分": 0.0,
            "未解释残差": 20.0,
        },
        "evidence": [
            "[BRPC_RPC_FRAMEWORK_SLOW] method=WorkerOCService.QueryAndGet "
            "e2e_us=20000 server_req_queue_us=0 server_exec_us=0 "
            "network_residual_us=0 cntl_error_code=1008 cntl_failed=1"
        ],
        "urma_requests": [],
        "error_family": "RPC截止超时",
    }

    mod._apply_focus_breakdown(row)

    assert row["focus_breakdown_ms"]["RPC框架"] == 0.0
    assert row["focus_breakdown_ms"]["未解释残差"] == 20.0


def test_focus_breakdown_does_not_infer_framework_without_complete_rpc_timing():
    mod = load_module()
    row = {
        "attribution_ms": {
            "RPC网络": 1.0,
            "RPC排队": 0.0,
            "QueryMeta": 0.0,
            "URMA超时等待": 0.0,
            "URMA": 0.0,
            "远端供数处理": 0.0,
            "数据访问父窗口/未细分": 0.0,
            "未解释残差": 9.0,
        },
        "evidence": [
            "[BRPC_RPC_FRAMEWORK_SLOW] method=datasystem.WorkerOCService.Get "
            "e2e_us=10000 network_residual_us=1000 cntl_error_code=0 cntl_failed=0"
        ],
        "urma_requests": [],
        "error_family": None,
    }

    mod._apply_focus_breakdown(row)

    assert row["focus_breakdown_ms"]["RPC框架"] == 0.0
    assert row["focus_breakdown_ms"]["未解释残差"] == 9.0


def test_latency_segments_follow_report_reference_bands_and_exclude_sub_5ms():
    mod = load_module()
    latencies = [4.9, 5.0, 5.999, 6.0, 6.999, 7.0, 9.999, 10.0, 20.0, 20.001, 35.0]
    rows = [
        {
            "trace_id": f"trace-{index:04d}",
            "timestamp": f"2026-08-15T10:00:00.{index:06d}",
            "failed": index % 17 == 0,
            "client_ms": latency,
            "primary_problem": (
                "URMA超时"
                if mod.STAGE_NAMES[index % len(mod.STAGE_NAMES)] == "URMA超时等待"
                else mod.STAGE_NAMES[index % len(mod.STAGE_NAMES)]
            ),
        }
        for index, latency in enumerate(latencies)
    ]
    rows[1]["focus_primary_problem"] = "RPC框架"

    segments = mod._build_latency_segments(rows)

    assert [segment["label"] for segment in segments] == ["5–6ms", "6–7ms", "7–10ms", "10–20ms", ">20ms"]
    assert [segment["trace_count"] for segment in segments] == [2, 2, 2, 2, 2]
    assert [segment["segment_id"] for segment in segments] == [1, 2, 3, 4, 5]
    assert "trace-0000" not in {trace_id for segment in segments for trace_id in segment["trace_ids"]}
    assert "trace-0008" in segments[3]["trace_ids"]
    assert "trace-0009" in segments[4]["trace_ids"]
    assert all(sum(segment["problem_counts"].values()) == segment["trace_count"] for segment in segments)
    assert segments[0]["dominant_problem"] == "RPC框架"
    assert segments[0]["problem_counts"]["RPC框架"] == 1


def test_nested_batch_network_is_counted_once_and_attribution_closes(run_dir: Path):
    summary_path = run_dir / "summary.json"
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    batch_trace = trace(
        "batch-network-trace",
        20,
        10,
        timestamp="2026-08-15T10:00:04.000001",
        batch_network_ms=3,
        local_ms=10,
    )
    batch_trace["rpc_slow"] = {"datasystem.WorkerOCService.BatchGetObjectRemote": 1}
    batch_trace["rpc_slow_fields_us"] = {"network_residual_us": metric(3000)}
    summary["traces"]["batch-network-trace"] = batch_trace
    summary["trace_count"] += 1
    summary_path.write_text(json.dumps(summary), encoding="utf-8")

    mod = load_module()
    analysis = mod.build_analysis(run_dir, top_n=100, deadline_ms=20)
    row = next(item for item in analysis["traces"] if item["trace_id"] == "batch-network-trace")

    assert row["attribution_ms"]["RPC网络"] == pytest.approx(3.0)
    assert sum(row["attribution_ms"].values()) == pytest.approx(row["client_ms"], abs=1e-6)


def test_direct_read_latency_phases_close_local_cache_false_budget(run_dir: Path):
    summary_path = run_dir / "summary.json"
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    direct = trace(
        "direct-read-phases",
        4,
        0,
        timestamp="2026-08-15T10:00:05.000001",
        urma_ms=2,
        worker="data-worker-direct",
    )
    direct["latency_summary_us"] = {
        "client.process.direct_route": 10,
        "client.rpc.direct_query_and_get": 200,
        "client.rpc.direct_get_data": 3600,
        "client.process.direct_materialize": 20,
        "client.process.get": 170,
    }
    summary["traces"]["direct-read-phases"] = direct
    summary["trace_count"] += 1
    summary_path.write_text(json.dumps(summary), encoding="utf-8")

    analysis = load_module().build_analysis(run_dir, top_n=100, deadline_ms=20, local_cache=False)
    row = next(item for item in analysis["traces"] if item["trace_id"] == "direct-read-phases")

    assert row["query_meta_ms"] == pytest.approx(0.2)
    assert row["attribution_ms"]["URMA"] == pytest.approx(2.0)
    assert row["attribution_ms"]["数据访问父窗口/未细分"] == pytest.approx(1.8)
    assert "直连Worker本地/未细分" not in row["attribution_ms"]
    assert row["attribution_ms"]["未解释残差"] == pytest.approx(0.0)
    assert sum(row["attribution_ms"].values()) == pytest.approx(row["client_ms"], abs=1e-6)


def test_direct_read_with_closed_provider_and_urma_windows_is_classified_as_urma_slow(run_dir: Path):
    summary_path = run_dir / "summary.json"
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    item = trace(
        "closed-urma-read",
        6.594,
        0,
        timestamp="2026-08-15T10:00:05.010001",
        query_meta_ms=1.206,
        worker="data-worker-urma",
    )
    item["latency_summary_us"] = {
        "client.rpc.direct_query_and_get": 1206,
        "client.rpc.direct_get_data": 5364,
        "client.process.get": 24,
    }
    item["ub_events"] = [
        chunked_urma_event(
            "2026-08-15T10:00:05.010001",
            "data-worker-urma",
            5.086,
            "wr-1",
            1,
            2,
            post_us=100000,
            observed_us=105086,
        ),
        chunked_urma_event(
            "2026-08-15T10:00:05.010071",
            "data-worker-urma",
            1.622,
            "wr-2",
            2,
            2,
            post_us=100010,
            observed_us=105101,
        ),
    ]
    item["urma_elapsed_ms"] = {
        "total": {"count": 2, "min": 1.622, "p50": 3.354, "p90": 4.74, "p99": 5.051, "max": 5.086}
    }
    item["evidence_coverage"]["urma"] = "present"
    item["evidence"].extend(
        [
            {
                "source": "client.log",
                "member": "closed-urma-read",
                "line": 2,
                "worker": "client-a",
                "host_ip": "",
                "text": "2026-08-15T10:00:05.010001 [TransportGet] Phase latency, phasesUs={data_transfer:5346}",
            },
            {
                "source": "worker.log",
                "member": "closed-urma-read",
                "line": 3,
                "worker": "data-worker-urma",
                "host_ip": "",
                "text": "2026-08-15T10:00:05.010001 Processing pull object[obj] hasUrmaInfo[1], cost: 5.132ms",
            },
            {
                "source": "worker.log",
                "member": "closed-urma-read",
                "line": 4,
                "worker": "data-worker-urma",
                "host_ip": "",
                "text": "2026-08-15T10:00:05.010001 [GetObjectRemote] finish, requestTransport: UB, cost: 5.164ms",
            },
        ]
    )
    summary["traces"]["closed-urma-read"] = item
    summary_path.write_text(json.dumps(summary), encoding="utf-8")

    analysis = load_module().build_analysis(run_dir, top_n=100, deadline_ms=20, local_cache=False)
    row = next(value for value in analysis["traces"] if value["trace_id"] == "closed-urma-read")

    assert row["data_access_scope"] == "URMA慢完成"
    assert row["access_path_breakdown"]["client_data_transfer_ms"] == pytest.approx(5.346)
    assert row["access_path_breakdown"]["provider_pull_ms"] == pytest.approx(5.132)
    assert row["access_path_breakdown"]["provider_finish_ms"] == pytest.approx(5.164)
    assert row["access_path_breakdown"]["closure_ratio_pct"] >= 90
    assert "最慢URMA Elapsed Time" in row["data_access_evidence"]


def test_worker_access_parent_contains_urma_when_worker_process_get_is_leaf(run_dir: Path):
    summary_path = run_dir / "summary.json"
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    item = trace(
        "worker-parent-with-urma",
        19.088,
        18.998,
        timestamp="2026-08-31T15:01:05.621610",
        urma_ms=18.902,
        worker="data-worker",
    )
    item["latency_summary_us"] = {
        "client.rpc.get": 89,
        "worker.urma.urma_total": 18_959,
        "worker.process.get": 5,
        "client.process.get": 33,
    }
    summary["traces"]["worker-parent-with-urma"] = item
    summary["trace_count"] += 1
    summary_path.write_text(json.dumps(summary), encoding="utf-8")

    row = next(
        value
        for value in load_module().build_analysis(
            run_dir, top_n=100, deadline_ms=20, local_cache=False
        )["traces"]
        if value["trace_id"] == "worker-parent-with-urma"
    )

    assert row["worker_process_ms"] == pytest.approx(18.998)
    assert row["access_path_breakdown"]["data_parent_ms"] == pytest.approx(18.998)
    assert row["access_path_breakdown"]["closure_ratio_pct"] == pytest.approx(99.495, abs=0.01)
    assert row["data_access_scope"] == "URMA慢完成"
    assert row["focus_breakdown_ms"]["URMA通信"] == pytest.approx(18.902)
    assert row["focus_breakdown_ms"]["Get其他业务"] == pytest.approx(0.096)
    assert row["focus_breakdown_ms"]["未解释残差"] == pytest.approx(0.09)


def test_client_direct_pipeline_remains_parent_when_larger_than_worker_window(run_dir: Path):
    summary_path = run_dir / "summary.json"
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    item = trace(
        "client-direct-parent",
        16.0,
        10.0,
        timestamp="2026-08-31T15:01:06.000000",
        worker="data-worker",
    )
    item["latency_summary_us"] = {
        "client.process.get": 15_000,
        "worker.process.get": 5_000,
    }
    summary["traces"]["client-direct-parent"] = item
    summary["trace_count"] += 1
    summary_path.write_text(json.dumps(summary), encoding="utf-8")

    row = next(
        value
        for value in load_module().build_analysis(run_dir, top_n=100, deadline_ms=20)["traces"]
        if value["trace_id"] == "client-direct-parent"
    )

    assert row["worker_process_ms"] == pytest.approx(15.0)
    assert row["focus_breakdown_ms"]["Get其他业务"] == pytest.approx(15.0)
    assert row["focus_breakdown_ms"]["未解释残差"] == pytest.approx(1.0)


def test_provider_pull_dominates_when_urma_is_fast(run_dir: Path):
    summary_path = run_dir / "summary.json"
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    item = trace(
        "provider-pull-slow",
        6.450,
        0,
        timestamp="2026-08-15T10:00:05.020001",
        query_meta_ms=0.144,
        urma_ms=0.137,
        worker="data-worker-provider",
    )
    item["latency_summary_us"] = {
        "client.rpc.direct_query_and_get": 144,
        "client.rpc.direct_get_data": 6245,
        "client.process.get": 61,
    }
    item["evidence"].extend(
        [
            {
                "source": "client.log",
                "member": "provider-pull-slow",
                "line": 2,
                "worker": "client-a",
                "host_ip": "",
                "text": "2026-08-15T10:00:05.020001 [TransportGet] Phase latency, phasesUs={data_transfer:6245}",
            },
            {
                "source": "worker.log",
                "member": "provider-pull-slow",
                "line": 3,
                "worker": "data-worker-provider",
                "host_ip": "",
                "text": "2026-08-15T10:00:05.020001 Processing pull object[obj] hasUrmaInfo[1], cost: 6.109ms",
            },
            {
                "source": "worker.log",
                "member": "provider-pull-slow",
                "line": 4,
                "worker": "data-worker-provider",
                "host_ip": "",
                "text": "2026-08-15T10:00:05.020001 [GetObjectRemote] finish, requestTransport: UB, cost: 6.139ms",
            },
        ]
    )
    summary["traces"]["provider-pull-slow"] = item
    summary_path.write_text(json.dumps(summary), encoding="utf-8")

    analysis = load_module().build_analysis(run_dir, top_n=100, deadline_ms=20, local_cache=False)
    row = next(value for value in analysis["traces"] if value["trace_id"] == "provider-pull-slow")

    assert row["data_access_scope"] == "Data Worker供数处理慢"
    assert row["primary_problem"] == "远端供数处理"
    assert "6.109ms" in row["data_access_evidence"]
    assert "URMA" in row["data_access_evidence"]
    assert row["non_transport_analysis"]["deep_category"] == "Data Worker供数处理慢"
    assert "Processing pull 6.109ms" in row["non_transport_analysis"]["conclusion"]


def test_outer_get_server_queue_is_named_instead_of_shm_parent_window(run_dir: Path):
    summary_path = run_dir / "summary.json"
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    item = trace(
        "outer-rpc-queue",
        8.791,
        8.614,
        timestamp="2026-08-15T10:00:05.030001",
        worker="worker-queue",
    )
    item["evidence"].extend(
        [
            {
                "source": "client.log",
                "member": "outer-rpc-queue",
                "line": 2,
                "worker": "client-a",
                "host_ip": "",
                "text": "2026-08-15T10:00:05.030001 | trace | 0 | DS_KV_CLIENT_GET | 8791 | 8388608 | transportType: SHM",
            },
            {
                "source": "client.log",
                "member": "outer-rpc-queue",
                "line": 3,
                "worker": "client-a",
                "host_ip": "",
                "text": "2026-08-15T10:00:05.030001 [BRPC_RPC_FRAMEWORK_SLOW] method=datasystem.WorkerOCService.Get e2e_us=8730 server_req_queue_us=8597 server_exec_us=22 network_residual_us=106 cntl_error_code=0 cntl_failed=0",
            },
        ]
    )
    item["rpc_slow"] = {"datasystem.WorkerOCService.Get": 1}
    summary["traces"]["outer-rpc-queue"] = item
    summary_path.write_text(json.dumps(summary), encoding="utf-8")

    analysis = load_module().build_analysis(run_dir, top_n=100, deadline_ms=20, local_cache=False)
    row = next(value for value in analysis["traces"] if value["trace_id"] == "outer-rpc-queue")

    assert row["data_access_scope"] == "Client→Worker RPC排队慢"
    assert "server_req_queue 8.597ms" in row["data_access_evidence"]
    assert "SHM拷贝" in row["data_access_evidence"]


def test_outer_get_network_is_named_separately_from_final_shm_delivery(run_dir: Path):
    summary_path = run_dir / "summary.json"
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    item = trace(
        "outer-rpc-network",
        7.272,
        0,
        timestamp="2026-08-15T10:00:05.040001",
        worker="worker-network",
    )
    item["latency_summary_us"] = {"client.rpc.get": 7173, "client.process.get": 93}
    item["evidence"].extend(
        [
            {
                "source": "client.log",
                "member": "outer-rpc-network",
                "line": 2,
                "worker": "client-a",
                "host_ip": "",
                "text": "2026-08-15T10:00:05.040001 | trace | 0 | DS_KV_CLIENT_GET | 7272 | 1835008 | transportType:SHM",
            },
            {
                "source": "client.log",
                "member": "outer-rpc-network",
                "line": 3,
                "worker": "client-a",
                "host_ip": "",
                "text": "2026-08-15T10:00:05.040001 [BRPC_RPC_FRAMEWORK_SLOW] method=datasystem.WorkerOCService.Get e2e_us=7196 server_req_queue_us=6 server_exec_us=14 network_residual_us=7172 cntl_error_code=0 cntl_failed=0",
            },
        ]
    )
    item["rpc_slow"] = {"datasystem.WorkerOCService.Get": 1}
    summary["traces"]["outer-rpc-network"] = item
    summary_path.write_text(json.dumps(summary), encoding="utf-8")

    analysis = load_module().build_analysis(run_dir, top_n=100, deadline_ms=20, local_cache=False)
    row = next(value for value in analysis["traces"] if value["trace_id"] == "outer-rpc-network")

    assert row["data_access_scope"] == "Client→Worker RPC网络慢"
    assert "network residual 7.172ms" in row["data_access_evidence"]
    assert "最终SHM交付" in row["data_access_evidence"]


def test_single_getobjectremote_is_client_to_data_worker_rpc_not_worker_parent(run_dir: Path):
    summary_path = run_dir / "summary.json"
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    direct = trace(
        "single-direct-get",
        11.249,
        0,
        timestamp="2026-08-15T10:00:05.100001",
        urma_ms=0.086,
        worker="data-worker-direct",
    )
    direct["latency_summary_us"] = {
        "client.process.direct_route": 2,
        "client.rpc.direct_query_and_get": 132,
        "client.rpc.direct_get_data": 11086,
        "client.process.direct_materialize": 12,
        "client.process.get": 17,
    }
    direct["evidence"].append(
        {
            "source": "client.log",
            "member": "single-direct-get",
            "line": 2,
            "worker": "client-a",
            "host_ip": "",
            "text": (
                "2026-08-15T10:00:05.100001 [BRPC_RPC_FRAMEWORK_SLOW] "
                "method=datasystem.WorkerWorkerOCService.GetObjectRemote "
                "e2e_us=11026 remote_processing_us=11024 server_req_queue_us=2 "
                "server_exec_us=179 network_residual_us=10841 cntl_error_code=0 cntl_failed=0"
            ),
        }
    )
    summary["traces"]["single-direct-get"] = direct
    summary["trace_count"] += 1
    summary_path.write_text(json.dumps(summary), encoding="utf-8")

    mod = load_module()
    analysis = mod.build_analysis(run_dir, top_n=100, deadline_ms=20, local_cache=False)
    row = next(item for item in analysis["traces"] if item["trace_id"] == "single-direct-get")

    assert row["data_rpc_observed"] is True
    assert row["data_rpc_e2e_ms"] == pytest.approx(11.026)
    assert row["data_rpc_network_ms"] == pytest.approx(10.841)
    assert row["data_rpc_server_ms"] == pytest.approx(0.179)
    assert row["attribution_ms"]["RPC网络"] == pytest.approx(10.841)
    assert row["data_access_scope"] == "Client→Data Worker RPC网络慢"
    assert "Client发起GetObjectRemote" in row["data_access_evidence"]
    assert "Worker ProcessGet" not in row["data_access_evidence"]


def test_chunked_urma_write_keeps_two_wrs_and_uses_slowest_elapsed_time(run_dir: Path):
    summary_path = run_dir / "summary.json"
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    item = trace(
        "chunked-write",
        8,
        0,
        timestamp="2026-08-15T10:00:06.000001",
        worker="data-worker-chunk",
    )
    item["ub_events"] = [
        chunked_urma_event(
            "2026-08-15T10:00:06.000001",
            "data-worker-chunk",
            0.138,
            "131139",
            1,
            2,
            post_us=158953756113,
            observed_us=158953756258,
        ),
        chunked_urma_event(
            "2026-08-15T10:00:06.000071",
            "data-worker-chunk",
            0.194,
            "131140",
            2,
            2,
            post_us=158953756120,
            observed_us=158953756320,
        ),
    ]
    item["urma_elapsed_ms"] = {
        "total": {"count": 2, "min": 0.138, "p50": 0.166, "p90": 0.1884, "p99": 0.19344, "max": 0.194}
    }
    item["evidence_coverage"]["urma"] = "present"
    summary["traces"]["chunked-write"] = item
    summary["trace_count"] += 1
    summary_path.write_text(json.dumps(summary), encoding="utf-8")

    mod = load_module()
    analysis = mod.build_analysis(run_dir, top_n=100, deadline_ms=20, local_cache=False)
    row = next(value for value in analysis["traces"] if value["trace_id"] == "chunked-write")

    assert [request["write_chunk_index"] for request in row["urma_requests"]] == [1, 2]
    assert len(row["urma_logical_writes"]) == 1
    logical = row["urma_logical_writes"][0]
    assert logical["write_index"] == 1
    assert logical["wr_count"] == 2
    assert logical["expected_wr_count"] == 2
    assert logical["complete"] is True
    assert logical["wall_clock_ms"] == pytest.approx(0.207)
    assert logical["slowest_wr_ms"] == pytest.approx(0.194)
    assert logical["sum_wr_ms"] == pytest.approx(0.332)
    assert row["urma_trace"]["critical_path_ms"] == pytest.approx(0.194)
    assert row["urma_trace"]["latency_basis"] == "最慢WR"
    assert row["urma_trace"]["wr_count"] == 2
    assert row["urma_trace"]["logical_write_count"] == 1
    assert analysis["aggregate"]["urma_analysis"]["wr_count"] >= 2
    assert analysis["aggregate"]["urma_analysis"]["logical_write_count"] >= 1
    assert analysis["aggregate"]["urma_analysis"]["logical_write_slowest_wr_ms"]["count"] >= 1
    html_text = mod.render_html(analysis, "Chunked URMA")
    assert "Client Get → 逻辑 URMA Write → WR分片" in html_text
    assert "不求和" in html_text
    assert "URMA Elapsed Time" in html_text
    assert "两个" in html_text


def test_worker_query_and_get_inline_urma_is_removed_from_query_parent(run_dir: Path):
    summary_path = run_dir / "summary.json"
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    item = trace(
        "worker-inline-query-urma",
        6,
        0,
        timestamp="2026-08-24T10:46:58.312841",
        query_meta_ms=5.7,
        worker="data-worker-8",
    )
    item["latency_summary_us"] = {"client.rpc.direct_query_and_get": 5_700}
    item["ub_events"] = [
        chunked_urma_event(
            "2026-08-24T10:46:58.315630",
            "data-worker-8",
            2.417,
            "154032",
            1,
            2,
            post_us=130862131031,
            observed_us=130862133454,
        ),
        chunked_urma_event(
            "2026-08-24T10:46:58.318453",
            "data-worker-8",
            5.213,
            "154034",
            2,
            2,
            post_us=130862131054,
            observed_us=130862136273,
        ),
    ]
    item["urma_elapsed_ms"] = {"total": metric(5.213) | {"count": 2}}
    item["evidence_coverage"]["urma"] = "present"
    item["evidence"].append(
        {
            "source": "fixture.log",
            "member": "worker-inline-query-urma",
            "line": 2,
            "worker": "data-worker-8",
            "host_ip": "",
            "text": (
                "2026-08-24T10:46:58.318620 | [SLOW LOG] QueryAndGet done, "
                "keyCount: 1, inlineHits: 1, misses: 0, transport: UB, "
                "preprocess: 0.003ms, localRead: 5.338ms, metadata: 0.000ms, "
                "delivery: 0.027ms, total: 5.368ms, status: code: [OK]"
            ),
        }
    )
    item["evidence"].append(
        {
            "source": "fixture.log",
            "member": "worker-inline-query-urma",
            "line": 3,
            "worker": "client-a",
            "host_ip": "",
            "text": (
                "[BRPC_RPC_FRAMEWORK_SLOW] "
                "method=datasystem.WorkerOCService.QueryAndGet e2e_us=5600 "
                "server_req_queue_us=8 server_exec_us=5320 network_residual_us=250 "
                "cntl_error_code=0 cntl_failed=0"
            ),
        }
    )
    summary["traces"]["worker-inline-query-urma"] = item
    summary["trace_count"] += 1
    summary_path.write_text(json.dumps(summary), encoding="utf-8")

    row = next(
        value
        for value in load_module().build_analysis(run_dir, top_n=100, deadline_ms=None)["traces"]
        if value["trace_id"] == "worker-inline-query-urma"
    )

    assert row["inline_query_urma_ms"] == pytest.approx(5.213)
    assert row["query_and_get_parent_ms"] == pytest.approx(5.7)
    assert row["query_and_get_exclusive_ms"] == pytest.approx(0.487)
    assert row["attribution_ms"]["URMA"] == pytest.approx(5.213)
    assert row["query_and_get_exclusive_ms"] == pytest.approx(0.487)
    assert row["attribution_ms"]["RPC网络"] == pytest.approx(0.250)
    assert row["attribution_ms"]["RPC排队"] == pytest.approx(0.008)
    assert row["attribution_ms"]["QueryMeta"] == pytest.approx(0.229)
    assert row["data_access_scope"] == "QueryAndGet inline URMA"
    assert "同 Worker/同 attempt 唯一匹配" in row["data_access_evidence"]
    assert sum(row["attribution_ms"].values()) == pytest.approx(row["client_ms"])
    html = load_module().render_html(
        load_module().build_analysis(run_dir, top_n=100, deadline_ms=None), "Inline URMA"
    )
    assert "PR2165 inline 互斥归因" in html
    assert "QueryAndGet其他" in html
    assert "URMA建链" in html
    assert "URMA通信" in html
    assert "URMA调度/线程开销" in html
    assert "其他调度/线程开销" in html
    assert "RPC网络相关" in html
    assert "RPC框架" in html
    assert "QueryAndGet父窗口（原始）" in html
    assert "已剝离 inline URMA" in html


def test_worker_query_and_get_inline_urma_sums_sequential_logical_writes(run_dir: Path):
    summary_path = run_dir / "summary.json"
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    item = trace(
        "worker-inline-query-two-writes",
        6,
        0,
        timestamp="2026-08-24T10:46:58.312000",
        query_meta_ms=5.0,
        worker="data-worker-8",
    )
    item["latency_summary_us"] = {"client.rpc.direct_query_and_get": 5_000}
    item["ub_events"] = [
        chunked_urma_event(
            "2026-08-24T10:46:58.313500",
            "data-worker-8",
            2.0,
            "154032",
            1,
            1,
            post_us=130862131000,
            observed_us=130862133000,
        ),
        chunked_urma_event(
            "2026-08-24T10:46:58.315500",
            "data-worker-8",
            2.0,
            "154033",
            1,
            1,
            post_us=130862133100,
            observed_us=130862135100,
        ),
    ]
    item["urma_elapsed_ms"] = {"total": metric(2.0) | {"count": 2}}
    item["evidence_coverage"]["urma"] = "present"
    item["evidence"].append(
        {
            "source": "fixture.log",
            "member": "worker-inline-query-two-writes",
            "line": 2,
            "worker": "data-worker-8",
            "host_ip": "",
            "text": (
                "2026-08-24T10:46:58.316000 | [SLOW LOG] QueryAndGet done, "
                "keyCount: 2, inlineHits: 2, misses: 0, transport: UB, "
                "preprocess: 0.003ms, localRead: 4.800ms, metadata: 0.000ms, "
                "delivery: 0.027ms, total: 5.000ms, status: code: [OK]"
            ),
        }
    )
    summary["traces"]["worker-inline-query-two-writes"] = item
    summary["trace_count"] += 1
    summary_path.write_text(json.dumps(summary), encoding="utf-8")

    row = next(
        value
        for value in load_module().build_analysis(run_dir, top_n=100, deadline_ms=None)["traces"]
        if value["trace_id"] == "worker-inline-query-two-writes"
    )

    assert row["inline_query_urma_ms"] == pytest.approx(4.0)
    assert row["query_and_get_exclusive_ms"] == pytest.approx(1.0)
    assert row["attribution_ms"]["URMA"] == pytest.approx(4.0)
    assert "2个串行逻辑Write" in row["inline_query_urma_basis"]


def test_worker_query_and_get_inline_urma_requires_same_worker(run_dir: Path):
    summary_path = run_dir / "summary.json"
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    item = trace(
        "worker-inline-query-other-worker",
        6,
        0,
        timestamp="2026-08-24T10:46:58.312841",
        query_meta_ms=5.7,
        urma_ms=5.2,
        worker="urma-worker",
    )
    item["latency_summary_us"] = {"client.rpc.direct_query_and_get": 5_700}
    item["evidence"].append(
        {
            "source": "fixture.log",
            "member": "worker-inline-query-other-worker",
            "line": 2,
            "worker": "query-worker",
            "host_ip": "",
            "text": (
                "2026-08-24T10:46:58.318620 | QueryAndGet done, keyCount: 1, "
                "inlineHits: 1, misses: 0, transport: UB, total: 5.368ms"
            ),
        }
    )
    summary["traces"]["worker-inline-query-other-worker"] = item
    summary["trace_count"] += 1
    summary_path.write_text(json.dumps(summary), encoding="utf-8")

    row = next(
        value
        for value in load_module().build_analysis(run_dir, top_n=100, deadline_ms=None)["traces"]
        if value["trace_id"] == "worker-inline-query-other-worker"
    )

    assert row["inline_query_urma_ms"] is None
    assert row["attribution_ms"]["QueryMeta"] == pytest.approx(5.7)


def test_access_location_uses_each_client_trace_actual_transport(run_dir: Path):
    summary_path = run_dir / "summary.json"
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    for index, transport in enumerate(("SHM", "UB", "TCP", None), start=1):
        trace_id = f"transport-{transport or 'unknown'}"
        item = trace(trace_id, 4, 0, timestamp=f"2026-08-15T10:00:0{index}.000001")
        suffix = f" | {{transportType:{transport}}}" if transport else ""
        item["evidence"].append(
            {
                "source": "client-access.log",
                "member": trace_id,
                "line": 2,
                "worker": "client",
                "host_ip": "",
                "text": (
                    f"2026-08-15T10:00:0{index}.000001 | I | client | trace | 0 | "
                    f"DS_KV_CLIENT_GET | 4000 | 8388608 | key{suffix}"
                ),
            }
        )
        summary["traces"][trace_id] = item
        summary["trace_count"] += 1
    summary_path.write_text(json.dumps(summary), encoding="utf-8")

    analysis = load_module().build_analysis(run_dir, top_n=100, deadline_ms=20, local_cache=False)
    rows = {row["trace_id"]: row for row in analysis["traces"]}

    assert rows["transport-SHM"]["access_location"] == "本节点SHM"
    assert rows["transport-UB"]["access_location"] == "远端Data Worker"
    assert rows["transport-TCP"]["access_location"] == "位置不确定（TCP）"
    assert rows["transport-unknown"]["access_location"] == "未确认"
    assert analysis["aggregate"]["access_locations"] == {
        "本节点SHM": 1,
        "远端Data Worker": 1,
        "位置不确定（TCP）": 1,
        "未确认": 5,
    }
    html_text = load_module().render_html(analysis, "Transport fixture")
    assert 'id="access-location-filter"' in html_text
    assert "访问位置" in html_text
    assert "SHM=本节点共享内存；UB=远端 Data Worker；TCP=位置不确定" in html_text
    assert "仅覆盖当前 TopN 输入" in html_text
    assert "直连 Data Worker" not in html_text
    assert ".chart-title{text-align:center}" in html_text
    assert ".hero h1{overflow-wrap:anywhere;word-break:break-word}" in html_text
    assert "classList.add('chart-title')" in html_text
    assert 'id="time-segment-controls"' in html_text
    assert 'id="time-segment-scope"' in html_text
    assert 'id="time-segment-chart"' in html_text
    assert "图 1-5 Client 总时延五档问题分布" in html_text
    assert '图 1-5 Client总时延五档' in html_text
    assert '图 1-4 Client总时延五档' not in html_text
    assert ".time-segment-button.active" in html_text
    assert "let activeTimeSegment=null" in html_text
    assert "function scopeRows()" in html_text
    assert "function selectTimeSegment(segmentId)" in html_text
    assert "档位筛选仅过滤总览、Stacked Bars 与 Trace 表" in html_text
    assert ".correlation-grid{display:grid;grid-template-columns:minmax(0,1fr)" in html_text
    assert "table{width:100%;table-layout:fixed" in html_text
    assert ".table-wrap{max-height:560px;overflow-y:auto;overflow-x:hidden" in html_text
    assert "#trace-table th:nth-child(6)" in html_text
    assert ".worker-name{max-width:none" in html_text
    assert "#urma-trace-table th:nth-child(10){width:29%}" in html_text
    assert "#non-transport-table th:nth-child(11){width:21%}" in html_text
    assert "#worker-correlation-table th:nth-child(10){width:23%}" in html_text
    assert "function evidenceMs(value)" in html_text
    assert "Data RPC e2e/network/server: ${evidenceMs(row.data_rpc_e2e_ms)}" in html_text
    assert "#urma-edge-table th:nth-child(2)" in html_text


def test_missing_rpc_and_urma_evidence_stays_unobserved(run_dir: Path):
    summary_path = run_dir / "summary.json"
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    blind = trace("blind", 10, 0, timestamp="2026-08-15T10:00:06.000001")
    blind["evidence_coverage"]["urma"] = "missing"
    blind["urma_elapsed_ms"] = {"total": {"count": 0, "min": None, "p50": None, "p90": None, "p99": None, "max": None}}
    summary["traces"]["blind"] = blind
    summary["trace_count"] += 1
    summary_path.write_text(json.dumps(summary), encoding="utf-8")

    mod = load_module()
    analysis = mod.build_analysis(run_dir, top_n=100, deadline_ms=20)
    row = next(item for item in analysis["traces"] if item["trace_id"] == "blind")

    assert not row["rpc_observed"]
    assert not row["urma_observed"]
    assert row["non_transport_analysis"]["deep_category"] == "Client/Worker观测未闭合"
    assert "未观测到可关联的 RPC" in row["non_transport_analysis"]["conclusion"]


@pytest.mark.parametrize(
    "marker",
    [
        "[URMA_WAIT_TIMEOUT] [urma_request_id:42] timedout waiting, elapsedMs=15.058000",
        "[URMA-WAIT-TIMEOUT] requestId=42 elapsedMs=15.058000",
        "Timed out waiting for urma_request_id_42, elapsedMs=15.058000",
    ],
)
def test_failed_urma_wait_timeout_is_an_error_family_not_unsegmented_parent(
    run_dir: Path, marker: str
):
    summary_path = run_dir / "summary.json"
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    timeout = trace(
        "urma-timeout",
        20,
        18,
        timestamp="2026-08-15T10:00:06.000001",
        local_ms=18,
        status=1004,
    )
    timeout["errors"]["URMA_WAIT_TIMEOUT"] = 1
    timeout["evidence"].append(
        {
            "source": "fixture.log",
            "member": "urma-timeout",
            "line": 2,
            "worker": "worker-a",
            "host_ip": "",
            "text": marker,
        }
    )
    timeout["evidence"].extend(
        [
            {
                "source": "fixture.log",
                "member": "urma-timeout",
                "line": 3,
                "worker": "worker-a",
                "host_ip": "",
                "text": "[URMA_SEND_LANE_TIMEOUT_OBSERVED] pendingWrs=2 sealed=1",
            },
            {
                "source": "fixture.log",
                "member": "urma-timeout",
                "line": 4,
                "worker": "worker-a",
                "host_ip": "",
                "text": "[URMA_SEND_LANE_FORCE_RELEASE] pendingWrs=2 orphanWrsAtDecision=3",
            },
            {
                "source": "fixture.log",
                "member": "urma-timeout",
                "line": 5,
                "worker": "client-a",
                "host_ip": "",
                "text": "[TransportGet][UB] Unexpected TCP payload response",
            },
        ]
    )
    summary["traces"]["urma-timeout"] = timeout
    summary["trace_count"] += 1
    summary_path.write_text(json.dumps(summary), encoding="utf-8")

    mod = load_module()
    analysis = mod.build_analysis(run_dir, top_n=100, deadline_ms=20)
    row = next(item for item in analysis["traces"] if item["trace_id"] == "urma-timeout")

    assert row["urma_timeout_observed"] is True
    assert row["urma_timeout_max_ms"] == 15.058
    assert row["error_family"] == "URMA超时"
    assert row["error_subcategory"] == "URMA completion超时·多pending WR"
    assert row["error_chain_category"] == "URMA超时→UB异常响应→1004"
    assert row["error_failure_point"] == "URMA WRITE completion在等待窗口内未返回"
    assert "接收端未完成" in row["error_root_cause_boundary"]
    assert row["primary_problem"] == "URMA超时"
    assert row["primary_stage"] == "数据访问父窗口/未细分"
    assert analysis["problem_summary"]["URMA超时"] == 1
    assert analysis["aggregate"]["error_summary"]["URMA超时"] == 1
    assert analysis["aggregate"]["error_detail_summary"]["subcategories"] == {
        "URMA completion超时·多pending WR": 1
    }
    assert analysis["aggregate"]["error_detail_summary"]["chains"] == {
        "URMA超时→UB异常响应→1004": 1
    }
    assert analysis["aggregate"]["problem_summary"]["URMA超时"]["metric_name"] == "URMA timeout elapsedMs"
    assert analysis["aggregate"]["problem_summary"]["URMA超时"]["stage_p50_ms"] == 15.058
    assert analysis["aggregate"]["focus_problem_summary"]["URMA超时"]["metric_name"] == "URMA timeout elapsedMs"
    assert analysis["aggregate"]["focus_problem_summary"]["URMA超时"]["stage_p50_ms"] == 15.058
    assert sum(row["attribution_ms"].values()) <= row["client_ms"]

    html_text = mod.render_html(analysis, "URMA timeout fixture")
    assert "URMA超时" in html_text
    assert "错误细分" in html_text
    assert "error-subcategory-chart" in html_text
    assert "error-chain-chart" in html_text
    assert "URMA超时标记" in html_text
    assert "已观测到 URMA_WAIT_TIMEOUT；失败 WR 没有完成态时不伪造 URMA 耗时" in html_text
    assert "problem==='URMA超时'" in html_text


def test_get_1004_receive_buffer_oom_is_not_reported_as_urma_completion_timeout(run_dir: Path):
    summary_path = run_dir / "summary.json"
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    failed = trace(
        "get-client-arena-oom",
        2.755,
        0,
        timestamp="2026-08-26T09:01:05.960575",
        status=1004,
    )
    failed["evidence"].append(
        {
            "source": "fixture.log",
            "member": "get-client-arena-oom",
            "line": 2,
            "worker": "client-a",
            "host_ip": "",
            "text": (
                "[TransportGet][UB] Receive buffer preparation failed, "
                "causeStatus=code: [Out of memory], msg: [UnknownType no space in arena: 3, "
                "reason=fresh_extent_unavailable]"
            ),
        }
    )
    summary["traces"]["get-client-arena-oom"] = failed
    summary["trace_count"] += 1
    summary_path.write_text(json.dumps(summary), encoding="utf-8")

    mod = load_module()
    analysis = mod.build_analysis(run_dir, top_n=100, deadline_ms=20)
    row = next(item for item in analysis["traces"] if item["trace_id"] == "get-client-arena-oom")

    assert row["error_family"] == "Client UB接收缓冲分配失败"
    assert row["error_subcategory"] == "Client arena fresh extent不足"
    assert row["error_chain_category"] == "Client接收缓冲分配失败→1004"
    assert row["primary_problem"] == "未解释残差"
    assert row["non_transport_analysis"]["deep_category"] == "Client UB接收缓冲分配失败"
    assert "不是已完成 WR 变慢" in row["non_transport_analysis"]["conclusion"]
    assert next(
        item
        for item in analysis["aggregate"]["non_transport_analysis"]["categories"]
        if item["category"] == "Client UB接收缓冲分配失败"
    )["trace_count"] == 1


def test_query_and_get_breakdown_extracts_uniquely_nested_urma_timeout(run_dir: Path):
    summary_path = run_dir / "summary.json"
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    trace_id = "query-and-get-inline-timeout"
    item = trace(
        trace_id,
        20.398,
        0,
        timestamp="2026-08-25T22:35:24.459500",
        query_meta_ms=15.828,
        status=1001,
    )
    item["latency_summary_us"] = {
        "client.process.direct_route": 6,
        "client.rpc.direct_query_and_get": 15_828,
        "client.rpc.direct_get_data": 4_537,
        "client.process.direct_materialize": 2,
        "client.process.get": 16,
    }
    item["errors"]["URMA_WAIT_TIMEOUT"] = 1
    item["evidence"].extend(
        [
            {
                "source": "worker.log",
                "member": trace_id,
                "line": 2,
                "worker": "worker-13",
                "host_ip": "",
                "text": (
                    "2026-08-25T22:35:24.475183 | [URMA_WAIT_TIMEOUT] "
                    "[urma_request_id:15267] timedout waiting, elapsedMs=15.506000, op=WRITE"
                ),
            },
            {
                "source": "worker.log",
                "member": trace_id,
                "line": 3,
                "worker": "worker-13",
                "host_ip": "",
                "text": (
                    "2026-08-25T22:35:24.475281 | [SLOW LOG] QueryAndGet done, "
                    "keyCount: 1, inlineHits: 0, misses: 1, transport: UB, "
                    "preprocess: 0.003ms, localRead: 15.578ms, metadata: 0.032ms, "
                    "delivery: 0.027ms, total: 15.640ms, status: code: [OK]"
                ),
            },
            {
                "source": "client.log",
                "member": trace_id,
                "line": 4,
                "worker": "client-4",
                "host_ip": "",
                "text": (
                    "2026-08-25T22:35:24.475314 | [BRPC_RPC_FRAMEWORK_SLOW] "
                    "method=datasystem.WorkerOCService.QueryAndGet e2e_us=15776 "
                    "server_req_queue_us=12 server_exec_us=15612 network_residual_us=141 "
                    "cntl_error_code=0 cntl_failed=0"
                ),
            },
        ]
    )
    summary["traces"][trace_id] = item
    summary["trace_count"] += 1
    summary_path.write_text(json.dumps(summary), encoding="utf-8")

    mod = load_module()
    analysis = mod.build_analysis(run_dir, top_n=100, deadline_ms=20)
    row = next(value for value in analysis["traces"] if value["trace_id"] == trace_id)

    assert row["query_urma_timeout_ms"] == pytest.approx(15.506)
    assert row["query_urma_timeout_basis"] == "同Worker QueryAndGet父窗口内唯一URMA_WAIT_TIMEOUT"
    assert row["attribution_ms"]["URMA超时等待"] == pytest.approx(15.506)
    assert row["attribution_ms"]["QueryMeta"] == pytest.approx(0.169)
    assert row["primary_stage"] == "URMA超时等待"
    assert row["primary_problem"] == "URMA超时"
    assert sum(row["attribution_ms"].values()) == pytest.approx(row["client_ms"])

    html_text = mod.render_html(analysis, "QueryAndGet timeout breakdown")
    assert "URMA超时等待窗口" in html_text


def test_query_and_get_breakdown_keeps_parent_when_any_attempt_is_ambiguous():
    row = {
        "urma_timeout_observed": True,
        "attribution_ms": {stage: 0.0 for stage in load_module().STAGE_NAMES},
        "evidence_records": [],
        "primary_problem": "QueryMeta",
    }
    row["attribution_ms"]["QueryMeta"] = 20.0
    for line, text in enumerate(
        (
            "2026-08-25T22:35:24.015000 | [URMA_WAIT_TIMEOUT] "
            "[urma_request_id:1] elapsedMs=9.000",
            "2026-08-25T22:35:24.020000 | QueryAndGet done, localRead: 9.5ms, total: 10ms",
            "2026-08-25T22:35:25.014000 | [URMA_WAIT_TIMEOUT] "
            "[urma_request_id:2] elapsedMs=8.000",
            "2026-08-25T22:35:25.015000 | [URMA_WAIT_TIMEOUT] "
            "[urma_request_id:3] elapsedMs=8.500",
            "2026-08-25T22:35:25.020000 | QueryAndGet done, localRead: 9.5ms, total: 10ms",
        ),
        start=1,
    ):
        row["evidence_records"].append(
            {"source": "worker.log", "line": line, "worker": "worker-13", "text": text}
        )

    mod = load_module()
    mod._apply_query_urma_timeout_attribution(row)

    assert row["query_urma_timeout_ms"] is None
    assert row["attribution_ms"]["QueryMeta"] == 20.0
    assert row["attribution_ms"]["URMA超时等待"] == 0.0


@pytest.mark.parametrize(
    "method, expected_subcategory, expected_chain, expected_scope",
    [
        (
            "datasystem.master.MasterOCService.QueryAndGet",
            "QueryMeta RPC deadline",
            "QueryMeta RPC超时→TransportGet失败→1001",
            "Client等待Meta Owner QueryAndGet超时",
        ),
        (
            "datasystem.WorkerWorkerOCService.GetObjectRemote",
            "Data RPC deadline",
            "Data RPC超时→TransportGet失败→1001",
            "Client→Data Worker RPC截止超时",
        ),
    ],
)
def test_non_urma_rpc_deadlines_have_error_family_and_root_cause_boundary(
    run_dir: Path,
    method: str,
    expected_subcategory: str,
    expected_chain: str,
    expected_scope: str,
):
    summary_path = run_dir / "summary.json"
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    trace_id = f"deadline-{expected_subcategory}"
    item = trace(
        trace_id,
        20,
        5,
        timestamp="2026-08-15T10:00:07.000001",
        status=1001,
    )
    item["evidence"].extend(
        [
            {
                "source": "fixture.log",
                "member": "deadline",
                "line": 2,
                "worker": "client-a",
                "host_ip": "",
                "text": f"[BRPC_RPC_FRAMEWORK_SLOW] method={method} e2e_us=20000",
            },
            {
                "source": "fixture.log",
                "member": "deadline",
                "line": 3,
                "worker": "client-a",
                "host_ip": "",
                "text": "[TransportGet][TransportLayer] Get failed, status: code: [RPC deadline exceeded]",
            },
        ]
    )
    summary["traces"][trace_id] = item
    summary["trace_count"] += 1
    summary_path.write_text(json.dumps(summary), encoding="utf-8")

    analysis = load_module().build_analysis(run_dir, top_n=100, deadline_ms=20)
    row = next(value for value in analysis["traces"] if value["trace_id"] == trace_id)

    assert row["error_family"] == "RPC截止超时"
    assert row["error_subcategory"] == expected_subcategory
    assert row["error_chain_category"] == expected_chain
    assert row["failure_reason"] == expected_subcategory
    assert row["data_access_scope"] == expected_scope
    if expected_subcategory == "Data RPC deadline":
        assert row["data_rpc_network_ms"] is None
        assert row["data_rpc_server_ms"] is None
    assert "服务端执行" in row["error_root_cause_boundary"]
    assert analysis["aggregate"]["error_summary"]["RPC截止超时"] == 1


def test_successful_query_and_get_does_not_mask_later_data_rpc_deadline(run_dir: Path):
    summary_path = run_dir / "summary.json"
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    trace_id = "query-success-data-timeout"
    item = trace(trace_id, 20.1, 0, timestamp="2026-08-15T10:00:07.100001", status=1001)
    item["latency_summary_us"] = {
        "client.rpc.direct_query_and_get": 18500,
        "client.rpc.direct_get_data": 1500,
    }
    item["evidence"].extend(
        [
            {
                "source": "fixture.log",
                "member": trace_id,
                "line": 2,
                "worker": "client-a",
                "host_ip": "",
                "text": (
                    "[BRPC_RPC_FRAMEWORK_SLOW] "
                    "method=datasystem.master.MasterOCService.QueryAndGet e2e_us=18400 "
                    "server_exec_us=8 network_residual_us=18390 cntl_error_code=0 cntl_failed=0"
                ),
            },
            {
                "source": "fixture.log",
                "member": trace_id,
                "line": 3,
                "worker": "client-a",
                "host_ip": "",
                "text": (
                    "[TransportGet][DataPlane] GetObjectRemote->[data-worker:31501] "
                    "RPC deadline exceeded, remaining -49 us"
                ),
            },
        ]
    )
    summary["traces"][trace_id] = item
    summary["trace_count"] += 1
    summary_path.write_text(json.dumps(summary), encoding="utf-8")

    analysis = load_module().build_analysis(run_dir, top_n=100, deadline_ms=20)
    row = next(value for value in analysis["traces"] if value["trace_id"] == trace_id)

    assert row["failure_reason"] == "Data RPC deadline"
    assert row["data_access_scope"] == "Client→Data Worker RPC截止超时"
    assert row["query_meta_detail"]["category"] == "QueryAndGet成功·后续Data RPC失败"


def test_successful_query_and_get_does_not_mask_later_urma_connect_deadline(run_dir: Path):
    summary_path = run_dir / "summary.json"
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    trace_id = "query-success-urma-connect-timeout"
    item = trace(trace_id, 20.2, 0, timestamp="2026-08-15T10:00:07.200001", status=1001)
    item["latency_summary_us"] = {
        "client.rpc.direct_query_and_get": 19500,
        "client.rpc.direct_get_data": 500,
    }
    item["evidence"].extend(
        [
            {
                "source": "fixture.log",
                "member": trace_id,
                "line": 2,
                "worker": "client-a",
                "host_ip": "",
                "text": (
                    "[BRPC_RPC_FRAMEWORK_SLOW] "
                    "method=datasystem.master.MasterOCService.QueryAndGet e2e_us=19400 "
                    "server_exec_us=8 network_residual_us=19390 cntl_error_code=0 cntl_failed=0"
                ),
            },
            {
                "source": "fixture.log",
                "member": trace_id,
                "line": 3,
                "worker": "client-a",
                "host_ip": "",
                "text": (
                    "UB establish failed: WorkerWorkerExchangeUrmaConnectInfo->[data-worker:31501] "
                    "RPC deadline exceeded. API deadline exceeded, remaining -66 us"
                ),
            },
        ]
    )
    summary["traces"][trace_id] = item
    summary["trace_count"] += 1
    summary_path.write_text(json.dumps(summary), encoding="utf-8")

    analysis = load_module().build_analysis(run_dir, top_n=100, deadline_ms=20)
    row = next(value for value in analysis["traces"] if value["trace_id"] == trace_id)

    assert row["failure_reason"] == "Data URMA建链截止超时"
    assert row["data_access_scope"] == "Client→Data Worker URMA建链截止超时"
    assert row["query_meta_detail"]["category"] == "QueryAndGet成功·后续URMA建链失败"


@pytest.mark.parametrize(
    "query_total_ms,rpc_e2e_ms,rpc_network_ms,urma_ms,expected_category,expected_slow_urma",
    [
        (18.0, 17.9, 17.8, 0.6, "QueryAndGet成功·RPC residual主导", False),
        (16.0, 2.0, 1.9, 0.7, "QueryAndGet成功·重试/多次尝试累计", False),
        (4.0, 3.8, 1.0, 2.0, "QueryAndGet TryGet·URMA慢", True),
    ],
)
def test_query_meta_detail_separates_retry_rpc_residual_and_inline_urma(
    run_dir: Path,
    query_total_ms: float,
    rpc_e2e_ms: float,
    rpc_network_ms: float,
    urma_ms: float,
    expected_category: str,
    expected_slow_urma: bool,
):
    summary_path = run_dir / "summary.json"
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    trace_id = f"query-detail-{expected_category}"
    item = trace(
        trace_id,
        query_total_ms + 1,
        0,
        timestamp="2026-08-15T10:00:08.000001",
        query_meta_ms=query_total_ms,
        urma_ms=urma_ms,
    )
    item["latency_summary_us"] = {
        "client.rpc.direct_query_and_get": int(query_total_ms * 1000),
        "client.rpc.direct_get_data": 500,
    }
    item["evidence"].append(
        {
            "source": "fixture.log",
            "member": trace_id,
            "line": 3,
            "worker": "client-a",
            "host_ip": "",
            "text": (
                "[BRPC_RPC_FRAMEWORK_SLOW] "
                "method=datasystem.master.MasterOCService.QueryAndGet "
                f"e2e_us={int(rpc_e2e_ms * 1000)} server_exec_us=5 "
                f"network_residual_us={int(rpc_network_ms * 1000)} "
                "cntl_error_code=0 cntl_failed=0"
            ),
        }
    )
    item["evidence"].append(
        {
            "source": "fixture.log",
            "member": trace_id,
            "line": 4,
            "worker": "meta-owner",
            "host_ip": "",
            "text": (
                f"[SLOW LOG] Processing pull object[{trace_id}] hasUrmaInfo[1], "
                f"cost: {urma_ms}ms, src=:-1, dst=meta-owner:31501"
            ),
        }
    )
    summary["traces"][trace_id] = item
    summary["trace_count"] += 1
    summary_path.write_text(json.dumps(summary), encoding="utf-8")

    analysis = load_module().build_analysis(run_dir, top_n=100, deadline_ms=20)
    row = next(value for value in analysis["traces"] if value["trace_id"] == trace_id)

    assert row["query_meta_detail"]["category"] == expected_category
    assert row["query_meta_detail"]["try_get_urma_observed"] is True
    assert row["query_meta_detail"]["slow_urma"] is expected_slow_urma


def test_query_meta_detail_accepts_worker_service_query_and_get(run_dir: Path):
    summary_path = run_dir / "summary.json"
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    trace_id = "worker-service-query-and-get"
    item = trace(
        trace_id,
        5.4,
        0.3,
        timestamp="2026-08-24T10:49:15.000001",
        query_meta_ms=5.354,
    )
    item["latency_summary_us"] = {"client.rpc.direct_query_and_get": 5354}
    item["evidence"].append(
        {
            "source": "fixture.log",
            "member": trace_id,
            "line": 2,
            "worker": "client-a",
            "host_ip": "",
            "text": (
                "[BRPC_RPC_FRAMEWORK_SLOW] "
                "method=datasystem.WorkerOCService.QueryAndGet e2e_us=5292 "
                "server_req_queue_us=7 server_exec_us=254 network_residual_us=5019 "
                "cntl_error_code=0 cntl_failed=0"
            ),
        }
    )
    summary["traces"][trace_id] = item
    summary["trace_count"] += 1
    summary_path.write_text(json.dumps(summary), encoding="utf-8")

    analysis = load_module().build_analysis(run_dir, top_n=100, deadline_ms=None)
    row = next(value for value in analysis["traces"] if value["trace_id"] == trace_id)

    assert row["query_meta_detail"]["category"] == "QueryAndGet成功·RPC residual主导"
    assert row["query_meta_detail"]["rpc_e2e_ms"] == 5.292
    assert row["query_meta_detail"]["rpc_network_residual_ms"] == 5.019
    assert row["attribution_ms"]["RPC网络"] == 5.019
    assert row["attribution_ms"]["RPC排队"] == 0.007
    assert row["attribution_ms"]["QueryMeta"] == pytest.approx(0.328)
    assert row["primary_problem"] == "RPC网络"


@pytest.mark.parametrize(
    "with_worker_done,expected_category",
    [
        (True, "QueryAndGet localRead慢·URMA未观测"),
        (False, "QueryAndGet父窗口·服务端未观测"),
    ],
)
def test_query_meta_detail_separates_local_read_from_unobserved_server(
    run_dir: Path, with_worker_done: bool, expected_category: str
):
    summary_path = run_dir / "summary.json"
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    trace_id = f"query-coverage-{with_worker_done}"
    item = trace(
        trace_id,
        5.2,
        0.0,
        timestamp="2026-08-24T08:00:13.868017",
        query_meta_ms=5.1,
    )
    item["latency_summary_us"] = {"client.rpc.direct_query_and_get": 5_100}
    if with_worker_done:
        item["evidence"].append(
            {
                "source": "fixture.log",
                "member": trace_id,
                "line": 2,
                "worker": "data-worker",
                "host_ip": "",
                "text": (
                    "2026-08-24T08:00:13.867900 | QueryAndGet done, keyCount: 1, "
                    "inlineHits: 1, misses: 0, transport: UB, preprocess: 0.003ms, "
                    "localRead: 4.915ms, metadata: 0.001ms, delivery: 0.021ms, "
                    "total: 4.942ms, status: code: [OK]"
                ),
            }
        )
    summary["traces"][trace_id] = item
    summary["trace_count"] += 1
    summary_path.write_text(json.dumps(summary), encoding="utf-8")

    row = next(
        value
        for value in load_module().build_analysis(run_dir, top_n=100, deadline_ms=None)["traces"]
        if value["trace_id"] == trace_id
    )

    assert row["query_meta_detail"]["category"] == expected_category
    assert row["query_meta_detail"]["worker_query_done_observed"] is with_worker_done
    assert row["query_meta_detail"]["local_read_ms"] == (4.915 if with_worker_done else None)


@pytest.mark.parametrize(
    "error_code, expected_category",
    [(1008, "BatchGet超时/重试"), (0, "Data Worker服务端处理")],
)
def test_remote_handler_does_not_treat_missing_urma_as_zero(
    run_dir: Path, error_code: int, expected_category: str
):
    summary_path = run_dir / "summary.json"
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    trace_id = f"remote-{error_code}"
    remote = trace(trace_id, 20, 20, timestamp="2026-08-15T10:00:07.000001", local_ms=20)
    remote["evidence_coverage"]["urma"] = "missing"
    remote["urma_elapsed_ms"] = {"total": {"count": 0, "max": None}}
    remote["evidence"].append(
        {
            "source": "fixture.log",
            "member": trace_id,
            "line": 2,
            "worker": "worker-b",
            "host_ip": "",
            "text": (
                "method=datasystem.WorkerOCService.BatchGetObjectRemote "
                f"e2e_us=15000 server_exec_us=15000 network_residual_us=0 cntl_error_code={error_code}"
            ),
        }
    )
    summary["traces"][trace_id] = remote
    summary["trace_count"] += 1
    summary_path.write_text(json.dumps(summary), encoding="utf-8")

    mod = load_module()
    analysis = mod.build_analysis(run_dir, top_n=100, deadline_ms=20)
    row = next(item for item in analysis["traces"] if item["trace_id"] == trace_id)

    assert not row["urma_observed"]
    assert row["non_transport_analysis"]["deep_category"] == expected_category
    assert "URMA" in row["non_transport_analysis"]["conclusion"]
    assert "URMA 已观测最大 0.000ms" not in row["non_transport_analysis"]["conclusion"]


def test_local_cache_false_uses_client_direct_topology_for_batchget_and_urma(run_dir: Path):
    summary_path = run_dir / "summary.json"
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    direct = trace(
        "client-direct",
        20,
        20,
        timestamp="2026-08-15T10:00:09.000001",
        urma_ms=1,
        local_ms=20,
        worker="data-worker-b",
    )
    direct["evidence"].append(
        {
            "source": "fixture.log",
            "member": "client-direct",
            "line": 3,
            "worker": "data-worker-b",
            "host_ip": "",
            "text": (
                "[Get/RemotePull] method=datasystem.WorkerWorkerOCService.BatchGetObjectRemote "
                "e2e_us=15000 server_exec_us=15000 network_residual_us=0 cntl_error_code=1008"
            ),
        }
    )
    summary["traces"]["client-direct"] = direct
    summary["trace_count"] += 1
    summary_path.write_text(json.dumps(summary), encoding="utf-8")

    mod = load_module()
    analysis = mod.build_analysis(run_dir, top_n=100, deadline_ms=20, local_cache=False)
    row = next(item for item in analysis["traces"] if item["trace_id"] == "client-direct")
    html_text = mod.render_html(analysis, "Client direct fixture")

    assert analysis["metadata"]["local_cache"] is False
    assert analysis["topology"]["kind"] == "client_direct"
    assert row["non_transport_analysis"]["deep_category"] == "BatchGet超时/重试"
    assert "Client→Data Worker BatchGet" in row["non_transport_analysis"]["conclusion"]
    assert all(item["target_worker"] == "Client" for item in row["urma_requests"])
    assert "Client → Meta Owner → Data Worker" in html_text
    assert "Worker间BatchGet" not in html_text
    assert "Worker 间 BatchGet" not in html_text
    assert "Worker→Worker BatchGet" not in html_text


def test_unknown_local_cache_keeps_topology_unconfirmed(run_dir: Path):
    mod = load_module()
    analysis = mod.build_analysis(run_dir, top_n=4, deadline_ms=20)
    html_text = mod.render_html(analysis, "Unknown topology fixture")

    assert analysis["metadata"]["local_cache"] is None
    assert analysis["topology"]["kind"] == "unknown"
    assert any("local_cache" in item and "unknown" in item for item in analysis["limitations"])
    assert "local cache 模式未知" in html_text
    assert "Worker间BatchGet" not in html_text


def test_worker_role_is_not_inferred_from_line_count(run_dir: Path):
    mod = load_module()
    analysis = mod.build_analysis(run_dir, top_n=100, deadline_ms=20)
    local = next(item for item in analysis["traces"] if item["trace_id"] == "local-trace")
    urma = next(item for item in analysis["traces"] if item["trace_id"] == "urma-trace")

    assert local["direct_data_worker"] == "未明确"
    assert urma["urma_source_workers"] == ["worker-b"]


def test_optional_trace_files_degrade_instead_of_failing(run_dir: Path):
    mod = load_module()
    analysis = mod.build_analysis(run_dir, top_n=100, deadline_ms=None)
    assert analysis["trace_count"] == 4
    assert analysis["evidence_coverage"]["parsed_traces"] == "missing"
    assert analysis["evidence_coverage"]["events"] == "missing"
    assert analysis["limitations"]
    assert analysis["metadata"]["run_dir"] == "run"
    assert analysis["metadata"]["case"] == "fixture"
    assert analysis["metadata"]["inputs"] == ["fixture.log"]
    assert str(run_dir.parent) not in json.dumps(analysis)


def test_worker_only_trace_is_excluded_from_client_topn(run_dir: Path):
    summary_path = run_dir / "summary.json"
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    summary["traces"]["worker-only"] = trace(
        "worker-only", 0, 12, timestamp="2026-08-15T10:00:05.000001", local_ms=12
    )
    summary["trace_count"] += 1
    summary_path.write_text(json.dumps(summary), encoding="utf-8")

    mod = load_module()
    analysis = mod.build_analysis(run_dir, top_n=100, deadline_ms=20)

    assert analysis["source_trace_count"] == 5
    assert analysis["excluded_without_client_window"] == 1
    assert all(row["trace_id"] != "worker-only" for row in analysis["traces"])
    assert any("lack a Client latency window" in item for item in analysis["limitations"])


@pytest.mark.parametrize(
    ("flow", "expected"),
    [
        ("DS_KV_CLIENT_SET", True),
        ("DS_KV_CLIENT_MSET", True),
        ("DS_KV_CLIENT_CREATE", True),
        ("DS_KV_CLIENT_PUBLISH", True),
        ("DS_KV_CLIENT_SETNX", False),
        ("DS_KV_CLIENT_MSETNX", False),
        ("DS_KV_CLIENT_MCREATE", False),
        ("DS_POSIX_SET", False),
    ],
)
def test_write_flow_filter_matches_supported_operation_contract(flow: str, expected: bool):
    assert load_module()._is_write_flow({"flows": {flow: 1}}) is expected


def test_write_trace_has_separate_create_publish_breakdown(run_dir: Path):
    summary_path = run_dir / "summary.json"
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    write_trace = trace("set-trace", 12, 0, timestamp="2026-08-15T10:00:08.000001")
    write_trace["flows"] = {"DS_KV_CLIENT_SET": 1, "DS_POSIX_SET": 1}
    write_trace["latency_summary_us"] = {
        "client.rpc.create": 2_000,
        "client.rpc.create_total": 2_000,
        "client.process.memory_copy": 3_000,
        "client.rpc.publish": 6_000,
        "client.rpc.publish_total": 6_000,
        "worker.process.publish": 1_000,
        "worker.rpc.create_meta": 2_000,
    }
    write_trace["evidence"].extend(
        [
            {
                "source": "fixture.log",
                "member": "set-trace",
                "line": 2,
                "worker": "client-a",
                "host_ip": "",
                "text": (
                    "[BRPC_RPC_FRAMEWORK_SLOW] method=datasystem.WorkerOCService.Create "
                    "e2e_us=2000 server_req_queue_us=100 server_exec_us=1500 "
                    "network_residual_us=200 cntl_error_code=0 cntl_failed=0"
                ),
            },
            {
                "source": "fixture.log",
                "member": "set-trace",
                "line": 3,
                "worker": "client-a",
                "host_ip": "",
                "text": (
                    "[BRPC_RPC_FRAMEWORK_SLOW] method=datasystem.WorkerOCService.Publish "
                    "e2e_us=6000 server_req_queue_us=200 server_exec_us=5000 "
                    "network_residual_us=500 cntl_error_code=0 cntl_failed=0"
                ),
            },
        ]
    )
    summary["traces"]["set-trace"] = write_trace
    summary["trace_count"] += 1
    summary_path.write_text(json.dumps(summary), encoding="utf-8")

    mod = load_module()
    analysis = mod.build_analysis(run_dir, top_n=100, deadline_ms=20)

    assert analysis["write_trace_count"] == 1
    assert analysis["excluded_non_get"] == 1
    assert all(row["trace_id"] != "set-trace" for row in analysis["traces"])
    row = analysis["write_traces"][0]
    assert row["write_breakdown_ms"] == pytest.approx(
        {
            "Create RPC其他": 1.5,
            "写入MemoryCopy": 3.0,
            "写入URMA通信": 0.0,
            "写入URMA调度/线程开销": 0.0,
            "Publish RPC其他": 2.0,
            "Worker Publish/元数据": 3.0,
            "其他调度/线程开销": 0.3,
            "RPC网络相关": 0.7,
            "RPC框架": 0.5,
            "未解释残差": 1.0,
        }
    )
    assert sum(row["write_breakdown_ms"].values()) == pytest.approx(row["client_ms"])
    assert row["create_rpc_ms"] == 2.0
    assert row["publish_rpc_ms"] == 6.0
    assert row["write_data_basis"] == "client.process.memory_copy"
    assert analysis["write_aggregate"]["trace_count"] == 1
    html = mod.render_html(analysis, "Read/write fixture")
    assert "写入瓶颈分析" in html
    assert "Create RPC其他" in html
    assert "Publish RPC其他" in html
    assert "set-trace" in html
    assert 'href="#write-analysis"' in html
    assert "__WRITE_ROWS__" not in html
    assert "__WRITE_AGG__" not in html


def test_write_urma_is_split_from_enclosing_memory_copy_window(run_dir: Path):
    summary_path = run_dir / "summary.json"
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    write_trace = trace(
        "ub-set-trace",
        10,
        0,
        timestamp="2026-08-15T10:00:09.000001",
        urma_ms=4,
    )
    write_trace["flows"] = {"DS_KV_CLIENT_SET": 1}
    write_trace["latency_summary_us"] = {
        "client.rpc.create": 1_000,
        "client.process.memory_copy": 5_000,
        "client.urma.ub_transfer": 4_000,
        "client.rpc.publish": 3_000,
    }
    write_trace["ub_events"][0]["wake_sched_latency_us"] = 500
    summary["traces"]["ub-set-trace"] = write_trace
    summary["trace_count"] += 1
    summary_path.write_text(json.dumps(summary), encoding="utf-8")

    row = load_module().build_analysis(run_dir, top_n=100, deadline_ms=20)["write_traces"][0]

    assert row["write_data_ms"] == 5.0
    assert row["write_breakdown_ms"]["写入MemoryCopy"] == 1.0
    assert row["write_breakdown_ms"]["写入URMA通信"] == 3.5
    assert row["write_breakdown_ms"]["写入URMA调度/线程开销"] == 0.5
    assert row["write_breakdown_ms"]["其他调度/线程开销"] == 0.0
    assert sum(row["write_breakdown_ms"].values()) == pytest.approx(row["client_ms"])


def test_failed_write_rpc_does_not_prove_network_or_framework_split():
    mod = load_module()

    split = mod._write_rpc_split(
        20.0,
        [
            {
                "e2e": 20_000,
                "server_req_queue": 100,
                "server_exec": 15_000,
                "network_residual": 3_000,
                "cntl_error_code": 1008,
                "cntl_failed": 1,
            }
        ],
    )

    assert split == {
        "other": 20.0,
        "queue": 0.0,
        "network": 0.0,
        "framework": 0.0,
    }


def test_required_run_files_fail_clearly(tmp_path: Path):
    mod = load_module()
    with pytest.raises(mod.InputContractError, match="manifest.json.*summary.json.*triage.json"):
        mod.build_analysis(tmp_path, top_n=100, deadline_ms=None)


@pytest.mark.parametrize("deadline_ms", [0, -1, float("inf"), float("nan")])
def test_deadline_must_be_positive_and_finite(run_dir: Path, deadline_ms: float):
    mod = load_module()
    with pytest.raises(ValueError, match="positive finite"):
        mod.build_analysis(run_dir, top_n=100, deadline_ms=deadline_ms)


def test_write_outputs_requires_force(run_dir: Path, tmp_path: Path):
    mod = load_module()
    analysis = mod.build_analysis(run_dir, top_n=3, deadline_ms=20)
    output = tmp_path / "bottleneck.local.html"
    output.write_text("existing", encoding="utf-8")
    with pytest.raises(FileExistsError):
        mod.write_outputs(analysis, output, title="fixture", force=False)


def test_html_is_self_contained_and_has_dashboard_contract(run_dir: Path):
    mod = load_module()
    analysis = mod.build_analysis(run_dir, top_n=4, deadline_ms=20)
    html_text = mod.render_html(analysis, "Fixture bottleneck")
    for marker in (
        "Fixture bottleneck",
        "TopN 时间序列 Stacked Bars",
        "关键阶段耗时占比",
        "URMA 批量分析",
        "同 Worker 时间关联",
        "correlation-worker-filter",
        "worker-correlation-chart-rpc",
        "worker-correlation-chart-ub",
        "worker-correlation-chart-metadata",
        "worker-correlation-chart-data",
        "worker-correlation-summary",
        "worker-correlation-table",
        "worker-correlation-pager",
        "1.5ms",
        "未观测到对应证据",
        "非 RPC 主导深挖",
        "Data Worker 粒度分析",
        "Trace 阶段明细",
        "Trace 证据日志",
        "data-sort-key",
        "download-all-traces",
        "Inflight WR",
        "urma-time-pager",
        "echarts.init",
    ):
        assert marker in html_text
    assert "https://cdn.jsdelivr.net" not in html_text
    assert "<script src=" not in html_text
    assert 'data-id="${esc(r.trace_id)}"' in html_text
    assert 'data-id="${esc(row.trace_id)}"' in html_text
    assert "QueryMeta 当前 0 条主导" not in html_text
    assert "远端供数非URMA" not in html_text
    for same_fixture_literal in ("d897aee1", "77fb2d9a", "bcd67"):
        assert same_fixture_literal not in html_text


def test_html_download_labels_follow_requested_topn(run_dir: Path):
    mod = load_module()
    analysis = mod.build_analysis(run_dir, top_n=3, deadline_ms=20)

    html_text = mod.render_html(analysis, "Fixture Top3")

    assert "top100-all" not in html_text.lower()
    assert "Top100 当前筛选" not in html_text
    assert "top${ROWS.length}-all-${ROWS.length}" in html_text
    assert "Top${ROWS.length} 当前筛选" in html_text


def test_write_outputs_copies_preserved_raw_archives_and_renders_download_links(
    run_dir: Path, tmp_path: Path
):
    raw_dir = run_dir / "raw" / "inputs"
    raw_dir.mkdir(parents=True)
    raw_package = raw_dir / "01-fixture-traces.tar.gz"
    raw_package.write_bytes(b"original trace package")
    manifest_path = run_dir / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["inputs"] = [
        {
            "path": "/private/source/fixture-traces.tar.gz",
            "preserved_name": raw_package.name,
            "size": raw_package.stat().st_size,
            "sha256": "fixture-sha256",
            "members": ["trace-a.log"],
        }
    ]
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

    mod = load_module()
    analysis = mod.build_analysis(run_dir, top_n=3, deadline_ms=20)
    output = tmp_path / "share" / "bottleneck.html"
    mod.write_outputs(analysis, output, title="Raw archive fixture", source_run_dir=run_dir)

    copied = output.parent / "raw-inputs" / raw_package.name
    html_text = output.read_text(encoding="utf-8")
    persisted = json.loads(output.with_name("bottleneck.analysis.json").read_text(encoding="utf-8"))
    assert copied.read_bytes() == raw_package.read_bytes()
    assert persisted["metadata"]["raw_input_archives"] == [
        {
            "name": raw_package.name,
            "size_bytes": raw_package.stat().st_size,
            "sha256": "fixture-sha256",
            "download_path": f"raw-inputs/{raw_package.name}",
        }
    ]
    assert "下载原始 Trace 数据包" in html_text
    assert f'href="raw-inputs/{raw_package.name}"' in html_text
    assert "/private/source" not in html_text


def test_html_handles_a_run_without_urma_evidence(run_dir: Path):
    summary_path = run_dir / "summary.json"
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    del summary["traces"]["urma-trace"]
    summary["trace_count"] -= 1
    summary_path.write_text(json.dumps(summary), encoding="utf-8")

    mod = load_module()
    analysis = mod.build_analysis(run_dir, top_n=100, deadline_ms=None)
    html_text = mod.render_html(analysis, "No URMA fixture")

    assert analysis["aggregate"]["urma_analysis"]["trace_count"] == 0
    assert "本次无 URMA 证据" in html_text


def test_evidence_text_is_not_rewritten_by_template_generalization(run_dir: Path):
    mod = load_module()
    analysis = mod.build_analysis(run_dir, top_n=4, deadline_ms=30)
    sentinel = "20ms deadline | Trace 原始日志 | SAME 3x105 QPS | __ROWS__ | __AGG__ | __ECHARTS_SOURCE__"
    analysis["traces"][0]["evidence"].append(sentinel)

    html_text = mod.render_html(analysis, "Generic")

    assert sentinel in html_text
    assert "30ms deadline" in html_text


def test_single_urma_request_does_not_claim_zero_correlation(run_dir: Path):
    mod = load_module()
    analysis = mod.build_analysis(run_dir, top_n=4, deadline_ms=20)
    assert analysis["aggregate"]["urma_analysis"]["inflight_total_correlation"] is None
    html_text = mod.render_html(analysis, "One URMA request")
    assert "样本不足或无方差，相关性不可计算" in html_text


def test_dropped_evidence_is_preserved_in_report_and_export_contract(run_dir: Path):
    summary_path = run_dir / "summary.json"
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    summary["traces"]["rpc-trace"]["dropped_evidence"] = 17
    summary_path.write_text(json.dumps(summary), encoding="utf-8")

    mod = load_module()
    analysis = mod.build_analysis(run_dir, top_n=4, deadline_ms=20)
    html_text = mod.render_html(analysis, "Truncated evidence")

    row = next(item for item in analysis["traces"] if item["trace_id"] == "rpc-trace")
    assert row["dropped_evidence"] == 17
    assert '"dropped_evidence":17' in html_text
    assert "不是原始日志全量" in html_text
    assert "下载 TopN 证据" in html_text


def test_cli_writes_json_and_html(run_dir: Path, tmp_path: Path, capsys):
    mod = load_module()
    output = tmp_path / "report.html"
    assert mod.main(
        [
            "--run-dir",
            str(run_dir),
            "--top",
            "3",
            "--local-cache",
            "false",
            "--source-ref",
            "current-main-ref",
            "--output",
            str(output),
        ]
    ) == 0
    assert output.exists()
    analysis_path = tmp_path / "bottleneck.analysis.json"
    assert analysis_path.exists()
    written_analysis = json.loads(analysis_path.read_text(encoding="utf-8"))
    assert written_analysis["metadata"]["local_cache"] is False
    assert written_analysis["metadata"]["current_source_ref"] == "current-main-ref"
    assert "fixture · Top3 关键瓶颈" in output.read_text(encoding="utf-8")
    assert Path(capsys.readouterr().out.strip()) == output.resolve()


def test_skill_routes_main_bottleneck_requests_through_triage_first():
    repo = Path(__file__).resolve().parents[2]
    skill = (repo / ".skills" / "ds-trace-triage" / "SKILL.md").read_text(encoding="utf-8")
    agent = (repo / ".skills" / "ds-trace-triage" / "agents" / "openai.yaml").read_text(encoding="utf-8")
    for marker in (
        "主问题关键瓶颈",
        "TopN/Top100",
        "scripts/ds_trace_bottleneck.py",
        "bottleneck.analysis.json",
        "bottleneck.local.html",
        "先完成 `ds_trace_triage.py run`",
        "不得自动发布",
        "--local-cache true|false",
        "BatchGetObjectRemote",
        "Worker→Worker",
    ):
        assert marker in skill
    assert "关键瓶颈" in agent
