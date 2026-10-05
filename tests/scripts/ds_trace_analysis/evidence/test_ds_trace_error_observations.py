"""GET and NUMA consume the same error evidence without changing attribution."""
from trace_test_loader import REPO_ROOT

import sys
from pathlib import Path

sys.path.insert(0, str(REPO_ROOT / "scripts"))

from trace_analysis import bottleneck, numa
from trace_analysis.evidence.errors import observe_error_evidence


def test_timeout_markers_and_missing_completion_remain_separate():
    evidence = [
        "[URMA_WAIT_TIMEOUT] elapsedMs=15.391 urma_request_id:7 op=WRITE",
        "[URMA_ELAPSED_TOTAL] urma post to completion cost: 2.635ms, condition wait: 2.63645ms",
        "URMA_SEND_LANE_FORCE_RELEASE pendingWrs=2",
    ]
    observations = observe_error_evidence(evidence)
    assert observations["urma_timeout"]
    assert observations["urma_timeout_elapsed_ms"] == 15.391
    assert observations["pending_wrs"] == 2
    assert bottleneck._classify_urma_timeout_detail(1010, observations)["error_pending_wrs"] == 2
    assert numa.classify_error_chain({"operation": "PUT", "status": 1010, "evidence": evidence})["closed"]


def test_legacy_urma_elapsed_format_keeps_its_value():
    observations = observe_error_evidence(["[URMA_WAIT_TIMEOUT] elapsedMs=.5"])
    assert observations["urma_timeout_elapsed_ms"] == 0.5


def test_failed_data_rpc_wins_over_successful_query_rpc():
    evidence = [
        "method=WorkerOCService.QueryAndGet e2e_us=1000 cntl_failed=0",
        "method=WorkerWorkerOCService.GetObjectRemote e2e_us=5000 cntl_failed=1 "
        "cntl_error_code=1008 RPC deadline exceeded",
    ]
    detail = bottleneck._classify_rpc_deadline_detail(1001, observe_error_evidence(evidence))
    assert detail["error_subcategory"] == "Data RPC deadline"


def test_successful_get_rpc_keeps_method_without_timeout_classification():
    observations = observe_error_evidence([
        "method=WorkerWorkerOCService.GetObjectRemote e2e_us=500 cntl_failed=0",
        "method=WorkerOCService.QueryAndGet e2e_us=700 cntl_failed=0",
    ])
    assert observations["get_method"]
    assert observations["query_method"]
    assert not observations["rpc_deadline"]
    assert not observations["urma_timeout"]


def test_error_code_without_deadline_text_is_still_classified():
    observations = observe_error_evidence([
        "method=WorkerWorkerOCService.GetObjectRemote e2e_us=5000 cntl_failed=1 cntl_error_code=1008"
    ])
    assert observations["rpc_deadline"]
    assert observations["failed_methods"] == ["WorkerWorkerOCService.GetObjectRemote"]


def test_numa_reuses_observations_when_provided(monkeypatch):
    evidence = ["[URMA-WAIT-TIMEOUT] elapsedMs=6 unexpectedly returned TCP payload"]
    observations = observe_error_evidence(evidence)
    monkeypatch.setattr(numa, "observe_error_evidence", lambda _: (_ for _ in ()).throw(
        AssertionError("NUMA reinterpreted persisted observations")))
    record = {"operation": "GET", "status": 1004, "evidence": evidence,
              "error_observations": observations}
    assert numa.classify_error_chain(record)["closed"]
