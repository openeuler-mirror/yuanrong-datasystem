"""Admission limits apply to declared estimates, with FIFO ordering and exception release."""
from trace_test_loader import REPO_ROOT
import importlib
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest

sys.path.insert(0, str(REPO_ROOT / "scripts"))
ResourceBudget = importlib.import_module("trace_analysis.execution_budget").ResourceBudget
execution_budget = importlib.import_module("trace_analysis.execution_budget")


def wait_for(predicate):
    deadline = time.monotonic() + 3
    while not predicate():
        if time.monotonic() >= deadline:
            pytest.fail("admission state did not converge")
        time.sleep(0.005)


@pytest.mark.parametrize("slots,memory", [(0, None), (-1, None), (True, None), (1.5, None),
                                          (1, 0), (1, -1), (1, float("nan")), (1, float("inf"))])
def test_invalid_budget_configuration_is_rejected(slots, memory):
    with pytest.raises(ValueError):
        ResourceBudget(slots, memory)


@pytest.mark.parametrize("estimate", [None, 101, 0, -1, float("nan"), float("inf"), True])
def test_unknown_or_impossible_estimate_is_rejected_before_queueing(estimate):
    budget = ResourceBudget(2, 100)
    with pytest.raises(ValueError):
        with budget.acquire(estimate):
            pytest.fail("inadmissible task executed")
    assert budget.snapshot() == {"active": 0, "waiting": 0, "reserved_mb": 0}


def test_failure_releases_slot_and_memory():
    budget = ResourceBudget(1, 100)
    with pytest.raises(RuntimeError, match="producer failure"):
        with budget.acquire(75):
            assert budget.snapshot() == {"active": 1, "waiting": 0, "reserved_mb": 75}
            raise RuntimeError("producer failure")
    with budget.acquire(100):
        assert budget.snapshot()["reserved_mb"] == 100
    assert budget.snapshot() == {"active": 0, "waiting": 0, "reserved_mb": 0}


def test_slots_only_mode_allows_unknown_estimate_and_blocks_at_slot_limit():
    budget = ResourceBudget(1)
    entered = threading.Event()
    def task():
        with budget.acquire():
            entered.set()
    with ThreadPoolExecutor(max_workers=1) as pool:
        with budget.acquire():
            future = pool.submit(task)
            wait_for(lambda: budget.snapshot()["waiting"] == 1)
            assert not entered.is_set()
            assert budget.snapshot()["active"] == 1
        future.result(timeout=3)
    assert entered.is_set()
    assert budget.snapshot()["active"] == 0


def test_memory_admission_is_fifo_even_when_smaller_task_would_fit():
    budget = ResourceBudget(3, 100)
    large_entered, small_entered = threading.Event(), threading.Event()
    release_large = threading.Event()
    order = []
    def large():
        with budget.acquire(80):
            order.append("large")
            large_entered.set()
            assert release_large.wait(3)
    def small():
        with budget.acquire(40):
            order.append("small")
            small_entered.set()
    with ThreadPoolExecutor(max_workers=2) as pool:
        try:
            with budget.acquire(60):
                first = pool.submit(large)
                wait_for(lambda: budget.snapshot()["waiting"] == 1)
                second = pool.submit(small)
                wait_for(lambda: budget.snapshot()["waiting"] == 2)
                assert not large_entered.is_set()
                assert not small_entered.is_set()
            assert large_entered.wait(3)
            assert not small_entered.is_set()
            assert budget.snapshot()["reserved_mb"] == 80
        finally:
            release_large.set()
        first.result(timeout=3)
        second.result(timeout=3)
    assert order == ["large", "small"]
    assert budget.snapshot() == {"active": 0, "waiting": 0, "reserved_mb": 0}


def test_two_stages_run_concurrently_when_slots_and_memory_fit():
    budget = ResourceBudget(2, 100)
    entered, release = threading.Event(), threading.Event()
    def task():
        with budget.acquire(40):
            entered.set()
            assert release.wait(3)
    with ThreadPoolExecutor(max_workers=1) as pool:
        with budget.acquire(60):
            future = pool.submit(task)
            try:
                assert entered.wait(3)
                assert budget.snapshot() == {"active": 2, "waiting": 0, "reserved_mb": 100}
            finally:
                release.set()
            future.result(timeout=3)
    assert budget.snapshot() == {"active": 0, "waiting": 0, "reserved_mb": 0}


@pytest.mark.parametrize("cores,memory,expected", [(80, 98304, 19), (4, 102400, 4)])
def test_auto_jobs_respects_cpu_and_available_memory(monkeypatch, cores, memory, expected):
    monkeypatch.setattr(execution_budget, "host_resources", lambda: (cores, memory), raising=False)
    slots, budget, decision = execution_budget.resolve_execution("auto", None, {"triage": 4096}, ("triage",))
    assert slots == expected
    assert budget == pytest.approx(memory * 0.8)
    assert decision["host_cpu_count"] == cores
    assert decision["host_available_memory_mb"] == memory


def test_explicit_jobs_above_eight_is_preserved():
    slots, memory, _ = execution_budget.resolve_execution(24, None, {}, ())
    assert slots == 24
    assert memory is None


@pytest.mark.parametrize("jobs", [0, -1, True, 1.5])
def test_explicit_jobs_rejects_invalid_slot_counts(jobs):
    with pytest.raises(ValueError, match="positive integer"):
        execution_budget.resolve_execution(jobs, None, {}, ())


@pytest.mark.parametrize("value,expected", [("auto", "auto"), ("24", 24)])
def test_cli_jobs_accepts_auto_and_positive_integers(value, expected):
    assert execution_budget.parse_jobs(value) == expected


@pytest.mark.parametrize("value", ["0", "-1", "1.5", "many"])
def test_cli_jobs_rejects_invalid_values(value):
    with pytest.raises(ValueError, match="jobs"):
        execution_budget.parse_jobs(value)


def test_auto_jobs_honors_smaller_declared_memory_budget(monkeypatch):
    monkeypatch.setattr(execution_budget, "host_resources", lambda: (80, 102400), raising=False)
    slots, memory, _ = execution_budget.resolve_execution("auto", 12288, {"triage": 4096}, ("triage",))
    assert slots == 3
    assert memory == 12288


@pytest.mark.parametrize("estimates", [{}, {"triage": True}, {"triage": float("inf")}, {"triage": 0}])
def test_auto_jobs_rejects_missing_or_invalid_estimates(monkeypatch, estimates):
    monkeypatch.setattr(execution_budget, "host_resources", lambda: (80, 102400), raising=False)
    with pytest.raises(ValueError, match="estimate"):
        execution_budget.resolve_execution("auto", None, estimates, ("triage",))


@pytest.mark.parametrize("available", [None, 100])
def test_auto_jobs_rejects_unavailable_or_insufficient_memory(monkeypatch, available):
    monkeypatch.setattr(execution_budget, "host_resources", lambda: (80, available), raising=False)
    with pytest.raises(ValueError, match="memory"):
        execution_budget.resolve_execution("auto", None, {"triage": 4096}, ("triage",))


def test_host_resources_uses_affinity_and_cgroup_limits(tmp_path, monkeypatch):
    proc = tmp_path / "proc"
    group = tmp_path / "cgroup"
    proc.mkdir()
    group.mkdir()
    (proc / "meminfo").write_text("MemAvailable: 104857600 kB\n")
    (group / "cpu.max").write_text("1200000 100000\n")
    (group / "memory.max").write_text(str(16 * 1024**3))
    (group / "memory.current").write_text(str(4 * 1024**3))
    monkeypatch.setattr(execution_budget.os, "sched_getaffinity", lambda _: set(range(80)))
    assert execution_budget.host_resources(proc, group) == (12, 12288)


def test_host_resources_includes_current_cgroup_and_ancestor_limits(tmp_path, monkeypatch):
    proc = tmp_path / "proc"
    group = tmp_path / "cgroup"
    (proc / "self").mkdir(parents=True)
    (group / "workload").mkdir(parents=True)
    (proc / "self/cgroup").write_text("0::/workload\n")
    (proc / "meminfo").write_text("MemAvailable: 104857600 kB\n")
    (group / "cpu.max").write_text("400000 100000\n")
    (group / "workload/cpu.max").write_text("800000 100000\n")
    (group / "workload/memory.max").write_text(str(8 * 1024**3))
    (group / "workload/memory.current").write_text(str(2 * 1024**3))
    monkeypatch.setattr(execution_budget.os, "sched_getaffinity", lambda _: set(range(80)))
    assert execution_budget.host_resources(proc, group) == (4, 6144)
