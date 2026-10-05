"""FIFO admission for stage slots and caller-declared memory estimates, not RSS limits."""
from collections import deque
from contextlib import contextmanager
import math
import os
from numbers import Real
from pathlib import Path
from threading import Condition

AUTO_MEMORY_FRACTION = 0.8
BYTES_PER_MIB = 1024 * 1024
KIB_PER_MIB = 1024
CPU_QUOTA_FIELD_COUNT = 2
CGROUP_V2_PREFIX = "0::"


def parse_jobs(value):
    if value == "auto":
        return value
    try:
        jobs = int(value)
    except (ValueError, TypeError) as error:
        raise ValueError("jobs must be a positive integer or auto") from error
    if jobs < 1:
        raise ValueError("jobs must be a positive integer or auto")
    return jobs


def _resource_text(path):
    try:
        return path.read_text().strip()
    except OSError:
        return ""


def _cgroup_paths(proc_root, cgroup_root):
    current = cgroup_root
    for line in _resource_text(proc_root / "self/cgroup").splitlines():
        if line.startswith(CGROUP_V2_PREFIX):
            candidate = (cgroup_root / line[len(CGROUP_V2_PREFIX):].lstrip("/")).resolve()
            if candidate.is_relative_to(cgroup_root.resolve()) and candidate.is_dir():
                current = candidate
    paths = [current]
    while current != cgroup_root and current.is_relative_to(cgroup_root):
        current = current.parent
        paths.append(current)
    return paths


def host_resources(proc_root=Path("/proc"), cgroup_root=Path("/sys/fs/cgroup")):
    try:
        cores = len(os.sched_getaffinity(0))
    except (AttributeError, OSError):
        cores = os.cpu_count() or 1
    available_mb = None
    for line in _resource_text(proc_root / "meminfo").splitlines():
        if line.startswith("MemAvailable:"):
            available_mb = int(line.split()[1]) / KIB_PER_MIB
    for group in _cgroup_paths(proc_root, cgroup_root):
        cpu = _resource_text(group / "cpu.max").split()
        valid_quota = (len(cpu) == CPU_QUOTA_FIELD_COUNT and cpu[0].isdigit()
                       and cpu[1].isdigit() and int(cpu[1]) > 0)
        if valid_quota:
            cores = min(cores, max(1, int(cpu[0]) // int(cpu[1])))
        limit = _resource_text(group / "memory.max")
        used = _resource_text(group / "memory.current")
        if limit.isdigit() and used.isdigit():
            remaining_mb = max(0, int(limit) - int(used)) / BYTES_PER_MIB
            available_mb = remaining_mb if available_mb is None else min(available_mb, remaining_mb)
    return max(1, cores), available_mb


def resolve_execution(jobs, memory_mb, estimates, stages):
    if jobs != "auto":
        ResourceBudget(jobs, memory_mb)
        return jobs, memory_mb, {"requested_jobs": jobs}
    cores, available_mb = host_resources()
    if available_mb is None or not _positive_number(available_mb):
        raise ValueError("auto jobs requires observable available memory; use explicit jobs and memory_mb")
    automatic_mb = available_mb * AUTO_MEMORY_FRACTION
    if memory_mb is not None:
        if not _positive_number(memory_mb):
            raise ValueError("memory_mb must be a finite positive number")
        automatic_mb = min(automatic_mb, memory_mb)
    for stage in stages:
        if not _positive_number(estimates.get(stage)):
            raise ValueError(f"auto jobs requires a measured positive memory estimate for {stage}")
    maximum_mb = max(estimates[stage] for stage in stages)
    if maximum_mb > automatic_mb:
        raise ValueError("stage memory estimate exceeds the available automatic memory budget")
    slots = min(cores, int(automatic_mb // maximum_mb))
    return slots, automatic_mb, {"requested_jobs": "auto", "host_cpu_count": cores,
                                 "host_available_memory_mb": available_mb,
                                 "auto_memory_fraction": AUTO_MEMORY_FRACTION,
                                 "maximum_stage_estimate_mb": maximum_mb}


def _positive_number(value):
    return isinstance(value, Real) and not isinstance(value, bool) and math.isfinite(value) and value > 0


class ResourceBudget:
    def __init__(self, slots, memory_mb=None):
        if not isinstance(slots, int) or isinstance(slots, bool) or slots < 1:
            raise ValueError("slots must be a positive integer")
        if memory_mb is not None and not _positive_number(memory_mb):
            raise ValueError("memory_mb must be a finite positive number")
        self.slots = slots
        self.memory_mb = memory_mb
        self._condition = Condition()
        self._queue = deque()
        self._active = 0
        self._reserved_mb = 0

    def snapshot(self):
        with self._condition:
            return {"active": self._active, "waiting": len(self._queue), "reserved_mb": self._reserved_mb}

    def _estimate(self, estimated_mb):
        if estimated_mb is None:
            if self.memory_mb is not None:
                raise ValueError("estimated_mb is required when a memory budget is configured")
            return 0
        if not _positive_number(estimated_mb):
            raise ValueError("estimated_mb must be a finite positive number")
        if self.memory_mb is not None and estimated_mb > self.memory_mb:
            raise ValueError("estimated_mb exceeds the entire memory budget")
        return estimated_mb

    @contextmanager
    def acquire(self, estimated_mb=None):
        estimate = self._estimate(estimated_mb)
        ticket = object()
        with self._condition:
            self._queue.append(ticket)
            try:
                self._condition.wait_for(lambda: (
                    self._queue[0] is ticket and self._active < self.slots
                    and (self.memory_mb is None or self._reserved_mb + estimate <= self.memory_mb)
                ))
            except BaseException:
                self._queue.remove(ticket)
                self._condition.notify_all()
                raise
            self._queue.popleft()
            self._active += 1
            self._reserved_mb += estimate
            self._condition.notify_all()
        try:
            yield
        finally:
            with self._condition:
                self._active -= 1
                self._reserved_mb -= estimate
                if self._active == 0:
                    self._reserved_mb = 0
                self._condition.notify_all()
