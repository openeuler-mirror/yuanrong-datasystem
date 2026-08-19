#!/usr/bin/env python3
"""Static routing contracts for the two post-triage analysis skills."""

from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]


def test_bottleneck_and_numa_skills_are_separate_post_processors():
    bottleneck = (ROOT / ".skills/ds-trace-bottleneck-analysis/SKILL.md").read_text(encoding="utf-8")
    numa = (ROOT / ".skills/ds-trace-numa-analysis/SKILL.md").read_text(encoding="utf-8")

    assert "Use when" in bottleneck.split("---", 2)[1]
    assert "Use when" in numa.split("---", 2)[1]
    assert "scripts/ds_trace_triage.py" in bottleneck
    assert "scripts/ds_trace_bottleneck.py" in bottleneck
    assert "scripts/ds_trace_numa_analysis.py" in numa
    assert "bottleneck.analysis.json" in numa
    assert "URMA_WAIT_TIMEOUT" in bottleneck
    assert "GetObjectRemote" in bottleneck
    assert "Client Get → 逻辑 URMA Write → WR分片" in bottleneck
    assert "WR耗时不可求和" in bottleneck
    assert "Meta Owner目标" in bottleneck
    assert "同 Worker 时间关联" in bottleneck
    assert "URMA_WAIT_TIMEOUT" in numa
    assert "缺失" in bottleneck and "未观测" in bottleneck
    assert "缺失" in numa and "未观测" in numa


def test_base_triage_skill_routes_instead_of_duplicating_specialist_workflows():
    triage = (ROOT / ".skills/ds-trace-triage/SKILL.md").read_text(encoding="utf-8")

    assert "ds-trace-bottleneck-analysis" in triage
    assert "ds-trace-numa-analysis" in triage
    assert triage.count("python3 scripts/ds_trace_bottleneck.py") <= 1
    assert "python3 scripts/ds_trace_numa_analysis.py" not in triage
