#!/usr/bin/env python3
"""Routing and evidence contracts for the unified Trace skill."""
from trace_test_loader import REPO_ROOT

from pathlib import Path


ROOT = REPO_ROOT


def test_bottleneck_and_numa_references_are_separate_post_processors():
    bottleneck = (ROOT / ".skills/ds-trace-analysis-pipeline/references/bottleneck.md").read_text(encoding="utf-8")
    numa = (ROOT / ".skills/ds-trace-analysis-pipeline/references/numa.md").read_text(encoding="utf-8")

    assert "scripts/ds_trace_analysis.py triage" in bottleneck
    assert "scripts/ds_trace_analysis.py read" in bottleneck
    assert "scripts/ds_trace_analysis.py numa" in numa
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
    triage = (ROOT / ".skills/ds-trace-analysis-pipeline/references/triage.md").read_text(encoding="utf-8")

    assert "(bottleneck.md)" in triage
    assert "(numa.md)" in triage
    assert triage.count("python3 scripts/ds_trace_analysis.py read") <= 1
    assert "python3 scripts/ds_trace_analysis.py numa" not in triage
    assert "读取九阶段" in triage
    assert "七阶段" not in triage


def test_bottleneck_skill_routes_multi_run_suite_without_reparsing_raw_logs():
    bottleneck = (ROOT / ".skills/ds-trace-analysis-pipeline/references/bottleneck.md").read_text(encoding="utf-8")

    assert "scripts/ds_trace_analysis.py suite" in bottleneck
    assert "Multi-run control variable analysis" in bottleneck
    assert "每个 Run" in bottleneck or "every configured run" in bottleneck
    assert "must never merge Trace rows across runs" in bottleneck
    assert "capped anomaly samples" in bottleneck
    assert "not an occurrence rate" in bottleneck
    assert "implementation" in bottleneck
    assert "object size" in bottleneck


def test_repository_context_registers_trace_analysis_workflows():
    registry = (ROOT / ".repo_context/modules/overview/repository-skills.md").read_text(encoding="utf-8")
    routing = (ROOT / ".repo_context/playbooks/upkeep/skill-trigger-routing.md").read_text(encoding="utf-8")

    for skill in (
        "ds-trace-analysis-pipeline",
    ):
        assert f"`{skill}`" in registry
        assert f"`{skill}`" in routing


def test_unified_skill_references_exist_and_old_skills_are_removed():
    import re
    skill = ROOT / ".skills/ds-trace-analysis-pipeline/SKILL.md"
    for link in re.findall(r"\]\(([^)#]+)(?:#[^)]*)?\)", skill.read_text()):
        assert (skill.parent / link).is_file(), link
    for name in ("triage", "bottleneck-analysis", "numa-analysis"):
        assert not (ROOT / f".skills/ds-trace-{name}/SKILL.md").exists()


def test_mode_docs_use_single_skill_and_public_entry():
    import re

    skill_root = ROOT / ".skills/ds-trace-analysis-pipeline"
    references = [skill_root / "references" / f"{name}.md"
                  for name in ("triage", "bottleneck", "numa", "reports")]
    docs = references + [
        ROOT / "docs/source_zh_cn/appendix/trace_analysis_usage.md",
        ROOT / "docs/source_zh_cn/appendix/trace_triage_methodology.md",
        ROOT / ".repo_context/modules/infra/observability/performance-troubleshooting.md",
    ]
    retired_command = re.compile(r"python3\s+scripts/ds_trace_(?!analysis\.py\b)[a-z_]+\.py")
    for path in docs:
        content = path.read_text(encoding="utf-8")
        assert not retired_command.search(content), path
    for path in references:
        assert "not a separate skill" in path.read_text(encoding="utf-8"), path
