#!/usr/bin/env python3
"""Contract tests for the isolated multi-run bottleneck suite."""

from __future__ import annotations

import importlib.util
import io
import json
import tarfile
from pathlib import Path

import pytest


SCRIPT = Path(__file__).resolve().parents[2] / "scripts" / "ds_trace_bottleneck_suite.py"


def load_module():
    spec = importlib.util.spec_from_file_location("ds_trace_bottleneck_suite", SCRIPT)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def write_archive(path: Path, members: list[str]) -> None:
    with tarfile.open(path, "w:gz") as archive:
        for member in members:
            info = tarfile.TarInfo(member)
            payload = b"content-must-not-be-parsed"
            info.size = len(payload)
            archive.addfile(info, io.BytesIO(payload))


def row(trace_id: str, client_ms: float, problem: str, *, transport: str = "UB", urma_ms=None) -> dict:
    requests = [] if urma_ms is None else [{"total_ms": urma_ms, "is_slow": urma_ms > 1.5}]
    return {
        "trace_id": trace_id,
        "client_ms": client_ms,
        "failed": client_ms > 20,
        "primary_problem": problem,
        "transport": transport,
        "access_location": "远端Data Worker" if transport == "UB" else "本节点SHM",
        "attribution_ms": {
            "RPC网络": 0.0,
            "QueryMeta": client_ms if problem == "QueryMeta" else 0.0,
            "URMA": urma_ms if urma_ms is not None else 0.0,
            "远端供数处理": 0.0,
            "数据访问父窗口/未细分": 0.0,
            "未解释残差": 0.0,
        },
        "urma_observed": urma_ms is not None,
        "urma_requests": requests,
    }


def write_analysis(path: Path, rows: list[dict]) -> None:
    path.write_text(json.dumps({"schema_version": 1, "traces": rows}), encoding="utf-8")


def run_cfg(tmp_path: Path, run_id: str, *, implementation: str, load: str, size: str, rows: list[dict], cap=1000):
    archive = tmp_path / f"{run_id}.tar.gz"
    analysis = tmp_path / f"{run_id}.json"
    member_by_latency = []
    for item in rows:
        band = "GET_5000_7000" if item["client_ms"] < 7 else "GET_7000_10000"
        member_by_latency.append(f"{run_id}/{band}/{item['trace_id']}")
    write_archive(archive, member_by_latency)
    write_analysis(analysis, rows)
    return {
        "id": run_id,
        "label": run_id,
        "implementation": implementation,
        "local_cache": implementation == "true",
        "placement": "preferred-same-node" if implementation == "same" else "preferred-meta-owner" if implementation == "meta" else "bound-worker",
        "size": size,
        "load": load,
        "client_shape": "single" if load == "315" else load.split("x", 1)[0] + " clients" if "x" in load else "single",
        "input_archive": str(archive),
        "analysis_json": str(analysis),
        "triage_report": f"reports/{run_id}-triage.html",
        "bottleneck_report": f"reports/{run_id}-bottleneck.html",
        "numa_report": f"reports/{run_id}-numa.html",
        "sampling_cap_per_band": cap,
    }


def manifest(tmp_path: Path) -> dict:
    value = {
        "schema_version": 1,
        "title": "0818 SAME vs META WR均衡对比",
        "source_ref": "main-ref",
        "sampling": {"kind": "capped-anomaly-bands", "max_per_band": 1000},
        "runs": [
            run_cfg(tmp_path, "true-315", implementation="true", load="315", size="8MB", rows=[row("t1", 5.5, "URMA", transport="SHM", urma_ms=1.5)]),
            run_cfg(tmp_path, "same-315", implementation="same", load="315", size="8MB", rows=[row("s1", 5.8, "URMA", urma_ms=1.501)]),
            run_cfg(tmp_path, "meta-315", implementation="meta", load="315", size="8MB", rows=[row("m1", 6.2, "QueryMeta")]),
            run_cfg(tmp_path, "meta-3x105", implementation="meta", load="3x105", size="8MB", rows=[row("m2", 8.8, "URMA", urma_ms=2.0)]),
            run_cfg(tmp_path, "meta-105-1kb", implementation="meta", load="105", size="1KB", rows=[row("m3", 6.0, "QueryMeta")]),
            run_cfg(tmp_path, "meta-105-8mb", implementation="meta", load="105", size="8MB", rows=[row("m4", 6.2, "URMA", urma_ms=2.2)]),
        ],
    }
    value["runs"][1]["read_path"] = "legacy-worker-pull"
    return value


def test_build_suite_keeps_runs_isolated_and_builds_control_groups(tmp_path: Path):
    value = manifest(tmp_path)
    value["overview"] = [{"title": "证据结论", "text": "两个 Run 独立解析"}]
    suite = load_module().build_suite(value)

    assert [item["id"] for item in suite["runs"]] == [
        "true-315", "same-315", "meta-315", "meta-3x105", "meta-105-1kb", "meta-105-8mb"
    ]
    assert [item["trace_count"] for item in suite["runs"]] == [1, 1, 1, 1, 1, 1]
    assert suite["overview"] == [{"title": "证据结论", "text": "两个 Run 独立解析"}]
    assert suite["runs"][1]["read_path"] == "legacy-worker-pull"
    families = {group["family"] for group in suite["control_groups"]}
    assert {"implementation", "client_shape", "object_size"}.issubset(families)
    implementation = next(group for group in suite["control_groups"] if group["family"] == "implementation")
    assert implementation["run_ids"] == ["true-315", "same-315", "meta-315"]
    implementation_insight = next(item for item in suite["insights"] if item["group_id"] == implementation["id"])
    assert implementation_insight["band"] == "5–7ms"
    assert "true-315" in implementation_insight["text"]
    assert "档内占比" in implementation_insight["text"]


def test_suite_prefers_focus_breakdown_and_exposes_data_access_scope(tmp_path: Path):
    focused = row("focused", 6.2, "数据访问父窗口/未细分", urma_ms=5.8)
    focused["focus_primary_problem"] = "URMA通信"
    focused["focus_breakdown_ms"] = {
        "URMA建链": 0.0,
        "URMA通信": 5.8,
        "URMA调度/线程开销": 0.0,
        "QueryAndGet其他业务": 0.0,
        "Get其他业务": 0.2,
        "其他调度/线程开销": 0.0,
        "RPC网络相关": 0.0,
        "RPC框架": 0.0,
        "未解释残差": 0.2,
    }
    focused["data_access_scope"] = "URMA慢完成"
    value = {
        "schema_version": 1,
        "runs": [
            run_cfg(
                tmp_path,
                "focused-run",
                implementation="meta",
                load="315",
                size="8MB",
                rows=[focused],
            )
        ],
    }

    suite = load_module().build_suite(value)
    band = suite["runs"][0]["bands"]["5–7ms"]

    assert band["dominant_problem"] == "URMA通信"
    assert band["problem_counts"]["URMA通信"] == 1
    assert band["stage_p90_ms"]["URMA通信"] == pytest.approx(5.8)
    assert band["data_access_scope_counts"] == {"URMA慢完成": 1}
    assert band["data_access_scope_shares_pct"] == {"URMA慢完成": 100.0}

    html = load_module().render_suite_html(suite, "window.echarts={};")
    assert "数据访问定位细分" in html
    assert 'id="scope"' in html
    assert "not occurrence rates" in " ".join(suite["limitations"])


def test_band_mapping_uses_member_names_and_missing_evidence_stays_unobserved(tmp_path: Path):
    cfg = run_cfg(
        tmp_path,
        "case",
        implementation="meta",
        load="315",
        size="8MB",
        rows=[row("a", 5.5, "URMA", urma_ms=1.5), row("b", 8.1, "QueryMeta", urma_ms=None)],
    )
    suite = load_module().build_suite({"schema_version": 1, "source_ref": "ref", "sampling": {"kind": "capped-anomaly-bands", "max_per_band": 1000}, "runs": [cfg]})
    bands = suite["runs"][0]["bands"]

    assert bands["5–7ms"]["sample_count"] == 1
    assert bands["7–10ms"]["sample_count"] == 1
    assert bands["5–7ms"]["slow_urma_wr_count"] == 0
    assert bands["7–10ms"]["urma_wr_p90_ms"] is None
    assert suite["runs"][0]["unmatched_trace_count"] == 0


def test_duplicate_ids_and_unmatched_traces_are_rejected(tmp_path: Path):
    cfg = run_cfg(tmp_path, "dup", implementation="same", load="315", size="8MB", rows=[row("x", 5.5, "URMA", urma_ms=2)])
    module = load_module()
    with pytest.raises(ValueError, match="duplicate run id"):
        module.build_suite({"schema_version": 1, "source_ref": "ref", "sampling": {}, "runs": [cfg, cfg]})

    analysis = Path(cfg["analysis_json"])
    write_analysis(analysis, [row("not-in-archive", 5.5, "URMA", urma_ms=2)])
    with pytest.raises(ValueError, match="unmatched Trace IDs"):
        module.build_suite({"schema_version": 1, "source_ref": "ref", "sampling": {}, "runs": [cfg]})


def test_load_manifest_resolves_data_paths_relative_to_manifest(tmp_path: Path):
    cfg = run_cfg(tmp_path, "relative", implementation="meta", load="105", size="8MB", rows=[row("r", 5.5, "URMA", urma_ms=2)])
    cfg["input_archive"] = Path(cfg["input_archive"]).name
    cfg["analysis_json"] = Path(cfg["analysis_json"]).name
    path = tmp_path / "suite.manifest.json"
    path.write_text(json.dumps({"schema_version": 1, "source_ref": "ref", "sampling": {}, "runs": [cfg]}), encoding="utf-8")

    loaded = load_module().load_manifest(path)

    assert loaded["runs"][0]["input_archive"] == str(tmp_path / cfg["input_archive"])
    assert loaded["runs"][0]["analysis_json"] == str(tmp_path / cfg["analysis_json"])


def test_renderer_is_self_contained_and_links_every_run(tmp_path: Path):
    module = load_module()
    suite = module.build_suite(manifest(tmp_path))
    html = module.render_suite_html(suite, "window.echarts={init(){return {setOption(){},resize(){}}}};")

    assert "https://" not in html
    assert "0818 SAME vs META WR均衡对比" in html
    assert "capped anomaly samples" in html
    assert "chart-title" in html
    assert "overflow-x:auto" not in html
    for item in suite["runs"]:
        assert item["triage_report"] in html
        assert item["bottleneck_report"] in html
        assert item["numa_report"] in html


def test_renderer_escapes_manifest_overview_before_inserting_html(tmp_path: Path):
    value = manifest(tmp_path)
    value["overview"] = [{"title": "<img onerror=alert(1)>", "text": "<script>alert(1)</script>"}]
    module = load_module()
    html = module.render_suite_html(
        module.build_suite(value),
        "window.echarts={init(){return {setOption(){},resize(){}}}};",
    )

    assert "esc(x.title)" in html
    assert "esc(x.text)" in html


def test_companion_skill_contract_and_base_parser_is_not_imported():
    root = Path(__file__).resolve().parents[2]
    skill = root / ".skills" / "ds-trace-bottleneck-analysis" / "SKILL.md"
    text = skill.read_text(encoding="utf-8")
    for marker in ("ds_trace_triage.py", "capped", "run isolation", "control variable", "unobserved"):
        assert marker in text
    source = SCRIPT.read_text(encoding="utf-8")
    assert "import ds_trace_triage" not in source
    assert "from ds_trace_triage" not in source
