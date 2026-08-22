#!/usr/bin/env python3
"""Contract tests for PR2081 NUMA/chip trace analysis."""

from __future__ import annotations

import importlib.util
import io
import json
import tarfile
from pathlib import Path


SCRIPT = Path(__file__).resolve().parents[2] / "scripts" / "ds_trace_numa_analysis.py"


def load_module():
    spec = importlib.util.spec_from_file_location("ds_trace_numa_analysis", SCRIPT)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def write_archive(path: Path, *, include_timecollect: bool = False) -> None:
    members = [
        "trace_collect/core/1004_DS/getBuffer-1;dup",
        "trace_collect/time/GET_10000_20000/getBuffer-1;dup",
        "trace_collect/core/1010_DS/setStringView-2;put",
        "trace_collect/time/GET_5000_7000/getBuffer-3;single",
        "trace_collect/time/GET_5000_7000/getBuffer-3;single_1",
        "trace_collect/time/GET_5000_7000/getBuffer-4;missing",
    ]
    if include_timecollect:
        members.append("timeCollect/GET_7000_10000/getBuffer-5;timecollect")
    with tarfile.open(path, "w:gz") as archive:
        for member in members:
            info = tarfile.TarInfo(member)
            payload = b"must-not-be-parsed"
            info.size = len(payload)
            archive.addfile(info, io.BytesIO(payload))


def urma_event(total_ms: float, chips: dict[str, int] | None, *, request_id: int) -> dict:
    return {
        "event_type": "total",
        "cost_ms": total_ms,
        "request_id": request_id,
        "timestamp": "2026-08-17T15:30:51.000000",
        "worker": "worker-6",
        "src_chip_inflight": chips,
        "urma_inflight_wr_count": sum(chips.values()) if chips else None,
        "raw": f"[URMA_ELAPSED_TOTAL] urma_request_id:{request_id}",
    }


def trace(operation: str, *, status: int, latency_us: int, events: list[dict], evidence: list[str]) -> dict:
    return {
        "flows": {operation: 1},
        "access_latency_ms_by_role": {"client": {"max": latency_us / 1000}},
        "access_statuses": {str(status): 1},
        "ub_events": events,
        "evidence": [
            {"timestamp": "2026-08-17T15:30:51.000000", "worker": "worker-6", "raw": raw}
            for raw in evidence
        ],
    }


def fixture_models():
    duplicated_wr = urma_event(1.5, {"1": 9, "2": 13}, request_id=705684)
    summary = {
        "traces": {
            "getBuffer-1;dup": trace(
                "DS_KV_CLIENT_GET",
                status=1004,
                latency_us=15_893,
                events=[duplicated_wr, dict(duplicated_wr), urma_event(15.391, {"1": 10, "2": 12}, request_id=705685)],
                evidence=["[URMA_WAIT_TIMEOUT] elapsedMs=15.391 unexpectedly returned TCP payload"],
            ),
            "setStringView-2;put": trace(
                "DS_KV_CLIENT_PUT",
                status=1010,
                latency_us=15_118,
                events=[urma_event(1.501, {"1": 3, "2": 3}, request_id=12550)],
                evidence=["URMA_WAIT_TIMEOUT op=WRITE"],
            ),
            "getBuffer-3;single": trace(
                "DS_KV_CLIENT_GET",
                status=0,
                latency_us=5_500,
                events=[urma_event(0.8, {"2": 88}, request_id=10)],
                evidence=[],
            ),
            "getBuffer-4;missing": trace(
                "DS_KV_CLIENT_GET", status=0, latency_us=5_700, events=[], evidence=[]
            ),
        }
    }
    bottleneck = {
        "traces": [
            {
                "trace_id": trace_id,
                "client_ms": client_ms,
                "primary_problem": problem,
                "failed": status != 0,
                "status": status,
                "transport": "UB",
                "direct_data_worker": "worker-6",
                "evidence": [],
                "urma_requests": [],
            }
            for trace_id, client_ms, problem, status in (
                ("getBuffer-1;dup", 15.893, "URMA", 1004),
                ("getBuffer-3;single", 5.5, "URMA", 0),
                ("getBuffer-4;missing", 5.7, "未解释残差", 0),
            )
        ]
    }
    return summary, bottleneck


def test_cohort_membership_deduplicates_trace_and_preserves_overlap(tmp_path: Path):
    archive = tmp_path / "input.tar.gz"
    write_archive(archive, include_timecollect=True)
    module = load_module()

    cohorts = module.build_cohort_index(archive)

    assert set(cohorts) == {
        "getBuffer-1;dup", "setStringView-2;put", "getBuffer-3;single", "getBuffer-4;missing",
        "getBuffer-5;timecollect",
    }
    assert cohorts["getBuffer-1;dup"] == {"core/1004_DS", "time/GET_10000_20000"}
    assert cohorts["getBuffer-5;timecollect"] == {"time/GET_7000_10000"}
    assert module.count_archive_trace_files(archive) == 7


def test_chip_metrics_keep_missing_unobserved_and_use_strict_slow_threshold(tmp_path: Path):
    archive = tmp_path / "input.tar.gz"
    write_archive(archive)
    summary, bottleneck = fixture_models()
    module = load_module()

    records = module.build_trace_records(summary, bottleneck, module.build_cohort_index(archive))
    by_id = {item["trace_id"]: item for item in records}
    aggregate = module.build_aggregate(records, {"head": "8e4aa6967"})

    assert len(records) == 4
    assert by_id["getBuffer-1;dup"]["chip_mode"] == "双 chip"
    assert by_id["getBuffer-1;dup"]["chip1_peak"] == 10
    assert by_id["getBuffer-1;dup"]["chip2_peak"] == 13
    assert by_id["getBuffer-3;single"]["chip_mode"] == "仅 chip 2"
    assert by_id["getBuffer-4;missing"]["chip_mode"] == "未观测"
    assert by_id["getBuffer-4;missing"]["chip_skew"] is None
    assert by_id["getBuffer-1;dup"]["slow_wr_count"] == 1
    assert len(by_id["getBuffer-1;dup"]["urma_requests"]) == 2
    assert by_id["getBuffer-1;dup"]["timeout_elapsed_ms"] == 15.391
    assert by_id["setStringView-2;put"]["slow_wr_count"] == 1
    assert aggregate["unique_trace_count"] == 4
    assert aggregate["chip_mode_counts"] == {"双 chip": 2, "仅 chip 2": 1, "未观测": 1}


def test_real_triage_contract_parses_error_text_and_string_chip_map(tmp_path: Path):
    archive = tmp_path / "input.tar.gz"
    write_archive(archive)
    summary = {
        "traces": {
            "setStringView-2;put": {
                "flows": {"DS_KV_CLIENT_SET": 1},
                "access_latency_ms_by_role": {"client": {"max": 16.303}},
                "errors": {"URMA_WAIT_TIMEOUT": 4, "status=1010": 1},
                "ub_events": [
                    {
                        "event_type": "total",
                        "cost_ms": 1.501,
                        "src_chip_inflight": "{1:2,2:51}",
                        "worker": "client-5",
                        "raw": "[URMA_ELAPSED_TOTAL] srcChipInflight:{1:2,2:51}",
                    }
                ],
                "evidence": [
                    {
                        "text": "[URMA_WAIT_TIMEOUT] dataSize=4194304, op=WRITE status=1010",
                        "worker": "client-5",
                    }
                ],
            }
        }
    }
    module = load_module()

    records = module.build_trace_records(summary, {"traces": []}, module.build_cohort_index(archive))
    record = next(item for item in records if item["trace_id"] == "setStringView-2;put")

    assert record["status"] == 1010
    assert record["chip_mode"] == "双 chip"
    assert record["chip1_peak"] == 2
    assert record["chip2_peak"] == 51
    assert record["worker"] == "client-5"
    assert record["evidence"] == ["[URMA_WAIT_TIMEOUT] dataSize=4194304, op=WRITE status=1010"]
    assert module.classify_error_chain(record)["family"] == "PUT URMA WRITE等待超时1010"


def test_error_chains_separate_get_1004_from_put_1010(tmp_path: Path):
    archive = tmp_path / "input.tar.gz"
    write_archive(archive)
    summary, bottleneck = fixture_models()
    module = load_module()
    records = module.build_trace_records(summary, bottleneck, module.build_cohort_index(archive))
    by_id = {item["trace_id"]: item for item in records}

    get_error = module.classify_error_chain(by_id["getBuffer-1;dup"])
    put_error = module.classify_error_chain(by_id["setStringView-2;put"])

    assert get_error["family"] == "GET URMA超时后上浮1004"
    assert get_error["closed"] is True
    assert get_error["signals"] == ["URMA等待超时", "UB响应形态异常", "Client状态1004"]
    assert put_error["family"] == "PUT URMA WRITE等待超时1010"
    assert put_error["closed"] is True
    assert "UB响应形态异常" not in put_error["signals"]


def test_error_chain_reports_missing_timeout_as_unclosed():
    module = load_module()
    record = {
        "trace_id": "getBuffer-unclosed",
        "operation": "GET",
        "status": 1004,
        "evidence": ["Operation failed: Urma operation failed"],
    }

    result = module.classify_error_chain(record)

    assert result["family"] == "GET URMA错误1004（链路未闭合）"
    assert result["closed"] is False
    assert result["missing"] == ["URMA等待超时", "UB响应形态异常"]


def test_error_chain_separates_client_ub_receive_buffer_oom_from_urma_timeout():
    module = load_module()
    record = {
        "trace_id": "getBuffer-client-arena-oom",
        "operation": "GET",
        "status": 1004,
        "evidence": [
            "[TransportGet][UB] Receive buffer preparation failed, "
            "causeStatus=code: [Out of memory], msg: [UnknownType no space in arena: 3, "
            "reason=fresh_extent_unavailable]"
        ],
    }

    result = module.classify_error_chain(record)

    assert result["family"] == "GET UB接收缓冲分配失败1004"
    assert result["closed"] is True
    assert result["signals"] == ["Client UB接收缓冲准备失败", "Client arena内存不足", "Client状态1004"]
    assert result["missing"] == []


def test_insights_report_observed_get_1004_families_instead_of_assuming_timeout_band():
    module = load_module()
    records = [
        {
            "status": 1004,
            "chip_mode": "未观测",
            "timeout_elapsed_ms": None,
            "error_chain": {"family": "GET UB接收缓冲分配失败1004"},
        },
        {
            "status": 1004,
            "chip_mode": "未观测",
            "timeout_elapsed_ms": 15.5,
            "error_chain": {"family": "GET URMA超时后上浮1004"},
        },
    ]

    insight = module.build_insights(records, [], [])[2]

    assert insight["title"] == "GET 1004错误链"
    assert "UB接收缓冲分配失败1004=1" in insight["text"]
    assert "URMA超时后上浮1004=1" in insight["text"]
    assert "10–20ms" not in insight["title"]


def test_error_chain_normalizes_urma_wait_timeout_marker_variants():
    module = load_module()
    for marker in (
        "[URMA_WAIT_TIMEOUT] elapsedMs=15.391 unexpectedly returned TCP payload",
        "[URMA-WAIT-TIMEOUT] elapsedMs=15.391 unexpectedly returned TCP payload",
        "URMA wait timeout elapsedMs=15.391 unexpectedly returned TCP payload",
    ):
        result = module.classify_error_chain(
            {"operation": "GET", "status": 1004, "evidence": [marker]}
        )
        assert result["family"] == "GET URMA超时后上浮1004"
        assert result["closed"] is True


def test_latency_band_and_source_chain_are_data_and_source_derived(tmp_path: Path):
    archive = tmp_path / "input.tar.gz"
    write_archive(archive)
    summary, bottleneck = fixture_models()
    module = load_module()
    records = module.build_trace_records(summary, bottleneck, module.build_cohort_index(archive))

    bands = {item["cohort"]: item for item in module.summarize_latency_bands(records)}
    chain = module.build_source_chain(
        {
            "head": "8e4aa6967fef67ced6228b478924b33d0e703835",
            "base": "56b9e583097121fe7e48f388318a8417149ce8ef",
        }
    )

    assert bands["time/GET_10000_20000"]["unique_trace_count"] == 1
    assert bands["time/GET_5000_7000"]["unique_trace_count"] == 2
    assert bands["time/GET_5000_7000"]["chip_mode_counts"] == {"仅 chip 2": 1, "未观测": 1}
    assert [item["stage"] for item in chain] == [
        "arena配置", "NUMA内存绑定", "多arena分配", "chip信息传播", "发送端chip选择", "Trace观测"
    ]
    assert all(item["source_ref"] == "8e4aa6967fef67ced6228b478924b33d0e703835" for item in chain)


def test_time_buckets_keep_error_slow_wr_and_chip_dimensions():
    module = load_module()
    records = [
        {
            "timestamp": "2026-08-17T15:30:51.100000",
            "status": 1004,
            "slow_wr_count": 1,
            "chip_mode": "双 chip",
        },
        {
            "timestamp": "2026-08-17T15:30:51.900000",
            "status": 0,
            "slow_wr_count": 2,
            "chip_mode": "仅 chip 2",
        },
        {
            "timestamp": None,
            "status": 1010,
            "slow_wr_count": 0,
            "chip_mode": "未观测",
        },
    ]

    buckets = module.summarize_time_buckets(records)

    assert buckets == [
        {
            "second": "2026-08-17T15:30:51",
            "trace_count": 2,
            "error_count": 1,
            "slow_wr_count": 3,
            "dual_chip_count": 1,
        }
    ]


def write_run_contract(tmp_path: Path) -> tuple[Path, Path, Path]:
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    summary, bottleneck = fixture_models()
    (run_dir / "summary.json").write_text(json.dumps(summary), encoding="utf-8")
    (run_dir / "manifest.json").write_text(
        json.dumps({"case_name": "pr2081-numa", "code_ref": "8e4aa6967"}), encoding="utf-8"
    )
    bottleneck_path = tmp_path / "bottleneck.json"
    bottleneck_path.write_text(json.dumps(bottleneck), encoding="utf-8")
    archive = tmp_path / "input.tar.gz"
    write_archive(archive)
    return run_dir, bottleneck_path, archive


def test_build_analysis_and_renderer_are_self_contained(tmp_path: Path):
    module = load_module()
    run_dir, bottleneck_path, archive = write_run_contract(tmp_path)
    source = {
        "head": "8e4aa6967fef67ced6228b478924b33d0e703835",
        "base": "56b9e583097121fe7e48f388318a8417149ce8ef",
        "pr": 2095,
    }

    analysis = module.build_analysis(
        run_dir,
        bottleneck_path,
        archive,
        source,
        {
            "qps_per_node": 315,
            "client_count": 8,
            "threads_per_client": 16,
            "workers_per_node": 1,
        },
    )
    html = module.render_html(
        analysis, "window.echarts={init(){return {setOption(){},resize(){},dispose(){}}}};"
    )

    assert analysis["aggregate"]["unique_trace_count"] == 4
    assert analysis["aggregate"]["error_family_counts"] == {
        "GET URMA超时后上浮1004": 1,
        "PUT URMA WRITE等待超时1010": 1,
        "成功": 2,
    }
    assert analysis["latency_bands"]
    assert analysis["source_chain"]
    assert analysis["metadata"]["runtime_config"] == {
        "qps_per_node": 315,
        "client_count": 8,
        "threads_per_client": 16,
        "workers_per_node": 1,
        "qps_per_client": 39.375,
        "client_threads_per_node": 128,
    }
    assert {item["title"] for item in analysis["insights"]} >= {"多 chip 并发已观测", "短时延档主瓶颈"}
    chip_insight = next(item for item in analysis["insights"] if item["title"] == "多 chip 并发已观测")
    assert "不能证明队列均衡 override" in chip_insight["text"]
    assert any(item["stage"] == "inflight均衡" for item in analysis["source_chain"])
    assert "https://" not in html
    for marker in (
        "核心判断", "Core错误族", "GET/PUT时延档", "chip 1/2", "Worker与时间",
        "Top Trace", "Trace明细", "PR 2095源码链", "证据边界", "download-filtered",
        'href="bottleneck.html"', 'href="triage.html"', "runtime-config",
    ):
        assert marker in html
    assert "const DATA=" in html


def test_renderer_uses_runtime_pr_metadata_and_keeps_wide_tables_responsive(tmp_path: Path):
    module = load_module()
    run_dir, bottleneck_path, archive = write_run_contract(tmp_path)
    analysis = module.build_analysis(
        run_dir,
        bottleneck_path,
        archive,
        {"head": "head-ref", "base": "base-ref", "pr": 9999},
    )

    html = module.render_html(
        analysis, "window.echarts={init(){return {setOption(){},resize(){},dispose(){}}}};"
    )

    assert "PR 9999" in html
    assert "PR2081" not in html
    assert "PR 2081" not in html
    assert 'class="table-wrap"' in html
    assert ".table-wrap{width:100%;overflow-x:auto}" in html


def test_renderer_uses_generic_source_label_when_pr_is_not_supplied(tmp_path: Path):
    module = load_module()
    run_dir, bottleneck_path, archive = write_run_contract(tmp_path)
    analysis = module.build_analysis(
        run_dir,
        bottleneck_path,
        archive,
        {"head": "head-ref", "base": "head-ref", "pr": 0},
    )

    html = module.render_html(
        analysis, "window.echarts={init(){return {setOption(){},resize(){},dispose(){}}}};"
    )

    assert "PR 0" not in html
    assert "PR0" not in html
    assert "当前源码链" in html
    assert "源码基线" in html


def test_runtime_config_keeps_missing_values_unconfigured(tmp_path: Path):
    module = load_module()
    run_dir, bottleneck_path, archive = write_run_contract(tmp_path)

    analysis = module.build_analysis(run_dir, bottleneck_path, archive, {"head": "abc"})

    assert analysis["metadata"]["runtime_config"] == {
        "qps_per_node": None,
        "client_count": None,
        "threads_per_client": None,
        "workers_per_node": None,
        "qps_per_client": None,
        "client_threads_per_node": None,
    }


def test_cli_writes_json_and_html(tmp_path: Path):
    module = load_module()
    run_dir, bottleneck_path, archive = write_run_contract(tmp_path)
    echarts = tmp_path / "echarts.js"
    echarts.write_text("window.echarts={init(){return {setOption(){},resize(){},dispose(){}}}};", encoding="utf-8")
    output = tmp_path / "index.html"
    analysis_json = tmp_path / "analysis.json"

    rc = module.main(
        [
            "--run-dir", str(run_dir),
            "--bottleneck-analysis", str(bottleneck_path),
            "--archive", str(archive),
            "--source-head", "8e4aa6967fef67ced6228b478924b33d0e703835",
            "--source-base", "56b9e583097121fe7e48f388318a8417149ce8ef",
            "--qps-per-node", "315",
            "--client-count", "8",
            "--threads-per-client", "16",
            "--workers-per-node", "1",
            "--pr", "2081",
            "--echarts", str(echarts),
            "--output", str(output),
            "--analysis-json", str(analysis_json),
            "--force",
        ]
    )

    assert rc == 0
    assert output.exists()
    assert json.loads(analysis_json.read_text(encoding="utf-8"))["aggregate"]["unique_trace_count"] == 4
