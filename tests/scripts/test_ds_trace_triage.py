import importlib.util
import gzip
import io
import json
import re
import tarfile
from pathlib import Path

import pytest


SCRIPT = Path(__file__).resolve().parents[2] / "scripts" / "ds_trace_triage.py"


def _load_module():
    spec = importlib.util.spec_from_file_location("ds_trace_triage", SCRIPT)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _write_tar_gz(path, files):
    with tarfile.open(path, "w:gz") as tar:
        for name, content in files.items():
            data = content.encode("utf-8")
            info = tarfile.TarInfo(name)
            info.size = len(data)
            tar.addfile(info, io.BytesIO(data))


def test_tar_gz_trace_bundle_is_parsed_by_trace_and_key_dimensions(tmp_path):
    trace_id = "019f7b27-56f0-74f0-9a68-5b3742f11e23"
    bundle = tmp_path / "trace-bundle.tar.gz"
    _write_tar_gz(
        bundle,
        {
            "kvchachjpworker-0-worker7/worker.log": "\n".join(
                [
                    f"2026-07-18T19:20:03.100000 | INFO | access_recorder | 192.0.2.10 | 42 | {trace_id} | - | 0 | DS_KV_CLIENT_GET | 518923 | 4096",
                    f"2026-07-18T19:20:03.130000 | INFO | worker | 192.0.2.10 | 42 | {trace_id} | [Get] Done, totalCost: 518.9ms, exceed 3ms: {{ ProcessGetObjectRequest: 517 ms, QueryMeta: 0 ms }}",
                    f"2026-07-18T19:20:03.150000 | WARN | worker | 192.0.2.10 | 42 | {trace_id} | [ZMQ_RPC_FRAMEWORK_SLOW] e2e_us=8012 client_req_framework_us=100 remote_processing_us=7600 client_rsp_framework_us=120 server_req_queue_us=20 server_exec_us=7500 server_rsp_queue_us=80 network_residual_us=292 method=WorkerOCService.Get",
                    f"2026-07-18T19:20:03.200000 | WARN | worker | 192.0.2.20 | 42 | {trace_id} | [URMA_ELAPSED_TOTAL] cost 517.732ms, request id:77, src address: 192.0.2.20, target address: 192.0.2.10, dataSize:4194304, cpuid:12, status: OK",
                    f"2026-07-18T19:20:03.201000 | WARN | worker | 192.0.2.20 | 42 | {trace_id} | [URMA_ELAPSED_POLL_JFC] cost 0.309ms, request id:77",
                    f"2026-07-18T19:20:03.202000 | WARN | worker | 192.0.2.20 | 42 | {trace_id} | [URMA_ELAPSED_NOTIFY] cost 0.041ms, request id:77",
                    f"2026-07-18T19:20:03.203000 | WARN | worker | 192.0.2.20 | 42 | {trace_id} | [URMA_ELAPSED_THREAD_SHED] cost 12.500ms, request id:77",
                    f"2026-07-18T19:20:03.230000 | ERROR | worker | 192.0.2.10 | 42 | {trace_id} | RPC deadline exceeded while waiting WorkerOCService.Get",
                ]
            )
        },
    )

    mod = _load_module()
    report = mod.analyze_inputs([str(bundle)], code_ref="unit-test")

    assert report["schema_version"] == 1
    assert report["trace_count"] == 1
    assert report["dimensions"]["time"]["first_ts"].startswith("2026-07-18T19" + ":20:03")
    assert report["dimensions"]["workers"]["kvchachjpworker-0-worker7"]["line_count"] == 8
    assert report["dimensions"]["flow"]["DS_KV_CLIENT_GET"] == 1
    assert report["dimensions"]["latency_ms"]["access"]["p50"] == 518.923
    assert report["dimensions"]["breakdown_ms"]["ProcessGetObjectRequest"]["sum"] == 517.0
    assert report["dimensions"]["rpc_slow"]["WorkerOCService.Get"]["count"] == 1
    assert report["dimensions"]["rpc_slow"]["WorkerOCService.Get"]["server_exec_us"]["p50"] == 7500
    assert report["dimensions"]["rpc_slow"]["WorkerOCService.Get"]["network_residual_us"]["p50"] == 292
    assert report["dimensions"]["urma_elapsed"]["total"]["p50"] == 517.732
    assert report["dimensions"]["urma_elapsed"]["poll_jfc"]["p50"] == 0.309
    assert report["dimensions"]["urma_elapsed"]["notify"]["p50"] == 0.041
    assert report["dimensions"]["urma_elapsed"]["thread_sched"]["p50"] == 12.5
    assert report["dimensions"]["errors"]["RPC deadline exceeded"] == 1
    diagnosis = report["dimensions"]["diagnosis"]
    assert diagnosis["symptom_line"]["label"] == "错误线"
    assert "RPC deadline exceeded" in diagnosis["symptom_line"]["text"]
    assert diagnosis["latency_line"]["label"] == "慢时延线"
    assert "518.923" in diagnosis["latency_line"]["text"]
    assert diagnosis["evidence_boundary"]["label"] == "证据边界"
    assert diagnosis["customer_expression"]["label"] == "客户表达"
    recommendations = report["dimensions"]["recommendations"]
    assert any(item["category"] == "source_validation" for item in recommendations)
    assert any(item["category"] == "observability" for item in recommendations)
    assert any(item["category"] == "ub_urma" for item in recommendations)
    source_appendix = report["dimensions"]["source_appendix"]
    assert {"通用", "读取", "写入"}.issubset({item["scope"] for item in source_appendix})
    assert any(item["log_surface"] == "access log" for item in source_appendix)
    assert any(item["log_surface"] == "URMA_ELAPSED_TOTAL" for item in source_appendix)
    assert any("CodeGraph" in item["validation"] for item in source_appendix)
    assert any("client -> entry worker" in item["flow_stage"] for item in source_appendix)
    assert any("entry worker -> data worker" in item["flow_stage"] for item in source_appendix)
    assert any("data worker UB write" in item["flow_stage"] for item in source_appendix)
    assert any("entry worker -> meta worker publish" in item["flow_stage"] for item in source_appendix)
    flow_stages = report["dimensions"]["flow_stages"]
    assert "read" in flow_stages
    assert "write" in flow_stages
    assert any(edge["name"] == "read: entry worker -> data worker"
               for edge in flow_stages["read"]["edges"])
    assert any(edge["name"] == "write: entry worker -> meta worker publish"
               for edge in flow_stages["write"]["edges"])
    edge_names = {edge["name"] for edge in flow_stages["edges"]}
    assert "client -> entry worker" in edge_names
    assert "entry worker -> meta worker" in edge_names
    assert "entry worker -> data worker" in edge_names
    assert "data worker -> entry worker UB write" in edge_names
    assert "entry worker -> meta worker publish" in edge_names
    assert report["traces"][trace_id]["classification"] == "client_deadline_with_urma_wait"


def test_latency_summary_and_write_memory_copy_are_preserved(tmp_path):
    trace_id = "8cd13fe6-8d65-4d14-8a3f-e78b5261780c"
    log = tmp_path / "client.log"
    log.write_text(
        "\n".join(
            [
                f"2026-07-19T03:40:00.000000 | INFO | client | 192.0.2.30 | 7 | {trace_id} | - | 0 | DS_KV_CLIENT_SET | 4268 | 1835008",
                f"2026-07-19T03:40:00.001000 | INFO | client | 192.0.2.30 | 7 | {trace_id} | Set done latencySummary:{{client.process.memory_copy:2988, client.rpc.publish:690, client.rpc.create:490, client.process.set:21}}",
                f"2026-07-19T03:40:00.002000 | INFO | worker | kvchachzpworker-0-worker10 | 7 | {trace_id} | Publish done latencySummary:{{worker.rpc.create_meta:2702, worker.process.publish:64}} safe IP 192.0.2.254",
            ]
        ),
        encoding="utf-8",
    )

    mod = _load_module()
    report = mod.analyze_inputs([str(log)], code_ref="unit-test")
    trace = report["traces"][trace_id]

    assert trace["classification"] == "write_memory_copy_dominant"
    assert trace["latency_summary_us"]["client.process.memory_copy"] == 2988
    assert trace["latency_summary_us"]["client.rpc.publish"] == 690
    assert "latencySummary:{client.process.memory_copy:2988" in trace["latency_summary_raw"][0]
    assert report["dimensions"]["latency_summary_us"]["client.process.memory_copy"]["p50"] == 2988
    assert report["dimensions"]["flow"]["DS_KV_CLIENT_SET"] == 1


def test_ub_current_log_fields_time_buckets_and_worker_edges_are_structured(tmp_path):
    trace_id = "019f7c61-31b8-7d20-bbd7-56869c2c4c2a"
    log = tmp_path / "worker.log"
    log.write_text(
        "\n".join(
            [
                f"2026-07-20T10:00:00.000000 | INFO | access_recorder | 192.0.2.10 | 1 | {trace_id} | - | 0 | DS_KV_CLIENT_GET | 20298 | 4096",
                f"2026-07-20T10:00:00.005000 | INFO | access_recorder | 192.0.2.10 | 1 | {trace_id} | - | 0 | DS_POSIX_GET | 231321 | 4096",
                f"2026-07-20T10:00:00.010000 | INFO | worker | kventryworker-0-worker1 | 1 | {trace_id} | [Get] Done, clientId: c1, objects: 1, transferPath: UB, totalCost: 230.100ms, inflightRemoteGet: 9 exceed 3ms: {{ ProcessGetObjectRequest: 230 ms }} safe IP 192.0.2.254",
                f"2026-07-20T10:00:00.020000 | INFO | worker | kventryworker-0-worker1 | 1 | {trace_id} | Remote get request:[881] object:[obj-a], offset[0] size[4194304] src address: 192.0.2.10, dst address: 192.0.2.20",
                f"2026-07-20T10:00:00.040000 | INFO | worker | kventryworker-0-worker1 | 1 | {trace_id} | Remote get success, objectKey: obj-a, path: UB, cost: 231.321ms src address: 192.0.2.10, dst address: 192.0.2.20",
                f"2026-07-20T10:00:00.050000 | WARN | worker | kvdataworker-0-worker2 | 1 | {trace_id} | [URMA_ELAPSED_TOTAL]: Time from urma_post_jetty_send_wr to urma_write completion total cost 231.001ms, wait os sched thread finish time(std::condition_variable.wait_for): 230.500ms, request id:881, src address: 192.0.2.20, target address: 192.0.2.10, dataSize:4194304, cpuid:23, status: OK, urma_inflight_wr_count: 11, wakeSchedLatencyUs:4500, srcChipInflight:{{2:5}}",
                f"2026-07-20T10:00:00.051000 | WARN | worker | kvdataworker-0-worker2 | 1 | {trace_id} | [URMA_ELAPSED_POLL_JFC]: urma_poll_jfc cost 309us, cpuid: 23, suggest: check URMA, safe IP 192.0.2.254",
                f"2026-07-20T10:00:00.052000 | WARN | worker | kvdataworker-0-worker2 | 1 | {trace_id} | [URMA_ELAPSED_NOTIFY]: urma_poll_jfc thread notify urma_post_jetty_send_wr thread wake up cost 0.041ms, cpuid: 23, count: 1, safe IP 192.0.2.254",
                f"2026-07-20T10:00:00.052500 | WARN | worker | kvdataworker-0-worker2 | 1 | {trace_id} | [URMA_ELAPSED_THREAD_SHED]: urma_poll_jfc loop gap, lastPollEndToThisPollStart 78000us, lastPollStartToThisPollStart 79000us, cpuid: 23, safe IP 192.0.2.254",
                f"2026-07-20T10:00:00.053000 | WARN | worker | kvdataworker-0-worker2 | 1 | {trace_id} | [URMA_ELAPSED_THREAD_SHED]: urma_poll_jfc thread wake up after nanosleep(1us) cost 12500us, cpuid: 23, safe IP 192.0.2.254",
                f"2026-07-20T10:00:00.070000 | ERROR | worker | kventryworker-0-worker1 | 1 | {trace_id} | RPC deadline exceeded while waiting WorkerOCService.Get safe IP 192.0.2.254",
            ]
        ),
        encoding="utf-8",
    )

    mod = _load_module()
    report = mod.analyze_inputs([str(log)], code_ref="unit-test")
    trace = report["traces"][trace_id]
    assert trace["access_latency_ms_by_role"]["client"]["p50"] == 20.298
    assert trace["access_latency_ms_by_role"]["worker"]["p50"] == 231.321

    total = next(event for event in trace["ub_events"] if event["event_type"] == "total")
    assert total["request_id"] == "881"
    assert total["src_addr"] == "192.0.2.20"
    assert total["target_addr"] == "192.0.2.10"
    assert total["data_size"] == 4194304
    assert total["cpuid"] == 23
    assert total["status"] == "OK"
    assert total["urma_inflight_wr_count"] == 11
    assert total["wait_os_sched_ms"] == 230.5
    assert total["wake_sched_latency_us"] == 4500
    assert total["src_chip_inflight"] == "{2:5}"
    loop_gap = next(event for event in trace["ub_events"] if event.get("thread_sched_kind") == "poll_loop_gap")
    assert loop_gap["last_poll_end_to_start_us"] == 78000
    assert loop_gap["last_poll_start_to_start_us"] == 79000
    sleep_wake = next(event for event in trace["ub_events"] if event.get("thread_sched_kind") == "nanosleep_wake")
    assert sleep_wake["sleep_target_us"] == 1
    assert report["dimensions"]["urma_elapsed"]["poll_jfc"]["p50"] == 0.309
    assert report["dimensions"]["urma_elapsed"]["thread_sched"]["p50"] == 45.25
    assert report["dimensions"]["ub_summary"]["transfer_path"]["UB"] == 2
    assert report["dimensions"]["ub_summary"]["edges"]["192.0.2.20 -> 192.0.2.10"]["count"] == 1
    assert report["dimensions"]["time_buckets"]["1000ms"][0]["trace_count"] == 1
    assert report["dimensions"]["time_buckets"]["1000ms"][0]["burst_score"] >= 1
    assert report["dimensions"]["worker_summary"]["kventryworker-0-worker1"]["roles"] == ["entry_worker"]
    assert report["dimensions"]["worker_summary"]["kvdataworker-0-worker2"]["roles"] == ["data_worker"]
    assert report["dimensions"]["worker_edges"]["192.0.2.20 -> 192.0.2.10"]["p99_ms"] == 231.001
    ub_workers = report["dimensions"]["ub_worker_summary"]["workers"]
    assert ub_workers["kventryworker-0-worker1"]["role"] == "ub_entry"
    assert ub_workers["kventryworker-0-worker1"]["entry_events"] == 3
    assert ub_workers["kvdataworker-0-worker2"]["role"] == "ub_exit"
    assert ub_workers["kvdataworker-0-worker2"]["exit_events"] == 5
    assert ub_workers["kvdataworker-0-worker2"]["latency_ms"]["p99"] == 231.001
    assert report["dimensions"]["ub_worker_summary"]["time_buckets"][0]["exit_events"] == 5
    lifecycle = report["dimensions"]["ub_lifecycle_summary"]
    assert lifecycle["metrics"]["wait_os_sched_ms"]["p99"] == 230.5
    assert lifecycle["metrics"]["wake_sched_latency_ms"]["p99"] == 4.5
    assert lifecycle["metrics"]["poll_loop_gap_ms"]["p99"] == 78.0
    assert lifecycle["metrics"]["nanosleep_wake_ms"]["p99"] == 12.5
    assert lifecycle["metrics"]["remote_get_wr_count"]["p99"] == 9
    assert lifecycle["metrics"]["urma_inflight_wr_count"]["p99"] == 11
    assert lifecycle["chip_inflight"]["2"]["p99"] == 5
    assert lifecycle["requests"][0]["request_id"] == "881"
    assert lifecycle["requests"][0]["src_chip_inflight"] == "{2:5}"
    assert lifecycle["requests"][0]["remote_get_wr_count"] == 9
    assert lifecycle["requests"][0]["urma_inflight_wr_count"] == 11
    assert "late_worker_completion" in trace["triage_flags"]


def test_run_pipeline_writes_intermediate_outputs_and_html_targets(tmp_path):
    trace_id = "019f7c62-9e9f-7792-a5d8-f4d30275bafe"
    log = tmp_path / "client.log"
    log.write_text(
        "\n".join(
            [
                f"2026-07-20T11:00:00.000000 | INFO | client | 192.0.2.30 | 1 | {trace_id} | - | 0 | DS_KV_CLIENT_SET | 4268 | 1024",
                f"2026-07-20T11:00:00.001000 | INFO | client | 192.0.2.30 | 1 | {trace_id} | Set done latencySummary:{{client.process.memory_copy:2988, client.rpc.publish:690, client.rpc.create:490, client.process.set:21}}",
            ]
        ),
        encoding="utf-8",
    )

    mod = _load_module()
    run_dir = mod.run_pipeline([str(log)], tmp_path / "runs", case_name="set-case", scenario="memory-copy")

    assert (run_dir / "manifest.json").exists()
    assert (run_dir / "events.jsonl").exists()
    assert (run_dir / "summary.json").exists()
    assert (run_dir / "triage.json").exists()
    assert (run_dir / "triage.md").exists()
    assert (run_dir / "report.local.html").exists()
    assert (run_dir / "report.site.html").exists()
    assert (run_dir / "site_publish.md").exists()
    manifest = json.loads((run_dir / "manifest.json").read_text())
    assert manifest["case_name"] == "set-case"
    assert manifest["scenario"] == "memory-copy"
    assert manifest["render_targets"]["local"]["path"] == "report.local.html"
    assert manifest["render_targets"]["site"]["path"] == "report.site.html"
    assert manifest["render_targets"]["site"]["publish_doc"] == "site_publish.md"
    html = (run_dir / "report.local.html").read_text(encoding="utf-8")
    site_html = (run_dir / "report.site.html").read_text(encoding="utf-8")
    assert "echarts" in html
    assert "<aside><h2>Trace 分析报告</h2><nav id=\"nav\">" in html
    assert "aside{position:sticky" in html
    assert "main{flex:1;min-width:0;width:auto" in html
    assert "main{margin-left:245px" not in html
    assert "class=\"subtitle\"" in html
    assert "id=\"run-metadata-table\"" in html
    assert "class=\"metadata-table\"" in html
    assert "#run-metadata-table th:first-child" in html
    assert "set-case" in html
    assert "memory-copy" in html
    assert "inputs.md" in html
    assert "raw/inputs" in html
    assert "raw/extracted" in html
    assert "client.log" in html
    assert "class=\"panel insight\"" in html
    assert "id=\"diagnosis-list\"" in html
    assert "id=\"coverage-table\"" in html
    assert "日志覆盖与缺失观测面" in html
    assert "client_access" in html
    assert "latency_summary" in html
    assert "id=\"download-report-summary\"" in html
    assert "下载分析摘要" in html
    assert "trace-report-summary.md" in html
    assert "id=\"overall-guide\"" in html
    assert "id=\"chapter-guide-list\"" in html
    assert "表 1-1 整体导读" in html
    assert "chapter-guide" in html
    assert "summary-points" in html
    assert "summary-hot" in html
    assert "summary-warn" in html
    assert "summary-key" in html
    assert "function chapterSummaryTexts()" in html
    assert "function renderChapterGuide(summaries)" in html
    assert "function highlightSummaryLatency(match, raw)" in html
    assert "function highlightSummaryText(text)" in html
    assert "function summaryPointsHtml(summary)" in html
    assert r"\b(\d+(?:\.\d+)?)ms\b" in html
    assert "value >= 20 ? 'summary-hot'" in html
    assert "value >= 5 ? 'summary-warn'" in html
    assert "图 5-1/5-2/5-3/5-4/5-5/5-6" in html
    assert "id=\"recommendation-table\"" in html
    assert "id=\"source-appendix-common-table\"" in html
    assert "id=\"source-appendix-read-table\"" in html
    assert "id=\"source-appendix-write-table\"" in html
    assert "通用字段映射" in html
    assert "读取字段映射" in html
    assert "写入字段映射" in html
    assert "renderSourceAppendixTables()" in html
    assert "sourceAppendixByScope" in html
    assert "['log surface','flow stage','source hint','validation','report reading']" in html
    assert "代码与字段映射" in html
    assert "建议与后续口径" in html
    assert "错误线" in html
    assert "慢时延线" in html
    assert "证据边界" in html
    assert "id=\"classification-chart\"" in html
    assert "id=\"cohort-chart\"" in html
    for title in [
        "图 2-1 输入包 / Cohort 对比",
        "图 2-2 分类分布",
        "图 2-3 错误文本 / 状态分布",
        "图 3-1 读取 Top 时延",
        "图 3-2 读取 Flow 分布",
        "图 3-3 读取时间桶 Breakdown",
        "图 3-4 写入 Top 时延",
        "图 3-5 写入 Flow 分布",
        "图 3-6 写入时间桶 Breakdown",
        "图 4-2 读取 Worker 分布",
        "图 4-4 写入 Worker 分布",
        "图 5-1 UB 生命周期",
        "图 5-2 WR / Inflight Count",
        "图 5-4 UB 时间桶",
        "图 5-5 读取 UB Edge",
        "图 5-6 写入 UB Edge",
        "图 6-1 选中 Trace Breakdown",
        "日志框 6-3 Trace 全量日志",
    ]:
        assert title in html
    assert "<h3>图 " not in html
    for old_figure in ["图 2-0", "图 3-2a", "图 3-3b", "图 4-0a", "图 5-1b", "图 5-5a", "图 5-5b"]:
        assert old_figure not in html
    for summary_id in [
        "section-summary-s2",
        "section-summary-s3",
        "section-summary-s4",
        "section-summary-s5",
        "section-summary-s6",
        "section-summary-s7",
        "section-summary-s8",
    ]:
        assert f"id=\"{summary_id}\"" in html
    for summary_marker in [
        "function renderSectionSummaries()",
        "本章结论",
        "读取瓶颈",
        "Worker 集中",
        "UB 耗时",
        "Top trace",
        "缺失观测面",
        "原始 JSON 保留",
    ]:
        assert summary_marker in html
    assert "id=\"cohort-table\"" in html
    assert "id=\"read-latency-chart\"" in html
    assert "id=\"write-latency-chart\"" in html
    assert "renderLatencySection('read', 'Read')" in html
    assert "renderLatencySection('write', 'Write')" in html
    assert "metricLabelMap" in html
    assert "Worker RemoteGet RPC" in html
    assert "latencyChartRows" in html
    assert "latencyRowsForOperation" in html
    assert "Flow Breakdown" in html
    assert "flowRowsForOperation" in html
    assert "sortable-th" in html
    assert "sortRowsForTable" in html
    assert "sortCellValue(value, header)" in html
    assert "distributionMatch" in html
    assert "max|p99|p90|p50|count" in html
    assert "time|trace|worker|edge|classification|status|access" in html
    assert "data-sort-index" in html
    assert "aria-sort" in html
    assert "renderPagedTable(id, pagerId, headers, rows, rowAttrs, pageSize=5)" in html
    assert "state.sort" in html
    assert "let topTraceSort" in html
    assert "function topTraceDisplayRows" in html
    assert "const sortedDisplayRows = sortRowsForTable(topTraceDisplayRows(filteredTraceRows), topTraceSort, topTraceHeaders)" in html
    assert "onSort:index =>" in html
    assert "topTraceSort = nextSortState(topTraceSort, index)" in html
    assert "flowLabelMap" in html
    assert "Worker GET" in html
    assert "wideHorizontalGrid" in html
    assert "grid:wideHorizontalGrid" in html
    assert "id=\"read-time-breakdown-chart\"" in html
    assert "id=\"write-time-breakdown-chart\"" in html
    assert "buildTraceTimeBuckets" in html
    assert "Time Bucket Latency Stages" in html
    assert "client/access p99 upper bound" in html
    assert "Entry→Data RPC" in html
    assert "stageDisplayName" in html
    assert "DataWorker UB/URMA" in html
    assert "stageDetailText" in html
    assert "研发流程" in html
    assert "id=\"flow-stage-chart\"" in html
    assert "id=\"read-flow-stage-chart\"" in html
    assert "id=\"write-flow-stage-chart\"" in html
    assert "function autoCenterFlowGraph(chartInstance)" in html
    assert "myAutoCenter" in html
    assert "title:'自适应居中'" in html
    assert "dataView:{readOnly:true}" in html
    assert "id=\"flow-stage-table\"" in html
    assert "id=\"read-flow-stage-table\"" in html
    assert "id=\"write-flow-stage-table\"" in html
    assert "id=\"read-flow-section\"" in html
    assert "id=\"write-flow-section\"" in html
    assert "读取流程证据块" in html
    assert "写入流程证据块" in html
    assert "flow-section" in html
    assert "href=\"#cohort-table\">表 2-1 Cohort" in html
    assert "href=\"#classification-table\">表 2-2 分类" in html
    assert "href=\"#read-flow-stage-chart\">图 4-1 读取流程" in html
    assert "href=\"#read-flow-stage-table\">表 4-1 读取流程" in html
    assert "href=\"#read-worker-chart\">图 4-2 读取 Worker" in html
    assert "href=\"#read-worker-table\">表 4-2 读取 Worker" in html
    assert "href=\"#write-flow-stage-chart\">图 4-3 写入流程" in html
    assert "href=\"#write-flow-stage-table\">表 4-3 写入流程" in html
    assert "href=\"#write-worker-chart\">图 4-4 写入 Worker" in html
    assert "href=\"#write-worker-table\">表 4-4 写入 Worker" in html
    assert "href=\"#ub-lifecycle-table\">表 5-1 生命周期" in html
    assert "href=\"#ub-wr-count-chart\">图 5-2 WR / Inflight" in html
    assert "href=\"#ub-request-table\">表 5-2 UB Request" in html
    assert "href=\"#ub-worker-time-chart\">图 5-4 UB 时间桶" in html
    assert "href=\"#ub-worker-time-table\">表 5-4 UB 时间桶" in html
    assert "href=\"#read-ub-edge-chart\">图 5-5 读取 UB Edge" in html
    assert "href=\"#write-ub-edge-chart\">图 5-6 写入 UB Edge" in html
    assert "href=\"#selected-trace-chart\">图 6-1 选中 Trace" in html
    assert "href=\"#selected-trace-log\">日志框 6-3 全量日志" in html
    assert "--report-font-size" in html
    assert ".caption{text-align:center" in html
    assert "font-size:var(--report-font-size)" in html
    assert "const chartTextStyle" in html
    assert "textStyle:chartTextStyle" in html
    assert "图 4-1 读取流程证据块：看 Entry→Data RPC 与 DataWorker UB/URMA。" in html
    assert "图 4-3 写入流程证据块：区分 createbuffer、client publish、entry/meta publish。" in html
    assert "读写链路分开看" in html
    assert "edge.summary" in html
    assert "edge.reason" in html
    assert "rollup" in html
    assert "id=\"read-worker-chart\"" in html
    assert "id=\"write-worker-chart\"" in html
    assert "id=\"read-ub-edge-chart\"" in html
    assert "id=\"write-ub-edge-chart\"" in html
    assert "id=\"ub-worker-role-chart\"" in html
    assert "id=\"ub-worker-time-chart\"" in html
    assert "id=\"ub-lifecycle-chart\"" in html
    assert "id=\"ub-wr-count-chart\"" in html
    assert "ubLifecycleLatencyMetrics" in html
    assert "ubWrCountRows" in html
    assert "WR / Inflight Count" in html
    assert "按实际采样字段展示" in html
    assert "class=\"panel full-row\"" in html
    assert "id=\"ub-worker-role-table\"" in html
    assert "class=\"adaptive-table\"" in html
    assert "#ub-worker-role-table" in html
    assert "#ub-request-table" in html
    assert "overflow-x:hidden" in html
    assert "missingMetricCell" in html
    assert "未采样" in html
    assert "id=\"ub-worker-time-table\"" in html
    assert "id=\"ub-lifecycle-table\"" in html
    assert "class=\"nowrap-table\"" in html
    assert "#ub-lifecycle-table" in html
    assert "white-space:nowrap" in html
    assert "renderTable('ub-lifecycle-table', ['metric','count','p50','p90','p99','max'], metricRows" in html
    assert "ub-lifecycle-table-pager" not in html
    assert "id=\"ub-request-table\"" in html
    assert "class=\"flow-section ub-table-stack\"" in html
    assert "id=\"ub-worker-role-table-pager\"" in html
    assert "id=\"ub-worker-time-table-pager\"" in html
    assert "id=\"ub-request-table-pager\"" in html
    assert "ub_worker_summary" in html
    assert "ub_lifecycle_summary" in html
    assert "UB 入口/出口 Worker" in html
    assert "renderUbWorkerViews" in html
    assert "renderUbLifecycleViews" in html
    assert "href=\"#s5\">5. UB / URMA" in html
    assert "图 5-1 UB 生命周期" in html
    assert "表 5-1 UB 生命周期指标" in html
    assert "workerRowsForOperation" in html
    assert "renderWorkerSection" in html
    assert "renderUbSection" in html
    assert "function workerDisplayName(raw)" in html
    assert "function workerRelationName(raw)" in html
    assert "client ${parts.left} → worker ${parts.worker}" in html
    assert "worker ${parts.left} → worker ${parts.worker}" in html
    assert "value=\"${escapeHtml(name)}\">${escapeHtml(workerRelationName(name))}</option>" in html
    assert "display_worker" in html
    assert "display_top_edges" in html
    assert "id=\"read-worker-table-pager\"" in html
    assert "id=\"write-worker-table-pager\"" in html
    for worker_filter_id in [
        "read-worker-filter",
        "read-worker-table-filter",
        "write-worker-filter",
        "write-worker-table-filter",
        "ub-worker-role-filter",
        "ub-worker-role-table-filter",
        "ub-worker-time-filter",
        "ub-worker-time-table-filter",
    ]:
        assert f"id=\"{worker_filter_id}\"" in html
    assert "id=\"worker-table-filter\"" not in html
    assert "workerTableFilterValue" not in html
    assert "workerScopedFilterIds" in html
    assert "workerFilterValue" in html
    assert "renderWorkerDependentViews" in html
    assert "id=\"read-ub-edge-table-pager\"" in html
    assert "id=\"write-ub-edge-table-pager\"" in html
    assert "renderPagedTable" in html
    assert "formatCell" in html
    assert "cell-hot" in html
    assert "cell-warn" in html
    assert "cell-ok" in html
    assert "hotrow" in html
    assert "warnrow" in html
    assert "class=\"controls pager\"" in html
    assert "搜索 trace / worker / 关键词" in html
    assert "id=\"worker-filter\"" in html
    assert "id=\"request-status-filter\"" in html
    assert "id=\"operation-filter\"" in html
    assert "Trace 查看读写视角" in html
    assert "全部请求" in html
    assert "只看失败" in html
    assert "只看成功" in html
    assert "按 access max 降序" in html
    assert "traceStartTime" in html
    assert "traceAccessLatencyMs" in html
    assert "latency ms" in html
    assert "hasTraceFailure" in html
    assert "requestStatus" in html
    assert "statusMatchesTrace" in html
    assert "['time','trace','classification','latency ms','errors','access','workers']" in html
    assert "联动 Trace 列表与选中 Trace Breakdown" in html
    assert "读写视角" in html
    assert "全部读写" in html
    assert "只看读取" in html
    assert "只看写入" in html
    assert "function traceOperation" in html
    assert "function operationMatches" in html
    assert "function stageMatchesOperation" in html
    assert "function renderOperationViews" in html
    assert "const visibleStageRows" in html
    assert "No observed stage duration for selected operation" in html
    assert "missing" in html
    assert "id=\"trace-page-size\"" in html
    assert "全部 Worker" in html
    assert "<option value=\"4\" selected>4</option>" in html
    assert "let pageSize = 4" in html
    assert "download-selected-raw" in html
    assert "download-filtered-evidence" in html
    assert "selectedTraceSummaryRows" in html
    assert "client access" in html
    assert "worker access" in html
    assert "key latencySummary" in html
    assert "summaryrow" in html
    assert "id=\"selected-stage-table\"" in html
    assert "id=\"selected-stage-legend\"" in html
    assert "stage-pill" in html
    assert "legend:{show:false}" in html
    assert "点击 Trace 行联动，按阶段耗时排序，单位 ms。" in html
    assert "highlightLogLine" in html
    assert "renderTraceLogBlocks" in html
    assert "traceLogBlockSummary" in html
    assert "trace-log-summary" in html
    assert "trace-log-focus" in html
    assert "重点:" in html
    assert "class=\"trace-log-block" in html
    assert "Client 侧日志" in html
    assert "Entry Worker 日志" in html
    assert "Meta Worker 观测" in html
    assert "Data Worker / UB 日志" in html
    assert "function axisBase" in html
    assert "setOption(option, true)" in html
    assert "dataZoom" in html
    assert "markLine" in html
    assert "log-error" in html
    assert "log-deadline" in html
    assert "log-urma" in html
    assert "log-rpc" in html
    assert "log-latency" in html
    assert "log-slow" in html
    assert "echarts.init" in html
    assert "id=\"classification-table\"" in html
    assert "id=\"error-table\"" in html
    assert "id=\"read-worker-table\"" in html
    assert "id=\"write-worker-table\"" in html
    assert "id=\"top-trace-table\"" in html
    assert "表 6-1 Top Trace" in html
    assert html.index("<table id=\"top-trace-table\"") < html.index("id=\"trace-pager\"")
    assert "Error Breakdown" in html
    for marker in [
        "/assets/css/site.css",
        "/assets/js/site.js",
        "id=\"run-metadata-table\"",
        "id=\"coverage-table\"",
        "id=\"flow-stage-chart\"",
        "id=\"download-report-summary\"",
        "trace-report-summary.md",
    ]:
        assert marker in site_html
    publish_doc = (run_dir / "site_publish.md").read_text(encoding="utf-8")
    assert "target_host: `<publish-host>`" in publish_doc
    assert "target_path: `<publish-root>/perf/" in publish_doc
    assert ("https://yche" + ".me/perf/") in publish_doc
    assert "report.site.html" in publish_doc
    assert "## Index Registration" in publish_doc
    assert "var P" in publish_doc
    assert "perf/" in publish_doc
    assert "<publish-root>/index.html" in publish_doc
    triage = json.loads((run_dir / "triage.json").read_text())
    assert triage["issue_candidates"][0]["classification"] == "write_memory_copy_dominant"


def test_trace_triage_gate_stays_out_of_ci_build():
    repo = Path(__file__).resolve().parents[2]
    ci_build = (repo / ".gitee" / "ci_build.sh").read_text(encoding="utf-8")
    skill = (repo / ".skills" / "ds-trace-triage" / "SKILL.md").read_text(encoding="utf-8")
    methodology = (repo / "docs" / "source_zh_cn" / "appendix" / "trace_triage_methodology.md").read_text(
        encoding="utf-8"
    )

    assert "ds_trace_triage.py" not in ci_build
    assert "先不要接入 .gitee/ci_build.sh" in skill
    assert "候选 CI 门禁" in skill
    assert "catalog-index registration" in skill
    assert "var P" in skill
    assert "copied but not fully registered" in skill
    assert "当前不默认接入 .gitee/ci_build.sh" in methodology


def test_trace_triage_docs_and_fixtures_are_review_scan_friendly():
    repo = Path(__file__).resolve().parents[2]
    docs_and_skill = "\n".join(
        [
            (repo / ".skills" / "ds-trace-triage" / "SKILL.md").read_text(encoding="utf-8"),
            (repo / "docs" / "source_zh_cn" / "appendix" / "trace_triage_methodology.md").read_text(
                encoding="utf-8"
            ),
        ]
    )
    fixture_text = (repo / "tests" / "scripts" / "test_ds_trace_triage.py").read_text(encoding="utf-8")
    script_text = (repo / "scripts" / "ds_trace_triage.py").read_text(encoding="utf-8")

    assert ("/home" + "/") not in docs_and_skill
    assert ("/tmp" + "/") not in docs_and_skill
    assert ("/var" + "/www/html") not in docs_and_skill
    assert ("xq" + "yun-") not in docs_and_skill
    assert ("yche" + ".me") not in docs_and_skill
    assert not re.search(r"\b(?:10|172\.(?:1[6-9]|2\d|3[0-1])|192\.168)\.\d{1,3}\.\d{1,3}\b", fixture_text)
    assert not re.search(r"\b(?:10|172\.(?:1[6-9]|2\d|3[0-1])|192\.168)\.\d{1,3}\.\d{1,3}\b", script_text)


def test_run_pipeline_preserves_raw_extracted_logs_and_reuses_cache(tmp_path):
    trace_id = "019f7d06-cbda-7381-a95f-99fed6ee3732"
    bundle = tmp_path / "multi-input.tar.gz"
    _write_tar_gz(
        bundle,
        {
            "case-a/kventryworker-0-worker1/worker.log": "\n".join(
                [
                    f"2026-07-20T12:00:00.000000 | INFO | access_recorder | 192.0.2.10 | 1 | {trace_id} | - | 0 | DS_KV_CLIENT_GET | 20298 | 4096",
                    f"2026-07-20T12:00:00.010000 | INFO | worker | kventryworker-0-worker1 | 1 | {trace_id} | [Get] Done, clientId: c1, objects: 1, transferPath: UB, totalCost: 20.298ms, inflightRemoteGet: 1 safe IP 192.0.2.254",
                ]
            )
        },
    )

    mod = _load_module()
    first = mod.run_pipeline([str(bundle)], tmp_path / "runs", case_name="cache-case", scenario="round-a")
    second = mod.run_pipeline([str(bundle)], tmp_path / "runs", case_name="cache-case", scenario="round-a")
    forced = mod.run_pipeline([str(bundle)], tmp_path / "runs", case_name="cache-case", scenario="round-a", force=True)

    assert first == second
    assert forced != first
    manifest = json.loads((first / "manifest.json").read_text())
    assert manifest["cache"]["status"] == "created"
    assert manifest["inputs"][0]["members"] == ["case-a/kventryworker-0-worker1/worker.log"]
    assert (first / "raw" / "inputs" / "multi-input.tar.gz").exists()
    extracted = first / "raw" / "extracted" / "multi-input.tar.gz" / "case-a" / "kventryworker-0-worker1" / "worker.log"
    assert extracted.exists()
    assert trace_id in extracted.read_text()
    input_doc = first / "inputs.md"
    assert input_doc.exists()
    input_text = input_doc.read_text(encoding="utf-8")
    assert "cache-case" in input_text
    assert "round-a" in input_text
    assert str(bundle) in input_text
    assert "raw/inputs/multi-input.tar.gz" in input_text
    assert "raw/extracted/multi-input.tar.gz" in input_text
    assert json.loads((second / "manifest.json").read_text())["cache"]["status"] == "created"


def test_stage_breakdown_and_missing_evidence_are_emitted(tmp_path):
    read_trace = "019f7d07-bc52-7e1a-93e2-0a0372070197"
    write_trace = "019f7d08-52a0-780e-ac5d-131d879ba989"
    log = tmp_path / "mixed.log"
    log.write_text(
        "\n".join(
            [
                f"2026-07-20T12:10:00.000000 | INFO | access_recorder | 192.0.2.10 | 1 | {read_trace} | - | 0 | DS_KV_CLIENT_GET | 20298 | 4096",
                f"2026-07-20T12:10:00.001000 | INFO | client | 192.0.2.30 | 1 | {read_trace} | Get done latencySummary:{{client.rpc.get:20298}}",
                f"2026-07-20T12:10:00.010000 | INFO | worker | kventryworker-0-worker1 | 1 | {read_trace} | Remote get success, objectKey: obj-a, path: UB, cost: 231.321ms src address: 192.0.2.10, dst address: 192.0.2.20",
                f"2026-07-20T12:10:00.020000 | WARN | worker | kvdataworker-0-worker2 | 1 | {read_trace} | [URMA_ELAPSED_TOTAL] cost 231.001ms, request id:77, src address: 192.0.2.20, target address: 192.0.2.10, dataSize:4096, cpuid:2, status: OK",
                f"2026-07-20T12:10:01.000000 | INFO | access_recorder | 192.0.2.30 | 1 | {write_trace} | - | 0 | DS_KV_CLIENT_SET | 4268 | 1024",
                f"2026-07-20T12:10:01.001000 | INFO | client | 192.0.2.30 | 1 | {write_trace} | Set done latencySummary:{{client.process.memory_copy:2988, client.rpc.publish:690, client.rpc.create:490, client.process.set:21}}",
            ]
        ),
        encoding="utf-8",
    )

    mod = _load_module()
    report = mod.analyze_inputs([str(log)], code_ref="unit-test")
    read = report["traces"][read_trace]
    write = report["traces"][write_trace]

    read_stages = {stage["stage"]: stage for stage in read["stage_breakdown"]}
    assert read_stages["read.client_to_entry_worker"]["duration_ms"] == 20.298
    assert read_stages["read.entry_to_data_worker"]["duration_ms"] == 231.321
    assert read_stages["read.data_worker_ub_write"]["duration_ms"] == 231.001
    assert read_stages["read.entry_to_meta_worker"]["confidence"] == "missing"
    assert read["evidence_coverage"]["urma"] == "present"
    assert read["missing_evidence"][0]["stage"] == "read.entry_to_meta_worker"

    write_stages = {stage["stage"]: stage for stage in write["stage_breakdown"]}
    assert write_stages["write.client_to_entry_createbuffer"]["duration_ms"] == 0.49
    assert write_stages["write.client_memory_copy"]["duration_ms"] == 2.988
    assert write_stages["write.client_to_entry_publish"]["duration_ms"] == 0.69
    assert write_stages["write.entry_to_meta_publish"]["confidence"] == "missing"
    assert report["dimensions"]["coverage"]["surfaces"]["urma_elapsed"]["status"] == "present"
    assert report["dimensions"]["coverage"]["surfaces"]["rpc_slow"]["status"] == "missing"
    flow_edges = {edge["name"]: edge for edge in report["dimensions"]["flow_stages"]["edges"]}
    data_edge = flow_edges["entry worker -> data worker"]
    assert data_edge["rollup"]["p99_ms"] == 231.321
    assert "192.0.2.10" in data_edge["rollup"]["top_ips"]
    assert "p99=231.321ms" in data_edge["summary"]
    assert data_edge["reason"]
    first_bucket = report["dimensions"]["time_buckets"]["1000ms"][0]
    assert first_bucket["stage_breakdown_ms"]["read.entry_to_data_worker"]["p99"] == 231.321
    assert first_bucket["stage_breakdown_ms"]["read.data_worker_ub_write"]["p99"] == 231.001


def test_trace_evidence_preserves_late_urma_elapsed_lines(tmp_path):
    trace_id = "019f7d09-efb4-7c57-8a89-2f6f6e06a321"
    log = tmp_path / "late-urma.log"
    lines = [
        f"2026-07-20T12:20:00.{i:06d} | INFO | worker | kvworker-0-worker1 | 1 | {trace_id} | filler line {i} safe IP 192.0.2.254"
        for i in range(13)
    ]
    lines.append(
        f"2026-07-20T12:20:01.000000 | WARN | worker | kvdataworker-0-worker2 | 1 | {trace_id} | [URMA_ELAPSED_TOTAL] cost 9.123ms, request id:88, src address: 192.0.2.20, target address: 192.0.2.10, dataSize:4096, cpuid:2, status: OK"
    )
    log.write_text("\n".join(lines), encoding="utf-8")

    mod = _load_module()
    report = mod.analyze_inputs([str(log)], code_ref="unit-test")
    evidence_text = "\n".join(item["text"] for item in report["traces"][trace_id]["evidence"])

    assert len(report["traces"][trace_id]["evidence"]) == 14
    assert "URMA_ELAPSED_TOTAL" in evidence_text
    assert report["traces"][trace_id]["evidence_coverage"]["urma"] == "present"


def test_grep_prefixed_log_lines_still_build_time_buckets(tmp_path):
    trace_id = "019f7d12-1f09-762a-9163-c547ef71a101"
    log = tmp_path / "grep-output.log"
    log.write_text(
        f"/data/worker.log:42:2026-07-20T14:00:00.000000 | INFO | access_recorder | 192.0.2.10 | 1 | {trace_id} | - | 0 | DS_KV_CLIENT_GET | 8000 | 0\n",
        encoding="utf-8",
    )

    mod = _load_module()
    report = mod.analyze_inputs([str(log)], code_ref="unit-test")
    buckets = report["dimensions"]["time_buckets"]["1000ms"]

    assert len(buckets) == 1
    assert buckets[0]["trace_count"] == 1
    assert buckets[0]["bucket_start"].startswith("2026-07-20T14" + ":00:00")


def test_multiple_input_files_are_reported_as_cohorts(tmp_path):
    trace_a = "019f7d10-1f09-762a-9163-c547ef71a101"
    trace_b = "019f7d10-b880-7937-b945-382161303102"
    noisy = tmp_path / "noisy.log"
    clean = tmp_path / "clean.log"
    noisy.write_text(
        f"2026-07-20T14:00:00.000000 | INFO | access_recorder | 192.0.2.10 | 1 | {trace_a} | - | 1001 | DS_KV_CLIENT_GET | 20298 | 0\n"
        f"2026-07-20T14:00:00.001000 | ERROR | client | 192.0.2.10 | 1 | {trace_a} | RPC deadline exceeded\n",
        encoding="utf-8",
    )
    clean.write_text(
        f"2026-07-20T14:00:01.000000 | INFO | access_recorder | 192.0.2.20 | 1 | {trace_b} | - | 0 | DS_KV_CLIENT_GET | 8000 | 0\n",
        encoding="utf-8",
    )

    mod = _load_module()
    report = mod.analyze_inputs([str(noisy), str(clean)], code_ref="unit-test")
    cohorts = report["dimensions"]["cohorts"]

    assert cohorts["noisy.log"]["trace_count"] == 1
    assert cohorts["noisy.log"]["errors"]["RPC deadline exceeded"] == 1
    assert cohorts["noisy.log"]["classifications"]["client_deadline_20ms"] == 1
    assert cohorts["clean.log"]["trace_count"] == 1
    assert cohorts["clean.log"]["errors"] == {}
    assert cohorts["clean.log"]["classifications"]["access_latency_only"] == 1
    assert report["traces"][trace_a]["input_sources"] == ["noisy.log"]


def test_shared_trace_id_keeps_cohort_errors_at_source_level(tmp_path):
    trace_id = "019f7d10-b880-7937-b945-382161303199"
    clean = tmp_path / "clean.log"
    noisy = tmp_path / "noisy.log"
    clean.write_text(
        f"2026-07-20T14:00:00.000000 | INFO | access_recorder | 192.0.2.10 | 1 | {trace_id} | - | 0 | DS_KV_CLIENT_GET | 8000 | 0\n",
        encoding="utf-8",
    )
    noisy.write_text(
        f"2026-07-20T14:00:00.001000 | ERROR | client | 192.0.2.20 | 1 | {trace_id} | RPC deadline exceeded\n",
        encoding="utf-8",
    )

    mod = _load_module()
    report = mod.analyze_inputs([str(clean), str(noisy)], code_ref="unit-test")
    cohorts = report["dimensions"]["cohorts"]

    assert cohorts["clean.log"]["trace_count"] == 1
    assert cohorts["clean.log"]["errors"] == {}
    assert cohorts["noisy.log"]["errors"]["RPC deadline exceeded"] == 1
    assert report["traces"][trace_id]["errors"]["RPC deadline exceeded"] == 1


def test_dizao_directory_marks_noise_cohorts(tmp_path):
    trace_a = "019f7d11-1f09-762a-9163-c547ef71a101"
    trace_b = "019f7d11-b880-7937-b945-382161303102"
    noisy_dir = tmp_path / "round1-dizao"
    clean_dir = tmp_path / "round2"
    noisy_dir.mkdir()
    clean_dir.mkdir()
    noisy = noisy_dir / "worker.log"
    clean = clean_dir / "worker.log"
    noisy.write_text(
        f"2026-07-20T14:00:00.000000 | INFO | access_recorder | 192.0.2.10 | 1 | {trace_a} | - | 1001 | DS_KV_CLIENT_GET | 20298 | 0\n"
        f"2026-07-20T14:00:00.001000 | ERROR | client | 192.0.2.10 | 1 | {trace_a} | RPC deadline exceeded\n",
        encoding="utf-8",
    )
    clean.write_text(
        f"2026-07-20T14:00:01.000000 | INFO | access_recorder | 192.0.2.20 | 1 | {trace_b} | - | 0 | DS_KV_CLIENT_GET | 8000 | 0\n",
        encoding="utf-8",
    )

    mod = _load_module()
    report = mod.analyze_inputs([str(noisy_dir), str(clean_dir)], code_ref="unit-test")
    cohorts = report["dimensions"]["cohorts"]

    assert cohorts["有底噪(dizao)"]["trace_count"] == 1
    assert cohorts["有底噪(dizao)"]["errors"]["RPC deadline exceeded"] == 1
    assert cohorts["无底噪(wudizao)"]["trace_count"] == 1
    assert cohorts["无底噪(wudizao)"]["errors"] == {}
    assert report["traces"][trace_a]["input_sources"] == ["有底噪(dizao)"]
    assert report["traces"][trace_b]["input_sources"] == ["无底噪(wudizao)"]


def test_independent_stage_functions_consume_previous_artifacts(tmp_path):
    trace_id = "019f7d09-3aa3-77bd-a738-9965d66c9f23"
    log = tmp_path / "stage.log"
    log.write_text(
        "\n".join(
            [
                f"2026-07-20T13:00:00.000000 | INFO | access_recorder | 192.0.2.30 | 1 | {trace_id} | - | 0 | DS_KV_CLIENT_SET | 4268 | 1024",
                f"2026-07-20T13:00:00.001000 | INFO | client | 192.0.2.30 | 1 | {trace_id} | Set done latencySummary:{{client.process.memory_copy:2988, client.rpc.publish:690, client.rpc.create:490, client.process.set:21}}",
            ]
        ),
        encoding="utf-8",
    )

    mod = _load_module()
    run_dir = mod.parse_stage([str(log)], tmp_path / "runs", case_name="stage-case", scenario="stage-smoke")

    assert (run_dir / "manifest.json").exists()
    assert (run_dir / "events.jsonl").exists()
    assert (run_dir / "parsed_traces.json").exists()
    assert not (run_dir / "summary.json").exists()

    summary_path = mod.aggregate_stage(run_dir)
    assert summary_path == run_dir / "summary.json"
    summary = json.loads(summary_path.read_text())
    assert summary["dimensions"]["flow"]["DS_KV_CLIENT_SET"] == 1

    triage_path = mod.triage_stage(run_dir)
    triage = json.loads(triage_path.read_text())
    assert triage["issue_candidates"][0]["classification"] == "write_memory_copy_dominant"

    assert mod.render_local_stage(run_dir) == run_dir / "report.local.html"
    assert mod.render_site_stage(run_dir) == run_dir / "report.site.html"
    assert mod._verify_html_inline_script(run_dir / "report.local.html") in {
        "inline-script-present", "node-check-passed"
    }
    assert (run_dir / "site_publish.md").exists()
    manifest = json.loads((run_dir / "manifest.json").read_text())
    assert manifest["stages"]["parse"]["status"] == "done"
    assert manifest["stages"]["aggregate"]["status"] == "done"
    assert manifest["stages"]["triage"]["status"] == "done"
    assert manifest["render_targets"]["local"]["status"] == "generated"
    assert manifest["render_targets"]["site"]["status"] == "generated"
    assert manifest["render_targets"]["site"]["publish_doc"] == "site_publish.md"


def test_cli_stage_commands_run_incrementally(tmp_path, capsys):
    trace_id = "019f7d0a-6891-7f63-81dc-1452e30fa4f1"
    log = tmp_path / "cli-stage.log"
    log.write_text(
        f"2026-07-20T13:10:00.000000 | INFO | access_recorder | 192.0.2.30 | 1 | {trace_id} | - | 0 | DS_KV_CLIENT_GET | 20298 | 1024\n",
        encoding="utf-8",
    )

    mod = _load_module()
    assert mod.main(["parse", str(log), "--out", str(tmp_path / "runs"), "--case", "cli-stage"]) == 0
    run_dir = Path(capsys.readouterr().out.strip())
    assert (run_dir / "parsed_traces.json").exists()
    assert not (run_dir / "summary.json").exists()

    assert mod.main(["aggregate", str(run_dir)]) == 0
    assert Path(capsys.readouterr().out.strip()).name == "summary.json"
    assert mod.main(["triage", str(run_dir)]) == 0
    assert Path(capsys.readouterr().out.strip()).name == "triage.json"
    assert mod.main(["render-local", str(run_dir)]) == 0
    assert Path(capsys.readouterr().out.strip()).name == "report.local.html"
    assert mod.main(["render-site", str(run_dir)]) == 0
    assert Path(capsys.readouterr().out.strip()).name == "report.site.html"
    assert mod.main(["publish-site", str(run_dir), "--dry-run"]) == 0
    publish_output = capsys.readouterr().out.strip()
    assert ("https://yche" + ".me/perf/") in publish_output
    assert "DRY-RUN" in publish_output
    manifest = json.loads((run_dir / "manifest.json").read_text())
    assert manifest["render_targets"]["site"]["publish"]["status"] == "dry-run"


def test_publish_site_stage_executes_copy_and_verify_commands(tmp_path, monkeypatch):
    trace_id = "019f7d0b-ded8-7927-9979-0ddc4141613e"
    log = tmp_path / "publish.log"
    log.write_text(
        f"2026-07-20T13:15:00.000000 | INFO | access_recorder | 192.0.2.30 | 1 | {trace_id} | - | 0 | DS_KV_CLIENT_GET | 20298 | 1024\n",
        encoding="utf-8",
    )

    mod = _load_module()
    run_dir = mod.run_pipeline([str(log)], tmp_path / "runs", case_name="publish-case", code_ref="unit-test")
    calls = []

    def fake_run(cmd, check, **kwargs):
        calls.append((cmd, kwargs))
        class Result:
            returncode = 0
            stdout = (
                'Trace 分析报告 id="coverage-table" id="flow-stage-chart" '
                'id="download-report-summary" /assets/css/site.css /assets/js/site.js'
            )
        return Result()

    monkeypatch.setattr(mod.subprocess, "run", fake_run)
    monkeypatch.setenv(mod.PUBLISH_HOST_ENV, "publish-host.example")
    monkeypatch.setenv(mod.PUBLISH_ROOT_ENV, "/srv/site/perf")
    url = mod.publish_site_stage(run_dir, dry_run=False)

    assert url.startswith("https://yche" + ".me/perf/")
    assert calls[0][0][0] == "scp"
    assert calls[0][0][1] == str(run_dir / "report.site.html")
    assert calls[0][0][2].startswith("publish-host.example:/srv/site/perf/")
    assert calls[1][0][:2] == ["curl", "-fsSI"]
    assert calls[2][0][:4] == ["curl", "-fsSL", "-A", "Mozilla/5.0"]
    assert calls[2][1]["capture_output"] is True
    assert calls[2][1]["text"] is True
    manifest = json.loads((run_dir / "manifest.json").read_text(encoding="utf-8"))
    assert manifest["render_targets"]["site"]["publish"]["status"] == "published"
    assert manifest["render_targets"]["site"]["publish"]["live_markers"] == "verified"
    assert manifest["render_targets"]["site"]["publish"]["source_size_bytes"] > 0
    assert manifest["render_targets"]["site"]["publish"]["max_site_html_bytes"] == mod.DEFAULT_SITE_HTML_MAX_BYTES


def test_publish_site_stage_requires_target_env_for_real_publish(tmp_path, monkeypatch):
    trace_id = "019f7d0b-e4e2-7764-8cf9-fab58e4cbfbb"
    log = tmp_path / "publish-env.log"
    log.write_text(
        f"2026-07-20T13:15:00.000000 | INFO | access_recorder | 192.0.2.30 | 1 | {trace_id} | - | 0 | DS_KV_CLIENT_GET | 20298 | 1024\n",
        encoding="utf-8",
    )

    mod = _load_module()
    run_dir = mod.run_pipeline([str(log)], tmp_path / "runs", case_name="publish-env-case", code_ref="unit-test")
    calls = []

    monkeypatch.delenv(mod.PUBLISH_HOST_ENV, raising=False)
    monkeypatch.delenv(mod.PUBLISH_ROOT_ENV, raising=False)
    monkeypatch.setattr(mod.subprocess, "run", lambda *args, **kwargs: calls.append((args, kwargs)))

    with pytest.raises(SystemExit, match=mod.PUBLISH_HOST_ENV):
        mod.publish_site_stage(run_dir, dry_run=False)
    assert calls == []


def test_publish_site_stage_blocks_oversized_html_before_copy(tmp_path, monkeypatch):
    trace_id = "019f7d0b-e9ec-79d1-9567-14e4179ace75"
    log = tmp_path / "publish-large.log"
    log.write_text(
        f"2026-07-20T13:16:00.000000 | INFO | access_recorder | 192.0.2.30 | 1 | {trace_id} | - | 0 | DS_KV_CLIENT_GET | 20298 | 1024\n",
        encoding="utf-8",
    )

    mod = _load_module()
    run_dir = mod.run_pipeline([str(log)], tmp_path / "runs", case_name="publish-large", code_ref="unit-test")
    calls = []

    def fake_run(cmd, check, **kwargs):
        calls.append((cmd, kwargs))
        raise AssertionError("scp/curl must not run for oversized HTML")

    monkeypatch.setattr(mod.subprocess, "run", fake_run)
    with pytest.raises(SystemExit) as exc:
        mod.publish_site_stage(run_dir, dry_run=False, max_site_html_bytes=1)

    assert "Refuse to publish oversized site HTML" in str(exc.value)
    assert calls == []
    manifest = json.loads((run_dir / "manifest.json").read_text(encoding="utf-8"))
    publish = manifest["render_targets"]["site"]["publish"]
    assert publish["status"] == "blocked-size-limit"
    assert publish["source_size_bytes"] > publish["max_site_html_bytes"]


def test_trace_run_pipeline_object_exposes_stage_boundaries(tmp_path):
    trace_id = "019f7d0e-c704-7767-917a-3907373e9d31"
    log = tmp_path / "object-pipeline.log"
    log.write_text(
        f"2026-07-20T13:20:00.000000 | INFO | access_recorder | 192.0.2.30 | 1 | {trace_id} | - | 0 | DS_KV_CLIENT_GET | 20298 | 1024\n",
        encoding="utf-8",
    )

    mod = _load_module()
    pipeline = mod.TraceRunPipeline()
    run_dir = pipeline.parse([str(log)], tmp_path / "runs", case_name="object-pipeline", code_ref="unit-test")

    assert isinstance(pipeline.store, mod.TraceRunStore)
    assert isinstance(pipeline.site_publisher, mod.TraceSitePublisher)
    assert (run_dir / "parsed_traces.json").exists()
    assert pipeline.aggregate(run_dir).name == "summary.json"
    assert pipeline.triage(run_dir).name == "triage.json"
    assert pipeline.render_local(run_dir).name == "report.local.html"
    assert pipeline.render_site(run_dir).name == "report.site.html"

    manifest = json.loads((run_dir / "manifest.json").read_text(encoding="utf-8"))
    assert manifest["stages"]["parse"]["status"] == "done"
    assert manifest["stages"]["aggregate"]["status"] == "done"
    assert manifest["render_targets"]["local"]["status"] == "generated"


def test_trace_analyzer_composes_object_boundaries_for_accumulation_and_dimensions(tmp_path):
    trace_id = "019f7d0e-c704-7767-917a-3907373e9d31"
    log = tmp_path / "object-analyzer.log"
    log.write_text(
        f"2026-07-20T13:25:00.000000 | INFO | access_recorder | 192.0.2.30 | 1 | {trace_id} | - | 0 | DS_KV_CLIENT_GET | 20298 | 1024\n",
        encoding="utf-8",
    )

    mod = _load_module()
    analyzer = mod.TraceAnalyzer()
    report = analyzer.analyze([str(log)], code_ref="unit-test")

    assert isinstance(analyzer.accumulator, mod.TraceAccumulator)
    assert isinstance(analyzer.dimension_builder, mod.TraceDimensionBuilder)
    assert hasattr(analyzer.accumulator, "ingest")
    assert hasattr(analyzer.dimension_builder, "build")
    assert report["trace_count"] == 1
    assert report["dimensions"]["flow"]["DS_KV_CLIENT_GET"] == 1


def test_trace_analyzer_uses_injected_accumulator_and_dimension_builder(tmp_path):
    trace_id = "019f7d0f-80ec-73bc-91ce-8e84de820001"
    log = tmp_path / "inject.log"
    log.write_text(
        "\n".join(
            [
                f"2026-07-20T13:26:00.000000 | INFO | access_recorder | 192.0.2.30 | 1 | {trace_id} | - | 0 | DS_KV_CLIENT_GET | 20298 | 1024",
                "no trace id here",
            ]
        ),
        encoding="utf-8",
    )

    class RecordingAccumulator:
        def __init__(self, paths):
            self.paths = list(paths)
            self.ingested = []

        def ingest(self, parsed, line):
            self.ingested.append((parsed["trace_id"], line))

        def finish(self):
            return {"paths": self.paths, "ingested": self.ingested}

    class RecordingDimensionBuilder:
        def __init__(self):
            self.snapshot = None

        def build(self, snapshot, paths, code_ref="unknown"):
            self.snapshot = snapshot
            return {
                "code_ref": code_ref,
                "inputs": list(paths),
                "trace_count": len(snapshot["ingested"]),
                "traces": [trace_id for trace_id, _ in snapshot["ingested"]],
            }

    mod = _load_module()
    builder = RecordingDimensionBuilder()
    analyzer = mod.TraceAnalyzer(accumulator_cls=RecordingAccumulator, dimension_builder=builder)

    report = analyzer.analyze([str(log)], code_ref="unit-test")

    assert isinstance(analyzer.accumulator, RecordingAccumulator)
    assert report["trace_count"] == 1
    assert report["traces"] == [trace_id]
    assert builder.snapshot["paths"] == [str(log)]


def test_trace_input_reader_reads_plain_gzip_and_skips_invalid_binary(tmp_path):
    trace_id = "019f7d0f-80ec-73bc-91ce-8e84de820002"
    gz = tmp_path / "reader.log.gz"
    with gzip.open(gz, "wt", encoding="utf-8") as f:
        f.write(
            f"2026-07-20T13:27:00.000000 | INFO | worker | kvworker-0-worker1 | 1 | {trace_id} | gzip line safe IP 192.0.2.254\n"
        )
    invalid = tmp_path / "invalid.gz"
    invalid.write_bytes(b"not-a-valid-gzip")

    mod = _load_module()
    rows = list(mod.TraceInputReader().iter_lines([str(gz), str(invalid)]))

    assert rows == [(str(gz), gz.name, 1, rows[0][3])]
    assert trace_id in rows[0][3]
    assert mod.TraceInputReader().failures == []


def test_invalid_input_fails_by_default_and_can_be_recorded_as_partial(tmp_path):
    valid_trace = "019f7d0f-80ec-73bc-91ce-8e84de820012"
    valid = tmp_path / "valid.log"
    invalid = tmp_path / "invalid.gz"
    valid.write_text(
        f"2026-07-20T13" + ":" + f"27" + ":" + f"00.000000 | INFO | worker | kvworker-0-worker1 | 1 | {valid_trace} | ok\n",
        encoding="utf-8",
    )
    invalid.write_bytes(b"not-a-valid-gzip")

    mod = _load_module()
    with pytest.raises(SystemExit, match="Failed to read trace input"):
        mod.analyze_inputs([str(valid), str(invalid)], code_ref="unit-test")

    report = mod.analyze_inputs([str(valid), str(invalid)], code_ref="unit-test", allow_partial_inputs=True)
    assert report["trace_count"] == 1
    assert report["dimensions"]["input_failures"][0]["path"] == str(invalid)


def test_raw_tar_preservation_rejects_absolute_member_paths(tmp_path):
    bundle = tmp_path / "unsafe.tar.gz"
    _write_tar_gz(bundle, {"/escape.log": "unsafe"})

    mod = _load_module()
    store = mod.TraceRunStore()
    with pytest.raises(ValueError, match="Unsafe tar member path"):
        store.prepare_parse_run([str(bundle)], tmp_path / "runs", "unsafe-case", "", "ref")
    assert not (tmp_path / "escape.log").exists()


def test_raw_tar_preservation_enforces_member_budget(tmp_path, monkeypatch):
    bundle = tmp_path / "many.tar.gz"
    _write_tar_gz(bundle, {"a.log": "a", "b.log": "b"})

    mod = _load_module()
    monkeypatch.setattr(mod, "DEFAULT_MAX_TAR_MEMBERS", 1)
    with pytest.raises(ValueError, match="Tar member count exceeds"):
        mod.TraceRunStore().prepare_parse_run([str(bundle)], tmp_path / "runs", "budget-case", "", "ref")


def test_trace_dimension_builder_emits_empty_report_contract():
    mod = _load_module()
    snapshot = mod.TraceAccumulator([]).finish()

    report = mod.TraceDimensionBuilder().build(snapshot, [], code_ref="empty-ref")

    assert report["code_ref"] == "empty-ref"
    assert report["trace_count"] == 0
    assert report["dimensions"]["time"] == {"first_ts": None, "last_ts": None}
    assert report["dimensions"]["coverage"]["surfaces"]["client_access"]["status"] == "missing"
    assert report["dimensions"]["ub_summary"] == {"transfer_path": {}, "edges": {}}


def test_trace_run_store_prepares_parse_run_cache_and_raw_inputs(tmp_path):
    trace_id = "019f7d0f-80ec-73bc-91ce-8e84de820003"
    bundle = tmp_path / "store-input.tar.gz"
    _write_tar_gz(
        bundle,
        {"case/worker.log": f"2026-07-20T13:28:00.000000 | INFO | worker | 192.0.2.10 | 1 | {trace_id} | store\n"},
    )

    mod = _load_module()
    store = mod.TraceRunStore()
    prepared = store.prepare_parse_run([str(bundle)], tmp_path / "runs", "store-case", "scenario", "ref")

    assert prepared["cached"] is False
    assert (prepared["run_dir"] / "raw" / "inputs" / bundle.name).exists()
    assert (prepared["run_dir"] / "raw" / "extracted" / bundle.name / "case" / "worker.log").exists()
    store.write_parse_outputs(
        prepared["run_dir"],
        {"dimensions": {"time": {"first_ts": None, "last_ts": None}}, "traces": {}},
        [],
        "store-case",
        "scenario",
        "ref",
        prepared["created_at"],
        prepared["cache_key"],
        prepared["identities"],
    )

    cached = store.prepare_parse_run([str(bundle)], tmp_path / "runs", "store-case", "scenario", "ref")

    assert cached == {"run_dir": prepared["run_dir"], "cached": True}
    manifest = json.loads((prepared["run_dir"] / "manifest.json").read_text(encoding="utf-8"))
    assert manifest["inputs"][0]["members"] == ["case/worker.log"]


def test_directory_cache_identity_changes_when_contents_change(tmp_path):
    trace_a = "019f7d0f-80ec-73bc-91ce-8e84de820013"
    trace_b = "019f7d0f-80ec-73bc-91ce-8e84de820014"
    input_dir = tmp_path / "input-dir"
    input_dir.mkdir()
    log = input_dir / "worker.log"
    log.write_text(
        f"2026-07-20T13" + ":" + f"28" + ":" + f"00.000000 | INFO | worker | kvworker-0-worker1 | 1 | {trace_a} | first\n",
        encoding="utf-8",
    )

    mod = _load_module()
    first = mod.run_pipeline([str(input_dir)], tmp_path / "runs", case_name="dir-cache")
    log.write_text(
        f"2026-07-20T13" + ":" + f"28" + ":" + f"00.000000 | INFO | worker | kvworker-0-worker1 | 1 | {trace_b} | second\n",
        encoding="utf-8",
    )
    second = mod.run_pipeline([str(input_dir)], tmp_path / "runs", case_name="dir-cache")

    assert second != first
    assert trace_b in json.loads((second / "summary.json").read_text(encoding="utf-8"))["traces"]


def test_ub_lifecycle_request_ids_are_scoped_by_trace(tmp_path):
    trace_a = "019f7d0f-80ec-73bc-91ce-8e84de820015"
    trace_b = "019f7d0f-80ec-73bc-91ce-8e84de820016"
    log = tmp_path / "ub-requests.log"
    log.write_text(
        "\n".join(
            [
                f"2026-07-20T13:29:00.000000 | WARN | worker | kvworker-0-worker1 | 1 | {trace_a} | [URMA_ELAPSED_TOTAL] cost 7.1ms, request id:42, src address: 192.0.2.10, target address: 192.0.2.20, status: OK",
                f"2026-07-20T13:29:00.001000 | WARN | worker | kvworker-0-worker2 | 1 | {trace_b} | [URMA_ELAPSED_TOTAL] cost 9.2ms, request id:42, src address: 192.0.2.30, target address: 192.0.2.40, status: OK",
            ]
        ),
        encoding="utf-8",
    )

    mod = _load_module()
    report = mod.analyze_inputs([str(log)], code_ref="unit-test")
    requests = report["dimensions"]["ub_lifecycle_summary"]["requests"]

    assert len([row for row in requests if row["request_id"] == "42"]) == 2
    assert {row["trace_id"] for row in requests} == {trace_a, trace_b}


def test_trace_site_publisher_size_guard_reports_ok_and_too_large(tmp_path):
    report = tmp_path / "report.site.html"
    report.write_text("abc", encoding="utf-8")

    mod = _load_module()
    publisher = mod.TraceSitePublisher()

    assert publisher.size_guard(report, max_bytes=3)["size_status"] == "ok"
    too_large = publisher.size_guard(report, max_bytes=2)
    assert too_large["size_status"] == "too_large"
    assert too_large["source_size_bytes"] == 3


def test_parser_extension_rules_add_new_errors_and_metrics(tmp_path):
    trace_id = "019f7d0c-d7a5-7967-bbd3-cbb6d899a501"
    log = tmp_path / "extension.log"
    log.write_text(
        "\n".join(
            [
                f"2026-07-20T13:30:00.000000 | INFO | access_recorder | 192.0.2.30 | 1 | {trace_id} | - | 0 | DS_KV_CLIENT_GET | 20298 | 1024",
                f"2026-07-20T13:30:00.001000 | WARN | worker | kvdataworker-0-worker2 | 1 | {trace_id} | [URMA_ELAPSED_DMA] cost 2500us, lane:2, safe IP 192.0.2.254",
                f"2026-07-20T13:30:00.002000 | ERROR | worker | kvdataworker-0-worker2 | 1 | {trace_id} | DMA_WAIT_TIMEOUT lane 2 safe IP 192.0.2.254",
            ]
        ),
        encoding="utf-8",
    )

    mod = _load_module()
    mod.register_error_pattern("DMA_WAIT_TIMEOUT")
    mod.register_metric_rule(
        "urma_dma",
        r"\[URMA_ELAPSED_DMA\].*?cost\s+([\d.]+)\s*(us|ms)",
        unit_group=2,
    )
    report = mod.analyze_inputs([str(log)], code_ref="unit-test")

    assert report["dimensions"]["errors"]["DMA_WAIT_TIMEOUT"] == 1
    assert report["traces"][trace_id]["errors"]["DMA_WAIT_TIMEOUT"] == 1
    assert report["dimensions"]["custom_metrics_ms"]["urma_dma"]["p50"] == 2.5
    assert report["traces"][trace_id]["custom_metrics_ms"]["urma_dma"] == 2.5


def test_trace_parser_uses_injected_rules_without_global_state():
    trace_id = "019f7d0d-9185-74c3-85df-57cd02a3d901"
    line = (
        f"2026-07-20T13:31:00.000000 | ERROR | worker | kvdataworker-0-worker2 | 1 | {trace_id} | safe IP 192.0.2.254 "
        "[URMA_ELAPSED_DMA] cost 2500us DMA_WAIT_TIMEOUT lane 2"
    )

    mod = _load_module()
    rules = mod.ParserRules()
    rules.register_error_pattern("DMA_WAIT_TIMEOUT")
    rules.register_metric_rule(
        "urma_dma",
        r"\[URMA_ELAPSED_DMA\].*?cost\s+([\d.]+)\s*(us|ms)",
        unit_group=2,
    )
    parsed = mod.TraceParser(rules).parse_line("worker.log", "worker.log", 1, line)

    assert parsed["trace_id"] == trace_id
    assert parsed["worker"] == "kvdataworker-0-worker2"
    assert parsed["errors"] == ["DMA_WAIT_TIMEOUT"]
    assert parsed["custom_metrics_ms"] == {"urma_dma": 2.5}


def test_cli_self_test_writes_json_and_markdown(tmp_path, capsys):
    mod = _load_module()
    out_json = tmp_path / "summary.json"
    out_md = tmp_path / "summary.md"

    rc = mod.main(["--self-test", "--output-json", str(out_json), "--output-md", str(out_md)])

    assert rc == 0
    assert json.loads(out_json.read_text())["self_test"] is True
    assert "Trace Triage Summary" in out_md.read_text()
    assert "self-test passed" in capsys.readouterr().out
