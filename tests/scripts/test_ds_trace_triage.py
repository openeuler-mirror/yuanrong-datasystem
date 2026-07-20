import importlib.util
import io
import json
import tarfile
from pathlib import Path


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
                    f"2026-07-18T19:20:03.100000 | INFO | access_recorder | 192.168.168.206 | 42 | {trace_id} | - | 0 | DS_KV_CLIENT_GET | 518923 | 4096",
                    f"2026-07-18T19:20:03.130000 | INFO | worker | 192.168.168.206 | 42 | {trace_id} | [Get] Done, totalCost: 518.9ms, exceed 3ms: {{ ProcessGetObjectRequest: 517 ms, QueryMeta: 0 ms }}",
                    f"2026-07-18T19:20:03.150000 | WARN | worker | 192.168.168.206 | 42 | {trace_id} | [ZMQ_RPC_FRAMEWORK_SLOW] e2e_us=8012 client_req_framework_us=100 remote_processing_us=7600 client_rsp_framework_us=120 server_req_queue_us=20 server_exec_us=7500 server_rsp_queue_us=80 network_residual_us=292 method=WorkerOCService.Get",
                    f"2026-07-18T19:20:03.200000 | WARN | worker | 192.168.233.92 | 42 | {trace_id} | [URMA_ELAPSED_TOTAL] cost 517.732ms, request id:77, src address:192.168.233.92:31501, target address:192.168.168.206:31501, dataSize:4194304, cpuid:12, status: OK",
                    f"2026-07-18T19:20:03.201000 | WARN | worker | 192.168.233.92 | 42 | {trace_id} | [URMA_ELAPSED_POLL_JFC] cost 0.309ms, request id:77",
                    f"2026-07-18T19:20:03.202000 | WARN | worker | 192.168.233.92 | 42 | {trace_id} | [URMA_ELAPSED_NOTIFY] cost 0.041ms, request id:77",
                    f"2026-07-18T19:20:03.203000 | WARN | worker | 192.168.233.92 | 42 | {trace_id} | [URMA_ELAPSED_THREAD_SHED] cost 12.500ms, request id:77",
                    f"2026-07-18T19:20:03.230000 | ERROR | worker | 192.168.168.206 | 42 | {trace_id} | RPC deadline exceeded while waiting WorkerOCService.Get",
                ]
            )
        },
    )

    mod = _load_module()
    report = mod.analyze_inputs([str(bundle)], code_ref="unit-test")

    assert report["schema_version"] == 1
    assert report["trace_count"] == 1
    assert report["dimensions"]["time"]["first_ts"].startswith("2026-07-18T19:20:03")
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
    assert any(item["log_surface"] == "access log" for item in source_appendix)
    assert any(item["log_surface"] == "URMA_ELAPSED_TOTAL" for item in source_appendix)
    assert any("CodeGraph" in item["validation"] for item in source_appendix)
    assert any("client -> entry worker" in item["flow_stage"] for item in source_appendix)
    assert any("entry worker -> data worker" in item["flow_stage"] for item in source_appendix)
    assert any("data worker UB write" in item["flow_stage"] for item in source_appendix)
    assert any("entry worker -> meta worker publish" in item["flow_stage"] for item in source_appendix)
    flow_stages = report["dimensions"]["flow_stages"]
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
                f"2026-07-19T03:40:00.000000 | INFO | client | 192.168.1.10 | 7 | {trace_id} | - | 0 | DS_KV_CLIENT_SET | 4268 | 1835008",
                f"2026-07-19T03:40:00.001000 | INFO | client | 192.168.1.10 | 7 | {trace_id} | Set done latencySummary:{{client.process.memory_copy:2988, client.rpc.publish:690, client.rpc.create:490, client.process.set:21}}",
                f"2026-07-19T03:40:00.002000 | INFO | worker | kvchachzpworker-0-worker10 | 7 | {trace_id} | Publish done latencySummary:{{worker.rpc.create_meta:2702, worker.process.publish:64}}",
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
                f"2026-07-20T10:00:00.000000 | INFO | access_recorder | 10.0.0.1 | 1 | {trace_id} | - | 0 | DS_KV_CLIENT_GET | 20298 | 4096",
                f"2026-07-20T10:00:00.010000 | INFO | worker | kventryworker-0-worker1 | 1 | {trace_id} | [Get] Done, clientId: c1, objects: 1, transferPath: UB, totalCost: 230.100ms, inflightRemoteGet: 9 exceed 3ms: {{ ProcessGetObjectRequest: 230 ms }}",
                f"2026-07-20T10:00:00.020000 | INFO | worker | kventryworker-0-worker1 | 1 | {trace_id} | Remote get request:[881] object:[obj-a], offset[0] size[4194304] src address:10.0.0.1:31501, dst address:10.0.0.2:31501",
                f"2026-07-20T10:00:00.040000 | INFO | worker | kventryworker-0-worker1 | 1 | {trace_id} | Remote get success, objectKey: obj-a, path: UB, cost: 231.321ms src address:10.0.0.1:31501, dst address:10.0.0.2:31501",
                f"2026-07-20T10:00:00.050000 | WARN | worker | kvdataworker-0-worker2 | 1 | {trace_id} | [URMA_ELAPSED_TOTAL]: Time from urma_post_jetty_send_wr to urma_write completion total cost 231.001ms, wait os sched thread finish time(std::condition_variable.wait_for): 230.500ms, request id:881, src address:10.0.0.2:31501, target address:10.0.0.1:31501, dataSize:4194304, cpuid:23, status: OK, urma_inflight_wr_count: 11",
                f"2026-07-20T10:00:00.051000 | WARN | worker | kvdataworker-0-worker2 | 1 | {trace_id} | [URMA_ELAPSED_POLL_JFC]: urma_poll_jfc cost 309us, cpuid: 23, suggest: check URMA",
                f"2026-07-20T10:00:00.052000 | WARN | worker | kvdataworker-0-worker2 | 1 | {trace_id} | [URMA_ELAPSED_NOTIFY]: urma_poll_jfc thread notify urma_post_jetty_send_wr thread wake up cost 0.041ms, cpuid: 23, count: 1",
                f"2026-07-20T10:00:00.053000 | WARN | worker | kvdataworker-0-worker2 | 1 | {trace_id} | [URMA_ELAPSED_THREAD_SHED]: urma_poll_jfc thread wake up after nanosleep(1us) cost 12500us, cpuid: 23",
                f"2026-07-20T10:00:00.070000 | ERROR | worker | kventryworker-0-worker1 | 1 | {trace_id} | RPC deadline exceeded while waiting WorkerOCService.Get",
            ]
        ),
        encoding="utf-8",
    )

    mod = _load_module()
    report = mod.analyze_inputs([str(log)], code_ref="unit-test")
    trace = report["traces"][trace_id]

    total = next(event for event in trace["ub_events"] if event["event_type"] == "total")
    assert total["request_id"] == "881"
    assert total["src_addr"] == "10.0.0.2:31501"
    assert total["target_addr"] == "10.0.0.1:31501"
    assert total["data_size"] == 4194304
    assert total["cpuid"] == 23
    assert total["status"] == "OK"
    assert total["urma_inflight_wr_count"] == 11
    assert total["wait_os_sched_ms"] == 230.5
    assert report["dimensions"]["urma_elapsed"]["poll_jfc"]["p50"] == 0.309
    assert report["dimensions"]["urma_elapsed"]["thread_sched"]["p50"] == 12.5
    assert report["dimensions"]["ub_summary"]["transfer_path"]["UB"] == 2
    assert report["dimensions"]["ub_summary"]["edges"]["10.0.0.2:31501 -> 10.0.0.1:31501"]["count"] == 1
    assert report["dimensions"]["time_buckets"]["1000ms"][0]["trace_count"] == 1
    assert report["dimensions"]["time_buckets"]["1000ms"][0]["burst_score"] >= 1
    assert report["dimensions"]["worker_summary"]["kventryworker-0-worker1"]["roles"] == ["entry_worker"]
    assert report["dimensions"]["worker_summary"]["kvdataworker-0-worker2"]["roles"] == ["data_worker"]
    assert report["dimensions"]["worker_edges"]["10.0.0.2:31501 -> 10.0.0.1:31501"]["p99_ms"] == 231.001
    assert "late_worker_completion" in trace["triage_flags"]


def test_run_pipeline_writes_intermediate_outputs_and_html_targets(tmp_path):
    trace_id = "019f7c62-9e9f-7792-a5d8-f4d30275bafe"
    log = tmp_path / "client.log"
    log.write_text(
        "\n".join(
            [
                f"2026-07-20T11:00:00.000000 | INFO | client | 10.0.0.9 | 1 | {trace_id} | - | 0 | DS_KV_CLIENT_SET | 4268 | 1024",
                f"2026-07-20T11:00:00.001000 | INFO | client | 10.0.0.9 | 1 | {trace_id} | Set done latencySummary:{{client.process.memory_copy:2988, client.rpc.publish:690, client.rpc.create:490, client.process.set:21}}",
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
    assert "class=\"subtitle\"" in html
    assert "id=\"run-metadata-table\"" in html
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
    assert "id=\"recommendation-table\"" in html
    assert "id=\"source-appendix-table\"" in html
    assert "代码与字段映射" in html
    assert "建议与后续口径" in html
    assert "错误线" in html
    assert "慢时延线" in html
    assert "证据边界" in html
    assert "id=\"classification-chart\"" in html
    assert "id=\"cohort-chart\"" in html
    assert "id=\"cohort-table\"" in html
    assert "id=\"latency-chart\"" in html
    assert "id=\"flow-stage-chart\"" in html
    assert "id=\"flow-stage-table\"" in html
    assert "Client→Entry→Meta/Data 流程" in html
    assert "id=\"worker-chart\"" in html
    assert "class=\"controls pager\"" in html
    assert "搜索 trace / worker / 关键词" in html
    assert "id=\"worker-filter\"" in html
    assert "id=\"trace-page-size\"" in html
    assert "全部 Worker" in html
    assert "每页" in html
    assert "download-selected-raw" in html
    assert "download-filtered-evidence" in html
    assert "highlightLogLine" in html
    assert "echarts.init" in html
    assert "id=\"classification-table\"" in html
    assert "id=\"error-table\"" in html
    assert "id=\"worker-table\"" in html
    assert "id=\"top-trace-table\"" in html
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
    assert "xqyun-32c32g" in publish_doc
    assert "/var/www/html/perf/" in publish_doc
    assert "https://yche.me/perf/" in publish_doc
    assert "report.site.html" in publish_doc
    triage = json.loads((run_dir / "triage.json").read_text())
    assert triage["issue_candidates"][0]["classification"] == "write_memory_copy_dominant"


def test_run_pipeline_preserves_raw_extracted_logs_and_reuses_cache(tmp_path):
    trace_id = "019f7d06-cbda-7381-a95f-99fed6ee3732"
    bundle = tmp_path / "multi-input.tar.gz"
    _write_tar_gz(
        bundle,
        {
            "case-a/kventryworker-0-worker1/worker.log": "\n".join(
                [
                    f"2026-07-20T12:00:00.000000 | INFO | access_recorder | 10.0.0.1 | 1 | {trace_id} | - | 0 | DS_KV_CLIENT_GET | 20298 | 4096",
                    f"2026-07-20T12:00:00.010000 | INFO | worker | kventryworker-0-worker1 | 1 | {trace_id} | [Get] Done, clientId: c1, objects: 1, transferPath: UB, totalCost: 20.298ms, inflightRemoteGet: 1",
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
                f"2026-07-20T12:10:00.000000 | INFO | access_recorder | 10.0.0.1 | 1 | {read_trace} | - | 0 | DS_KV_CLIENT_GET | 20298 | 4096",
                f"2026-07-20T12:10:00.001000 | INFO | client | 10.0.0.9 | 1 | {read_trace} | Get done latencySummary:{{client.rpc.get:20298}}",
                f"2026-07-20T12:10:00.010000 | INFO | worker | kventryworker-0-worker1 | 1 | {read_trace} | Remote get success, objectKey: obj-a, path: UB, cost: 231.321ms src address:10.0.0.1:31501, dst address:10.0.0.2:31501",
                f"2026-07-20T12:10:00.020000 | WARN | worker | kvdataworker-0-worker2 | 1 | {read_trace} | [URMA_ELAPSED_TOTAL] cost 231.001ms, request id:77, src address:10.0.0.2:31501, target address:10.0.0.1:31501, dataSize:4096, cpuid:2, status: OK",
                f"2026-07-20T12:10:01.000000 | INFO | access_recorder | 10.0.0.9 | 1 | {write_trace} | - | 0 | DS_KV_CLIENT_SET | 4268 | 1024",
                f"2026-07-20T12:10:01.001000 | INFO | client | 10.0.0.9 | 1 | {write_trace} | Set done latencySummary:{{client.process.memory_copy:2988, client.rpc.publish:690, client.rpc.create:490, client.process.set:21}}",
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


def test_multiple_input_files_are_reported_as_cohorts(tmp_path):
    trace_a = "019f7d10-1f09-762a-9163-c547ef71a101"
    trace_b = "019f7d10-b880-7937-b945-382161303102"
    noisy = tmp_path / "noisy.log"
    clean = tmp_path / "clean.log"
    noisy.write_text(
        f"2026-07-20T14:00:00.000000 | INFO | access_recorder | 10.0.0.1 | 1 | {trace_a} | - | 1001 | DS_KV_CLIENT_GET | 20298 | 0\n"
        f"2026-07-20T14:00:00.001000 | ERROR | client | 10.0.0.1 | 1 | {trace_a} | RPC deadline exceeded\n",
        encoding="utf-8",
    )
    clean.write_text(
        f"2026-07-20T14:00:01.000000 | INFO | access_recorder | 10.0.0.2 | 1 | {trace_b} | - | 0 | DS_KV_CLIENT_GET | 8000 | 0\n",
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


def test_independent_stage_functions_consume_previous_artifacts(tmp_path):
    trace_id = "019f7d09-3aa3-77bd-a738-9965d66c9f23"
    log = tmp_path / "stage.log"
    log.write_text(
        "\n".join(
            [
                f"2026-07-20T13:00:00.000000 | INFO | access_recorder | 10.0.0.9 | 1 | {trace_id} | - | 0 | DS_KV_CLIENT_SET | 4268 | 1024",
                f"2026-07-20T13:00:00.001000 | INFO | client | 10.0.0.9 | 1 | {trace_id} | Set done latencySummary:{{client.process.memory_copy:2988, client.rpc.publish:690, client.rpc.create:490, client.process.set:21}}",
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
        f"2026-07-20T13:10:00.000000 | INFO | access_recorder | 10.0.0.9 | 1 | {trace_id} | - | 0 | DS_KV_CLIENT_GET | 20298 | 1024\n",
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
    assert "https://yche.me/perf/" in publish_output
    assert "DRY-RUN" in publish_output
    manifest = json.loads((run_dir / "manifest.json").read_text())
    assert manifest["render_targets"]["site"]["publish"]["status"] == "dry-run"


def test_publish_site_stage_executes_copy_and_verify_commands(tmp_path, monkeypatch):
    trace_id = "019f7d0b-ded8-7927-9979-0ddc4141613e"
    log = tmp_path / "publish.log"
    log.write_text(
        f"2026-07-20T13:15:00.000000 | INFO | access_recorder | 10.0.0.9 | 1 | {trace_id} | - | 0 | DS_KV_CLIENT_GET | 20298 | 1024\n",
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
    url = mod.publish_site_stage(run_dir, dry_run=False)

    assert url.startswith("https://yche.me/perf/")
    assert calls[0][0][0] == "scp"
    assert calls[0][0][1] == str(run_dir / "report.site.html")
    assert calls[0][0][2].startswith("xqyun-32c32g:/var/www/html/perf/")
    assert calls[1][0][:2] == ["curl", "-fsSI"]
    assert calls[2][0][:4] == ["curl", "-fsSL", "-A", "Mozilla/5.0"]
    assert calls[2][1]["capture_output"] is True
    assert calls[2][1]["text"] is True
    manifest = json.loads((run_dir / "manifest.json").read_text(encoding="utf-8"))
    assert manifest["render_targets"]["site"]["publish"]["status"] == "published"
    assert manifest["render_targets"]["site"]["publish"]["live_markers"] == "verified"


def test_trace_run_pipeline_object_exposes_stage_boundaries(tmp_path):
    trace_id = "019f7d0e-c704-7767-917a-3907373e9d31"
    log = tmp_path / "object-pipeline.log"
    log.write_text(
        f"2026-07-20T13:20:00.000000 | INFO | access_recorder | 10.0.0.9 | 1 | {trace_id} | - | 0 | DS_KV_CLIENT_GET | 20298 | 1024\n",
        encoding="utf-8",
    )

    mod = _load_module()
    pipeline = mod.TraceRunPipeline()
    run_dir = pipeline.parse([str(log)], tmp_path / "runs", case_name="object-pipeline", code_ref="unit-test")

    assert (run_dir / "parsed_traces.json").exists()
    assert pipeline.aggregate(run_dir).name == "summary.json"
    assert pipeline.triage(run_dir).name == "triage.json"
    assert pipeline.render_local(run_dir).name == "report.local.html"
    assert pipeline.render_site(run_dir).name == "report.site.html"

    manifest = json.loads((run_dir / "manifest.json").read_text(encoding="utf-8"))
    assert manifest["stages"]["parse"]["status"] == "done"
    assert manifest["stages"]["aggregate"]["status"] == "done"
    assert manifest["render_targets"]["local"]["status"] == "generated"


def test_parser_extension_rules_add_new_errors_and_metrics(tmp_path):
    trace_id = "019f7d0c-d7a5-7967-bbd3-cbb6d899a501"
    log = tmp_path / "extension.log"
    log.write_text(
        "\n".join(
            [
                f"2026-07-20T13:30:00.000000 | INFO | access_recorder | 10.0.0.9 | 1 | {trace_id} | - | 0 | DS_KV_CLIENT_GET | 20298 | 1024",
                f"2026-07-20T13:30:00.001000 | WARN | worker | kvdataworker-0-worker2 | 1 | {trace_id} | [URMA_ELAPSED_DMA] cost 2500us, lane:2",
                f"2026-07-20T13:30:00.002000 | ERROR | worker | kvdataworker-0-worker2 | 1 | {trace_id} | DMA_WAIT_TIMEOUT lane 2",
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
        f"2026-07-20T13:31:00.000000 | ERROR | worker | kvdataworker-0-worker2 | 1 | {trace_id} | "
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
