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


def test_cli_self_test_writes_json_and_markdown(tmp_path, capsys):
    mod = _load_module()
    out_json = tmp_path / "summary.json"
    out_md = tmp_path / "summary.md"

    rc = mod.main(["--self-test", "--output-json", str(out_json), "--output-md", str(out_md)])

    assert rc == 0
    assert json.loads(out_json.read_text())["self_test"] is True
    assert "Trace Triage Summary" in out_md.read_text()
    assert "self-test passed" in capsys.readouterr().out
