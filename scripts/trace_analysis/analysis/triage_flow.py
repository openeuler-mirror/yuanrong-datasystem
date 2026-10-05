"""Observed read and write flow-stage projections."""

from collections import Counter

from ..ingest.triage import IP_RE
from .triage_stats import _percentiles


def _ips_from_text(text):
    return [match.group(0) for match in IP_RE.finditer(text or "")]


def _flow_stage_rollup(trace_rows, stage_names):
    values = []
    trace_ids = []
    workers = Counter()
    ips = Counter()
    top_trace = None
    top_value = None
    for trace_id, trace in trace_rows.items():
        stage_values = [
            stage.get("duration_ms") for stage in trace.get("stage_breakdown", [])
            if stage.get("stage") in stage_names and stage.get("duration_ms") is not None
        ]
        if not stage_values:
            continue
        value = max(stage_values)
        values.append(value)
        trace_ids.append(trace_id)
        if top_value is None or value > top_value:
            top_value = value
            top_trace = trace_id
        workers.update(trace.get("workers", {}))
        for event in trace.get("ub_events", []):
            if event.get("src_addr"):
                ips[event["src_addr"]] += 1
            if event.get("target_addr"):
                ips[event["target_addr"]] += 1
            ips.update(_ips_from_text(event.get("raw", "")))
        for evidence in trace.get("evidence", []):
            ips.update(_ips_from_text(evidence.get("text", "")))
    pct = _percentiles(values)
    return {
        "trace_count": len(set(trace_ids)),
        "p50_ms": pct.get("p50"),
        "p99_ms": pct.get("p99"),
        "max_ms": pct.get("max"),
        "top_trace": top_trace,
        "top_workers": [worker for worker, _ in workers.most_common(3)],
        "top_ips": [ip for ip, _ in ips.most_common(4)],
    }


def _flow_edge_summary(rollup, fallback):
    if not rollup.get("trace_count"):
        return fallback
    parts = [f"p99={rollup.get('p99_ms', '')}ms", f"max={rollup.get('max_ms', '')}ms"]
    if rollup.get("top_workers"):
        parts.append("worker " + ", ".join(rollup["top_workers"][:2]))
    if rollup.get("top_ips"):
        parts.append("IP " + ", ".join(rollup["top_ips"][:2]))
    return " / ".join(parts)


def _flow_candidate_edges(read_count, write_count, ub_summary):
    observed_transfer_paths = sorted(ub_summary.get("transfer_path", {}).keys())
    return [
        {
            "name": "client -> entry worker",
            "operation": "Client→Entry RPC/UB",
            "source": "client",
            "target": "entry",
            "transports": ["RPC", "UB"],
            "status": "observed" if read_count or write_count else "not_observed",
            "observed_transfer_paths": observed_transfer_paths,
            "report_reading": "当前报告把 Client→Entry 作为入口窗口；传输可能随实现演进从 RPC/TCP/SHM 扩展到 UB。",
        },
        {
            "name": "client -> data worker",
            "operation": "Client→Data UB",
            "source": "client",
            "target": "data",
            "transports": ["UB"],
            "status": "future_candidate",
            "observed_transfer_paths": [],
            "report_reading": "后续若客户端可直接访问 DataWorker，需要解析 client-side UB source/target 并独立成边。",
        },
        {
            "name": "client -> meta worker",
            "operation": "Client→Meta Direct",
            "source": "client",
            "target": "meta",
            "transports": ["RPC", "UB"],
            "status": "future_candidate",
            "observed_transfer_paths": [],
            "report_reading": "后续若客户端可直接访问 MetaWorker，需要从 method、dst/src 和 access path 识别直连元数据边。",
        },
    ]


def _build_flow_stages(coverage, flow_counts, ub_summary, trace_rows=None):
    trace_rows = trace_rows or {}
    surfaces = coverage.get("surfaces", {})

    def surface_status(name):
        item = surfaces.get(name, {})
        return {
            "status": item.get("status", "missing"),
            "events": item.get("events", 0),
        }

    read_count = sum(count for name, count in flow_counts.items() if "GET" in name)
    write_count = sum(count for name, count in flow_counts.items()
                      if any(op in name for op in ("SET", "CREATE", "PUBLISH")))
    ub_edges = ub_summary.get("edges", {})
    ub_transfer_count = ub_summary.get("transfer_path", {}).get("UB", 0)
    candidate_edges = _flow_candidate_edges(read_count, write_count, ub_summary)
    rollups = {
        "read_client_entry": _flow_stage_rollup(trace_rows, {"read.client_to_entry_worker"}),
        "write_client_createbuffer": _flow_stage_rollup(trace_rows, {"write.client_to_entry_createbuffer"}),
        "write_client_publish": _flow_stage_rollup(trace_rows, {"write.client_to_entry_publish"}),
        "write_client_entry": _flow_stage_rollup(trace_rows, {
            "write.client_to_entry_createbuffer",
            "write.client_to_entry_publish",
        }),
        "read_entry_meta": _flow_stage_rollup(trace_rows, {"read.entry_to_meta_worker"}),
        "write_entry_meta": _flow_stage_rollup(trace_rows, {"write.entry_to_meta_publish"}),
        "client_entry": _flow_stage_rollup(trace_rows, {
            "read.client_to_entry_worker",
            "write.client_to_entry_createbuffer",
            "write.client_to_entry_publish",
        }),
        "entry_meta": _flow_stage_rollup(trace_rows, {
            "read.entry_to_meta_worker",
            "write.entry_to_meta_publish",
        }),
        "entry_data": _flow_stage_rollup(trace_rows, {"read.entry_to_data_worker"}),
        "data_ub": _flow_stage_rollup(trace_rows, {"read.data_worker_ub_write"}),
    }
    nodes = [
        {
            "id": "client",
            "label": "Client",
            "role": "client",
            "top_ips": rollups["client_entry"].get("top_ips", [])[:2],
        },
        {
            "id": "entry",
            "label": "Entry Worker",
            "role": "entry_worker",
            "top_workers": rollups["client_entry"].get("top_workers", [])[:2],
            "top_ips": rollups["entry_data"].get("top_ips", [])[:2],
        },
        {
            "id": "meta",
            "label": "Meta Worker",
            "role": "meta_worker",
            "top_ips": rollups["entry_meta"].get("top_ips", [])[:2],
        },
        {
            "id": "data",
            "label": "Data Worker",
            "role": "data_worker",
            "top_workers": rollups["data_ub"].get("top_workers", [])[:2],
            "top_ips": rollups["data_ub"].get("top_ips", [])[:2],
        },
    ]
    read_edges = [
        {
            "name": "read: client -> entry worker",
            "source": "client",
            "target": "entry",
            "operation": "Client→Entry RPC/UB",
            "evidence": f"{surface_status('client_access')['events']} access events, {read_count} read flows",
            "status": surface_status("client_access")["status"],
            "summary": _flow_edge_summary(rollups["read_client_entry"], "client read access upper bound"),
            "rollup": rollups["read_client_entry"],
            "reason": "客户侧端到端窗口，作为上界，不和内部 RPC/UB 子阶段相加。",
            "report_reading": "客户看到的耗时和错误起点，先用于表象定界。",
        },
        {
            "name": "read: entry worker -> meta worker",
            "source": "entry",
            "target": "meta",
            "operation": "Entry→Meta RPC",
            "evidence": f"{surface_status('latency_summary')['events']} latencySummary events",
            "status": surface_status("latency_summary")["status"],
            "summary": _flow_edge_summary(rollups["read_entry_meta"], "QueryMeta evidence"),
            "rollup": rollups["read_entry_meta"],
            "reason": "元数据 RPC 阶段；若 p99/max 高，优先复核 QueryMeta/CreateMeta slow log 和 MetaWorker。",
            "report_reading": "只有出现 QueryMeta/GetObjMetaInfo 耗时或 RPC slow 时才归因到元数据路径。",
        },
        {
            "name": "read: entry worker -> data worker",
            "source": "entry",
            "target": "data",
            "operation": "Entry→Data RPC",
            "evidence": f"{len(ub_edges)} UB edge buckets, {ub_transfer_count} UB transfer markers",
            "status": "present" if ub_edges or ub_transfer_count else "missing",
            "summary": _flow_edge_summary(rollups["entry_data"], "RemotePull/BatchGetObjectRemote evidence"),
            "rollup": rollups["entry_data"],
            "reason": "EntryWorker 等待 DataWorker 远端数据；常用于解释 client deadline 后服务端仍继续完成。",
            "report_reading": "读取主路径的远端数据获取窗口，用来解释 client deadline 后 worker 继续完成。",
        },
        {
            "name": "read: data worker -> entry worker UB write",
            "source": "data",
            "target": "entry",
            "operation": "URMA Write",
            "evidence": f"{surface_status('urma_elapsed')['events']} URMA elapsed events",
            "status": surface_status("urma_elapsed")["status"],
            "summary": _flow_edge_summary(rollups["data_ub"], "URMA elapsed evidence"),
            "rollup": rollups["data_ub"],
            "reason": (
                "DataWorker 通过 URMA Write 反向写回 EntryWorker；看 total、request id、"
                "src/target、dataSize、cpuid 和 inflight。"
            ),
            "report_reading": "读取路径的数据回传阶段，方向是 DataWorker -> EntryWorker，不是 EntryWorker -> DataWorker。",
        },
    ]
    write_edges = [
        {
            "name": "write: client -> entry worker createbuffer",
            "source": "client",
            "target": "entry",
            "operation": "CreateBuffer",
            "evidence": (
                f"{write_count} write flows, "
                f"{surface_status('latency_summary')['events']} latencySummary events"
            ),
            "status": (
                "present"
                if write_count or surface_status("latency_summary")["status"] == "present"
                else "missing"
            ),
            "summary": _flow_edge_summary(
                rollups["write_client_createbuffer"], "CreateBuffer client RPC evidence"
            ),
            "rollup": rollups["write_client_createbuffer"],
            "reason": "写路径客户侧 createbuffer/publish 请求窗口，需要和 Entry→Meta publish 区分。",
            "report_reading": "写流程先拆 client createbuffer/publish，再看 entry/meta 发布。",
        },
        {
            "name": "write: client -> entry worker publish",
            "source": "client",
            "target": "entry",
            "operation": "Client Publish",
            "evidence": (
                f"{write_count} write flows, "
                f"{surface_status('latency_summary')['events']} latencySummary events"
            ),
            "status": (
                "present"
                if write_count or surface_status("latency_summary")["status"] == "present"
                else "missing"
            ),
            "summary": _flow_edge_summary(rollups["write_client_publish"], "client publish evidence"),
            "rollup": rollups["write_client_publish"],
            "reason": "写路径 publish 从 Client 到 EntryWorker；慢时延需和本地 memory copy 及 meta publish 分开看。",
            "report_reading": "client publish 是写路径入口，不代表 UB 读传输。",
        },
        {
            "name": "write: entry worker -> meta worker publish",
            "source": "entry",
            "target": "meta",
            "operation": "Entry→Meta Publish",
            "evidence": (
                f"{write_count} write flows, "
                f"{surface_status('latency_summary')['events']} latencySummary events"
            ),
            "status": (
                "present"
                if write_count or surface_status("latency_summary")["status"] == "present"
                else "missing"
            ),
            "summary": _flow_edge_summary(rollups["write_entry_meta"], "publish metadata evidence"),
            "rollup": rollups["write_entry_meta"],
            "reason": "写路径元数据发布阶段；需要和 createbuffer/client publish 区分。",
            "report_reading": "写流程需要拆开 createbuffer、client publish、entry publish、meta publish。",
        },
    ]
    compat_edges = []
    for edge in read_edges + write_edges:
        old_edge = dict(edge)
        old_edge["name"] = old_edge["name"].replace("read: ", "").replace("write: ", "")
        compat_edges.append(old_edge)
    return {
        "nodes": nodes,
        "edges": compat_edges,
        "candidate_edges": candidate_edges,
        "read": {"nodes": nodes, "edges": read_edges},
        "write": {"nodes": nodes[:3], "edges": write_edges},
    }
