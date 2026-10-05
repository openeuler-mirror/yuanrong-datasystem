"""Triage cohorts, diagnosis, source coverage and time buckets."""

from collections import Counter, defaultdict
from datetime import datetime

from ..ingest.triage import RPC_MAX_CONCURRENCY_ERROR
from .triage_stats import _percentiles


def _build_cohorts(trace_rows):
    cohorts = defaultdict(lambda: {
        "trace_ids": set(),
        "errors": Counter(),
        "classifications": Counter(),
        "access_latencies": [],
        "workers": Counter(),
    })
    for trace_id, trace in trace_rows.items():
        sources = trace.get("input_sources") or ["unknown"]
        for source in sources:
            source_stats = trace.get("source_stats", {}).get(source, {})
            cohort = cohorts[source]
            cohort["trace_ids"].add(trace_id)
            source_errors = source_stats.get("errors", {})
            cohort["errors"].update(source_errors)
            if source_errors:
                cohort["classifications"][trace.get("classification", "deadline_or_error")] += 1
            elif source_stats.get("access_latency_ms", {}).get("p50", 0) >= 20:
                cohort["classifications"]["slow_access"] += 1
            else:
                cohort["classifications"][trace.get("classification", "unknown")] += 1
            if source_stats.get("access_latency_ms", {}).get("p50") is not None:
                cohort["access_latencies"].append(source_stats["access_latency_ms"]["p50"])
            cohort["workers"].update(source_stats.get("workers", {}))
    rows = {}
    for source, cohort in sorted(cohorts.items()):
        rows[source] = {
            "trace_count": len(cohort["trace_ids"]),
            "errors": dict(cohort["errors"]),
            "classifications": dict(cohort["classifications"]),
            "access_latency_ms": _percentiles(cohort["access_latencies"]),
            "top_workers": {k: v for k, v in cohort["workers"].most_common(10)},
        }
    return rows


def _build_diagnosis(errors, classifications, access_latencies, coverage, cohorts):
    top_error, top_error_count = (errors.most_common(1)[0] if errors else ("none", 0))
    top_class, top_class_count = (classifications.most_common(1)[0] if classifications else ("unknown", 0))
    access = _percentiles(access_latencies)
    surfaces = coverage.get("surfaces", {})
    present = [name for name, item in surfaces.items() if item.get("status") == "present"]
    missing = [name for name, item in surfaces.items() if item.get("status") != "present"]
    cohort_count = len(cohorts)
    cohort_text = (
        f"输入被拆成 {cohort_count} 个 cohort，报告应先比较每个输入包的分布再合并判断。"
        if cohort_count > 1 else "当前只有一个输入 cohort，重点看该输入内部的 trace/error/worker 分布。"
    )
    return {
        "symptom_line": {
            "label": "错误线",
            "text": f"主要失败表象是 {top_error}（{top_error_count} 次），用于回答客户为什么看到失败。",
        },
        "latency_line": {
            "label": "慢时延线",
            "text": (
                f"access p50={access.get('p50', '')}ms、p99={access.get('p99', '')}ms、max={access.get('max', '')}ms；"
                "再结合 latencySummary、breakdown、RPC slow、URMA/UB edge 判断时间花在哪里。"
            ),
        },
        "evidence_boundary": {
            "label": "证据边界",
            "text": (
                f"已观测面：{', '.join(present) or 'none'}；缺失/未采样面：{', '.join(missing) or 'none'}。"
                "缺失项只能标为观测盲区，不能直接当根因。"
            ),
        },
        "customer_expression": {
            "label": "客户表达",
            "text": (
                f"建议描述为“{top_class} 是当前最大根因族（{top_class_count} 条 trace）”。"
                f"{cohort_text} 该判断来自日志聚合的 observed evidence，源码/CodeGraph 复核应另列。"
            ),
        },
    }


def _build_recommendations(classifications, coverage, cohorts, ub_summary):
    surfaces = coverage.get("surfaces", {})
    recommendations = [{
        "category": "source_validation",
        "title": "固定源码 ref 并用 CodeGraph/源码复核调用链",
        "detail": "报告中的根因族来自日志聚合，应继续用 main/master 对应 ref 验证 timeout 传递、EntryWorker、MetaWorker、DataWorker 和 UB/URMA 分支。",
    }]
    missing = [name for name, item in surfaces.items() if item.get("status") != "present"]
    if missing:
        recommendations.append({
            "category": "observability",
            "title": "补齐缺失观测面",
            "detail": f"当前缺失或未采样：{', '.join(missing)}。这些字段缺失时只能标为观测盲区，不能直接下根因结论。",
        })
    else:
        recommendations.append({
            "category": "observability",
            "title": "保持现有日志字段稳定输出",
            "detail": "client access、latencySummary、RPC slow、URMA elapsed、error 面均出现时，可继续扩大真实脱敏 fixture 做回归。",
        })
    if len(cohorts) > 1:
        recommendations.append({
            "category": "cohort_compare",
            "title": "多输入包按 cohort 对比后再合并结论",
            "detail": (
                "先分别比较每个输入包的 trace_count、errors、classifications、access latency "
                "和 top workers，再判断是否属于有无底噪差异或同源残留问题。"
            ),
        })
    if ub_summary.get("transfer_path") or ub_summary.get("edges"):
        recommendations.append({
            "category": "ub_urma",
            "title": "UB/URMA 按 write/wait/notify 时序继续定界",
            "detail": (
                "结合 transfer path、src->target edge、URMA total、poll JFC、notify、thread scheduling、"
                "dataSize、cpuid、inflight 判断，不要只凭 URMA total 单字段归因。"
            ),
        })
    if classifications.get("client_deadline_20ms") or classifications.get("client_deadline_with_urma_wait"):
        recommendations.append({
            "category": "deadline",
            "title": "拆开 client deadline 和 worker 后续完成阶段",
            "detail": (
                "20ms client timeout 是失败触发点；同 trace 的 worker access、RemotePull、"
                "BatchGetObjectRemote、URMA 日志用于判断服务端是否在 deadline 后继续完成。"
            ),
        })
    if classifications.get("rpc_max_concurrency"):
        recommendations.append({
            "category": "rpc_capacity",
            "title": "重点确认 RPC 线程数/并发不足",
            "detail": (
                "出现 error_code=2004 / max_concurrency 时，按时间桶和 worker 聚合观察是否集中在少数 worker；"
                "这通常意味着服务端 RPC 并发或线程池容量不足。"
            ),
        })
    return recommendations


def _build_source_appendix(coverage):
    rows = [
        {
            "scope": "通用",
            "log_surface": "access log",
            "flow_stage": "client -> entry worker",
            "source_hint": "ObjectClientImpl / ClientWorkerRemoteApi / Worker OC access path",
            "validation": (
                "Use CodeGraph on pinned main/master ref, then direct source reads for client "
                "timeout, status, and WorkerRpc propagation."
            ),
            "report_reading": (
                "Defines user-visible latency/status; use it as symptom line, not as standalone "
                "worker-side root cause."
            ),
        },
        {
            "scope": "写入",
            "log_surface": "latencySummary",
            "flow_stage": "client -> entry worker createbuffer / publish",
            "source_hint": "client summary emitters around Set/Create/Publish and buffer preparation",
            "validation": (
                "Use CodeGraph to map each summary key to current write path; preserve raw "
                "latencySummary text in evidence."
            ),
            "report_reading": (
                "Explains write-side stage contribution even when no standalone slow log crosses "
                "threshold."
            ),
        },
        {
            "scope": "读取",
            "log_surface": "GetObjMetaInfo / QueryMeta",
            "flow_stage": "entry worker -> meta worker",
            "source_hint": "ClientWorkerRemoteApi::GetObjMetaInfo / meta service query path",
            "validation": (
                "Use CodeGraph and direct source reads to verify timeout budget and meta RPC "
                "branch before attributing QueryMeta."
            ),
            "report_reading": (
                "Only call meta path slow when logs expose QueryMeta/GetObjMetaInfo cost; "
                "absence is an observation gap."
            ),
        },
        {
            "scope": "通用",
            "log_surface": "RPC slow",
            "flow_stage": "RPC framework client/server/network split",
            "source_hint": "brpc_perf_trace.h / rpc framework slow log emitters",
            "validation": (
                "Check method name and fields such as server_exec_us and network_residual_us "
                "against current transport code."
            ),
            "report_reading": "Separates server execution, queueing, framework, and residual/network windows.",
        },
        {
            "scope": "读取",
            "log_surface": "RemotePull / BatchGetObjectRemote",
            "flow_stage": "entry worker -> data worker",
            "source_hint": "WorkerRemoteWorkerOCApi / WorkerWorkerOCServiceImpl::BatchGetObjectRemote",
            "validation": "Use CodeGraph to verify current remote get branch, fallback, and aggregation behavior.",
            "report_reading": (
                "Explains worker-side completion after client deadline; compare with client "
                "access window."
            ),
        },
        {
            "scope": "读取",
            "log_surface": "URMA_ELAPSED_TOTAL",
            "flow_stage": "data worker UB write completion",
            "source_hint": "UrmaManager::WaitToFinish / LogUrmaWaitToFinishElapsed",
            "validation": (
                "Use CodeGraph plus source reads; compare total with wait_for, wakeSchedLatencyUs, "
                "srcChipInflight, request id, src/target, dataSize, cpuid, and inflight."
            ),
            "report_reading": (
                "Treat as post/write completion wait window; use wait/wake fields to split OS "
                "scheduling from completion cost."
            ),
        },
        {
            "scope": "读取",
            "log_surface": "URMA_ELAPSED_POLL_JFC / NOTIFY / THREAD_SHED",
            "flow_stage": "data worker UB poll and wake scheduling",
            "source_hint": "UrmaManager::PollJfcWait / ds_urma_poll_jfc / ds_urma_wait_jfc / nanosleep",
            "validation": (
                "Use CodeGraph plus source reads; split poll_jfc cost, notify wakeup, poll-loop "
                "gap, and nanosleep(1us) wake cost."
            ),
            "report_reading": (
                "When total is high, these fields indicate whether the delay sits in polling, "
                "notification, or poll-thread scheduling."
            ),
        },
        {
            "scope": "写入",
            "log_surface": "Publish / CreateBuffer",
            "flow_stage": "entry worker -> meta worker publish",
            "source_hint": "CreateBuffer/Publish client APIs and meta worker publish path",
            "validation": (
                "Use CodeGraph to separate createbuffer, client publish, entry worker publish, "
                "and meta worker publish; verify each stage against latencySummary or slow logs."
            ),
            "report_reading": (
                "For write traces, keep createbuffer and publish as separate phases instead of "
                "merging all write latency."
            ),
        },
    ]
    missing = [name for name, item in coverage.get("surfaces", {}).items() if item.get("status") != "present"]
    if missing:
        rows.append({
            "scope": "通用",
            "log_surface": "missing evidence",
            "flow_stage": "observability boundary",
            "source_hint": ", ".join(missing),
            "validation": "Add or recover the missing logs before turning absence into a root-cause claim.",
            "report_reading": "Mark as observation gap in customer reports.",
        })
    return rows


def _build_time_buckets(trace_rows, bucket_ms):
    buckets = defaultdict(lambda: {
        "trace_ids": set(),
        "error_count": 0,
        "max_concurrency_error_count": 0,
        "slow_count": 0,
        "access_latencies": [],
        "stage_latencies": defaultdict(list),
        "top_workers": Counter(),
    })
    for trace_id, trace in trace_rows.items():
        first_ts = trace.get("first_ts")
        if not first_ts:
            continue
        dt = datetime.fromisoformat(first_ts)
        epoch_ms = int(dt.timestamp() * 1000)
        start_ms = epoch_ms - (epoch_ms % bucket_ms)
        bucket = buckets[start_ms]
        bucket["trace_ids"].add(trace_id)
        errors = trace.get("errors", {})
        bucket["error_count"] += sum(errors.values())
        bucket["max_concurrency_error_count"] += errors.get(RPC_MAX_CONCURRENCY_ERROR, 0)
        if trace.get("classification") not in ("unknown", "access_latency_only"):
            bucket["slow_count"] += 1
        if trace.get("access_latency_ms", {}).get("p50") is not None:
            bucket["access_latencies"].append(trace["access_latency_ms"]["p50"])
        for stage in trace.get("stage_breakdown", []):
            duration = stage.get("duration_ms")
            if duration is None or stage.get("confidence") == "missing":
                continue
            bucket["stage_latencies"][stage["stage"]].append(duration)
        for worker in trace.get("workers", {}):
            bucket["top_workers"][worker] += 1
    rows = []
    for start_ms, bucket in sorted(buckets.items()):
        rows.append({
            "bucket_start": datetime.fromtimestamp(start_ms / 1000.0).isoformat(),
            "bucket_ms": bucket_ms,
            "trace_count": len(bucket["trace_ids"]),
            "error_count": bucket["error_count"],
            "max_concurrency_error_count": bucket["max_concurrency_error_count"],
            "slow_count": bucket["slow_count"],
            "p50_access_ms": _percentiles(bucket["access_latencies"]).get("p50"),
            "p99_access_ms": _percentiles(bucket["access_latencies"]).get("p99"),
            "stage_breakdown_ms": {
                stage: _percentiles(values)
                for stage, values in sorted(bucket["stage_latencies"].items())
            },
            "burst_score": max(bucket["slow_count"], bucket["error_count"], 1),
            "gap_score": 0,
            "top_workers": [w for w, _ in bucket["top_workers"].most_common(3)],
        })
    return rows
