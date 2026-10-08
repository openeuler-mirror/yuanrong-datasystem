#!/usr/bin/env python3
"""Post-process ds-trace-triage and bottleneck outputs for NUMA/chip analysis."""

from __future__ import annotations

import argparse
import json
import re
import tarfile
from collections import Counter
from pathlib import Path
from typing import Any
import sys

from .resources import asset_path, echarts_path
from .archive import archive_digest, collection_cohort
from .evidence.errors import observe_error_evidence
from .evidence.normalized import load_evidence, summary_digest


def _cohort_from_member(name: str) -> str | None:
    return collection_cohort(name)


def _cohorts_from_members(members) -> tuple[dict[str, set[str]], int]:
    cohorts: dict[str, set[str]] = {}
    member_count = 0
    for name in members:
        cohort = _cohort_from_member(name)
        if cohort is None:
            continue
        member_count += 1
        trace_id = re.sub(r"_\d+$", "", Path(name).name)
        cohorts.setdefault(trace_id, set()).add(cohort)
    return cohorts, member_count


def _scan_archive_cohorts(archive_path: Path) -> tuple[dict[str, set[str]], int]:
    with tarfile.open(archive_path, "r:*") as archive:
        return _cohorts_from_members(member.name for member in archive if member.isfile())


def _cohorts_from_inventory(manifest: dict, archive_path: Path) -> tuple[dict[str, set[str]], int] | None:
    inputs = manifest.get("inputs")
    if not isinstance(inputs, list):
        return None
    try:
        size = archive_path.stat().st_size
    except OSError:
        return None
    candidates = []
    for item in inputs:
        if not isinstance(item, dict):
            continue
        expected_size = item.get("size")
        expected_digest = item.get("sha256")
        members = item.get("members")
        size_matches = type(expected_size) is int and expected_size == size
        members_valid = isinstance(members, list) and all(isinstance(member, str) for member in members)
        if size_matches and isinstance(expected_digest, str) and members_valid:
            candidates.append((expected_digest, members))
    if not candidates:
        return None
    digest = archive_digest(archive_path)
    for expected_digest, members in candidates:
        if expected_digest == digest:
            return _cohorts_from_members(members)
    return None


def build_cohort_index(archive_path: Path) -> dict[str, set[str]]:
    """Map a Trace ID to every collection cohort containing it."""
    return _scan_archive_cohorts(archive_path)[0]


def count_archive_trace_files(archive_path: Path) -> int:
    with tarfile.open(archive_path, "r:*") as archive:
        return sum(member.isfile() and _cohort_from_member(member.name) is not None for member in archive)


def _metric_max(value: Any) -> float | None:
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, dict):
        maximum = value.get("max")
        return float(maximum) if isinstance(maximum, (int, float)) else None
    return None


def _client_ms(trace: dict, row: dict | None) -> float | None:
    if row is not None and isinstance(row.get("client_ms"), (int, float)):
        return float(row["client_ms"])
    client = (trace.get("access_latency_ms_by_role") or {}).get("client")
    return _metric_max(client)


def _operation(trace: dict) -> str:
    flows = trace.get("flows") or {}
    for name in flows:
        upper = str(name).upper()
        if "GET" in upper:
            return "GET"
        if any(token in upper for token in ("PUT", "SET", "CREATE", "PUBLISH")):
            return "PUT"
    return "未明确"


def _status(trace: dict, row: dict | None) -> int | None:
    if row is not None and isinstance(row.get("status"), int):
        return row["status"]
    codes = []
    for value in (trace.get("access_statuses") or {}):
        try:
            codes.append(int(value))
        except (TypeError, ValueError):
            continue
    for key in (trace.get("errors") or {}):
        match = re.fullmatch(r"status=(\d+)", str(key))
        if match:
            codes.append(int(match.group(1)))
    nonzero = [code for code in codes if code]
    return nonzero[0] if nonzero else (0 if codes else None)


def _evidence_raw(trace: dict, row: dict | None) -> list[str]:
    result: list[str] = []
    for item in trace.get("evidence") or []:
        raw = (item.get("raw") or item.get("text")) if isinstance(item, dict) else item
        if raw and str(raw) not in result:
            result.append(str(raw))
    if row:
        for item in row.get("evidence") or []:
            raw = item.get("raw") if isinstance(item, dict) else item
            if raw and str(raw) not in result:
                result.append(str(raw))
    return result


def _chip_values(event: dict) -> tuple[int, int] | None:
    values = event.get("src_chip_inflight")
    if isinstance(values, str):
        values = {
            match.group(1): int(match.group(2))
            for match in re.finditer(r"(\d+)\s*:\s*(\d+)", values)
        }
    if not isinstance(values, dict):
        return None
    chip1 = values.get("1", values.get(1, 0))
    chip2 = values.get("2", values.get(2, 0))
    if not isinstance(chip1, (int, float)) or not isinstance(chip2, (int, float)):
        return None
    return int(chip1), int(chip2)


def _rows_from_evidence(summary: dict, evidence: dict) -> dict:
    traces = summary.get("traces") or {}
    entries = evidence.get("traces")
    if not isinstance(entries, dict) or set(entries) != set(traces):
        raise ValueError("NUMA Evidence Trace IDs differ from Triage summary")
    rows = []
    for trace_id, trace in traces.items():
        entry = entries[trace_id]
        read = entry.get("read") if isinstance(entry, dict) else None
        if not isinstance(read, dict):
            raise ValueError(f"NUMA Evidence lacks read observations: {trace_id}")
        client_us = read.get("client_us")
        observed_status = read.get("status")
        source_status = _status(trace, None)
        has_client_observation = (isinstance(client_us, (int, float))
                                  and not isinstance(client_us, bool) and client_us > 0)
        status = source_status
        if status is None and (has_client_observation or "write" in entry):
            if type(observed_status) is int:
                status = observed_status
        rows.append({
            "trace_id": trace_id,
            "client_ms": client_us / 1000 if has_client_observation else None,
            "status": status,
            "transport": read.get("transport") if read.get("transport") != "未知" else None,
            "direct_data_worker": read.get("direct_data_worker"),
            "failed": status not in {None, 0},
        })
    return {"traces": rows}


def _observation_class(status, chip_mode, slow_wr_count):
    if slow_wr_count:
        return "慢WR观测"
    if status not in {None, 0}:
        return "错误状态"
    if chip_mode != "未观测":
        return "chip inflight观测"
    return "未观测到WR/chip"


def build_trace_records(
    summary: dict, trace_context: dict, cohorts: dict[str, set[str]],
    classification_basis: str = "read_diagnosis",
) -> list[dict]:
    """Build one record per unique Trace while preserving multi-cohort membership."""
    summary_traces = summary.get("traces") or {}
    input_rows = [*(trace_context.get("traces") or []), *(trace_context.get("write_traces") or [])]
    rows = {row["trace_id"]: row for row in input_rows}
    trace_ids = sorted(set(summary_traces) | set(rows) | set(cohorts))
    records: list[dict] = []
    for trace_id in trace_ids:
        trace = summary_traces.get(trace_id) or {}
        row = rows.get(trace_id)
        events = []
        event_keys = set()
        for event in trace.get("ub_events") or []:
            if event.get("event_type") not in {"total", "urma_total"} or not isinstance(
                event.get("cost_ms"), (int, float)
            ):
                continue
            key = event.get("raw") or (
                event.get("request_id"), event.get("timestamp"), event.get("cost_ms"), event.get("worker")
            )
            if key in event_keys:
                continue
            event_keys.add(key)
            events.append(event)
        chip_pairs = []
        for event in events:
            pair = _chip_values(event)
            if pair is not None:
                chip_pairs.append(pair)
        chip1_total = sum(pair[0] for pair in chip_pairs)
        chip2_total = sum(pair[1] for pair in chip_pairs)
        chip1_peak = max((pair[0] for pair in chip_pairs), default=None)
        chip2_peak = max((pair[1] for pair in chip_pairs), default=None)
        if not chip_pairs:
            chip_mode = "未观测"
            chip_skew = None
        elif chip1_total > 0 and chip2_total > 0:
            chip_mode = "双 chip"
            chip_skew = abs(chip1_total - chip2_total) / (chip1_total + chip2_total)
        elif chip1_total > 0:
            chip_mode = "仅 chip 1"
            chip_skew = 1.0
        elif chip2_total > 0:
            chip_mode = "仅 chip 2"
            chip_skew = 1.0
        else:
            chip_mode = "已观测但无 inflight"
            chip_skew = None
        requests = [
            {
                "request_id": event.get("request_id"),
                "timestamp": event.get("timestamp"),
                "worker": event.get("worker"),
                "total_ms": float(event["cost_ms"]),
                "is_slow": float(event["cost_ms"]) > 1.5,
                "src_chip_inflight": event.get("src_chip_inflight"),
                "urma_inflight_wr_count": event.get("urma_inflight_wr_count"),
                "raw": event.get("raw"),
            }
            for event in events
        ]
        row_worker = row.get("direct_data_worker") if row else None
        if row_worker in {None, "", "未明确", "unknown"}:
            row_worker = None
        worker = row_worker or next((request["worker"] for request in requests if request["worker"]), None)
        if not worker:
            trace_workers = [name for name in (trace.get("workers") or {}) if name not in {"unknown", "未明确"}]
            worker = next((name for name in trace_workers if "worker-0-worker" in name), None)
            worker = worker or (trace_workers[0] if trace_workers else None)
        evidence = _evidence_raw(trace, row)
        error_observations = observe_error_evidence(evidence)
        status = _status(trace, row)
        slow_wr_count = sum(request["is_slow"] for request in requests)
        if classification_basis == "numa_observation":
            observation_class = _observation_class(status, chip_mode, slow_wr_count)
        else:
            observation_class = row.get("primary_problem") if row and row.get("primary_problem") else "未分类"
        records.append(
            {
                "trace_id": trace_id,
                "cohorts": sorted(cohorts.get(trace_id, set())),
                "operation": _operation(trace),
                "status": status,
                "client_ms": _client_ms(trace, row),
                "primary_problem": row.get("primary_problem") if row else None,
                "transport": row.get("transport") if row else None,
                "worker": worker,
                "timestamp": trace.get("first_ts")
                or next((request["timestamp"] for request in requests if request["timestamp"]), None),
                "failed": bool(row.get("failed")) if row else (status not in {None, 0}),
                "chip_mode": chip_mode,
                "chip1_total": chip1_total if chip_pairs else None,
                "chip2_total": chip2_total if chip_pairs else None,
                "chip1_peak": chip1_peak,
                "chip2_peak": chip2_peak,
                "chip_skew": chip_skew,
                "urma_requests": requests,
                "slow_wr_count": slow_wr_count,
                "observation_class": observation_class,
                "timeout_elapsed_ms": error_observations["timeout_elapsed_ms"],
                "error_observations": error_observations,
                "evidence": evidence,
            }
        )
    return records


def build_aggregate(records: list[dict], source: dict) -> dict:
    return {
        "unique_trace_count": len(records),
        "chip_mode_counts": dict(Counter(record["chip_mode"] for record in records)),
        "operation_counts": dict(Counter(record["operation"] for record in records)),
        "status_counts": dict(Counter(str(record["status"]) for record in records)),
        "source": source,
    }


def classify_error_chain(record: dict) -> dict:
    """Classify only evidence-backed 1004/1010 chains."""
    observations = record.get("error_observations") or observe_error_evidence(record.get("evidence") or [])
    operation = record.get("operation")
    status = record.get("status")
    has_timeout = observations["urma_timeout"]
    has_response_shape = observations["response_shape"]
    has_receive_buffer_failure = observations["receive_buffer_failure"]
    has_arena_oom = observations["arena_oom"]
    is_failed_get = operation == "GET" and status == 1004
    if is_failed_get and has_receive_buffer_failure and has_arena_oom:
        return {
            "family": "GET UB接收缓冲分配失败1004",
            "closed": True,
            "signals": ["Client UB接收缓冲准备失败", "Client arena内存不足", "Client状态1004"],
            "missing": [],
        }
    signals: list[str] = []
    if has_timeout:
        signals.append("URMA等待超时")
    if has_response_shape:
        signals.append("UB响应形态异常")
    if status == 1004:
        signals.append("Client状态1004")
    if status == 1010:
        signals.append("Client状态1010")

    if operation == "GET" and status == 1004:
        required = [("URMA等待超时", has_timeout), ("UB响应形态异常", has_response_shape)]
        closed = all(present for _, present in required)
        return {
            "family": "GET URMA超时后上浮1004" if closed else "GET URMA错误1004（链路未闭合）",
            "closed": closed,
            "signals": signals,
            "missing": [name for name, present in required if not present],
        }
    if operation == "PUT" and status == 1010:
        closed = has_timeout and observations["write_operation"]
        return {
            "family": "PUT URMA WRITE等待超时1010" if closed else "PUT URMA超时1010（链路未闭合）",
            "closed": closed,
            "signals": signals,
            "missing": [] if closed else ["URMA WRITE等待超时"],
        }
    if status not in {None, 0}:
        return {
            "family": f"其他状态{status}",
            "closed": False,
            "signals": signals,
            "missing": ["可证明的完整错误链"],
        }
    return {"family": "成功", "closed": True, "signals": signals, "missing": []}


def summarize_latency_bands(records: list[dict]) -> list[dict]:
    cohorts = set()
    for record in records:
        for cohort in record.get("cohorts", []):
            if cohort.startswith("time/"):
                cohorts.add(cohort)
    result = []
    for cohort in sorted(cohorts):
        members = [record for record in records if cohort in record.get("cohorts", [])]
        latencies = sorted(
            float(record["client_ms"])
            for record in members
            if isinstance(record.get("client_ms"), (int, float))
        )
        result.append(
            {
                "cohort": cohort,
                "unique_trace_count": len(members),
                "operation_counts": dict(Counter(record["operation"] for record in members)),
                "problem_counts": dict(
                    Counter(record["observation_class"] for record in members)
                ),
                "chip_mode_counts": dict(Counter(record["chip_mode"] for record in members)),
                "client_min_ms": latencies[0] if latencies else None,
                "client_max_ms": latencies[-1] if latencies else None,
                "slow_wr_count": sum(record["slow_wr_count"] for record in members),
            }
        )
    return result


def summarize_time_buckets(records: list[dict]) -> list[dict]:
    buckets: dict[str, dict] = {}
    for record in records:
        timestamp = record.get("timestamp")
        if not timestamp:
            continue
        second = str(timestamp)[:19]
        bucket = buckets.setdefault(
            second,
            {"second": second, "trace_count": 0, "error_count": 0, "slow_wr_count": 0, "dual_chip_count": 0},
        )
        bucket["trace_count"] += 1
        bucket["error_count"] += record.get("status") not in {None, 0}
        bucket["slow_wr_count"] += record.get("slow_wr_count") or 0
        bucket["dual_chip_count"] += record.get("chip_mode") == "双 chip"
    return [bucket for _, bucket in sorted(buckets.items())]


def build_insights(records: list[dict], latency_bands: list[dict], time_buckets: list[dict],
                   classification_basis: str = "read_diagnosis") -> list[dict]:
    observed = [record for record in records if record.get("chip_mode") != "未观测"]
    dual = [record for record in observed if record.get("chip_mode") == "双 chip"]
    insights = [
        {
            "title": "多 chip 并发已观测",
            "text": (
                f"{len(observed)} 条 Trace 有 srcChipInflight 证据，其中 {len(dual)} 条同时出现 chip 1/2"
                f"（{100 * len(dual) / len(observed):.1f}%）。该字段是发送侧完成时刻的全局并发快照，"
                "可证明两颗 source chip 存在并发 WR；不能证明当前 WR 选中了哪颗 chip、"
                "不能证明队列均衡 override 已执行，也不能单独换算带宽收益。"
                if observed
                else "本批 Trace 未观测到 srcChipInflight，不能判断多 chip 并发或队列均衡是否工作。"
            ),
        }
    ]
    short_bands = [band for band in latency_bands if "5000_7000" in band["cohort"] or "7000_10000" in band["cohort"]]
    short_count = sum(band["unique_trace_count"] for band in short_bands)
    short_problems = Counter()
    for band in short_bands:
        short_problems.update(band["problem_counts"])
    if short_count and short_problems:
        problem, count = short_problems.most_common(1)[0]
        classification = "主要观察" if classification_basis == "numa_observation" else "主导分类"
        short_text = (
            f"5–10ms 两档共 {short_count} 条 cohort 成员，{classification}为“{problem}” "
            f"{count} 条（{100 * count / short_count:.1f}%）。"
        )
    else:
        short_text = ("本批数据没有可用于 5–10ms 汇总的 cohort，短时延档观察未建立。"
                      if classification_basis == "numa_observation" else
                      "本批数据没有可用于 5–10ms 汇总的 cohort，短时延档主瓶颈未观测。")
    title = "短时延档主要观察" if classification_basis == "numa_observation" else "短时延档主瓶颈"
    insights.append({"title": title, "text": short_text})

    status_1004 = [record for record in records if record.get("status") == 1004]
    error_families = Counter(
        (record.get("error_chain") or {}).get("family", "未分类") for record in status_1004
    )
    family_text = "、".join(f"{name}={count}" for name, count in error_families.most_common()) or "未观测"
    timeout_1004 = [record for record in status_1004 if record.get("timeout_elapsed_ms") is not None]
    insights.append(
        {
            "title": "GET 1004错误链",
            "text": (
                f"状态 1004 共 {len(status_1004)} 条：{family_text}。"
                f"其中 {len(timeout_1004)} 条保留可量化的 URMA timeout elapsedMs；"
                "接收缓冲分配失败属于 Client 内存准备问题，不是已完成 WR 变慢。"
            ),
        }
    )
    peak = max(time_buckets, key=lambda item: item["error_count"], default=None)
    insights.append(
        {
            "title": "错误时间集中度",
            "text": (
                f"错误峰值位于 {peak['second']}，该秒 {peak['error_count']} 条错误、{peak['trace_count']} 条 Trace。"
                "时间集中支持短时突发/并发拥塞假设，但仅凭 Trace 不能把它定性为 incast。"
                if peak and peak["error_count"]
                else "未观测到可定位到秒级时间戳的错误。"
            ),
        }
    )
    return insights


def build_source_chain(source: dict) -> list[dict]:
    head = source.get("head") or "unknown"
    chain = [
        {
            "stage": "arena配置",
            "source": "src/datasystem/common/rdma/urma_manager.cpp:SetClientTransportArenaConfig",
            "judgment": "读取 DATASYSTEM_UB_TRANSPORT_ARENA_NUM 并设置 ub_transport_arena_num",
            "source_ref": head,
        },
        {
            "stage": "NUMA内存绑定",
            "source": "src/datasystem/common/rdma/urma_manager.cpp:BindClientTransportMemory",
            "judgment": "等大 arena range 按已发现 NUMA node 轮转 mbind",
            "source_ref": head,
        },
        {
            "stage": "多arena分配",
            "source": "src/datasystem/common/shared_memory/arena.cpp:ArenaManager::CreateArenaGroup",
            "judgment": "UB_TRANSPORT pool 拆为多个 arena，ArenaGroup 轮转选择 arena",
            "source_ref": head,
        },
        {
            "stage": "chip信息传播",
            "source": "src/datasystem/client/object_cache/transport/data_plane/ub_transporter.cpp:NumaIdToChipId",
            "judgment": "接收 buffer NUMA id 转为传输描述中的 chip id",
            "source_ref": head,
        },
        {
            "stage": "发送端chip选择",
            "source": "src/datasystem/common/rdma/urma_manager.cpp:GetAffinitySrcChipIdForPost",
            "judgment": "发送端按 transmitted chip 与 NUMA affinity 策略选择 source chip",
            "source_ref": head,
        },
        {
            "stage": "Trace观测",
            "source": "src/datasystem/common/rdma/urma_manager.cpp:GetSrcChipInflightWrCountsString",
            "judgment": "URMA_ELAPSED_TOTAL 输出完成时刻的全局 srcChipInflight 快照；不等于当前 WR 的选片决策",
            "source_ref": head,
        },
    ]
    if source.get("pr") == 2095:
        chain.insert(
            -1,
            {
                "stage": "inflight均衡",
                "source": "src/datasystem/common/rdma/urma_manager.cpp:GetAffinitySrcChipId",
                "judgment": "当前源码仅在 chip1/chip2 inflight 差值严格大于阈值时覆盖 RR 候选；Trace 快照不是选择时刻",
                "source_ref": head,
            },
        )
    return chain


def normalize_runtime_config(runtime_config: dict | None) -> dict:
    config = runtime_config or {}
    qps_per_node = config.get("qps_per_node")
    client_count = config.get("client_count")
    threads_per_client = config.get("threads_per_client")
    workers_per_node = config.get("workers_per_node")
    return {
        "qps_per_node": qps_per_node,
        "client_count": client_count,
        "threads_per_client": threads_per_client,
        "workers_per_node": workers_per_node,
        "qps_per_client": (
            qps_per_node / client_count
            if isinstance(qps_per_node, (int, float))
            and isinstance(client_count, int)
            and client_count > 0
            else None
        ),
        "client_threads_per_node": (
            client_count * threads_per_client
            if isinstance(client_count, int)
            and client_count > 0
            and isinstance(threads_per_client, int)
            and threads_per_client > 0
            else None
        ),
    }


def build_analysis(
    run_dir: Path,
    bottleneck_path: Path | None,
    archive_path: Path,
    source: dict,
    runtime_config: dict | None = None,
    *, evidence_path: Path | None = None,
) -> dict:
    summary_path = run_dir / "summary.json"
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    manifest_path = run_dir / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8")) if manifest_path.exists() else {}
    if evidence_path is not None:
        if bottleneck_path is not None:
            raise ValueError("NUMA accepts either Evidence or a legacy read model")
        evidence = load_evidence(evidence_path)
        if evidence.get("summary_sha256") != summary_digest(summary_path):
            raise ValueError("NUMA Evidence does not match Triage summary")
        trace_context = _rows_from_evidence(summary, evidence)
    elif bottleneck_path is not None:
        trace_context = json.loads(bottleneck_path.read_text(encoding="utf-8"))
    else:
        raise ValueError("NUMA requires validated Evidence or a legacy read model")
    inventory_path = run_dir / "inventory.json"
    try:
        inventory = json.loads(inventory_path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        inventory = {}
    if (not isinstance(inventory, dict) or inventory.get("schema_version") != 1
            or inventory.get("inputs") != manifest.get("inputs")):
        inventory = manifest
    indexed = _cohorts_from_inventory(inventory, archive_path)
    cohorts, archive_member_count = indexed if indexed is not None else _scan_archive_cohorts(archive_path)
    classification_basis = "numa_observation" if evidence_path is not None else "read_diagnosis"
    records = build_trace_records(summary, trace_context, cohorts, classification_basis)
    for record in records:
        record["error_chain"] = classify_error_chain(record)
        record.pop("error_observations", None)
        if not record.get("worker"):
            record["worker"] = next(
                (request["worker"] for request in record["urma_requests"] if request.get("worker")), None
            )
    aggregate = build_aggregate(records, source)
    aggregate.update(
        {
            "archive_member_trace_files": archive_member_count,
            "cohort_membership_count": sum(len(values) for values in cohorts.values()),
            "cohort_counts": dict(Counter(cohort for values in cohorts.values() for cohort in values)),
            "overlap_trace_count": sum(len(values) > 1 for values in cohorts.values()),
            "error_family_counts": dict(Counter(record["error_chain"]["family"] for record in records)),
            "slow_wr_count": sum(record["slow_wr_count"] for record in records),
            "dual_chip_trace_count": sum(record["chip_mode"] == "双 chip" for record in records),
            "chip_observed_trace_count": sum(record["chip_mode"] != "未观测" for record in records),
        }
    )
    latency_bands = summarize_latency_bands(records)
    time_buckets = summarize_time_buckets(records)
    return {
        "schema_version": 1,
        "metadata": {
            "case": manifest.get("case_name") or manifest.get("case") or "pr2081-numa",
            "run_dir": run_dir.name,
            "archive": archive_path.name,
            "source": source,
            "runtime_config": normalize_runtime_config(runtime_config),
            **({"classification_basis": classification_basis} if evidence_path is not None else {}),
        },
        "aggregate": aggregate,
        "latency_bands": latency_bands,
        "time_buckets": time_buckets,
        "insights": build_insights(records, latency_bands, time_buckets, classification_basis),
        "source_chain": build_source_chain(source),
        "limitations": [
            "同一 Trace 在 Core 与时延档目录重复出现时只计一次，cohort 标签保留多值。",
            "srcChipInflight 是发送侧完成时刻的全局 inflight 快照；它不能证明当前 WR 的选片结果，也不能单独证明接收端带宽或端到端吞吐收益。",
            "缺失的 RPC、URMA、chip、CPU、锁或调度字段保持未观测，不按 0 处理。",
            (
                f"当前单包没有修改前同配置基线，不计算 PR {source['pr']} 的性能提升百分比。"
                if source.get("pr")
                else "当前单包没有同配置对照基线，不计算性能提升百分比。"
            ),
        ],
        "traces": records,
    }


HTML_TEMPLATE = asset_path("numa.html").read_text(encoding="utf-8")


def _safe_json(value: object) -> str:
    return (
        json.dumps(value, ensure_ascii=False, separators=(",", ":"))
        .replace("<", "\\u003c")
        .replace(">", "\\u003e")
        .replace("&", "\\u0026")
    )


def render_html(analysis: dict, echarts_source: str) -> str:
    source = analysis.get("metadata", {}).get("source") or {}
    pr = source.get("pr")
    if pr:
        template = HTML_TEMPLATE.replace("PR2081", f"PR{pr}").replace("PR 2081", f"PR {pr}")
    else:
        template = HTML_TEMPLATE.replace("PR2081 多NUMA", "多NUMA")
        template = template.replace("PR2081 NUMA诊断", "NUMA诊断")
        template = template.replace("PR 2081 · ", "")
        template = template.replace("PR 2081源码链", "当前源码链")
    chart_support = asset_path("charts.js").read_text(encoding="utf-8")
    chart_support += "\n" + asset_path("chapter_navigation.js").read_text(
        encoding="utf-8"
    )
    shared_style = (
        asset_path("numa.css").read_text(encoding="utf-8")
        + "\n"
        + asset_path("shared.css").read_text(encoding="utf-8")
    )
    template = template.replace("</head>", "<style>" + shared_style + "</style></head>", 1)
    navigation = asset_path("navigation.js").read_text(encoding="utf-8")
    template = template.replace("</body>", "<script>" + navigation + "</script></body>", 1)
    worker_time = asset_path("numa_worker_time.js").read_text(encoding="utf-8")
    template = template.replace("</body>", "<script>" + worker_time + "</script></body>", 1)
    template = template.replace("__ECHARTS_SOURCE__", echarts_source + "</script><script>" + chart_support, 1)
    from .rendering.registry import embed_registry
    return embed_registry(template.replace("__DATA_JSON__", _safe_json(analysis), 1), "numa")


def write_outputs(analysis, output, analysis_json, *, force=False, library_path=None):
    for target in (output, analysis_json):
        if target.exists() and not force:
            raise FileExistsError(f"refusing to overwrite {target}; pass --force")
    library_path = library_path or echarts_path()
    output.parent.mkdir(parents=True, exist_ok=True)
    analysis_json.parent.mkdir(parents=True, exist_ok=True)
    analysis_json.write_text(json.dumps(analysis, ensure_ascii=False), encoding="utf-8")
    output.write_text(render_html(analysis, library_path.read_text(encoding="utf-8")), encoding="utf-8")
    return output.resolve(), analysis_json.resolve()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-dir", type=Path, required=True)
    parser.add_argument("--bottleneck-analysis", type=Path, required=True)
    parser.add_argument("--archive", type=Path, required=True)
    parser.add_argument("--source-head", required=True)
    parser.add_argument("--source-base", required=True)
    parser.add_argument("--pr", type=int)
    parser.add_argument("--qps-per-node", type=float)
    parser.add_argument("--client-count", type=int)
    parser.add_argument("--threads-per-client", type=int)
    parser.add_argument("--workers-per-node", type=int)
    parser.add_argument("--echarts", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--analysis-json", type=Path, required=True)
    parser.add_argument("--force", action="store_true")
    args = parser.parse_args(argv)
    for target in (args.output, args.analysis_json):
        if target.exists() and not args.force:
            parser.error(f"refusing to overwrite {target}; pass --force")
    library_path = (
        args.echarts
        or echarts_path()
    )
    analysis = build_analysis(
        args.run_dir,
        args.bottleneck_analysis,
        args.archive,
        {"head": args.source_head, "base": args.source_base, "pr": args.pr},
        {
            "qps_per_node": args.qps_per_node,
            "client_count": args.client_count,
            "threads_per_client": args.threads_per_client,
            "workers_per_node": args.workers_per_node,
        },
    )
    write_outputs(analysis, args.output, args.analysis_json, force=args.force, library_path=library_path)
    sys.stdout.write(f"{args.output}\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
