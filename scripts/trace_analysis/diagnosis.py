"""Shared read/write diagnosis vocabulary and evidence rules for Trace reports."""
from __future__ import annotations

READ_STAGES = (
    "URMA建链", "URMA通信", "URMA调度/线程开销", "QueryAndGet其他业务",
    "Get其他业务", "其他调度/线程开销", "RPC网络相关", "RPC框架", "未解释残差",
)
WRITE_STAGES = (
    "Create RPC其他", "写入MemoryCopy", "写入URMA通信", "写入URMA调度/线程开销",
    "Publish RPC其他", "Worker Publish/元数据", "其他调度/线程开销", "RPC网络相关",
    "RPC框架", "未解释残差",
)
READ_STAGE_GUIDANCE = {
    "URMA建链": "检查 URMA 建链与连接复用证据；仅在日志明确观测时归因。",
    "URMA通信": "核对 URMA_ELAPSED_TOTAL、完成事件和 trace_us；缺失时标记未观测。",
    "URMA调度/线程开销": "单独检查 wake、poll、sleep、线程调度字段，不能并入通信。",
    "QueryAndGet其他业务": "保留 QueryAndGet 父窗口中无法进一步拆分的业务耗时。",
    "Get其他业务": "保留 Get 阶段中无更细证据的业务耗时。",
    "RPC网络相关": "使用 RPC 框架和网络残差证据；不能用 transportType 推断网络原因。",
    "RPC框架": "使用客户端/服务端框架字段扣除已明确的 handler 与网络阶段。",
    "其他调度/线程开销": "仅归入有调度或排队证据的剩余时间。",
    "未解释残差": "证据不足时保留残差，不强行归因。",
}


def guidance(stage: str, write: bool = False) -> str:
    if write:
        return {
            "写入URMA通信": "核对写入 URMA 完成路径和 dataSize；没有完成证据时标记未观测。",
            "写入URMA调度/线程开销": "单独检查 wake、poll、sleep 和线程调度字段。",
            "写入MemoryCopy": "核对显式 MemoryCopy 阶段，不与 URMA 通信合并。",
        }.get(stage, READ_STAGE_GUIDANCE.get(stage, "保留当前阶段证据和未解释残差。"))
    return READ_STAGE_GUIDANCE.get(stage, "保留当前阶段证据和未解释残差。")


def worker_log_assessment(trace: dict, collection: list[dict] | None = None) -> dict:
    """Keep collection availability distinct from parsed Worker-stage coverage."""
    coverage = trace.get("evidence_coverage") or {}
    observed_roles = [role for role in ("entry_worker", "meta_worker", "data_worker")
                      if coverage.get(role) == "present"]
    targets = []
    for item in collection or []:
        status = item["collection_status"]
        refs = item.get("evidence_refs") or []
        terminated = status == "not_collected" and item.get("reason") == "pod_terminated" and bool(refs)
        labels = {
            "not_collected": "目标 Worker 日志未采集",
            "collected": "日志已采集；对应请求是否匹配需独立核对",
            "out_of_window": "采集记录声明请求不在覆盖窗口内",
            "lifecycle_mismatch": "POD/进程实例不匹配",
        }
        targets.append({
            "worker": item.get("worker"), "pod_uid": item.get("pod_uid"),
            "collection_status": status, "evidence_refs": refs,
            "termination_supported": terminated,
            "message": "采集记录报告 POD 终止导致日志未采集" if terminated else labels[status],
        })
    if targets:
        state = "collection_evidence"
        message = "；".join(f"{x['worker']}：{x['message']}" for x in targets)
    elif observed_roles:
        state = "partial"
        message = "已关联部分 Worker 阶段；不代表所有目标 Worker 日志完整"
    else:
        state = "coverage_unknown"
        message = "未关联 Worker 阶段；缺少采集清单，无法区分日志不存在、抽样缺失或关联未匹配。不能据此断言 POD 被 kill"
    return {"state": state, "message": message, "observed_roles": observed_roles,
            "targets": targets, "timing_policy": "保留 Client 证据；缺失 Worker 阶段不补零、不作根因归属"}
