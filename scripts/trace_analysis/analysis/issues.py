"""Read failure chains and observed root-cause boundaries."""

from __future__ import annotations

from ..evidence.errors import failed_data_rpc, failed_query_rpc


def _classify_urma_timeout_detail(status: int, observations: dict) -> dict[str, str | int | None]:
    """Describe the observed failure point and upward chain without guessing a hardware root cause."""

    pending_wrs = observations["pending_wrs"]
    if pending_wrs is None:
        subcategory = "URMA completion超时·pending未观测"
    elif pending_wrs > 1:
        subcategory = "URMA completion超时·多pending WR"
    else:
        subcategory = "URMA completion超时·单pending WR"

    unexpected_payload = observations["unexpected_payload"]
    rpc_deadline = observations["rpc_deadline"]
    if status == 1004 and unexpected_payload:
        chain = "URMA超时→UB异常响应→1004"
    elif status == 1001 and rpc_deadline:
        chain = "URMA超时→外层RPC deadline→1001"
    elif status:
        chain = f"URMA超时→状态{status}（上浮细节未闭合）"
    else:
        chain = "URMA超时（上浮状态未观测）"

    recovery = []
    if observations["send_lane_timeout"]:
        recovery.append("send lane已封存")
    if observations["send_lane_release"]:
        recovery.append("send lane已强制回收")
    return {
        "error_subcategory": subcategory,
        "error_chain_category": chain,
        "error_failure_point": "URMA WRITE completion在等待窗口内未返回",
        "error_root_cause_boundary": (
            "当前证据不能继续区分接收端未完成、链路/设备丢失、CQ/JFC轮询或线程唤醒异常"
        ),
        "error_recovery_action": "、".join(recovery) if recovery else "恢复动作未观测",
        "error_pending_wrs": pending_wrs,
    }


def _classify_rpc_deadline_detail(status: int, observations: dict) -> dict[str, str | int | None]:
    """Classify an observed RPC deadline while keeping its unobserved interval explicit."""

    receive_buffer_failure = observations["receive_buffer_failure"]
    arena_oom = observations["arena_oom"]
    if status == 1004 and receive_buffer_failure and arena_oom:
        return {
            "error_family": "Client UB接收缓冲分配失败",
            "error_subcategory": "Client arena fresh extent不足",
            "error_chain_category": "Client接收缓冲分配失败→1004",
            "error_failure_point": "Client为 UB 接收准备内存时 arena 分配失败",
            "error_root_cause_boundary": (
                "日志已闭合到 Client arena fresh extent 不足；"
                "这不是 URMA completion 超时，也不能由此推断已完成 WR 变慢"
            ),
            "error_recovery_action": "TransportGet终止数据读取并上浮1004",
            "error_pending_wrs": None,
        }
    if not status or not observations["rpc_deadline"]:
        return {}
    data_timeout = failed_data_rpc(observations)
    urma_connect_timeout = observations["connect_deadline"]
    query_timeout = failed_query_rpc(observations)
    # Method-specific failure evidence wins over the mere presence of another
    # successful RPC in the same Trace. QueryAndGet may succeed before a later
    # GetObjectRemote consumes the remaining API deadline.
    if urma_connect_timeout:
        subcategory = "Data URMA建链截止超时"
        chain = f"URMA建链超时→数据访问失败→{status}"
        failure_point = "WorkerWorkerExchangeUrmaConnectInfo未在剩余deadline内完成"
    elif data_timeout:
        subcategory = "Data RPC deadline"
        chain = f"Data RPC超时→TransportGet失败→{status}"
        failure_point = "GetObjectRemote未在deadline内返回"
    elif query_timeout:
        subcategory = "QueryMeta RPC deadline"
        chain = f"QueryMeta RPC超时→TransportGet失败→{status}"
        failure_point = "WorkerOCService.QueryAndGet未在deadline内返回"
    elif observations["get_method"] and not observations["query_rpc_mention"]:
        subcategory = "Data RPC deadline"
        chain = f"Data RPC超时→TransportGet失败→{status}"
        failure_point = "GetObjectRemote未在deadline内返回"
    elif observations["query_method"]:
        subcategory = "QueryMeta RPC deadline"
        chain = f"QueryMeta RPC超时→TransportGet失败→{status}"
        failure_point = "WorkerOCService.QueryAndGet未在deadline内返回"
    else:
        subcategory = "RPC deadline·方法未细分"
        chain = f"RPC超时→TransportGet失败→{status}"
        failure_point = "RPC未在deadline内返回"
    return {
        "error_family": "RPC截止超时",
        "error_subcategory": subcategory,
        "error_chain_category": chain,
        "error_failure_point": failure_point,
        "error_root_cause_boundary": (
            "失败RPC缺少完整闭环时，服务端执行、响应发送、网络交付和客户端截止观察之间仍不可区分"
        ),
        "error_recovery_action": "TransportGet终止/上浮失败",
        "error_pending_wrs": None,
    }
