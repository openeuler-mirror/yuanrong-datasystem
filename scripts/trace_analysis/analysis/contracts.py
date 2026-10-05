"""Stable read-stage taxonomy shared by attribution and aggregate models."""

from __future__ import annotations


from ..diagnosis import READ_STAGES


CATEGORY_REMOTE = "明确远端数据阶段"


CATEGORY_WORKER = "数据访问处理未细分"


CATEGORY_CLIENT_RPC = "Client→Data Worker RPC residual"


STAGE_NAMES = (
    "RPC网络",
    "RPC排队",
    "QueryMeta",
    "URMA超时等待",
    "URMA",
    "远端供数处理",
    "数据访问父窗口/未细分",
    "未解释残差",
)


FOCUS_STAGE_NAMES = READ_STAGES


PROBLEM_NAMES = tuple(stage for stage in STAGE_NAMES if stage != "URMA超时等待") + ("URMA超时",)


NON_TRANSPORT_CATEGORIES = (
    "Client UB接收缓冲分配失败",
    "Client/Worker观测未闭合",
    "BatchGet超时/重试",
    "Data Worker服务端处理",
    "明确本地ProcessGet耗时",
    "Client数据获取窗口未细分",
    "ProcessGet内部未细分",
)
