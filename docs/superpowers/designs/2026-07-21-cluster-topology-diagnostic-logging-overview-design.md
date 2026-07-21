# Cluster Topology 诊断日志概要设计

- 文档状态：草案
- 作者：Codex
- 评审人：Topology Runtime owner、Worker Runtime owner、SRE/运维 owner、测试 owner
- 最后更新：2026-07-22
- 源码基线：`yrds/master`
- 关联详细设计：`docs/superpowers/designs/2026-07-20-cluster-topology-flow-logging-detailed-design.md`
- 关联执行计划：`docs/superpowers/plans/2026-07-21-cluster-topology-diagnostic-logging.md`
- 用户定位手册：`docs/source_zh_cn/appendix/cluster_log_diagnosis_guide.md`

## 0. 结论摘要

本方案面向 cluster topology 控制面日志，目标是让用户在日志平台中通过 `CLUSTER_` 还原历史拓扑事件，并能通过 `flow=<scene>`、prefix trace、`epoch`、`operation_prefix`、`task_prefix` 下钻到 controller、materializer、executor、callback 和 coordination backend 的责任边界。

当前 PR 遵循最小修改原则：不改变 topology 状态机、placement、task payload、callback 顺序或 backend keyspace；只新增一个向后兼容的 active batch 诊断 trace 字段和一组结构化日志字段。缩容 callback 当前源码顺序仍是 data drain 后 metadata handoff，本方案只把该顺序和耗时暴露出来，不在本 PR 改缩容业务顺序。

## 1. coordination backend 为 etcd 时的事件分类

| 分类 | 事件语义 | 当前日志覆盖 |
|---|---|---|
| E1 watch 生命周期 | controller、worker、observer 注册 watch 或触发 resync。 | `CLUSTER_WATCH flow=watch` |
| E2 membership lease 写入 | 节点发布 STARTING、RESTARTING、DOWNGRADE_RESTARTING、READY、EXITING。 | `CLUSTER_MEMBERSHIP flow=membership|restart action=lease_update` |
| E3 restart reconciliation | 重启节点完成本地恢复并发布 READY。 | `CLUSTER_FLOW flow=restart action=reconciliation_done`、`CLUSTER_MEMBERSHIP flow=restart state=READY` |
| E4 controller membership 视图 | controller 将 READY/EXITING 事实写入 topology member 状态。 | `CLUSTER_MEMBERSHIP_OBSERVED flow=membership action=view` |
| E5 topology authority/bootstrap | topology key 初始化，首批成员形成初始 topology。 | `CLUSTER_FLOW flow=bootstrap action=authority_init|start|finish`、`CLUSTER_CHANGE flow=bootstrap` |
| E6 多节点扩容 | INITIAL/JOINING 成员生成 active batch、task、notify、finish。 | `CLUSTER_FLOW flow=scale_out`、`CLUSTER_CHANGE`、`CLUSTER_TASK` |
| E7 多节点缩容 | PRE_LEAVING/LEAVING 成员生成 active batch，执行 data drain 和 metadata handoff，等待外部退出并 finish。 | `CLUSTER_FLOW flow=scale_in`、`CLUSTER_CHANGE`、`CLUSTER_TASK`、`CLUSTER_SCALE_IN` |
| E8 节点故障处理 | controller 确认 failed member 后生成 failure active batch 并 final。 | `CLUSTER_FLOW flow=failure`、`CLUSTER_FAILURE`、`CLUSTER_TASK` |
| E9 扩缩容异常变迁 | scale-out timeout、scale-in external termination wait、failure replan、membership cleanup。 | `CLUSTER_FLOW action=finish|wait|replan` |
| E10 backend access failed | worker 读 topology、notify 或 watch 相关访问失败。 | `CLUSTER_BACKEND flow=backend action=access_failed` |
| E11 backend 降级和恢复 | worker 进入 CONTROL_DEGRADED、ROLE_ISOLATED、NOT_READY 或恢复 NORMAL。 | `CLUSTER_DEGRADED flow=backend action=state_change`、`CLUSTER_BACKEND action=recovered` |
| E12 runtime operation failure | worker runtime 队列、reload、notify 等操作失败。 | `CLUSTER_RUNTIME_OPERATION_FAILED flow=runtime` |
| E13 cluster shutdown | 全部成员退出后 controller 清空 topology。 | `CLUSTER_FLOW flow=shutdown`、`CLUSTER_SHUTDOWN` |

覆盖标准：

- 过滤 `CLUSTER_` 能按时间得到 topology 相关历史事件。
- 过滤 `CLUSTER_ AND flow=<scene>` 能看到初始化、重启、扩容、缩容、故障、backend 降级、shutdown 的主线。
- 过滤 `CLUSTER_ AND result=failed` 能得到失败阶段、归类原因和原样 `status`。
- 复制 `CLUSTER_FLOW action=start` 的 prefix trace 后，可以串联同一轮高层扩容、缩容或故障处理。

## 2. 日志格式与字段原则

日志平台中的完整日志格式：

```text
<time> | <severity> | <file:line> | <host> | <pid:tid> | <trace> | <cluster> | <message>
```

message 不重复 prefix 中已有的 `severity`、trace 和 cluster。高层 topology 变更 trace 形如 `cluster;<uuid_suffix>`，例如 `cluster;f4a91c7e2b30`。`epoch` 只表示内部 active batch/version，不作为 trace。

字段优先级：

| 字段 | 用途 |
|---|---|
| `flow` | 场景入口：`bootstrap`、`restart`、`membership`、`scale_out`、`scale_in`、`failure`、`backend`、`runtime`、`shutdown`、`watch`。 |
| `action` | 阶段入口：`start`、`commit`、`task_created`、`notify`、`progress`、`data_drain`、`metadata_handoff`、`wait`、`finish` 等。 |
| `component` | 责任边界：`controller`、`materializer`、`executor`、`callback`、`backend`、`worker_runtime`、`observer`。 |
| `result` / `reason` / `status` | 终态、归类原因和完整原始错误。失败日志必须保留 `status`，本轮不清洗、不截断。 |
| `members` / `participants` | 低频控制面日志全量打印成员或参与节点，不做 sample 上限。 |
| `operation_prefix` / `task_prefix` | 高频 task/callback 日志只打印短 id，用于下钻。 |
| `flow_duration_ms` / `phase_duration_ms` | flow 总耗时和单阶段耗时。 |

## 3. 关键决策

| 决策 | 内容 | 理由 |
|---|---|---|
| D1 | 复用已有 `CLUSTER_` tag，不新增 `CLUSTER_NODE_FAILURE` 这类平行主 tag。 | 避免用户检索入口分裂。 |
| D2 | active batch 增加 `diagnostic_trace_id`。 | 同一轮 scale-out、scale-in、failure 的 controller、executor、callback 日志需要跨进程共享 trace。 |
| D3 | membership lease update 使用独立随机 trace。 | lease update 是高层变更的输入事实，未写入 active batch，不能假装与后续 flow 使用同一 trace。 |
| D4 | 低频控制面全量打印 members/participants，高频 task/callback 保持轻量。 | 11 worker 缩容要看到所有成员，同时避免 progress/retry 日志膨胀。 |
| D5 | 运维和开发共用同一套 INFO/WARN/ERROR `CLUSTER_` 日志。 | 事故后开发也依赖日志平台，不设计隐藏的开发-only 日志。 |

## 4. 范围

包含：

- cluster diagnostics helper；
- `ChangeBatchPb.diagnostic_trace_id` 及 domain/codec 传播；
- controller flow、membership observed、topology commit、shutdown 日志；
- materializer task_created 日志；
- executor notify/progress/recover 日志；
- worker callback data_drain、metadata_handoff、failure callback_step 日志；
- topology engine/observer watch、ring、backend、degraded、runtime failure 日志；
- etcd/Ds coordination backend membership lease update 和 restart reconciliation 日志；
- 中文附录用户定位手册和 `.repo_context` 入口。

不包含：

- 缩容 metadata-first 业务顺序调整；
- master metadata manager 内部 primary handoff 日志重构；
- task janitor stale cleanup 日志重构；
- failure missing/resolved 细粒度检测日志；
- failure 抢占/恢复的独立 preempt/resume 日志；
- 新日志平台、metrics 或全局 logging framework。

这些不包含项保留为后续 cluster 日志规范或缩容行为修复的独立工作，不在本 PR 伪装为已交付能力。

## 5. 用户定位入口

| 场景 | 检索入口 |
|---|---|
| 全部拓扑事件 | `<cluster_name> AND CLUSTER_` |
| 初始化 | `<cluster_name> AND CLUSTER_ AND flow=bootstrap` |
| 重启恢复 | `<cluster_name> AND CLUSTER_ AND flow=restart` |
| 扩容 | `<cluster_name> AND CLUSTER_ AND flow=scale_out` |
| 缩容 | `<cluster_name> AND CLUSTER_ AND flow=scale_in` |
| 节点故障 | `<cluster_name> AND CLUSTER_ AND flow=failure` |
| backend 降级/恢复 | `<cluster_name> AND CLUSTER_ AND flow=backend` |
| 失败 | `<cluster_name> AND CLUSTER_ AND result=failed` |
| shutdown | `<cluster_name> AND CLUSTER_ AND flow=shutdown` |

完整示例、解读模板和开发交接字段见 `docs/source_zh_cn/appendix/cluster_log_diagnosis_guide.md`。

## 6. 验证策略

| 验证项 | 方式 |
|---|---|
| trace 形态和 scope 恢复 | `tests/ut/cluster/topology_diagnostic_log_test.cpp` |
| members 全量打印 | `tests/ut/cluster/topology_diagnostic_log_test.cpp` |
| proto schema 兼容 | `tests/ut/cluster/cluster_topology_schema_test.cpp` |
| codec round-trip | `tests/ut/cluster/topology_repository_codec_test.cpp` |
| controller 创建 active batch trace | `tests/ut/cluster/topology_controller_test.cpp` |
| executor 传播 trace 到 callback action | `tests/ut/cluster/topology_task_executor_test.cpp` |
| source guard | grep 禁止 payload `cluster=`、`severity=`、`trace_id=`、`batch_type`、`type_name=`、`CLUSTER_SCALE_IN_DRAIN` |
| 文档入口 | 附录 index 能检索到 `cluster_log_diagnosis_guide.md` |

## 7. 风险与回滚

| 风险 | 评估 |
|---|---|
| 性能 | 新增日志在控制面、watch、callback、backend 状态变化等低频或后台路径；高频 progress 不打印 members。 |
| 并发 | `ClusterTraceScope` 只操作线程本地 trace；controller flow start 时间保存在 controller 自有线程上下文。 |
| 兼容 | proto3 新增字段 3 向后兼容，旧数据 decode 后 trace 为空，新代码会 fallback 本地 trace。 |
| 可观测性 | `status` 原样打印提升排障能力，但评审需确认对应 Status 不携带密钥或对象 key。 |
| 回滚 | revert 本 PR 后旧 type/epoch 字段仍可读，未知 proto 字段会被旧代码跳过。 |
