# 集群日志定位指南

本文面向日志检索、事故复盘和开发排障，说明如何使用 `CLUSTER_` 日志定位集群拓扑发布、扩容、缩容和故障恢复问题。

## 关联一轮拓扑变更

集群控制器、任务执行器和业务回调运行在不同线程，不能假设一轮变更始终使用同一个 trace。定位时优先使用以下字段关联：

| 字段 | 含义 |
| --- | --- |
| `cluster` | 集群名。 |
| `batch_type` / `type_name` | `SCALE_OUT`、`SCALE_IN` 或 `FAILURE`。 |
| `batch_epoch` / `epoch` | 一轮拓扑变更的稳定编号。 |
| `task_prefix` | epoch 内的单个迁移或恢复任务。 |
| `operation_prefix` | 跨重试稳定的业务操作编号。 |
| `stage` | `metadata_migration`、`data_drain`、`cleanup`、`failure_recovery` 或 `task_completion`。 |
| `stage_event` | 阶段的 `start` 或 `finish`。 |
| `executor` | 实际执行任务的 worker。 |
| `source` / `target` / `failed` | 迁移源、迁移目标或故障节点。 |
| `range_count` / `hash_ranges` | 本任务覆盖的闭区间数量和具体范围。 |
| `status` / `outcome` | 阶段结果、重试或持久化结果。 |

推荐从 controller 的批次开始日志获取 epoch，再逐层下钻：

```text
CLUSTER_CHANGE_BATCH AND action=start
CLUSTER_CHANGE_BATCH AND action=replan
CLUSTER_TASK AND epoch=<epoch>
CLUSTER_TASK AND task_prefix=<task_prefix>
CLUSTER_TASK_CALLBACK AND operation_prefix=<operation_prefix>
```

`sample` 默认完整打印当前支持的32个节点。若未来成员数超过32，末尾会出现
`...(truncated,total=<member_count>)`，不能把截断后的列表当作完整成员集合。

## 范围和哈希环格式

任务迁移范围使用闭区间：

```text
range_count=2 hash_ranges=[[0,1023],[4096,8191]]
```

哈希环范围使用 `(previous_token,current_token]`。起点大于终点表示跨越 `uint32` 环尾，例如
`(300,100]`；单 token 环打印为 `full`。

新 topology 版本提交或在本地发布时，`CLUSTER_RING` 打印：

- `committed_ring`：当前对业务请求生效的 owner ring。
- `prospective_ring`：当前普通批次完成后将生效的 owner ring；没有普通批次或处于故障批次时与
  `committed_ring` 相同。

每个 ring 字段包含节点地址、成员 id 前缀、成员状态和该节点对应的全部范围：

```text
CLUSTER_RING role=worker status=published version=18 batch_type=SCALE_OUT batch_epoch=18 \
committed_ring=[{address=<worker-a>,id_prefix=<id>,state=ACTIVE,ranges=[(300,100]]},\
{address=<worker-b>,id_prefix=<id>,state=JOINING,ranges=[]}] \
prospective_ring=[{address=<worker-a>,id_prefix=<id>,state=ACTIVE,ranges=[(200,100]]},\
{address=<worker-b>,id_prefix=<id>,state=JOINING,ranges=[(100,200]]}]
```

完整 ring 只在 controller CAS 成功或 Worker/Observer 确实发布新版本时以 INFO 打印；重复读取同一版本只保留
VLOG 摘要。单次输出量与成员数和 token 总数线性相关；默认每节点 4 个 token，32 节点时每个 ring 视图包含
128 个范围。使用非默认 token 数时，应按实际 token 总数评估单次日志行长度和磁盘容量；为保证拓扑故障可还原，
INFO 日志仍保留全部节点及其范围。

## 扩容

扩容的正常阶段顺序：

1. `CLUSTER_CHANGE_BATCH action=start batch_type=SCALE_OUT`：topology CAS 已成功，新节点进入 `JOINING`。
2. `CLUSTER_TASK action=callback_submitted stage=metadata_migration stage_event=start`：迁移任务开始。
3. `CLUSTER_TASK action=callback_finished stage=metadata_migration stage_event=finish`：业务回调结束。
4. `CLUSTER_TASK action=progress stage=task_completion stage_event=finish`：任务范围已持久化为完成。
5. `CLUSTER_CHANGE_BATCH action=finalized batch_type=SCALE_OUT`：最终 topology 已提交，新节点进入 `ACTIVE`。

任务日志包含实际迁移方向和范围：

```text
CLUSTER_TASK action=callback_submitted stage=metadata_migration stage_event=start type_name=SCALE_OUT \
epoch=<epoch> task_prefix=<task> operation_prefix=<operation> executor=<source-worker> \
source=<source-worker> target=<joining-worker> range_count=2 hash_ranges=[[0,1023],[4096,8191]]
```

若只有 batch start 而没有 callback submitted，按相同 epoch 检查 notify、任务读取和 backend 日志。若 callback
finished 成功但没有 progress，优先检查 `MarkTaskScopeFinished` 对应的 repository 状态。

## 缩容

缩容的正常阶段顺序：

1. `CLUSTER_CHANGE_BATCH action=start batch_type=SCALE_IN`：topology CAS 已成功，退出节点进入 `LEAVING`。
2. `callback_submitted/finished stage=metadata_migration`：单个 task 的元数据迁移开始和结束。
3. `CLUSTER_SCALE_IN action=metadata_done checkpoint=persisted checkpoint_scope=task`：该 task 的
   metadata-done marker 已持久化。
4. 同一 source 的所有 metadata task 完成后，`source_gate=ready`；未完成时为 `source_gate=waiting`。
5. `CLUSTER_SCALE_IN action=data_drain stage=data_drain stage_event=start|finish
   drain_scope=source_worker`：退出 worker 的数据清理开始和结束。
6. `CLUSTER_TASK action=cleanup_submitted|cleanup_finished stage=cleanup`：Snapshot fence 授权后的本地清理。
7. `CLUSTER_TASK action=progress stage=task_completion stage_event=finish`：任务最终完成。
8. `CLUSTER_CHANGE_BATCH action=finalized batch_type=SCALE_IN`：退出节点从 topology 删除。

`metadata_done` 是 task 级持久化 checkpoint，不代表整个 source 已完成。应结合以下字段判断：

```text
CLUSTER_SCALE_IN action=metadata_done checkpoint=persisted checkpoint_scope=task \
source_gate=waiting epoch=<epoch> task_prefix=<task> operation_prefix=<operation> \
executor=<leaving-worker> source=<leaving-worker> target=<target-worker> \
range_count=1 hash_ranges=[[1024,2047]]
```

data drain 扫描的是 source worker 的本地数据，不是只迁移到日志中的一个 target。日志因此使用
`drain_scope=source_worker target_role=trigger`，其中通用 `target` 字段仅表示触发当前 task 的目标，避免把它
误解为整个数据清理的唯一目标。

## 故障恢复

故障恢复的正常阶段顺序：

1. `CLUSTER_FAILURE action=plan outcome=start_pending|replan_pending`：controller 已生成候选计划，尚未确认
   topology CAS。
2. `CLUSTER_CHANGE_BATCH action=start|replan batch_type=FAILURE`：CAS 和精确回读成功，故障批次已明确开始。
3. `CLUSTER_TASK action=callback_submitted stage=failure_recovery stage_event=start`：恢复任务开始。
4. `CLUSTER_TASK action=callback_finished stage=failure_recovery stage_event=finish`：best-effort 恢复回调结束。
5. `CLUSTER_TASK action=progress stage=task_completion stage_event=finish`：恢复范围进度已持久化。
6. `CLUSTER_CHANGE_BATCH action=finalized batch_type=FAILURE`：最终 topology 已提交，故障节点被删除。

故障任务使用 `executor` 和 `failed` 表达参与者，并打印恢复范围：

```text
CLUSTER_TASK action=callback_submitted stage=failure_recovery stage_event=start type_name=FAILURE \
epoch=<epoch> executor=<surviving-worker> failed=<failed-worker> \
range_count=1 hash_ranges=[[2048,4095]]
```

故障可能抢占扩容或缩容。此时先按时间找到 `batch_type=FAILURE action=start|replan`，再用旧普通批次 epoch
确认其最后一条 task/progress 日志；不要仅凭抢占前的 `callback_finished` 判断普通批次已经 finalized。

## 常见定位判断

| 现象 | 优先检查 |
| --- | --- |
| 没有 batch start | `CLUSTER_MEMBERSHIP_OBSERVED`、`CLUSTER_FAILURE_DETECT` 和 CAS 冲突。 |
| 有 batch start，没有 notify | `CLUSTER_TASK action=materialize` 的 task/notify 数量以及 watch/backend 状态。 |
| 有 notify，没有 callback start | 本地 task 读取、execution fence 和 callback queue。 |
| callback finish 失败 | 同 `operation_prefix` 的 `callback_failed`、`retry_scheduled` 和原始 `status`。 |
| 缩容 metadata_done 反复 waiting | 同 `source`、`epoch` 下是否仍有其他 task 未打印 metadata_done。 |
| 缩容 metadata 完成但 data drain 未开始 | 最后一条 metadata_done 是否为 `source_gate=ready`，以及 worker readiness。 |
| data drain 完成但任务不结束 | `cleanup_submitted`、`cleanup_finished`、Snapshot fence 和 repository progress。 |
| ring 与预期迁移方向不一致 | 对比同 version 的 `committed_ring`、`prospective_ring` 和 task `hash_ranges`。 |
| 故障计划存在但没有明确开始 | 查找 topology CAS 失败；只有 `CLUSTER_CHANGE_BATCH action=start|replan` 才表示提交成功。 |

交接给开发时至少保留：

```text
cluster：<cluster>
batch：type=<type>, epoch=<epoch>, topology_version=<version>
task：task_prefix=<task>, operation_prefix=<operation>
stage：<stage>/<stage_event>
participants：executor=<executor>, source=<source>, target=<target>, failed=<failed>
scope：range_count=<count>, hash_ranges=<ranges>
ring：committed_ring=<ring>, prospective_ring=<ring>
result：outcome=<outcome>, status=<raw status>
```
