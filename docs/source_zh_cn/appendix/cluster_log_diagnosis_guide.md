# 集群日志定位指南

本文面向日志平台检索、事故复盘和开发排障，说明如何用 `CLUSTER_` 日志定位集群初始化、重启、扩容、缩容、节点故障、coordination backend 降级、过程中失败和全集群退出。

示例环境：

- 集群名：`AZ`。
- worker：`127.0.0.1:31501`、`127.0.0.2:31501`、`127.0.0.3:31501`、`127.0.0.4:31501`。
- 真实环境检索时把 `AZ` 替换成目标集群名，或使用日志平台已有的 cluster prefix 字段过滤。

## 日志格式

每条日志分为 prefix 和 message：

```text
<time> | <severity> | <file:line> | <host> | <pid:tid> | <trace> | <cluster> | <message>
```

`severity`、trace 和 cluster 都在 prefix 中。message 内不会再重复打印 `severity=`、`trace_id=` 或 `cluster=`。高层集群变更事件的 trace 形如 `cluster;<uuid_suffix>`，例如 `cluster;f4a91c7e2b30`；同一轮扩容、缩容或故障处理应使用同一个 trace。

常用字段：

| 字段 | 含义 |
|---|---|
| `flow` | 事件类型：`bootstrap`、`restart`、`scale_out`、`scale_in`、`failure`、`backend`、`shutdown`、`membership`、`watch`。 |
| `action` | 当前阶段，例如 `start`、`task_created`、`notify`、`data_drain`、`finish`。 |
| `component` | 责任边界：`controller`、`materializer`、`executor`、`callback`、`backend`、`worker_runtime`、`observer`。 |
| `epoch` | 内部 topology batch/version 编号。优先使用 prefix trace 收敛同一轮事件，`epoch` 用于区分内部批次。 |
| `members` | controller 当前看到的完整 membership 列表。 |
| `participants` | 本轮扩容、缩容、故障、bootstrap 或 shutdown 涉及的节点列表。 |
| `source` / `target` | metadata 或 data 迁移方向。 |
| `executor` | 实际执行 task 或 callback 的 worker。 |
| `operation_prefix` | 业务操作短 id，用于串联 metadata、data drain 和 primary handoff。 |
| `task_prefix` | task 短 id，用于在同一 epoch 内定位单个 task。 |
| `timeout_ms` | `node_timeout_s * 1000`，节点进入 lost/missing 的阈值。 |
| `dead_ms` | `node_dead_timeout_s * 1000`，节点确认 dead 的阈值。 |
| `flow_duration_ms` | 从 flow start 到当前终点或等待日志的总耗时。 |
| `phase_duration_ms` | 单阶段耗时，例如 data drain 或 backend outage 持续时间。 |
| `result` | 阶段结果：`pending`、`accepted`、`waiting`、`success`、`failed`。 |
| `reason` | 归类后的原因，便于检索和统计。 |
| `status` | 原样完整错误信息，本轮不清洗、不截断。 |

## 通用定位步骤

1. 拉取集群拓扑历史：

```text
AZ AND CLUSTER_
```

2. 按场景缩小范围：

| 场景 | 检索入口 |
|---|---|
| 初始化 | `AZ AND CLUSTER_ AND flow=bootstrap` |
| 重启恢复 | `AZ AND CLUSTER_ AND flow=restart` |
| 扩容 | `AZ AND CLUSTER_ AND flow=scale_out` |
| 缩容 | `AZ AND CLUSTER_ AND flow=scale_in` |
| 节点故障 | `AZ AND CLUSTER_ AND flow=failure` |
| backend 降级/恢复 | `AZ AND CLUSTER_ AND flow=backend` |
| 全集群退出 | `AZ AND CLUSTER_ AND flow=shutdown` |
| 失败事件 | `AZ AND CLUSTER_ AND result=failed` |

3. 从 `CLUSTER_FLOW action=start` 复制高层变更的 prefix trace，例如 `cluster;f4a91c7e2b30`，再按 trace、节点、`epoch`、`operation_prefix` 或 `task_prefix` 二次过滤。`CLUSTER_MEMBERSHIP action=lease_update` 是触发输入事实，可能使用不同 trace。

4. 输出结论时说明：事件类型、目标节点、起点、最后一个有效阶段、终点、耗时、失败原因、是否被故障抢占、是否恢复。

## 扩容示例

场景：`127.0.0.3:31501` 和 `127.0.0.4:31501` 同时加入原 2 节点集群。

检索入口：

```text
AZ AND CLUSTER_ AND flow=scale_out
```

关键日志：

```text
2026-07-21T10:20:00.000101 | I | etcd_coordination_backend.cpp:118 | 127.0.0.3 | 7447:7447 | cluster;7f50d1200a33 | AZ | CLUSTER_MEMBERSHIP flow=membership action=lease_update component=backend role=worker node=127.0.0.3:31501 state=READY result=success reason=finished status=OK
2026-07-21T10:20:00.001101 | I | etcd_coordination_backend.cpp:118 | 127.0.0.4 | 7447:7447 | cluster;41c2a88b5e10 | AZ | CLUSTER_MEMBERSHIP flow=membership action=lease_update component=backend role=worker node=127.0.0.4:31501 state=READY result=success reason=finished status=OK
2026-07-21T10:20:00.018512 | I | topology_controller.cpp:704 | 127.0.0.1 | 7447:7447 | cluster;88e3d12ca561 | AZ | CLUSTER_MEMBERSHIP_OBSERVED flow=membership action=view component=controller role=controller topology_version=71 member_count=4 state_counts=initial=2,joining=0,active=2,pre_leaving=0,leaving=0,failed=0 members=[127.0.0.1:31501/ACTIVE,127.0.0.2:31501/ACTIVE,127.0.0.3:31501/INITIAL,127.0.0.4:31501/INITIAL] result=changed
2026-07-21T10:20:00.038742 | I | topology_controller.cpp:819 | 127.0.0.1 | 7447:7447 | cluster;f4a91c7e2b30 | AZ | CLUSTER_FLOW flow=scale_out action=start component=controller role=controller epoch=72 participant_count=2 participants=[127.0.0.3:31501/JOINING,127.0.0.4:31501/JOINING] result=accepted
2026-07-21T10:20:00.045100 | I | topology_task_materializer.cpp:291 | 127.0.0.1 | 7447:7447 | cluster;f4a91c7e2b30 | AZ | CLUSTER_TASK flow=scale_out action=task_created component=materializer role=controller epoch=72 topology_version=72 task_count=2 notify_count=2 pending_count=2 result=success
2026-07-21T10:20:00.052204 | I | topology_task_executor.cpp:315 | 127.0.0.3 | 7447:7450 | cluster;f4a91c7e2b30 | AZ | CLUSTER_TASK flow=scale_out action=notify component=executor role=worker epoch=72 executor=127.0.0.3:31501 task_count=1 task_prefix=4c91 result=accepted
2026-07-21T10:20:00.052811 | I | topology_task_executor.cpp:315 | 127.0.0.4 | 7447:7450 | cluster;f4a91c7e2b30 | AZ | CLUSTER_TASK flow=scale_out action=notify component=executor role=worker epoch=72 executor=127.0.0.4:31501 task_count=1 task_prefix=9d12 result=accepted
2026-07-21T10:20:18.255480 | I | topology_controller.cpp:875 | 127.0.0.1 | 7447:7447 | cluster;f4a91c7e2b30 | AZ | CLUSTER_FLOW flow=scale_out action=finish component=controller role=controller epoch=72 participant_count=2 participants=[127.0.0.3:31501/ACTIVE,127.0.0.4:31501/ACTIVE] flow_duration_ms=18216 result=success
```

解读：

- 两个新节点都先发布 READY，controller 随后在 membership view 中看到两个 `INITIAL` topology member。
- controller 在 `CLUSTER_FLOW action=start` 的 `cluster;f4a91c7e2b30` trace 下生成一轮 `scale_out`，`participant_count=2` 证明这是多 worker 批次。
- `task_created` 后两个 executor 都收到 notify，说明任务生成和分发正常。
- `finish result=success flow_duration_ms=18216` 表示扩容耗时约 18.2 秒。

## 缩容示例

场景：`127.0.0.3:31501` 和 `127.0.0.4:31501` 同时退出 4 节点集群。

检索入口：

```text
AZ AND CLUSTER_ AND flow=scale_in
```

关键日志：

```text
2026-07-21T10:30:00.000101 | I | etcd_coordination_backend.cpp:134 | 127.0.0.3 | 7447:7447 | cluster;6ad9280bb312 | AZ | CLUSTER_MEMBERSHIP flow=membership action=lease_update component=backend role=worker node=127.0.0.3:31501 state=EXITING result=success reason=finished status=OK
2026-07-21T10:30:00.000220 | I | etcd_coordination_backend.cpp:134 | 127.0.0.4 | 7447:7447 | cluster;90ee1b447af8 | AZ | CLUSTER_MEMBERSHIP flow=membership action=lease_update component=backend role=worker node=127.0.0.4:31501 state=EXITING result=success reason=finished status=OK
2026-07-21T10:30:00.018512 | I | topology_controller.cpp:704 | 127.0.0.1 | 7447:7447 | cluster;5b39e74a8031 | AZ | CLUSTER_MEMBERSHIP_OBSERVED flow=membership action=view component=controller role=controller topology_version=79 member_count=4 state_counts=initial=0,joining=0,active=2,pre_leaving=2,leaving=0,failed=0 members=[127.0.0.1:31501/ACTIVE,127.0.0.2:31501/ACTIVE,127.0.0.3:31501/PRE_LEAVING,127.0.0.4:31501/PRE_LEAVING] result=changed
2026-07-21T10:30:00.038742 | I | topology_controller.cpp:819 | 127.0.0.1 | 7447:7447 | cluster;a0d2c8e91754 | AZ | CLUSTER_FLOW flow=scale_in action=start component=controller role=controller epoch=80 participant_count=2 participants=[127.0.0.3:31501/LEAVING,127.0.0.4:31501/LEAVING] result=accepted
2026-07-21T10:30:00.045100 | I | topology_task_materializer.cpp:291 | 127.0.0.1 | 7447:7447 | cluster;a0d2c8e91754 | AZ | CLUSTER_TASK flow=scale_in action=task_created component=materializer role=controller epoch=80 topology_version=80 task_count=4 notify_count=2 pending_count=4 result=success
2026-07-21T10:30:20.088880 | I | worker_topology_phase_callbacks.cpp:195 | 127.0.0.3 | 7447:7450 | cluster;a0d2c8e91754 | AZ | CLUSTER_SCALE_IN flow=scale_in action=data_drain component=callback role=worker epoch=80 source=127.0.0.3:31501 target=127.0.0.1:31501 executor=127.0.0.3:31501 operation_prefix=bf18 task_prefix=0e51 callback_phase=data_drain phase_duration_ms=19057 result=success reason=finished status=OK
2026-07-21T10:30:21.031204 | I | worker_topology_phase_callbacks.cpp:58 | 127.0.0.3 | 7447:7450 | cluster;a0d2c8e91754 | AZ | CLUSTER_SCALE_IN flow=scale_in action=metadata_handoff component=callback role=worker epoch=80 source=127.0.0.3:31501 target=127.0.0.1:31501 executor=127.0.0.3:31501 operation_prefix=bf18 task_prefix=0e51 callback_phase=metadata_handoff phase_duration_ms=942 result=success reason=finished status=OK
2026-07-21T10:30:21.062118 | I | topology_controller.cpp:766 | 127.0.0.1 | 7447:7447 | cluster;a0d2c8e91754 | AZ | CLUSTER_FLOW flow=scale_in action=wait component=controller role=controller epoch=80 topology_version=80 participant_count=2 participants=[127.0.0.3:31501/LEAVING,127.0.0.4:31501/LEAVING] flow_duration_ms=21023 result=waiting reason=external_termination
2026-07-21T10:30:24.900118 | I | topology_controller.cpp:875 | 127.0.0.1 | 7447:7447 | cluster;a0d2c8e91754 | AZ | CLUSTER_FLOW flow=scale_in action=finish component=controller role=controller epoch=80 participant_count=2 participants=[127.0.0.3:31501/REMOVED,127.0.0.4:31501/REMOVED] flow_duration_ms=24862 result=success
```

解读：

- 当前实现的回调顺序是先 data drain 再 metadata handoff；该样例中 data drain 阶段耗时约 19 秒，metadata handoff 随后快速完成。
- `wait reason=external_termination` 表示迁移任务已经完成，但 controller 仍在等待退出节点的外部终止事实。
- `finish result=success flow_duration_ms=24862` 表示本轮缩容整体耗时约 24.9 秒。

## 节点故障示例

场景：`127.0.0.3:31501` 和 `127.0.0.4:31501` 同时故障。

检索入口：

```text
AZ AND CLUSTER_ AND flow=failure
```

关键日志：

```text
2026-07-21T10:40:05.230742 | W | topology_controller.cpp:583 | 127.0.0.1 | 7447:7447 | cluster;c83a2fe014aa | AZ | CLUSTER_FLOW flow=failure action=start component=controller role=controller epoch=91 topology_version=90 participant_count=2 participants=[127.0.0.3:31501/FAILED,127.0.0.4:31501/FAILED] result=accepted reason=dead_timeout
2026-07-21T10:40:05.245100 | I | topology_task_materializer.cpp:291 | 127.0.0.1 | 7447:7447 | cluster;c83a2fe014aa | AZ | CLUSTER_TASK flow=failure action=task_created component=materializer role=controller epoch=91 topology_version=91 task_count=2 notify_count=2 pending_count=2 result=success
2026-07-21T10:40:17.899118 | I | topology_controller.cpp:858 | 127.0.0.1 | 7447:7447 | cluster;c83a2fe014aa | AZ | CLUSTER_FAILURE flow=failure action=finalizing component=controller role=controller epoch=91 topology_version=91 participant_count=2 participants=[127.0.0.3:31501/FAILED,127.0.0.4:31501/FAILED] result=pending
2026-07-21T10:40:17.900118 | I | topology_controller.cpp:866 | 127.0.0.1 | 7447:7447 | cluster;c83a2fe014aa | AZ | CLUSTER_FLOW flow=failure action=finish component=controller role=controller epoch=91 participant_count=2 participants=[127.0.0.3:31501/REMOVED,127.0.0.4:31501/REMOVED] flow_duration_ms=12669 result=success
```

解读：

- `action=start reason=dead_timeout` 表示 controller 已确认故障并生成 failure flow。
- `action=finalizing result=pending` 表示 failure final topology 正在提交；`action=finish result=success` 才是终点。
- 两个节点都进入同一轮 failure flow，`participant_count=2` 说明是多 worker 故障处理。

## backend 降级与恢复示例

检索入口：

```text
AZ AND CLUSTER_ AND flow=backend
```

关键日志：

```text
2026-07-21T10:50:00.000101 | W | topology_engine.cpp:502 | 127.0.0.2 | 7447:7450 | cluster;be37a91c0d55 | AZ | CLUSTER_BACKEND flow=backend action=access_failed component=backend role=worker backend=coordination backend_state=unavailable result=failed reason=backend_access_failed
2026-07-21T10:50:00.020101 | W | topology_engine.cpp:542 | 127.0.0.2 | 7447:7450 | cluster;be37a91c0d55 | AZ | CLUSTER_DEGRADED flow=backend action=state_change component=backend role=worker previous_level=NORMAL level=CONTROL_DEGRADED result=success reason=control_backend_unavailable
2026-07-21T10:50:05.120101 | I | topology_engine.cpp:457 | 127.0.0.2 | 7447:7450 | cluster;be37a91c0d55 | AZ | CLUSTER_BACKEND flow=backend action=recovered component=backend role=worker backend=coordination backend_state=available topology_version=63 topology_revision=10020 digest_prefix=cc9011 result=success
2026-07-21T10:50:05.121101 | I | topology_engine.cpp:542 | 127.0.0.2 | 7447:7450 | cluster;be37a91c0d55 | AZ | CLUSTER_DEGRADED flow=backend action=state_change component=backend role=worker previous_level=CONTROL_DEGRADED level=NORMAL result=success reason=backend_recovered
```

解读：同一时间窗口内如果扩缩容没有推进，先看是否存在 `backend_state=unavailable` 或 watch resync。backend 恢复后再继续看 task/callback 是否恢复推进。

## 过程中失败示例

检索入口：

```text
AZ AND CLUSTER_ AND result=failed
```

缩容 metadata handoff 或 data drain 失败：

```text
2026-07-21T11:00:06.048880 | W | worker_topology_phase_callbacks.cpp:57 | 127.0.0.4 | 7447:7450 | cluster;d0c18a4f92b7 | AZ | CLUSTER_SCALE_IN flow=scale_in action=metadata_handoff component=callback role=worker epoch=70 source=127.0.0.4:31501 target=127.0.0.2:31501 executor=127.0.0.4:31501 operation_prefix=bf18 task_prefix=0e51 callback_phase=metadata_handoff phase_duration_ms=5019 result=failed reason=metadata_failed status=K_RUNTIME_ERROR:Node exiting, change primary copy failed
2026-07-21T11:00:06.050980 | W | worker_topology_phase_callbacks.cpp:195 | 127.0.0.4 | 7447:7450 | cluster;d0c18a4f92b7 | AZ | CLUSTER_SCALE_IN flow=scale_in action=data_drain component=callback role=worker epoch=70 source=127.0.0.4:31501 target=127.0.0.2:31501 executor=127.0.0.4:31501 operation_prefix=bf18 task_prefix=0e51 callback_phase=data_drain phase_duration_ms=5019 result=failed reason=callback_failed status=K_RUNTIME_ERROR:Node exiting, change primary copy failed
2026-07-21T11:00:06.061010 | W | topology_task_executor.cpp:741 | 127.0.0.4 | 7447:7450 | cluster;d0c18a4f92b7 | AZ | CLUSTER_TASK flow=scale_in action=progress component=executor role=worker epoch=70 executor=127.0.0.4:31501 task_prefix=0e51 operation_prefix=bf18 result=failed reason=callback_failed retry=scheduled status=K_RUNTIME_ERROR:Node exiting, change primary copy failed
```

解读：`component=callback` 和 `component=executor` 使用相同 `operation_prefix`、`task_prefix`，并保留原始 `status`。这说明卡点在 metadata/data drain 回调执行链路，不是 controller 没有启动缩容。

扩容超时：

```text
2026-07-21T11:05:30.038742 | W | topology_controller.cpp:899 | 127.0.0.1 | 7447:7447 | cluster;e2c66ddf9011 | AZ | CLUSTER_FLOW flow=scale_out action=finish component=controller role=controller epoch=73 topology_version=73 participant_count=2 participants=[127.0.0.3:31501/JOINING,127.0.0.4:31501/JOINING] flow_duration_ms=30000 result=failed reason=scaleout_timeout
```

解读：本轮扩容到达 batch deadline 后失败，`participants` 保留仍未进入 ACTIVE 的 JOINING 节点。

controller 提交冲突：

```text
2026-07-21T11:06:01.038742 | W | topology_controller.cpp:1016 | 127.0.0.1 | 7447:7447 | cluster;e2c66ddf9011 | AZ | CLUSTER_CHANGE flow=scale_out action=commit component=controller role=controller expected_version=72 desired_version=73 observed_version=73 active_flow=scale_out active_epoch=73 member_count=4 state_counts=initial=0,joining=2,active=2,pre_leaving=0,leaving=0,failed=0 members=[127.0.0.1:31501/ACTIVE,127.0.0.2:31501/ACTIVE,127.0.0.3:31501/JOINING,127.0.0.4:31501/JOINING] cas_result=conflict result=failed reason=cas_conflict status=K_TRY_AGAIN:topology CAS lost to another Controller
```

解读：controller 已感知并尝试提交本轮变更，但 topology CAS 输给了另一个 controller 或更新者。先按同一时间窗口检索 `CLUSTER_CHANGE action=commit`，确认哪一个提交最终 `result=success`。

## 初始化、重启与退出示例

初始化：

```text
2026-07-21T09:00:00.018512 | I | topology_controller.cpp:942 | 127.0.0.1 | 7447:7447 | cluster;b71e23a9c018 | AZ | CLUSTER_FLOW flow=bootstrap action=start component=controller role=controller topology_version=1 participant_count=4 participants=[127.0.0.1:31501/INITIAL,127.0.0.2:31501/INITIAL,127.0.0.3:31501/INITIAL,127.0.0.4:31501/INITIAL] result=accepted reason=initial_placement
2026-07-21T09:00:00.024512 | I | topology_controller.cpp:995 | 127.0.0.1 | 7447:7447 | cluster;b71e23a9c018 | AZ | CLUSTER_CHANGE flow=bootstrap action=commit component=controller role=controller expected_version=0 committed_version=1 active_flow=none active_epoch=0 member_count=4 state_counts=initial=0,joining=0,active=4,pre_leaving=0,leaving=0,failed=0 members=[127.0.0.1:31501/ACTIVE,127.0.0.2:31501/ACTIVE,127.0.0.3:31501/ACTIVE,127.0.0.4:31501/ACTIVE] topology_revision=9001 digest_prefix=1d8f67 cas_result=committed result=success
2026-07-21T09:00:00.026512 | I | topology_controller.cpp:875 | 127.0.0.1 | 7447:7447 | cluster;b71e23a9c018 | AZ | CLUSTER_FLOW flow=bootstrap action=finish component=controller role=controller epoch=1 participant_count=4 participants=[127.0.0.1:31501/ACTIVE,127.0.0.2:31501/ACTIVE,127.0.0.3:31501/ACTIVE,127.0.0.4:31501/ACTIVE] flow_duration_ms=8 result=success
```

重启恢复：

```text
2026-07-21T09:10:00.000101 | I | etcd_coordination_backend.cpp:111 | 127.0.0.2 | 7447:7447 | cluster;9ac3b1178e44 | AZ | CLUSTER_MEMBERSHIP flow=restart action=lease_update component=backend role=worker node=127.0.0.2:31501 state=RESTARTING result=success reason=finished status=OK
2026-07-21T09:10:03.800101 | I | etcd_coordination_backend.cpp:145 | 127.0.0.2 | 7447:7447 | cluster;9ac3b1178e44 | AZ | CLUSTER_FLOW flow=restart action=reconciliation_done component=backend role=worker node=127.0.0.2:31501 result=success reason=finished status=OK
2026-07-21T09:10:03.810101 | I | etcd_coordination_backend.cpp:146 | 127.0.0.2 | 7447:7447 | cluster;9ac3b1178e44 | AZ | CLUSTER_MEMBERSHIP flow=restart action=lease_update component=backend role=worker node=127.0.0.2:31501 state=READY result=success reason=finished status=OK
```

全集群退出：

```text
2026-07-21T11:20:00.018512 | I | topology_controller.cpp:478 | 127.0.0.1 | 7447:7447 | cluster;a9e1027cb681 | AZ | CLUSTER_FLOW flow=shutdown action=start component=controller role=controller participant_count=4 participants=[127.0.0.1:31501/LEAVING,127.0.0.2:31501/LEAVING,127.0.0.3:31501/LEAVING,127.0.0.4:31501/LEAVING] result=accepted
2026-07-21T11:20:00.068512 | I | topology_controller.cpp:478 | 127.0.0.1 | 7447:7447 | cluster;a9e1027cb681 | AZ | CLUSTER_SHUTDOWN flow=shutdown action=finish component=controller role=controller participant_count=4 participants=[127.0.0.1:31501/REMOVED,127.0.0.2:31501/REMOVED,127.0.0.3:31501/REMOVED,127.0.0.4:31501/REMOVED] flow_duration_ms=50 result=success
```

## 开发定位模板

先用场景入口找到 prefix trace，再按 component 下钻：

```text
cluster;<uuid_suffix> AND component=controller
cluster;<uuid_suffix> AND component=materializer
cluster;<uuid_suffix> AND component=executor
cluster;<uuid_suffix> AND component=callback
cluster;<uuid_suffix> AND component=backend
```

交接给开发时建议保留：

```text
trace：cluster;<uuid_suffix>
flow/action：<flow>/<last_action>
component：<last_component>
topology：version=<topology_version>, revision=<topology_revision>, digest_prefix=<digest_prefix>
batch：epoch=<epoch>, active_flow=<flow>, previous_flow=<flow or none>
task：task_prefix=<id>, task_count=<n>, notify_count=<n>, pending_count=<n>, retry=<scheduled|exhausted>
callback：callback_phase=<phase>, operation_prefix=<id>, attempt=<n>, next_backoff_ms=<ms>, deadline_ms=<ms>
backend：backend=coordination, backend_state=<state>
error：result=<result>, reason=<reason>, status=<raw status>
判断：<controller/materializer/executor/callback/backend 哪一层最可疑，以及证据>
```

## 常见判断

| 现象 | 优先看什么 |
|---|---|
| 没有 flow start | 看 `CLUSTER_MEMBERSHIP action=lease_update` 是否已发布 READY/EXITING，以及 `CLUSTER_MEMBERSHIP_OBSERVED` 是否已看到 INITIAL/PRE_LEAVING/FAILED。 |
| 有 start 没有 task_created | 看 `component=controller` 的 `CLUSTER_CHANGE` 是否 CAS 成功。 |
| 有 task_created 没有 notify | 看 `CLUSTER_TASK action=task_created` 的 `notify_count` 和 watch/backend 日志。 |
| 有 notify 没有 progress | 看 executor 节点是否存活，以及同 trace 下是否有 `CLUSTER_BACKEND` 降级。 |
| 缩容 metadata 快但 data 慢 | 看 `CLUSTER_SCALE_IN action=data_drain phase_duration_ms`。 |
| 缩容出现 `Node exiting, change primary copy failed` | 按 `operation_prefix` 和 `task_prefix` 串 `component=callback` 与 `component=executor`。 |
| 故障处理打断扩缩容 | 看同一时间窗口内 `flow=failure action=start`，再回查前一轮 `scale_out` 或 `scale_in` 的最后一条 `CLUSTER_FLOW`。 |
| result 是 pending | 表示当前阶段已经开始但还不是终态，例如 failure finalizing 正在提交。 |
