# Object Cache Eviction Design

## Document Metadata

- Status:
  - `active`
- Design scope:
  - current implementation
- Primary code paths:
  - `src/datasystem/worker/object_cache/worker_oc_eviction_manager.*`
  - `src/datasystem/worker/object_cache/eviction_list.*`
  - `src/datasystem/worker/object_cache/worker_oc_spill.*`
  - `src/datasystem/worker/object_cache/obj_cache_shm_unit.cpp`
  - `src/datasystem/worker/stream_cache/worker_sc_allocate_memory.cpp`
  - `src/datasystem/worker/object_cache/worker_master_oc_api.*`
  - `src/datasystem/master/object_cache/oc_metadata_manager.*`
  - `src/datasystem/protos/master_object.proto`
- Primary source-of-truth files:
  - `worker_oc_eviction_manager.cpp`
  - `worker_oc_eviction_manager.h`
  - `eviction_list.cpp`
  - `worker_oc_spill.cpp`
  - `oc_metadata_manager.cpp`
  - `master_object.proto`
- Last verified against source:
  - `2026-06-03`
- Related context docs:
  - `.repo_context/modules/runtime/object-cache-eviction/README.md`
  - `.repo_context/modules/runtime/worker-runtime.md`
  - `.repo_context/modules/runtime/cluster-management.md`
  - `.repo_context/modules/infra/l2cache/README.md`
  - `.repo_context/modules/infra/slot/design.md`
  - `.repo_context/modules/quality/tests-and-reproduction.md`
- Related playbooks:
  - `.repo_context/playbooks/features/runtime/object-cache-eviction/implementation.md`
- Related user-facing or internal docs:
  - Detailed Chinese design notes are kept out of the repository and carried in the PR description or local workspace
    notes.

## Purpose

- Why this design document exists:
  - 解释 worker object cache eviction 的触发、候选选择、动作状态机、对象锁和 master metadata 清理关系。
- What problem this module solves:
  - 在共享内存或 spill 空间接近水位时，尽量释放 worker 内存或磁盘空间，同时维护 L2、master metadata 和对象生命周期语义。
- Who or what depends on this module:
  - object cache create/get/update 路径、stream cache 分配路径、L2/cache mode 语义、master metadata、spill 文件管理、远端迁移。

## Business And Scenario Overview

- Why this capability is needed:
  - object cache 和 stream cache 共用 worker 资源；后台 eviction 用于降低前台分配失败概率。
- Target users, callers, or operators:
  - worker object cache、stream cache、运维侧容量配置和性能排障。
- Typical usage time or trigger conditions:
  - object 分配前或 OOM 重试时；
  - stream 分配前或 OOM 重试时；
  - schedule thread 每 10 秒巡检 object/stream 水位；
  - spill 写入前发现 spill 空间超过高水位。
- Typical deployment or runtime environment:
  - 单 worker 或多 worker 集群，可能启用本地 spill、远端 memory spill、L2 cache 或 shared disk/slot。
- Expected user or operator experience:
  - 前台请求在压力下仍尽可能通过后台释放空间继续执行；
  - eviction 慢时应能从日志、perf point、trace 和测试入口定位到 master RPC、spill I/O、对象锁或候选队列。

## Goals

- 在 object/stream 内存超过高水位时释放到低水位附近。
- 维护 primary copy、L2 copy、none-L2-evict 对象生命周期和 master metadata 一致性。
- 对 L2 已有可恢复副本的对象优先做本地 copy 删除。
- 对可 spill 对象异步写盘或远端迁移，减少主 eviction loop 阻塞。
- 对失败候选保留重试机会，避免对象永远失去 eviction 资格。

## Non-Goals

- 不定义 L2 backend 的持久格式或 slot recovery 规则。
- 不定义 hash-ring 或 cluster-manager 的 worker 路由协议。
- 不通过 `eviction_thread_num` 暴露 memory eviction 并发调优；该 flag 已删除，主 eviction 并发仍由 `isDone_` 单任务门闩约束。
- 不保证 `DeleteAllCopyMetaReqPb.async_delete` 会让 worker 侧 RPC 异步化。

## Scope

- In scope:
  - memory eviction list；
  - memory eviction action state machine；
  - spill write and spill eviction；
  - master metadata cleanup calls used by eviction；
  - eviction thread/config/test surface。
- Out of scope:
  - client API behavior beyond observable tests；
  - L2 backend implementation；
  - hash-ring token allocation和扩缩容任务；
  - ExpiredObjectManager 内部过期扫描算法。
- Important boundaries with neighboring modules:
  - worker runtime 负责启动 manager；eviction 负责压力下的对象处置。
  - L2/slot 负责数据可恢复性；eviction 只读取 write mode、writeback done 和 cache type。
  - master metadata 负责全局元数据；eviction 通过 `RemoveMeta` 或 `DeleteAllCopyMeta` 请求修改它。

## Terminology

| Term | Meaning in this repository | Source or note |
| --- | --- | --- |
| `EvictionList` | 带计数的候选队列，`FindEvictCandidate` 对非零 counter 做 second chance | `eviction_list.*` |
| `Q1` / `Q2` / `READD_COUNTER` | 初始/重试 counter。无 worker ref 通常 `Q1`，有 worker ref `Q2`，锁失败重加 `READD_COUNTER` | `eviction_list.h` |
| `DELETE` | 删除本地 copy 并异步向 master 移除本 worker copy metadata | `EvictObject`, `SubmitAsyncMasterTask` |
| `FREE_MEMORY` | 对已 spill 的对象释放本地 shm，保留 object table 和 spill 状态 | `EvictObject` |
| `SPILL` | 本地写 spill 文件，成功后释放内存并标记 spilled | `TryEvictObject`, `SpillImpl` |
| `MIGRATE` | 通过 `DataMigrator` 将对象迁移到其他 worker 内存 | `MigrateData` |
| `END_LIFE` | 对 none-L2-evict 或 write-back-l2-cache-evict 对象调用 `DeleteAllCopyMeta`，结束对象生命周期 | primary end-life lane, `DeleteNoneL2CacheEvictableObject` fallback |
| `async_delete` | `DeleteAllCopyMetaReqPb` 请求字段；为 true 时 Master 将 key 加入 `ExpiredObjectManager` 后返回 | `master_object.proto`, `OCMetadataManager::DeleteAllCopyMetaImpl` |

## Current State And Design Choice Notes

- Current implementation or baseline behavior:
  - `MemEvictionThread` 固定为源码内部单线程，不再暴露 `eviction_thread_num`。
  - `WorkerOcEvictionManager::Evict` 用 `isDone_.compare_exchange_strong` 保证一个 manager 同时只有一个 `EvictionTask`。
  - `EvictionTask` 在主循环里同步决定每个对象动作；本地 spill 和 master `RemoveMeta` 被异步化，主 loop 中的 `END_LIFE` 当前投递到 primary end-life lane。
  - `EvictSpilledObjects` 和 `SpillImpl` no-space fallback 仍同步调用 `DeleteNoneL2CacheEvictableObject`；该路径构造 `DeleteAllCopyMetaReqPb` 时使用 `object_keys`、本地 worker address 和 `redirect=true`。
  - `eviction_thread_num` 已删除，且 dscli 默认配置、k8s deployment、k8s daemonset Helm values/template、中文部署文档和示例需要同步清理该参数。
  - 当前正式方案将 memory eviction 主 loop 的所有 `Action::END_LIFE` 投递到
    `primaryEndLifeThreadPool_`；`PRIMARY_END_LIFE_THREAD_NUM=4` 个 drain 线程同步发送 RPC，请求
    `async_delete=true` 时 Master 入队后快速返回，`EvictSpilledObjects` 和 `SpillImpl` no-space fallback
    保持同步。
- Relevant constraints from current release or deployment:
  - 对象 table、shm、spill state、master metadata 之间依赖 create time/version 防止误删。
  - eviction 是后台路径，但被 allocation retry 触发时会直接影响前台延迟和成功率。
- Selected approach and why:
  - 主 eviction loop 保持单任务，降低候选队列和全局水位并发复杂度。
  - 耗时 spill I/O 进入 `SpillThread`；本地 copy metadata 删除进入 `MasterTaskThread`。
  - `NONE_L2_CACHE_EVICT` 没有 L2 可恢复副本，primary copy 必须先让 master 进入全局删除语义，再释放本地对象。
  - primary end-life 在短锁校验后释放对象锁，由 drain 线程发送带 `async_delete=true` 的
    `DeleteAllCopyMeta`；Master 入队成功返回后重新获取对象锁并复核 version，再执行本地 erase，不引入
    前台可见 pending 状态。
- Known tradeoffs:
  - 历史 `END_LIFE` 同步 RPC 会阻塞 `EvictionTask`；当前主 loop 已通过 primary end-life lane 隔离该瓶颈。
  - primary end-life lane 不在对象写锁内等待 `DeleteAllCopyMeta`；同步 fallback 仍会阻塞同 key 并发操作。
  - 单任务门闩让单纯增加 `MemEvictionThread` 数量在当前主流程中基本没有吞吐收益，因此不再暴露 `eviction_thread_num`。
  - primary end-life lane 入队成功只表示任务已接管，不表示内存已经释放；实际释放仍发生在 lane 成功 erase 本地对象之后。

## Architecture Overview

- High-level structure:
  - 触发层：object/stream allocator 和定时巡检。
  - 候选层：`EvictionList` 保存可驱逐对象。
  - 决策层：`GetObjectNextAction` 选择动作。
  - 执行层：本地 erase/free/spill/migrate/master delete。
  - 回收层：spill eviction list 和 compaction。
- Key runtime roles or processes:
  - `MemEvictionThread`
  - `SpillEvictionThread`
  - `MasterTaskThread`
  - `SpillThread`
  - `scheduleEvictThread`
- Key persistent state, if any:
  - spill 文件和 spill 文件索引；
  - master metadata；
  - ExpiredObjectManager 队列；
  - L2/slot 中的可恢复对象数据。
- Key control-flow or data-flow stages:
  - high watermark trigger -> candidate -> object lock -> action -> local/master/spill effects -> failed candidate re-add。

## Entry Points, External Interfaces, And Integration Points

- Public APIs:
  - `WorkerOcEvictionManager::Add`
  - `WorkerOcEvictionManager::Erase`
  - `WorkerOcEvictionManager::Evict`
  - `WorkerOcEvictionManager::EvictClearObject`
  - `WorkerOcEvictionManager::TryEvictSpilledObjects`
  - `EvictWhenMemoryExceedThrehold`
- Internal service entrypoints:
  - `EvictionTask`
  - `TryEvictObject`
  - `SpillImpl`
  - `BatchSpillImpl`
  - `EvictSpilledObjects`
  - `DeleteNoneL2CacheEvictableObject`
- External protocols, schemas, or data formats:
  - `RemoveMetaReqPb`
  - `DeleteAllCopyMetaReqPb`
  - `DeleteAllCopyMetaReqPb.object_keys`
  - `DeleteAllCopyMetaReqPb.ids_with_version`
- CLI commands, config flags, or environment variables:
  - see configuration model below.
- Background jobs, threads, or callbacks:
  - schedule eviction loop；
  - memory eviction task；
  - spill task futures；
  - async master metadata task；
  - spill compaction thread。
- Cross-module integration points:
  - shared-memory allocator；
  - object table and object lock；
  - L2/cache write mode；
  - ETCD cluster manager for metadata address；
  - master metadata manager；
  - ExpiredObjectManager；
  - data migrator。

## Core Components

| Component | Responsibility | Key files | Notes |
| --- | --- | --- | --- |
| `WorkerOcEvictionManager` | 主 eviction 状态机、线程池、candidate 处理、master/spill/migrate 调度 | `worker_oc_eviction_manager.*` | 当前一个 manager 同时只跑一个 `EvictionTask` |
| `EvictionList` | 候选对象 second-chance 队列 | `eviction_list.*` | counter 递减到 0 才返回候选 |
| `WorkerOcSpill` | 本地 spill 文件写入、读取、删除、容量判断、compaction | `worker_oc_spill.*` | `spill_thread_num` 决定 file manager 数量和写入并行度 |
| `WorkerMasterOCApi` | worker 到 master 的 metadata RPC | `worker_master_oc_api.*` | remote API 有 retry，local API 直接调 master service |
| `OCMetadataManager` | master 侧 `RemoveMeta`/`DeleteAllCopyMeta` 处理 | `oc_metadata_manager.*` | `async_delete` 入队 ExpiredObjectManager |
| `ObjectGlobalRefTable` | 判断 object 是否仍被 worker/client 引用 | object cache ref table | `Add` 时影响 Q1/Q2 |
| `DataMigrator` | 远端 memory spill/migrate | `data_migrator/*` | 仅在 `spill_to_remote_worker` 且目标有足够内存时进入 |

## Main Flows

### Memory Pressure Trigger

1. object 分配调用 `AllocateMemoryForObject`，在 `retryOnOOM` 时先调用 `EvictWhenMemoryExceedThrehold`。
2. stream 分配调用 `WorkerSCAllocateMemory::AllocateMemoryForStream`，在 stream pressure 下也触发 object eviction。
3. `WorkerOcEvictionManager::Init` 启动 `scheduleEvictThread`，每 10 秒检查 object 和 stream 水位。
4. object 高水位阈值为 `max(maxAvailableMemory * 0.9, maxAvailableMemory - eviction_reserve_mem_threshold_mb)`。
5. eviction loop 运行到 `IsAboveLowWaterMark` 不再满足或候选队列为空；低水位因子是 0.8。

Key files:

- `obj_cache_shm_unit.cpp`
- `worker_sc_allocate_memory.cpp`
- `worker_oc_eviction_manager.cpp`

Failure-sensitive steps:

- 高水位触发发生在前台分配路径附近，eviction 慢会增加分配重试和 OOM 暴露概率。
- `Evict` 已有任务运行时只打印 `Evict is going on...`，不会排队第二个主 eviction task。

### Candidate Selection

1. object 被加入 manager 时，`Add` 查询 global ref count。
2. 无 worker ref 的对象以 `Q1` 加入，有 worker ref 的对象以 `Q2` 加入。
3. `EvictionList::FindEvictCandidate` 从 `oldest_` 开始扫描。
4. counter 非 0 的节点被递减并获得 second chance。
5. counter 为 0 的节点成为候选。
6. 候选拿不到对象写锁时，从 list 移除并以 `READD_COUNTER` 放入失败列表，task 结束时再加入。

Key files:

- `eviction_list.cpp`
- `worker_oc_eviction_manager.cpp`

Failure-sensitive steps:

- `EvictionList` 自身有锁保护，但对象状态由 `SafeObjType` 锁保护；不能只依赖 list membership 判断对象可删。
- 重试 counter 太高会降低短期释放效率，太低会增加热点对象锁竞争。

### Memory Eviction Task

1. `Evict` 通过 `isDone_` 门闩提交 `EvictionTask` 到 `MemEvictionThread`。
2. `EvictionTask` 找候选并 `TryWLock` 对象。
3. `IsObjectEvictable` 确认对象仍在 eviction list，binary 对象仍有 shm，非 binary 无 L2 状态不会被继续处理。
4. `GetObjectNextAction` 选择动作。
5. `TryEvictObject` 调 `EvictObject` 执行动作，并对 `SPILL`/`MIGRATE` 建立 async task。
6. `DELETE` 对象被聚合，达到 300 个或 10 ms 后提交到 `MasterTaskThread` 执行 `RemoveMeta`。
7. 主 loop 非阻塞回收已完成的 spill futures；结束时等待剩余 spill futures。
8. 失败对象按 counter 回填 eviction list，`isDone_` 置回 true。

Key files:

- `worker_oc_eviction_manager.cpp`

Failure-sensitive steps:

- `ObjectKV objectKV(candidateId, *entry)` 依赖 `entry` 已经存在；维护代码时要保持错误分支不会解引用空 entry。
- batch `RemoveMeta` 是异步的；本地 object table 已经删除，metadata 清理失败只会记录日志并在 master task 内重试。
- memory eviction 主 loop 中的 `END_LIFE` 当前投递到独立 primary end-life lane，不走 `MasterTaskThread`；
  `EvictSpilledObjects` 和 `SpillImpl` no-space fallback 保留同步 `DeleteAllCopyMeta`。

### Action State Machine

| Condition | Action | Current effect |
| --- | --- | --- |
| not primary copy | `DELETE` | 删除本地 copy，异步 `RemoveMeta` |
| `CacheType::DISK` and has L2 | `DELETE` | 删除本地 copy，异步 `RemoveMeta` |
| `CacheType::DISK` and `NONE_L2_CACHE_EVICT` | `END_LIFE` | memory eviction 主 loop 投递 primary lane；spill fallback 仍同步 `DeleteAllCopyMeta` |
| `CacheType::DISK` without L2 and not evict type | `RETAIN` | 保留 |
| already spilled | `FREE_MEMORY` | 释放 shm |
| remote spill enabled and peer memory enough | `MIGRATE` | batch 远端迁移 |
| local spill enabled and spill not too full | `SPILL` | 异步本地 spill |
| local spill enabled, spill nearly full, object has L2 | `DELETE` | 删除本地 copy |
| no spill and write-back-l2-cache-evict | `END_LIFE` | memory eviction 主 loop 投递 primary lane；lane 在 Master 接受或连续三次 RPC 通信失败后执行本地删除，成功后移除 async send |
| no spill and has L2 | `DELETE` | 删除本地 copy |
| no spill and `NONE_L2_CACHE_EVICT` | `END_LIFE` | memory eviction 主 loop 投递 primary lane |
| otherwise | `RETAIN` | 保留 |

Key files:

- `GetObjectNextAction`
- `EvictObject`

Failure-sensitive steps:

- `NONE_L2_CACHE_EVICT` 的 primary copy 正常路径不能直接本地 erase；可用性优先策略仅在同一 batch
  连续三次 `DeleteAllCopyMeta` RPC 通信失败后允许强制释放，并记录 ERROR。
- L2 已存在的判断对 write-back 依赖 `stateInfo.IsWriteBackDone()`。

### Synchronous `NONE_L2_CACHE_EVICT` Fallback Path

1. `EvictSpilledObjects` 选中已 spill 的 none-L2-evict 候选并持有对象写锁，或 `SpillImpl` no-space
   fallback 在重新取得对象写锁后处理 none-L2-evict 对象。
2. 这两个 fallback 路径直接调用 `DeleteNoneL2CacheEvictableObject`，不经过 memory eviction 主 loop 的
   `EvictObject` primary lane 提交。
3. 该函数通过 `ClusterManager` 查询 object key 的 metadata master 地址。
4. worker 创建 `WorkerMasterOCApi` 并同步调用 `DeleteAllCopyMeta`。
5. 该同步 fallback 请求携带 `object_keys`、本地 worker address 和 `redirect=true`；primary end-life
   lane 使用 `ids_with_version`，但该 fallback 仍保留历史 `object_keys` 形态。
6. master 端 `DeleteAllCopyMetaImpl` 合并 `object_keys` 和 `ids_with_version`，再放入 `DeleteObjectMediator`。
7. 当前 fallback 请求不设置 `need_forward_objs_without_meta`，因此 master 走 `FindNeedDeleteIds` /
   `NotifyDeleteAndClearMeta` 同步删除元数据。
8. worker 通过 `CollectDeleteAllCopyMetaResult` 检查 `last_rc`、per-key failure、outdated、
   objs-without-meta 和 redirect/meta-moving 信息，确认成功后删除 spill 文件，清除 spill state，并从本地
   object table erase。

Key files:

- `worker_oc_eviction_manager.cpp`
- `worker_master_oc_api.cpp`
- `master_object.proto`
- `oc_metadata_manager.cpp`

Failure-sensitive steps:

- fallback RPC 是同步等待；即使 master 端删除入队，RPC 往返和 master 处理延迟仍会阻塞对应 fallback
  eviction task。
- fallback 调用发生在对象写锁持有期间；锁用于串行化对象状态、spill state、object table erase 和
  create-time 版本，但也会扩大锁持有时间。
- 正式异步化方案不新增前台可见 pending-delete 状态；它在 eviction manager 内部维护 `objectKey -> version` pending，lane 执行前重新拿对象 WLock 并复核 version/action。

### Local Spill Path

1. action 为 `SPILL` 时，`TryEvictObject` 记录对象大小和 create time。
2. 主 loop 释放对象写锁，提交 `SubmitSpillTask` 到 `SpillThread`。
3. `SpillImpl` 按版本读锁对象，latch shm，判断对象是否可在 spill 满时被淘汰。
4. 释放读锁后调用 `WorkerOcSpill::Spill` 写本地文件。
5. 如果 `NONE_L2_CACHE_EVICT` 对象遇到 `K_NO_SPACE`，重新写锁并走 `DeleteNoneL2CacheEvictableObject`。
6. spill 成功后，按版本重新写锁对象；若版本变化或锁失败，删除刚写入的 spill 文件做回滚。
7. 成功重锁后释放 shm 并设置 spill state。

Key files:

- `worker_oc_eviction_manager.cpp`
- `worker_oc_spill.cpp`

Failure-sensitive steps:

- I/O 前释放锁减少阻塞，但必须用 create time 防止 spill 老版本。
- `NONE_L2_CACHE_EVICT` spill 满时的 fallback 仍会同步调用 master。

### Remote Migrate Path

1. action 为 `MIGRATE` 时，`TryEvictObject` 把对象加入当前 batch task。
2. batch 达到 512 个对象时提交 `SubmitBatchSpillTask`。
3. `BatchSpillImpl` 仅支持 `spill_to_remote_worker=true`，调用 `MigrateData`。
4. `DataMigrator` 按 object keys 和 size 执行迁移，并返回失败 key。
5. batch 失败 key 重新加入 eviction list。

Key files:

- `worker_oc_eviction_manager.cpp`
- `data_migrator/*`

Failure-sensitive steps:

- batch task id 是 thread-local-ish 的 `evictSpillTaskId`；修改并发模型前必须重新审视该状态归属。
- 远端迁移依赖 `NodeSelector::HasEnoughAvailableMemory` 和 cluster manager 可用性。

### Spill Space Eviction

1. `SpillImpl` 写入前调用 `TryEvictSpilledObjects(dataSize)`。
2. 若 spill 空间超过高水位且没有运行中的 spill eviction task，则提交单线程 `EvictSpilledObjects`。
3. spill eviction list 同样用 `EvictionList` 选择候选。
4. 对象必须拿到写锁，且满足 write-through、write-back done 或 `NONE_L2_CACHE_EVICT` 才可淘汰。
5. `NONE_L2_CACHE_EVICT` 调 `DeleteNoneL2CacheEvictableObject`；其他 L2 可恢复对象调用 `DeleteL2CacheEvictableObject` 删除 spill 文件并清除 spill state。
6. 删除后如仍接近水位，触发 `WorkerOcSpill::ForceCompact`。

Key files:

- `worker_oc_eviction_manager.cpp`
- `worker_oc_spill.cpp`

Failure-sensitive steps:

- write-back 未完成的对象不能从 spill 删除。
- `DeleteNoneL2CacheEvictableObject` 仍是同步 master RPC，并在 spill eviction 线程中持有对象写锁执行。

## Data And State Model

- Important in-memory state:
  - `memEvictionList_`: memory eviction candidates。
  - `spillEvictionList_`: spill 文件 eviction candidates。
  - `isDone_`: 主 memory eviction 是否空闲。
  - `objectTable_`: local object metadata and `SafeObjType` entries。
  - `pendingSpillSize`: 当前 task 内已提交但未释放的 spill/migrate size。
  - `deletedObjects`: `DELETE` batch 中待异步 `RemoveMeta` 的 key/version。
  - `pendingPrimaryEndLifeObjects_`: primary end-life lane 的内部去重和背压状态，key 为 `objectKey`，value 为 `entry->GetCreateTime()`。
- Important on-disk or external state:
  - spill directory and files；
  - master metadata；
  - ExpiredObjectManager async delete queue；
  - L2 backend data。
- Ownership and lifecycle rules:
  - `ObjectTable` owns local object entries；`DELETE` and `END_LIFE` can erase it。
  - `WorkerOcSpill` owns spill files；object entry holds spill state。
  - master metadata is authoritative for global object/copy visibility。
- Concurrency or thread-affinity rules:
  - 主 eviction 同一时间一个 `EvictionTask`。
  - spill write futures run on `SpillThread`。
  - `RemoveMeta` runs on `MasterTaskThread`。
  - spill eviction runs on one `SpillEvictionThread` task at a time。
  - object mutation requires `SafeObjType` write lock；spill read path uses read lock plus shm latch。
  - primary end-life lane 使用源码内固定的 4 线程池；不增加独立 RPC 线程，不复用
    `MasterTaskThread`，也不增加用户可见线程数配置。
- Ordering requirements or invariants:
  - 先决定 action，再做 local/master/spill effects。
  - `SPILL` 必须先持有 shm guard 并写成功，再释放 shm 和设置 spill state。
  - `END_LIFE` 必须先取得 master 删除成功响应，再删除本地 object table。
  - `DELETE` 可以先删本地 copy，再异步清理本 worker copy metadata。

## External Interaction And Dependency Analysis

- Dependency graph summary:
  - allocator -> eviction manager -> object table/object lock -> local memory/spill or master metadata。
- Critical upstream services or modules:
  - shared-memory allocator；
  - object cache create/update/get；
  - stream cache allocator。
- Critical downstream services or modules:
  - master metadata service；
  - ETCD cluster manager metadata routing；
  - local filesystem；
  - L2 backend。
- Failure impact from each critical dependency:
  - master/RPC slow: primary end-life lane 或同步 fallback 变慢；memory eviction 主 loop 仍可继续处理 local copy，
    `DELETE` metadata cleanup may lag。
  - ETCD metadata address unavailable: `DeleteNoneL2CacheEvictableObject` fails and candidate re-adds。
  - spill filesystem full/slow: spill futures fail or delay，主 loop pending size accounting影响水位判断。
  - object lock busy: candidate delayed with `READD_COUNTER`。
- Version, protocol, or schema coupling:
  - `ids_with_version` protects `DeleteAllCopyMeta` from deleting newer object incarnation。
  - `async_delete` is protobuf field 8；comment defines master-side enqueue semantics。
- Deployment or operations dependencies:
  - spill directory capacity and fd limit；
  - memory reserve threshold；
  - master availability；
  - cluster routing correctness。

## Open Source Software Selection And Dependency Record

| Dependency | Purpose in this module | Why selected | Alternatives considered | License | Version / upgrade strategy | Security / maintenance notes |
| --- | --- | --- | --- | --- | --- | --- |
| oneTBB concurrent hash map / spin rw mutex | `EvictionList` index and list protection | already used in repository common runtime | std mutex/map would change performance profile | existing repository dependency | repository-level third-party strategy | avoid extending lock hold time around object/RPC work |

## Configuration Model

| Config | Type | Default or source | Effect | Risk if changed |
| --- | --- | --- | --- | --- |
| `eviction_reserve_mem_threshold_mb` | uint32 | default 10240 | 高水位阈值保留内存 | 过小会晚驱逐，过大可能过早驱逐 |
| `spill_directory` | string | empty | 非空启用本地 spill | 空表示无法本地 spill，更多对象走 delete/end-life/retain |
| `spill_size_limit` | uint64 | 0 | spill 容量上限；0 用启动时空闲空间 95% | 过大可能挤压磁盘，过小增加 no-space |
| `spill_thread_num` | uint32 | default 8 | spill 写文件并行度和 file manager 数量 | 过高增加 CPU/I/O/fd 压力 |
| `spill_file_max_size_mb` | uint64 | default 200 | 单 spill 文件大小上限 | 影响 inode 与 compact 行为 |
| `spill_file_open_limit` | uint64 | default 512 | spill fd 上限 | 过高可能超过系统限制，过低增加 reopen |
| `spill_enable_readahead` | bool | true | spill 读取 readahead | offset read 场景可能读放大 |
| `spill_to_remote_worker` | bool | false | 允许远端 memory migrate | 引入跨 worker 迁移失败和路由依赖 |

## Availability

- Availability goals or service-level expectations:
  - eviction 应作为后台压力释放机制，避免前台分配路径长期不可用。
- Deployment topology and redundancy model:
  - 单 worker eviction 只释放本 worker 资源；多 worker 下 master metadata 和远端 migrate 引入跨节点依赖。
- Single-point-of-failure analysis:
  - master metadata 对 `END_LIFE` 是同步依赖。
  - 本地 spill 目录对 `SPILL` 是同步 I/O 依赖。
- Failure domains and blast-radius limits:
  - `DELETE` metadata cleanup 失败影响 master copy metadata 收敛，不应恢复本地已删对象。
  - `END_LIFE` 路由或 Master 业务失败时重加；同一 batch 连续三次 RPC 通信失败后强制删除本地对象。
- Capacity reservation or headroom assumptions:
  - object 高水位 0.9，低水位 0.8。
  - spill 高/低水位由 `WorkerOcSpill` factor 控制；active spill size 低水位用于删除 spill 文件。

## Reliability

- Expected failure modes:
  - object lock busy；
  - master RPC failure；
  - metadata moving or failed ids；
  - spill no space；
  - version changed during async spill；
  - local object already removed。
- Data consistency, durability, or correctness requirements:
  - 不能删除新版本对象 metadata。
  - write-back 未完成前不能把唯一可恢复副本删除。
  - none-L2-evict 对象生命周期结束必须通过 master 全局删除语义。
- Retry, idempotency, deduplication, or exactly-once assumptions:
  - `RemoveMetaFromMasterForEviction` 在 `AsyncMasterTask` 内最多重试 3 次。
  - eviction metadata cleanup 的单次 pass 最多执行初始 owner 请求和每个首跳 redirect target 的一次转发；
    转发请求禁用 redirect，目标再次返回 redirect 时将该组对象留待下一次 pass，禁止递归形成 A/B 循环。
  - master `AsyncDeleteByExpired` 对已删除中的对象把 `K_TRY_AGAIN` 视为成功。
  - lock/version 失败的 spill task 会回滚或重试。
- What must remain true after failure:
  - 失败候选应保留重试资格；
  - local shm 不应在 spill 失败时释放；
  - newer object incarnation 不应被旧 eviction 删除。

## Resilience

- Overload protection, rate limiting, or admission control:
  - 主 eviction 单任务门闩限制后台并发。
  - `DELETE` metadata cleanup batch size 300 或 10 ms flush。
  - remote migrate batch threshold 512。
- Timeout, circuit-breaker, and backpressure strategy:
  - source-visible timeout mainly在 RPC layer；primary end-life lane 使用固定 pending 上限，每次
    `DeleteAllCopyMeta` 调用使用 1s API 总预算，RPC 通信错误最多调用三轮，但没有新增用户可见队列超时
    或熔断 flag。
- Dependency failure handling:
  - master metadata cleanup失败日志记录并对部分路径重试；
  - spill no-space 对 none-L2-evict 可降级为 end-life delete；
  - lock busy重加候选。
- Safe rollback, feature-gating, or kill-switch strategy:
  - 关闭本地 spill: empty `spill_directory`。
  - 关闭远端 spill: `spill_to_remote_worker=false`。

## Security, Privacy, And Safety Constraints

- Authn/authz expectations:
  - worker-master RPC 使用 `AkSkManager` 初始化 API 并签名请求。
- Input validation requirements:
  - config flag validator 校验 eviction/spill 数值范围和路径。
- Secrets, credentials, or key-management requirements:
  - 文档和测试不应记录真实 AK/SK；测试中使用固定 fake key。
- Resource or isolation constraints:
  - spill 路径、fd、磁盘容量和 shared memory 都是可耗尽资源。
- Unsafe changes to avoid:
  - 绕过 `ids_with_version`；
  - 在未确认 L2/writeback done 时删除唯一副本；
  - 在对象锁外修改 object table 或 spill state。

## Observability

- Main logs:
  - `Eviction start.`
  - `Evict is going on...`
  - `EvictionList size before/after evict`
  - `PRIMARY_END_LIFE_DIAG stage=<stage>`：正常阶段使用 `VLOG(1)`；阶段耗时或 queue wait 达到 100 ms、
    RPC 错误或 per-key 失败使用 `LOG(WARNING)`。阶段覆盖 eviction summary、dequeue、route grouping、
    candidate prepare、RPC attempt、local cleanup 和整批 drain。
  - eviction summary、route grouping、candidate prepare、RPC attempt 和 local cleanup 使用
    `event=start/complete` 配对；start 后没有对应 complete 表示任务仍停留在该阶段。
    `eviction_summary.elapsed_ms` 记录整个主 `EvictionTask` 的耗时，用于区分主 eviction 卡顿与
    primary end-life lane 积压。
  - primary end-life pressure 字段包括 pending、queued、active drain 和 pending limit；
    dequeue 记录 oldest queue wait，RPC attempt 记录 master、attempt 和单轮/累计耗时。
  - `RemoveMetaFromMasterForEviction failed`
  - `DeleteNoneL2CacheEvictableObject start/end`
  - `Spill eviction list size before/after evict`
- Metrics:
  - perf points record eviction list add/erase/find、one object、delete、free。
- Traces or correlation fields:
  - thread tasks propagate `Trace::Instance().GetTraceID()` into async master/spill tasks。
- Debug hooks or inspection commands:
  - inject points: `worker.Evict`, `worker.SubmitSpillTask`, `worker.DeleteAllCopyMeta`, `evictAction.setDelete`, `WorkerOcEvictionManager.GetMetaAddressForObject`。
- How to tell the module is healthy:
  - eviction list size decreases under pressure；
  - failed size does not grow continuously；
  - `Evict is going on...` is not continuously emitted under allocation pressure；
  - spill eviction list and active spill size eventually drop below low water。

## Compatibility And Invariants

- External compatibility constraints:
  - `DeleteAllCopyMetaReqPb` fields are signed with AK/SK; field ordering and repeated `ids_with_version` are intentionally used instead of map。
  - `async_delete` protobuf semantics are master-side enqueue semantics。
- Internal invariants:
  - object mutation under `SafeObjType` lock。
  - async spill must revalidate create time。
  - memory eviction list membership must be checked after locking。
  - failed candidates must not be silently dropped unless object no longer exists。
- File format, wire format, or schema stability notes:
  - changing `DeleteAllCopyMetaReqPb` can affect auth signature compatibility。
- Ordering or naming stability notes:
  - thread names and log text are used in operations/debugging。

## Performance Characteristics

- Hot paths:
  - object/stream allocation trigger checks；
  - candidate list add/erase/find；
  - object write lock acquisition；
  - master RPC for `END_LIFE`；
  - spill file I/O。
- Known expensive operations:
  - synchronous `DeleteAllCopyMeta` under object write lock；
  - spill disk write and compact；
  - remote migration batch。
- Buffering, batching, or async behavior:
  - `DELETE` `RemoveMeta` batch: 300 keys or 10 ms。
  - `MIGRATE` batch: 512 keys。
  - `SPILL` futures are polled nonblocking during main loop and waited at task end。
- Resource limits or scaling assumptions:
  - 主 memory eviction 当前不水平扩展。
  - spill write scales with `spill_thread_num` but受磁盘和 fd 限制。

## Current Bottleneck Notes And Optimization Candidates

- Historical bottleneck addressed by this plan:
  - `NONE_L2_CACHE_EVICT` primary copy 的 `END_LIFE` 曾在 `EvictionTask` 中同步调用
    `DeleteAllCopyMeta`，而且通常持有对象写锁；一个慢 master/RPC 会拖住主 eviction task。
- Remaining bottleneck:
  - primary end-life lane 在锁外等待 `DeleteAllCopyMeta`，并在 RPC 完成后按 version 重新锁住对象；
    `EvictSpilledObjects` / `SpillImpl` no-space fallback 仍在对象写锁内同步等待。
- Why object lock matters:
  - 写锁保证对象 create time、spill state、shm、object table erase 和并发 get/set/evict 互斥。
  - 同步 RPC 放在写锁内，会让同一个 key 的其他操作等待；当前方案避免它延长主 eviction task 的单对象处理时间。
- Why `eviction_thread_num` is not enough:
  - 历史上该 flag 只配置 `MemEvictionThread` pool 大小；`Evict` 的 `isDone_` gate 只允许一个 `EvictionTask`。
  - 该 flag 容易误导运维以为调大可以提升 eviction 并发，现已删除；删除必须同步清理源码、测试、dscli、k8s deployment、k8s daemonset、部署文档和示例。
- Selected formal plan: worker-side primary end-life lane.
  - 将 memory eviction 主 loop 中所有 `Action::END_LIFE` 任务移到固定 4 线程的
    `primaryEndLifeThreadPool_`。
  - 入队成功从 `memEvictionList_` 移除并记录 `objectKey -> entry->GetCreateTime()` pending；pending 上限使用源码内固定常量，不新增用户可见配置。
  - lane 使用内部 queue/drain 模型而不是 per-key lambda 直接 RPC；同一个 master 的 key 聚合为 batch `DeleteAllCopyMeta`，不同 master 拆开请求。
  - pending 上限只约束 key 数，不约束对象字节数；lane 在发送 `DeleteAllCopyMeta` 前使用触发本次
    eviction 的 `needSize` 复查 low watermark，并按对象大小控制 batch 预计释放量；若当前已达低水位或
    batch 预算不足，则跳过本次 end-life、清 pending 并用 `READD_COUNTER` 回补 eviction list。
  - `WRITE_BACK_L2_CACHE_EVICT` 不在 pending 预占或提交 lane 前执行 `asyncSendManager_.Remove(objectKey)`；
    只有当 lane 重新拿到对象写锁、`DeleteAllCopyMeta` 对该 key 成功且本地删除成功后才移除 async send
    queue，避免 low watermark、锁失败、版本变化、redirect/meta moving 等回补路径丢失 write-back 回写任务。
  - `DeleteAllCopyMeta` 已成功但本地 cleanup 失败时，lane 记录该 key/version 的 metadata-deleted 状态；
    后续 retry 跳过 master metadata 删除，只重试本地 cleanup。
  - pending duplicate 表示已有 lane task 接管该 key，返回 OK，不重复提交，也不回补 eviction list。
  - lane 执行前用 `TryWLock()` 固定短重试处理刚启动时的锁冲突，然后通过窄 guard helper 复核 version、evictable、primary 和 end-life mode；不完整复刻 `GetObjectNextAction`。
  - lane guard 不能直接调用当前 `IsObjectEvictable()`，因为当前 helper 要求 key 仍在 `memEvictionList_`，而 accepted lane task 已从 list 移除。
  - batch 内多个对象 WLock 必须按稳定顺序获取，建议按 object key 排序；不得在 pending/queue mutex 下获取对象 WLock 或发送 RPC。
  - low watermark 复查应发生在获取 master 地址或至少发送 `DeleteAllCopyMeta` 前，且必须包含本次前台分配的
    `needSize`；已达低水位时不得 erase 本地对象，也不主动调用 `Evict()`。
  - `DeleteAllCopyMeta` batch 使用 repeated `ids_with_version` 并设置 `async_delete=true`；Master 入队成功
    即表示接受。同一 batch 只对 RPC 通信错误立即重试，最多调用三次；第三次仍失败时强制本地删除。
    metadata owner 路由失败、`failed_object_keys`、redirect info、`meta_is_moving` 和 Master 业务错误
    保持保守失败，不触发强制删除。
  - Master 侧只增加与该语义直接相关的保护：`PENDING` 必须标记为失败，已失败 key 和初次 no-meta 后回生
    的 metadata 都不得继续 cleanup；不增加版本下界判断或新的逐 key response。
  - `GetMetaAddress()` 已知 master 不可达或路由不可解析时快速跳过，不发 RPC；`K_RPC_UNAVAILABLE` 归类为 master/connection unavailable，`K_NOT_FOUND` 归类为 route/meta-address unavailable。
  - remote master 每次 RPC 调用由 1s API 总预算约束，通信失败最多进行三轮调用；local-bypass master
    通过 request timeout 和 master-side `timeoutDuration` 传递预算，但不是传输层强制中断。
  - primary end-life 的 `DeleteAllCopyMetaReqPb` 设置 `async_delete=true`；Master 使用请求中的 version
    将 key 加入 `ExpiredObjectManager`，入队成功即回复，后台继续做引用检查、通知和 metadata cleanup。
  - 三轮 RPC 通信失败后记录 ERROR，重新锁住对象并复核 version 后强制本地删除；其他失败清 pending，
    并用 `READD_COUNTER` 回补 `memEvictionList_`。lane 不主动调用 `Evict()`。
  - `primaryEndLifeThreadPool_` shutdown 需要在 manager 析构中显式 reset；若要丢弃未开始任务，需要使用 `ThreadPool` droppable 模式，否则默认线程池会 drain 队列。
  - `EvictSpilledObjects` 和 `SpillImpl` no-space fallback 继续保持同步，不属于本方案范围。
- Candidate 2: release object lock around RPC with version recheck.
  - RPC 前记录 version 并标记 intent，释放锁后调用 master，成功后重拿锁校验版本再 erase。
  - 若单独采用该候选，可缩短锁持有时间，但主 eviction task 仍同步等 RPC；只能缓解对象级阻塞，不能提升主 loop 吞吐。
- Batch `DeleteAllCopyMeta` for selected plan:
  - 已纳入正式 primary end-life lane，而不是后续独立候选。
  - 复用 `DeleteAllCopyMetaReqPb.ids_with_version` repeated 字段，并增加 `async_delete` 请求字段。
  - 同 master 聚合、跨 master 拆分；batch limit 和 short flush delay 是源码内部常量，不新增用户可见配置。
- Candidate 4: redesign true memory eviction concurrency.
  - 需要移除或细化 `isDone_` gate，设计共享水位、candidate list、pendingSpillSize、delete batch、spill task id 的并发所有权。
  - 已删除误导性 `eviction_thread_num`；未来若重新设计并发，不能只恢复该 flag，还要完整设计并发所有权和部署兼容语义。

## Build, Test, And Verification

- Build entrypoints:
  - `bash build.sh -t build`
- Fast verification commands:
  - `ctest -R EvictionManagerTest`
  - `ctest -R SpillEvictionTest`
  - `ctest -R KVCacheClientEvictTest`
- Representative unit, integration, and system tests:
  - `tests/ut/worker/object_cache/worker_oc_eviction_test.cpp`
  - `tests/ut/worker/object_cache/worker_oc_spill_eviction_test.cpp`
  - `tests/st/client/kv_cache/kv_cache_client_evict_test.cpp`
- Recommended validation for risky changes:
  - object allocation pressure with and without spill；
  - stream-triggered object eviction；
  - `NONE_L2_CACHE_EVICT` basic, concurrent, timeout, no-spill, and two-worker metadata removal cases；
  - write-back/write-through L2 eviction；
  - spill full and spill eviction；
  - master RPC failure injection；
  - slow master with large queued primary objects, verifying no obvious over-evict after memory reaches low watermark；
  - same-master primary end-life batch aggregation and cross-master split。

## Self-Verification Cases

| ID | Test scenario | Purpose | Preconditions | Input / steps | Expected result |
| --- | --- | --- | --- | --- | --- |
| EV-MEM-001 | object allocation triggers eviction | 验证高/低水位和候选 list | small shared memory | create objects until pressure | eviction runs and memory frees or spills |
| EV-NONE-001 | `NONE_L2_CACHE_EVICT` basic | 验证 end-life 语义 | worker with spill | set many none-L2-evict objects | latest object can be read, evicted old objects may become not found |
| EV-NONE-002 | no-spill none-L2-evict | 验证无 spill 时 END_LIFE | worker without spill directory | set many none-L2-evict objects | eviction succeeds through master delete |
| EV-NONE-003 | two-worker last local copy metadata | 验证所有 copy meta 清理 | two workers | remove both copy locations | query meta returns not exist |
| EV-SPILL-001 | spill full eviction | 验证 spill eviction list | small spill limit | fill spill and add evictable objects | spill files are deleted/compacted below water |
| EV-LOCK-001 | object lock busy | 验证 re-add behavior | object held by another operation | trigger eviction | candidate re-adds with retry counter |
| EV-RPC-001 | DeleteAllCopyMeta timeout/failure | 验证有界重试和可用性兜底 | inject worker/master RPC failure | evict none-L2-evict primary | each call is bounded to 1s; after three communication failures, local erase still rechecks object version |
| EV-RPC-002 | slow master and large primary objects | 验证 low watermark 复查避免过度释放 | small memory, mixed local and large none-L2 primary objects | delay master delete, let local eviction reduce pressure, then recover master | queued primary end-life does not continue after low watermark is reached; no obvious over-evict |
| EV-RPC-003 | batch DeleteAllCopyMeta by master | 验证同 master 聚合和跨 master 拆分 | multi-master or injectable routing | evict multiple none-L2 primary keys mapped to same and different masters | same-master keys are sent in batch, different-master keys are split, partial failures do not erase failed keys |

## Common Change Scenarios

### Adding A New Eviction Action

- Recommended extension point:
  - `Action` enum, `GetObjectNextAction`, `EvictObject`, `TryEvictObject`, trace aggregator, tests。
- Required companion updates:
  - update this design doc, module README, playbook if the action adds a new dependency。
- Review checklist:
  - object lock ownership；
  - local object table lifecycle；
  - master metadata lifecycle；
  - failed candidate re-add；
  - shutdown behavior。

### Modifying Existing Behavior

- Likely breakpoints:
  - `NONE_L2_CACHE_EVICT` semantics；
  - write-back done判断；
  - spill no-space fallback；
  - `isDone_` gate；
  - batch thresholds。
- Compatibility checks:
  - protobuf fields and AK/SK signature；
  - version semantics；
  - ST behavior for restart and remote get。
- Observability checks:
  - logs still identify action, failed keys, sizes, and elapsed time；
  - perf points remain around hot sections。

### Debugging A Production Issue

- First files to inspect:
  - `worker_oc_eviction_manager.cpp`
  - `worker_oc_spill.cpp`
  - `worker_master_oc_api.cpp`
  - `oc_metadata_manager.cpp`
- First runtime evidence to inspect:
  - repeated `Evict is going on...`；
  - `DeleteNoneL2CacheEvictableObject start` without timely end；
  - `RemoveMetaFromMasterForEviction failed`；
  - spill no-space logs；
  - eviction list failed size。
- Common misleading symptoms:
  - `async_delete=true` does not mean worker thread is not blocked。
  - `eviction_thread_num` 已不存在；memory eviction 是否并发取决于 `isDone_` 和候选队列模型，而不是线程池配置。
  - spill thread count increase may move bottleneck to disk I/O or object locks。

## Open Questions

- 如果 batch 需求继续增强，优先复用现有 repeated key/version；是否需要 eviction 专用 batch API 另行评估。
- 是否在后续独立方案中扩展 `EvictSpilledObjects` 或 `SpillImpl` fallback 的异步化；当前正式方案明确不覆盖。
