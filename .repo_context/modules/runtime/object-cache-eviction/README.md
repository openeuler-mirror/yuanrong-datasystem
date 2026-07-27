# Object Cache Eviction

## Scope

- Path(s):
  - `src/datasystem/worker/object_cache/worker_oc_eviction_manager.*`
  - `src/datasystem/worker/object_cache/eviction_list.*`
  - `src/datasystem/worker/object_cache/worker_oc_spill.*`
  - `src/datasystem/worker/object_cache/obj_cache_shm_unit.cpp`
  - `src/datasystem/worker/stream_cache/worker_sc_allocate_memory.cpp`
  - `src/datasystem/worker/object_cache/worker_master_oc_api.*`
  - `src/datasystem/master/object_cache/oc_metadata_manager.*`
  - `src/datasystem/protos/master_object.proto`
- Why this module exists:
  - 记录 worker object cache 在内存水位、spill 空间水位、L2 缓存语义、master 元数据删除之间的 eviction 决策。
  - 固化 `NONE_L2_CACHE_EVICT`、L2 已落盘对象、spill 对象、远端迁移对象的不同处置路径。
  - 给性能、并发、可用性和恢复相关改动提供统一入口。
- Primary source files to verify against:
  - `worker_oc_eviction_manager.cpp`
  - `worker_oc_eviction_manager.h`
  - `eviction_list.cpp`
  - `eviction_list.h`
  - `worker_oc_spill.cpp`
  - `worker_oc_spill.h`
  - `oc_metadata_manager.cpp`
  - `master_object.proto`

## Responsibilities

- Verified:
  - `EvictWhenMemoryExceedThrehold` 在 object 或 stream 分配前/重试期间按高水位触发 memory eviction。
  - `WorkerOcEvictionManager::Init` 创建 memory eviction、spill eviction、master metadata task、spill task 和定时检查线程池。
  - `EvictionList` 使用带计数的 clock/second-chance 队列选择候选对象，不是严格 LRU。
  - `GetObjectNextAction` 根据 primary copy、cache type、spill 状态、L2 是否可用、远端迁移能力和本地 spill 能力选择 `DELETE`、`FREE_MEMORY`、`SPILL`、`MIGRATE`、`END_LIFE` 或 `RETAIN`。
  - `DELETE` 路径本地擦除 object table 后异步批量 `RemoveMeta`。
  - `DeleteNoneL2CacheEvictableObject` 仍同步调用 master `DeleteAllCopyMeta`，该路径仅保留给
    `EvictSpilledObjects` 和 `SpillImpl` no-space fallback。
  - 当前分支已将 memory eviction 主 loop 中的所有 `Action::END_LIFE` 投递到
    `primaryEndLifeThreadPool_`；Worker 的 drain 线程同步发起 `DeleteAllCopyMeta` RPC，不创建独立 RPC
    线程。primary end-life 请求设置 `async_delete=true`，Master 将 key 加入 `ExpiredObjectManager` 后
    快速返回，实际 metadata cleanup 由 Master 异步执行。`EvictSpilledObjects` 和 `SpillImpl` no-space
    fallback 仍保持同步。
  - primary end-life lane 使用 `objectKey -> entry->GetCreateTime()` pending 去重，pending 上限使用源码内固定常量，不新增用户可见配置。正常路径等待 Master 接受删除；同一 batch 连续三次 RPC 通信失败后，可用性策略允许强制本地 erase。
  - primary end-life lane 需要 drain 内部 queue，并将同一个 master 的 key 聚合为 batch `DeleteAllCopyMeta`；不同 master 必须拆成不同请求，batch 使用 repeated `ids_with_version`。
  - primary end-life 复用现有 `DeleteAllCopyMeta` 响应；Master 异步入队成功即表示接受，仅
    `failed_object_keys` 和 redirect key 做逐 key 重试，`meta_is_moving` 或无法归因到具体 key 的
    `last_rc` 错误才整批重试。每次 Worker RPC 调用使用 1s API 总超时预算。
  - Master 不新增结果协议，只保证创建中 key 进入 `failed_object_keys`，且初次 no-meta 后 metadata 回生或
    key 已被提前判失败时不再执行 metadata cleanup。
  - pending 上限只限制 key 数，不限制对象字节数；primary lane 必须在发送 `DeleteAllCopyMeta` 前用触发本次
    eviction 的 `needSize` 复查 low watermark，并按对象大小控制 batch 预计释放量，已达低水位则跳过本次
    end-life、清 pending 并回补 eviction list，避免大对象 queued primary 后续造成过度释放。
  - primary end-life lane guard 不能直接复用当前 `IsObjectEvictable()`；accepted lane tasks 已从 `memEvictionList_` 移除，需要不依赖 list membership 的窄 guard。
  - `GetMetaAddress()` 快速失败需要区分 `K_RPC_UNAVAILABLE` 的 master/connection unavailable 和
    `K_NOT_FOUND` 的 route/meta-address unavailable；路由失败不发送 RPC，也不触发强制本地删除。
  - spill 写入由 `spill_thread_num` 控制并行度；spill 文件淘汰由单线程 `SpillEvictionThread` 控制。
  - `eviction_thread_num` 已删除；`MemEvictionThread` 固定为内部单线程，`isDone_` 门闩仍保证同一 manager 同时只有一个 `EvictionTask` 运行。
  - 删除 `eviction_thread_num` 时必须同步清理 dscli 默认配置、k8s deployment、k8s daemonset Helm values/template、部署文档和示例，避免部署继续传递未知 flag。
- Verified in current branch:
  - primary end-life lane 已实现，并已通过 focused UT 覆盖 pending 上限、low watermark 跳过和
    `DeleteAllCopyMeta` per-key 失败解析。

## Companion Docs

- Matching metadata JSON:
  - `.repo_context/modules/metadata/runtime.object-cache-eviction.json`
- Matching `design.md`:
  - `.repo_context/modules/runtime/object-cache-eviction/design.md`
- Matching feature playbook:
  - `.repo_context/playbooks/features/runtime/object-cache-eviction/implementation.md`
- Related formal design material:
  - Detailed Chinese design notes are kept out of the repository and carried in the PR description or local workspace
    notes.
- Reason if either is intentionally omitted:
  - 不省略。该模块处在内存热路径、后台线程、RPC 元数据、spill 持久状态和恢复语义交叉处，需要设计文档和实施 playbook。

## Module Boundary Assessment

- Canonical module boundary:
  - `runtime.object-cache-eviction` owns worker object cache eviction decision, scheduling, local memory/spill cleanup, and master metadata cleanup integration.
- Candidate sibling submodules considered:
  - `runtime.worker-runtime`: worker 生命周期父模块，保留启动和服务面，不承载 eviction 状态机细节。
  - `infra.l2cache`: L2 backend 和持久化能力父模块；eviction 只消费 write mode/L2 状态，不拥有 backend 实现。
  - `infra.slot`: 分布式磁盘 slot 的持久格式和恢复；eviction 只通过 object 状态和 L2 可见性做决策。
  - `runtime.topology`: 负责 worker 路由和扩缩容；eviction 通过 `TopologyEngine` 和 `NodeSelector` 获取 master 地址或远端迁移能力。
- Why they stay inside the parent module or split out:
  - eviction 有独立线程、队列、水位、对象锁、RPC 和 spill 文件生命周期，因此从 worker runtime 拆成 sibling module。
  - L2、slot、topology 仍作为依赖模块记录，避免把 backend 持久格式和路由协议混入 eviction 文档。

## Key Entry Points

- Public APIs:
  - `WorkerOcEvictionManager::Init`
  - `WorkerOcEvictionManager::Add`
  - `WorkerOcEvictionManager::Erase`
  - `WorkerOcEvictionManager::Evict`
  - `WorkerOcEvictionManager::TryEvictSpilledObjects`
  - `EvictWhenMemoryExceedThrehold`
- Internal services / executables:
  - `MemEvictionThread`: 执行 memory eviction task。
  - `SpillEvictionThread`: 执行 spill 文件淘汰 task。
  - `MasterTaskThread`: 异步提交 `RemoveMetaFromMasterForEviction`。
  - `SpillThread`: 写本地 spill 文件或提交远端迁移 batch。
  - `scheduleEvictThread`: 每 10 秒检查 object/stream 水位并触发 eviction。
- Config flags or environment variables:
  - `eviction_reserve_mem_threshold_mb`: 参与 object/stream 高水位阈值计算，默认 10240 MB。
  - `spill_directory`: 为空时禁用本地 spill。
  - `spill_size_limit`: spill 目录容量限制，0 表示使用启动时目录空闲空间的 95%。
  - `spill_thread_num`: `SpillThread` 和 `SpillFileManager` 并行度，默认 8。
  - `spill_file_max_size_mb`: 单个 spill 文件大小上限，默认 200 MB。
  - `spill_file_open_limit`: spill 文件打开 fd 上限，默认 512。
  - `spill_to_remote_worker`: 允许将本地内存压力迁移到其他 worker 内存，默认 false。

## Main Dependencies

- Upstream callers:
  - object cache shared-memory allocation path: `AllocateMemoryForObject`
  - stream cache allocation path: `WorkerSCAllocateMemory::AllocateMemoryForStream`
  - object create/get/update paths that call `Add` / `Erase`
- Downstream modules:
  - worker object table and `SafeObjType` object lock
  - `ObjectGlobalRefTable<ClientKey>`
  - `WorkerOcSpill`
  - `WorkerMasterOCApi`
  - `OCMetadataManager`
  - `ExpiredObjectManager`
  - `DataMigrator` and `NodeSelector`
  - `ClusterManager`
- External dependencies:
  - worker-to-master RPC
  - local filesystem for spill
  - shared-memory allocator

## Build And Test

- Build commands:
  - `bash build.sh -t build`
- Fast verification commands:
  - `ctest -R EvictionManagerTest`
  - `ctest -R SpillEvictionTest`
  - `ctest -R KVCacheClientEvictTest`
- Representative tests:
  - `tests/ut/worker/object_cache/worker_oc_eviction_test.cpp`
  - `tests/ut/worker/object_cache/worker_oc_spill_eviction_test.cpp`
  - `tests/st/client/kv_cache/kv_cache_client_evict_test.cpp`

## Review And Bugfix Notes

- Common change risks:
  - 未来若把 `END_LIFE` 的同步 master RPC 放回 memory eviction 主 loop，会再次放大 master/RPC 抖动对
    eviction 吞吐的影响；primary end-life lane 在对象锁外发送 RPC，spill/no-space fallback 仍同步持有对象锁。
  - `eviction_thread_num` 已删除；如果未来重新设计 memory eviction 并发，不能通过恢复该 flag 绕过 `isDone_`，需要重新设计候选队列、水位和失败回填的一致性。
  - 删除 `eviction_thread_num` 必须同时覆盖 `cli/deploy/conf/worker_config.json`、`k8s_deployment/helm_chart/worker.config`、`k8s/helm_chart/datasystem/values.yaml`、`k8s/helm_chart/datasystem/templates/worker_daemonset.yaml`、`docs/source_zh_cn/deployment/dscli.md`、`docs/source_zh_cn/deployment/k8s_configuration.md` 和示例文档。
  - `DELETE` 和 `END_LIFE` 语义不同：前者只移除本 worker copy metadata，后者删除所有 copy metadata 并结束对象生命周期。
  - spill 写入会释放对象写锁再执行 I/O，之后按 create time 重新加锁校验版本；不要绕过版本校验。
  - 当前分支的 primary end-life lane 已使用 `ids_with_version` 并处理 `failed_object_keys` / `outdated_objs`；
    `EvictSpilledObjects` 和 `SpillImpl` fallback 保留的同步 `DeleteNoneL2CacheEvictableObject` 仍使用
    `object_keys`。
  - primary end-life lane 在短锁复核后释放对象锁，由当前 drain 线程同步发送带
    `async_delete=true` 的 `DeleteAllCopyMeta`；Master 入队成功返回后，Worker 重新获取对象锁并复核
    version，再做本地 erase。该方案不引入前台可见 pending 状态，也不改变 spill eviction 和 spill
    no-space fallback 的同步语义。
- Important invariants:
  - 对象被选为候选后必须先取得对象写锁；拿不到锁时从 eviction list 暂时移除并以 `READD_COUNTER` 回填。
  - `IsObjectEvictable` 必须确认对象仍在 eviction list 且 binary 对象仍有 shm。
  - `SPILL` 成功后才释放内存并标记 spill state；重加锁失败时需要回滚 spill 文件。
  - spill eviction 只删除 write-through、write-back 且 writeback done、或 `NONE_L2_CACHE_EVICT` 对象。
  - master metadata 或路由失败的对象回到 eviction list；仅同一 batch 的 `DeleteAllCopyMeta` 连续三次
    返回 RPC 通信错误时，打印 force-delete ERROR 日志并强制释放本地对象。
  - eviction `RemoveMeta` 只允许初始 metadata owner 重定向一次；转发请求使用 `redirect=false`，目标若仍返回
    redirect、`meta_is_moving` 或 failed ids，则相关对象保留重试资格，不继续递归转发。
- Observability or debugging hooks:
  - Logs: `Eviction start`, `Evict is going on`, `EvictionList size before/after evict`, `Spill eviction list size before/after evict`。
  - Worker primary end-life 使用 `PRIMARY_END_LIFE_DIAG` 标记 `eviction_summary`、`dequeue`、`route_group`、
    `prepare`、`rpc_attempt`、`local_cleanup` 和 `drain_batch`。正常阶段使用 `VLOG(1)`；阶段耗时或
    queue wait 达到 100 ms、RPC 返回错误或 per-key 失败时使用 `LOG(WARNING)`。
  - `eviction_summary`、`route_group`、`prepare`、`rpc_attempt` 和 `local_cleanup` 同时记录
    `event=start/complete`，最后一条 start 没有对应 complete 时可直接定位停留阶段；
    `eviction_summary.elapsed_ms` 是整个主 `EvictionTask` 的耗时。
  - primary end-life 阶段日志包含 pending、queued、active drain 和 pending limit；
    `dequeue` 额外记录 oldest task 的 `queue_wait_ms`，RPC 日志记录 master、attempt 和单轮/累计耗时。
  - Perf keys: `WORKER_EVICT_LIST_ADD`, `WORKER_EVICT_LIST_ERASE`, `WORKER_EVICT_LIST_FIND`, `WORKER_EVICT_ONE_OBJECT`, `WORKER_EVICT_DELETE`, `WORKER_EVICT_FREE`。
  - Inject points include `worker.Evict`, `worker.SubmitSpillTask`, `worker.DeleteAllCopyMeta`, `evictAction.setDelete`, `worker.MigrateData.setMaxRetryCount`。

## Design Notes To Revisit

- 当前正式方案：memory eviction 主 loop 的 `END_LIFE` 进入 primary end-life lane，由
  `PRIMARY_END_LIFE_THREAD_NUM=4` 的 drain 线程同步发送 RPC，
  pending 上限固定常量，write-back 仅在 lane 重新锁住对象、`DeleteAllCopyMeta` 成功且本地删除成功后移除
  async send queue，metadata 已删除但本地 cleanup 失败的 key 记录为 local-cleanup retry，pending duplicate
  直接视为已有 task 接管，lane drain 内部 queue 并按 master 聚合 batch
  `DeleteAllCopyMeta`，batch 使用 repeated
  `ids_with_version`，lane 用固定短重试和不依赖 eviction list membership 的窄 guard helper 复核状态，发送
  `DeleteAllCopyMeta` 前复查 low watermark 并按对象大小控制 batch 预计释放量以避免大对象过度释放；
  请求设置 `async_delete=true`，Master 使用请求 version 加入 `ExpiredObjectManager`，每次 Worker RPC
  调用预算为 1s；仅明确失败、
  redirect、meta moving 或无法归因的批次错误使用 `READD_COUNTER` 回补 eviction list，且不主动触发
  `Evict()`。
- 同一 batch 的 `DeleteAllCopyMeta` 只在返回 RPC 通信错误时立即重试，最多调用三次；第三次仍失败则
  打印 ERROR，重新获取对象锁并复核 version，再强制执行本地删除。路由失败、Master per-key 拒绝、
  `meta_is_moving`、低水位跳过、锁失败、version 变化和本地 cleanup 失败不触发该可用性兜底。
- 如果未来要让 memory eviction 真正并发，需要先设计候选队列并发、对象锁竞争、低水位判断和 batch flush 的一致性，不能恢复 `eviction_thread_num` 作为调优入口。
- 如果未来扩展到 `EvictSpilledObjects` 或 `SpillImpl` fallback 异步化，需要单独定义 spill eviction list erase、compact 触发和 spill 成功收尾语义。
