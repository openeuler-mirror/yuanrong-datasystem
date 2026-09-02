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
  - 淘汰策略通过 `PolicyRoute` 路由：稳定态持 active list/strategy；热更新态同时持 source/target
    list/strategy。Clock/Heat 参数由 strategy 构造时的不可变快照持有，不再由策略热路径反复读取目标参数。
    - `ClockEvictionStrategy` 封装现有时钟算法，行为与策略化前逐字节等价（`OnAdd==OnCacheHit==EvictionList::Add`，
      `Decay()` 空，`SelectCandidate()==FindEvictCandidate`）。
    - `HeatEvictionStrategy`：`EvictionList::Node` 新增 `heat`(double, 上限 `FLAGS_eviction_heat_max_counter` 默认 256)、
      `lastAccessMs`、`lastDelayMs` 字段。`OnAdd`(`AddHeatNode`) 首次入列初始化 heat=`FLAGS_eviction_heat_initial_counter`(默认 2，
      而非 max——fresh 不被算作热数据以避免触发误 rebalance，且 heat=阈值不进首轮淘汰、fallback 靠 lastAccess 平局保护)；`OnCacheHit`(`IncrementHeat`)
      缓存命中时按 `min(1, 4KiB/ShmUnit::GetMigratableSize())` 增加 heat（封顶）并刷新 lastAccess；未知大小兼容
      旧接口并记 1。该 allocator-byte 归一化避免 128 KiB 对象与 1--4 KiB 对象每次访问获得相同信用、过快进入
      heat>4 精确保护层；`Decay()` 周期衰减 `count_new=count_old*0.5^(dt/T)`，
      primary/local copy 用不同 T（`IsObjectPrimaryCopy` 经 `objectTable_` 查询），local T 更小→衰减更快→更易淘汰；
      淘汰轮次通过 `GetHeatCandidates` 产生最多 256 个有序快照，优先 heat<阈值者、否则最低 heat，平局取
      lastAccess 最旧；对象写锁取得后用同 key generation/update-seq 再校验。无关 key 的插入不再使整批候选失效。
      eviction 专用扫描从首个非最近访问且 heat<2 的节点起，最多检查 `8*batch=2048` 个有效快照，并保留其中
      最冷的 256 个，避免 first-match 的链表插入顺序偏差；策略在 list 锁外通过 ObjectTable `TryRLock` 读取
      `ShmUnit::GetMigratableSize()`，heat<=4 的候选按 heat/allocator-byte 排序，无法解析大小者后置。heat>4
      继续按 heat、lastAccess 精确排序。最近访问保护窗口为 100ms，阻止同一淘汰突发立即删除刚访问对象，但不让
      大对象 refill 在多轮淘汰中获得整整 1 秒的无条件保护。rebalance 仍使用自己的全局 heat 有序批次。
      候选批次中的 generation/update-seq 快照按 key 独立校验：某个候选因并发命中变 stale 时只跳过该 key，
      不再丢弃其余仍有效的快照并重复执行最多 2048 项扫描、256 项大小解析和排序；最终对象写锁后的逐 key
      校验保持不变。
      命中热路径从已持有的 `ShmUnit` 读取 migratable size，增加一次浮点除法；对已存在 key 仍使用 TBB shared
      accessor 和 atomic CAS，不获取同 key 独占 accessor 或全局 list 写锁，不增加对象查找、分配或 IO。
      `DecayAll` 先在 list 锁下快照不可变 key/generation，再在 per-key accessor 下取得一致 heat/timestamp 快照；
      写回检查 generation/lastDelay，保留快照后的命中增量，并跳过同名重建、并发 reinsert 和重叠 decay。
  - **Clock/Heat worker 内热更新首版**：
    `BeginPolicyUpdate` 先禁止新淘汰并请求当前淘汰轮在候选边界退出，最多等待 30 秒；已进入对象淘汰的 candidate
    继续完成，spill future、失败回加、metadata flush 以及 primary-end-life queue/worker/pending map 全部 drain 后
    才创建 target list；异步失败回加在转换期间计入 mutation generation 并经 Router 写入正确列表。
    迁移期间 Hit/Add 在固定 256 条 key stripe 内执行 target-first：target 缺失时转换 source 快照、创建 target、
    将本次访问记入 target，source 留待后台 `MoveOnePolicyNode` 合并并删除；创建失败则回退更新 source。
    Clock→Heat 后台合并使用 `min(targetHeat + convertedSourceHeat, maxCounter)`；跨 worker migrated heat 和恢复路径
    仍取最大值，防止重试或回滚重复累计。
    Erase 同时删除两表，Extract 优先 target。`MigratePolicyBatch` 有界扫描 source，source 为空后进入 VERIFYING，
    `CommitPolicyUpdate` 核对 list/index/HeatState 内部结构，并按 ObjectTable eligible resident copy 核对 target
    membership；审计期间使用 entry `TryRLock` 避免 ObjectTable→entry 锁反转，并在取得 route 独占锁后复核
    mutation generation，关闭 audit→publish 的竞态窗口。last-good 原子文件写入在 route 锁外完成，
    `ACTIVATING` 阶段的业务 mutation 继续路由到 target，写入成功后再在 route 独占锁下发布 target。
    inactive list 在下一轮反向更新中复用。
    Value/ObjectTable/ShmUnit 不参与复制。
  - 控制通道复用资源上报：`WorkerStat` 上报 active/control epoch、phase、READY/CONVERTING/ACTIVE/FAILED、
    对象进度和有界失败原因，`ResourceReportRspPb` 返回 `EvictionPolicyUpdatePb`；`NodeSelector` 每轮仅推进一个
    batch，batch 上限为 4096。master `ResourceManager` 接受 controller 提交的
    PRECHECK/COMMIT、epoch/target/batch/cohort 和准入门槛，
    通过 coordination backend CAS 持久化 rollout，启动加载并周期刷新其他 master 的提交，再按 worker 地址稳定 hash
    下发。`GetEvictionPolicyUpdateProgress` 返回该 master 最近观测的 worker ACK；worker 明细保留用于诊断，但
    READY/CONVERTING/ACTIVE/FAILED 计数只统计当前 rollout epoch，避免上一 epoch 的失败状态污染新任务进度；
    跨 master READY barrier 由控制器汇总。
    worker 在 drain 前通过原子文件持久化 transition intent，重启恢复 last-good，并只允许未完成 COMMIT 向同一
    epoch/target 收敛。COMMIT 下发的同一资源报告以及所有非 STABLE worker 报告均标记为 rebalance not-ready，
    避免禁止淘汰期间继续成为 rebalance source/target。RebalanceTask 同时携带 source/target policy+epoch；
    source 启动与逐批校验、target RPC 准入再校验。worker 在 COMMIT 前暂停并排空 outbound rebalance 与 inbound
    migration，转换结束后恢复；默认 memory rebalance 的 SPILL 迁移在 URMA 开启时可走 read/write 快速通道，
    direct/NotifyRemoteGet 请求携带相同 target fence，并在 target 准入前校验，阻止迟到旧 epoch
    写入；携带 heat metadata 或启用 keep-local-copy 时仍走 TCP。当前基线直接使用最终协议，memory rebalance
    task 与对应迁移请求必须携带完整 policy/epoch fence。
  - heat 衰减不在淘汰时做（淘汰时 counter 不衰减），改为在 worker 周期资源上报前做一次：`NodeSelector::CollectClusterInfo`
    开头调用已注册的 pre-report hook（`RegisterPreReportHook`，仿 `RegisterRebalanceTaskHandler`，
    于 `WorkerOCServer::CreateRebalanceExecutor` 注册、`StopRebalanceExecutor` 反注册），
    钩子有独立 30 秒最小执行间隔，master 不可用时 500ms 资源上报重试不会放大 O(N) 扫描。heat eviction 下
    调 `MaintainHeatAndCollectHotPrimaryStats()`，在一次 eviction-list 快照和一次对象解析中同时完成 decay、
    post-decay hot-primary 统计及全部 stable-primary 字节统计。clock 默认不扫描。workload telemetry 的
    `WorkerOCServer.copyWatermarkTelemetryIntervalMs` 注入注册独立的 pre-report hook：Worker 向 Master 周期上报
    资源前按注入周期刷新一次只读冷/温/热快照。该 hook 在 heat maintenance 之后注册，因此同一轮同时到期时
    统计衰减后的 Heat 状态；Clock 热数据定义为 counter>=Q2(2)。它不缩短 heat decay/master 调度统计周期，
    也不覆盖 Master 调度输入；生产默认路径不注册该额外 Clock/Heat 遥测扫描。
  - **热度 rebalance 策略**（`FLAGS_rebalance_strategy`="heat"，默认 "memory"=既有 usage 驱动 rebalance）：
    与热度淘汰联动，优先迁 source 侧热度最低的 stable primary copy 到低水位 target。
    - master：`HeatRebalanceScheduler`（独立类，继承 `RebalanceScheduler` 基类；`MemoryRebalanceScheduler` 同继承但逻辑逐字不变）。
      source 触发为两条 OR 路径：usage>60% 且仍有 primary（内存压力兜底），或 usage>50% 且
      hot bytes/capacity>40%；target 合格=usage<50% 且 hot bytes/capacity<30%；迁移量=
      MIN(source capacity 的 10%、target available-在途、per-round 上限)；排序=使用率升序→扣除在途后空间降序→nodeId；
      成功不加 cooldown，但 target 在途字节保留到更新的 target 资源快照到达，避免使用成功前的旧
      available 重复派发；失败/超时释放在途并加 cooldown。
    - worker：`WorkerOcEvictionManager::MaintainHeatAndCollectHotPrimaryStats`(pre-report hook)统计热 primary
      count/bytes 与全部 stable primary count/bytes；bytes 使用 `ShmUnit::GetMigratableSize()`，与 allocator real usage、
      迁移预算同单位。原 count/hot-bytes 经 `NodeSelector::SetHotPrimaryReport`→`WorkerStat` proto 字段(7/8/9)
      上报 master；完整缓存快照经 `OBJECT_COPY_WATERMARK` 写入 resource log，采集线程本身不扫描对象。
      `HeatRebalanceCandidateProvider::Select` 用 `EvictionList::GetHeatCandidates(+inf)` 按 heat 升序选 stable primary，
      避免热 primary 迁完后冷 primary 把 keep-local source 卡在高水位。成功迁移后 executor 立即只读刷新 source 的
      master hot-primary report，避免继续使用 30 秒旧 ownership 快照。迁移完成后的
      `OnMigrated` 对 memory/heat 均为 no-op。两种 provider 都把 task `max_bytes` 当作 target room/inflight 的硬预算，
      超过剩余预算的候选会 unmark 后跳过。SPILL source object 与 eviction node 的清理由
      `AsyncResourceReleaser` 在对象精确版本写锁内通过 RAII transaction 统一提交；状态错误或异常会恢复 eviction
      node 并重试，线程入口 catch-all 阻止异常穿透 `noexcept`，也避免同 key 重建的 TOCTOU。
      `RebalanceExecutor` 缓存最近一个 terminal result；master 因结果 RPC 丢失而重放同一 task id 时只重报结果，
      不重复迁移。缓存为单项、进程内状态；worker 重启后的跨进程去重不在本次修复范围。
    - 校验：`rebalance_strategy=heat`⟹`eviction_strategy=heat`、`eviction_heat_initial_counter<rebalance_heat_hot_counter_threshold`、
      source low usage/hot 阈值分别严格小于对应 high 阈值；flag 快照访问器在 `eviction_heat.{h,cpp}`，
      校验在 `worker_update_flag_check.cpp`。
    - **rebalance/eviction 互斥不变**：`TryMarkRebalancingObject`/`IsObjectBeingRebalanced` + `EvictionTask` 写锁配对仍生效（两策略共用）。

  - **keep-local-copy 迁移模式**（`FLAGS_rebalance_keep_local_copy`=true，默认关闭）：
    与 `rebalance_strategy` 正交（memory/heat 都可开）。开启后 `RebalanceExecutor` 用 `MigrateType::REBALANCE_KEEP_LOCAL`
    替代 `SPILL`；迁移后 src 保留 local 非 primary 副本（`SetPrimaryCopy(false)` 降级），而非 `AsyncResourceReleaser` 擦除。
    - 控制点：`MigrateDataHandler::ReleaseResources` 对 `REBALANCE_KEEP_LOCAL` 的 confirmed ids 调 `DemotePrimaryCopies`（Get→WLock→
      校验本批次 create-time version→`SetPrimaryCopy(false)`→WUnlock），同 key 重建对象不会被旧迁移结果误降级；
      master 返回的 expired ids 通过 `MigrateDataRspPb.expired_ids` 单独传递并按精确版本释放。legacy SPILL 仍把
      expired ids 编入 success 响应，保持旧 source 的 wire contract。
      `ReplacePrimaryImpl` 的 `remove_location=(type==SPILL)`
      天然 false → master locations 保留 src；candidate provider 的 `OnMigrated` 为 no-op，eviction-list 节点有效不擦除。
      `data_migrator.cpp`/`worker_oc_service_migrate_impl.cpp` 各 MigrateType switch 加 case。
    - direct/URMA-read 迁移当前只允许 legacy `SPILL`；keep-local 及其他非 SPILL 类型走 TCP。direct sender 不序列化
      新增的 `type/has_type` 字段，以保持旧 AK/SK peer 的 canonical bytes；新 target 将字段缺失解释为 legacy SPILL，
      并拒绝显式非 SPILL direct 请求。
    - MEMORY `SPILL` / `REBALANCE_KEEP_LOCAL` 的 target 准入不能只凭 high-water 或 source 所见剩余空间提前
      返回 OOM：target 可能仍持有可淘汰冷对象。source 的 legacy 1 MiB batching floor 对这两类迁移放行，即使
      target 上报 0 剩余空间也形成 singleton batch；target 再按对象实际大小走
      `AllocateMemoryForObject(..., retryOnOOM=true)`，在对象锁保护下执行 allocate-evict-retry。disk migration 和
      `SCALE_DOWN` 保留严格容量门禁。该路径不增加锁或改变锁序。
    - 降级后对象 non-primary：`TryGetObjectSize`/热 primary 统计跳过、heat decay 用 local 半衰期、eviction 可 DELETE 回收。
    - 滚动升级：旧 target 不识别新的迁移枚举值，因此默认保持 legacy SPILL 擦除路径；确认所有 target 升级后再显式
      设置 `rebalance_keep_local_copy=true`。回滚时恢复 false。

  - cache-hit 命中点（`worker_oc_service_get_impl.cpp` 的 `IncMemHit(1)` 处：快路径 `SubmitAsyncAddEvictTask` 与
    WLock 路径 `KeepObjectDataInMemory`）把已持有的 allocator-accounted migratable size 一并传给
    `evictionManager_->OnCacheHit`；publish/create/migrate/remote 回填
    中，publish/create/migrate 保持 `Add`（→`OnAdd`，首次入列）；由成功 Get 触发的 spill/L2/remote 回填调用
    `OnRefill`。单 key 和 BatchGet 的远端回填都明确走 refill 语义；Heat 的 `OnRefill` 通过单个
    `RefillHeatNode` 在节点可见前原子计入本次 size-normalized Get，4 KiB 及以下对象的默认 heat 从 2 升到 3，
    128 KiB 对象只增加 0.03125。Heat eviction 候选扫描把最近 100ms 访问的节点排在批次末尾，但在释放预算需要
    时仍可选中。保护复用原子 `lastAccessMs`，不增加 per-object 字段；Clock 忽略 size，
    `OnRefill==OnCacheHit==OnAdd`，行为不变。
  - `GetObjectNextAction` 根据 primary copy、cache type、spill 状态、L2 是否可用、远端迁移能力和本地 spill 能力选择 `DELETE`、`FREE_MEMORY`、`SPILL`、`MIGRATE`、`END_LIFE` 或 `RETAIN`。
  - `DELETE` 路径本地擦除 object table 后异步批量 `RemoveMeta`。
  - eviction manager 删除本地副本时使用 `RemoveMetaReqPb::EVICTION`；Get 失败后的 requester location 清理使用
    `RemoveMetaReqPb::NORMAL`，两者不能互换。
    异步 EVICTION cleanup 可能与同版本 rebalance primary promotion 交错：若请求抵达 Master 时待删地址已是
    当前 `primary_address`，`OCMetadataManager::RemoveMetaLocation` 将请求按幂等成功处理但保留 location。
    object version 本身不能 fence 这种同版本 ownership 变化；普通 non-primary cleanup 不受影响，primary
    end-life 仍由 `DeleteAllCopyMeta` 协调。
  - `DeleteNoneL2CacheEvictableObject` 仍同步调用 master `DeleteAllCopyMeta`，该路径仅保留给
    `EvictSpilledObjects` 和 `SpillImpl` no-space fallback；初始 owner 可重定向一次，转发请求设置
    `redirect=false`，目标再次重定向或失败时保留本地对象并重试。
  - 当前分支已将 memory eviction 主 loop 中的所有 `Action::END_LIFE` 投递到
    `primaryEndLifeThreadPool_`；Worker 的 drain 线程同步发起 `DeleteAllCopyMeta` RPC，不创建独立 RPC
    线程。primary end-life 请求设置 `async_delete=true`，Master 将 key 加入 `ExpiredObjectManager` 后
    快速返回，实际 metadata cleanup 由 Master 异步执行。`EvictSpilledObjects` 和 `SpillImpl` no-space
    fallback 仍保持同步。
  - primary end-life lane 使用 `objectKey -> entry->GetCreateTime()` pending 去重，pending 上限使用源码内固定常量，不新增用户可见配置。正常路径等待 Master 接受删除；同一 `(owner, topologyVersion)` 的三个延迟调度轮次均发生可重试通信失败后，可用性策略允许强制本地 erase。
  - primary end-life lane 使用 4 个常驻 drain、ready/delayed queue 和 Worker 内全局 owner lane；同一 `HostPort` 最多一个 RPC 在途。每个 drain 一次只执行一个 owner batch，其余健康 owner batch 放回 ready queue 供其他 drain 接管。
  - primary end-life 复用现有 `DeleteAllCopyMeta` 响应；Master 异步入队成功即表示接受。
    `failed_object_keys` 做逐 key 重试，redirect key 在释放 source owner lane 后按目标 master 进入统一调度，转发保留
    `address`、`ids_with_version` 和 `async_delete=true`，并设置 `redirect=false`。目标再次重定向、
    `meta_is_moving`、RPC 失败或无法分类的错误只回填对应 target group，不递归转发，也不触发源 owner 的
    三次超时强制删除。每个逻辑 attempt 的 source 请求和独立首跳转发共享 1s API 总超时预算。
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
  - primary end-life lane 已实现 owner 单飞和跨轮延迟重试，并通过 focused UT 覆盖 pending 上限、low watermark 跳过、
    `DeleteAllCopyMeta` per-key 失败解析、一跳 redirect、同 owner 单飞、健康 owner 进展、隔离后的新 owner 重路由和三轮强制删除。

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
  - `spill_io_mode`: `buffered`（默认）保留同步写路径；`direct_io_uring` 为每个 `SpillFileManager` 创建独立 ring，
    准备线程只提交持有 SHM lease 的异步请求。
    ring depth 16、反压上限 64 请求/8 MiB、WRITE batch 32 请求/4 MiB/1 ms 是内部策略，不增加用户可见配置；
    WRITE 已完成但尚未被 future 消费的请求仍计入反压。空队列允许一个超过 8 MiB 的对象进入，避免大对象永久
    返回 `K_TRY_AGAIN`。Direct I/O 内部对齐固定为 4 KiB，不依赖全局 `memory_alignment`；地址或长度不满足
    Direct I/O 约束的对象写入独立 buffered fallback 文件。将 `memory_alignment` 配为 4096 可提高 Direct 路径
    覆盖率，但不是启用 `direct_io_uring` 的启动前提。direct async spill 不执行 DATASYNC，也不预分配 spill 文件。
  - `spill_file_max_size_mb`: 单个 spill 文件大小上限，默认 200 MB。
  - `spill_file_open_limit`: spill 文件打开 fd 上限，默认 512。
  - `spill_to_remote_worker`: 允许将本地内存压力迁移到其他 worker 内存，默认 false。
  - `eviction_strategy`: 启动时的初始淘汰策略，`"clock"`(默认，现有时钟算法) 或 `"heat"`(热度衰减策略)；
    运行时 Clock/Heat 切换由 master 的 epoch/cohort 控制通道执行，不直接动态修改该 flag。
    默认 Clock 的 resident list node 只保留 object key 与 counter；Heat 原子、时间戳和 generation 位于仅 Heat 策略
    创建的旁路表，避免为每个 Clock 对象支付 Heat 状态开销。
  - `eviction_heat_half_life_primary_s` / `eviction_heat_half_life_local_s`: heat 策略 primary / local copy 衰减半衰期 T(秒)，产品默认 600s/300s，local 应 ≤ primary（local 更易淘汰）；当前为启动配置，不支持运行时修改，升级后须重启 Worker 才使用新默认值。
  - `eviction_heat_threshold`: heat 策略首轮候选阈值（heat < 阈值者优先淘汰）；当前为启动配置。
  - `eviction_heat_max_counter`: heat 计数器递增上限（默认 256）；当前为启动配置。淘汰候选因并发冲突回插时恢复
    被选中时的 heat，不再把对象直接提升到 cap；若期间有并发 hit，则保留两者较大值。
  - `eviction_heat_initial_counter`: 新对象 `OnAdd` 初始 heat（默认 2，非 max）；当前为启动配置。须 ≥ `eviction_heat_threshold`、≤ `eviction_heat_max_counter` 且 < `rebalance_heat_hot_counter_threshold`，使 fresh 既不进首轮淘汰、又不被算作热数据。
  - `rebalance_strategy`: rebalance 策略选择，`"memory"`(默认，既有 usage 驱动) 或 `"heat"`(热度驱动)，仅启动时切换；`"heat"` 须配 `eviction_strategy="heat"`。
  - `rebalance_heat_hot_counter_threshold`: heat>此值=热数据（默认 4），须 > `eviction_heat_initial_counter`。
  - `rebalance_heat_source_usage_percent`: 高水位+仍有 primary 的内存压力兜底路径（默认 60）。
  - `rebalance_heat_source_usage_percent_low` / `_source_hot_ratio_percent`: 中水位+高热度路径（默认 50/40）。
  - `rebalance_heat_target_usage_percent` / `_target_hot_ratio_percent`: target 双重合格线（usage< 且热占比<，默认 50/30）。
  - `rebalance_keep_local_copy`: rebalance 迁移后 src 是否保留 local 非 primary 副本（默认 false=legacy SPILL
    擦除，true=降级保留），与 `rebalance_strategy` 正交。滚动升级时保持 false，待 target 全部升级后再显式启用。
  - 测试注入开启的 pre-resource-report copy-watermark telemetry 将稳定 Primary 按 allocator bytes 分为冷/温/热：
    Clock 使用 counter 0/1/>=2，Heat 使用 heat<`eviction_heat_threshold`、中间闭区间、
    heat>`rebalance_heat_hot_counter_threshold`。该分类不进入 Master 调度 protobuf，生产默认 callback 不启用。

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
  - `ctest -R EvictPrimaryRedirectScaleTest`
- Representative tests:
  - `tests/ut/worker/object_cache/worker_oc_eviction_test.cpp`
  - `tests/ut/worker/object_cache/worker_oc_spill_eviction_test.cpp`
  - `tests/st/client/kv_cache/kv_cache_client_evict_test.cpp`
  - `tests/st/worker/object_cache/evict_primary_redirect_scale_test.cpp`

## Review And Bugfix Notes

- Common change risks:
  - 未来若把 `END_LIFE` 的同步 master RPC 放回 memory eviction 主 loop，会再次放大 master/RPC 抖动对
    eviction 吞吐的影响；primary end-life lane 在对象锁外发送 RPC，spill/no-space fallback 仍同步持有对象锁。
  - `eviction_thread_num` 已删除；如果未来重新设计 memory eviction 并发，不能通过恢复该 flag 绕过 `isDone_`，需要重新设计候选队列、水位和失败回填的一致性。
  - 删除 `eviction_thread_num` 必须同时覆盖 `cli/deploy/conf/worker_config.json`、`k8s_deployment/helm_chart/worker.config`、`k8s/helm_chart/datasystem/values.yaml`、`k8s/helm_chart/datasystem/templates/worker_daemonset.yaml`、`docs/source_zh_cn/deployment/dscli.md`、`docs/source_zh_cn/deployment/k8s_configuration.md` 和示例文档。
  - `DELETE` 和 `END_LIFE` 语义不同：前者只移除本 worker copy metadata，后者删除所有 copy metadata 并结束对象生命周期。
  - spill 写入会释放对象写锁再执行 I/O，之后按 create time 重新加锁校验版本；不要绕过版本校验。
  - direct async spill 是可丢失的临时缓存，不承诺掉电恢复。future 只在 WRITE CQE 全部成功后 ready，随后仍需
    由 eviction manager 精确版本写锁发布 location；CQ线程不得释放 SHM 或修改对象状态。
  - ring 初始化、submit 或 CQ wait 失败时，Manager 必须先取消并销毁 ring，再降级到异步 buffered
    `pwrite`。两种 async backend 都不执行 DATASYNC，避免失败完成后仍有 CQE 引用已释放请求。
  - `spill_io_stats.cumul_spill_in_bytes` 记录完整 WRITE 成功的 record 字节；用两个资源日志样本做差并除以
    时间可得到 Worker 总 spill 文件写带宽。`[SpillAsyncStats]` 额外输出每 Manager 的区间/累计字节和 MiB/s；
    direct backend 对应 O_DIRECT WRITE CQE，底层块设备带宽和写放大仍需用 `iostat` 交叉验证。
  - 当前分支的 primary end-life lane 已使用 `ids_with_version` 并处理 `failed_object_keys` / `outdated_objs`；
    `EvictSpilledObjects` 和 `SpillImpl` fallback 保留的同步 `DeleteNoneL2CacheEvictableObject` 仍使用
    `object_keys`。
  - primary end-life lane 在短锁复核后释放对象锁，由当前 drain 线程同步发送带
    `async_delete=true` 的 `DeleteAllCopyMeta`；Master 入队成功返回后，Worker 重新获取对象锁并复核
    version，再做本地 erase。该方案不引入前台可见 pending 状态，也不改变 spill eviction 和 spill
    no-space fallback 的同步语义。
  - 初始 owner 的 redirect 响应不会在 source 调用栈内同步转发；Worker 释放 source owner lane 后按 target 聚合并
    调度一次。源响应同时携带 `last_rc` 和 redirect 时仍先处理可归因的 redirect；无法归因的非 redirect
    key 保守回填。目标失败只回填该组，不能进入源 owner 的三次超时强制删除策略。
- Important invariants:
  - 对象被选为候选后必须先取得对象写锁；拿不到锁时从 eviction list 暂时移除并以 `READD_COUNTER` 回填。
  - `IsObjectEvictable` 必须确认对象仍在 eviction list 且 binary 对象仍有 shm。
  - `SPILL` 成功后才释放内存并标记 spill state；重加锁失败时需要回滚 spill 文件。
  - 迁移成功后的异步本地释放以 `ObjectTable` 为所有权真值：精确版本写锁校验通过后先从
    `ObjectTable` 删除，再清理派生的 eviction-list membership。派生清理失败最多留下可自愈的陈旧候选，
    不能先摘 eviction node 再依赖可能失败的反向恢复，否则会留下仍可访问但永不驱逐的活对象。
  - spill eviction 只删除 write-through、write-back 且 writeback done、或 `NONE_L2_CACHE_EVICT` 对象。
  - 对 `NONE_L2_CACHE_EVICT`，真实 `EVICTION` 表示本地数据已经消失，即使还有 migration-inflight location
    也删除整条 metadata；`NORMAL` 只删除请求中的 location，仅当最后一个 location 消失时删除整条 metadata。
  - 路由失败清理 pending 并回到 eviction list，批量限频记录诊断日志；Master 业务失败的对象同样回到 eviction list。仅 source owner 在相同 topology version 的三个调度轮次
    返回可重试 RPC 通信错误时，打印 force-delete ERROR 日志并强制释放本地对象。source 成功 redirect 后重置原路由失败预算，redirect target 的 RPC
    错误只回填该 target group。
  - eviction `RemoveMeta`、primary end-life `DeleteAllCopyMeta` 和同步 fallback 都只允许初始 metadata
    owner 重定向一次；转发请求使用 `redirect=false`，目标若仍返回 redirect、`meta_is_moving` 或 failed
    ids，则相关对象保留重试资格，不继续递归转发。
- Observability or debugging hooks:
  - Logs: `Eviction start`, `Evict is going on`, `EvictionList size before/after evict`, `Spill eviction list size before/after evict`。
  - Worker primary end-life 使用 `PRIMARY_END_LIFE_DIAG` 标记 `eviction_summary`、`dequeue`、`route_group`、
    `prepare`、`rpc_attempt`、`local_cleanup` 和 `drain_batch`。正常阶段使用
    `VLOG(1)`；阶段耗时或
    queue wait 达到 100 ms、RPC 返回错误或 per-key 失败时使用 `LOG(WARNING)`。
  - `eviction_summary`、`route_group`、`prepare`、`rpc_attempt` 和 `local_cleanup` 同时记录
    `event=start/complete`，最后一条 start 没有对应 complete 时可直接定位停留阶段；
    `eviction_summary.elapsed_ms` 是整个主 `EvictionTask` 的耗时。
  - primary end-life 阶段日志包含 pending、ready、delayed、owner waiting、in-flight owner、active drain 和 pending limit；
    `dequeue` 额外记录 oldest task 的 `queue_wait_ms`，RPC 日志记录 master、topology version、source/redirect、
    deferred、attempt 和单轮/累计耗时。
  - Perf keys: `WORKER_EVICT_LIST_ADD`, `WORKER_EVICT_LIST_ERASE`, `WORKER_EVICT_LIST_FIND`, `WORKER_EVICT_ONE_OBJECT`, `WORKER_EVICT_DELETE`, `WORKER_EVICT_FREE`。
  - Inject points include `worker.Evict`, `worker.SubmitSpillTask`, `worker.DeleteAllCopyMeta`, `evictAction.setDelete`, `worker.MigrateData.setMaxRetryCount`。

## Design Notes To Revisit

- 当前正式方案：memory eviction 主 loop 的 `END_LIFE` 进入 primary end-life lane，由
  `PRIMARY_END_LIFE_THREAD_NUM=4` 的常驻 drain 线程同步发送 RPC；全局 owner lane 保证同一地址最多一个 RPC 在途，
  pending 上限固定常量，write-back 仅在 lane 重新锁住对象、`DeleteAllCopyMeta` 成功且本地删除成功后移除
  async send queue，metadata 已删除但本地 cleanup 失败的 key 记录为 local-cleanup retry，pending duplicate
  直接视为已有 task 接管，lane drain 内部 queue 并按 master 聚合 batch
  `DeleteAllCopyMeta`，batch 使用 repeated
  `ids_with_version`，lane 用延迟重试和不依赖 eviction list membership 的窄 guard helper 复核状态，发送
  `DeleteAllCopyMeta` 前复查 low watermark 并按对象大小控制 batch 预计释放量以避免大对象过度释放；
  请求设置 `async_delete=true`，Master 使用请求 version 加入 `ExpiredObjectManager`，每次 Worker RPC
  调用预算为 1s；仅明确失败、
  redirect、meta moving 或无法归因的批次错误使用 `READD_COUNTER` 回补 eviction list，且不主动触发
  `Evict()`。
- 每个 owner lease 只发送一次 `DeleteAllCopyMeta`；可重试通信失败延迟 100 ms 后重新读取 placement。相同
  `(owner, topologyVersion)` 的第三次调度失败才打印 ERROR，重新获取对象锁并复核 version，再强制执行本地删除。路由失败清理 pending 并回补 eviction list；Master per-key 拒绝、
  `meta_is_moving`、低水位跳过、锁失败、version 变化和本地 cleanup 失败不触发该可用性兜底。
- 如果未来要让 memory eviction 真正并发，需要先设计候选队列并发、对象锁竞争、低水位判断和 batch flush 的一致性，不能恢复 `eviction_thread_num` 作为调优入口。
- 如果未来扩展到 `EvictSpilledObjects` 或 `SpillImpl` fallback 异步化，需要单独定义 spill eviction list erase、compact 触发和 spill 成功收尾语义。
