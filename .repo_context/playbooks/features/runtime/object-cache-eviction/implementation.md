# Object Cache Eviction Implementation Playbook

## Metadata

- Status:
  - `active`
- Feature scope:
  - mixed
- Owning module or area:
  - `runtime.object-cache-eviction`
- Primary code paths:
  - `src/datasystem/worker/object_cache/worker_oc_eviction_manager.*`
  - `src/datasystem/worker/object_cache/eviction_list.*`
  - `src/datasystem/worker/object_cache/worker_oc_spill.*`
  - `src/datasystem/worker/object_cache/obj_cache_shm_unit.cpp`
  - `src/datasystem/worker/stream_cache/worker_sc_allocate_memory.cpp`
  - `src/datasystem/worker/object_cache/worker_master_oc_api.*`
  - `src/datasystem/master/object_cache/oc_metadata_manager.*`
  - `src/datasystem/protos/master_object.proto`
- Related module docs:
  - `.repo_context/modules/runtime/object-cache-eviction/README.md`
  - `.repo_context/modules/runtime/worker-runtime.md`
  - `.repo_context/modules/infra/l2cache/README.md`
- Related design docs:
  - `.repo_context/modules/runtime/object-cache-eviction/design.md`
  - Detailed Chinese design notes are kept out of the repository and carried in the PR description or local workspace
    notes.
- Related tests or validation entrypoints:
  - `tests/ut/worker/object_cache/worker_oc_eviction_test.cpp`
  - `tests/ut/worker/object_cache/worker_oc_spill_eviction_test.cpp`
  - `tests/st/client/kv_cache/kv_cache_client_evict_test.cpp`
- Last verified against source:
  - `2026-08-20`

## Purpose

- Why this playbook exists:
  - eviction 改动横跨 shared memory、对象锁、spill 文件、master metadata、L2 可恢复性和后台线程。
- What change class it standardizes:
  - 修改候选选择、水位阈值、动作状态机、`NONE_L2_CACHE_EVICT` 生命周期、spill eviction、远端迁移、master 删除 RPC 或线程配置。
- What risks it is meant to reduce:
  - foreground OOM 放大、对象误删、write-back 数据丢失、master metadata 不收敛、对象锁长时间持有、后台任务无法退出或 retry storm。

## When To Use This Playbook

- Use when:
  - 修改 `worker_oc_eviction_manager.*`、`eviction_list.*`、`worker_oc_spill.*`；
  - 修改 object/stream allocation 中的 eviction trigger；
  - 修改 `DeleteAllCopyMeta` / `RemoveMeta` 在 eviction 中的使用方式；
  - 修改 `NONE_L2_CACHE_EVICT`、write-through、write-back、disk cache type 的 eviction 语义；
  - 修改 memory eviction 单任务模型、spill 配置、batch threshold 或线程池行为。
- Do not use when:
  - 只修改 L2 backend 持久格式；用 L2/slot playbook。
  - 只修改 hash-ring 扩缩容路由；用 hash-ring 或 cluster-manager playbook。
- Escalate to design-first review when:
  - 要让 memory eviction 多任务并发；
  - 要把 `END_LIFE` 改成异步或批量；
  - 要改变 `DeleteAllCopyMetaReqPb` schema 或 version 语义；
  - 要允许 write-back 未完成对象被删除；
  - 要改变 failure retry 或对象 table erase 顺序。

Note:

- 当前 primary end-life lane 已在短锁校验后释放对象锁，由 drain 线程同步发送带
  `async_delete=true` 的 RPC；Master 入队成功返回后重新锁住对象并复核 version。不要重新扩大范围到
  `EvictSpilledObjects` 或 `SpillImpl` fallback，也不要引入前台可见 pending 状态，除非先形成新的设计
  结论。

## Preconditions

- Required context to read first:
  - `.repo_context/modules/runtime/object-cache-eviction/README.md`
  - `.repo_context/modules/runtime/object-cache-eviction/design.md`
  - `.repo_context/modules/overview/engineering-principles.md`
  - `.repo_context/modules/quality/tests-and-reproduction.md`
- Required source files to inspect first:
  - `worker_oc_eviction_manager.cpp`
  - `worker_oc_eviction_manager.h`
  - `eviction_list.cpp`
  - `worker_oc_spill.cpp`
  - `worker_master_oc_api.cpp`
  - `oc_metadata_manager.cpp`
  - `master_object.proto`
- Required assumptions to verify before coding:
  - change touches memory eviction, spill eviction, or both；
  - object is primary or non-primary copy；
  - object has L2, write-back done, or none-L2-evict；
  - local spill, remote spill, or no spill deployment；
  - master RPC failure behavior and version protection；
  - foreground allocation impact。

## Request Intake

- Requested behavior change:
  - record exact object modes, cache type, resource pressure, and expected master metadata result.
- Explicit non-goals:
  - state whether L2 backend, hash-ring, or client API semantics are intentionally unchanged.
- Affected users, processes, or services:
  - worker object cache, stream allocator, master metadata, local spill filesystem, possibly other workers.
- Backward-compatibility expectations:
  - preserve protobuf compatibility and old object version safety unless explicitly migrating.

## Risk Classification

| Risk Area | Question to answer before implementation | Low-risk signal | Escalation signal |
| --- | --- | --- | --- |
| object lifecycle | can a live/newer object be erased? | create time/version rechecked | erase without version or lock |
| data durability | is there a recoverable L2/spill copy? | write-through or write-back done | write-back pending or none-L2 unique copy |
| metadata consistency | which master API is used? | same `RemoveMeta`/`DeleteAllCopyMeta` semantics | new delete ordering or schema |
| concurrency | who owns object lock and pending state? | lock scope unchanged or shorter with recheck | RPC/I/O under new locks or multi-task eviction |
| performance | does foreground allocation wait longer? | async/batch unchanged or improved | synchronous RPC/I/O added to main loop |
| operations | can config be misunderstood? | docs/logs updated | flag behavior changes without migration note |
| recovery | what happens after worker restart? | ST restart behavior unchanged | local erase before durable/global state is safe |

## Source Verification Checklist

- [ ] confirm `GetObjectNextAction` branch for the affected mode
- [ ] confirm whether object write lock is held across the modified code
- [ ] confirm object table erase/free/spill state order
- [ ] confirm `RemoveMeta` vs `DeleteAllCopyMeta` semantics
- [ ] confirm whether the path currently uses `object_keys` or `ids_with_version`; if adding async or retry behavior, preserve create-time/version safety
- [ ] confirm failed keys are re-added or intentionally terminal
- [ ] confirm spill file rollback on failure
- [ ] confirm allocation trigger and high/low watermark impact
- [ ] confirm representative UT/ST coverage or add it

## Design Checklist

- [ ] identify the narrowest extension point
- [ ] state whether the change is local worker-only or master-visible
- [ ] state whether the change affects object lock hold time
- [ ] state whether the change affects foreground allocation latency
- [ ] state whether the change affects persistent spill files or master metadata
- [ ] state whether config docs or operator expectations change
- [ ] define rollback or feature-gating if behavior is risky
- [ ] if implementing primary end-life lane, confirm it only covers memory eviction main-loop `Action::END_LIFE`
- [ ] if implementing primary end-life lane, confirm `EvictSpilledObjects` and `SpillImpl` no-space fallback remain synchronous unless a separate design says otherwise

## Implementation Plan

1. Map the target behavior to one current action: `DELETE`, `FREE_MEMORY`, `SPILL`, `MIGRATE`, `END_LIFE`, or `RETAIN`.
2. Keep changes inside that action path unless the request explicitly changes the state machine.
3. Preserve object lock and create-time/version checks; if shortening lock scope, add a post-RPC/post-I/O recheck.
4. Preserve failure re-add behavior for retryable candidates.
5. Add or update focused UT for local state-machine/list behavior.
6. Add or update ST when master metadata, restart, cross-worker, or client-visible semantics change.
7. Update `.repo_context/modules/runtime/object-cache-eviction/*` if behavior, tests, or config semantics change.

## Formal Primary END_LIFE Lane Plan

Use this section when implementing the formal primary end-life lane plan.

- Scope:
  - cover memory eviction main loop `Action::END_LIFE` for both `NONE_L2_CACHE_EVICT` and `WRITE_BACK_L2_CACHE_EVICT`;
  - keep `EvictSpilledObjects` synchronous;
  - keep `SpillImpl` none-L2 no-space fallback synchronous.
- Threading:
  - use the source-fixed `PRIMARY_END_LIFE_THREAD_NUM=4` `primaryEndLifeThreadPool_`;
  - start four persistent drains; each drain sends one owner batch synchronously, then returns to the ready/delayed scheduler;
  - maintain a Worker-global owner lane keyed by `HostPort`; the same owner may have at most one RPC in flight, while different owners may use different drains concurrently;
  - when one raw batch contains multiple free owners, let the current drain lease one deterministic owner and return the other groups to the ready queue for other drains;
  - do not reuse `masterTaskThreadPool_`;
  - do not add user-visible thread-count or pending-limit flags.
- Pending:
  - maintain `objectKey -> version`, where version is `entry->GetCreateTime()`;
  - use pending size as the authoritative backpressure counter;
  - use the source constant `PRIMARY_END_LIFE_PENDING_LIMIT = 1024`;
  - remember that the pending limit bounds key count, not bytes; large queued primary objects are controlled by the low-watermark recheck and per-batch release budget before `DeleteAllCopyMeta`, not by a global pending-bytes budget in this plan;
  - clear pending only if the stored version matches the task version;
  - if the key is already pending, treat the existing lane task as owner: return OK, do not call
    `asyncSendManager_.Remove(objectKey)`, do not submit another task, and do not re-add the key.
- Main loop behavior:
  - check duplicate/pending capacity first, then write pending;
  - do not call `asyncSendManager_.Remove(objectKey)` in the main loop after pending reserve; pending accepted only means
    the lane may try end-life later, not that local deletion is guaranteed;
  - for `WRITE_BACK_L2_CACHE_EVICT`, call `asyncSendManager_.Remove(objectKey)` only after the lane has locked the
    object and local deletion succeeded, either after Master acceptance or after three RPC communication failures;
  - if `DeleteAllCopyMeta` succeeded but local cleanup failed, remember the key/version as metadata-deleted and retry
    local cleanup without sending another `DeleteAllCopyMeta`;
  - do not call `asyncSendManager_.Remove(objectKey)` on pending duplicate, pending full, enqueue failure, low-watermark
    skip, lock failure, version mismatch, redirect/meta moving, or any other re-add path;
  - enqueue success means the task was pushed to the primary end-life queue and accepted by the lane, not "memory freed";
  - enqueue failure, pending full, or thread-pool submit exception returns an error so the main loop re-adds the key;
- Lane behavior:
  - drain a bounded snapshot from the ready queue using the 10 ms flush window, and promote delayed retries with the scheduler condition variable;
  - group every dequeued or awakened task again with `MetadataRouteResolver::GroupOwners()`; same-master keys use one batch, while route failures clear pending and re-add to the eviction list so they cannot occupy the bounded pending queue indefinitely;
  - preserve the batch topology version. Source retry budget is keyed by `(owner, topologyVersion)` and resets when either value changes or the source successfully redirects to another owner;
  - park tasks in `owner.waitingTasks` when that owner already has an in-flight RPC; release uses allocation-free list splice to move them back to the unrouted ready queue so they observe the latest topology;
  - consume active isolation through the latest placement snapshot: after a FAILURE topology moves the key from owner A
    to recovery owner B, no new RPC may be sent to A. Do not equate `MemberState::FAILED` with metadata deletion;
  - classify grouping errors per key: `K_RPC_UNAVAILABLE` as master/connection unavailable and `K_NOT_FOUND` as route/meta-address unavailable; grouping failures do not send RPC and must not erase local objects;
  - before RPC, acquire object WLock with fixed short retry for `K_TRY_AGAIN`; use source constants such as
    `PRIMARY_END_LIFE_LOCK_RETRY_TIMES = 3` and `PRIMARY_END_LIFE_LOCK_RETRY_INTERVAL_MS = 1`;
  - do not treat the first lock conflict as business failure because the main loop may still be releasing WLock;
  - acquire multiple object WLocks in a stable order, preferably sorted by object key; never hold the pending mutex or queue mutex while acquiring object WLocks or sending RPC;
  - use a narrow guard helper to revalidate version, evictable, primary, and end-life mode; do not fully replay `GetObjectNextAction` with invented `pendingSpillSize`;
  - the lane guard must not call the current `IsObjectEvictable()` directly because that helper requires
    `memEvictionList_.Exist(objectKey)` and may erase the list, while accepted lane tasks are already removed from the list;
  - before sending each batch `DeleteAllCopyMeta`, recheck the current memory low watermark using the same eviction
    waterline semantics and the triggering allocation `needSize`; if low watermark is already reached, skip this
    end-life task, do not send RPC, do not erase the local object, clear pending, re-add to `memEvictionList_` with
    `READD_COUNTER`, and do not actively call `Evict()`;
  - while building a batch, account for each candidate's expected released size, preferably `entry->GetDataSize() + entry->GetMetadataSize()` adjusted to the actual local-memory release semantics; avoid adding more keys once the batch would clearly exceed the current space above low watermark, except that one oversized object may still be selected to make progress;
  - use repeated `ids_with_version` for batch `DeleteAllCopyMeta`; do not use `object_keys` in the new eviction lane;
  - set `async_delete=true`; the Master must preserve the request version when it inserts the key into
    `ExpiredObjectManager` and return after enqueue succeeds;
  - set a 1s total API budget per Worker RPC call through `reqTimeoutDuration` with RAII reset;
  - treat `GetMetaAddress()` unavailable errors as fast skip without sending RPC; classify `K_RPC_UNAVAILABLE` as
    master/connection unavailable and `K_NOT_FOUND` as route or meta-address unavailable rather than always calling it a failed master;
  - for `async_delete=true`, treat successful Master enqueue as acceptance and retry `failed_object_keys` and redirect
    keys; RPC failure, `meta_is_moving`, or a non-OK `last_rc` without specific failed keys fails the whole batch;
  - on the Master, make `PENDING` an explicit failed key and prevent cleanup after a key was already rejected or after
    metadata reappeared following the initial no-meta lookup; do not add a reverse version-bound check;
  - do not add a new dependency on `DeleteAllCopyMetaRspPb.delete_result`; current master delete paths communicate the
    relevant failure/version result through `failed_object_keys`, `outdated_objs`, `objs_without_meta`, redirect info,
    `meta_is_moving`, and `last_rc`;
  - on success only, delete spill file if needed, clear spill state, and erase object table;
  - send only one source RPC per owner lease. A retryable transport error releases the owner lane and schedules the task
    after 100 ms; do not synchronously send attempt two or three in the same drain call;
  - after the third retryable communication error for the same `(owner, topologyVersion)`, log an ERROR, reacquire the
    object WLock, revalidate version, and force local deletion;
  - `meta_is_moving` stays pending and delayed. Route failures, Master per-key failures, and non-RPC batch errors remain
    conservative failures that clear pending and re-add to `memEvictionList_` with `READD_COUNTER`; route-failure logging must be aggregated and rate limited;
  - parse source redirect without forwarding in the source call stack. Release the source owner lane, then schedule a
    single `redirect=false` target attempt through the same global owner lanes and with the source deadline remainder;
  - do not actively call `Evict()` after re-adding from the lane.
- Timeout and shutdown:
  - remote master uses a 1s budget for each source `WorkerRemoteMasterOCApi::DeleteAllCopyMeta` call; retries are separate
    delayed rounds, and a redirect target consumes the source call's remaining logical budget;
  - local-bypass master uses request timeout and master-side `timeoutDuration`, but this is not a transport-level forced interrupt;
  - set the scheduler stopping flag and notify all four persistent drains before resetting `primaryEndLifeThreadPool_`;
  - reset `primaryEndLifeThreadPool_` in `WorkerOcEvictionManager` destruction before dependencies used by lane tasks can be released.
- Observability:
  - use `PRIMARY_END_LIFE_DIAG` stages for eviction summary, dequeue, route grouping, candidate prepare, RPC attempt,
    local cleanup, and complete drain batch;
  - pair `event=start/complete` for the main eviction summary, route grouping, candidate prepare, RPC attempt, and
    local cleanup so the last unmatched start identifies the stalled stage;
  - record the full `EvictionTask` elapsed time in the completed eviction summary to distinguish main-loop stalls from
    primary end-life lane backlog;
  - use `VLOG(1)` for normal completion and `LOG(WARNING)` when stage latency or oldest queue wait reaches 100 ms,
    or when an RPC/per-key result is unsuccessful;
  - include pending, ready, delayed, owner waiting, in-flight owner, active drain, and pending limit in the pressure snapshot;
  - record master, attempt number, single-attempt latency, and cumulative latency for `DeleteAllCopyMeta`;
  - take the queue snapshot under `primaryEndLifeMutex_`, but format and emit logs after releasing the mutex.
- Locking:
  - release object WLock before the RPC and reacquire it with version validation before local erase;
  - do not add foreground-visible `END_LIFE_PENDING` / `OBJECT_DELETING` state.

## Guardrails

- Must preserve:
  - `NONE_L2_CACHE_EVICT` primary copy normally waits for Master end-life acceptance; the explicit availability
    override permits forced local erase after three retryable RPC communication failures for the same route identity；
  - write-back objects are only treated as L2-existing after writeback done；
  - async spill revalidates version before freeing shm；
  - failed candidates return to an eviction list unless object no longer exists；
  - 如果修改 eviction end-life 的异步、重试或队列行为，需要使用 create-time/version 防止误删新 incarnation。
  - primary end-life lane 入队成功只表示任务已接管；实际内存释放必须等 lane 成功 erase。
  - primary end-life lane 的 Master 业务失败回补 eviction list 时使用 `READD_COUNTER`，且不主动调用
    `Evict()`；路由失败清理 pending 并回补 eviction list，仅同一 `(owner, topologyVersion)` 三轮可重试通信失败进入强制本地删除。
- Must not change without explicit review:
  - `isDone_` single-task gate；
  - memory eviction 线程数、单任务门闩或等价运行时并发配置；
  - `DeleteAllCopyMetaReqPb` fields or signing-sensitive order；
  - object table erase order for `END_LIFE`；
  - spill no-space fallback for none-L2-evict；
  - write-back pending eviction policy。
- Must verify in source before claiming:
  - exact thread pool affected by a config flag；
  - master-side meaning of `async_delete`；
  - object lock scope around RPC or I/O。
  - memory eviction 线程配置变更需覆盖 worker source, tests, dscli config, k8s deployment, k8s daemonset Helm chart, deployment docs, and compatibility behavior。

## Validation Plan

- Fast checks:
  - `ctest -R EvictionManagerTest`
  - `ctest -R SpillEvictionTest`
- Representative tests:
  - `tests/ut/worker/object_cache/worker_oc_eviction_test.cpp`
  - `tests/ut/worker/object_cache/worker_oc_spill_eviction_test.cpp`
  - `tests/st/client/kv_cache/kv_cache_client_evict_test.cpp`
- Manual verification:
  - force small shared memory and spill limits；
  - inspect logs for `EvictionList size before/after evict` and failed size；
  - inject `worker.DeleteAllCopyMeta` or master failure for negative path；
  - confirm object is not gettable after successful end-life。
- Negative-path verification:
  - master RPC timeout/failure；
  - object version changes during async spill；
  - object lock busy；
  - spill directory full；
  - worker restart after none-L2-evict object。

## Review Checklist

- [ ] change matches requested behavior and non-goals
- [ ] object lock scope is explicit and justified
- [ ] foreground allocation latency impact is assessed
- [ ] master metadata and local object table ordering is safe
- [ ] spill file rollback/cleanup path is safe
- [ ] config flag semantics are documented if changed
- [ ] tests include at least one failure or race-sensitive path when behavior changes
- [ ] context docs are updated when behavior, invariant, or validation path changes

## Context Update Requirements

- Module docs to update:
  - `.repo_context/modules/runtime/object-cache-eviction/README.md`
- Design docs to update:
  - `.repo_context/modules/runtime/object-cache-eviction/design.md`
- Additional playbooks to update:
  - this file if workflow, risks, or validation changes

## Open Questions

- 是否需要新增专门的 ST 覆盖 master RPC 慢/失败时 primary end-life lane 隔离主 eviction loop 的效果。
- 是否需要后续独立方案支持 `EvictSpilledObjects` 异步化或锁外 RPC；当前锁外 RPC 只覆盖 primary lane。
- batch `DeleteAllCopyMeta` 已纳入当前正式 primary end-life lane 方案。

## Pending Verification

- 尚未建立一组专门的性能基准来量化 primary end-life lane 对 `END_LIFE` 同步 RPC 瓶颈的隔离效果。
