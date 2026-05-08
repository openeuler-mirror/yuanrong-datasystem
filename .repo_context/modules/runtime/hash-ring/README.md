# Hash Ring

## Scope

- Path(s):
  - `src/datasystem/worker/hash_ring`
  - `src/datasystem/protos/hash_ring.proto`
  - cluster-manager integration in `src/datasystem/worker/cluster_manager/etcd_cluster_manager.*`
  - master/object-cache, stream-cache, replica-manager, and worker object-cache subscribers to `HashRingEvent`
- Why this module exists:
  - maintain the distributed-master ownership map used to route object and stream metadata;
  - serialize cluster topology and in-progress scale tasks through ETCD-compatible storage;
  - drive metadata/data migration when workers join, leave, fail, restart, or move across AZ-aware routing boundaries.
- Primary source files to verify against:
  - `src/datasystem/worker/hash_ring/hash_ring.cpp`
  - `src/datasystem/worker/hash_ring/hash_ring.h`
  - `src/datasystem/worker/hash_ring/hash_ring_allocator.cpp`
  - `src/datasystem/worker/hash_ring/hash_ring_task_executor.cpp`
  - `src/datasystem/worker/hash_ring/hash_ring_health_check.cpp`
  - `src/datasystem/worker/hash_ring/read_hash_ring.cpp`
  - `src/datasystem/worker/hash_ring/hash_ring_event.h`
  - `src/datasystem/worker/hash_ring/hash_ring_tools.cpp`
  - `src/datasystem/protos/hash_ring.proto`

## Responsibilities

- Verified:
  - owns the local worker's hash-ring state machine: `NO_INIT`, `INIT`, `RUNNING`, `PRE_RUNNING`, `PRE_LEAVING`, `FAIL`;
  - stores the canonical ring as serialized `HashRingPb` at `ETCD_RING_PREFIX` (`/datasystem/ring`) and updates it mostly through `EtcdStore::CAS`;
  - assigns four virtual tokens per worker by default through `HashRingAllocator::defaultHashTokenNum`;
  - initializes the first cluster ring, adds `INITIAL` workers to an already initialized ring, removes failed workers, and marks voluntary scale-down workers;
  - converts ring changes into `HashRingEvent` callbacks for metadata migration, data cleanup, replica lifecycle, redirect checks, and cross-AZ cleanup;
  - keeps local read-side maps (`tokenMap_`, `workerUuid2AddrMap_`, `workerAddr2UuidMap_`, `relatedWorkerMap_`) derived from `HashRingPb`;
  - runs `HashRingHealthCheck` to detect long-stuck scale-up, scale-down, initial, joining, and leaving states, optionally self-healing when `enable_hash_ring_self_healing=true`;
  - provides `ReadHashRing` for read-only rings from other AZs when multi-cluster routing is enabled.
- Pending verification:
  - exact object-cache and stream-cache side effects for every `HashRingEvent` subscriber;
  - complete interaction with `enable_meta_replica` and the replica DB placement model beyond the paths observed in `HashRingTaskExecutor`.

## Companion Docs

- Matching metadata JSON:
  - `.repo_context/modules/metadata/runtime.hash-ring.json`
- Matching `design.md`:
  - `.repo_context/modules/runtime/hash-ring/design.md`
- Cross-module DFX matrix:
  - `.repo_context/modules/runtime/cluster-management-dfx-matrix.md`
- Matching feature playbook:
  - `.repo_context/playbooks/features/runtime/hash-ring/implementation.md`
- Reason if either is intentionally omitted:
  - not omitted; this module is recovery-sensitive, compatibility-sensitive, and cross-module.

## Module Boundary Assessment

- Canonical module boundary:
  - `src/datasystem/worker/hash_ring` plus the `HashRingPb` wire/persistent schema in `src/datasystem/protos/hash_ring.proto`.
- Candidate sibling submodules considered:
  - ETCD client/proto access stays outside this module; hash ring uses `EtcdStore` but does not own the ETCD RPC implementation.
  - `cluster_manager` is a sibling runtime module; it owns ETCD watches, worker keepalive state, and event dispatch into hash ring.
  - object/stream metadata managers stay outside this module; they subscribe to ring events and execute migrations.
- Why they stay inside the parent module or split out:
  - `HashRingAllocator`, `HashRingTaskExecutor`, `HashRingHealthCheck`, `ReadHashRing`, and `hash_ring_tools` are internal pieces of one persisted state machine and should be read together.

## Key Entry Points

- Public/internal C++ entrypoints:
  - `HashRing::InitWithEtcd(bool isMultiReplicaEnable)`
  - `HashRing::InitWithoutEtcd(bool isMultiReplicaEnable, const std::string &hashRing)`
  - `HashRing::HandleRingEvent(const mvccpb::Event &, const std::string &prefix)`
  - `HashRing::UpdateRing(const std::string &, int64_t version, bool forceUpdate = false)`
  - `HashRing::InspectAndProcessPeriodically()`
  - `HashRing::RemoveWorkers(const std::unordered_set<std::string> &workers)`
  - `HashRing::VoluntaryScaleDown()`
  - `HashRing::GetMasterAddr`, `GetMasterUuid`, `GetPrimaryWorkerUuid`, `GetUuidInCurrCluster`
  - `ReadHashRing::UpdateRing` for other-AZ ring snapshots
- Persistent schema:
  - `WorkerPb`: worker `hash_tokens`, `worker_uuid`, `state`, `need_scale_down`
  - `ChangeNodePb`: changed hash ranges for scale-up or scale-down work
  - `HashRingPb`: `workers`, `cluster_has_init`, `add_node_info`, `del_node_info`,
    `key_with_worker_id_meta_map`, `update_worker_map`
- Config flags or environment variables:
  - `enable_distributed_master`: false maps the local state to centralized-master `NO_INIT`;
  - `auto_del_dead_node`: enables automatic failed-worker deletion when distributed master is enabled;
  - `add_node_wait_time_s`: delay before initial token generation or adding workers to an existing ring;
  - `node_dead_timeout_s`: participates in hash-ring health-check timing;
  - `enable_hash_ring_self_healing`: enables automatic repair of some stuck ring states;
  - `rolling_update_timeout_s`: retention window for `update_worker_map`;
  - `etcd_address` / `metastore_address`: backend address selection, with Metastore preferred if set.

## Main Dependencies

- Upstream callers:
  - `EtcdClusterManager` constructs `HashRing`, passes ring and cluster watch events, runs periodic inspection, and delegates routing APIs to it.
  - worker shutdown / lossless exit paths call `EtcdClusterManager::VoluntaryScaleDown`, which delegates to `HashRing::VoluntaryScaleDown`.
- Downstream modules:
  - ETCD-compatible storage through `EtcdStore`.
  - object-cache and stream-cache metadata managers via `HashRingEvent::MigrateRanges`, `RecoverMetaRanges`,
    `RecoverAsyncTaskRanges`, `ClearDataWithoutMeta`, and `LocalClearDataWithoutMeta`.
  - object-cache worker service via `BeforeVoluntaryExit`, `DataMigrationReady`, and local clear-data events.
  - replica manager via `ClusterInitFinish`, `ScaleupFinish`, and `ScaleDownFinish`.
  - cluster manager via `SyncClusterNodes`, `GetFailedWorkers`, `GetDbPrimaryLocation`, and redirect callbacks.
- External dependencies:
  - ETCD-compatible KV and watch semantics, including CAS, range get, lease-backed cluster table, and monotonic revisions.

## Main Flows

### Hash Ring Initialization

1. `EtcdClusterManager::Init` starts ETCD watches for ring and cluster tables, then calls `HashRing::InitWithEtcd` or `InitWithoutEtcd`.
2. `InitWithEtcd` initializes master address through `ETCD_MASTER_ADDRESS_TABLE` if `master_address` is empty.
3. It reads an old ring snapshot from the selected backend address key and writes/updates `ETCD_RING_PREFIX` by CAS through `HashRing::InitRing`.
4. `InitRing` detects restart vs first start by `workers[workerAddr_]`, may reuse `update_worker_map`, and inserts a new `INITIAL` worker when missing.
5. If the existing ring is already initialized, `UpdateWhenNodeRestart` force-updates local state so the worker can restore unfinished scaling tasks.
6. In distributed-master mode, `HashRingHealthCheck` starts after init.

Important nuance:

- If both `etcd_address` and `metastore_address` are empty, `InitWithEtcd` returns early; worker startup validation normally rejects this before runtime.
- If `enable_distributed_master=false`, `UpdateLocalState` maps to `NO_INIT`; the ring still records enough UUID/startup information for cluster-manager behavior.

### First Cluster Token Generation

1. Periodic `HashRing::InspectAndProcessPeriodically` sees local state `INIT` and `cluster_has_init=false`.
2. After a short startup wait, `TryFirstInit` attempts CAS on `ETCD_RING_PREFIX`.
3. Only the first `MAX_CANDIDATE_WORKER_NUM` sorted workers generate tokens to reduce CAS conflicts.
4. `HashRingAllocator::GenerateAllHashTokens` evenly divides `UINT32_MAX` into `workerNum * defaultHashTokenNum` positions, assigns tokens round-robin by sorted worker address, marks all workers `ACTIVE`, and sets `cluster_has_init=true`.
5. Ring watch events call `UpdateRing`, which rebuilds local maps and emits `ClusterInitFinish`.

### Add Node / Scale Up

1. A new worker inserts itself into `HashRingPb.workers` as `INITIAL` during `InitRing`.
2. Existing workers periodically call `TryAdd` when the ring is initialized and no conflicting scale task is active.
3. `HashRing::AddNode` computes workers in `INITIAL` state with no tokens and creates a new ring by CAS.
4. `HashRingAllocator::AddNode` repeatedly splits the largest ownership range, taking about 55% of that range for each new virtual token.
5. `GetAddNodeInfo` records, per new destination worker, which source workers must migrate which ranges.
6. `HashRingTaskExecutor::SubmitScaleUpTask` asynchronously notifies `MigrateRanges`; on success it CAS-marks matching `add_node_info` ranges finished.
7. `HashRingAllocator::FinishAddNodeInfoIfNeed` clears `add_node_info` and transitions all joining workers to `ACTIVE` once all ranges are finished.

### Passive Remove / Failed Node Scale Down

1. `EtcdClusterManager` tracks failed workers from keepalive events and calls `HashRing::RemoveWorkers(GetFailedWorkers())`.
2. `RemoveWorker` first chooses a bounded set of process workers derived from related workers and `MAX_CANDIDATE_WORKER_NUM`; only those workers attempt to write `del_node_info`.
3. The selected worker CAS-checks that local `ringInfo_` still matches ETCD, then uses `HashRingAllocator::RemoveNode`.
4. `RemoveNode` transfers the failed node's ranges to the next available node and records recovery work under `del_node_info[failedWorker]`.
5. `HashRingTaskExecutor::SubmitScaleDownTask` responds to incremental `del_node_info` and either recovers from ETCD/l2cache or, when multi-replica is enabled, submits migration toward the standby primary DB.
6. After recovery, `EraseFinishedDelNodeInfo` removes completed `del_node_info` entries and erases the worker from `workers`.

### Voluntary Scale Down

1. Worker shutdown/lossless-exit flow calls `HashRing::VoluntaryScaleDown`, which CAS-sets `workers[local].need_scale_down=true`.
2. After `UpdateRing`, `UpdateLocalState` sets `needVoluntaryScaleDown_`.
3. Periodic `InspectAndProcessPeriodically` waits for the local node to be primary for its DB, then calls `GenerateVoluntaryScaleDownChangingInfo`.
4. That CAS changes the worker to `LEAVING` and uses `HashRingAllocator::RemoveNodeVoluntarily` to write `add_node_info` toward standby workers.
5. Scale-up-style metadata migration runs; when uuid-range migration finishes, `ClearTokenForScaleDown` moves `key_with_worker_id_meta_map`, writes `update_worker_map`, clears the leaving worker UUID, and removes matching hash tokens.
6. After metadata is migrated and `DataMigrationReady` succeeds, `SubmitMigrateDataTask` notifies `BeforeVoluntaryExit`, then CAS-erases the worker from the ring.

### Restart / Rolling Upgrade

1. `InitRing` sees an existing `workers[workerAddr_]` entry and sets `StartUpState::RESTART`.
2. If `update_worker_map[workerAddr_]` exists, the worker may reuse the previous UUID.
3. `AddUpgradeRange` injects point-range migration work (`from == end`) so metadata with the reused UUID can move back.
4. `ClearWorkerMapOnInterval` removes stale `update_worker_map` entries after `rolling_update_timeout_s` unless a worker is waiting for UUID restoration.

### Other-AZ Read Ring

1. `EtcdClusterManager::ConstructOtherAzHashRing` creates `ReadHashRing` instances for configured other AZs.
2. `ReadHashRing::UpdateRing` parses other-AZ ring events, marks itself `RUNNING` after `cluster_has_init`, updates maps, and emits `OtherAzNodeDeadEvent` or `LocalClearDataWithoutMeta` for cross-AZ deletion scenarios.
3. Cross-AZ route queries use the read ring to rehash or translate worker UUIDs into addresses.

## Build And Test

- Build commands:
  - CMake: `./build.sh -t ut -j <N>` or nearest project build target that includes `worker_hash_ring`
  - Bazel target: `//src/datasystem/worker/hash_ring:hash_ring`
- Fast verification commands:
  - `./build.sh -t ut -- -R hash_ring_allocator_test`
  - `./build.sh -t ut -- -R hash_ring_health_check_test`
- Representative tests:
  - `tests/ut/worker/object_cache/hash_ring_allocator_test.cpp`
  - `tests/ut/worker/object_cache/hash_ring_health_check_test.cpp`
  - `tests/st/worker/object_cache/hash_ring_test.cpp`
  - `tests/st/client/kv_cache/kv_client_hashring_healing_test.cpp`
  - scale and voluntary-scale system tests under `tests/st/client/kv_cache/*scale*`

## Review And Bugfix Notes

- Common change risks:
  - any change to `HashRingPb` field semantics changes persisted ETCD state and ring watch compatibility;
  - ring updates are CAS-heavy and usually retried by many workers, so harmless-looking loops can amplify ETCD pressure;
  - `HashRingPb` and local derived maps are separate state copies, so bugs often appear as stale route maps rather than malformed protobuf;
  - `add_node_info` and `del_node_info` are both task queues and state-machine guards; clearing either too early can lose migration/recovery work;
  - `from == end` is overloaded for UUID metadata migration and must not be simplified as an empty range;
  - `state_ == FAIL` is terminal in `ChangeStateTo`; avoid code that expects recovery from `FAIL` in-process.
- Important invariants:
  - active/leaving workers with hash tokens populate `tokenMap_`;
  - `cluster_has_init=true` means the ring can route, but pending `add_node_info` or `del_node_info` still means migration or recovery is active;
  - scale-up and scale-down are serialized by checks in `GetAddingWorkers` and `GetLeavingWorkers`;
  - passive deletion requires `enable_distributed_master && auto_del_dead_node`;
  - worker UUID mappings must stay consistent across `workers`, `key_with_worker_id_meta_map`, and `update_worker_map`.
- Observability or debugging hooks:
  - logs from `SummarizeHashRing`, `SplitRingJson`, `HashRingHealthCheck`, and `HashRingTaskExecutor`;
  - inject points beginning with `HashRing.*`, `hashring.*`, `worker.HashRingHealthCheck`, and `voluntaryscaledown.*`;
  - `worker_cli.cpp` can read/write ring JSON through ETCD for inspection or manual recovery.

## Current Design Weaknesses

- `HashRingPb` is a single hot CAS key. Initial join, scale-up, passive scale-down, voluntary scale-down, rolling upgrade cleanup, and health-check self-healing all contend on `/datasystem/ring`.
- The module uses both persisted ETCD state and in-memory derived state; correctness depends on watches, revision baselines, `MessageDifferencer`, and periodic repair staying aligned.
- The state machine is spread across `HashRing::UpdateLocalState`, `InspectAndProcessPeriodically`, `HashRingAllocator`, `HashRingTaskExecutor`, `HashRingHealthCheck`, and `EtcdClusterManager`, which makes lifecycle reasoning hard.
- The event bus hides strong cross-module dependencies. Hash ring code calls object/stream metadata migration, replica updates, cluster-node sync, and worker shutdown hooks without a typed ownership boundary.
- Scale task records are stored inside the same protobuf as routing metadata. This couples routing reads to migration progress and makes partial failure cases difficult to reason about.
- Self-healing can erase workers or pending task records when enabled. This is operationally useful, but it also means a repair policy can mutate the same authoritative ring state used for routing.

## Open Questions

- Whether existing deployments rely on serialized `HashRingPb` compatibility across rolling upgrades.
- Whether `MAX_CANDIDATE_WORKER_NUM=5` sufficiently bounds CAS conflict under large-scale failover, or only reduces first-init/add/remove writer count.
- Whether `HashRingPb` should be split into separate routing, membership, and task-state records to reduce CAS pressure and simplify ownership.
