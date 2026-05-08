# Cluster Manager

## Scope

- Path(s):
  - `src/datasystem/worker/cluster_manager`
  - startup integration in `src/datasystem/worker/worker_oc_server.cpp`
  - worker readiness and reconciliation integration in `src/datasystem/worker/object_cache/worker_oc_service_impl.cpp`
  - event subscribers in object-cache, stream-cache, replica-manager, slot-recovery, and hash-ring modules
- Why this module exists:
  - track local and other-AZ worker membership from ETCD-compatible watch events;
  - connect ETCD lease keepalive state to worker lifecycle, restart reconciliation, timeout/failure handling, passive scale-down, and voluntary scale-down cleanup;
  - feed hash-ring state machine progress and route metadata requests to the correct worker/replica DB;
  - bridge low-level ETCD events to higher-level metadata, replica, slot-recovery, and worker API cleanup events.
- Primary source files to verify against:
  - `src/datasystem/worker/cluster_manager/etcd_cluster_manager.cpp`
  - `src/datasystem/worker/cluster_manager/etcd_cluster_manager.h`
  - `src/datasystem/worker/cluster_manager/cluster_node.h`
  - `src/datasystem/worker/cluster_manager/worker_health_check.cpp`
  - `src/datasystem/worker/cluster_event_type.h`
  - `src/datasystem/worker/hash_ring/hash_ring_event.h`
  - `src/datasystem/worker/worker_oc_server.cpp`
  - `src/datasystem/worker/object_cache/worker_oc_service_impl.cpp`

## Responsibilities

- Verified:
  - constructs and owns one local `HashRing` and optional `ReadHashRing` instances for other AZs;
  - creates ETCD tables for ring, cluster, replica group, and master address;
  - reads startup `ClusterInfo` from ETCD/Metastore or from local RocksDB during ETCD-down restart;
  - starts ETCD watches for ring, cluster, and replica-group prefixes;
  - enqueues ETCD watch events into a priority queue where ring events outrank cluster events;
  - tracks local and other-AZ workers in `ClusterNode` tables with `ACTIVE`, `TIMEOUT`, and `FAILED` states;
  - interprets `KeepAliveValue.state` as node lifecycle tags: `start`, `restart`, `recover`, `ready`, `exiting`, and `d_rst`;
  - drives passive scale-down by demoting timed-out nodes, notifying metadata/slot-recovery events, and calling `HashRing::RemoveWorkers`;
  - delegates voluntary scale-down to `HashRing::VoluntaryScaleDown` after marking local cluster-manager state as leaving;
  - triggers restart/network-recovery reconciliation through event subscribers;
  - routes object metadata requests through hash ring, read rings, and replica-manager primary DB lookup;
  - exposes a worker readiness probe file through `worker_health_check`.
- Pending verification:
  - exact semantics of every object-cache and stream-cache subscriber for node timeout/recovery;
  - every slot-recovery side effect after `SlotRecoveryFailedWorkersEvent`;
  - operator-visible recovery procedure for manual ring/cluster-table repair.

## Companion Docs

- Matching metadata JSON:
  - `.repo_context/modules/metadata/runtime.cluster-manager.json`
- Matching `design.md`:
  - `.repo_context/modules/runtime/cluster-manager/design.md`
- Cross-module DFX matrix:
  - `.repo_context/modules/runtime/cluster-management-dfx-matrix.md`
- Matching feature playbook:
  - `.repo_context/playbooks/features/runtime/cluster-manager/implementation.md`
- Reason if either is intentionally omitted:
  - not omitted; this module is lifecycle-sensitive and cross-module.

## Module Boundary Assessment

- Canonical module boundary:
  - `src/datasystem/worker/cluster_manager` plus event definitions it directly publishes or subscribes to.
- Candidate sibling submodules considered:
  - `runtime.hash-ring` owns persisted ring state, token/range algorithms, and migration task records.
  - `runtime.etcd-metadata` owns ETCD/Metastore RPC, watch, lease, and CAS mechanics.
  - object-cache and stream-cache metadata managers own actual metadata recovery, reconciliation, and clearing.
  - replica-manager owns primary replica DB selection and replica-group watch handling.
- Why this stays its own module:
  - `EtcdClusterManager` is not a pure wrapper around ETCD or hash ring; it is the runtime lifecycle coordinator that joins ETCD membership, hash-ring state, health probes, route lookup, and event-bus side effects.

## Key Entry Points

- Static/startup entrypoints:
  - `EtcdClusterManager::CreateEtcdStoreTable`
  - `EtcdClusterManager::ConstructClusterInfoViaEtcd`
  - `WorkerOCServer::ConstructClusterInfo`
  - `WorkerOCServer::ConstructClusterInfoDuringEtcdCrash`
- Runtime lifecycle entrypoints:
  - `EtcdClusterManager::Init`
  - `EtcdClusterManager::Shutdown`
  - `EtcdClusterManager::SetWorkerReady`
  - `EtcdClusterManager::CheckWaitNodeTableComplete`
  - `EtcdClusterManager::InformEtcdReconciliationDone`
  - `EtcdClusterManager::VoluntaryScaleDown`
- Event handling entrypoints:
  - `EnqueEvent`
  - `DequeEventCallHandler`
  - `HandleRingEvent`
  - `HandleClusterEvent`
  - `HandleNodeAdditionEvent`
  - `HandleNodeRemoveEvent`
  - `HandleOtherAzNodeAddEvent`
  - `HandleOtherAzNodeRemoveEvent`
  - `DemoteTimedOutNodes`
  - `SyncNodeTableWithHashRing`
  - `CleanupWorker`
- Routing entrypoints:
  - `GetMetaAddress`
  - `GetMetaAddressNotCheckConnection`
  - `GetMasterAddr`
  - `GetPrimaryReplicaLocationByObjectKey`
  - `GetPrimaryReplicaLocationByAddr`
  - `GroupObjKeysByMasterHostPort`
  - `QueryMasterAddrInOtherAz`
- Health probe entrypoints:
  - `ResetHealthProbe`
  - `SetHealthProbe`
  - `IsHealthy`
  - `SetUnhealthy`

## Main Dependencies

- Upstream callers:
  - `WorkerOCServer` creates ETCD store, constructs `ClusterInfo`, initializes `EtcdClusterManager`, starts services, and invokes voluntary scale-down during shutdown.
  - `WorkerOCServiceImpl` checks node table completeness, sets health, updates ETCD node state to `ready`, and informs reconciliation completion.
  - master/object/stream services use route helpers and subscribe to cluster events.
- Downstream modules:
  - ETCD-compatible metadata backend through `EtcdStore`;
  - local and other-AZ hash rings through `HashRing` and `ReadHashRing`;
  - object-cache metadata manager, stream-cache metadata manager, notify-worker managers, replica-manager, slot-recovery manager, worker remote APIs, fast transport cleanup, and health probe file.
- External dependencies:
  - ETCD lease expiration and watch delivery;
  - worker-to-worker RPC for checking ETCD state during local keepalive/network failure;
  - configured cluster/AZ names and startup flags.

## Main Flows

### Cluster Startup With ETCD Available

1. `WorkerOCServer::ConstructClusterInfo` checks backend health, then calls `ConstructClusterInfoViaEtcd`.
2. `ConstructClusterInfoViaEtcd` creates required tables and loads local workers, other-AZ workers, other-AZ hash rings, replica groups, and the ETCD revision.
3. `EtcdClusterManager::Init` installs the ETCD event handler, validates timeout flags, and caches ring/cluster prefixes.
4. It starts the node utility thread and orphan-node monitor before processing startup nodes.
5. `SetupInitialClusterNodes` enqueues fake local-AZ PUT events for already known workers, except the local worker when ETCD is available, and directly inserts other-AZ workers.
6. It starts watches for ring, cluster, and replica-group prefixes from the startup revision.
7. It initializes the local hash ring with ETCD and initializes read rings for other AZs.
8. It starts lease keepalive for the local cluster-table worker key.

Important nuance:

- The cluster-manager background thread is started before hash-ring initialization because hash-ring initialization depends on watch events processed by that thread.
- Cluster events are cached if distributed hash ring is not yet workable; ring events have higher priority so the ring can become workable first.

### Startup During ETCD Crash

1. `WorkerOCServer::ConstructClusterInfo` falls back to `ConstructClusterInfoDuringEtcdCrash`.
2. The worker loads hash-ring, cluster-table, and replica-group information from local RocksDB-backed cluster store.
3. It queries a bounded number of active local workers to reconcile cluster state.
4. `EtcdClusterManager::Init` calls `HashRing::InitWithoutEtcd` with the local ring snapshot.
5. Keepalive is still initialized with `isEtcdAvailableWhenStart=false`, which marks the local state as downgrade restart (`d_rst`).

Important nuance:

- ETCD-down startup is allowed only for distributed-master mode in this path; centralized-master restart without ETCD is rejected.

### Event Queue And Dispatch

1. `EtcdStore` watch callback invokes `EnqueEvent`.
2. Cluster-table events are enqueued as `PrefixType::CLUSTER`.
3. Ring events are enqueued as `PrefixType::RING`.
4. Replica-group events bypass the queue and notify `ReplicaEvent` directly.
5. `DequeEventCallHandler` flushes cached cluster events when possible, pops one priority event, and handles it.
6. Ring events update the local hash ring or the matching other-AZ read ring.
7. Cluster events parse `KeepAliveValue` and dispatch to local-AZ or other-AZ add/remove handlers.

Important nuance:

- Watch compensation in `EtcdWatch` can generate fake events, and cluster-manager itself can generate fake add/delete events for missing nodes. Handlers must distinguish these from real ETCD lease events.

### Node Addition And Scale Up

1. A worker writes a lease-bound cluster-table key through keepalive.
2. Cluster-manager receives a PUT event with state `start`, `restart`, `recover`, `ready`, `exiting`, or `d_rst`.
3. If the node is new, `AddNewNode` inserts an `ACTIVE` `ClusterNode` and removes any stale dead-worker API state.
4. Independently, the joining worker has already inserted itself into `HashRingPb.workers` as `INITIAL` during hash-ring initialization.
5. The node utility thread periodically calls `hashRing_->InspectAndProcessPeriodically`, which can generate first tokens or add-node migration work.
6. Once hash-ring migration finishes, route lookups can send traffic to the new active worker/replica DB.

Important nuance:

- Node table addition and hash-ring scale-up are separate state machines. A node can be `ACTIVE` in `clusterNodeTable_` while still not active or not ready in the hash ring.

### Passive Failed-Node Scale Down

1. ETCD lease expiration or fake local keepalive failure produces a cluster-table DELETE event.
2. `HandleNodeRemoveEvent` marks the node `TIMEOUT` and emits `NodeTimeoutEvent(workerAddr, changePrimary=true, removeMeta=false, isOtherAz=false)`.
3. `DemoteTimedOutNodes` later promotes `TIMEOUT` to `FAILED` after `node_dead_timeout_s - node_timeout_s`.
4. `HandleFailedNode` emits `NodeTimeoutEvent(workerAddr, changePrimary=false, removeMeta=true, isOtherAz=false)` and `StartClearWorkerMeta`.
5. `NotifySlotRecovery` emits failed workers to slot recovery.
6. The node utility thread calls `hashRing_->RemoveWorkers(GetFailedWorkers())`, which starts hash-ring passive scale-down when `auto_del_dead_node` allows it.
7. `SyncNodeTableWithHashRing` and the orphan-node monitor later clean cluster-node/API state when the worker disappears from the ring or ETCD.

Important nuance:

- `clusterNodeTable_`, ETCD cluster table, and `HashRingPb` can intentionally disagree for a period of time during passive scale-down.

### Voluntary Scale Down

1. `WorkerOCServer::PreShutDown` detects scale-in mode and starts client/async-task drain.
2. It updates ETCD node state to `exiting` before waiting for local clients to leave.
3. It calls `EtcdClusterManager::VoluntaryScaleDown`, which marks local `isLeaving_` and delegates to `HashRing::VoluntaryScaleDown`.
4. Hash-ring migration moves metadata/data and eventually erases the worker from the ring.
5. When a DELETE event arrives for a node whose last cluster state was `exiting`, `HandleExitingNodeRemoveEvent` distinguishes crash-during-leaving from successful voluntary exit.
6. Successful voluntary exit clears primary-copy state, dead-worker APIs, worker metadata, and the cluster-node table entry.

Important nuance:

- A voluntary node that crashes while still pre-leaving/leaving is turned back into a passive failed-node flow and can trigger slot recovery.

### Restart And Reconciliation

1. A restarted node writes cluster-table state `restart` or `d_rst`.
2. `HandleNodeAdditionEvent` detects restart/downgrade-restart and, when this process is the current master, calls `IfNeedTriggerReconciliation`.
3. Reconciliation first clears worker metadata unless this is downgrade restart, then emits `NodeRestartEvent`.
4. If a failed/timed-out node returns as `recover` or `d_rst`, `ProcessNetworkRecovery` emits network recovery events and may ask the recovered worker to send metadata.
5. `WorkerOCServiceImpl` sets the health probe only after expected reconciliation completes or the give-up path decides it is safe, and then calls `InformEtcdReconciliationDone` to move ETCD state to `ready`.

Important nuance:

- `CheckWaitNodeTableComplete` has a restart-only no-progress early termination path, so restart wait behavior differs from first startup.

### Missing Node / Fake Event Repair

1. After a full cluster restart, ETCD lease keys may be gone while hash-ring workers still exist.
2. `CompleteNodeTableWithFakeNode` compares valid workers in the ring with nodes in cluster tables.
3. For missing nodes, it enqueues a fake PUT followed by a fake DELETE using value `0;start`.
4. Delete handling turns that fake node into the timeout/failure flow, allowing passive scale-down to remove stale ring workers.
5. `ScheduledCheckCompleteNodeTableWithFakeNode` only runs after worker health is set, distributed hash ring is workable, and centralized mode is not used.

Important nuance:

- Fake removal is guarded so it cannot remove a real node that already has a non-fake event value.

### Other-AZ Tracking And Routing

1. Constructor creates `ReadHashRing` objects for configured other AZ names when distributed master and `other_cluster_names` are enabled.
2. Startup `ClusterInfo` loads other-AZ workers and other-AZ hash rings.
3. ETCD watches are created with `ifWatchOtherAz=true`, so local watcher receives other-AZ ring and cluster events too.
4. Other-AZ cluster events update `otherClusterNodeTable_`.
5. Route helpers query read rings and check other-AZ node connectivity before returning a `MetaAddrInfo` marked as other-AZ.

Important nuance:

- Other-AZ worker liveness is tracked only as cluster-manager table state and read-ring state; the local worker does not write those other-AZ rings.

## Build And Test

- Build commands:
  - CMake: `cluster_manager` and `worker_health_check` static libraries.
  - Bazel: `//src/datasystem/worker/cluster_manager:etcd_cluster_manager`
  - Bazel: `//src/datasystem/worker/cluster_manager:worker_health_check`
- Representative tests:
  - `tests/st/worker/object_cache/etcd_cluster_manager_test.cpp`
  - `tests/st/client/kv_cache/kv_client_scale_test.cpp`
  - `tests/st/client/kv_cache/kv_client_etcd_dfx_test.cpp`
  - `tests/st/client/kv_cache/kv_client_hashring_healing_test.cpp`
- Useful test scenarios:
  - cluster-manager startup and restart table completion;
  - no-progress early termination for restart;
  - ETCD restart/crash during worker restart;
  - passive scale-down during scale-up;
  - voluntary scale-down with client/async-task drain;
  - liveness probe and health probe gating.

## Review And Bugfix Notes

- Common change risks:
  - cluster-table `KeepAliveValue` state strings are lifecycle API, not log-only fields;
  - node-table state and hash-ring state are deliberately separate and must be updated through their own flows;
  - fake events exist in multiple layers and must not be treated as normal lease events without guard checks;
  - cluster events can be cached before hash-ring readiness, so event-order changes can deadlock startup;
  - route lookup depends on replica-manager events, not only hash-ring ownership;
  - local and other-AZ event keys are parsed with prefix/string logic, so prefix changes can break dispatch.
- Important invariants:
  - `node_dead_timeout_s > node_timeout_s`;
  - `node_timeout_s * 1000 > heartbeat_interval_ms`;
  - cluster-table writes must be lease-bound and become `ready` only after reconciliation/health readiness;
  - distributed-master cluster events should wait until hash ring is workable;
  - voluntary `exiting` nodes must not be treated the same as unexpected failures.
- Observability or debugging hooks:
  - `NodesToString` and `OtherAzNodesToString`;
  - priority queue size logs in `EnqueEvent`;
  - inject points beginning with `EtcdClusterManager.*`, `worker.RunKeepAliveTask`, `recover.*`, and `WorkerOCServiceImpl.Reconciliation.*`;
  - worker health file path from `health_check_path`.

## Current Design Weaknesses

- The module owns too many concerns at once: ETCD event dispatch, node state, hash-ring progression, reconciliation, routing, health probe, other-AZ tracking, and cleanup orchestration.
- Runtime state is split across ETCD cluster-table rows, `clusterNodeTable_`, `HashRingPb`, local read-ring maps, RocksDB cluster snapshots, health-probe files, and replica-manager state.
- The event model is hard to reason about because real ETCD events, ETCD watch-compensation fake events, startup fake PUT events, and cluster-manager fake add/delete repair events all reach similar handlers.
- The background utility thread is overloaded: it handles events, reconciliation give-up checks, timeout demotion, hash-ring remove/add inspection, node-table/ring sync, and fake-node scheduling.
- Passive failure handling is multi-phase and partially duplicated: DELETE marks `TIMEOUT`, later demotion marks `FAILED`, hash-ring deletion happens separately, and orphan cleanup later erases APIs/table entries.
- Scale-up and scale-down still depend on the single hot hash-ring CAS key while cluster-manager adds extra watches, scans, fake events, and per-node checks around that key.
- Large cluster changes scale poorly: every worker has watch/keepalive/background loops, `GetFailedWorkers` iterates node tables, orphan cleanup can query ETCD per orphan, and many workers can call hash-ring inspection periodically.
- Module boundaries are porous. Cluster-manager directly depends on hash-ring internals, ETCD store internals, object/stream metadata events, replica-manager events, slot recovery, worker remote APIs, fast transport cleanup, and health probe state.
- String-based key/AZ parsing and lifecycle tags (`start`, `restart`, `recover`, `ready`, `exiting`, `d_rst`) make compatibility fragile.

## Open Questions

- Whether cluster-manager should be split into membership watch, lifecycle state machine, route lookup, and repair policy components.
- Whether node membership should become a single typed state machine shared by ETCD and in-memory state instead of two loosely synchronized copies.
- Whether large-scale add/remove should avoid per-worker periodic hash-ring CAS attempts and per-orphan ETCD queries.
- Whether fake-event repair should be moved into an explicit reconciliation subsystem with provenance and idempotency markers.
