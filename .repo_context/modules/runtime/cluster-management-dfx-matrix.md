# Cluster Management DFX Scenario Matrix

## Scope

This file is the standalone DFX matrix for runtime topology changes and metadata-backend failures.

Covered scenario families:

- scale-up
- scale-down, including passive, voluntary, fake-node repair, and all-node cases
- restart, including rolling UUID restoration and reconciliation
- ETCD crash or recovery, including degraded startup, keepalive failure, watch recovery, and CAS failure

Source of truth:

- `src/datasystem/worker/cluster_manager/etcd_cluster_manager.cpp`
- `src/datasystem/worker/cluster_manager/cluster_node.h`
- `src/datasystem/worker/hash_ring/hash_ring.cpp`
- `src/datasystem/worker/hash_ring/hash_ring_allocator.cpp`
- `src/datasystem/worker/hash_ring/hash_ring_task_executor.cpp`
- `src/datasystem/worker/hash_ring/hash_ring_health_check.cpp`
- `src/datasystem/worker/hash_ring/hash_ring_tools.cpp`
- `src/datasystem/worker/worker_oc_server.cpp`
- `src/datasystem/worker/object_cache/worker_oc_service_impl.cpp`
- `src/datasystem/common/kvstore/etcd/etcd_store.cpp`
- `src/datasystem/common/kvstore/etcd/etcd_watch.cpp`
- `src/datasystem/protos/hash_ring.proto`

Key tests and inject anchors:

- `tests/st/client/kv_cache/kv_client_scale_test.cpp`
- `tests/st/client/kv_cache/kv_client_etcd_dfx_test.cpp`
- `tests/st/client/kv_cache/kv_client_hashring_healing_test.cpp`
- `tests/st/worker/object_cache/etcd_cluster_manager_test.cpp`
- inject points such as `worker.HashRing.TryAdd`, `SubmitScaleDownTask.skip`,
  `HashRing.NotReceiveUpdate`, `EtcdWatch.StoreEvents.IgnoreEvent`,
  `worker.RunKeepAliveTask`, `worker.KeepAlive.send`, and
  `EtcdClusterManager.CheckWaitNodeTableComplete.*`

## Coverage Axes

Use these axes when adding or reviewing cases. A DFX claim is incomplete if it only covers the happy path.

- Event source: real ETCD watch, active watch compensation, passive watch compensation, keepalive fake DELETE,
  cluster-manager fake add/delete, local RocksDB startup snapshot.
- Worker role: local worker, remote worker, selected process worker, joining destination, failed source,
  voluntary leaving worker, standby/recovery worker, other-AZ worker.
- Ring phase: `NO_INIT`, first init, `INITIAL`, `JOINING`, `ACTIVE`, `PRE_LEAVING`, `LEAVING`, `FAIL`.
- Persisted ring records: `workers`, `add_node_info`, `del_node_info`, `key_with_worker_id_meta_map`,
  `update_worker_map`, `cluster_has_init`.
- Cluster-node state: ETCD keepalive value `start`, `restart`, `recover`, `ready`, `exiting`, `d_rst`;
  in-memory `ClusterNode` state `ACTIVE`, `TIMEOUT`, `FAILED`.
- Backend condition: ETCD available, ETCD unavailable at startup, ETCD unavailable after startup, ETCD recovered,
  ETCD watch stream lost, ETCD writable to peers but not to local node, Metastore compatibility mode.
- Deployment mode: distributed master, centralized master, multi-replica enabled, single-replica fallback,
  cross-AZ metadata enabled.

## Scale-Up Matrix

| ID | Scenario / fault point | Primary path | State to inspect | Expected DFX behavior | Tests / hooks |
| --- | --- | --- | --- | --- | --- |
| SU-01 | First cluster initialization | `HashRing::TryFirstInit` after `InitWithEtcd` | `cluster_has_init=false`, sorted candidate workers, `workers[*].hash_tokens` | Only a bounded candidate set writes first tokens through CAS; local ring becomes workable after watch update. | `WaitAllNodesJoinIntoHashRing`, `HashRing.InitWithEtcd.*` |
| SU-02 | Normal new worker joins existing ring | cluster PUT -> `HandleNodeAdditionEvent`; new worker writes `INITIAL`; `InspectAndProcessPeriodically` -> `TryAdd` | `workers[new].state=INITIAL`, no active `add_node_info` or `del_node_info` | `HashRingAllocator::AddNode` splits ranges, writes `add_node_info`, marks worker `JOINING`, then task executor migrates metadata and finalizes to `ACTIVE`. | scale-up flows in `kv_client_scale_test.cpp` |
| SU-03 | Multiple new workers join together | `HashRing::GetAddingWorkers` and `HashRing::AddNode` | multiple `INITIAL` workers, single `/datasystem/ring` CAS key | Workers can be batched into one ring update, but all writers still contend on the same ring CAS. | batch start paths in scale tests |
| SU-04 | Join waits because scale-down is active | `GetAddingWorkers` returns empty when `del_node_info` exists | `del_node_info_size()>0`, new worker still `INITIAL` | Scale-up defers until scale-down task clears `del_node_info`; `needForceJoin_` is the escape hatch when failed nodes cannot be removed without initial workers joining. | `LEVEL1_ScaleDownWhenScaleUpStart`, `LEVEL2_StartWhenScalingDown` |
| SU-05 | Join waits because another scale-up is active | `GetAddingWorkers` returns empty when `add_node_info` exists or any worker is `JOINING` | existing `add_node_info`, `JOINING` worker | Scale-up is serialized; pending worker stays `INITIAL` until previous `add_node_info` clears. | `HashRing.NotReceiveUpdate`, `LEVEL2_DestNodeNetDelay` |
| SU-06 | Destination/joining node is delayed or misses ring update | ring watch update delayed at joining node | `workers[dest].state=JOINING`, `add_node_info[dest]` still present | Other workers continue migration; delayed destination can later receive the ring and become active when all ranges finish. | `LEVEL2_DestNodeNetDelay` |
| SU-07 | Metadata migration fails or redirects during scale-up | `SubmitScaleUpTask`, `NeedRedirect`, failed `BatchMigrateMetadata` | `add_node_info` unfinished, request key hash in migrated ranges | Reads/writes may be redirected to the destination or current master while migration is incomplete; task finalization must not clear unfinished ranges. | `LEVEL1_RedirectWhenMigrationFailed`, `ScaleUpDelayRedirect` |
| SU-08 | Source worker fails while add-node work is pending | `HashRingAllocator::UpdateHashWhenSrcWorkerFailed` during `RemoveNode` | unfinished `add_node_info`, generated `del_node_info[source]` | Pending add ranges are marked finished as needed and recovery work is written to `del_node_info` so data/meta is recovered from surviving/joining workers. | `LEVEL1_AddNodeWhenOldNodeFailed`, `LEVEL2_TestScaleDownDuringScaleUp` |
| SU-09 | Joining destination fails during scale-up | `ProcessAddNodeInfoWhenJoiningNodeRemove` and `faultNodeIsNewNode` branch | `add_node_info[dest]`, `workers[dest].state=JOINING`, generated `del_node_info[dest]` | Destination is removed from add-node work, unfinished ranges are reassigned, and the dead joining worker is erased or passively scaled down. | `LEVEL1_JoiningNodeScaleDown` |
| SU-10 | Scale-up starts while hash ring is not fully initialized | cluster events cached before ring workable | local ring state before `cluster_has_init` | Cluster events are cached or ignored until the ring can process them; a node removed during init should still be passively removed after init. | `TestScaleDownDuringScaleUp` |
| SU-11 | ETCD watch delay in centralized-master scale-up | delayed `HandleNodeAdditionEvent` | cluster table has new worker, local master event delayed | Client operations should retry/redirect until master sees the worker and routing converges. | `RetryWhenEtcdWatchDelay` |
| SU-12 | Scale-up during ETCD crash | worker start with ETCD unavailable | `CheckEtcdHealth` fails, no writable ring CAS | New node cannot safely join because the ring key cannot be mutated; expected result is start failure until ETCD recovers. | `LEVEL1_TestScaleUpWorkerDuringEtcdCrash` |
| SU-13 | Cross-AZ scale-up event | other-AZ cluster-table PUT | `otherClusterNodeTable_`, other-AZ ring maps | Other-AZ state is tracked separately; local ring ownership is not changed by another AZ's add event. | cross-AZ ETCD DFX tests |

## Scale-Down Matrix

### Passive Scale-Down

| ID | Scenario / fault point | Primary path | State to inspect | Expected DFX behavior | Tests / hooks |
| --- | --- | --- | --- | --- | --- |
| SD-P-01 | Normal lease expiration or worker DELETE | ETCD DELETE -> `HandleNodeRemoveEvent` | cluster node `ACTIVE -> TIMEOUT`, keepalive row deleted | Node timeout events fire with primary-copy change enabled; node is later demoted to failed. | `ShutdownWorkerAndDelKeyInEtcdTest`, common scale-down tests |
| SD-P-02 | Timeout node demoted to failed | `DemoteTimedOutNodes` -> `HandleFailedNode` | `ClusterNode::FAILED`, failed worker set | Master notifies `NodeTimeoutEvent`, `StartClearWorkerMeta`, slot recovery, and hash-ring removal loop. | `LEVEL1_DeleteWhenMasterTimeout` |
| SD-P-03 | Normal passive ring removal | `HashRing::RemoveWorkers` -> `RemoveWorker` | `del_node_info[failed]`, `key_with_worker_id_meta_map[uuid]` | Selected process workers write `del_node_info`; task executor recovers metadata/data and CAS-erases finished `del_node_info` and worker. | `LEVEL2_KeyWithL2Cache`, `LEVEL2_KeyWithoutL2Cache`, `ScaleDownQuerySize` |
| SD-P-04 | Multiple nodes fail in same window | `RemoveWorkers(GetFailedWorkers())` with several failed nodes | multiple `del_node_info` records, selected process workers | Failures are processed in bounded batches; recovery can serialize through the shared ring CAS and task thread pool. | `LEVEL1_ScaleDownAtSameTime`, `LEVEL2_ScaleDownInOneBatch` |
| SD-P-05 | Process worker for scale-down task fails | `GetProcessWorkerForRemoveWorker`, `SubmitScaleDownTask` retry/restore | failed process worker, stale `del_node_info` | Another eligible process worker must restore or retry the unfinished scale-down task. | `LEVEL2_RestoreScaleDown`, `TaskRetry` |
| SD-P-06 | Failed node is still `INITIAL` | `RemoveWorker` inserts empty `del_node_info` | failed worker has no tokens and no uuid | Ring can erase a never-active worker without data-range recovery. | `LEVEL1_ScaleDownNewWorker` |
| SD-P-07 | Standby/recovery worker missing or fails | `CheckScaleDownInfoStandbyWorker`, recovery selection | `del_node_info` target worker not in table or unhealthy | Health-check repair should regenerate or wait for valid standby recovery assignment. | `LEVEL1_TestStandbyNodeFailedRestart`, hash-ring healing tests |
| SD-P-08 | Node restarts during active scale-down | new keepalive PUT while `del_node_info` exists | old worker in `del_node_info`, new worker start attempt | Restarted worker should not rejoin as active until scale-down finishes; existing metadata remains recoverable. | `LEVEL1_TestStartWorkerDuringScaleDown1`, `LEVEL2_StartWhenScalingDown` |
| SD-P-09 | Local worker is passively removed | `NeedToTryRemoveWorker(workerAddr_)` | local ring state `FAIL` or worker missing | Worker marks itself unhealthy and exits, using SIGTERM for voluntary/pre-leaving cases and SIGKILL for passive failure. | `LEVEL1_ClearWorkerWhenEtcdWorkerNetCrash`, liveness tests |
| SD-P-10 | auto delete disabled | `HashRing::EnableAutoDelDeadNode` false | failed cluster nodes remain outside ring removal path | Cluster-node failure events still happen, but hash-ring passive removal is skipped. | flag-dependent behavior; verify manually when changing flags |
| SD-P-11 | L2 cache / ETCD metadata recovery | `RecoverMetaAndDataOfFaultWorker`, L2 and ETCD meta paths | `ETCD_META_TABLE_PREFIX`, `ETCD_LOCATION_TABLE_PREFIX`, L2 data | Write-through/L2 data is recovered through metadata recovery; non-L2 data without external meta may be cleared. | `LEVEL2_KeyWithL2Cache`, `GetKeyWithL2CacheDuringScaleDown`, `ClearDataWithUuid` |
| SD-P-12 | Node timeout while scale-down task is already running | master selection or recovery is slow, then more workers timeout | existing `del_node_info`, additional cluster DELETEs | Additional timeout events should not corrupt the running scale-down task; process may intentionally shut down remaining workers if no safe recovery path remains. | `LEVEL1_NodeTimeoutWhenScaleDownTaskRunning` |

### Voluntary Scale-Down

| ID | Scenario / fault point | Primary path | State to inspect | Expected DFX behavior | Tests / hooks |
| --- | --- | --- | --- | --- | --- |
| SD-V-01 | Normal voluntary shutdown starts | worker writes `exiting`; `VoluntaryScaleDown` CAS sets `need_scale_down` | keepalive state `exiting`, ring worker `need_scale_down=true` | Worker drains clients and async tasks before hash-ring voluntary migration proceeds. | `worker.VoluntaryScaleDown`, graceful shutdown paths |
| SD-V-02 | Voluntary metadata migration | `GenerateVoluntaryScaleDownChangingInfo` | worker `ACTIVE -> LEAVING`, `add_node_info` point/range entries | Ring chooses standby worker and writes add-node-style metadata migration for the leaving worker. | voluntary scale-down tests and hash-ring healing tests |
| SD-V-03 | Voluntary data migration gate | `SubmitMigrateDataTaskIfNeed`, `DataMigrationReady`, `BeforeVoluntaryExit` | leaving worker with empty uuid/tokens but still in ring | Data migration starts only after metadata migration has finished; worker exits after `BeforeVoluntaryExit`. | `recover.toexiting.delay`, voluntary task hooks |
| SD-V-04 | Voluntary node crashes before finishing | `HandleExitingNodeRemoveEvent` with `IsPreLeaving` or `IsLeaving` | node state `exiting`, ring still pre-leaving/leaving | Cluster manager treats it as crash during voluntary scale-down, triggers passive timeout and slot recovery. | hash-ring healing voluntary cases |
| SD-V-05 | Voluntary node already finished | `HandleExitingNodeRemoveEvent` when ring no longer needs worker | node `exiting`, no active leaving state | Cluster-manager erases node table entry, clears worker metadata, removes APIs, and changes primary copy. | graceful exit paths |
| SD-V-06 | All workers voluntary scale down | `runningWorkerSize == allScaleDownWorkers.size()` | all workers marked `need_scale_down`, no standby | Migration is skipped or marked finished because no standby exists; `allWorkersVoluntaryScaleDown_` allows shutdown. | no direct enabled voluntary-all-worker test found; compare no-standby pressure with `NoStandbyWorker` |
| SD-V-07 | Voluntary standby worker fails | voluntary `add_node_info` destination becomes failed | `add_node_info` for leaving source plus failed destination | Fault handling converts or regenerates the voluntary migration work and may fall back to passive recovery. | `CheckVoluntaryScaleDownInfo`, healing tests |

### Fake-Repair And Orphan Cleanup

| ID | Scenario / fault point | Primary path | State to inspect | Expected DFX behavior | Tests / hooks |
| --- | --- | --- | --- | --- | --- |
| SD-F-01 | Ring contains worker missing from cluster-node table after full restart | `CompleteNodeTableWithFakeNode` | worker in hash ring, absent from node table and ETCD cluster table | Cluster manager enqueues fake PUT and fake DELETE so the missing worker can be timed out and scaled down. | `DISABLED_LEVEL1_CutNodeWhenRestart`, `ChangeCluster1`, `ChangeCluster2` |
| SD-F-02 | Fake DELETE races with real node | `HandleNodeRemoveEvent` fake guard | fake event value versus real node event value | Fake remove cannot remove a real node that has already rejoined. | `CompleteNodeTableWithFakeNode` guard |
| SD-F-03 | Zombie worker in ring | scheduled fake completion and ring health check | worker in ring with no real process/cluster row | Zombie worker is eventually removed from hash ring. | `ZombieNodeInHashRing` |
| SD-F-04 | Orphan cluster node not found in ring | orphan node monitor | node table has entry, hash ring does not | Orphan cleanup verifies ETCD row/timestamp and erases stale API/meta resources. | `SyncClusterNodes`, orphan cleanup paths |
| SD-F-05 | Other-AZ worker removal | `HandleOtherAzNodeRemoveEvent` | `otherClusterNodeTable_`, other-AZ hash ring | Local cluster table/ring are not directly mutated; cross-AZ metadata events are emitted separately. | cross-AZ DFX tests |

## Restart Matrix

| ID | Scenario / fault point | Primary path | State to inspect | Expected DFX behavior | Tests / hooks |
| --- | --- | --- | --- | --- | --- |
| RS-01 | Normal worker restart with ETCD available | keepalive `restart`; `HandleNodeAdditionEvent` | cluster node active/failed/timeout, event type `restart` | Master triggers reconciliation when current node is master; worker becomes ready after reconciliation or controlled give-up. | `RestartAllClusterManagers1`, `RestartAllClusterManagers2` |
| RS-02 | Restart from failed state | `HandleNodeStateToActive` and `HandleFailedNodeToActive` | found node `FAILED`, event type `restart` or `recover` | Master sends local-failed-node and metadata recovery events; dead worker APIs are removed. | restart flows in `etcd_cluster_manager_test.cpp` |
| RS-03 | Restart from timeout/network-recovery state | `ProcessNetworkRecovery` | found node `TIMEOUT`, event type `recover` or `d_rst` | Network recovery event runs; `hashRing_->RecoverMigrationTask` restores unfinished scale-up work for that node. | `LEVEL1_ClearWorkerWhenEtcdWorkerNetCrash`, keepalive tests |
| RS-04 | Downgrade restart after ETCD unavailable at startup | keepalive state `d_rst` | `InitKeepAlive(..., isEtcdAvailableWhenStart=false)`, event type `d_rst` | Node is treated as downgrade restart; new-node metadata check runs after ETCD recovery. | `LEVEL1_TestRestartWorkerDuringEtcdCrash`, cross-AZ restart during crash |
| RS-05 | Restart while scale-up or scale-down tasks are pending | `RestoreScalingTaskIfNeeded(true)` and task executor restore | non-empty `add_node_info` or `del_node_info` at startup | Worker restores scale-up and scale-down tasks after becoming healthy enough; restart restoration waits for health in task executor. | `LEVEL2_RestoreScaleDown`, `LEVEL2_StartWhenScalingDown` |
| RS-06 | Restart with rolling UUID restoration | `update_worker_map`, `AddUpgradeRange`, point-range migration | worker has empty/current uuid plus `update_worker_map[addr]` | Reused UUID metadata is migrated back through point-range entries; stale update map is later cleared. | rolling/restart metadata paths |
| RS-07 | Rolling update timeout | `ClearWorkerMapOnInterval`, `WorkerUuidRemovable` | `update_worker_map` age > `rolling_update_timeout_s` | Stale UUID restoration record is removed unless the worker is still waiting for restoration. | `HashRingTools.WorkerUuidRemovable` |
| RS-08 | Reconciliation cannot complete | `WorkerOCServiceImpl` reconciliation and health probe | worker not ready, health probe false | Worker readiness is withheld until reconciliation completes or controlled skip/give-up path is used. | `TestSetHealthProbe`, `LEVEL1_TestRestartWorkerDuringEtcdCrash2` |
| RS-09 | Node-table completion has no progress | `CheckWaitNodeTableComplete` | expected cluster-table size, no progress timer | Restart-only no-progress early termination prevents indefinite wait; non-restart path must not use the early break incorrectly. | `NoProgressEarlyTermination`, `NoProgressEarlyTerminationOnlyForRestart` |
| RS-10 | Restart after passive scale-down | restarted old worker no longer in ring | worker absent from ring, old uuid moved/recovered | Old worker joins as a new node and must not recover stale RocksDB data as authoritative. | `LEVEL2_KeyWithL2Cache`, `WorkerRestartRecoverTest` |
| RS-11 | Restart and delete ETCD metadata | restart worker then metadata delete | `ETCD_META_TABLE_PREFIX`, `ETCD_LOCATION_TABLE_PREFIX` | Metadata owned by restarted worker remains addressable and deletable after restart. | `LEVEL1_RestartDelEtcd` |
| RS-12 | Centralized-master restart difference | centralized mode branch | `enable_distributed_master=false`, master address | Worker restart may not trigger the same distributed-master recovery path; centralized master owns reconciliation. | centralized master scale/restart tests |
| RS-13 | Cross-AZ restart during ETCD crash | RocksDB snapshot plus remote peer reconciliation | local and other-AZ rings/workers | Restarted worker can serve cross-AZ metadata when distributed-master degraded startup succeeds. | `KVClientEtcdDfxCrossAzTest.LEVEL1_TestRestartWorkerDuringEtcdCrash` |

## ETCD Crash And Recovery Matrix

| ID | Scenario / fault point | Primary path | State to inspect | Expected DFX behavior | Tests / hooks |
| --- | --- | --- | --- | --- | --- |
| EC-01 | ETCD crashes after cluster is running | keepalive/watch failures after workers are ready | keepalive timeout flag, ring local cache, RocksDB cluster snapshot | Existing workers continue serving operations that do not require fresh ETCD writes; ETCD writes fail or retry depending operation. | `LEVEL1_TestEtcdRestart` |
| EC-02 | ETCD unavailable at worker startup in distributed-master mode | `ConstructClusterInfoDuringEtcdCrash` | `clusterInfo.etcdAvailable=false`, RocksDB hash rings/workers/replica groups | Worker loads local cluster state from RocksDB, queries a bounded set of active peers, reconciles, and starts with keepalive state `d_rst`. | `TestRestartDuringEtcdCrash`, cross-AZ crash restart |
| EC-03 | ETCD unavailable at worker startup in centralized-master mode | `ConstructClusterInfoDuringEtcdCrash` guard | `enable_distributed_master=false` | Startup is rejected because centralized-master degraded restart is not supported. | centralized disabled ETCD DFX restart test |
| EC-04 | Restart worker during ETCD crash, reconciliation impossible | degraded startup cannot reconcile enough state | missing local ring or peer state | Worker start/ready fails rather than serving with unverifiable metadata. | `LEVEL1_TestRestartWorkerDuringEtcdCrash2` |
| EC-05 | Scale-up during ETCD crash | new worker needs ring CAS and keepalive | ETCD health check failed, no cluster PUT/ring CAS | New worker cannot join until ETCD recovers; expected start failure. | `LEVEL1_TestScaleUpWorkerDuringEtcdCrash` |
| EC-06 | Keepalive RPC fails but ETCD is not writable anywhere | `LaunchKeepAliveThreads` retry branch | local keepalive timeout, peer ETCD writable check false | Worker only retries lease recreation; it does not emit fake DELETE or self-scale-down because the backend itself appears unavailable. | `worker.RunKeepAliveTask`, `LEVEL1_TestEtcdKeepAlive` |
| EC-07 | Local worker loses ETCD connectivity while peers can write ETCD | `HandleKeepAliveFailed` fake DELETE path | local keepalive failed, ETCD writable check true | Local fake DELETE is fed into cluster event handler; if recovery does not happen before dead timeout, worker may kill itself. | `LEVEL1_ClearWorkerWhenEtcdWorkerNetCrash` |
| EC-08 | Keepalive recovers after restart | `AutoCreate` state transition `restart/start -> recover -> ready` | cluster table value state and timestamp | New lease is created, cluster row is rewritten, and ready state is set after recovery/reconciliation. | `TestSetHealthProbe`, keepalive DFX tests |
| EC-09 | ETCD watch stream fails | `WatchRun` -> `ReInitWatch` | watch producer closed, active retrieval revision | Watch is actively retrieved before stream recreation so missed events can be compensated. | `DISABLED_TestWatchEventLost`, `EtcdStore.WatchRun.shutdown` |
| EC-10 | Watch event is missed while backend is writable | `EtcdWatch::GenerateFakeEventIfNeeded` | local key-version cache versus prefix scan | Fake PUT and DELETE events are generated from prefix scan; fake PUT may be delayed until version order is safe. | `EtcdWatch.StoreEvents.IgnoreEvent`, `RetrieveEventPassively` |
| EC-11 | DELETE event while backend not writable | watch compensation writable guard | delete event on cluster prefix, backend not writable | Some DELETE cluster events are intentionally ignored during backend unavailability to avoid false failover. | watch compensation path |
| EC-12 | ETCD CAS commit fails or conflicts | `EtcdStore::CAS` retry loop and transaction compare | ring key version, CAS retry count, `K_TRY_AGAIN` | Operation retries with bounded attempts and random sleep; persistent failure surfaces as start/scale failure. | `TestEtcdCommitFailed` |
| EC-13 | ETCD recovers after degraded writes | `UpdateNodeState(ready)`, watch reinit, cluster event replay | keepalive state `recover/ready`, watch revision | Worker transitions to ready after reconciliation; event streams are recreated and local caches converge. | `LEVEL1_TestEtcdRestart`, `TestRestartDuringEtcdCrash` |
| EC-14 | ETCD cluster table empty after full-cluster stop | startup node table empty but ring still has workers | ring workers not in `clusterNodeTable_` | `CompleteNodeTableWithFakeNode` creates fake add/delete events so stale ring workers can be passively removed. | `DISABLED_LEVEL1_CutNodeWhenRestart`, `ChangeCluster1`, `ChangeCluster2` |
| EC-15 | ETCD/Metastore mode mismatch or Metastore limitations | same `EtcdStore`-style APIs over Metastore backend | backend address selection, table/history behavior | Runtime prefers `metastore_address` over `etcd_address`; Metastore compatibility should be verified separately because its operational envelope is not the same as external ETCD. | metastore unit tests plus runtime startup checks |
| EC-16 | CAS pressure during scale storms | all topology mutations on `/datasystem/ring` | high writer fanout, repeated ring serialization | Correctness relies on bounded CAS retry, but this is a known scale/DFX weakness and can fail under high churn. | stress required; no thousand-node coverage found |

## Cross-Scenario Combinations That Must Not Be Dropped

| ID | Combination | Why it is special | Source-backed handling |
| --- | --- | --- | --- |
| X-01 | scale-down during scale-up | Source failure and destination failure use different hash-ring allocator branches. | `UpdateHashWhenSrcWorkerFailed`, `ProcessAddNodeInfoWhenJoiningNodeRemove`; covered by joining/source failure tests. |
| X-02 | scale-up during scale-down | New workers should remain `INITIAL` unless forced join is needed to make removal possible. | `GetAddingWorkers`, `needForceJoin_`; covered by start-while-scaling-down tests. |
| X-03 | restart during scale-down | Restarted node must not reappear as active while its old identity is being removed. | `del_node_info` gate, `RestoreScalingTaskIfNeeded`, start-during-scale-down tests. |
| X-04 | restart during scale-up | Pending add-node work must be restored or recovered if the joining node was in `add_node_info`. | `RestoreScalingTaskIfNeeded(true)`, `RecoverMigrationTask`. |
| X-05 | ETCD crash during restart | Startup path switches from ETCD scan to RocksDB plus peer reconciliation, or fails safely. | `ConstructClusterInfoDuringEtcdCrash`, `d_rst`, ETCD DFX restart tests. |
| X-06 | ETCD crash during scale-up | New ring CAS cannot happen; join must fail or wait until ETCD recovers. | `LEVEL1_TestScaleUpWorkerDuringEtcdCrash`. |
| X-07 | ETCD crash during passive scale-down | Real DELETE/watch may be missing; keepalive/writable checks decide retry versus fake DELETE. | `LaunchKeepAliveThreads`, `HandleKeepAliveFailed`, watch compensation. |
| X-08 | ETCD crash during voluntary scale-down | `exiting` state and `need_scale_down` CAS may split; crash before ring completion must fall to passive handling. | `HandleExitingNodeRemoveEvent`, `IsPreLeaving`, `IsLeaving`. |
| X-09 | full cluster restart with missing nodes | ETCD cluster table can be empty while ring still has workers. | `CompleteNodeTableWithFakeNode` fake add/delete completion. |
| X-10 | cross-AZ restart or ETCD crash | Local and other-AZ ring/node tables must not be conflated. | other-AZ handlers and `ConstructClusterInfoDuringEtcdCrash` loading other-AZ snapshots. |
| X-11 | multi-replica recovery during topology change | Scale-down executor switches recovery strategy and primary DB selection. | `SubmitScaleDownTaskMultiReplica`, replica/primary DB events. |
| X-12 | local worker is both participant and victim | Self-removal must stop service to avoid stale async tasks. | `NeedToTryRemoveWorker(workerAddr_)`, health probe, SIGTERM/SIGKILL paths. |

## Known DFX Weaknesses Exposed By The Matrix

- `/datasystem/ring` is a single hot protobuf CAS key for first init, scale-up, passive scale-down, voluntary
  scale-down, restart UUID restoration, task completion, and health-check repair.
- Scale storms amplify ETCD pressure because many workers watch broad prefixes, parse the full ring, and can race on
  CAS of the same record.
- CAS retry is bounded and generic; conflict-heavy expansion or shrink can surface user-visible failure instead of
  being absorbed by a coordinator or queue.
- The lifecycle state machine is split across ETCD keepalive tags, `ClusterNode`, `HashRingPb`, hash-ring local state,
  task records, readiness, reconciliation, health probes, and RocksDB snapshots.
- There are two state planes: persisted ETCD/Metastore records and in-memory state. Much of the DFX logic is repair code
  for keeping them eventually consistent.
- Fake events have multiple origins: ETCD watch compensation, keepalive self-failure, startup node-table completion,
  and cluster-manager repair. They share handlers with real events, so provenance must be checked manually.
- Module boundaries are blurry: cluster-manager directly coordinates hash-ring, ETCD store, object-cache, stream-cache,
  replica-manager, slot recovery, worker APIs, fast transport, and health probes.
- Centralized-master, distributed-master, multi-replica, single-replica, and cross-AZ paths do not have the same failure
  envelope.
- No source-backed evidence was found that this model can safely handle thousand-node expansion/shrink. The design
  suggests severe ETCD and CAS contention before that scale.

## Review Checklist

Before claiming a DFX scenario is covered, verify:

- Which event source produced the signal and whether it is real or fake.
- Which persisted records changed in ETCD/Metastore.
- Which in-memory states must converge after the event.
- Whether the local worker is the actor, the victim, or only an observer.
- Whether an unfinished `add_node_info`, `del_node_info`, or `update_worker_map` record exists.
- Whether readiness/health is gated by reconciliation.
- Whether the scenario differs in centralized-master, distributed-master, multi-replica, or cross-AZ mode.
- Whether the expected behavior is covered by an enabled test, a disabled test, or only an inject point.
