# Worker Runtime

## Scope

- Paths:
  - `src/datasystem/worker`
  - closely related neighbors: `src/datasystem/master`, `src/datasystem/server`, `src/datasystem/protos`
- Why this module exists:
  - host the datasystem worker process and its in-process services;
  - expose common worker services, object-cache services, stream-cache services, and cluster participation behavior;
  - manage client registration, shared-memory/RPC data exchange, health, and lifecycle.
- Primary source files to verify against:
  - `src/datasystem/worker/CMakeLists.txt`
  - `src/datasystem/worker/worker_main.cpp`
  - `src/datasystem/worker/data_worker.cpp`
  - `src/datasystem/worker/worker_service_impl.cpp`
  - `src/datasystem/worker/worker_oc_server.cpp`
  - `src/datasystem/worker/worker_cli.cpp`
  - `src/datasystem/cluster/runtime/topology_engine.cpp`
  - `docs/source_zh_cn/design_document/cluster_management.md`

## Responsibilities

- Verified:
  - `datasystem_worker_bin` builds the `datasystem_worker` executable from `worker_main.cpp`.
  - worker runtime also builds shared and static worker libraries used by tests and embedded flows.
  - worker module owns several subareas:
    - `client_manager`
    - `object_cache`
    - `stream_cache`
    - integration with `src/datasystem/cluster`
    - `perf_service` when enabled
  - `worker_main.cpp` initializes the singleton worker, runs a signal-driven loop, performs periodic perf ticking and config monitoring, then runs `PreShutDown` and `ShutDown`.
  - `data_worker.cpp` owns top-level startup/shutdown orchestration, log initialization, THP handling, RocksDB pre-init, and embedded-worker entrypoints exported through C symbols.
  - Parameterized `DataWorker::InitAndRun` requires a non-null `ICoordinatorDiscovery`; that overload always selects the Coordinator backend and passes the injected Discovery to `WorkerOCServer`, while paired `onStart`/`onStop` callbacks remain optional. Command-line and embedded static startup instead use `coordinator_address` to select Coordinator versus ETCD/metastore mode and construct a static Discovery only for Coordinator mode. Coordinator proxy initialization calls its provider once, validates and caches only the first returned address, and keeps all later RPCs fixed to it. Provider updates require rebuilding the runtime object or restarting the Worker.
  - `worker_service_impl.cpp` implements common worker service behavior such as client registration, client disconnect, shared-memory FD transfer, version checks, and auth-related request handling.
  - object-cache Create, MultiCreate, Publish, and MultiPublish handlers select authentication by request origin:
    routed requests with `is_routed=true` call `worker::AuthenticateRequest` using the signed request tenant, while
    legacy gateway requests retain the registered-client authentication path. Deploy workers with this routed-request
    branch before enabling routed Set or MSet in newer clients during rolling upgrades.
  - global-reference decrease treats a missing worker-to-master API after a successful owner lookup as
    `K_RPC_UNAVAILABLE`; affected ids enter the existing RPC-failure path instead of being silently skipped during a
    connection rebuild window.
  - `worker_oc_server.cpp` assembles object-cache and stream-cache related worker-side services and declares many worker runtime flags.
  - worker CLI helpers can export/import the v3 topology ring state through ETCD or Metastore-backed metadata.
  - topology member ids remain binary, topology-internal identities. Worker business UUIDs and public object-location
    ids remain printable and must not expose the topology id bytes.
  - the ETCD-phase Controller role preserves each distinct `RESTARTING` membership generation before generic watch
    doorbell coalescing and forwards it to the Worker host on the Controller state thread. Surviving Workers then restore
    locally held metadata to the restarted current owner. Fast-restart recovery is required even when the legacy
    failure-time `enable_metadata_recovery` switch is disabled, and owner selection must use the business metadata-route
    helper so centralized and distributed metadata modes stay aligned. The restarted Worker independently schedules
    bounded reconciliation requests to every committed metadata owner, so readiness stays closed until the existing
    completion path succeeds or reaches its bounded give-up policy.
  - if Failure final already removed the old member, the later process is a fresh `INITIAL` admission and must not
    restore the removed identity's remote references as if it were the same restart generation.
- Pending verification:
  - precise split between `ds_master`, `ds_server`, and worker-owned service composition at runtime;
  - complete startup sequence inside `Worker::InitWorker()` after the early initialization path already inspected;
  - all interactions between `WorkerOCServer` and replica/data-migration components.

## Build Artifacts

- Verified from `src/datasystem/worker/CMakeLists.txt`:
  - `datasystem_worker_static`
  - `datasystem_worker_shared`
  - `datasystem_worker_bin` with output name `datasystem_worker`
- Important linked neighbors:
  - `ds_master`
  - `ds_server`
  - `worker_object_cache`
  - `worker_stream_cache`
  - `worker_client_manager`
  - `topology`

## Runtime Entry Points

- Process entry:
  - `src/datasystem/worker/worker_main.cpp`
- Singleton orchestrator:
  - `DataWorker::GetInstance()`
- Embedded worker hooks exported from `data_worker.cpp`:
  - `CreateWorker`
  - `WorkerDestroy`
  - `InitEmbeddedWorker`
- Common service implementation:
  - `WorkerServiceImpl`
- Object/stream service assembly:
  - `WorkerOCServer`

## Key Runtime Behaviors

- Startup:
  - parse and validate worker/master/bind addresses
  - initialize logs and worker flags
  - pre-initialize RocksDB storage
  - set up runtime services and signal handling
  - `DataWorker` selects the coordination backend before constructing `WorkerOCServer`: parameterized startup always passes its required injected Discovery and therefore selects Coordinator mode, while command-line and embedded static startup wrap a non-empty `coordinator_address` in internal `StaticCoordinatorDiscovery` or pass null for ETCD/metastore.
  - `WorkerOCServer::Init()` constructs and explicitly initializes a discovery-backed Coordinator proxy from the injected provider, or selects ETCD/metastore, then configures `TopologyEngine::Builder`. Coordinator proxy `Init` requires a non-empty provider result, caches only `front()`, and ignores the remaining candidates. All subsequent RPCs use that cached address once. Changing the provider output or selected endpoint requires rebuilding the runtime object or restarting the Worker, while multi-node Coordinator availability remains the responsibility of the Coordinator Raft layer. The Engine creates and owns both role backends, the hash algorithm, Worker runtime, Controller runtime, Janitor, and optional recovery reporter. Worker code does not assemble or retain those concrete components. Callback targets are initialized before
    `TopologyEngine::Start()`, so callbacks cannot run against partially constructed services. A missing initial topology
    keeps Engine `NOT_READY` while the co-located Controller establishes authority. The Worker publishes READY only after
    the first membership lease succeeds, and writes the ready-check file only after committed membership, a placement
    probe, and Worker RPC health all succeed. No Worker-local topology authority is persisted.
  - when `enable_urma=true`, URMA connection warmup runs after object-cache startup/restart handling and before
    `ReadinessProbe()`: it synchronously prepares the local warmup object, then starts best-effort async peer warmup
    without delaying readiness
  - URMA warmup treats `K_WORKER_PULL_OBJECT_NOT_FOUND` for internal `_urma_` URMA warmup requests as a silent
    best-effort miss because ready-state discovery can race peer warmup-object creation; ordinary RemoteGet failures
    still use normal error logs
  - object-cache worker-to-master RPC warmup also starts before `ReadinessProbe()`: a best-effort asynchronous startup
    task reads immutable topology snapshots until the ready member set is stable or the startup warmup window expires;
    later Snapshot publication callbacks enqueue bounded warmup for newly ready members
- Steady state:
  - accept/register clients
  - manage shared-memory or socket-based FD passing for client-worker IPC
  - serve object-cache and stream-cache requests
  - tick perf manager, drive lightweight metrics summary emission, and monitor config changes
  - memory-rebalance scheduling cross-checks ResourceManager candidates against one current immutable topology
    Snapshot and assigns only `ACTIVE` sources and targets. Before the first Snapshot is available, it preserves the
    legacy resource-readiness fallback instead of blocking scheduling.
  - NodeSelector passes the exact master address that returned each rebalance task to RebalanceExecutor. Before every
    bounded migration batch, the executor expires the task when that assigned master is `FAILED`, locally unreachable,
    or absent from the current topology; a successor master reconstructs scheduling from later resource reports rather
    than accepting completion for the predecessor's in-memory task.
- Shutdown:
  - Parameterized lifecycle callbacks run outside `initMutex_`; once `onStart` is attempted, cleanup invokes `onStop` exactly once. The first lifecycle error is returned while later cleanup errors are logged, and internal shutdown always continues.
  - `PreShutDown` then `ShutDown`
  - `DataWorker` relinquishes its `WorkerOCServer` owner before returning from `ShutDown`, including error paths. The
    server and topology component destructors provide the final safe-stop/join fallback, so runtime-owned objects are
    destroyed before later-created function-local singleton dependencies begin static teardown.
  - voluntary ScaleIn first closes local client admission while the membership lease remains READY, drains existing
    clients and asynchronous tasks, then publishes EXITING. The Worker keeps its Engine, callback executor, and lease
    alive until a current immutable snapshot no longer contains the local member; only then may process shutdown begin.
    This preserves the source for the whole ScaleIn task barrier. The external process manager remains the bounded final
    termination authority when the control plane cannot complete the transition.
  - ordinary metadata mutations remain rejected after the local ScaleIn admission gate closes, but the fenced callback
    propagates its non-empty `businessOperationId` as `RemoveMetaReqPb.topology_operation_id`. Metadata owners use that
    marker only to allow the callback's own idempotent remove/give-up-primary effects. The data phase, final source
    cleanup, and redirect retries must all preserve the same marker; ordinary requests leave the field unset.
  - when `enable_leaving_intercept` is enabled, the object-cache `Create`, `Publish`, `MultiCreate`, and `MultiPublish`
    RPC entrypoints read the same local ScaleIn drain gate and return `K_SCALE_DOWN` before entering their write
    processors. This gate marks topology scale-in draining, not process-level exit. Read-only RPCs keep their existing
    behavior, and disabling the flag preserves the legacy write path.
  - metadata ownership task ranges do not describe where object data is physically resident. ScaleIn therefore drains
    the leaving Worker's complete local object table once per source/batch before task-scoped metadata migration. The
    Worker callback adapter coalesces concurrent disjoint tasks behind a deadline-aware process-local gate; metadata
    migration and prepared cleanup remain constrained by each task's `IKeyFilter`.
  - topology-task ScaleIn data migration accepts only `ACTIVE` destinations. Standby selection excludes every member
    leaving in the same batch. Before its local drain starts, a target closes socket/direct migration admission and
    waits up to the 10-second topology stop budget for every already-admitted migration to finish; subsequent requests
    are rejected. Timeout keeps the gate closed and returns an explicit error so the external lifecycle manager can
    enforce final termination. Concurrent leavers cannot exchange objects after either member takes its drain snapshot.
  - tear down runtime services and service threads
  - `WorkerOCServer` first drains business RPC ingress and then calls only `TopologyEngine::Shutdown(deadline)`. In ETCD
    mode Engine closes the Worker-owned Store's unified watch and keepalive once, drains Worker execution, and stops the
    externally-fed Controller/Janitor before fully shutting down that Store once. Coordinator mode closes role-specific
    event sources through its existing ingress/backend ownership. Any timeout preserves the full dependency chain for a
    later retry; borrowed Store/Proxy
    and business callback owners outlive the Engine. The abnormal destructor path first stops Rebalance, NodeSelector,
    and Worker background threads. If bounded shutdown did not converge, it performs a final safe Engine join while
    metadata/service callback targets remain alive; it then shuts down the metadata/service borrowers, resets their
    endpoint and route adapters, destroys ResourceManager and its rebalance scheduler before their borrowed membership
    view, and finally destroys Engine before Store/Proxy/callback owners.
  - embedded mode uses exported destroy helpers

## Cluster And Metadata Notes

- Verified:
  - current docs and code support ETCD, Coordinator transport, and Metastore-based metadata paths.
  - `worker_oc_server.cpp` enforces that at least one of `etcd_address` or `metastore_address` is set.
  - `WorkerOCServer` uses the constructor-selected Discovery pointer for proxy creation, Controller Store construction, watch-service construction, and `TopologyEngine` backend selection; these branches must not independently re-read `coordinator_address`.
  - `WorkerOCServer` constructs `TopologyEngine` only through its nested Builder. ETCD supplies existing role Store
    resources and Coordinator supplies a Proxy plus bind/drain ingress; Engine internally creates the role backends,
    algorithm and Controller Runtime. Engine also registers the ETCD topology keyspace on both role Stores.
    `WorkerTopologyReferences` no longer exists.
  - topology Snapshot publication only coalesces the newest master-RPC warmup request on the Engine callback thread.
    One Worker background task scans that Snapshot, warms only new or changed member generations, and never queues one
    task or repeats one RPC for every unchanged member on every topology version.
  - ordinary Object/Stream/Master paths retain only the narrow capability they use: prebound
    `MetadataRouteResolver`, `PlacementFacade`, `MembershipEndpointView`, immutable Snapshot, or an Object-specific
    endpoint policy. `WorkerOCServiceImpl` is the sole business lifecycle owner allowed to retain a non-owning Engine
    pointer for semantic lifecycle and cold Host queries.
- Review caution:
  - topology behavior is spread across flags, `WorkerOCServer`, and `src/datasystem/cluster`, so config-only changes
    may still impact worker request routing and recovery behavior.

## Review And Bugfix Notes

- Common change risks:
  - edits in `worker_service_impl.cpp` can break both normal client registration and embedded/shared-memory flows;
  - changes in `worker_oc_server.cpp` can affect many runtime flags and service combinations at once;
  - worker startup/shutdown ordering is sensitive because the singleton exposes both process and embedded modes.
- Important invariants:
  - worker runtime expects valid worker address configuration before serving traffic;
  - `PreShutDown` is a meaningful phase distinct from `ShutDown`;
  - metadata backend must be ETCD or Metastore, not neither.
  - changes that must finish before Kubernetes readiness should run before `ReadinessProbe()` writes the ready-check
    file; background work that only optimizes later traffic should start after core services are registered and avoid
    delaying `Worker::InitWorker()` completion.
  - URMA connection warmup must not add client/KVClient dependencies; worker-side discovery uses existing `EtcdStore`
    state and worker-side remote-get helpers.
  - URMA warmup object races should stay out of normal warning/error logs so operators do not confuse internal best-effort
    warmup misses with user request failures.
  - Worker-master RPC warmup is one-way per initiating worker. Startup Snapshot reads warm each Worker's outbound paths,
    and Snapshot publication callbacks cover old-node to new-node paths during scale-out without a Rocks membership
    mirror.
  - topology routing distinguishes a real redirect from a ScaleOut transfer barrier. Structured callers receive
    `moving=true`; legacy boolean callers receive `true` with an empty target address so they defer the operation instead
    of redirecting back to the committed source or mutating metadata while migration is in flight.
- Useful files during debugging:
  - `src/datasystem/worker/worker_main.cpp`
  - `src/datasystem/worker/data_worker.cpp`
  - `src/datasystem/worker/worker_service_impl.cpp`
  - `src/datasystem/worker/worker_oc_server.cpp`
  - `src/datasystem/cluster/*`

## Worker Isolation Recovery Plan

- Scope:
  - keep the worker isolation/recovery orchestration cohesive behind a runtime-facing facade rather than scattering direct
    state checks through object-cache, KV, stream, and topology call sites;
  - route cluster information access through `ICoordinationBackend` or a runtime facade backed by it. Other modules must
    not call ETCD or Coordinator backend internals directly;
  - keep object-cache metadata recovery injected through callbacks or narrow hooks. The event producer should report
    isolation/recovery evidence, while object-cache owns the actual metadata/data operations.
- Intended dependency shape:
  - `EtcdStore` / Coordinator backend -> `ICoordinationBackend` -> topology runtime/controller -> worker runtime facade;
  - worker services -> worker runtime facade admission APIs;
  - worker runtime facade -> injected metadata-recovery/object-cache hooks for recovery evidence;
  - object-cache internals stay behind metadata recovery, clear-data, and primary-copy APIs;
  - object-cache public service headers must not expose runtime tracker/admission/state implementation headers. Recovery
    generation is a recovery contract token, while `WorkerRecoveryEvidenceTracker` stays private behind object-cache
    recovery state helpers;
  - object-cache metadata/resource recovery state is owned by `ObjectCacheRecoveryState`, not by worker runtime. The
    helper stores metadata recovery evidence, resource recovery requirements, and generation-aware evidence tracking;
    `WorkerOCServiceImpl` keeps only service-specific readiness checks such as slot recovery and eviction/resource
    probes, then exposes the existing service methods.
- Runtime facade responsibilities:
  - expose service-mode transitions (`RUNNING`, `DRAINING`, `LOCAL_ISOLATED`, `RECOVERING`, `STOPPING`);
  - expose admission checks for read, write, migration/rebalance target, and recovery/cleanup RPC;
  - aggregate recovery evidence and keep ordinary service closed until membership, ring, metadata, and data evidence pass;
  - hide backend scope classification, retry budget, recovery evidence tracking, and recovery phase internals from
    business services.
- Boundary refactor order:
  1. keep worker control-backend scope classification independent from `TopologyEngine`; worker code should use evidence
     values and runtime/facade callbacks rather than the cluster engine composition root;
  2. move central metadata address resolution out of `WorkerOCServer` direct table `CAS`/`Get` logic and behind a
     coordination/runtime facade;
  3. make slot-recovery coordination depend on a narrow recovery coordination store instead of directly depending on
     `EtcdStore`, so ETCD and Coordinator-backed modes share the same worker recovery contract;
  4. remove object-cache Get-path direct ETCD `RawGet` and physical-key construction by routing metadata reads through
     metadata route or object metadata facades;
  5. defer broader master object metadata store interface extraction to a follow-up because it touches legacy metadata
     storage outside the worker-isolation critical path.
- Completed boundary refactor slices:
  1. `WorkerOCServiceImpl` no longer stores metadata recovery evidence locks, resource recovery flags, or
     `WorkerRecoveryEvidenceTracker` directly; these are grouped in `ObjectCacheRecoveryState`.
  2. object-cache recovery state has focused UT coverage for metadata evidence updates, stale resource generation
     rejection, and recovery evidence generation invalidation.
  3. runtime module boundary tests assert business service/public headers do not include runtime tracker/admission/state
     internals directly.
  4. the object-cache RPC adapter used to probe peer control-backend state is no longer part of the
     `worker_object_cache` aggregate library; `WorkerOCServer` links it explicitly as a small composition dependency,
     while backend-scope classification remains in the runtime module.
  5. `WorkerOCServer` no longer reaches through `workerRuntime_.RuntimeState()` for local-isolation coordinator wiring
     or metrics publication; both operations go through `WorkerRuntimeFacade` semantic methods. Boundary tests assert
     the composition root does not regress to direct state access.
  6. `WorkerOCServiceImpl` no longer accepts, stores, or includes `EtcdStore`; object-cache business service code keeps
     the injected coordination/metadata capabilities and leaves concrete backend ownership in `WorkerOCServer`.
  7. Stream client-worker admission now goes through `WorkerRuntimeFacade` via the existing `ValidateWorkerState()`
     entrypoint; public stream headers forward-declare the facade and boundary tests prevent direct exposure of runtime
     admission/state internals.
- Acceptance coverage status against the worker-isolation story:
  - `EtcdKeepAliveIsolationTest.ConfirmedLocalIsolationPublishesDeleteAndIsolationCallbackOnce`: covered by
    `WorkerPushMetaTest.LEVEL1_TestKeepAliveLocalIsolationKeepsWorkerAliveAndProtectsPeerData`,
    `WorkerIsolationCoordinatorTest.LocalIsolationClosesAdmissionAndKeepsProcessAlive`, and
    `CoordinationBackendContractTest.LocalIsolationSignalsDoNotDeleteMembershipThroughBackend`.
  - `EtcdKeepAliveIsolationTest.GlobalEtcdOutageDoesNotPublishDeleteOrCloseAdmission`: covered by
    `EtcdStoreTest.TestKeepAliveGlobalEtcdFailureDoesNotReportLocalIsolation` and coordination-backend contract tests.
  - `HashRingSelfPassiveScaleDownDoesNotKill`: covered by
    `WorkerTopologyAvailabilityAdmissionTest.HashRingSelfPassiveScaleDownDoesNotKillWorker`, which verifies
    role-isolated topology availability closes admission as `LOCAL_ISOLATED` instead of killing the process and can
    reopen after complete recovery evidence.
  - `VoluntaryScaleDownStillStopsAfterDrain`: covered by
    `WorkerPushMetaTest.LEVEL1_TestVoluntaryScaleDownStillExitsControlled` and existing KV/object scale-down paths.
  - `RecoveredOldPrimaryDoesNotOverrideMasterPrimary`: covered by
    `OCNotifyWorkerManagerTest.RecoveredOldPrimaryDoesNotOverrideMasterPrimary`, which verifies the recovered old
    primary keeps its acknowledged location and fence op but cannot override the master-selected primary.
  - `OrphanLocalDataRequiresRecoveryOrClearDataWithoutMeta`: covered by
    `MetadataRecoveryTest.OrphanLocalDataRequiresRecoveryOrClearDataWithoutMeta` and clear-data UT retry paths.
  - `OtherWorkersRecoverMetadataBeforeClearingDataWithoutMetadata`: covered by
    `MetadataRecoveryTest.OtherWorkersRecoverMetadataBeforeClearingDataWithoutMetadata`.
  - `IsolatedWorkerMetaCleanupAllowsNewOwnerRebuild`: covered by
    `WorkerStalePrimaryTest.LEVEL1_IsolatedWorkerMetaCleanupAllowsNewOwnerRebuild`.
  - `RecoverableLocalDataRebuildsOrUpdatesMetadata`: covered by
    `MetaDataRecoveryManagerTest.RecoverableLocalDataRebuildsOrUpdatesMetadata`, which verifies a recoverable newer
    metadata record updates an older local object-table entry and makes the restarted local copy primary again.
  - `RecoveredCoordinationEntersRecoveringBeforeRunning`: covered by
    `WorkerPushMetaTest.LEVEL1_TestKeepAliveLocalIsolationRecoversThroughEvidenceGate`,
    `WorkerIsolationCoordinatorTest.LocalRecoveryStartsRecoveringBeforeTopologyReconciliation`, and runtime recovery UTs.
  - `WorkerServiceAdmissionRejectsReadWriteDuringIsolation`: covered for the shared service-mode matrix by
    `WorkerServiceAdmissionTest.AppliesServiceModeMatrix`; Object/KV paths have focused admission checks. Stream
    client-facing read/write entrypoints are covered by
    `ClientWorkerSCServiceAdmissionTest.WorkerServiceAdmissionRejectsStreamReadWriteDuringIsolation` and
    `ClientWorkerSCServiceAdmissionTest.WorkerServiceAdmissionRejectsStreamReadWriteDuringRecovering`; full Stream ST
    remains follow-up.
  - `MigrationTargetFiltersIsolatedWorker`: covered by
    `MigrationTargetIsolationTest.LEVEL1_MigrationTargetFiltersIsolatedAndRecoveringWorker` and DRAINING target tests.
  - `RecoveringWorkerFallsBackToLocalIsolatedOnDisconnect`: covered by
    `WorkerPushMetaTest.LEVEL1_TestRecoveringWorkerFallsBackToLocalIsolatedOnDisconnect` and
    `WorkerRecoveryControllerTest.SecondDisconnectDuringRecoveryKeepsAdmissionClosed`.
  - `MetadataRecoveryBestEffortRetryDoesNotBlockAvailability`: covered by
    `MetadataRecoveryTest.MetadataRecoveryBestEffortRetryDoesNotBlockAvailability` and clear-data best-effort UTs.
  - `MetadataRecoveryDoesNotHoldObjectTableLockDuringFullScan`: covered by
    `MetadataRecoverySelectorTest.SelectionReleasesObjectTableLockBeforeMatchAndBatching` and
    `MetadataRecoverySelectorTest.MetadataRecoveryUsesBoundedGenerationSnapshot`, which verify per-object matching and
    concurrent table mutations are not blocked by full-scan selection.
  - `RecoveryMetadataBatchRetriesOnlyFailedIdsAfterMembershipChange`: covered by
    `SlotRecoveryTest.RecoveryMetadataBatchRetriesOnlyFailedIdsAfterMembershipChange`, which verifies a mixed metadata
    recovery batch keeps already-pushed entries out of the deferred retry payload while retrying only failed ids after
    the coordination path becomes available again.
  - `ScaleInSourceStaysLeavingWhenPeerFails`: covered by
    `TopologyPlanBuilderTest.ScaleInSourceStaysLeavingWhenPeerFails`, which verifies a voluntary ScaleIn source remains
    `LEAVING` while a concurrent peer failure is replanned as `FAILED`.
  - Regression suite: partial. Focused topology/metadata/slot/notify-worker UTs and selected Object/KV STs have been
    run during development, but full CI, Bazel, Stream ST, and complete Object/KV ST are not yet green in this session.
  - Follow-up scale/fault cases to add before claiming full story closure:
    1. ScaleOut while one existing worker is `LOCAL_ISOLATED`; new-owner metadata rebuild must not read from the isolated
       worker before evidence passes.
    2. ScaleIn voluntary source plus concurrent peer local-isolation is now covered at topology replan level by
       `ScaleInSourceStaysLeavingWhenPeerFails`; migration-target filtering for the same combined path remains covered
       indirectly by active-target admission and needs a dedicated ST if we want end-to-end evidence.
    3. ScaleIn/ScaleOut task overlap plus transient global backend outage; workers must not self-isolate from global
       outage evidence and topology callbacks must remain idempotent.
    4. Recovery metadata batch with mixed success/failure while membership changes is now covered at UT level for the
       deferred retry payload; broader ST-level membership churn around the same path remains pending.
    5. ST-level KV/Object/Stream ordinary request coverage during `LOCAL_ISOLATED` and `RECOVERING`; unit coverage now
       verifies Stream client-facing admission for both modes through the same facade semantics, while full protocol ST
       remains pending.

## Fast Verification

- Build worker and tests:
  - `bash build.sh -t build`
- Run common topology UT after building tests:
  - `cd build && ./bin/ds_ut --gtest_filter=TopologyRepositoryTest.*:ClusterRegistryTest.*:ClusterMembershipTest.*:WorkerDirectoryTest.*:TopologyChangeHandlerTest.*`
- Run system tests that exercise worker/runtime paths:
  - `bash build.sh -t run_cases -l st`
- Helpful binaries from test build:
  - `datasystem_worker`
  - `ds_st`
  - `ds_st_object_cache`
  - `ds_st_kv_cache`
  - `ds_st_stream_cache`
  - `ds_st_embedded_client`

## Open Questions

- Which worker flags are safe to classify as “hot config” versus startup-only in future docs?
- Should hash-ring CLI operations live in this document permanently, or move to a deployment/ops-focused module later?
