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
- Runtime facade public-interface constraint:
  - other worker components should use only semantic `WorkerRuntimeFacade` entrypoints such as `MarkLocalIsolated`,
    `MarkRecovering`, `TryCompleteRecovery`, `CheckAdmission`, `AcquireNormalReadGuard`, `GetSnapshot`, and
    `PublishMetrics`;
  - they must not reach into runtime state manager, recovery controller, evidence tracker, topology availability mapper
    internals, ETCD keepalive/store internals, or Coordinator backend internals;
  - cluster information must cross module boundaries through `cluster::ICoordinationBackend` or narrower injected
    capability hooks/facades built on top of it. Concrete backend ownership stays in the composition root.
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
  8. the worker control-backend probe implementation moved from `worker/object_cache` to `worker/runtime`. Object-cache
     still owns the concrete worker-worker RPC API used by the probe, but the control-plane probing adapter is now a
     runtime composition dependency and no longer appears in the object-cache aggregate library or Bazel package.
     Boundary tests assert the file stays out of `worker/object_cache`, while the scope-classification target remains
     independent of object-cache transport details.
  9. topology callback object-cache actions are injected through `IWorkerTopologyObjectCacheActions`. Runtime callback
     code now owns the event sequencing and no longer includes or depends on `WorkerOCServiceImpl`; object-cache owns the
     concrete `WorkerTopologyObjectCacheActions` adapter that drains ScaleIn data, prepares ScaleIn cleanup, and submits
     Failure local cleanup. Boundary tests assert `worker_topology_phase_callbacks` stays free of
     `worker_oc_service_impl` build and include dependencies.
  10. object-cache Get metadata fallback now depends on the narrow `IObjectMetadataReader` hook instead of retaining
      `cluster::ICoordinationBackend` or constructing coordination keys in `WorkerOcServiceGetImpl`. The concrete
      `CoordinationObjectMetadataReader` owns backend availability checks, logical metadata key construction, and
      metadata protobuf parsing. Boundary tests assert the Get service implementation and service Bazel package do not
      regress to direct coordination-backend coupling.
  11. centralized metadata endpoint claim/read policy moved behind `CentralMetadataAddressResolver`. `WorkerOCServer`
      still chooses the concrete ETCD or Coordinator-backed `ICoordinationBackend`, but the metadata table creation,
      CAS claim, Get fallback, and coordination key names are now object-cache metadata policy. Boundary tests assert the
      worker composition root delegates this policy instead of inlining it.
  12. slot recovery keeps the `ICoordinationBackend` dependency in `CoordinationSlotRecoveryStore` implementation only;
      `slot_recovery_store.h` exposes the store contract and a forward declaration, so slot recovery manager users do
      not inherit coordination backend implementation headers.
  13. object-cache metadata coordination helper headers hide coordination-backend implementation details. Both
      `CentralMetadataAddressResolver` and `CoordinationObjectMetadataReader` expose only `ICoordinationBackend`
      forward declarations in their public headers; the concrete coordination backend header is included only by the
      corresponding `.cpp` files.
  14. `worker_oc_service_impl` Bazel dependencies no longer retain the obsolete `etcd_store` edge after the service
      stopped storing `EtcdStore`. The target keeps the narrower `etcd_constants` edge for existing metadata constants.
      `worker_isolation_coordinator` also declares its `worker_runtime_facade` Bazel dependency explicitly, matching the
      public header include used by the runtime coordinator boundary.
  15. `WorkerRuntimeFacade` no longer exposes its internal `WorkerRuntimeStateManager` through `RuntimeState()`. Runtime
      consumers, including object-cache and worker common-service tests, now use semantic facade methods such as
      `MarkRecovering`, `MarkLocalIsolated`, `TryCompleteRecovery`, and `GetSnapshot`. The boundary script asserts the
      facade does not regress to exposing the state manager by reference.
  16. `worker_control_backend_probe` now owns only the runtime peer-probe orchestration contract. The concrete
      Worker-Worker object-cache RPC adapter is injected by `WorkerOCServer` through `IControlBackendPeerProbeClient`,
      so the runtime target no longer links or includes object-cache transport/codec implementation details. Boundary
      tests assert the runtime target/source stays free of `worker_object_cache`, `worker_worker_oc_api`, and
      `worker_worker_peer_state_codec` dependencies.
  17. `CancellationToken` moved out of the broad topology callback contract into
      `cluster/executor/cancellation_token.h`. `data_migrator` now depends only on the narrow cooperative-cancellation
      signal instead of including `topology_phase_callbacks.h`, so object-cache migration code does not inherit the
      topology callback executor contract.
  18. `TopologyPhaseAction` moved out of the full callback executor contract into
      `cluster/executor/topology_phase_action.h`. `WorkerOcServiceClearDataFlow` now depends on only
      action/filter/cancellation inputs for failure cleanup materialization, keeping callback execution interfaces out
      of the clear-data workflow.
  19. `TopologyCleanupEffect` moved out of the full callback executor contract into
      `cluster/executor/topology_cleanup_effect.h`. `WorkerOCServiceImpl` now depends on action/filter/cancellation
      and cleanup-effect contracts directly, without including the `ITopologyPhaseCallbacks` executor interface.
  20. master object/stream metadata recovery managers now include only the topology metadata input contracts they use:
      action, key filter, storage scan plan, and cancellation token. They no longer inherit the full topology callback
      executor interface through `topology_phase_callbacks.h`.
  21. topology callback metadata recovery/cleanup actions are injected through `IWorkerTopologyMetadataActions`. Runtime
      callback code owns task sequencing and best-effort result aggregation, while object-cache owns the concrete
      `WorkerTopologyMetadataActions` adapter that calls OC/SC metadata migration, recovery, cleanup, and device-meta
      cleanup managers. Boundary tests assert `worker_topology_phase_callbacks` stays free of `MetadataManagerHolder`
      and concrete OC/SC metadata manager dependencies.
  22. topology availability admission now exposes only the `WorkerRuntimeFacade` public entrypoints. The older
      `WorkerRuntimeStateManager` / `WorkerRecoveryController` overloads were removed from the public header and
      implementation, so topology-to-runtime admission cannot bypass the facade boundary.
  23. object-cache recovery evidence aggregation now lives behind `ObjectCacheRecoveryState` hooks. `WorkerOCServiceImpl`
      injects only the slot evidence provider and resource-readiness probe, while metadata evidence, resource recovery
      generation, and object-cache recovery report aggregation stay in the object-cache recovery-state helper.
  24. `WorkerRecoveryEvidenceAdapter` no longer includes slot-recovery manager implementation headers. The adapter owns
      only the slot-incident terminal predicate it needs for recovery evidence, avoiding a Bazel dependency cycle between
      object-cache recovery evidence and the slot-recovery manager implementation.
  25. control-backend failure scope classification no longer rejects local-isolation evidence solely because peer
      topology authority stamp is newer than the isolated local worker's stale stamp. Fresh `AVAILABLE` peer evidence
      confirms local isolation; mismatched stamps still prevent proving a global outage when no peer reports available.
  26. voluntary ScaleIn `DRAINING` runtime state now uses `WorkerIsolationReason::NONE`, keeping the fault/isolation
      reason separate from voluntary drain state. Passive topology removal still uses `TOPOLOGY_PASSIVE_SCALE_DOWN`
      through topology availability admission.
  27. peer control-backend probes keep partial successful observations instead of discarding all evidence when one peer
      probe fails. The scope classifier can use a fresh partial `AVAILABLE` peer observation to close local admission,
      while global outage still requires full peer evidence.
  28. object-cache and stream hot-path admission now use `WorkerRuntimeFacade::AcquireAdmissionGuard` and hold the
      runtime read window while the admitted business operation runs. This closes the check/use race between a serving
      snapshot and a concurrent transition to `LOCAL_ISOLATED`, `RECOVERING`, or `DRAINING`; async paths capture a guard
      token until the queued work starts/runs, while public service headers still expose only the facade boundary.
  29. `ICoordinationBackend` keeps Coordinator concrete proxy headers out of the public coordination contract. The
      interface exposes only the abstract backend and a forward-declared proxy factory hook; concrete Coordinator proxy
      details stay in `DsCoordinationBackend`.
  30. controller-owned watch lifecycle is split from backend keepalive lifecycle through
      `ICoordinationBackend::ShutdownWatchEventSources()`. `TopologyController` Start rollback and Stop now close only
      watch event sources, so an ETCD fallback Controller wrapper sharing the Worker-owned Store cannot accidentally
      stop the Worker membership lease, local-isolation handler, or recovery callback path. Full
      `ShutdownEventSources()` remains the owner-level backend shutdown path.
- Recent focused verification:
  - After rebasing to `main/master@11805014d`, `scripts/clion_remote_build.sh tests-index` rebuilt the CLion remote
    UT/ST index with URMA Mock enabled, generated 1157 compile-command entries, reused `/home/ds-thirdparty-cache`
    without rebuilding third-party dependencies, and completed in 139s.
  - Added/updated 1 coordination-backend contract method and 2 topology-controller lifecycle expectations:
    `ICoordinationBackend::ShutdownWatchEventSources()` is required by the contract, and controller Start rollback/Stop
    assert watch-only shutdown rather than full event-source shutdown. Initial RED single-file syntax check failed in
    22.1s because the interface method did not exist; GREEN syntax check passed in 8.6s.
  - `cluster_topology_contract_ut --gtest_filter="TopologyControllerRuntimeTest.*:CoordinationBackendContractTest.*"`:
    19/19 tests passed in 2.07s.
  - `ds_ut --gtest_filter="WorkerAdmissionFacadeTest.*:WorkerIsolationCoordinatorTest.*:WorkerRecoveryControllerTest.*:WorkerRecoveryEvidenceBuilderTest.*:WorkerRecoveryEvidenceTrackerTest.*:WorkerRuntimeFacadeTest.*:WorkerRuntimeStateTest.*:WorkerTopologyAvailabilityAdmissionTest.*"`:
    43/43 tests passed in 0.30s.
  - `ds_ut_object --gtest_filter="ObjectCacheRecoveryStateTest.*:WorkerRecoveryEvidenceAdapterTest.*:WorkerOcServiceImplTest.*Recovery*:WorkerOcServiceImplTest.*OutOfMemory*:WorkerOcServiceImplTest.*ResourceRecovery*:WorkerOcServiceImplTest.*BuildObjectCacheRecoveryEvidence*:WorkerOcServiceImplTest.*ClearMatchedObjectsRecovers*:WorkerOcServiceImplTest.*NotifyRemoteGet*:MigrateDataServiceTest.*Drain*:MigrateDataServiceTest.*MigrateDataDirectResponse*"`:
    43/43 tests passed in 10.49s. During this slice,
    `WorkerOcServiceImplTest.DiskCreateOutOfMemoryRecordsDiskRecoveryRequirement` was narrowed from an over-broad
    mocked Create RPC path that timed out at 60s to a direct `MarkOutOfMemoryIfNeeded(..., DISK)` assertion that passes
    in 0.05s while preserving the disk-resource recovery requirement coverage.
  - Current focused UT total for this slice: 105/105 passed. `git diff --check` is clean, and
    `git clang-format --diff HEAD --` on the touched source/test files reports no changes.
  - Post hot-path guard refactor and rebase to latest `main/master`: `scripts/clion_remote_build.sh tests-index`
    rebuilt the CLion remote UT/ST index with URMA Mock enabled in 601s, generated 1157 compile-command entries, and
    reused the third-party cache in 0s.
  - Added/updated 1 runtime guard UT and boundary assertions for object-cache/stream guard usage:
    `WorkerAdmissionFacadeTest.AdmissionGuardHoldsRuntimeReadWindowForWrites` verifies a held admission guard blocks a
    concurrent local-isolation transition until the operation releases the guard.
  - Post-rebase focused CMake/remote checks passed:
    `WorkerAdmissionFacadeTest.*:WorkerRuntimeFacadeTest.*` ran 5/5 in 20ms,
    `ClientWorkerSCServiceAdmissionTest.*` ran 2/2 in 1ms,
    object-cache/migration guard filters ran 7/7 in 179ms, and
    `tests/scripts/test_worker_runtime_module_boundary.py` ran 27/27 in 1.406s.
  - Post-rebase Bazel 7.4.1 focused validation with `--config=release --config=test --config=urma_mock` passed:
    focused build for runtime/stream/object-cache targets passed in 325.317s; runtime Bazel UTs passed 2/2 in 68.185s
    with each test executing in about 0.1s; filtered object-cache Bazel UT passed 1/1 in 364.036s with test execution
    time 1.4s. The long wall time came from Bazel action-cache invalidation after `--cxxopt` changed, not from CMake
    third-party cache misses.
  - Added 1 boundary-contract test:
    `WorkerRuntimeModuleBoundaryTest.test_coordination_backend_interface_hides_concrete_proxy_headers`. Initial RED:
    the test failed because `coordination_backend.h` included `coordinator_service_proxy.h`; the first implementation
    attempt also failed CMake because the proxy was incorrectly forward-declared inside `datasystem::cluster`. GREEN:
    `ICoordinatorServiceProxy` is now forward-declared in the outer `datasystem` namespace, the public backend contract
    no longer includes the concrete proxy header, `python3 tests/scripts/test_worker_runtime_module_boundary.py` passed
    28/28 in 0.067s, `scripts/clion_remote_build.sh tests-index` passed in 264s with URMA Mock enabled and third-party
    cache hit in 1s, `cluster_topology_contract_ut` focused coordination backend tests passed 20/20 in 2ms, and Bazel
    7.4.1 `//src/datasystem/cluster/coordination_backend:coordination_backend` passed in 230.979s.
  - Post peer-probe partial-evidence fix: CLion remote `scripts/clion_remote_build.sh tests-index` rebuilt with URMA Mock
    enabled in 101s, generated 1157 compile-command entries, and reused the third-party cache in 0s.
  - Added 2 UT cases:
    `WorkerControlBackendProbeTest.KeepsSuccessfulObservationsWhenOnePeerProbeFails` and
    `WorkerControlBackendScopeTest.PartialAvailablePeerEvidenceConfirmsLocalIsolation`.
  - `ds_ut --gtest_filter="WorkerControlBackendProbeTest.*:WorkerControlBackendScopeTest.*"`: 9/9 tests passed in
    0.06s.
  - Remote Bazel 7.4.1 focused build for
    `//tests/ut/worker:worker_control_backend_probe_test //tests/ut/worker:worker_control_backend_scope_test` passed in
    2.59s; identical cached rerun passed in 0.37s.
  - Post review-fix verification for stamp mismatch and draining reason: CLion remote `scripts/clion_remote_build.sh
    tests-index` rebuilt with URMA Mock enabled in 138s, generated 1156 compile-command entries, and reused the
    third-party cache in 0s.
  - Added 2 UT cases/expectations:
    `WorkerControlBackendScopeTest.AvailablePeerWithMismatchedTopologyStampConfirmsLocalIsolation` and the updated
    `WorkerRuntimeStateTest.DrainingIsTerminalForServingTransitions`.
  - `ds_ut --gtest_filter="WorkerControlBackendScopeTest.*:WorkerRuntimeStateTest.*:WorkerRuntimeFacadeTest.*:WorkerServiceAdmissionTest.*:WorkerAdmissionFacadeTest.*"`:
    34/34 tests passed in 0.32s.
  - `python3 -m unittest tests/scripts/test_worker_runtime_module_boundary.py`: 26/26 tests passed in 0.075s; `git
    diff --check` and `git clang-format --diff HEAD --` on the changed runtime/test files were clean.
  - Remote Bazel 7.4.1 focused build with shared `.bazel-cache/distdir`,
    `--config=release --config=test --config=urma_mock`, and cache-owner execution passed for the five runtime/admission
    UT targets in 4.19s. The identical cached rerun passed in 0.45s with one internal action.
  - Post adapter-boundary refactor verification: CLion remote `scripts/clion_remote_build.sh tests-index` rebuilt the
    UT/ST index with URMA Mock enabled in 170s, generated 1156 compile-command entries, and reused the third-party cache
    without rebuilding it.
  - `ds_ut_object --gtest_filter="ObjectCacheRecoveryStateTest.*"`: 4/4 tests passed in 0.05s after the hook injection
    split.
  - `ds_ut_object --gtest_filter="WorkerOcServiceImplTest.*Evidence*:WorkerOcServiceImplTest.*Recovery*:WorkerOcServiceImplTest.*Admission*:WorkerOcServiceImplTest.RestartReconciliationMarksRuntimeRecoveringBeforeFanout:WorkerOcServiceImplTest.MasterWorkerClassifiesCleanupAndRecoveryRpcs:WorkerRecoveryEvidenceAdapterTest.*:ObjectCacheRecoveryStateTest.*:SlotRecoveryTest.*:MetadataRecoveryManagerTest.*:CentralMetadataAddressResolverTest.*:TopologyPhaseCallbacksTest.*"`:
    62/62 tests passed in 0.30s.
  - `python3 -m unittest tests/scripts/test_worker_runtime_module_boundary.py`: 26/26 tests passed in 0.082s; `git
    diff --check` and `git clang-format --diff HEAD -- src/datasystem/worker/object_cache/worker_recovery_evidence_adapter.cpp tests/ut/worker/BUILD.bazel`
    were clean.
  - Remote Bazel 7.4.1 focused build with shared `.bazel-cache/distdir`,
    `--config=release --config=test --config=urma_mock`, and cache-owner execution passed for
    `//tests/ut/worker:object_cache_recovery_state_test //tests/ut/worker:worker_oc_service_impl_test` in 50.72s. The
    identical cached rerun passed in 0.38s with no package reload and one internal action, confirming Bazel cache reuse.
  - After rebasing to `main/master@52433afc2`, `scripts/clion_remote_build.sh tests-index` rebuilt the CLion remote
    UT/ST index with URMA Mock enabled, 1156 compile-command entries, third-party cache hit in 1s, and total script
    time 103s.
  - `ds_ut_object --gtest_filter="ObjectCacheRecoveryStateTest.*"`: 4/4 tests passed in 0.05s after adding the injected
    recovery evidence hook case.
  - `ds_ut_object --gtest_filter="WorkerOcServiceImplTest.*Evidence*:WorkerOcServiceImplTest.*Recovery*:WorkerOcServiceImplTest.*Admission*:WorkerOcServiceImplTest.RestartReconciliationMarksRuntimeRecoveringBeforeFanout:WorkerOcServiceImplTest.MasterWorkerClassifiesCleanupAndRecoveryRpcs:WorkerRecoveryEvidenceAdapterTest.*:ObjectCacheRecoveryStateTest.*:SlotRecoveryTest.*:MetadataRecoveryManagerTest.*:CentralMetadataAddressResolverTest.*:TopologyPhaseCallbacksTest.*"`:
    62/62 tests passed in 0.31s.
  - `python3 -m unittest tests/scripts/test_worker_runtime_module_boundary.py`: 26/26 tests passed in 0.063s after
    hiding `WorkerRuntimeStateReadGuard` from object-cache public headers and keeping object-cache recovery aggregation
    behind hook injection.
  - `git diff --check` clean; `git clang-format --diff HEAD --` on the modified object-cache recovery files reported
    no formatting changes.
  - `scripts/clion_remote_build.sh tests-index` with `BUILD_WITH_URMA_MOCK` path generated 1149 compile-command entries
    before this slice and built UT/ST targets; after the probe move, `scripts/clion_remote_build.sh index` rebuilt source
    in 124s with third-party cache hit in 0s, `worker_control_backend_probe`, `datasystem_worker_static`,
    `datasystem_worker_shared`, and `datasystem_worker_bin` all green.
  - `python3 -m unittest tests/scripts/test_worker_runtime_module_boundary.py`: 11 tests, 0.006s after adding the
    topology-callback/object-cache boundary assertion.
  - `scripts/clion_remote_build.sh index`: source build green in 298s, total 379s, third-party cache hit in 0s,
    `BUILD_WITH_URMA_MOCK` enabled; `worker_topology_phase_callbacks`, `worker_object_cache`,
    `datasystem_worker_static`, `datasystem_worker_shared`, and `datasystem_worker_bin` all green. The script emitted
    known repeated-strip `debuglink section already exists` diagnostics but exited 0.
  - `scripts/clion_remote_build.sh tests-index`: full UT/ST index build green in 506s, third-party cache hit in 1s,
    `BUILD_WITH_URMA_MOCK` enabled, generated 1150 compile-command entries including
    `worker_topology_object_cache_actions.cpp`, `worker_topology_phase_callbacks.cpp`,
    `topology_phase_callbacks_test.cpp`, `worker_runtime_facade.cpp`, `worker_oc_server.cpp`, and
    `metadata_recovery_manager_test.cpp`.
  - `python3 -m unittest tests/scripts/test_worker_runtime_module_boundary.py`: 11 tests, 0.004s after the final
    topology-callback/object-cache boundary split.
  - `ds_ut_object --gtest_filter=TopologyBusinessContractTest.*`: 3 tests, 0.05s; covers opaque topology business
    entrypoints, object-cache failure local actions injected behind the hook, and topology operation identity on
    RemoveMeta.
  - `ds_ut --gtest_filter=WorkerRuntimeFacadeTest.*:WorkerTopologyAvailabilityAdmissionTest.*:WorkerServiceAdmissionTest.*`:
    20 tests, 0.11s.
  - `ds_ut --gtest_filter=WorkerControlBackendScopeTest.*:WorkerRuntimeFacadeTest.*:WorkerTopologyAvailabilityAdmissionTest.*`:
    19 tests, 0.05s.
  - `cluster_topology_contract_ut --gtest_filter=TopologyEngineTest.*:TopologyFailureClassifierTest.*:HashAlgorithmTest.ScaleOutDoesNotUseFailedWorkerAsMigrationSource`:
    27 tests, 1.95s.
  - `python3 -m unittest tests/scripts/test_worker_runtime_module_boundary.py`: 12 tests, 0.014s after adding the Get
    service metadata-reader boundary assertion.
  - `scripts/clion_remote_build.sh tests-index`: full UT/ST index build green in 515s, source build 431s, third-party
    cache hit in 1s, `BUILD_WITH_URMA_MOCK` enabled, generated 1151 compile-command entries including
    `object_metadata_coordination_reader.cpp`, `worker_oc_service_get_impl.cpp`, `migrate_data_service_test.cpp`,
    `worker_topology_object_cache_actions.cpp`, and worker service composition files.
  - incremental `scripts/clion_remote_build.sh tests-index` after the Get metadata unavailable-error behavior fix:
    green in 164s, source build 54s, third-party cache hit in 1s, 1151 compile-command entries.
  - `ds_ut_object --gtest_filter=NotifyRemoteGetMigrationTest.QueryMetadataUsesCoordinationStoreLogicalKey:NotifyRemoteGetMigrationTest.QueryMetadataRejectsUnavailableCoordinationBackend`:
    2 tests, 0.05s; covers coordination logical-key lookup through the injected reader and non-absent propagation when
    the coordination backend is unavailable.
  - `ds_ut_object --gtest_filter=TopologyBusinessContractTest.*`: 3 tests, 0.05s.
  - `ds_ut --gtest_filter=WorkerRuntimeFacadeTest.*:WorkerTopologyAvailabilityAdmissionTest.*:WorkerServiceAdmissionTest.*`:
    20 tests, 0.10s.
  - `python3 -m unittest tests/scripts/test_worker_runtime_module_boundary.py`: 13 tests, 0.016s after adding the
    central metadata resolver composition-boundary assertion.
  - `scripts/clion_remote_build.sh tests-index`: incremental build green in 98s, source build 19s, third-party cache
    hit in 1s, `BUILD_WITH_URMA_MOCK` enabled, generated 1154 compile-command entries including
    `central_metadata_address_resolver.cpp`, `central_metadata_address_resolver_test.cpp`,
    `fake_coordination_backend.cpp`, and `worker_oc_server.cpp`.
  - `ds_ut_object --gtest_filter=CentralMetadataAddressResolverTest.*`: 2 tests, 0.05s; covers first-worker CAS claim
    and existing-address Get fallback through the resolver.
  - `ds_ut_object --gtest_filter=NotifyRemoteGetMigrationTest.QueryMetadataUsesCoordinationStoreLogicalKey:NotifyRemoteGetMigrationTest.QueryMetadataRejectsUnavailableCoordinationBackend:TopologyBusinessContractTest.*`:
    5 tests, 0.05s.
  - `ds_ut --gtest_filter=WorkerRuntimeFacadeTest.*:WorkerTopologyAvailabilityAdmissionTest.*:WorkerServiceAdmissionTest.*`:
    20 tests, 0.11s.
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
    `WorkerServiceAdmissionTest.AppliesServiceModeMatrix`; Object protocol read/write rejection during
    `LOCAL_ISOLATED` and `RECOVERING` is covered by
    `MigrationTargetIsolationTest.LEVEL1_ObjectClientRejectsReadWriteDuringIsolationAndRecovering`; KV protocol
    `Get/Set` rejection during `LOCAL_ISOLATED` and `RECOVERING` is covered by
    `KVClientEtcdDfxTest.LEVEL1_KVClientRejectsReadWriteDuringIsolationAndRecovering`. Stream protocol
    create/subscribe/receive rejection during `LOCAL_ISOLATED` and `RECOVERING` is covered by
    `StreamClientAdmissionTest.LEVEL1_StreamClientRejectsReadWriteDuringIsolationAndRecovering`, and the recovered
    path verifies data sent before local isolation remains readable after recovery before validating new
    create/subscribe/send/receive operations. The Stream ST observes the public health-gated
    `K_NOT_READY` status, while mode/reason details remain covered by
    `ClientWorkerSCServiceAdmissionTest.WorkerServiceAdmissionRejectsStreamReadWriteDuringIsolation` and
    `ClientWorkerSCServiceAdmissionTest.WorkerServiceAdmissionRejectsStreamReadWriteDuringRecovering`.
  - `MigrationTargetFiltersIsolatedWorker`: covered by
    `MigrationTargetIsolationTest.LEVEL1_MigrationTargetFiltersIsolatedAndRecoveringWorker`,
    `MigrationTargetOomTest.LEVEL1_MigrationTargetFiltersOutOfMemoryWorker`, and
    `MigrationTargetDrainingTest.LEVEL1_MigrationTargetFiltersDrainingWorker`. These ST probes now use the production
    `WorkerRemoteWorkerOCApi::MigrateDataProbe` path, so brpc and ZMQ modes follow the same stub selection as real
    worker-to-worker migration.
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
  - `ScaleOutMembersSurviveGlobalBackendOutagePause`: covered by
    `TopologyFailureClassifierTest.ScaleOutMembersSurviveGlobalBackendOutagePause`, which verifies a transient global
    membership-read outage during ScaleOut does not accumulate missing time or remove/fail present ACTIVE/JOINING
    members after the backend becomes readable again.
  - `ScaleOutProgressPostCommitFailureDoesNotDuplicateCallback`: covered by
    `TopologyTaskExecutorTest.ScaleOutProgressPostCommitFailureDoesNotDuplicateCallback`, which verifies a ScaleOut task
    progress CAS that commits before returning `K_RPC_UNAVAILABLE` is treated as already finished, and duplicate notify
    plus retry tick does not rerun the migration callback.
  - `ScaleInMetadataPostCommitFailureDoesNotDuplicateCallbackBeforeGateOpens`: covered by
    `TopologyTaskExecutorTest.ScaleInMetadataPostCommitFailureDoesNotDuplicateCallbackBeforeGateOpens`, which verifies
    a ScaleIn metadata marker CAS that commits before returning `K_RPC_UNAVAILABLE` is resolved through exact-write
    validation, does not open the source gate before the sibling admitted task is marked done, and does not duplicate
    metadata callbacks on the retry tick.
  - `ScaleOutDoesNotUseFailedWorkerAsMigrationSource`: covered by
    `HashAlgorithmTest.ScaleOutDoesNotUseFailedWorkerAsMigrationSource`, which verifies ScaleOut owner-change
    materialization does not use a failed member as the migration/rebuild source for the joining target.
  - `ScaleOutReplanDropsFailedJoiningAndKeepsRemainingJoiningBatch`: covered by
    `TopologyPlanBuilderTest.ScaleOutReplanDropsFailedJoiningAndKeepsRemainingJoiningBatch`, which verifies a failed
    `JOINING` member is removed from an active ScaleOut batch while the remaining `JOINING` member stays in the
    ScaleOut batch and receives owner-change work from the healthy committed source.
  - `FailureFinalResumesScaleOutBatchWhenJoiningMemberSurvives`: covered by
    `TopologyPlanBuilderTest.FailureFinalResumesScaleOutBatchWhenJoiningMemberSurvives`, which verifies a Failure batch
    that interrupts ScaleOut removes the failed committed member and resumes the original ScaleOut batch when at least
    one `JOINING` member survives.
  - `FailureFinalResumesScaleInBatchWhenLeavingMemberSurvives`: covered by
    `TopologyPlanBuilderTest.FailureFinalResumesScaleInBatchWhenLeavingMemberSurvives`, which verifies a Failure batch
    that interrupts ScaleIn removes the failed peer and resumes the original ScaleIn batch when a voluntary `LEAVING`
    member survives.
  - `ScaleInAndIsolationRejectMigrationTargetDuringCombinedFaultWindow`: covered by
    `WorkerServiceAdmissionTest.ScaleInAndIsolationRejectMigrationTargetDuringCombinedFaultWindow`, which locks the
    shared admission contract for a voluntary ScaleIn source (`DRAINING`) and a faulted peer
    (`LOCAL_ISOLATED`/`RECOVERING`): both reject `MIGRATION_TARGET`, while the conservative read/recovery/cleanup paths
    allowed by the runtime matrix remain open.
  - `ScaleInAndIsolationRejectMigrationTargetDuringCombinedFaultWindowSt`: covered by
    `MigrationTargetCombinedFaultTest.LEVEL1_MigrationTargetFiltersScaleInSourceAndIsolatedPeerTogether`, which starts a
    three-worker cluster, pauses one worker in voluntary ScaleIn `DRAINING`, drives another worker into
    `LOCAL_ISOLATED`, and verifies a healthy source worker's real worker-to-worker migration probe is rejected by both
    targets before the migration allocation service is reached.
  - `ScaleOutPreservesDataWhenWorkerIsLocallyIsolated`: covered by
    `WorkerPushMetaScaleOutFaultTest.LEVEL1_ScaleOutPreservesDataWhenWorkerIsLocallyIsolated`, which starts a
    two-worker object-cache cluster, drives worker 0 into actual keepalive-confirmed `LOCAL_ISOLATED`, then starts worker
    2 as a ScaleOut member. The ST verifies worker 0 stays alive, peer data remains readable during ScaleOut, and worker
    0 local data remains readable after local recovery.
  - `ScaleOutSurvivesTransientGlobalBackendOutage`: covered by
    `WorkerPushMetaScaleOutFaultTest.LEVEL1_ScaleOutSurvivesTransientGlobalBackendOutage`, which starts a two-worker
    object-cache cluster, stores data on both existing workers, completes ScaleOut to worker 2, then injects a transient
    global ETCD outage. The ST verifies all three workers classify the outage as global and stay alive, topology returns
    to three ACTIVE members after backend recovery, and pre-outage data remains readable through the ScaleOut member.
  - Regression suite: focused local/remote validation is green after rebasing to `main/master@0180554bf`; full PR CI and
    codecheck still need to be triggered and inspected before claiming merge readiness. Fresh focused evidence:
    - CMake build targets `ds_ut_object`, `ds_ut`, `ds_st_object_cache`, `ds_st_kv_cache`, `ds_st_stream_cache`, and
      `datasystem_worker_bin` built in the CLion remote tree with `BUILD_WITH_URMA_MOCK` enabled.
    - Object-cache focused UT: 42 tests passed in 0.25s.
    - Runtime/admission focused UT: 41 tests passed in 0.23s.
    - Story ST main path: 9 tests passed in 1:38.54.
    - KV admission ST: 2 tests passed in 21.73s.
    - migration/scale/fault ST: 5 tests passed in 50.44s.
    - Stream admission/data-retention ST: 1 test passed in 11.06s.
    - Metadata recovery ST group: 11 tests ran, 9 passed and 2 skipped for the known brpc migration gap in 2:15.28.
    - Runtime module boundary script: 26 tests passed in 0.106s.
    - `git diff --check` and `git clang-format --diff main/master -- <changed-cpp-files>` are clean.
    - Focused clang-tidy runs on the touched worker/object-cache files returned rc 0; output still contains existing
      legacy style warnings, so this is not a repository-wide zero-warning claim.
    - Remote Bazel 7.4.1 focused build with `--config=release --config=test --config=urma_mock` passed once in 6:15.93
      using the branch `.bazel-cache/distdir`, and the identical cached rerun passed in 1.17s.
    - Added ScaleOut/local-isolation ST:
      `WorkerPushMetaScaleOutFaultTest.LEVEL1_ScaleOutPreservesDataWhenWorkerIsLocallyIsolated` passed 1/1 in 12.27s.
    - Scale/fault ST pair regression:
      `WorkerPushMetaScaleOutFaultTest.LEVEL1_ScaleOutPreservesDataWhenWorkerIsLocallyIsolated` plus
      `MigrationTargetCombinedFaultTest.LEVEL1_MigrationTargetFiltersScaleInSourceAndIsolatedPeerTogether` passed 2/2
      in 27.33s.
    - Remote Bazel 7.4.1 focused build for the changed ST component
      `//tests/st/client/object_cache:client_dfx_test` passed in 25.80s with 32 actions; the immediate cached rerun
      passed in 0.45s with 1 internal action.
    - Added ScaleOut/global-backend-outage ST:
      `WorkerPushMetaScaleOutFaultTest.LEVEL1_ScaleOutSurvivesTransientGlobalBackendOutage` passed 1/1 in 14.18s. The
      initial RED probe that shut down ETCD before worker 2 could read topology failed in 10.78s with worker 2 exiting
      during startup; a second RED probe that waited for `MigrateData.afterAdmission` failed in 17.60s because this
      ScaleOut data set does not deterministically trigger that migration RPC, so the final accepted ST targets the
      stable post-ScaleOut transient-outage window.
    - Fresh exact post-format validation: CLion remote `tests-index` rebuilt with URMA Mock in 108s and refreshed 1156
      compile-command entries; the two ScaleOut STs
      `LEVEL1_ScaleOutSurvivesTransientGlobalBackendOutage:LEVEL1_ScaleOutPreservesDataWhenWorkerIsLocallyIsolated`
      passed in 27.95s; Bazel 7.4.1 `//tests/st/client/object_cache:client_dfx_test` passed in 24.15s and cached rerun
      passed in 0.34s.
    - Added metadata recovery membership-churn UT:
      `SlotRecoveryTest.DeferredRecoveryMetadataRetryIgnoresNewMembersAfterMembershipChange` passed 1/1 in 0.06s; the
      pair
      `RecoveryMetadataBatchRetriesOnlyFailedIdsAfterMembershipChange:DeferredRecoveryMetadataRetryIgnoresNewMembersAfterMembershipChange`
      passed 2/2 in 0.08s after a CLion remote `tests-index` rebuild with URMA Mock in 106s.
    - Added missing Bazel 7.4.1 coverage target
      `//tests/ut/worker:slot_recovery_manager_test`; build passed in 23.79s and cached rerun passed in 0.31s. The
      Bazel-built binary also ran
      `SlotRecoveryTest.DeferredRecoveryMetadataRetryIgnoresNewMembersAfterMembershipChange` successfully in 1.37s wall
      time.
  - Follow-up scale/fault cases to add before claiming full story closure:
    1. ScaleOut while one existing worker is isolated is now covered at ST level by
       `WorkerPushMetaScaleOutFaultTest.LEVEL1_ScaleOutPreservesDataWhenWorkerIsLocallyIsolated`; topology planning is
       covered by
       `ScaleOutDoesNotUseFailedWorkerAsMigrationSource`; active ScaleOut replan after a joining-member failure is
       covered by `ScaleOutReplanDropsFailedJoiningAndKeepsRemainingJoiningBatch`; Failure final resuming an interrupted
       ScaleOut batch is covered by `FailureFinalResumesScaleOutBatchWhenJoiningMemberSurvives`.
    2. ScaleIn voluntary source plus concurrent peer local-isolation is now covered at topology replan level by
       `ScaleInSourceStaysLeavingWhenPeerFails`; Failure final resuming an interrupted ScaleIn batch is covered by
       `FailureFinalResumesScaleInBatchWhenLeavingMemberSurvives`. Migration-target filtering for the same combined path
       is now locked both at the shared runtime admission-contract level by
       `ScaleInAndIsolationRejectMigrationTargetDuringCombinedFaultWindow` and at the real worker RPC level by
       `MigrationTargetCombinedFaultTest.LEVEL1_MigrationTargetFiltersScaleInSourceAndIsolatedPeerTogether`.
    3. ScaleOut plus transient global backend outage is now covered at ST level by
       `WorkerPushMetaScaleOutFaultTest.LEVEL1_ScaleOutSurvivesTransientGlobalBackendOutage`, which verifies a
       completed ScaleOut member and the two existing workers survive a transient global ETCD outage and preserve
       pre-outage object data. It is also covered at failure-classifier level by
       `ScaleOutMembersSurviveGlobalBackendOutagePause`; ScaleOut progress post-commit outage idempotency is covered by
       `ScaleOutProgressPostCommitFailureDoesNotDuplicateCallback`; ScaleIn task-overlap marker post-commit outage
       idempotency is covered at executor contract level by
       `ScaleInMetadataPostCommitFailureDoesNotDuplicateCallbackBeforeGateOpens`.
    4. Recovery metadata batch with mixed success/failure while membership changes is now covered at UT level by
       `RecoveryMetadataBatchRetriesOnlyFailedIdsAfterMembershipChange` and
       `DeferredRecoveryMetadataRetryIgnoresNewMembersAfterMembershipChange`. The second test changes membership between
       the first failed metadata push and the deferred retry, then verifies the retry keeps the original recovered-meta
       payload and sends only the failed object id. Broader ST-level membership churn around the same path remains a
       separate hardening follow-up. A direct attempt to extend
       `MetadataRecoveryTest.MetadataRecoveryBestEffortRetryDoesNotBlockAvailability` with dynamic ScaleOut during
       `BeforeRetryFailedMetadataRecovery` was rejected because the existing brpc restart window can fail before the new
       churn step (`WaitNodeReady(WORKER, 1)` returned `K_NOT_READY`/`Subprocess is abnormal` with `PushMetaToWorker`
       rejected as `NOT_RECOVERING`), so this needs a dedicated stable fixture rather than enlarging that restart ST.
    5. Stream local-isolation data retention is now covered by the current admission ST; broader Stream scale/fault
       data-retention ST remains pending if we want end-to-end evidence beyond the local-isolation recovery path. The
       existing Stream scale data-verification cases are present, but the current bRPC validation path explicitly skips
       the data-verification ScaleUp/ScaleDown/passive-fault cases because of the historical bRPC migration gap. A
       direct `--use_brpc=false` attempt against the current ST binary still launched workers with `-use_brpc=true`, so
       that run is counted only as skip evidence, not as ZMQ coverage.

## Fast Verification

- Recent focused verification for scale/fault overlap coverage:
  - Fresh Stream scale/fault evidence:
    `ds_st_stream_cache --gtest_filter="StreamClientAdmissionTest.LEVEL1_StreamClientRejectsReadWriteDuringIsolationAndRecovering"`
    passed 1/1 in 11.55s wall time on the bRPC path, covering Stream admission plus pre-isolation data retention after
    recovery. The broader bRPC Stream scale/fault filter
    `DataVerificationStreamClientScaleTest.TestVoluntaryScaleUp:DataVerificationStreamClientScaleTest.TestVoluntaryScaleDown:DataVerificationStreamClientPassiveScaleTest.TestPassiveScaleDown:DataVerificationStreamClientPassiveScaleTest.TestRestartPassiveScaleDown:StreamClientPassiveScaleTest.LEVEL1_TestRestartPassiveScaleDown`
    ran 5 tests in 24.57s with 1 pass and 4 skips; the pass is the non-data-verification passive restart path, while
    all four data-verification cases skip under the known bRPC migration gap. A follow-up
    `--use_brpc=false` run of the four data-verification tests ran 4/4 skipped in 9.20s because the workers still
    launched with `-use_brpc=true`, so it does not provide non-bRPC coverage.
  - Extended boundary coverage:
    `WorkerRuntimeModuleBoundaryTest.test_object_cache_service_does_not_keep_etcd_store` now also checks the
    `worker_oc_service_impl` Bazel target, and
    `WorkerRuntimeModuleBoundaryTest.test_worker_isolation_coordinator_bazel_declares_runtime_facade_dependency`
    checks the runtime coordinator BUILD dependency.
  - Initial RED: the object-cache boundary failed on the lingering
    `//src/datasystem/common/kvstore/etcd:etcd_store` Bazel dependency; the runtime coordinator boundary failed because
    `worker_isolation_coordinator.h` includes `worker_runtime_facade.h` but the Bazel target did not depend on
    `:worker_runtime_facade`.
  - GREEN: `python3 -m unittest tests/scripts/test_worker_runtime_module_boundary.py` passed 16/16 tests in 0.005s.
  - GREEN: `/usr/local/bin/bazel-7.4.1 --output_user_root=$PWD/.bazel-cache build --distdir=$PWD/.bazel-cache/distdir --config=release --config=test --config=urma_mock //src/datasystem/worker/object_cache:worker_oc_service_impl`
    passed in 166.319s after the dependency fix. The first attempt reproduced the missing `worker_runtime_facade.h`
    Bazel dependency and also showed the Bazel action cache for this focused path was not fully warm.
  - Added 1 boundary-contract test:
    `WorkerRuntimeModuleBoundaryTest.test_object_cache_coordination_headers_hide_backend_detail`.
  - Initial RED: the new boundary test failed because `central_metadata_address_resolver.h` and
    `object_metadata_coordination_reader.h` directly included `coordination_backend/coordination_backend.h` and did not
    forward declare `ICoordinationBackend`.
  - GREEN: `python3 -m unittest tests/scripts/test_worker_runtime_module_boundary.py` passed 15/15 tests in 0.008s.
  - GREEN: `scripts/clion_remote_build.sh tests-index` passed in 204s with third-party cache hit (`Compile thirdparty
    libraries success, total wall time: 1s`), URMA Mock enabled, and 1154 compile database entries.
  - GREEN: `ds_ut_object --gtest_filter="CentralMetadataAddressResolverTest.*"` passed 2/2 tests in 0ms gtest time,
    2.23s wall time.
  - GREEN: `ds_ut_object --gtest_filter="NotifyRemoteGetMigrationTest.QueryMetadataUsesCoordinationStoreLogicalKey:NotifyRemoteGetMigrationTest.QueryMetadataRejectsUnavailableCoordinationBackend"`
    passed 2/2 tests in 1ms gtest time, 1.75s wall time.
  - GREEN: `git diff --check` clean; `git clang-format --diff HEAD -- <changed-files>` reported no formatting changes.
  - Added 1 boundary-contract test:
    `WorkerRuntimeModuleBoundaryTest.test_slot_recovery_store_header_hides_coordination_backend_detail`.
  - Initial RED: the new boundary test failed because `slot_recovery_store.h` directly included
    `coordination_backend/coordination_backend.h` and did not forward declare `ICoordinationBackend`.
  - GREEN: `scripts/clion_remote_build.sh tests-index` passed in 201s with third-party cache hit (`Compile thirdparty
    libraries success, total wall time: 0s`), URMA Mock enabled, and 1154 compile database entries.
  - GREEN: `ds_ut_object --gtest_filter="SlotRecoveryTest.*"` passed 22/22 tests in 175ms gtest time, 0.23s wall time.
  - GREEN: `python3 -m unittest tests/scripts/test_worker_runtime_module_boundary.py` passed 14/14 tests in 0.004s.
  - Hardened 3 existing ST cases:
    `MigrationTargetIsolationTest.LEVEL1_MigrationTargetFiltersIsolatedAndRecoveringWorker`,
    `MigrationTargetOomTest.LEVEL1_MigrationTargetFiltersOutOfMemoryWorker`, and
    `MigrationTargetDrainingTest.LEVEL1_MigrationTargetFiltersDrainingWorker`.
  - Initial RED: the direct test stub path timed out 3/3 cases in 27.80s under `-use_brpc=true` because it bypassed the
    production `RpcStubCacheMgr`/brpc generic stub path; after switching to `WorkerRemoteWorkerOCApi`, the first run
    exposed missing ST-side `RpcStubCacheMgr` initialization as SIGSEGV in `RpcStubCacheMgr::GetStub`, then an OOM
    setup race where the worker could already be in `RECOVERING`.
  - GREEN: `scripts/clion_remote_build.sh tests-index` passed in 109s with third-party cache hit (`Compile thirdparty
    libraries success, total wall time: 1s`), URMA Mock enabled, and 1154 compile database entries.
  - GREEN: `ds_st_object_cache --gtest_filter="MigrationTargetIsolationTest.LEVEL1_MigrationTargetFiltersIsolatedAndRecoveringWorker:MigrationTargetOomTest.LEVEL1_MigrationTargetFiltersOutOfMemoryWorker:MigrationTargetDrainingTest.LEVEL1_MigrationTargetFiltersDrainingWorker"`
    passed 3/3 tests in 25.203s gtest time, 25.27s wall time.
  - Added 1 UT case:
    `TopologyTaskExecutorTest.ScaleOutProgressPostCommitFailureDoesNotDuplicateCallback`.
  - GREEN: `scripts/clion_remote_build.sh tests-index` passed in 86s with third-party cache hit (`Compile thirdparty
    libraries success, total wall time: 1s`), URMA Mock enabled, and 1154 compile database entries.
  - GREEN: `cluster_topology_contract_ut --gtest_filter="TopologyTaskExecutorTest.ScaleOutProgressPostCommitFailureDoesNotDuplicateCallback"`
    passed 1/1 test in 1ms gtest time, 0.03s wall time.
  - GREEN: `cluster_topology_contract_ut --gtest_filter="TopologyTaskExecutorTest.*"` passed 28/28 tests in 80ms
    gtest time, 0.12s wall time.
  - Added 1 UT case:
    `TopologyTaskExecutorTest.ScaleInMetadataPostCommitFailureDoesNotDuplicateCallbackBeforeGateOpens`.
  - Initial RED: 3-case focused suite failed in 0.04s because the new test asserted single-threaded metadata callback
    ordering while executor legitimately submits sibling metadata callbacks concurrently; the expectation was narrowed
    to the real contract.
  - GREEN: `scripts/clion_remote_build.sh tests-index` passed in 99s with third-party cache hit (`Compile thirdparty
    libraries success, total wall time: 0s`), URMA Mock enabled, and 1154 compile database entries.
  - GREEN: `cluster_topology_contract_ut --gtest_filter="TopologyTaskExecutorTest.ScaleInMetadataPostCommitFailureDoesNotDuplicateCallbackBeforeGateOpens:TopologyTaskExecutorTest.ScaleInMetadataGateWaitsForAllAdmittedSourceTasks:TopologyTaskExecutorTest.RetriesOnlyProgressAfterScaleInProgressCasFailure"`
    passed 3/3 tests in 5ms gtest time, 0.04s wall time.
  - GREEN: `cluster_topology_contract_ut --gtest_filter="TopologyTaskExecutorTest.*"` passed 27/27 tests in 82ms
    gtest time, 0.12s wall time.
  - GREEN: `python3 -m unittest tests/scripts/test_worker_runtime_module_boundary.py` passed 13/13 tests in 0.005s.
  - GREEN: `git diff --check` and `git clang-format --diff HEAD -- tests/ut/cluster/topology_task_executor_test.cpp`
    were clean.
  - Added 1 ST case:
    `MigrationTargetIsolationTest.LEVEL1_ObjectClientRejectsReadWriteDuringIsolationAndRecovering`.
  - Initial RED: the first version treated `ObjectClient::Create` as a write RPC and failed in 71.25s because Create can
    succeed as local buffer allocation while the real write/admission point is `Buffer::Publish`.
  - GREEN: `scripts/clion_remote_build.sh tests-index` passed twice after the test edits, with third-party cache hit
    (`Compile thirdparty libraries success, total wall time: 0s`), URMA Mock enabled, 1154 compile database entries, and
    total script times 117s / 110s. The script still emitted repeated-strip `debuglink section already exists`
    diagnostics during install but exited 0.
  - GREEN: `ds_st_object_cache --gtest_filter="MigrationTargetIsolationTest.LEVEL1_ObjectClientRejectsReadWriteDuringIsolationAndRecovering"`
    passed 1/1 test in 10.584s gtest time, 10.66s wall time, covering `NORMAL_READ` and `NORMAL_WRITE` rejection during
    both `LOCAL_ISOLATED` and `RECOVERING`.
  - GREEN: `ds_st_object_cache --gtest_filter="MigrationTargetIsolationTest.LEVEL1_MigrationTargetFiltersIsolatedAndRecoveringWorker:MigrationTargetIsolationTest.LEVEL1_ObjectClientRejectsReadWriteDuringIsolationAndRecovering:MigrationTargetOomTest.LEVEL1_MigrationTargetFiltersOutOfMemoryWorker:MigrationTargetDrainingTest.LEVEL1_MigrationTargetFiltersDrainingWorker"`
    passed 4/4 tests in 35.874s gtest time, 35.95s wall time.
  - GREEN: `python3 -m unittest tests/scripts/test_worker_runtime_module_boundary.py` passed 16/16 tests in 0.010s.
  - GREEN: `git diff --check` clean.
  - Added 1 ST case:
    `KVClientEtcdDfxTest.LEVEL1_KVClientRejectsReadWriteDuringIsolationAndRecovering`.
  - Initial RED: the first version started only the target worker and failed in 18.84s because no peer could prove the
    coordination backend was still reachable, so the keepalive failure stayed classified as a global backend outage
    rather than `LOCAL_ISOLATED`. Adding only Object-style worker flags still reproduced the same RED in 18.29s.
  - GREEN: `scripts/clion_remote_build.sh tests-index` passed after the test edits, with third-party cache hit
    (`Compile thirdparty libraries success, total wall time: 1s`), URMA Mock enabled, 1154 compile database entries, and
    total script time 97s. The script still emitted repeated-strip `debuglink section already exists` diagnostics during
    install but exited 0.
  - GREEN: `ds_st_kv_cache --gtest_filter="KVClientEtcdDfxTest.LEVEL1_KVClientRejectsReadWriteDuringIsolationAndRecovering"`
    passed 1/1 test in 11.371s gtest time, 11.45s wall time, covering KV `Get` and `Set` rejection during both
    `LOCAL_ISOLATED` and `RECOVERING` and verifying the original key is readable after recovery evidence completes.
  - Added 1 boundary-contract test:
    `WorkerRuntimeModuleBoundaryTest.test_worker_runtime_facade_does_not_expose_state_manager`.
  - Initial RED: the new boundary test failed because `WorkerRuntimeFacade` still exposed `RuntimeState()` and
    `WorkerRuntimeStateManager &` in its public header; 17 boundary tests ran in 0.011s with 1 failure.
  - GREEN: removed the public `RuntimeState()` accessor and updated affected UTs to use facade semantic methods.
    `python3 -m unittest tests/scripts/test_worker_runtime_module_boundary.py` passed 17/17 tests in 0.006s.
  - GREEN: `scripts/clion_remote_build.sh tests-index` passed in 199s with third-party cache hit (`Compile thirdparty
    libraries success, total wall time: 0s`), `BUILD_WITH_URMA_MOCK` enabled, and 1154 compile database entries.
  - GREEN: `ds_ut --gtest_filter="WorkerRuntimeFacadeTest.*:WorkerServiceImplAdmissionTest.*"` passed 6/6 tests in
    1ms gtest time, 0.05s wall time.
  - GREEN: `ds_ut_object --gtest_filter="WorkerOcServiceImplTest.*:ScaleDownNodeSelectorTest.ResourceReportReadinessFollowsRuntimeState:ScaleDownNodeSelectorTest.ResourceReportsRetryOutOfMemoryRecoveryUntilRunning:ScaleDownNodeSelectorTest.UnregisterResourceRecoveredHandlerWaitsForActiveCallback"`
    passed 71/71 tests in 11.144s gtest time, 11.20s wall time.
  - GREEN: `git diff --check` clean; `git clang-format --diff HEAD -- <changed-cpp-files>` reported no formatting
    changes.
  - Added 1 ST case:
    `StreamClientAdmissionTest.LEVEL1_StreamClientRejectsReadWriteDuringIsolationAndRecovering`.
  - Initial compile RED: the new Stream ST used `std::vector<DataElement>` while the public `Consumer::Receive` API
    expects `std::vector<Element>`.
  - Initial runtime RED: the focused Stream ST failed 1/1 in 11.692s gtest time, 11.76s wall time because the first
    assertion expected runtime mode text; Stream admission reaches the public health gate first and reports
    `K_NOT_READY`/`Worker not ready`. The test now asserts protocol-layer rejection, while UTs cover mode/reason detail.
  - GREEN: `scripts/clion_remote_build.sh tests-index` passed in 66s with third-party cache hit (`Compile thirdparty
    libraries success, total wall time: 0s`), source build time 5s, `BUILD_WITH_URMA_MOCK` enabled, and 1155 compile
    database entries.
  - GREEN: `ds_st_stream_cache --gtest_filter="StreamClientAdmissionTest.LEVEL1_StreamClientRejectsReadWriteDuringIsolationAndRecovering"`
    passed 1/1 test in 11.085s gtest time, 11.15s wall time, covering Stream create/subscribe/receive rejection during
    both `LOCAL_ISOLATED` and `RECOVERING`, then verifying recovered create/subscribe/send/receive succeeds.
  - Added 1 boundary-contract test:
    `WorkerRuntimeModuleBoundaryTest.test_control_backend_probe_runtime_target_does_not_depend_on_object_cache`.
  - Initial RED: the new boundary test failed because the `worker_control_backend_probe` Bazel target still depended on
    `worker_worker_oc_api` and `worker_worker_peer_state_codec`, and the runtime implementation directly included the
    object-cache RPC/codec headers.
  - GREEN: `python3 -m unittest tests/scripts/test_worker_runtime_module_boundary.py` passed 18/18 tests in 0.006s
    after changing runtime probe orchestration to use injected `IControlBackendPeerProbeClient` instances and moving the
    concrete Worker-Worker OC RPC adapter into `WorkerOCServer`.
  - GREEN: `scripts/clion_remote_build.sh tests-index` passed in 125s with third-party cache hit (`Compile thirdparty
    libraries success, total wall time: 0s`), source build time 51s, `BUILD_WITH_URMA_MOCK` enabled, and 1155 compile
    database entries.
  - GREEN: `ds_st_stream_cache --gtest_filter="StreamClientAdmissionTest.LEVEL1_StreamClientRejectsReadWriteDuringIsolationAndRecovering"`
    passed 1/1 test in 11.088s gtest time, 11.16s wall time after the peer-probe injection refactor.
  - Stabilized 1 existing Object ST:
    `WorkerPushMetaTest.LEVEL1_TestKeepAliveLocalIsolationRecoversThroughEvidenceGate`.
  - RED: after the peer-probe injection refactor this ST failed twice around 20s wall time, then failed in 14.44s/14.35s
    while waiting for the old direct `Create` inject-count probe. Logs showed local peer probing still worked
    (`Keepalive backend failure scope is local, probeTargets: 1, observations: 1`) and `AfterMarkLocalIsolated` executed,
    but closed worker admission prevented the direct stub request from reaching the internal `worker.Create.beforeValidate`
    injection point.
  - RED: switching the probe to public `ObjectClient::Create` showed repeated fast successful client-side creates instead
    of `K_NOT_READY`; this confirmed that Object `Create` prepares a local buffer and `Publish` is the operation that should
    be gated by worker admission.
  - GREEN: the ST now pre-creates unpublished buffers before the fault, asserts `Publish` is rejected with `K_NOT_READY`
    in both `LOCAL_ISOLATED` and `RECOVERING`, verifies the preserved object survives recovery, and checks the rejected
    unpublished keys remain `K_NOT_FOUND`.
  - GREEN: `scripts/clion_remote_build.sh tests-index` passed in 127s with third-party cache hit (`Compile thirdparty
    libraries success, total wall time: 0s`), source build time 44s, `BUILD_WITH_URMA_MOCK` enabled, and 1155 compile
    database entries.
  - GREEN: `ds_st_object_cache --gtest_filter="WorkerPushMetaTest.LEVEL1_TestKeepAliveLocalIsolationRecoversThroughEvidenceGate"`
    passed 1/1 test in 11.87s wall time.
  - Added 1 boundary-contract test:
    `WorkerRuntimeModuleBoundaryTest.test_data_migrator_does_not_depend_on_topology_callback_executor`.
  - Initial RED: the boundary suite failed 1/19 in 0.011s because `data_migrator.cpp` still included
    `datasystem/cluster/executor/topology_phase_callbacks.h`. Removing only that include exposed the true build-time
    dependency: `CancellationToken` was defined inside the broader callback header.
  - GREEN: split `CancellationToken` into `cluster/executor/cancellation_token.h` and changed `data_migrator.cpp` to
    include only that narrow header. `python3 -m unittest tests/scripts/test_worker_runtime_module_boundary.py` passed
    19/19 tests in 0.016s.
  - GREEN: `scripts/clion_remote_build.sh tests-index` passed in 247s with third-party cache hit (`Compile thirdparty
    libraries success, total wall time: 0s`), source build time 151s, `BUILD_WITH_URMA_MOCK` enabled, and 1155 compile
    database entries. The script emitted known repeated-strip `debuglink section already exists` diagnostics but exited 0.
  - GREEN: `git diff --check` clean; `git clang-format --diff HEAD -- <changed-files>` reported no formatting changes.
  - Added 1 boundary-contract test:
    `WorkerRuntimeModuleBoundaryTest.test_clear_data_flow_uses_narrow_topology_callback_inputs`.
  - Initial RED: the boundary suite failed 1/20 in 0.005s because `worker_oc_service_clear_data_flow.h` still included
    `datasystem/cluster/executor/topology_phase_callbacks.h`.
  - GREEN: split `TopologyPhaseAction` into `cluster/executor/topology_phase_action.h` and changed clear-data flow to
    include only `topology_phase_action.h`, `key_filter.h`, and `cancellation_token.h`.
    `python3 -m unittest tests/scripts/test_worker_runtime_module_boundary.py` passed 20/20 tests in 0.008s.
  - GREEN: `scripts/clion_remote_build.sh tests-index` passed in 231s with third-party cache hit (`Compile thirdparty
    libraries success, total wall time: 0s`), source build time 150s, `BUILD_WITH_URMA_MOCK` enabled, and 1155 compile
    database entries. The script emitted known repeated-strip `debuglink section already exists` diagnostics but exited 0.
  - GREEN: `git diff --check` clean; `git clang-format --diff HEAD -- <changed-files>` reported no formatting changes.
  - Added 1 boundary-contract test:
    `WorkerRuntimeModuleBoundaryTest.test_worker_oc_service_impl_uses_narrow_topology_callback_inputs`.
  - Initial RED: the boundary suite failed 1/21 in 0.010s because `worker_oc_service_impl.h` still included
    `datasystem/cluster/executor/topology_phase_callbacks.h`.
  - GREEN: split `TopologyCleanupEffect` into `cluster/executor/topology_cleanup_effect.h` and changed
    `WorkerOCServiceImpl` to include only action/filter/cancellation/effect topology inputs. `python3 -m unittest
    tests/scripts/test_worker_runtime_module_boundary.py` passed 21/21 tests in 0.008s.
  - GREEN: `scripts/clion_remote_build.sh tests-index` passed in 233s with third-party cache hit (`Compile thirdparty
    libraries success, total wall time: 0s`), source build time 148s, `BUILD_WITH_URMA_MOCK` enabled, and 1155 compile
    database entries. The script emitted known repeated-strip `debuglink section already exists` diagnostics but exited 0.
  - GREEN: `git diff --check` clean; `git clang-format --diff HEAD -- <changed-files>` reported no formatting changes.
  - Added 1 boundary-contract test:
    `WorkerRuntimeModuleBoundaryTest.test_master_metadata_managers_use_narrow_topology_callback_inputs`.
  - Initial RED: the boundary suite failed 1/22 in 0.006s because master object/stream metadata manager headers still
    included `datasystem/cluster/executor/topology_phase_callbacks.h`.
  - GREEN: changed `oc_metadata_manager.h`, `oc_migrate_metadata_manager.h`, `sc_metadata_manager.h`, and
    `sc_migrate_metadata_manager.h` to include only action/filter/storage-scan/cancellation topology input headers.
    `python3 -m unittest tests/scripts/test_worker_runtime_module_boundary.py` passed 22/22 tests in 0.012s.
  - GREEN: `scripts/clion_remote_build.sh tests-index` passed in 211s with third-party cache hit (`Compile thirdparty
    libraries success, total wall time: 0s`), source build time 120s, `BUILD_WITH_URMA_MOCK` enabled, and 1155 compile
    database entries. The script emitted known repeated-strip `debuglink section already exists` diagnostics but exited 0.
  - GREEN: `git diff --check` clean; `git clang-format --diff HEAD -- <changed-files>` reported no formatting changes.
  - Added 1 boundary-contract test:
    `WorkerRuntimeModuleBoundaryTest.test_full_topology_callback_contract_is_owned_by_executor_and_runtime_adapter_only`.
  - Initial RED: the boundary suite failed 1/23 in 0.077s because `worker_oc_server.cpp` directly included the full
    `datasystem/cluster/executor/topology_phase_callbacks.h` contract instead of relying on the worker runtime adapter.
  - GREEN: removed the unnecessary direct include from `worker_oc_server.cpp`; only the topology executor and
    `WorkerTopologyPhaseCallbacks` adapter include the full callback contract. `python3 -m unittest
    tests/scripts/test_worker_runtime_module_boundary.py` passed 23/23 tests in 0.077s.
  - GREEN: `scripts/clion_remote_build.sh tests-index` passed in 148s with third-party cache hit (`Compile thirdparty
    libraries success, total wall time: 0s`), source build time 51s, `BUILD_WITH_URMA_MOCK` enabled, and 1155 compile
    database entries. The script emitted known repeated-strip `debuglink section already exists` diagnostics but exited 0.
  - GREEN: `git diff --check` clean; `git clang-format --diff HEAD -- <changed-files>` reported no formatting changes.
  - Added 1 boundary-contract test:
    `WorkerRuntimeModuleBoundaryTest.test_topology_engine_uses_coordination_backend_not_concrete_etcd`.
  - Changed the C++ composition contract in
    `TopologyRuntimeCompositionTest.KeepsBackendEventsAndMembershipMutationsInsideOwners` to reject public
    `UseEtcd(EtcdStore&)` selection and require `UseUnifiedCoordinationBackends(...)` instead.
  - Initial RED: `scripts/clion_remote_build.sh tests-index` failed after 6.8s at
    `topology_runtime_composition_test.cpp` static assertions because `TopologyEngine::Builder` still exposed
    concrete `EtcdStore` selection and lacked the `ICoordinationBackend`-level unified backend injection method.
  - GREEN: `TopologyEngine` no longer includes or constructs `EtcdStore` / `EtcdCoordinationBackend`; table
    registration goes through `ICoordinationBackend::CreateTableWithExactPrefix`, and `WorkerOCServer` performs the
    ETCD adapter composition before passing member/controller backends into the Engine. The Bazel dependency from
    `cluster_topology` to `//src/datasystem/common/kvstore/etcd:etcd_store` was removed.
  - GREEN: `python3 -m unittest tests/scripts/test_worker_runtime_module_boundary.py` passed 24/24 tests in 0.066s.
  - GREEN: `scripts/clion_remote_build.sh tests-index` passed in 116s with third-party cache hit (`Compile thirdparty
    libraries success, total wall time: 1s`), source build time 41s, `BUILD_WITH_URMA_MOCK` enabled, and 1155 compile
    database entries. The script emitted known repeated-strip `debuglink section already exists` diagnostics but exited 0.
  - GREEN: remote
    `cluster_topology_contract_ut --gtest_filter="TopologyRuntimeCompositionTest.*"` passed 1/1 test in 0ms.
  - GREEN: `git diff --check` clean; `git clang-format --diff HEAD -- <changed-files>` reported no formatting changes.
  - Added 1 boundary-contract test:
    `WorkerRuntimeModuleBoundaryTest.test_topology_phase_callbacks_use_injected_metadata_actions`.
  - Updated 1 existing C++ contract case:
    `TopologyBusinessContractTest.FailureLocalActionsAreInjectedBehindHook` now verifies both object-cache and metadata
    actions are injected behind narrow runtime hook interfaces.
  - Initial RED: the boundary suite failed 1/25 in 0.078s because `worker_topology_phase_callbacks.{h,cpp}` still
    directly referenced `MetadataManagerHolder` and concrete OC/SC metadata manager classes.
  - GREEN: introduced `IWorkerTopologyMetadataActions` in runtime and moved concrete metadata manager calls into
    `WorkerTopologyMetadataActions` under `worker/object_cache`, preserving failure-task order:
    metadata recovery, metadata cleanup, local data cleanup, device metadata cleanup, then RPC-stub cleanup.
  - GREEN: `python3 -m unittest tests/scripts/test_worker_runtime_module_boundary.py` passed 25/25 tests in 0.066s.
  - GREEN: `scripts/clion_remote_build.sh tests-index` passed in 188s with third-party cache hit (`Compile thirdparty
    libraries success, total wall time: 1s`), `BUILD_WITH_URMA_MOCK` enabled, and 1156 compile database entries.
  - GREEN: remote `ds_ut_object --gtest_filter="TopologyBusinessContractTest.*"` passed 3/3 tests in 1ms gtest time,
    about 1.18s wall time.
  - GREEN: `git diff --check` clean; `git clang-format --diff HEAD -- <changed-files>` reported no formatting changes.
  - Added 1 UT case:
    `TopologyPlanBuilderTest.ScaleOutReplanDropsFailedJoiningAndKeepsRemainingJoiningBatch`.
  - GREEN: `scripts/clion_remote_build.sh tests-index` passed in 94s with third-party cache hit (`Compile thirdparty
    libraries success, total wall time: 0s`), source build time 8s, `BUILD_WITH_URMA_MOCK` enabled, and 1156 compile
    database entries.
  - GREEN: remote
    `cluster_topology_contract_ut --gtest_filter="TopologyPlanBuilderTest.ScaleOutReplanDropsFailedJoiningAndKeepsRemainingJoiningBatch"`
    passed 1/1 test in 0ms gtest time, about 1.24s wall time.
  - GREEN: remote
    `cluster_topology_contract_ut --gtest_filter="TopologyPlanBuilderTest.*:HashAlgorithmTest.ScaleOutDoesNotUseFailedWorkerAsMigrationSource:TopologyFailureClassifierTest.ScaleOutMembersSurviveGlobalBackendOutagePause"`
    passed 10/10 tests in 0ms gtest time, about 1.20s wall time.
  - GREEN: `git diff --check` clean; `git clang-format --diff HEAD -- tests/ut/cluster/topology_plan_builder_test.cpp`
    reported no formatting changes.
  - Added 1 UT case:
    `TopologyPlanBuilderTest.FailureFinalResumesScaleOutBatchWhenJoiningMemberSurvives`.
  - GREEN: `scripts/clion_remote_build.sh tests-index` passed in 73s with third-party cache hit (`Compile thirdparty
    libraries success, total wall time: 1s`), source build time 8s, `BUILD_WITH_URMA_MOCK` enabled, and 1156 compile
    database entries.
  - GREEN: remote
    `cluster_topology_contract_ut --gtest_filter="TopologyPlanBuilderTest.FailureFinalResumesScaleOutBatchWhenJoiningMemberSurvives"`
    passed 1/1 test in 0ms gtest time, about 1.21s wall time.
  - GREEN: remote `cluster_topology_contract_ut --gtest_filter="TopologyPlanBuilderTest.*"` passed 9/9 tests in 0ms
    gtest time, about 1.20s wall time.
  - GREEN: `git diff --check` clean; `git clang-format --diff HEAD -- tests/ut/cluster/topology_plan_builder_test.cpp`
    reported no formatting changes.
  - Added 1 UT case:
    `TopologyPlanBuilderTest.FailureFinalResumesScaleInBatchWhenLeavingMemberSurvives`.
  - GREEN: `scripts/clion_remote_build.sh tests-index` passed in 83s with third-party cache hit (`Compile thirdparty
    libraries success, total wall time: 1s`), source build time 8s, `BUILD_WITH_URMA_MOCK` enabled, and 1156 compile
    database entries.
  - GREEN: remote
    `cluster_topology_contract_ut --gtest_filter="TopologyPlanBuilderTest.FailureFinalResumesScaleInBatchWhenLeavingMemberSurvives"`
    passed 1/1 test in 0ms gtest time, about 1.29s wall time.
  - GREEN: remote `cluster_topology_contract_ut --gtest_filter="TopologyPlanBuilderTest.*"` passed 10/10 tests in 0ms
    gtest time, about 1.24s wall time.
  - GREEN: `git diff --check` clean; `git clang-format --diff HEAD -- tests/ut/cluster/topology_plan_builder_test.cpp`
    reported no formatting changes.
  - Bazel RED: remote Bazel 7.4.1 focused build
    `//src/datasystem/cluster:cluster_topology` failed because the generated virtual include tree could not expose
    `datasystem/cluster/executor/cancellation_token.h`; the header had been split into the topology scope but was not
    listed in the Bazel `hdrs`.
  - GREEN: after adding `executor/cancellation_token.h` to `cluster_topology_scope` hdrs, remote Bazel 7.4.1 focused
    build with `--config=release --config=test --config=urma_mock` and shared `.bazel-cache/distdir` passed in
    61.912s with 80 actions.
  - GREEN: the same remote Bazel 7.4.1 command rerun against the same cache passed in 0.295s with 1 internal action,
    confirming cache reuse for this focused target.
  - RED: added
    `WorkerRuntimeModuleBoundaryTest.test_topology_availability_public_header_uses_runtime_facade_only`; it failed
    because `worker_topology_availability_admission.h` still included `worker_runtime_state.h` and
    `worker_recovery_controller.h` and exposed StateManager/RecoveryController overloads.
  - GREEN: removed those public overloads, changed topology-availability UTs to drive behavior through
    `WorkerRuntimeFacade`, and reran `tests/scripts/test_worker_runtime_module_boundary.py`: 26/26 tests in 0.116s.
  - GREEN: `scripts/clion_remote_build.sh tests-index` passed in 186s with third-party cache hit (`Compile thirdparty
    libraries success, total wall time: 0s`), `BUILD_WITH_URMA_MOCK` enabled, and 1156 compile database entries.
  - GREEN: remote
    `ds_ut --gtest_filter="WorkerTopologyAvailabilityAdmissionTest.*"` from the CLion build tree passed 12/12 tests in
    0ms gtest time, about 1.21s wall time.
  - Added 1 UT case:
    `WorkerServiceAdmissionTest.ScaleInAndIsolationRejectMigrationTargetDuringCombinedFaultWindow`.
  - GREEN: `scripts/clion_remote_build.sh tests-index` passed in 113s with third-party cache hit (`Compile thirdparty
    libraries success, total wall time: 0s`), `BUILD_WITH_URMA_MOCK` enabled, source build time 28s, and 1156 compile
    database entries.
  - GREEN: remote
    `ds_ut --gtest_filter="WorkerServiceAdmissionTest.ScaleInAndIsolationRejectMigrationTargetDuringCombinedFaultWindow"`
    passed 1/1 test in 0ms gtest time, about 1.36s wall time.
  - GREEN: remote `ds_ut --gtest_filter="WorkerServiceAdmissionTest.*:WorkerAdmissionFacadeTest.*"` passed 11/11 tests
    in 51ms gtest time, about 1.42s wall time.
  - Extended 1 existing ST case:
    `StreamClientAdmissionTest.LEVEL1_StreamClientRejectsReadWriteDuringIsolationAndRecovering` now sends one Stream
    element before local isolation and verifies the same payload is readable after recovery.
  - GREEN: `scripts/clion_remote_build.sh tests-index` completed with third-party cache hit (`Compile thirdparty
    libraries success, total wall time: 1s`), source build time 20s, `BUILD_WITH_URMA_MOCK` enabled, and 1156 compile
    database entries.
  - GREEN: remote
    `ds_st_stream_cache --gtest_filter="StreamClientAdmissionTest.LEVEL1_StreamClientRejectsReadWriteDuringIsolationAndRecovering"`
    passed 1/1 test in 10.890s gtest time, covering Stream read/write admission during isolation/recovery plus
    pre-isolation data retention after recovery.
  - Added 1 ST case:
    `MigrationTargetCombinedFaultTest.LEVEL1_MigrationTargetFiltersScaleInSourceAndIsolatedPeerTogether`.
  - GREEN with remote connection caveat: `scripts/clion_remote_build.sh tests-index` reused third-party cache
    (`Compile thirdparty libraries success, total wall time: 0s`), built source in 25s with `BUILD_WITH_URMA_MOCK`
    enabled, and linked `ds_st_object_cache`; the SSH session then closed during install/strip, so the script returned
    255 after the focused binary had already been produced.
  - GREEN: remote
    `ds_st_object_cache --gtest_filter="MigrationTargetCombinedFaultTest.LEVEL1_MigrationTargetFiltersScaleInSourceAndIsolatedPeerTogether"`
    passed 1/1 test in 14.980s gtest time, covering the combined voluntary ScaleIn `DRAINING` source plus concurrent
    `LOCAL_ISOLATED` peer worker-to-worker migration-target rejection path.
  - RED, not committed: tried extending
    `MetadataRecoveryTest.MetadataRecoveryBestEffortRetryDoesNotBlockAvailability` to add a third worker while retry
    was paused at `WorkerOcServiceClearDataFlow.BeforeRetryFailedMetadataRecovery`. `scripts/clion_remote_build.sh
    tests-index` passed in 109s with third-party cache hit (`Compile thirdparty libraries success, total wall time: 0s`),
    source build time 25s, `BUILD_WITH_URMA_MOCK` enabled, and 1156 compile database entries. The focused ST failed
    before the new churn assertion at the existing restart readiness step (`WaitNodeReady(WORKER, 1)`), returning
    `K_NOT_READY`/`Subprocess is abnormal`; logs showed `PushMetaToWorker` rejected as `NOT_RECOVERING`. The experiment
    was reverted and the remaining ST-level membership-churn item should use a dedicated stable fixture.
  - GREEN: fixed the restart reconciliation admission race by marking restart reconciliation as pending in both worker
    runtime state and object-cache recovery evidence before fanout. Added
    `WorkerOcServiceImplTest.RestartReconciliationMarksRuntimeRecoveringBeforeFanout`, which first failed in 4ms when
    runtime/evidence stayed `RUNNING`/ready, then passed with
    `WorkerOcServiceImplTest.MasterWorkerClassifiesCleanupAndRecoveryRpcs` in 5ms total. Re-ran
    `MetadataRecoveryTest.MetadataRecoveryBestEffortRetryDoesNotBlockAvailability`, which passed in 15.300s after the
    fix; stale failed-run worker/etcd processes from earlier debugging were cleaned from the remote worktree.
  - RED: the focused object-cache evidence regression filter failed 2/45 after the slot-recovery backend-store fix
    because `WorkerOcServiceImplTest.BuildObjectCacheRecoveryEvidenceRequiresMetadataAndSlotReadiness` and
    `WorkerOcServiceImplTest.BuildObjectCacheRecoveryEvidenceTreatsNoMetadataWorkAsReady` still expected the old
    `slot_incidents_ready=0/0` detail even when slot recovery was disabled.
  - GREEN: updated those two expectations to the current `slot_recovery_disabled` evidence detail and re-ran the focused
    pair: 2/2 passed in 0.05s. Re-ran the broader object-cache evidence/admission filter: 45/45 passed in 0.08s.
  - GREEN: `scripts/clion_remote_build.sh tests-index` passed with third-party cache hit
    (`Compile thirdparty libraries success, total wall time: 0s`), `BUILD_WITH_URMA_MOCK` enabled, source build time
    30s, total build time 146s, and 1156 compile database entries; local CompDB validation reported 1156 entries and
    0 missing sources.
  - GREEN: local boundary/index scripts passed 29/29 in 0.322s:
    `python3 -m unittest tests/scripts/test_worker_runtime_module_boundary.py tests/scripts/test_ensure_clion_compdb_project.py`.
  - GREEN: focused UT regression after the slot evidence update passed 154 tests:
    runtime/admission/object-read 56/56 in 1.33s, object-cache evidence/recovery/admission 45/45 in 0.08s, cluster
    topology/coordination contracts 51/51 in 1.96s, and stream admission UT 2/2 in 0.05s.
  - GREEN: focused ST regression passed 18 tests:
    KV isolation/scale-down/fallback 3/3 in 40.07s, object isolation/recovery/combined scale-fault 6/6 in 129.57s,
    legacy object reference/coordinator compatibility 8/8 in 63.69s, and stream local-isolation data retention 1/1 in
    10.94s. The stream ST filter must use
    `StreamClientAdmissionTest.LEVEL1_StreamClientRejectsReadWriteDuringIsolationAndRecovering`; the old
    `StreamAdmissionTest.*` filter matches 0 tests and must not be counted as coverage.
  - GREEN: `git diff --check` was clean; `git clang-format --diff HEAD -- tests/ut/worker/object_cache/worker_oc_service_impl_test.cpp`
    reported no formatting changes.
  - GREEN: remote Bazel 7.4.1 focused build used the existing `.bazel-cache/distdir` and
    `--config=release --config=test --config=urma_mock` for `//tests/ut/worker:worker_oc_service_impl_test`.
    The first run as the cache owner `tianti` completed successfully in 5:56.81 with 4407 actions; the immediate rerun
    completed successfully in 1.17s with 1 internal action, confirming the target reuses the Bazel cache. The earlier
    0.01s failure was an execution-user/cache-owner mismatch: the remote SSH session ran as `root` while the worktree and
    `.bazel-cache` are owned by `tianti`.
  - RED: added
    `TopologyEngineTest.KeepAliveScopeCheckReturnsAfterFirstReachablePeerEvidence` to cover keepalive local-isolation
    confirmation when a peer already reports the control backend as AVAILABLE. The first run failed in 2004ms: the
    handler returned true but `probeCalls` was 20 instead of 1, proving the keepalive path still polled until the
    2-second scope budget even after local-isolation evidence was sufficient.
  - GREEN: changed `TopologyEngine::IsControlBackendReachableFromPeers` to return immediately after
    `ProbePeerBackendReachabilityOnce` confirms reachable peer evidence. The same focused case passed in 2ms during the
    first GREEN run and 1ms in the broader filter, with `probeCalls == 1`.
  - GREEN: `scripts/clion_remote_build.sh tests-index` passed with third-party cache hit
    (`Compile thirdparty libraries success, total wall time: 0s`), `BUILD_WITH_URMA_MOCK` enabled, 1157 compile database
    entries, and Ready CompDB output. The GREEN rebuild used 160s total wall time; source build remained incremental.
  - GREEN: local boundary script passed 28/28 in 0.088s; `git diff --check` was clean; `git clang-format --diff HEAD --`
    for `topology_engine.cpp`, `topology_engine_test.cpp`, and `fake_coordination_backend.*` reported no formatting
    changes.
  - GREEN: remote focused CMake UT filter passed 24/24 in 864ms, covering coordination backend contracts, DS
    coordination sessions, topology composition, global outage, asymmetric outage, missing quorum, and the new
    keepalive short-circuit case.
  - Bazel RED/GREEN: remote Bazel 7.4.1 focused build for `//src/datasystem/cluster:cluster_topology` plus
    `//tests/ut/cluster/testing:fake_coordination_backend` first failed in 4.645s because the test fake cpp used the
    CMake-only `ut/cluster/testing/fake_coordination_backend.h` include path. The fix changed that cpp to include its
    same-directory header directly. After syncing the changed files to the remote worktree, the same Bazel command
    passed in 21.570s with 29 actions, using the existing `.bazel-cache/distdir` and `--config=urma_mock`.
  - RED: changed
    `WorkerIsolationCoordinatorTest.LocalRecoveryStartsRecoveringBeforeTopologyReconciliation` to assert local recovery
    does not publish READY before topology/ownership evidence. The test failed in 0ms because
    `readyMembershipPublished` was true.
  - GREEN: added `TopologyEngine::MarkRecovering()` and changed local recovery wiring to publish RECOVERING membership
    through the topology facade instead of READY. Existing backends already promote `RECOVERING`/`RESTARTING` to READY
    in `InformReconciliationDone`, so READY remains tied to reconciliation completion. The worker recovery focused
    filter passed 7/7 tests in 0ms.
  - RED/GREEN: added `TopologyEngineTest.MarkRecoveringPublishesRecoveringMembershipState`; it first failed in 2ms
    because the test fake did not record `UpdateNodeState`, then passed in 2ms after the fake recorded lifecycle state
    mutations. A broader topology/coordination focused filter passed 14/14 tests in 859ms.
  - GREEN: local boundary script passed 28/28 in 0.070s; `git diff --check` was clean; clang-format on the touched
    topology/worker runtime files and tests reported no remaining formatting changes.
  - GREEN: remote Bazel 7.4.1 focused build with existing `.bazel-cache/distdir` and `--config=urma_mock` passed for
    `//src/datasystem/cluster:cluster_topology`, `//src/datasystem/worker/runtime:worker_runtime_core`, and
    `//tests/ut/cluster/testing:fake_coordination_backend` in 17.622s with 18 actions. A first attempt used a wrong
    non-existent label `//src/datasystem/worker/runtime:worker_runtime` and failed during target-pattern parsing in
    0.128s; this was command selection, not a compile failure.
  - RED: extended
    `WorkerOcServiceImplTest.NewRecoveryGenerationInvalidatesOldCompleteEvidence` so a new recovery generation must
    make the normal object-cache evidence report metadata/ownership not-ready before the new master reconciliation
    completes. The first run failed in 2ms because `pendingReport` still reused old `metadataReady=true` and
    `ownershipReady=true`.
  - GREEN: `ObjectCacheRecoveryState::BeginRecoveryEvidenceGeneration` now clears the latest metadata evidence, and
    `ReconcileLocalIsolationOwnership`/`ReconcileNetworkRecoveryOwnership` begin a new evidence generation before
    scheduling master ownership reconciliation. The adjusted single case passed in 2ms.
  - GREEN: object-cache focused evidence/resource regression passed 9/9 tests in 6ms, including
    `ObjectCacheRecoveryStateTest.*`, the new stale-generation check, metadata/slot readiness checks, and stale resource
    recovery publication checks. Two legacy expectations were updated from `slot_incidents_ready=0/0` to the current
    `slot_recovery_disabled` evidence detail.
  - GREEN: `git diff --check` was clean; `git clang-format --diff HEAD --` on the touched object-cache implementation
    and test files reported no formatting changes; boundary script passed 28/28 in 0.082s.
  - GREEN: remote Bazel 7.4.1 `//tests/ut/worker:worker_oc_service_impl_test` with
    `--test_filter=WorkerOcServiceImplTest.NewRecoveryGenerationInvalidatesOldCompleteEvidence` passed. Total Bazel
    elapsed time was 227.400s with 1539 actions and test runtime 1.5s; the run warned that `--cxxopt` changed and
    discarded analysis cache, so this was cache churn rather than third-party distdir download.
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
