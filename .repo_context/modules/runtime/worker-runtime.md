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
  - restart global-reference reconciliation is disabled by default in the Worker flag and dscli configuration because
    pure KVClient deployments do not maintain global references. ObjectClient deployments that use global references
    must explicitly enable `enable_reconciliation`; the Kubernetes Helm defaults continue to enable it for backward
    compatibility.
  - `worker_oc_server.cpp` assembles object-cache and stream-cache related worker-side services and declares many worker runtime flags.
  - topology Snapshot handling retains FAILED addresses for RPC-stub cleanup. When the same address later appears as
    ACTIVE, `WorkerOCServer` publishes the existing `RemoveDeadWorkerEvent` so the local metadata service removes the
    stale fault-worker entry.
  - async Worker notification cleanup commits RocksDB/ETCD removal or update before applying the matching in-memory
    epoch mutation. A persistence failure keeps the in-memory entry so the caller can retry without losing recovery state.
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
- Both CMake and Bazel link the Worker executable to an unprefixed shared jemalloc, so ordinary process-heap
  `malloc`/`free` and C++ allocation use jemalloc by default. DataSystem shared-memory arenas separately retain the
  `datasystem_`-prefixed static jemalloc. The shared runtime is packaged beside the Worker under `service/lib`.
  Profiling remains opt-in: CMake and Bazel `-x on` build the process allocator with jemalloc profiling support while
  preserving the same Worker link and package layout.

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
  - initialize logs and worker flags; the Git commit/branch and non-default flag snapshot are emitted immediately after
    `Logging::Start()` and before `WorkerOCServer` construction
  - pre-initialize RocksDB storage
  - set up runtime services and signal handling
  - `DataWorker` selects the coordination backend before constructing `WorkerOCServer`: parameterized startup always passes its required injected Discovery and therefore selects Coordinator mode, while command-line and embedded static startup wrap a non-empty `coordinator_address` in internal `StaticCoordinatorDiscovery` or pass null for ETCD/metastore.
  - Coordinator routing preserves the last application-level status from a valid Coordinator response when the shared
    routing budget or discovery refresh later expires. Transport/deadline status remains authoritative only when no
    Coordinator response was observed, so a recovering control plane is not misreported as network unreachability.
  - in `FLAGS_use_brpc` mode, `WorkerOCServer::InitRpcAndMemoryRuntime` calls
    `BrpcChannelFactory::EnsureGlobalInitialized()` immediately after configuring the brpc
    listen address and before any allocator/topology work. brpc::Channel::Init runs
    brpc::GlobalInitializeOrDie (pthread_once-guarded) on the first call; without pre-warm
    that first call happens inside `ConstructTopologyRuntime` via
    `CoordinatorServiceProxyBase::Range -> RpcStubCacheMgr::GetStub -> CreateBrpcChannel`,
    which would spawn the bthread worker pool inside a multi-threaded context. Pre-warming
    isolates brpc's once-init to a quiet single-threaded point. The same init path is also
    triggered idempotently at the top of `BrpcChannelFactory::Create` so client-SDK, test,
    and dsbench entrypoints also benefit. Under TSAN the brpc-internal init race in
    `bthread::TaskGroup::ready_to_run_remote` is additionally silenced by
    `//tools/tsan:default_suppressions`; see `modules/quality/build-test-debug.md`.
    `brpc_event_dispatcher_num` (or
    `DATASYSTEM_BRPC_EVENT_DISPATCHER_NUM`) can override brpc's global
    `event_dispatcher_num` before this one-shot initialization; zero preserves the upstream
    default and a positive value hashes socket I/O processing by fd across dispatchers; it does
    not parallelize accept on a single listening fd. The worker
    setting is exposed by `cli/deploy/conf/worker_config.json`, the DaemonSet Helm value
    `global.rpc.brpc.eventDispatcherNum`, and the Deployment image's `worker.config`; changing
    it requires a worker restart because brpc consumes it during process-wide one-shot init.
  - `WorkerOCServer::Init()` constructs and explicitly initializes a discovery-backed Coordinator proxy from the injected provider, or selects ETCD/metastore, then configures `TopologyEngine::Builder`. Coordinator proxy `Init` requires a non-empty provider result, caches only `front()`, and ignores the remaining candidates. All subsequent RPCs use that cached address once. Changing the provider output or selected endpoint requires rebuilding the runtime object or restarting the Worker, while multi-node Coordinator availability remains the responsibility of the Coordinator Raft layer. The Engine creates and owns both role backends, the hash algorithm, Worker runtime, Controller runtime, Janitor, and optional recovery reporter. Worker code does not assemble or retain those concrete components. Callback targets are initialized before
    `TopologyEngine::Start()`, so callbacks cannot run against partially constructed services. A missing initial topology
    keeps Engine `NOT_READY` while the co-located Controller establishes authority. The Worker publishes READY only after
    the first membership lease succeeds. Object-cache health remains closed until both startup reconciliation and
    committed topology placement are ready. `TopologyEngine::CheckLocalServingReady` checks availability, committed
    local membership, and placement against one immutable Snapshot, then rechecks availability; `ROLE_ISOLATED` and
    stopping states therefore cannot publish or retain startup health from a last-good Snapshot. Serving availability
    is publicly committed before its admission callback, while non-serving callbacks retain fail-close ordering. The
    Engine availability callback closes admission only for non-serving states and posts a no-I/O request to a Worker-owned
    coordinator. A serving notification does not transiently revoke an already published health state; when recovering
    from isolation, the previously closed admission stays closed until the coordinator serializes health-file changes,
    performs a fresh validation, and successfully republishes the marker. The coordinator continues low-frequency
    single-Snapshot validation after startup, so local membership or placement loss also revokes health even without
    another availability-level transition. The health marker is written by
    atomic temporary-file replacement; revocation directly unlinks it and treats only `ENOENT` as idempotent success.
    The startup poll has no fixed topology deadline, so a slow ScaleOut keeps the joining process alive and externally
    unready instead of failing startup. Before each health refresh, that poll also reevaluates the restart-reconciliation
    give-up deadline; expiry can therefore open only the reconciliation gate even when no new topology snapshot arrives,
    while committed membership and placement checks still control health publication. `ReadinessProbe`
    owns the single service-health wait even when no ready-check path is configured; brpc mode skips only the loopback
    RPC after health opens. SIGTERM ends that wait successfully so shutdown does not turn an in-progress startup into a
    process failure. The ready-check file is written only
    after committed membership, a placement probe, and Worker RPC health all succeed. No Worker-local topology
    authority is persisted.
  - Coordinator-mode witness probing uses three fixed Worker-owned loops, a bounded FIFO event queue, and an in-flight
    count. Every watch PUT contributes one independent single-target request; later target events do not cancel earlier
    work. Shutdown first rejects new probe events and clears pending work, then closes Engine watch ingress, waits for
    in-flight probe/report work to drain, and finally destroys the joined probe pool. A finite-deadline failure retains
    the Engine, pool, proxy, and callback dependencies so the same shutdown sequence can be retried safely.
  - Object-cache topology migration feeds the result of each actual remote `MigrateMetadata` RPC into the existing
    Coordinator peer-failure summary observer. Local preflight, cancellation, signing, and metadata-application errors
    are not transport observations. The first voluntary-exit request clears serving-phase peer-failure evidence; normal
    business observers remain closed after exit intent, while topology-owned outgoing migration may build fresh
    evidence as an `EXITING`/`LEAVING` reporter. The migration observer is cleared and drained before the master service
    can outlive the borrowed `TopologyEngine`.
  - when `enable_urma=true`, URMA connection warmup runs after object-cache startup/restart handling and before
    `ReadinessProbe()`: it synchronously prepares the local warmup object, then starts best-effort async peer warmup
    without delaying readiness
  - URMA warmup treats `K_WORKER_PULL_OBJECT_NOT_FOUND` for internal `_urma_` URMA warmup requests as a silent
    best-effort miss because ready-state discovery can race peer warmup-object creation; ordinary RemoteGet failures
    still use normal error logs
  - Worker Get and worker-to-worker RemoteGet URMA writeback failures return `ProviderUbFailureDetailPb` containing the
    provider Worker identity, failed write endpoint, and available raw provider/CQE status. Consumers only apply hard
    isolation when the detail identifies the actual responding Worker; RPC deadline/unavailable outcomes remain
    `SUSPECT` and do not close read-source admission, including while their active verification probe is in flight.
    A verification failure without authoritative CQE status `4` or `9` remains `SUSPECT` under bounded backoff; hard
    failure recovery stays fail-closed. Batch and oversized Get paths preserve the matching structured detail,
    including a failed object in an otherwise partially successful NotifyRemoteGet migration batch.
    Worker-to-Client and Worker-to-Worker writeback Events also carry a weak `PeerUbAdmission` owner and an operation
    token. A retained Event receiving a late status-4 CQE updates the writing Worker's self state; successful self
    recovery advances a separate generation so an older Event cannot quarantine the recovered sender. Regular and
    aggregate RemoteGet writes use the same ownership path without retaining their request payloads.
  - Foreground Worker-to-master `QueryMeta` retries transient retry-policy statuses within the caller deadline but
    treats `K_RPC_PEER_DEAD` as terminal. A cross-Worker Get therefore returns the dead metadata owner's status
    immediately instead of rebuilding the same dead peer's RPC stub until the configured request timeout expires.
    Background reconciliation and metadata push paths retain their bounded stub-rebuild retry because those operations
    are idempotent and must tolerate a restarting metadata owner.
  - Worker heartbeat and the UB-health membership sidecar publish exactly one self record. The service overwrites both
    Worker identity and incarnation at the publication boundary, so peer observations cannot leak into a self summary.
    Consumers reject stale epochs, retired incarnations, and summaries whose incarnation does not match the registered
    Worker. Accepting a trusted new incarnation clears process-local evidence for that endpoint so the restarted Worker
    can be readmitted. Lease expiry alone removes only the global quarantine and retains process-local evidence.
  - Multi-Worker URMA startup begins non-blocking local-sender verification only after the local topology member is
    `ACTIVE`; `INITIAL` and `JOINING` do not create UB failure evidence, and a single Worker skips this peer-dependent
    verification. Startup verification stays `SUSPECT` and writable unless an authoritative CQE status `4` or `9`
    identifies a hard failure. The long-lived URMA warmup controller serializes dedicated one-byte recovery writes
    outside admission locks. Trusted provider-local ERROR 4 remains a Local Observation for the Worker
    itself; the Worker's own lease-published summary is treated as an echo rather than an external recovery fence, so a
    successful self probe can publish the next writable summary instead of deadlocking behind its previous deny fact.
  - `WorkerWorkerTransportService.ProbeProviderUbRecovery` is the heartbeat-independent Client recovery control path.
    It returns the current self summary with the topology membership id as incarnation, rejects a stale expected
    incarnation, and skips data-plane work while the self summary is non-writable. Once writable, it imports the
    requesting Client's Jetty and dedicated recovery segment without serializing an unused reverse segment table, then
    issues one one-byte Worker-to-Client UB WRITE; it never substitutes a business response as recovery evidence.
  - object-cache worker-to-master RPC warmup also starts before `ReadinessProbe()`: a best-effort asynchronous startup
    task reads immutable topology snapshots until the ready member set is stable or the startup warmup window expires;
    later Snapshot publication callbacks enqueue bounded warmup for newly ready members
- Steady state:
  - accept/register clients
  - manage shared-memory or socket-based FD passing for client-worker IPC
  - serve object-cache and stream-cache requests
  - tick perf manager, drive lightweight metrics summary emission, and monitor config changes
  - after either ETCD or Coordinator becomes unreachable, a Worker with a last-good committed Snapshot keeps business
    admission open in `CONTROL_DEGRADED` while periodic authoritative exact reads continue. The Engine reuses the
    bounded Worker-to-Worker control-backend probe against every committed peer: a peer quorum reporting the same fresh
    `UNAVAILABLE` authority stamp confirms a cluster-wide backend outage, while any matching `AVAILABLE` report confirms
    the local Worker's backend path is isolated only after three consecutive probe rounds. Peer transport failures alone
    remain inconclusive to avoid correlated all-Worker isolation. Confirmed local isolation closes admission and arms
    the full node-dead timeout before `SIGKILL`; an authoritative exact read clears the confirmation/death state and is
    the only path back to `NORMAL`. A Coordinator keepalive that explicitly observes a stale or missing membership key
    queues one forced same-Leader Ensure; concurrent in-flight completion cannot consume that signal, while repeated
    pending signals coalesce on the existing single-thread ensure loop.
  - memory-rebalance scheduling cross-checks ResourceManager candidates against one current immutable topology
    Snapshot and assigns only `ACTIVE` sources and targets. Before the first Snapshot is available, it preserves the
    legacy resource-readiness fallback instead of blocking scheduling. Failed workers report structured failure
    attribution: source/no-candidate failures cool the source, target failures cool the target, control-plane failures
    cool neither, and unknown/mixed-version reports cool only the attempted source-target pair. Cooldowns expire only
    by their steady-clock TTL and are not erased by the affected Worker's next resource report.
  - NodeSelector passes the exact master address that returned each rebalance task to RebalanceExecutor. Before every
    bounded migration batch, the executor expires the task when that assigned master is `FAILED`, locally unreachable,
    or absent from the current topology; a successor master reconstructs scheduling from later resource reports rather
    than accepting completion for the predecessor's in-memory task.
  - Object migration combines topology role and UB admission. Ordinary sources and every new target must be `ACTIVE`;
    topology ScaleIn sources may remain `ACTIVE/PRE_LEAVING/LEAVING`. An incoming request already sent to a target is
    admitted while that target is `ACTIVE/PRE_LEAVING`; `JOINING/LEAVING/FAILED` reject it. Sender connection creation
    and per-batch admission, redirect, rebalance, recovery, and primary selection continue to require `ACTIVE`.
    Rebalance rechecks before candidate selection and every bounded batch.
    FastMigration read failures preserve target-local raw CQE evidence; NotifyRemoteGet write failures preserve the
    source Provider operator. Every migration batch rechecks both source and target admission, so a source failure
    stops following batches instead of being misattributed to a target. Recovery
    probes use a manager-owned, independently registered 4 KiB source segment; Worker startup does not initialize the
    client `UB_TRANSPORT` allocation pool solely for the one-byte probe.
  - Worker-to-worker migration responses distinguish success, failed, and skipped object keys as three mutually
    exclusive sets. `skipped_object_keys` (in `MigrateDataRspPb`, `MigrateDataDirectRspPb`, and `NotifyRemoteGetRspPb`)
    carries objects the target could not process because master metadata was not found (deleted concurrently, or
    meta-is-moving). The source `MigrateDataHandler` inserts `skipKeys` into `skipIds_` and excludes them from
    `successIds` before calling `ReleaseResources`, so skipped objects are never released at the source. Three migration
    paths route metadata-not-found to skipped: TCP `FillObjectsLocked`, URMA read `FillMetaToObjectEntries`, and URMA
    write `NotifyRemoteGet` entry plus `ReportUnattemptedObjects` catch-all. `needModifyPrimary` objects (equal-version
    copies already on target) are included in the metadata query set so that the skip check only fires when master
    genuinely has no metadata, not merely because the key was not queried. For SPILL migrations via `NotifyRemoteGet`,
    `is_spill` gates `ReplacePrimary`/`CreateMultiCopyMeta`/`ClearNeedDelete`: when `is_spill=true`, the target skips
    `CreateMultiCopyMeta`, executes `ReplacePrimary`, and clears `needDelete`, so the source's primary pointer transfers
    to the target before the source releases data. `RebalanceExecutor` treats skipped objects as non-failure
    (`failedObjects` excludes skipIds) and, when a batch is all-skip after partial success, retries candidate selection
    with a task-local `taskSkippedKeys` set so `SelectCandidates` scans past previously-skipped objects to find valid
    candidates behind them. `skippedObjects` is reported in task completion logs for observability.
  - Published full topology snapshots drive UB-state lifecycle reconciliation. Explicitly removed Worker addresses
    enter a `2 * node_timeout_s` grace period, after which local/global/incarnation buckets are removed and a bounded
    TTL tombstone rejects old incarnation replay. The warmup background loop prunes expired tombstones even when no
    later topology snapshot is published. Lease-empty UB snapshots still clear only Global Fact. Client routing
    filters immediately remove trusted/global/local-observation buckets for addresses absent from authoritative routing
    topology.
- Shutdown:
  - Parameterized lifecycle callbacks run outside `initMutex_`; once `onStart` is attempted, cleanup invokes `onStop` exactly once. The first lifecycle error is returned while later cleanup errors are logged, and internal shutdown always continues.
  - `PreShutDown` then `ShutDown`
  - `DataWorker` relinquishes its `WorkerOCServer` owner before returning from `ShutDown`, including error paths. The
    server and topology component destructors provide the final safe-stop/join fallback, so runtime-owned objects are
    destroyed before later-created function-local singleton dependencies begin static teardown.
  - voluntary ScaleIn records exit intent, immediately returns `K_SCALE_DOWN` from RPC health and, when the legacy
    leaving intercept is enabled, client-facing writes. With that intercept disabled, client writes remain admitted
    until the authoritative topology data drain begins; a dedicated process-local drain flag then fences writes even
    while the drain waits for already-admitted incoming migrations. Incoming migration admission closure is deliberately
    not reused as that write fence because failure/rejoin cleanup also closes the gate and must be able to resume ordinary
    traffic after reconciliation. Voluntary ScaleIn waits for clients and
    asynchronous tasks without marking the process health file unhealthy, then publishes EXITING; final process-health
    termination happens after topology removal or the bounded removal attempt. The Worker keeps its Engine,
    callback executor, and lease
    alive until a current immutable snapshot no longer contains the local member; only then may process shutdown begin.
    This preserves the source for the whole ScaleIn task barrier. The external process manager remains the bounded final
    termination authority when the control plane cannot complete the transition.
  - if a restarted Worker reads its local member as `LEAVING`, it claims the process-local exit fence before fallible
    helper-thread startup, keeps READY closed, and retries creation of only a missing pre-shutdown worker after a partial
    failure; the startup exact-snapshot pass propagates a persistent startup failure. A lazy Worker-owned thread, rather
    than the Engine Snapshot callback, republishes EXITING with capped exponential backoff and per-Worker jitter. Removal
    terminally cancels that publisher, prevents an older concurrent `LEAVING` callback from re-arming it, and requests
    process shutdown once. An initial publication that races synchronous Snapshot delivery during Engine startup is
    woken immediately by the Engine-RUNNING transition instead of waiting for backend-failure backoff. The publisher
    keeps retrying until authoritative removal or process shutdown; an independent
    give-up budget would recreate a permanent `LEAVING` state. This recovery is address-local and therefore independent
    for every member in a multi-Worker ScaleIn batch; it deliberately does not replay `PRE_LEAVING`.
  - the graceful-exit topology retries (`PublishExitingMembership` -> `TopologyEngine::MarkExiting`, and
    `WaitForTopologyRemoval` -> poll until the local member is gone from the authoritative snapshot) are bounded by the
    worker-owned constexpr `LOSSLESS_EXIT_GRACE` (worker_oc_server.cpp; 120s in production, 10s under `WITH_TESTS` so
    the lossless-exit ST case finishes in tens of seconds) through the
    `RetryUntilSuccessDuringGracefulExit(func, grace)` overload in `common/util/rpc_util.h`; on grace expiry it logs
    "Graceful-exit retry gave up before success" once and returns the last error. `PreShutDown` runs both topology
    steps best-effort (`LOG_IF_ERROR`, never early-return) and always reaches `RemoveWriteBackIdsLocation` cleanup
    before returning the topology status. `WaitClientsExit` still uses the unbounded overload because its own health
    check breaks the loop.
  - after authoritative topology removal, an empty membership snapshot marks full-cluster shutdown: the Worker
    immediately tells object metadata to discard TTL work, and later shutdown stops the TTL producer before draining its
    shared async pool. For partial removal, TTL work continues only while local metadata exists and its version does not
    postdate the task's original expiration timestamp; retries preserve that original timestamp, and migrated or
    recreated keys are pruned before each notification retry and before retry requeue. An already-running worker RPC is
    not force-cancelled.
  - ordinary metadata mutations remain rejected after local ScaleIn exit intent, but the fenced callback
    propagates its non-empty `businessOperationId` as `RemoveMetaReqPb.topology_operation_id`. Metadata owners use that
    marker only to allow the callback's own idempotent remove/give-up-primary effects. The data phase, final source
    cleanup, and redirect retries must all preserve the same marker; ordinary requests leave the field unset.
  - when `enable_leaving_intercept` is enabled, the object-cache `Create`, `Publish`, `MultiCreate`, and `MultiPublish`
    RPC entrypoints read the process-local ScaleIn exit intent and return `K_SCALE_DOWN` before entering their write
    processors. This client-facing gate is deliberately separate from incoming migration admission: a `PRE_LEAVING`
    Worker stops new client writes while it can still finish an already-selected ScaleIn target task. Read-only RPCs
    keep their existing behavior. Disabling the flag preserves the legacy write path only before data drain starts;
    once incoming migration admission closes, all four write entrypoints reject requests unconditionally so no write can
    arrive after the local drain snapshot.
  - metadata ownership task ranges do not describe where object data is physically resident. ScaleIn therefore drains
    the leaving Worker's complete local object table once per source/batch before task-scoped metadata migration. The
    Worker callback adapter coalesces concurrent disjoint tasks behind a deadline-aware process-local gate; metadata
    migration and prepared cleanup remain constrained by each task's `IKeyFilter`.
  - topology-task ScaleIn senders accept only `ACTIVE` destinations, while receivers tolerate `PRE_LEAVING` for
    already-selected or in-flight socket/direct/NotifyRemoteGet work. At the start of its own ScaleIn data callback, a
    source atomically closes incoming migration admission and waits within the callback deadline for every admitted
    request to finish before selecting its local drain snapshot. Subsequent requests are rejected; timeout keeps the
    gate closed and returns an explicit error. Concurrent leavers cannot exchange objects after either member takes its
    drain snapshot.
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
  - `WorkerOCService.QueryAndGet` is the metadata-affine direct-read entrypoint. It returns resident local objects over
    SHM, UB, or bounded TCP payloads and uses routed `PureQueryMeta` only for local misses, so metadata redirect handling
    stays inside Worker without changing `QueryMeta` semantics. The service handler owns only Worker-state validation
    and delegates request reading, authentication, response delivery, SHM rollback, access recording, and slow logging
    to `WorkerQueryAndGetImpl`. The local probe must remain side-effect free: no remote pull,
    subscription, placeholder creation, L2 load, or cache insertion. The response has exactly one positional result per
    requested key; a result without `data_result` delegates data retrieval to the Client's existing replica phase.
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

## Fast Verification

- Build worker and tests:
  - `bash build.sh -t build`
- Run common topology UT after building tests:
  - `cd build && ./bin/ds_ut --gtest_filter=TopologyRepositoryTest.*:ClusterRegistryTest.*:ClusterMembershipTest.*:WorkerDirectoryTest.*:TopologyChangeHandlerTest.*`
- Run focused fault-worker convergence coverage after building `ds_ut` and `ds_ut_object`:
  - `build/tests/ut/ds_ut --gtest_filter=WorkerOCServerTest.*`
  - `build/tests/ut/ds_ut_object --gtest_filter=OCNotifyWorkerManagerTest.TestDeadWorker*:OCNotifyWorkerManagerTest.TestTransientFaultWorker*:OCNotifyWorkerManagerTest.TestFaultWorkerCanOnlyUpgrade*:OCNotifyWorkerManagerTest.TestRemoveDeadWorkerEvent*:OCNotifyWorkerManagerTest.TestClearAsyncWorkerOpKeepsRetryStateOnPersistenceFailure`
- The equivalent Bazel targets are registered, but the current master dependency closure is blocked before test
  execution by the unrelated `tests/st/cluster/external_cluster.cpp` missing-header baseline issue.
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
