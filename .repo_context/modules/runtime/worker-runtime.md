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
  - `src/datasystem/worker/worker.cpp`
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
  - `worker.cpp` owns top-level startup/shutdown orchestration, log initialization, THP handling, RocksDB pre-init, and embedded-worker entrypoints exported through C symbols.
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
  - `Worker::GetInstance()`
- Embedded worker hooks exported from `worker.cpp`:
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
  - `WorkerOCServer::Init()` constructs separate Worker-role and Controller-role coordination backends, then starts
    `cluster::TopologyController`, `cluster::TopologyEngine`, and `cluster::TopologyTaskJanitor` only after callback
    targets are initialized. The callback adapter resolves the object-cache service through a Worker-owned late-bound
    provider because the service itself needs the Engine placement view during construction; callbacks cannot run until
    service initialization and runtime Start complete. A missing initial topology keeps Engine `NOT_READY` while the
    co-located Controller establishes authority instead of failing Worker initialization. It publishes READY only after
    the first membership lease succeeds. `Start()` writes the configured ready-check file only after the local member is
    committed, a placement probe resolves, and the Worker RPC health check succeeds. No Worker-local topology authority
    is persisted.
  - when `enable_urma=true`, URMA connection warmup runs after object-cache startup/restart handling and before
    `ReadinessProbe()`: it synchronously prepares the local warmup object, then starts best-effort async peer warmup
    without delaying readiness
  - URMA warmup treats `K_WORKER_PULL_OBJECT_NOT_FOUND` for internal `_urma_` URMA warmup requests as a silent
    best-effort miss because ready-state discovery can race peer warmup-object creation; ordinary RemoteGet failures
    still use normal error logs
  - object-cache worker-to-master RPC warmup also starts before `ReadinessProbe()`: a best-effort asynchronous startup
    task scans `GetAll(ETCD_CLUSTER_TABLE)` until the discovered ready worker/master set is stable or the startup
    warmup window expires, and later ETCD cluster ready PUT events trigger warmup for newly ready workers/masters
- Steady state:
  - accept/register clients
  - manage shared-memory or socket-based FD passing for client-worker IPC
  - serve object-cache and stream-cache requests
  - tick perf manager, drive lightweight metrics summary emission, and monitor config changes
- Shutdown:
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
  - `WorkerOCServer` stops both event sources, then Janitor, `cluster::TopologyEngine`, and
    `cluster::TopologyController`. Any timeout preserves the full dependency chain for a later retry; coordination
    backends and ETCD stores are torn down only after those owners stop successfully.
  - embedded mode uses exported destroy helpers

## Cluster And Metadata Notes

- Verified:
  - current docs and code support ETCD, Coordinator transport, and Metastore-based metadata paths.
  - `worker_oc_server.cpp` enforces that at least one of `etcd_address` or `metastore_address` is set.
  - `WorkerOCServer` constructs `cluster::HashAlgorithm`, Worker-role `cluster::TopologyEngine`, and independent
    `cluster::TopologyController`/Janitor ownership. ETCD uses separate backend/store instances per role;
    Coordinator transport uses separate `cluster::DsCoordinationBackend` instances.
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
  - Worker-master RPC warmup is one-way per initiating worker. Startup scans warm each worker's outbound paths, and the
    cluster ready event hook in `WorkerOCServer::UpdateClusterInfoInRocksDb` remains necessary for old-node to new-node
    paths during scale-out.
  - topology routing distinguishes a real redirect from a ScaleOut transfer barrier. Structured callers receive
    `moving=true`; legacy boolean callers receive `true` with an empty target address so they defer the operation instead
    of redirecting back to the committed source or mutating metadata while migration is in flight.
- Useful files during debugging:
  - `src/datasystem/worker/worker_main.cpp`
  - `src/datasystem/worker/worker.cpp`
  - `src/datasystem/worker/worker_service_impl.cpp`
  - `src/datasystem/worker/worker_oc_server.cpp`
  - `src/datasystem/cluster/*`

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
