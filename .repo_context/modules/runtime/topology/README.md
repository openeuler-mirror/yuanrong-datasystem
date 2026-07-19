# Cluster Topology Runtime

## Scope

- Status: `active`
- Canonical source roots:
  - `src/datasystem/cluster`
  - `src/datasystem/protos/cluster_topology.proto`
  - `src/datasystem/worker/worker_oc_server.cpp`
  - `src/datasystem/worker/metadata_route_resolver.{h,cpp}`
- The module owns authoritative cluster membership state, immutable routing snapshots, topology planning, task
  materialization/execution, and the ETCD-backed control loop.
- `DsCoordinationBackend` preserves the topology architecture while using the in-memory Coordinator transport. A
  restarted Coordinator fences its new lifetime with `CoordinatorId`, gates topology/task/notify access, accepts
  Worker-reported last-good topology candidates, installs one canonical highest version, and regenerates derived work.
  Moving topology control decisions into `datasystem_coordinator` remains later work.

## Current Design Shape

- `datasystem::cluster::TopologyEngine` is the Worker-role composition root. Its nested Builder is the sole Worker
  construction path. Engine creates and owns both role backends, the shared algorithm, Worker repository/reader,
  dispatcher, executor, immutable snapshot state, placement facade, endpoint view, `TopologyControllerRuntime`,
  Janitor, and optional Coordinator recovery reporter. It does not own standalone Observer.
- Every ETCD Worker may run a `TopologyController`. Controllers contend through the single authoritative topology-key
  CAS; controller identity is not persisted, and deterministic batch/task identities make duplicate reconciliation safe.
- `TopologyRepository` stores one `ClusterTopologyPb` authority record plus derived task/notify records and ScaleIn
  metadata-done markers. Derived records cannot replace topology authority; final progress and batch transitions are
  fenced by topology version and batch epoch.
- `cluster_topology.proto` owns these topology records under protobuf package `datasystem`; coordinator and other
  unrelated protobuf contracts remain separate schemas.
- `TopologyEngine`, `TopologyController`, and standalone `TopologyObserver` each own one serialized state loop. ETCD
  Workers use the Worker-owned `EtcdStore` and one unified watch stream for exact topology/local notify plus membership
  and derived task directories; Engine routes physical-key doorbells to the Worker and Controller dispatchers, and a
  topology event wakes both. Coordinator keeps role-specific backend watch registrations. Observer watches exact
  topology only.
- Foreground routing uses `PlacementFacade` and one immutable snapshot per single-key or batch decision. Batch-level
  failures leave the output unchanged; one item vector returns each per-key status beside its decision so successful
  results from the same snapshot survive without an extra aligned-vector allocation. Routing never performs backend IO
  or exposes protobuf/raw token ranges to business code.
  `TopologySnapshotState` uses a publication
  generation
  plus a thread-local weak cache so unchanged reads avoid repeatedly loading the atomically published shared pointer
  without retaining old 10K-member snapshots on long-lived request threads. During an ordinary batch, the Snapshot
  also derives the post-commit owner ring: ScaleOut transfer ranges wait on the committed source, while a ScaleIn
  source whose metadata handoff has completed redirects missing metadata to the prospective owner.
- Business migration/recovery is invoked through one opaque task callback. `IKeyFilter` and `StorageScanPlan` keep token
  representation internal, while callbacks receive stable operation identity, deadline, and cooperative cancellation.
- Scale-in execution is metadata-first. The first callback migrates source metadata and writes one metadata-done marker
  per task under the source/batch gate; only after all expected source markers exist does the executor re-enter the task
  to drain Worker-local data and prepare cleanup. Scale-in cleanup preparation only materializes the task scope. The
  final no-IO authorization runs under the same Snapshot publication lock used by topology installation; the bounded
  idempotent cleanup effect runs on the existing callback pool after releasing that lock and retains the original attempt
  deadline/cancellation. A stale fence therefore has no destructive effect. Authorization establishes that the old task
  was legal before a later Snapshot publication; Apply may finish afterward, remains idempotent under the preserved
  LEAVING/FAILED member fact, and must complete before ScaleIn progress allows final member removal. Worker callbacks
  install their remaining budget in the repository `ApiDeadline`; metadata-removal batches cap their thread-local RPC
  budget by that remaining deadline instead of restarting the default RPC timeout for every batch.
- `WorkerOCServer` owns only the `TopologyEngine` composition root plus the Store/Proxy and callback resources borrowed
  by it. Shutdown drains business RPC ingress and calls Engine once. In ETCD mode Engine closes the shared watch and
  keepalive event sources once, drains Worker execution, stops the externally-fed Controller/Janitor, and fully shuts
  down the Store once. Coordinator mode unbinds ingress and preserves role-specific event-source shutdown. A deadline
  failure preserves Engine and every borrowed dependency for retry. Engine Start is one-shot;
  component destructors safely stop and join as a final fallback and never call `std::terminate`, detach a live thread,
  or kill the process. The process manager owns the outer hard termination bound.
- Coordinator exposes `GetClusterRawSnapshot` as a cold, read-only diagnostic RPC. The handler validates a logical
  cluster name, reads the exact topology key and membership prefix through the existing `CoordinatorStore::Range`, and
  returns only raw KV facts. It bypasses the ordinary recovery read gate, but never decodes topology, derives
  health/ranges/routes, retries, or mutates state. Those operations belong to the dscli-local `client/cluster_query`
  layer.

## Persistence And Recovery

- Keyspace supports an optional cluster scope. A non-empty validated name uses
  `/datasystem/{cluster_name}/...`; an empty name uses `/datasystem/...` without an empty path segment. Multi-cluster
  deployments sharing one backend must use non-empty distinct names. The five logical paths are topology,
  tasks/migrate, tasks/delete, notify, cluster membership, and ScaleIn metadata-done markers.
- `TopologyKeyHelper` is the only topology keyspace builder. It owns logical tables, the legacy-compatible ETCD
  membership prefix, and allocation-free raw ETCD watch-key classification. `EtcdStore::CreateTableWithExactPrefix`
  registers these paths without legacy `FLAGS_cluster_name` prefix rewriting. `TopologyEngine::Builder` owns
  registration for the shared ETCD Store; Worker business composition does not construct topology keys or table
  mappings. `TopologyEngine` maps classified key kinds to Worker/Controller delivery policy.
- There is no persisted Worker-local topology authority. ETCD restart recovery reads the latest legal topology and
  reconstructs deterministic work. The in-memory Coordinator backend recovers only the latest topology from Workers;
  task/notify records are treated as absent and regenerated. Candidate arbitration is cluster-scoped and resource
  bounded; conflicting same-version digests block only that cluster until membership/evidence changes.
- Coordinator watches bind both `CoordinatorId` and `watch_id`. Watch registration uses a client registration ID so an
  ambiguous WatchRange result retries idempotently. Initial/recreated membership invalidates both Worker and Controller
  role plans using O(1) RESET doorbells; lease threads never wait for watch-registration RPCs.
- Task cleanup first CASes the exact task value to a repository-internal deletion tombstone, then performs physical
  deletion. The tombstone is never exposed as a task and temporarily fences same-ID rematerialization, closing the
  conditional-cleanup/delete race without extending `ICoordinationBackend`. The same janitor pass also removes stale
  ScaleIn metadata-done markers whose epoch is older than the active batch, or no newer than the final topology version
  when no batch is active.
- Failure metadata recovery is at-least-once, idempotent, and best effort. Normal recovery failure is not retried; a
  coordinator crash before final topology CAS may repeat it.
- Scale-out and scale-in callbacks use bounded retries. Exhausted scale-out removes the joining member so it can restart
  and re-enter as `INITIAL`; exhausted scale-in proceeds through external bounded termination and Failure handling.
  Object and stream callbacks treat per-item migration failures as retryable task failures, so a successful RPC status
  alone cannot advance the batch while selected metadata is still missing at the target. A completed failed attempt
  also clears its source-side migrating marker after restoring source state, allowing the next bounded retry to serve
  the metadata from the source instead of reporting a stale moving state.
- Failure preempts an ordinary batch by fencing its old execution round, preserving `JOINING`/`LEAVING` facts, completing
  the Failure batch first, and replanning ordinary work from the latest topology.

## Key Entry Points

- Runtime: `src/datasystem/cluster/runtime/topology_engine.{h,cpp}`
- Controller composition: `src/datasystem/cluster/control/topology_controller_runtime.{h,cpp}`
- Control: `src/datasystem/cluster/control/topology_controller.{h,cpp}`
- Execution: `src/datasystem/cluster/executor/topology_task_executor.{h,cpp}`
- Persistence: `src/datasystem/cluster/repository/topology_repository.{h,cpp}`
- Routing: `src/datasystem/cluster/routing/placement_facade.{h,cpp}`
- Backend: `src/datasystem/cluster/coordination_backend/etcd_coordination_backend.{h,cpp}`
- Existing Coordinator transport: `src/datasystem/cluster/coordination_backend/ds_coordination_backend.{h,cpp}`
- Worker composition: `src/datasystem/worker/worker_oc_server.cpp`
- Worker metadata routing adapter: `src/datasystem/worker/metadata_route_resolver.{h,cpp}`
- Standalone observer consumer: `src/datasystem/client/router_client.cpp`
- Read-only operator query: `src/datasystem/client/cluster_query/*` and
  `src/datasystem/coordinator/coordinator_service_impl.cpp`

## Invariants And Risks

- Do not reintroduce the deleted legacy topology module/schema, legacy ring keys, dual-read/write, fallback parsing, or
  local snapshot authority.
- Normal scale-out/scale-in must keep business traffic lossless. Data or metadata loss is accepted only after confirmed
  member Failure.
- Temporary endpoint observations stay process local. Only confirmed Failure enters the authoritative topology.
- At most one change type is active at a time; one batch may contain many members. Failure has highest priority and may
  preempt ordinary work. Scale-in waits for an already-running scale-out batch to finish.
- All callbacks must be deadline-aware, cooperatively cancellable, idempotent by operation ID, and safe under duplicate
  delivery. Process termination is supplied by Kubernetes or the process manager after bounded drain.
- Worker task notifications are derived, idempotent records. A notification observed after its active batch finalizes,
  or while a different batch is authoritative, is a stale no-op rather than a runtime failure.
- Background reconciliation must remain resource bounded and must always converge to a state that permits a later batch.

## Tests

- Main contract/component binary: `cluster_topology_contract_ut`.
- Core CTest selection:
  - `ctest -R 'ClusterTopology|TopologyRepository|TopologyObserver|PlacementFacade'`
  - `ctest -R 'TopologyController|TopologyTaskExecutor|TopologyEngine|TopologyDfx|TopologyShutdown'`
- Business adapter coverage lives in `ds_ut_object`, `ds_ut_stream`, and selected Worker/object/stream ST binaries.
- Operator-query coverage includes `CoordinatorStoreTest` raw RPC cases, `ClusterQueryProjectorTest`, Python
  `test_cli_query.py`, and a packaged-wheel real-backend smoke test.
- State machine, CAS/fence, crash points, retry, resource limits, and Shutdown belong in UT/LLT/component tests. ST only
  proves representative process, ETCD watch/lease, network, and real callback wiring.

## Update Triggers

- Update this module when cluster topology schema, keyspace/watch scope, routing semantics, callback contract,
  controller/executor recovery, Worker ownership, or shutdown ordering changes.
- Update the quality context when test binary ownership, labels, or remote validation commands change.
