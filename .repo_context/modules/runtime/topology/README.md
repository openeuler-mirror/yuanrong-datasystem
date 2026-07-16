# Cluster Topology Runtime

## Scope

- Status: `active`
- Canonical source roots:
  - `src/datasystem/cluster`
  - `src/datasystem/protos/cluster_topology.proto`
  - `src/datasystem/worker/worker_oc_server.cpp`
  - `src/datasystem/worker/worker_topology_references.{h,cpp}`
- The module owns authoritative cluster membership state, immutable routing snapshots, topology planning, task
  materialization/execution, and the ETCD-backed control loop.
- Phase 1 validates the ETCD authority path. `DsCoordinationBackend` preserves the existing Coordinator transport and
  service-discovery contract, but moving topology control decisions into `datasystem_coordinator` remains later work.

## Current Design Shape

- `datasystem::cluster::TopologyEngine` is the Worker-role runtime root. It owns its repository/reader, role-minimal
  dispatcher, executor, immutable snapshot state, placement facade, and endpoint view; it does not own the Controller,
  Janitor, or standalone Observer.
- Every ETCD Worker may run a `TopologyController`. Controllers contend through the single authoritative topology-key
  CAS; controller identity is not persisted, and deterministic batch/task identities make duplicate reconciliation safe.
- `TopologyRepository` stores one `ClusterTopologyPb` authority record and derived task/notify records. Derived records
  cannot replace topology authority; final progress and batch transitions are fenced by topology version and batch epoch.
- `cluster_topology.proto` owns these topology records under protobuf package `datasystem`; coordinator and other
  unrelated protobuf contracts remain separate schemas.
- `TopologyEngine`, `TopologyController`, and standalone `TopologyObserver` each own one serialized state loop and one
  dedicated backend instance. Worker watches exact topology plus its own notify key; Controller watches topology,
  membership, and derived task/notify directories; Observer watches exact topology only.
- Foreground routing uses `PlacementFacade` and one immutable snapshot per decision. It never performs backend IO and
  never exposes protobuf or raw token ranges to business code. `TopologySnapshotState` uses a publication generation
  plus a thread-local weak cache so unchanged reads avoid repeatedly loading the atomically published shared pointer
  without retaining old 10K-member snapshots on long-lived request threads. During an ordinary batch, the Snapshot
  also derives the post-commit owner ring: ScaleOut transfer ranges wait on the committed source, while a ScaleIn
  source whose metadata handoff has completed redirects missing metadata to the prospective owner.
- Business migration/recovery is invoked through one opaque task callback. `IKeyFilter` and `StorageScanPlan` keep token
  representation internal, while callbacks receive stable operation identity, deadline, and cooperative cancellation.
- Scale-in cleanup preparation only materializes the task scope. The final no-IO authorization runs under the same
  Snapshot publication lock used by topology installation; the bounded idempotent cleanup effect runs on the existing
  callback pool after releasing that lock and retains the original attempt deadline/cancellation. A stale fence
  therefore has no destructive effect. Authorization establishes that the old task was legal before a later Snapshot
  publication; Apply may finish afterward, remains idempotent under the preserved LEAVING/FAILED member fact, and must
  complete before ScaleIn progress allows final member removal. Worker callbacks install their remaining budget in the
  repository `ApiDeadline`; metadata-removal batches cap their thread-local RPC budget by that remaining deadline
  instead of restarting the default RPC timeout for every batch.
- `WorkerOCServer` owns separate Worker-role and Controller-role backends plus `TopologyEngine`, `TopologyController`,
  and `TopologyTaskJanitor`. Shutdown first idempotently stops both event sources, then Janitor, Engine, and Controller;
  each Runtime Stop can also stop its dedicated event source for standalone shutdown and destructor safety. Full backend
  Shutdown runs only after every Runtime is safe. Engine Start is one-shot; Stop records deadline expiry but never
  returns while a thread still captures Engine state. The process manager owns the outer termination bound. Normal
  lifecycle code uses bounded Stop/Shutdown; component destructors safely stop and join as a final fallback and never
  call `std::terminate`, detach a live thread, or kill the process.

## Persistence And Recovery

- Keyspace supports an optional cluster scope. A non-empty validated name uses
  `/datasystem/{cluster_name}/...`; an empty name uses `/datasystem/...` without an empty path segment. Multi-cluster
  deployments sharing one backend must use non-empty distinct names. The five logical paths are topology,
  tasks/migrate, tasks/delete, notify, and cluster membership.
- `TopologyKeyHelper` is the only table/key builder. `EtcdStore::CreateTableWithExactPrefix` registers these paths without
  legacy `FLAGS_cluster_name` prefix rewriting.
- There is no persisted Worker-local topology authority. ETCD restart recovery reads the latest legal topology and
  reconstructs deterministic work. A future in-memory coordinator backend recovers only the latest topology from a
  Worker; task/notify records are treated as absent and regenerated.
- Task cleanup first CASes the exact task value to a repository-internal deletion tombstone, then performs physical
  deletion. The tombstone is never exposed as a task and temporarily fences same-ID rematerialization, closing the
  conditional-cleanup/delete race without extending `ICoordinationBackend`.
- Failure metadata recovery is at-least-once, idempotent, and best effort. Normal recovery failure is not retried; a
  coordinator crash before final topology CAS may repeat it.
- Scale-out and scale-in callbacks use bounded retries. Exhausted scale-out removes the joining member so it can restart
  and re-enter as `INITIAL`; exhausted scale-in proceeds through external bounded termination and Failure handling.
  Object and stream callbacks treat per-item migration failures as retryable task failures, so a successful RPC status
  alone cannot advance the batch while selected metadata is still missing at the target.
- Failure preempts an ordinary batch by fencing its old execution round, preserving `JOINING`/`LEAVING` facts, completing
  the Failure batch first, and replanning ordinary work from the latest topology.

## Key Entry Points

- Runtime: `src/datasystem/cluster/runtime/topology_engine.{h,cpp}`
- Control: `src/datasystem/cluster/control/topology_controller.{h,cpp}`
- Execution: `src/datasystem/cluster/executor/topology_task_executor.{h,cpp}`
- Persistence: `src/datasystem/cluster/repository/topology_repository.{h,cpp}`
- Routing: `src/datasystem/cluster/routing/placement_facade.{h,cpp}`
- Backend: `src/datasystem/cluster/coordination_backend/etcd_coordination_backend.{h,cpp}`
- Existing Coordinator transport: `src/datasystem/cluster/coordination_backend/ds_coordination_backend.{h,cpp}`
- Worker composition: `src/datasystem/worker/worker_oc_server.cpp`
- Standalone observer consumer: `src/datasystem/client/router_client.cpp`

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
- Background reconciliation must remain resource bounded and must always converge to a state that permits a later batch.

## Tests

- Main contract/component binary: `cluster_topology_contract_ut`.
- Core CTest selection:
  - `ctest -R 'ClusterTopology|TopologyRepository|TopologyObserver|PlacementFacade'`
  - `ctest -R 'TopologyController|TopologyTaskExecutor|TopologyEngine|TopologyDfx|TopologyShutdown'`
- Business adapter coverage lives in `ds_ut_object`, `ds_ut_stream`, and selected Worker/object/stream ST binaries.
- State machine, CAS/fence, crash points, retry, resource limits, and Shutdown belong in UT/LLT/component tests. ST only
  proves representative process, ETCD watch/lease, network, and real callback wiring.

## Update Triggers

- Update this module when cluster topology schema, keyspace/watch scope, routing semantics, callback contract,
  controller/executor recovery, Worker ownership, or shutdown ordering changes.
- Update the quality context when test binary ownership, labels, or remote validation commands change.
