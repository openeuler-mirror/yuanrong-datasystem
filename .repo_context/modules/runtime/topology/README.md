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
  Workers use the Worker-owned `EtcdStore` and one unified watch stream for exact topology/local notify plus membership;
  Engine routes physical-key events by role. Controller validates and applies ETCD topology/membership values to its
  state-thread-owned fact cache; Worker keeps topology/local notify as exact-read doorbells. RESET, overflow, malformed
  payloads, revision gaps, and conflicts force Controller exact resync while retaining last-good state. Coordinator
  keeps role-specific backend watch registrations. Observer watches exact topology only.
- When unified ETCD loses write quorum or becomes unreachable after a last-good snapshot exists, Engine publishes
  `CONTROL_DEGRADED` without revoking business admission and keeps that immutable snapshot authoritative. The Controller
  treats ETCD membership absence as suspicion rather than proof of Worker failure: only members continuously absent for
  `nodeDeadTimeout` are direct-probed. Canonical committed-member order assigns each target to its fixed successor, so
  exactly one Controller owns that target's probe and each Controller owns at most one target. This ownership changes
  only after a new authoritative topology is committed; local membership timers cannot create extra reporters. A
  responding member starts a new absence window, and a target not owned or not probed by this Controller cannot enter
  its Failure plan. The peer RPC reads the cached control-backend observation and never synchronously queries ETCD
  inside the bounded liveness probe. A reachable peer with temporarily unavailable backend evidence therefore returns a
  successful `ready=false` response, so transport reachability remains distinguishable from authoritative topology
  evidence. Its protobuf carries the 16-byte binary member identity as `bytes`, so BRPC never rejects non-UTF-8
  identities. The `string`-to-`bytes` correction preserves protobuf wire type and lets upgraded readers consume
  successfully serialized legacy responses, but a legacy reader cannot consume every binary response from an upgraded
  Worker. Deploy or roll back this behavior cluster-wide; an ETCD outage during a mixed-version rolling upgrade is
  outside this contract.
  BRPC stub acquisition and channel establishment share this low-frequency probe's absolute deadline;
  default business-RPC stub lookup semantics remain unchanged. An absent direct response remains retryable until the same
  membership absence has continued through
  `nodeDeadTimeout` plus one
  `failureProbeTimeout`; only then is the member eligible for the existing Failure plan. Backend-unreadable intervals
  observed while reading either topology or membership do not consume the continuous-absence budget. Coordinator mode
  retains its original quorum-confirmed degradation versus local-isolation behavior.
- Repeated expected backend-access failures while already in `CONTROL_DEGRADED` still refresh the diagnostic
  `lastError`, but the warning log is sampled. State transitions and unexpected runtime failures remain unsampled.
- Topology observability is carried by structured `CLUSTER_*` logs on the low-frequency control path: watch events and
  queue overflow/coalescing, membership observations with bounded samples and digests, member join/leave/failure
  transitions, batch start/deadline/finalization, task notify/callback/progress, and Worker/Observer snapshot
  publication. Member samples print all members up to the current operational bound of 32 and mark larger future
  topologies as truncated. Task lifecycle logs carry `stage`/`stage_event`, executor/source/target/failed identities,
  and the exact closed `hash_ranges`. Correlate cross-thread work by batch epoch, task prefix, and business-operation
  prefix rather than assuming one trace spans the Controller and callback threads.

  | Keyword | Main source | Trigger and useful fields |
  | --- | --- | --- |
  | `CLUSTER_WATCH` | Controller/Worker/Observer loops | watch registration, resync, or fatal watch exits; includes role/scope/status. |
  | `CLUSTER_WATCH_EVENT` | Controller/Worker/Observer watch callbacks | topology, membership, task, notify, or observer watch ingress; includes action/revision/key. |
  | `CLUSTER_WATCH_QUEUE` | `CoordinationEventDispatcher` | queued event overflow/coalescing and reset doorbells; includes event counters and depth. |
  | `CLUSTER_MEMBERSHIP` | Controller membership reconciliation | membership read failures or per-cycle membership summary; includes version and member counts. |
  | `CLUSTER_MEMBERSHIP_OBSERVED` | Controller membership reconciliation | changed membership digest/sample after membership watch dirties state. |
  | `CLUSTER_MEMBER_TRANSITION` | Controller planner | member state changes such as INITIAL/JOINING/LEAVING/FAILED. |
  | `CLUSTER_FAILURE_DETECT` | Failure classifier/controller | endpoint or membership failures promoted to topology change candidates. |
  | `CLUSTER_CHANGE_BATCH` | Controller batch commit path | batch start, deadline expiration, finalization, and preemption; includes batch type/epoch/version. |
  | `CLUSTER_MEMBER_JOIN_SUMMARY` | Controller scale-out commit | summarized joining members admitted into a scale-out batch. |
  | `CLUSTER_MEMBER_LEAVE_SUMMARY` | Controller scale-in/failure commit | summarized leaving or failed members in a removal batch. |
  | `CLUSTER_CHANGE` | Controller decisions | scale-in wait, completion, abort, or no-op decisions with version and batch context. |
  | `CLUSTER_RECONCILE` | Controller serialized loop | successful reconciliation after queued events or commits; includes elapsed time and queue counters. |
  | `CLUSTER_DEGRADED` | Worker Engine | business-admission level and reason transitions during backend loss and recovery. |
  | `CLUSTER_RING` | Controller/Worker/Observer topology publication | newly committed or locally published version, membership counts, and per-member `committed_ring`/`prospective_ring` ranges. |
  | `CLUSTER_TASK` | Materializer/executor | task materialization, notify, stage start/finish/failure, exact participants/ranges, cleanup, and progress outcomes. |
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
  budget by that remaining deadline instead of restarting the default RPC timeout for every batch. Operationally,
  `CLUSTER_SCALE_IN action=metadata_done checkpoint_scope=task` describes one persisted task marker; `source_gate`
  distinguishes waiting from all-metadata-ready. Data drain is source-Worker scoped, so `target_role=trigger` marks the
  shared participant-scope `target` as the per-task trigger instead of implying that one target owns the whole drain.
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
- Worker startup calls membership keepalive initialization, then its external Controller ranges membership at revision
  `R` and accepts an exact topology only when its modification revision is no newer than `R`. A concurrent newer
  topology causes a bounded retry. Unified topology/membership watches start from `R + 1`; a Worker exact-read
  `K_NOT_FOUND` or `K_NOT_READY` changes only its local-notify target to `WATCH_FROM_NOW`, preserving the historical
  startup wait behavior without weakening the Controller baseline. Revision-bearing `GetAll` does not fall back to an
  ordinary read: a backend without a consistent snapshot revision returns `K_NOT_SUPPORTED`.
- Coordinator uses the same keepalive-init-then-single-reload-before-watch call order. Its watch descriptor revision
  remains zero because `WatchRange` ignores the field and returns `initial_kvs` plus a RESET doorbell. `K_NOT_FOUND` and
  `K_NOT_READY` allow bootstrap waiting; other reload errors fail before any `WatchRange` call. A successful reload
  synchronously publishes the Snapshot before watches are registered.
- Unified-ETCD recovery reuses the normal exact topology reload. The membership lease recreates its key before
  `IsKeepAliveTimeout()` becomes false. A transient recovery ordering where another Controller still sees the key absent
  is covered by the same direct Worker liveness check; it cannot remove a responding member from topology.
- Coordinator watches bind both `CoordinatorId` and `watch_id`. Watch registration uses a client registration ID so an
  ambiguous WatchRange result retries idempotently. Initial/recreated membership invalidates both Worker and Controller
  role plans using O(1) RESET doorbells; lease threads never wait for watch-registration RPCs.
- An ETCD canceled watch, including compaction cancellation, exits the producer and enters the existing whole-stream
  `WatchRun` recovery path. A RESET first makes both serialized consumers rebuild or retain last-good state; active
  compensation then reads current state, emits value-bearing fake PUT/DELETE events, advances every unified target
  revision, and rebuilds the stream as one unit rather than recreating an individual watcher. Passive compensation uses
  the same event path, with the abnormal retry interval selected after a failed compensation attempt.
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
- The unified-ETCD traffic-preservation contract covers a topology-stable normal -> backend fault -> normal sequence.
  A Worker failure, scale operation, restart, active batch, or non-`ACTIVE` topology member during the backend fault is
  outside that contract: the Worker safely retains last-good routing and remains `CONTROL_DEGRADED`, but control-plane
  progress and automatic return to `NORMAL` are not guaranteed until the conflicting operation is resolved.
- The request-level zero-failure contract covers ordinary in-memory routing and RPC connection checks, including
  `NONE_L2_CACHE_EVICT` objects. It does not cover a Get that must recover object metadata directly from ETCD through
  `oc_io_from_l2cache_need_metadata`; that L2 metadata fallback remains backend-dependent.
- Temporary endpoint observations stay process local. Only confirmed Failure enters the authoritative topology.
- At most one change type is active at a time; one batch may contain many members. Failure has highest priority and may
  preempt ordinary work. Scale-in waits for an already-running scale-out batch to finish.
- Every Failure owner change must source from a member selected into that Failure batch, whose committed state becomes
  `FAILED`, and must target an `ACTIVE` member. Preserved `PRE_LEAVING`/`LEAVING` facts from preempted ordinary work
  must not become Failure task sources; `INITIAL`/`JOINING` crashes remain uncommitted-member cleanup.
- All callbacks must be deadline-aware, cooperatively cancellable, idempotent by operation ID, and safe under duplicate
  delivery. Process termination is supplied by Kubernetes or the process manager after bounded drain.
- Worker task notifications are derived, idempotent records. A notification observed after its active batch finalizes,
  or while a different batch is authoritative, is a stale no-op rather than a runtime failure.
- Background reconciliation must remain resource bounded and must always converge to a state that permits a later batch.
- Topology Engine and Observer watch-registration logs identify `start_mode` (`from_now` or `after_revision`) and
  `last_processed_revision` (`none` or the numeric revision). Observer keeps its existing revision-zero watch behavior
  and reports `start_mode=after_revision last_processed_revision=0`; logs must not imply revision zero for every
  backend.

## Tests

- Main contract/component binary: `cluster_topology_contract_ut`.
- Core CTest selection:
  - `ctest -R 'ClusterTopology|TopologyRepository|TopologyObserver|PlacementFacade'`
  - `ctest -R 'TopologyController|TopologyTaskExecutor|TopologyEngine|TopologyDfx|TopologyShutdown'`
- Business adapter coverage lives in `ds_ut_object`, `ds_ut_stream`, and selected Worker/object/stream ST binaries.
- Operator-query coverage includes `CoordinatorStoreTest` raw RPC cases, `ClusterQueryProjectorTest`, Python
  `test_cli_query.py`, and a packaged-wheel real-backend smoke test.
- Failure-preemption coverage in `cluster_topology_contract_ut` exhausts unrelated member states and includes
  multi-crash scale-in, mixed scale-out/pending-scale-in, cascading Failure replans, and source/target diagnostics.
- State machine, CAS/fence, crash points, retry, resource limits, and Shutdown belong in UT/LLT/component tests. ST only
  proves representative process, ETCD watch/lease, network, and real callback wiring.
- ETCD availability release validation uses a real three-member ETCD cluster and release-package Workers. A continuous
  client loop must observe zero failed `Set`/`Get`/`Del` requests while losing one member, losing write quorum (two
  members), losing all members, and after each restoration. Do not substitute Coordinator-backed Engine tests for this
  unified-ETCD path.

## Update Triggers

- Update this module when cluster topology schema, keyspace/watch scope, routing semantics, callback contract,
  controller/executor recovery, Worker ownership, or shutdown ordering changes.
- Update the quality context when test binary ownership, labels, or remote validation commands change.
