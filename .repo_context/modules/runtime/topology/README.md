# Cluster Topology Runtime

## Scope

- Status: `active`
- Canonical source roots:
  - `src/datasystem/cluster`
  - `src/datasystem/protos/cluster_topology.proto`
  - `src/datasystem/coordinator/coordinator_store_backend.{h,cpp}`
  - `src/datasystem/coordinator/topology_control_host.{h,cpp}`
  - `src/datasystem/worker/worker_oc_server.cpp`
  - `src/datasystem/worker/metadata_route_resolver.{h,cpp}`
- The module owns authoritative cluster membership state, immutable routing snapshots, topology planning, task
  materialization/execution, and the backend-specific control loop.
- Headers under `src/datasystem/cluster` are repository-internal composition interfaces, not installed SDK headers or
  a cross-release source-compatibility surface. Bazel visibility and CMake public include propagation support monorepo
  consumers; external client compatibility is owned by installed headers under `include/datasystem`.
- `DsCoordinationBackend` preserves the topology architecture while using the in-memory Coordinator transport. A
  restarted Coordinator fences its new lifetime with `CoordinatorId`, gates topology/task/notify access, accepts
  Worker-reported last-good topology candidates, installs one canonical highest version, and regenerates derived work.
  Membership mutation and keepalive RPCs additionally carry the exact key modification revision, so a delayed RPC from
  an older same-address Worker incarnation cannot overwrite or renew the current membership record. Revisions are
  monotonic only within one Coordinator lifetime: a same-lifetime delayed Ensure retains the newer local revision, while
  a changed `CoordinatorId` replaces it with the new in-memory Store's ensured revision even when that number is lower.
  Worker Leader reconciliation installs ensured membership outside its state mutex because installation synchronously
  publishes membership readiness; it then rechecks Router identity before reporting so a successor lifetime can enqueue
  its fenced Ensure without recursive locking or a stale recovery report. Coordinator-side `EnsureLeaderMembership`
  uses the same `TopologyControlHost` reservation/completion transaction as normal membership Put, so topology recovery
  cannot become READY with committed memberships but no Controller runtime owner. Reconciler shutdown transfers its
  Ensure pool under the scheduling mutex and joins outside the lock, fencing already copied membership callbacks from
  pool destruction without deadlocking the Ensure loop.
  `TopologyControlHost` then creates one centralized `TopologyControllerRuntime` per admitted cluster inside
  `datasystem_coordinator`; a Coordinator-backed Worker only consumes topology/own-notify and executes business effects.

## Current Design Shape

- `datasystem::cluster::TopologyEngine` is the Worker-role composition root. Its nested Builder is the sole Worker
  construction path. Engine creates the member backend, algorithm, Worker repository/reader, dispatcher, executor,
  immutable snapshot state, placement facade, endpoint view, and optional Coordinator recovery reporter. ETCD mode also
  creates the existing controller backend, `TopologyControllerRuntime`, local liveness probe, and Janitor. Coordinator
  mode deliberately does not create those control-role components. Engine does not own standalone Observer.
- Every ETCD Worker may run a `TopologyController`. Controllers contend through the single authoritative topology-key
  CAS; controller identity is not persisted, and deterministic batch/task identities make duplicate reconciliation safe.
- `CoordinatorStoreBackend` adapts one cluster-scoped view of the existing in-memory `CoordinatorStore` to the unchanged
  `ICoordinationBackend` KV/CAS contract. It owns no thread, watch, lease, or recovery state.
- `TopologyControlHost` is the Coordinator-process lifecycle owner for `cluster_name -> TopologyControllerRuntime`.
  It admits only clusters whose membership mutation has committed, waits for `TopologyRecoveryManager` to report READY,
  starts/stops Runtime dependencies outside its mutex, enforces the active-cluster cap, and keeps each cluster's
  retry/blocking state isolated. Each entry owns its own `HashAlgorithm`, Store adapter, and Runtime, destroyed in
  Runtime -> backend -> algorithm order.
  Store topology/membership/migrate-task/delete-task mutations are payload-free doorbells only. Membership PUT bursts and
  task-progress bursts coalesce into the Host's dirty bit; notify mutations do not wake the Host. The Controller derives
  task IDs from current topology and exact-reads those IDs, so the Host never scans task prefixes.
- `TopologyRepository` stores one `ClusterTopologyPb` authority record plus derived task/notify records and ScaleIn
  metadata-done markers. Derived records cannot replace topology authority; final progress and batch transitions are
  fenced by topology version and batch epoch.
- `cluster_topology.proto` owns these topology records under protobuf package `datasystem`; coordinator and other
  unrelated protobuf contracts remain separate schemas.
- `TopologyEngine`, `TopologyController`, and standalone `TopologyObserver` each own one serialized state loop. ETCD
  Workers use the Worker-owned `EtcdStore` and one unified watch stream for exact topology/local notify plus membership;
  Engine routes physical-key events by role. Controller validates and applies ETCD topology/membership PUT values to its
  state-thread-owned fact cache; a membership DELETE exact-resyncs the complete prefix because the event carries no
  replacement value and same-revision deletes are dispatched individually. Worker keeps topology/local notify as
  exact-read doorbells. RESET, overflow, malformed payloads, revision gaps, and conflicts force Controller exact resync
  while retaining last-good state. Coordinator Host keeps Store mutations as RESET doorbells, so its Controller continues
  exact-reading Store facts. Observer watches exact topology only.
- When unified ETCD loses write quorum or becomes unreachable after a last-good snapshot exists, Engine publishes
  `CONTROL_DEGRADED` without revoking business admission and keeps that immutable snapshot authoritative. The Controller
  treats ETCD membership absence as suspicion rather than proof of Worker failure: only members continuously absent for
  `nodeDeadTimeout` are direct-probed. Canonical committed-member order assigns each target to its fixed successor, so
  exactly one Controller owns that target's probe and each Controller owns at most one target. This ownership changes
  only after a new authoritative topology is committed; local membership timers cannot create extra reporters. A
  responding member starts a new absence window, and a target not owned or not probed by this Controller cannot enter
  its Failure plan. When a topology has at least one committed member and none of those committed addresses has a
  matching membership row, the Controller preserves the last-good topology while reusing the same continuous-absence
  timer. The collective path observes only its deterministic sample subset in the classifier, discards timers outside
  that subset, and never materializes or sorts a full-topology failure classification on each reconcile. After
  `nodeDeadTimeout`, it samples at most five address-sorted committed members and probes one sample per reconcile. ETCD
  assigns the current address-smallest READY membership as the sole probe owner;
  Coordinator uses one stable central-owner identity and reuses the existing watch RPC transport with a validation-only
  event rejected during whole-batch validation. The dispatched application-error response proves process liveness
  without delivery to the watch handler or triggering rewatch. Topology-version, ETCD READY-owner, or no-READY
  transitions fence all accumulated sample progress, including A -> B -> A changes. The
  Coordinator probe's absolute deadline covers endpoint parsing, deadline-aware stub acquisition, concrete stub cast,
  and `HandleEvent`; provenance distinguishes a dispatched peer application error from a neutral local error. Any
  attributed response or application-level error resets that sample's full absence window. Only attributed
  deadline/unavailable outcomes accumulate as unreachable. Once every sample is unreachable, an exact membership reread
  must still contain no old
  committed address and at least one READY generation before the Controller bootstraps those READY generations through
  the existing empty-topology `BuildBootstrap` and topology CAS path. A returned old membership, absent READY
  generation, changed owner, read failure, or CAS conflict preserves the old topology. All except read failure discard
  stale sample progress; read failure instead retains it and pauses absence time. This includes a genuine sole-member
  stale topology. This fixed-five check is a bounded recovery heuristic, not strict fencing. A partitioned or unsampled
  old Worker can remain alive while replacement Workers form a temporarily independent cluster; this is an explicitly
  accepted risk of the bounded heuristic. When an old Worker reconnects and publishes the replacement authority, the
  existing local-member-missing path kills and restarts it so it can rejoin the authoritative ring. Before replacement
  topology CAS, rollback can remove the heuristic. After authority is replaced, binary rollback alone is unsafe;
  operators must use a controlled stop and topology recovery. The peer RPC reads the cached control-backend observation and
  never synchronously queries ETCD inside the bounded liveness probe. A reachable peer with temporarily unavailable
  backend evidence therefore returns a successful `ready=false` response, so transport reachability remains
  distinguishable from authoritative topology evidence. Its protobuf carries the 16-byte binary member identity as
  `bytes`, so BRPC never rejects non-UTF-8 identities. The `string`-to-`bytes` correction preserves protobuf wire type
  and lets upgraded readers consume successfully serialized legacy responses, but a legacy reader cannot consume every
  binary response from an upgraded Worker. Deploy or roll back this behavior cluster-wide; an ETCD outage during a
  mixed-version rolling upgrade is outside this contract.
  Coordinator collective probe progress is additionally bound to the current nonzero Raft leader term; loss of control
  authority or a term change discards accumulated samples before any further probe or replacement attempt. Each
  Coordinator-to-Worker control probe and the final collective replacement commit use that expected term under the
  shared leader-operation fence, so Leader stop or term replacement cannot authorize evidence accumulated by an older
  term. Election-free singleton mode uses one stable process-lifetime control epoch. Ordinary Controller Store mutations
  remain outside this narrow collective-recovery fence.
  BRPC stub acquisition and channel establishment share this low-frequency probe's absolute deadline;
  default business-RPC stub lookup semantics remain unchanged. After classifier confirmation, the owned successor runs
  one complete bounded direct probe (`failureProbeTimeout`). The probe returns one structured result per target with
  the optional observation, actual completion outcome, and elapsed time. Any correctly attributed direct response
  (including `ready=false` or stale evidence) resets the absence window. A target without a valid direct response is
  exact-read from authoritative membership before the existing Failure plan can start: membership presence resets the
  absence window, absence remains eligible, and read failure pauses the absence budget and aborts that reconcile tick.
  There is no second full probe wait gated on probe-start `missingMs` versus
  `nodeDeadTimeout + failureProbeTimeout`. Backend-unreadable intervals observed while reading either topology or
  membership do not consume the continuous-absence budget. In centralized Coordinator mode,
  `TopologyFailureClassifier` instead promotes a member only after it has remained absent from consecutive exact
  membership reads for `node_dead_timeout_s`; presence clears the window and Store read failures pause it. Collective
  stale-topology recovery additionally uses the bounded validation-only Coordinator-to-Worker watch-transport probe
  described above.
- Both backends use the same fixed three-second joining collection window beginning at the first eligible member.
  Later arrivals do not extend the deadline. An empty cluster admits the collected bootstrap members directly without
  migration; an initialized cluster starts one multi-member ScaleOut batch. Failure remains higher priority.
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
  | `CLUSTER_FAILURE_DETECT` | Failure classifier/controller | endpoint or membership failures promoted to topology change candidates; Witness probe decisions carry `probe_id`. |
  | `CLUSTER_WORKER_PROBE` | Worker/Coordinator probe delivery | end-to-end Witness event, queue, peer probe, report, and ingress stages correlated by `probe_id`. |
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
  also derives the post-commit owner ring. A ScaleOut transfer range waits while the committed source still owns or is
  migrating that key. Metadata absence alone is not completion evidence: after a successful callback and progress CAS,
  the Executor publishes immutable process-local completion ranges scoped by ScaleOut batch epoch. A key redirects only
  when its token is covered by that evidence; publication leaves the epoch by atomically clearing the evidence. A
  ScaleIn source whose metadata handoff has completed redirects missing metadata to the prospective owner. Migration
  RPC fence validation may wait through a bthread-aware condition variable only for the process-local Snapshot to catch
  up within the existing request deadline; it never synchronously reads the backend. A newer Snapshot in the same
  active batch remains valid after epoch/type/participant revalidation.
  ScaleIn revalidation requires the target to remain `ACTIVE`; a target that has entered `PRE_LEAVING` cannot accept
  metadata or data handoff from another leaving member.
- Business migration/recovery is invoked through one opaque task callback. `IKeyFilter` and `StorageScanPlan` keep token
  representation internal, while callbacks receive stable operation identity, deadline, and cooperative cancellation.
  `TopologyTaskExecutor` owns one mutex-protected state record per business operation; retry timing, attempt count,
  cancellation, progress-only retry, and ScaleIn stage facts cannot diverge across parallel maps. Sparse reverse indexes
  retain only operation IDs needed to scan scheduled retries without walking the full operation table and to determine
  when one source/batch metadata gate becomes ready.
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
- `WorkerOCServer` owns the `TopologyEngine` composition root, a fixed three-thread witness-probe pool with a bounded
  FIFO event queue and in-flight count, plus the Store/Proxy and callback resources borrowed by the Engine.
  `ConstructTopologyRuntime` sets classifier `nodeDeadTimeout` to
  `max(0, FLAGS_node_dead_timeout_s - FLAGS_node_timeout_s)` so confirmed failure tracks wall-clock
  `node_dead_timeout_s` after lease expiry (`node_timeout_s`), not a second full `node_timeout_s`. A zero budget
  confirms on the first successful membership-absence observation. Shutdown drains business RPC ingress, stops accepting
  probe events, clears pending probes, shuts down Engine ingress, waits for in-flight probe/report work, and only then
  destroys the joined probe pool. In ETCD mode Engine closes the shared watch and keepalive event sources once, drains
  Worker execution, stops the externally-fed Controller/Janitor, and fully shuts down the Store once. Coordinator mode
  unbinds ingress and preserves role-specific event-source shutdown. A deadline failure preserves Engine, probe pool,
  and every borrowed dependency for retry. Engine Start is one-shot;
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
  deployments sharing one backend must use non-empty distinct names. The seven logical paths are topology,
  tasks/migrate, tasks/delete, notify, probe, cluster membership, and ScaleIn metadata-done markers. Each probe PUT is a
  non-authoritative, overwriteable single-target event under `root/probe/<witness_address>`. Normal watch delivery handles
  each revision independently. The protocol does not require historical event recovery; Coordinator rewatch may redeliver
  the key's latest value as a duplicate event, which epoch/member/round fencing safely tolerates. Multiple witnesses provide
  best-effort redundancy, so probe delivery lowers but does not eliminate false-failure probability.
- `TopologyKeyHelper` is the only topology keyspace builder. It owns logical tables, the legacy-compatible ETCD
  membership prefix, and allocation-free physical watch-key classification across Coordinator and ETCD layouts.
  `EtcdStore::CreateTableWithExactPrefix`
  registers these paths without legacy `FLAGS_cluster_name` prefix rewriting. `TopologyEngine::Builder` owns
  registration for the shared ETCD Store; Worker business composition does not construct topology keys or table
  mappings. `TopologyEngine` maps classified key kinds to Worker/Controller delivery policy.
- There is no persisted Worker-local topology authority. ETCD restart recovery reads the latest legal topology and
  reconstructs deterministic work. The in-memory Coordinator backend recovers only the latest topology from Workers;
  task/notify records are treated as absent and regenerated. Candidate arbitration is cluster-scoped and resource
  bounded; conflicting same-version digests block only that cluster until membership/evidence changes. If all
  memberships briefly disappear without a Coordinator restart, a later returning member reuses a legal topology already
  present in the current process Store as the local authority; Worker evidence cannot overwrite or block it.
- Worker startup calls membership keepalive initialization, then its external Controller ranges membership at revision
  `R` and accepts an exact topology only when its modification revision is no newer than `R`. A concurrent newer
  topology causes a bounded retry. Unified topology/membership watches start from `R + 1`; a Worker exact-read
  `K_NOT_FOUND` or `K_NOT_READY` changes only its local-notify target to `WATCH_FROM_NOW`, preserving the historical
  startup wait behavior without weakening the Controller baseline. Before watch registration succeeds, the external
  Controller does not reconcile the baseline; Engine submits RESET after registration to exact-resync the read-watch
  window before enabling control decisions. Revision-bearing `GetAll` does not fall back to an ordinary read: a backend
  without a consistent snapshot revision returns `K_NOT_SUPPORTED`.
- UB health is a non-authoritative membership sidecar rather than topology state. Each Worker periodically publishes one
  self-only summary under a separate keyspace using the active membership lease/TTL and consumes the bounded O(N)
  snapshot into `PeerUbAdmission`. Missing leased records clear global quarantine, malformed live records preserve the
  last accepted quarantine, and neither path erases process-local failure evidence. Topology membership and Failure
  planning remain the only authoritative ownership inputs.
  - The Bazel `cluster_topology` target depends on the lightweight
    `//src/datasystem/common/object_cache:ub_health` target. Keep this boundary free of the full `common_object_cache`
    dependency so the coordinator does not inherit shared-memory and data-plane link requirements.
- Coordinator uses the same keepalive-init-then-single-reload-before-watch call order. Its watch descriptor revision
  remains zero because `WatchRange` ignores the field and returns `initial_kvs` plus a RESET doorbell. `K_NOT_FOUND` and
  `K_NOT_READY` allow bootstrap waiting; other reload errors fail before any `WatchRange` call. A successful reload
  synchronously publishes the Snapshot before watches are registered.
- A Coordinator-mode Worker whose restart-fact exact read returns `K_NOT_READY` during initial Coordinator recovery is
  provisionally initialized as a fresh start so membership and recovery reporting can bootstrap. Simultaneous
  Coordinator recovery and restart of that same Worker is an explicit Phase 3 limitation; the initial version assumes
  surviving Workers reconnect and report topology rather than guaranteeing this combined-failure case.
- A real restarted Worker remains `RESTARTING`/`RECOVERING` while Object Cache reconciliation is enabled. It publishes
  `READY` only from reconciliation completion or the explicit give-up terminal path; reconciliation completion counts
  distinct source addresses that have succeeded for the current membership timestamp, commits the membership
  transition before publishing process health, and propagates synchronous master-to-Worker delivery failures so the
  same restart generation remains retryable. Fresh starts and configurations without that reconciliation requirement
  retain the direct startup transition.
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
- Task cleanup first CASes the exact task value to a repository-internal, versioned deletion tombstone, then performs
  physical deletion. Every cleanup attempt, including one that observes an old tombstone, must win the CAS to the next
  tombstone version, so Janitors holding the same observed token cannot both delete. The tombstone is invisible as a task
  and fences same-ID writes during that delete attempt. Because the frozen `ICoordinationBackend` has no compare-delete,
  this is deliberately not a cross-process transaction: an overlapping later cleanup may race derived-state
  rematerialization. Task/notify are non-authoritative and deterministic Controller reconciliation restores any such
  record; topology correctness never depends on Janitor success. The same pass removes stale ScaleIn metadata-done
  markers whose epoch is older than the active batch, or no newer than the final topology version when no batch is active.
- Failure metadata recovery is at-least-once, idempotent, and best effort. Normal recovery failure is not retried; a
  coordinator crash before final topology CAS may repeat it. Object recovery scans only a configured shared ETCD-backed
  metadata store. When that backend is disabled, the scan is an empty successful recovery set; it does not fall back to
  Worker-local RocksDB or treat Coordinator topology storage as Object Cache metadata.
- Scale-out and scale-in callbacks use bounded retries. Exhausted scale-out removes the joining member so it can restart
  and re-enter as `INITIAL`; exhausted scale-in proceeds through external bounded termination and Failure handling.
  Callback window exhaustion advances the operation record's attempt count and re-arms its next-attempt deadline with
  bounded backoff rather than blindly resetting it through the due-operation preservation path; the operation stays
  pending so the controller's failure-confirmation / lease-expiry path can still finalize it.
  Object and stream callbacks treat per-item migration failures as retryable task failures, so a successful RPC status
  alone cannot advance the batch while selected metadata is still missing at the target. Object metadata success
  includes durable nested relationships and nested reference counts; target persistence failure keeps the source-side
  state authoritative for retry. A completed failed attempt also clears its source-side migrating marker after
  restoring source state, allowing the next bounded retry to serve the metadata from the source instead of reporting a
  stale moving state.
- Failure preempts an ordinary batch by fencing its old execution round, preserving `JOINING`/`LEAVING` facts, completing
  the Failure batch first, and replanning ordinary work from the latest topology. Business migration futures own the
  same cooperative cancellation state and absolute deadline as the outer callback. They recheck both before every
  destructive source-side metadata commit, so target acknowledgement cannot let a preempted old round continue deleting
  after its outer callback has returned.

## Key Entry Points

- Runtime: `src/datasystem/cluster/runtime/topology_engine.{h,cpp}`
- Controller composition: `src/datasystem/cluster/control/topology_controller_runtime.{h,cpp}`
- Control: `src/datasystem/cluster/control/topology_controller.{h,cpp}`
- Execution: `src/datasystem/cluster/executor/topology_task_executor.{h,cpp}`
- Persistence: `src/datasystem/cluster/repository/topology_repository.{h,cpp}`
- Routing: `src/datasystem/cluster/routing/placement_facade.{h,cpp}`
- Backend: `src/datasystem/cluster/coordination_backend/etcd_coordination_backend.{h,cpp}`
- Existing Coordinator transport: `src/datasystem/cluster/coordination_backend/ds_coordination_backend.{h,cpp}`
- Coordinator Store adapter: `src/datasystem/coordinator/coordinator_store_backend.{h,cpp}`
- Coordinator control host: `src/datasystem/coordinator/topology_control_host.{h,cpp}`
- Coordinator composition: `src/datasystem/coordinator/coordinator_service_impl.{h,cpp}`
- Worker composition: `src/datasystem/worker/data_worker.cpp`, `src/datasystem/worker/worker_oc_server.cpp`
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
- In Coordinator mode, membership TTL expiry creates a per-target, process-local witness probe round. For each selected
  witness, the Controller publishes one coordinator-ID-fenced single-target event under
  `root/probe/<witness_address>` through the existing watch channel. Each received PUT is admitted independently to a
  bounded FIFO queue; later target events do not cancel earlier events. Three fixed-concurrency loops probe targets with
  `GetClusterState` and report the target member identity, coordinator ID, probe round, and reachability with bounded
  best-effort attempts. One fresh reachable report from any invited witness gates `confirmedFailure` immediately before
  commit without rewriting membership or resetting the classifier's missing fact. Expired rounds without reachable
  evidence proceed through the existing Failure path; long Coordinator partitions remain protected only while successive
  rounds keep producing fresh reachable evidence. Probe delivery and reporting are not end-to-end guaranteed.
- Worker startup selects the coordination backend once: a non-null `ICoordinatorDiscovery` selects Coordinator mode, while a null pointer selects ETCD/metastore. All Worker composition branches use that constructor-selected pointer instead of independently re-reading `coordinator_address`.
- At most one change type is active at a time; one batch may contain many members. Failure has highest priority and may
  preempt ordinary work. Scale-in waits for an already-running scale-out batch to finish.
- Every Failure owner change must source from a member selected into that Failure batch, whose committed state becomes
  `FAILED`, and must target an `ACTIVE` member. Preserved `PRE_LEAVING`/`LEAVING` facts from preempted ordinary work
  must not become Failure task sources; `INITIAL`/`JOINING` crashes remain uncommitted-member cleanup.
- All callbacks must be deadline-aware, cooperatively cancellable, idempotent by operation ID, and safe under duplicate
  delivery. Process termination is supplied by Kubernetes or the process manager after bounded drain.
- Worker task notifications are derived, idempotent records. `TaskNotifyPb` carries the authoritative active-batch fence,
  exact task IDs, and Coordinator-mode restart timestamps. A notification observed after its active batch finalizes, or
  while a different batch is authoritative, is a stale no-op rather than a runtime failure. Restart effects execute
  after active tasks have been admitted, submit one pending generation batch to the Executor-owned callback pool,
  deduplicate successful timestamps in Worker memory, and retry failures from the next periodic exact own-notify read.
  The Worker Object Cache and metadata master each scan their local object/metadata table once per batch rather than once
  per restarted member. Accepted restart batches participate in the same shutdown drain as task callbacks. A slow
  restart cleanup therefore cannot consume the active batch's admission deadline.
- One derived generation owns one canonical restart-only notify suffix. Materialization encodes that shared map once,
  encodes each recipient's active-batch/task prefix independently, and appends the suffix before exact-byte CAS. This
  preserves canonical `TaskNotifyPb` bytes while avoiding repeated protobuf-map construction for every recipient; schema
  and parity tests fence the field-order assumption.
- The backend-neutral Janitor physically removes stale notify keys through a dedicated, versioned byte-matched
  tombstone. A concurrent rematerialization sees `K_TRY_AGAIN` while that tombstone remains; any later physical-delete
  race is healed by normal deterministic notify reconciliation. Legacy empty notify values are also cleanup candidates,
  so they cannot consume the scan prefix forever. Per-table rotating cursors ensure an undeletable sorted prefix cannot
  permanently starve later stale records; the current implementation still uses full-table `GetAll` before applying the
  bounded page and does not claim 10K-table scan efficiency.
- Background reconciliation must remain resource bounded and must always converge to a state that permits a later batch.
- Topology Engine and Observer watch-registration logs identify `start_mode` (`from_now` or `after_revision`) and
  `last_processed_revision` (`none` or the numeric revision). Observer keeps its existing revision-zero watch behavior
  and reports `start_mode=after_revision last_processed_revision=0`; logs must not imply revision zero for every
  backend.

## Tests

- Main contract/component binary: `cluster_topology_contract_ut`.
- Coordinator host/adapter coverage: `CoordinatorStoreBackendTest` and `TopologyControlHostTest` in `ds_ut`.
- Manual scale/performance coverage: `topology_control_perf_test`.
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
