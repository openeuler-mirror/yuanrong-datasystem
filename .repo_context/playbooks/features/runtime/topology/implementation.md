# Cluster Topology Implementation Workflow

Use with:

- `.repo_context/modules/runtime/topology/README.md`
- `.repo_context/modules/runtime/worker-runtime.md`
- `.repo_context/modules/runtime/etcd-metadata/README.md`
- `.repo_context/modules/quality/tests-and-reproduction.md`

## Before Editing

- Confirm the change belongs to `src/datasystem/cluster` or to a named Worker/business integration point. Do not add a
  sibling topology runtime, compatibility adapter, or duplicate backend watch loop.
- Identify whether the change touches foreground immutable-snapshot routing, authoritative topology CAS, derived tasks,
  callback side effects, endpoint observation, startup recovery, or shutdown/lifetime.
- Search for existing `TopologyKeyHelper`, repository codec, placement facade, task fence, retry classifier, cancellation,
  and diagnostic helpers before adding an API.
- Keep `ICoordinationBackend` aligned with the master interface plus the approved `ShutdownEventSources()` and
  `Shutdown()` lifecycle additions.

## Design Gates

- Persistence: only `ClusterTopologyPb` is authority; derived task/notify state is deterministic and regenerable.
- Concurrency: one engine serializes backend events; callbacks run in a bounded pool and return completion to the serial
  runtime for full-fence revalidation.
- Foreground path: use one immutable `TopologySnapshot`; no backend IO, token exposure, or callback work.
- Recovery: every crash point must replay idempotently or terminate the affected batch without blocking future batches.
- Failure: temporary endpoint status is local; confirmed Failure may preempt ordinary work and must heal from latest
  topology even during cascade or combined failures.
- Lifecycle: event sources stop first, accepted callbacks receive cancellation and bounded drain, runtime threads join,
  and backend/service owners outlive all users.
- Resources: enforce the named member, queue, task, notify, callback-pool, reconcile, and cleanup limits from the detailed
  design. Do not replace a limit with an unbounded scan or queue.

## Test-First Validation

- Add or select the narrowest contract test that fails before changing behavior.
- Use `cluster_topology_contract_ut` for schema/key/backend/repository/observer/controller/executor/DFX/Shutdown behavior.
- Use `ds_ut_object` or `ds_ut_stream` for real business callback and routing adapters.
- Use selected ST only for process composition, real ETCD watch/lease, network outage/recovery, multi-member scale flow,
  Router candidate failover, and one object/stream callback path.
- Before every ST, rebuild `datasystem_worker_bin` together with the relevant ST target.
- Treat a CTest case exceeding 60 seconds as hung and kill only its exact residual process with `kill -9`.
- For failures, collect the full first batch, group by root-cause family, compare a representative case with the frozen
  refactor2 baseline, and verify INJECT_POINT name/hit timing before changing production logic.

## Build And Review

- Validate remotely with `$ds-test` through one reused ProxyJump connection. Use `-j20` normally and never less than
  `-j16` unless the remote toolchain imposes a hard limit; do not run builds or log processing on the jump host.
- CMake test builds and Level0/Level1 precede the final Bazel Release-only build. Bazel must not add or build tests.
- Apply the repository hard gates: 120-column maximum, 50 effective lines per function, no unexplained magic numbers,
  copyright in every new C++ file, explanatory comments for non-obvious recovery/concurrency, and actionable logs.
- Run the cluster source guard and review API misuse resistance, ownership/lifetime, crash consistency, hot-path cost,
  bounded background work, diagnostics, build closure, and sensitive-information handling before completion.
