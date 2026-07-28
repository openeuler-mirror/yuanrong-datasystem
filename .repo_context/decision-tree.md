# Decision Tree

Use this file when you know the problem type but not the owning module.

## Feature Work

- For any non-trivial feature, bugfix, refactor, or design change:
  - read `modules/overview/engineering-principles.md`
  - read `playbooks/features/infra-engineering-workflow.md`
- If the change adds, moves, labels, or reviews tests:
  - read `playbooks/features/quality/test-implementation.md`
- If the change can affect latency, throughput, allocations, copies, locks, IO, or background work:
  - read `playbooks/features/performance-change.md`
- If the change touches locks, async queues, callbacks, atomics, shared state, or buffers:
  - read `playbooks/features/concurrency-and-memory-safety.md`
- If the change touches durable state, metadata, recovery, failover, compaction, migration, or preload:
  - read `playbooks/features/recovery-and-persistence.md`
- If the change starts from a public SDK call:
  - read `modules/client/client-sdk.md`
- If the change touches worker startup, request handling, or service flags:
  - read `modules/runtime/worker-runtime.md`
- If the change concerns current Coordinator election, braft, Raft lifecycle, recovery, or one-operation membership changes:
  - read `modules/runtime/coordinator-election/README.md`
- If the change designs or extends Coordinator election or product integration:
  - read `modules/runtime/coordinator-election/design.md`
  - follow `playbooks/features/runtime/coordinator-election/implementation.md`
- If the change concerns ETCD, Metastore, watch, keepalive, lease, CAS, or metadata-backend behavior:
  - read `modules/runtime/etcd-metadata/README.md`
  - for design or implementation changes, also read `modules/runtime/etcd-metadata/design.md` and
    `playbooks/features/runtime/etcd-metadata/implementation.md`
- If the change concerns topology, worker membership, routing, scale transitions, controller recovery, or failover:
  - read `modules/runtime/topology/README.md`
  - for implementation changes, also follow `playbooks/features/runtime/topology/implementation.md`
- If the change mostly affects plumbing used by many modules:
  - read `modules/infra/common-infra.md`

## Repo Context Generation

- If the task is to generate or backfill repo context for a named area:
  - read `playbooks/upkeep/module-context-generation.md`
- If the named area might actually contain multiple meaningful submodules:
  - use the split rules in `playbooks/upkeep/module-context-generation.md` before writing any module doc
- If the area is shared infra and includes its own persisted format, recovery lifecycle, or DFX workflow:
  - expect sibling modules instead of one coarse parent note

## Bugfix

- If you need to know what tests to run first:
  - read `modules/quality/tests-and-reproduction.md`
- If the failure depends on generated CTest names, labels, disabled tests, or serial execution:
  - read `modules/quality/test-suite-design.md`
- If the failure is in client init, auth, or SDK binding:
  - read `modules/client/client-sdk.md`
- If the failure appears only after worker start or during client registration:
  - read `modules/runtime/worker-runtime.md`
- If the failure concerns Coordinator Leader election, braft startup, Raft recovery, callback lifetime, shutdown, or
  `AddPeer` / `RemovePeer`:
  - read `modules/runtime/coordinator-election/README.md`
  - use `modules/runtime/coordinator-election/design.md` for lifecycle and recovery invariants
- If the failure mentions ETCD, Metastore, watch, lease, CAS, or metadata-backend availability:
  - read `modules/runtime/etcd-metadata/README.md`
- If the failure mentions topology, hash ring, worker membership, routing, scale, node readiness, controller recovery, or
  failover:
  - read `modules/runtime/topology/README.md`
- If the failure looks like transport, shared memory, persistence, logging, or metrics:
  - read `modules/infra/common-infra.md`

## Code Review

- Start with:
  - `playbooks/reviews/pr-review-checklist.md`
- API or behavior review:
  - `modules/client/client-sdk.md`
- Worker lifecycle or service review:
  - `modules/runtime/worker-runtime.md`
- Coordinator election, braft lifecycle, recovery, callback, or membership-operation review:
  - start with `modules/runtime/coordinator-election/README.md`
  - verify invariants in `modules/runtime/coordinator-election/design.md`
  - check workflow and validation gates in `playbooks/features/runtime/coordinator-election/implementation.md`
- ETCD or Metastore coordination review:
  - `modules/runtime/etcd-metadata/README.md`
  - `modules/runtime/etcd-metadata/design.md`
- Topology, worker membership, routing, scale, recovery, or failover review:
  - `modules/runtime/topology/README.md`
  - `playbooks/features/runtime/topology/implementation.md`
- Infra dependency or side-effect review:
  - `modules/infra/common-infra.md`
- Missing tests review:
  - `modules/quality/tests-and-reproduction.md`

## When Nothing Fits Cleanly

1. Read `modules/overview/repository-overview.md`
2. Read `generated/repo_index.md`
3. Open the closest module doc
4. Verify against source
5. If needed, create or refine a narrower module doc
