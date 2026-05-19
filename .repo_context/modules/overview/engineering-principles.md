# Engineering Principles

## Scope

- Status:
  - `active`
- Last verified against source:
  - `2026-05-14`
- Canonical source roots:
  - `AGENTS.md`
  - `CLAUDE.md`
  - `.cursor/rules/repo-context.mdc`
  - `.repo_context/`
  - `.codex/`
  - `src/datasystem`
  - `include/datasystem`
  - `tests`
  - `.skills`
- Why this module exists:
  - define repository-level engineering standards for AI and human contributors;
  - keep performance, concurrency, memory safety, persistence, recovery, and review expectations visible before local
    implementation details;
  - route future feature, bugfix, and review tasks to the smallest relevant playbook.

## System Identity

`yuanrong-datasystem` is a high-performance distributed cache and infrastructure system. Treat SDK request paths,
worker/master runtime paths, metadata paths, transfer paths, persistence paths, and recovery paths as infrastructure
software. Do not use ordinary business-application defaults when changing this repository.

Source code remains the final source of truth. This document is a normative engineering gate; module documents and
playbooks add local source-backed detail.

## Priority Order

When goals conflict, use this order unless the touched module has a stricter documented rule:

1. correctness, data integrity, and recovery safety
2. availability and retry/idempotency safety
3. latency and throughput on request paths
4. bounded resource usage and predictable backpressure
5. maintainability through small, explicit module boundaries
6. cosmetic cleanup

Cosmetic cleanup never justifies risk to the first four items.

## Performance Baseline

Assume cache operations and worker-to-worker transfer paths can be hot until source inspection proves otherwise.

Before changing code, identify whether the path is:

| Path type | Examples | Required assessment |
| --- | --- | --- |
| foreground request path | `KVClient::Set/Get`, `ObjectClientImpl::Put/Get/MSet`, worker CRUD/get paths, remote get, batch get | latency, allocations, copies, logs, lock hold time, RPC/IO, bounded loops |
| metadata coordination path | ETCD/Metastore access, hash-ring updates, cluster-manager state, master metadata | write ordering, CAS/lease behavior, retries, fanout, failure mode |
| data movement path | shared memory, RDMA/UCX/P2P, spill/load, l2cache transfer, preload | data copies, buffer lifetime, alignment, backpressure, flush/wait semantics |
| background path | eviction, async metadata queue, compaction, recovery, metrics/log flushing | foreground interference, queue bound, retry storm, shutdown safety |

Avoid adding these to hot paths unless source-backed measurements or a narrow design reason exists:

- unnecessary abstractions or virtual dispatch in tight loops
- string formatting or string copies
- heap allocations and repeated container growth
- broad mutexes or lock nesting
- blocking IO or synchronous remote calls
- high-frequency `shared_ptr` churn
- request-path logging
- unbounded scans, unbounded queues, or unbounded retry loops
- frequent `unordered_map` rehash
- background work that can starve foreground requests

When using containers in hot paths, reserve capacity when size is known, keep complexity explicit, and prefer local value
lifetimes over shared ownership.

## Persistence And Recovery

For every stateful feature or bugfix, ask whether state must survive restart, failover, compaction, migration, or
preload. If the answer is no, record the reason when the change is not obviously ephemeral.

Persistence-sensitive changes must assess:

- write ordering and atomicity
- fsync or equivalent durability expectations
- partial write and corrupted record behavior
- version compatibility and rollback behavior
- compaction safety and cleanup after interruption
- startup rebuild correctness
- metadata/data consistency across ETCD, Metastore, RocksDB, l2cache, slot storage, and local memory
- idempotency when a retry or recovery task replays the operation

Use `.repo_context/playbooks/features/recovery-and-persistence.md` when a change touches durable data, metadata,
recovery, failover, migration, compaction, or preload.

## Concurrency And Memory Safety

All shared mutable state must have an explicit model:

- owner and lifetime
- protecting lock or atomic discipline
- lock ordering when more than one lock can be held
- visibility between producer/consumer threads
- shutdown and cancellation behavior
- mutation boundary for callbacks and async tasks

Prefer:

- RAII and scoped locks
- value types and `unique_ptr` where ownership is singular
- shard/slot-level locks over global locks
- bounded queues and explicit backpressure
- narrow critical sections with no blocking IO while locked
- clear object lifetime handoff for async work

Treat raw pointers, detached threads, captured references, shared futures, and `shared_ptr` cycles as review-critical
surfaces. Use `.repo_context/playbooks/features/concurrency-and-memory-safety.md` for changes in shared state,
threading, async queues, lock behavior, or buffer ownership.

## Reuse Before New Code

Before implementing new logic, search for existing repository primitives:

- status/error style and macros
- logging and perf point helpers
- thread pools, async queues, timers, wait posts, events
- filesystem, RocksDB, ETCD, Metastore, l2cache, and slot helpers
- benchmark and perf client frameworks
- test cluster, inject point, and recovery test utilities
- existing request/response validation helpers

Do not duplicate a framework because the local call site seems small. If a new helper is needed, keep its responsibility
narrow and place it near the owning module.

## Module And Function Shape

Keep module boundaries explicit:

- high cohesion, low coupling
- interface separation where consumers do not need implementation detail
- dependency direction that follows existing source layout
- explicit startup, shutdown, and recovery lifecycle

Functions should normally do one job. If a function grows beyond about 50 lines, assess whether it should be split into
validation, prepare, execute, commit, cleanup, rollback, or error-handling pieces. This is a review trigger, not a blind
mechanical rule.

## Constants And Magic Numbers

Only `0` and `1` may appear without explanation in new or modified production code unless the surrounding code already
uses a source-backed convention. Give other numbers names that describe their engineering meaning, not only their value.

Examples of acceptable names:

- timeout or interval constants tied to a retry or polling contract
- batch-size or queue-limit constants tied to resource bounds
- protocol or format constants tied to compatibility
- test constants that describe scenario scale

## Tests And Regression

Every non-trivial change must choose tests from the risk surface:

- unit or component tests for local invariants
- system tests for worker/master/client interactions
- recovery tests for restart, failover, partial progress, or compaction
- performance tests or perf logging for hot path changes
- regression tests with inject points when failure ordering matters

Use existing test registration and labels from `.repo_context/modules/quality/tests-and-reproduction.md`. If tests are
not practical, record the reason and the best available verification.

## Review Gate

Use `.repo_context/playbooks/reviews/pr-review-checklist.md` for review work and before opening a PR for changes that
touch runtime, persistence, recovery, concurrency, or hot paths.

Use `.repo_context/playbooks/upkeep/ai-self-verification.md` before claiming a change is complete.

## Pending Verification

- This document records repository-level engineering expectations. It does not assert that every current source file
  already satisfies these expectations.
- Future module deep dives should add source-backed local hot-path and recovery maps for `object_cache`, `stream_cache`,
  `rdma`, `shared_memory`, and `kvstore`.
