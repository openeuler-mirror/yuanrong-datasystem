# Concurrency And Memory Safety Playbook

## Metadata

- Status:
  - `active`
- Feature scope:
  - locks, atomics, thread pools, async queues, callbacks, buffer ownership, and shared mutable state
- Owning module or area:
  - repository-wide concurrency and memory-safety gates
- Primary source paths:
  - `src/datasystem/common`
  - `src/datasystem/client`
  - `src/datasystem/worker`
  - `src/datasystem/master`
  - `tests`
- Last verified against source:
  - `2026-05-14`

## When To Use

Use this playbook when a change touches:

- `std::mutex`, `shared_mutex`, custom locks, atomics, condition variables, wait posts, or timers;
- thread pools, ordered queues, async task managers, callbacks, futures, promises, or background workers;
- shared memory, mmap, buffer lifetime, device memory, RDMA/P2P buffers, or payload ownership;
- maps, tables, object entries, metadata tables, or subscription state shared by multiple threads.

## Shared State Model

Before editing, write down the shared-state model:

| Field | Required answer |
| --- | --- |
| owner | class/module/thread responsible for lifetime |
| readers | threads or callbacks that read it |
| writers | threads or callbacks that mutate it |
| protection | mutex, atomic discipline, shard/slot lock, external sequencing, or immutable |
| lock order | order relative to other locks |
| blocking behavior | whether operations under lock can allocate, log, call RPC, or perform IO |
| shutdown | how waiters/tasks/callbacks stop safely |

## Ownership Rules

- Prefer values for local data and `unique_ptr` for singular ownership.
- Use `shared_ptr` only when ownership is genuinely shared; avoid repeated copies in tight loops.
- Avoid capturing references in async tasks unless the owner outlives the task by construction.
- Prefer RAII wrappers for resource cleanup and lock release.
- Keep callbacks from observing half-committed state.
- Make cancellation and destructor ordering explicit for background threads.

## Locking Rules

- Keep critical sections narrow.
- Do not hold broad locks while performing filesystem IO, RPC, sleeps, waits, logging bursts, or callbacks.
- Use shard/slot-level locks when the data is naturally partitioned.
- Document lock ordering when nested locks are unavoidable.
- Prefer `shared_lock` for read-mostly structures only when writer starvation and mutation semantics are acceptable.
- Avoid atomics for multi-field invariants unless the invariant is documented and testable.

## Memory And Buffer Rules

- Keep buffer ownership and lifetime visible across RPC, shared memory, RDMA, device, and persistence boundaries.
- Avoid extra data copies on get/set/transfer paths.
- Make offset, size, alignment, and bounds checks explicit.
- Treat raw pointers crossing async or device boundaries as review-critical.
- Do not free or reuse buffers before remote visibility or event completion is proven by the existing contract.

## Useful Searches

```bash
rtk rg -n "std::mutex|shared_mutex|lock_guard|unique_lock|shared_lock|atomic|condition_variable" src/datasystem
rtk rg -n "ThreadPool|OrderedThreadPool|Submit|Async|Future|Promise|Timer|WaitPost" src/datasystem tests
rtk rg -n "shared_ptr|unique_ptr|weak_ptr|reinterpret_cast|memcpy|mmap|Rdma|P2P|DeviceBuffer" src/datasystem
```

## Test Expectations

Use the smallest test that proves the invariant, then add broader stress or system coverage when concurrency ordering is
the feature:

- unit/component tests for ownership and state transitions;
- injected sleeps/failures for races;
- system tests for worker/client concurrency;
- repeated CTest runs for flaky race suspicion;
- sanitizer builds when available and practical from `.repo_context/modules/quality/build-test-debug.md`.

## Review Checklist

- [ ] Shared state has a named owner and protection mechanism.
- [ ] Lock ordering is documented or not needed.
- [ ] No blocking IO/RPC/wait under broad locks.
- [ ] Async captures are lifetime-safe.
- [ ] Shutdown drains or cancels pending work safely.
- [ ] Buffer ownership remains valid until consumers finish.
- [ ] Tests cover at least one conflicting or concurrent scenario when behavior depends on ordering.
