# Hash Ring Implementation Playbook

## Metadata

- Status:
  - `active`
- Feature scope:
  - mixed
- Owning module or area:
  - `runtime.hash-ring`
- Primary code paths:
  - `src/datasystem/worker/hash_ring`
  - `src/datasystem/protos/hash_ring.proto`
  - `src/datasystem/worker/cluster_manager/etcd_cluster_manager.*`
- Related module docs:
  - `.repo_context/modules/runtime/hash-ring/README.md`
  - `.repo_context/modules/runtime/cluster-management.md`
- Related design docs:
  - `.repo_context/modules/runtime/hash-ring/design.md`
- Related tests or validation entrypoints:
  - `tests/ut/worker/object_cache/hash_ring_allocator_test.cpp`
  - `tests/ut/worker/object_cache/hash_ring_health_check_test.cpp`
  - `tests/st/worker/object_cache/hash_ring_test.cpp`
  - `tests/st/client/kv_cache/kv_client_hashring_healing_test.cpp`
- Last verified against source:
  - `2026-05-08`

## Purpose

- Why this playbook exists:
  - hash-ring changes cross persisted ETCD state, routing, migration, cleanup, and worker lifecycle boundaries.
- What change class it standardizes:
  - changes to ring initialization, add/remove node algorithms, voluntary scale-down, passive deletion, restart recovery,
    other-AZ routing, health-check repair, or `HashRingPb`.
- What risks it is meant to reduce:
  - CAS storms, lost migration tasks, stale route maps, accidental metadata loss, and incompatible protobuf changes.

## When To Use This Playbook

- Use when:
  - modifying `src/datasystem/worker/hash_ring/*`;
  - changing `src/datasystem/protos/hash_ring.proto`;
  - changing cluster-manager behavior that calls `HashRing::Init*`, `HandleRingEvent`, `RemoveWorkers`, or routing APIs;
  - changing object/stream metadata subscriber behavior for hash-ring events.
- Do not use when:
  - changing only ETCD client mechanics with no hash-ring contract impact; use the ETCD context instead.
- Escalate to design-first review when:
  - the change alters `HashRingPb` compatibility;
  - the change changes when workers are erased from the ring;
  - the change increases CAS frequency or writer fanout;
  - the change modifies self-healing deletion/repair policy.

## Preconditions

- Required context to read first:
  - `.repo_context/modules/runtime/hash-ring/README.md`
  - `.repo_context/modules/runtime/hash-ring/design.md`
  - `.repo_context/modules/runtime/cluster-management.md`
- Required source files to inspect first:
  - `hash_ring.cpp`, `hash_ring_allocator.cpp`, `hash_ring_task_executor.cpp`, `hash_ring_health_check.cpp`,
    `hash_ring_event.h`, `hash_ring.proto`
- Required assumptions to verify before coding:
  - whether the change affects persisted ring state or only local derived maps;
  - whether `add_node_info` or `del_node_info` lifecycle changes;
  - which `HashRingEvent` subscribers observe the change;
  - whether ETCD CAS/write frequency changes.

## Risk Classification

| Risk Area | Question to answer before implementation | Low-risk signal | Escalation signal |
| --- | --- | --- | --- |
| persisted schema | does `HashRingPb` change? | no schema or field semantic change | new/repurposed field, changed range meaning |
| CAS pressure | does writer count or retry loop change? | same or fewer CAS writes | more periodic writes or more candidate writers |
| migration correctness | can a range be marked finished early? | completion still follows subscriber success | task record cleared before migration/cleanup |
| route consistency | do derived maps still match `HashRingPb`? | maps rebuilt only from ring state | side state bypasses `UpdateTokenMap` |
| failure behavior | what happens if source/dest dies mid-task? | covered by existing add/del recovery tests | new partial failure state |
| operations | can health-check self-healing delete more state? | logs only or narrower repair | broader worker/task erasure |

## Source Verification Checklist

- [ ] confirm the exact `HashRingPb` fields touched
- [ ] confirm CAS callbacks compare old local ring to ETCD ring when needed
- [ ] confirm range convention, especially wraparound and `from == end`
- [ ] confirm event subscribers and their return statuses
- [ ] confirm restart recovery through `RestoreScalingTaskIfNeeded`
- [ ] confirm interaction with `enable_distributed_master`, `auto_del_dead_node`, and `enable_hash_ring_self_healing`
- [ ] confirm multi-replica and non-multi-replica branches if the path reaches `HashRingTaskExecutor`

## Implementation Plan

1. Identify whether the change is algorithm-only, state-machine, persisted-schema, or cross-module event behavior.
2. Read the current flow in `HashRing`, `HashRingAllocator`, and `HashRingTaskExecutor`; do not change one in isolation if task records are involved.
3. Preserve existing CAS conflict handling and local-vs-ETCD ring comparison unless replacing it with a documented stronger guard.
4. Add or update focused allocator/health-check tests for pure state transitions.
5. Add or update system tests when ETCD watches, worker lifecycle, or metadata migration behavior changes.
6. Update `.repo_context/modules/runtime/hash-ring/*` if behavior, invariants, or test paths change.

## Guardrails

- Must preserve:
  - full-ring protobuf parse/serialize compatibility;
  - migration completion after subscriber success;
  - terminal nature of local `FAIL` state;
  - `from == end` as UUID metadata migration marker;
  - update-map retention while a worker is waiting for UUID restoration.
- Must not change without explicit review:
  - when workers are erased from `HashRingPb.workers`;
  - self-healing policy that deletes task records;
  - writer-candidate count or periodic CAS cadence;
  - `key_with_worker_id_meta_map` semantics.
- Must verify in source before claiming:
  - object-cache/stream-cache event behavior;
  - multi-replica DB-primary routing behavior;
  - cross-AZ read-ring behavior.

## Validation Plan

- Fast checks:
  - `./build.sh -t ut -- -R hash_ring_allocator_test`
  - `./build.sh -t ut -- -R hash_ring_health_check_test`
- Representative tests:
  - `tests/st/worker/object_cache/hash_ring_test.cpp`
  - `tests/st/client/kv_cache/kv_client_hashring_healing_test.cpp`
  - affected scale, voluntary-scale-down, cross-AZ, or replica tests under `tests/st/client/kv_cache`
- Manual verification:
  - inspect `/datasystem/ring` before and after the scenario;
  - confirm `add_node_info` and `del_node_info` drain as expected;
  - confirm active workers have non-empty UUIDs and tokens.
- Negative-path verification:
  - source worker fails during scale-up;
  - destination/joining worker fails during scale-up;
  - worker restarts with `update_worker_map`;
  - health-check sees stale ring with self-healing off and on.

## Review Checklist

- [ ] `HashRingPb` compatibility and migration semantics are explicit
- [ ] CAS writes remain bounded and justified
- [ ] route maps are rebuilt from authoritative state
- [ ] failure/restart paths were checked
- [ ] health-check behavior remains safe
- [ ] context docs were updated if behavior changed

## Context Update Requirements

- Module docs to update:
  - `.repo_context/modules/runtime/hash-ring/README.md`
- Design docs to update:
  - `.repo_context/modules/runtime/hash-ring/design.md`
- Additional playbooks to update:
  - this file if workflow or risk gates change
