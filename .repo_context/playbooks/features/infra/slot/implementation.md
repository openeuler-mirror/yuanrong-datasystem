# Slot Feature Implementation Playbook

## Metadata

- Status:
  - `active`
- Feature scope:
  - `mixed`
- Owning module or area:
  - `modules/infra/slot/`
- Primary code paths:
  - `src/datasystem/common/l2cache/slot_client/*`
  - `src/datasystem/worker/object_cache/slot_recovery/*`
  - `src/datasystem/worker/object_cache/slot_recovery_orchestrator.*`
  - `src/datasystem/worker/object_cache/service/worker_oc_service_migrate_impl.cpp`
  - `src/datasystem/worker/object_cache/worker_oc_service_impl.cpp`
- Related module docs:
  - `../../modules/infra/slot/README.md`
  - `../../modules/infra/l2cache/l2-cache-type.md`
- Related design docs:
  - `../../modules/infra/slot/design.md`
  - `../../modules/runtime/worker-runtime.md`
- Related tests or validation entrypoints:
  - `../../modules/quality/build-test-debug.md`
  - `../../modules/quality/tests-and-reproduction.md`
- Last verified against source:
  - `2026-04-17`

## Purpose

- Why this playbook exists:
  - standardize how to change slot persistence and recovery behavior without breaking persisted-format or recovery
    invariants.
- What change class it standardizes:
  - feature additions and compatibility-sensitive behavior changes inside distributed-disk slot storage and its worker
    recovery integrations.
- What risks it is meant to reduce:
  - replay corruption, manifest drift, unsafe takeover ordering, broken restart recovery, and weak DFX coverage.

## When To Use This Playbook

- Use when:
  - changing slot manifest, index, snapshot, writer, compactor, takeover planner, or file-root logic;
  - changing `MergeSlot()` or `PreloadSlot()` semantics;
  - changing worker startup repair, restart recovery, or failed-worker slot recovery behavior;
  - adding slot-specific DFX hooks, logs, or tests.
- Do not use when:
  - the task is only backend selection or OBS/SFS behavior with no slot-format change;
  - the task is a pure code-reading question with no proposed change.
- Escalate to design-first review when:
  - changing slot count or hash rules;
  - changing manifest field meanings, replay record semantics, or import-batch visibility rules;
  - changing source/target takeover ordering;
  - changing worker namespace derivation or recovery ordering;
  - changing operator-visible DFX expectations for recovery incidents.

## Preconditions

- Required context to read first:
  - `../../modules/infra/slot/design.md`
  - `../../modules/infra/slot/README.md`
  - `../../modules/infra/l2cache/l2-cache-type.md`
- Required source files to inspect first:
  - `src/datasystem/common/l2cache/slot_client/slot.cpp`
  - `src/datasystem/common/l2cache/slot_client/slot_manifest.cpp`
  - `src/datasystem/common/l2cache/slot_client/slot_index_codec.cpp`
  - `src/datasystem/common/l2cache/slot_client/slot_compactor.cpp`
  - `src/datasystem/common/l2cache/slot_client/slot_takeover_planner.cpp`
  - `src/datasystem/worker/object_cache/slot_recovery/slot_recovery_manager.cpp`
- Required assumptions to verify before coding:
  - whether the change touches persisted-format compatibility
  - whether the change affects worker recovery or migration ordering
  - whether DFX hooks or tests need to move with the behavior change

## Request Intake

- Requested behavior change:
  - write the target slot behavior in source-backed terms such as replay, compaction, takeover, or recovery.
- Explicit non-goals:
  - record what must not change, especially manifest semantics, hash rules, worker namespace derivation, or recovery
    ordering.
- Affected users, processes, or services:
  - distributed-disk workers, recovery executors, migration paths, and operators diagnosing slot incidents.
- Backward-compatibility expectations:
  - note whether existing slot directories, manifests, indexes, or recovery tasks must remain compatible.

## Risk Classification

| Risk Area | Question to answer before implementation | Low-risk signal | Escalation signal |
| --- | --- | --- | --- |
| persistence format | does the change avoid file-format or replay semantic changes? | no manifest/index meaning changes | manifest, index, or import-batch semantics change |
| recovery ordering | does restart or failed-worker flow stay intact? | request-path-only change | `PreloadSlot`, `MergeSlot`, or recovery sequencing changes |
| ownership | does worker namespace and slot ownership stay stable? | no path or hash changes | slot-count, path, or worker namespace derivation changes |
| observability | can failures still be diagnosed from logs/tests? | existing hooks remain valid | logs, inject points, or recovery evidence become weaker |
| performance | does the change avoid more hot-path IO or locking? | bounded work or background-only work | extra scans, fsyncs, or lock hold time on request paths |

## Source Verification Checklist

- [ ] confirm fixed slot-count and hash behavior in source
- [ ] confirm whether the change affects manifest, replay, compaction, or takeover
- [ ] confirm whether `MergeSlot()` or `PreloadSlot()` semantics change
- [ ] confirm whether worker startup or slot-recovery manager behavior changes too
- [ ] confirm representative tests exist for the affected path or record the gap explicitly
- [ ] confirm DFX hooks, logs, and on-disk artifacts still support diagnosis

## Design Checklist

- [ ] choose the narrowest slot layer that can implement the change
- [ ] record persisted-format invariants that must remain unchanged
- [ ] classify the change as one of: replay, write path, compaction, takeover, recovery, or DFX
- [ ] decide whether the change needs a rollout or migration note
- [ ] decide whether the change needs new inject hooks or stronger tests

## Implementation Plan

1. Write down the target slot behavior and non-goals in source-backed terms.
2. Read `slot/design.md` and inspect the exact slot and worker recovery files involved.
3. Classify the change using the risk table above.
4. If manifest, replay, takeover, or recovery ordering changes, update the design doc before code changes.
5. Implement the smallest viable change at the correct slot layer.
6. Validate the normal path plus at least one recovery or conflict path.
7. Update slot module docs, slot playbook, and any parent `l2cache` routing docs that changed.

## Guardrails

- Must preserve:
  - fixed slot hash rule and worker-namespace isolation
  - closed import-batch visibility semantics
  - source rename before target import publication during takeover
  - distributed-disk-only ownership of `MergeSlot()` and `PreloadSlot()`
- Must not change without explicit review:
  - manifest state machine meanings
  - index record semantics
  - recovery ordering between payload preload and metadata rebuild
  - DFX evidence needed to diagnose replay or recovery failures
- Must verify in source before claiming:
  - actual state-machine transitions in `slot.cpp`
  - actual recovery entrypoints in worker startup and `SlotRecoveryManager`
  - actual tests or inject points covering the changed path

## Validation Plan

- Fast checks:
  - build `common_slot_client` and affected worker object-cache targets
  - verify touched code compiles in both request and recovery entrypoints when relevant
- Representative tests:
  - `tests/ut/common/l2cache/slot_store_test.cpp`
  - `tests/ut/worker/object_cache/slot_worker_integration_test.cpp`
  - `tests/st/worker/object_cache/slot_end2end_test.cpp`
  - `tests/ut/worker/object_cache/slot_recovery_manager_test.cpp`
  - `tests/st/worker/object_cache/slot_recovery_manager_test.cpp`
- Manual verification:
  - inspect resulting manifest/index behavior for the changed path
  - confirm merge or preload still behaves correctly when relevant
  - confirm recovery logs and counters still make the path diagnosable
- Negative-path verification:
  - test one interrupted or conflicting path such as compaction-versus-takeover or torn-tail replay
  - test one restart or failed-worker recovery path if recovery behavior changed

## Review Checklist

- [ ] persisted-format and replay compatibility were reviewed
- [ ] worker recovery and migration coupling were rechecked against source
- [ ] slot ownership and path derivation remain stable
- [ ] DFX evidence remains strong enough for replay and recovery incidents
- [ ] `.repo_context` slot docs and playbooks were updated if structure or workflow changed

## Context Update Requirements

- Module docs to update:
  - `../../modules/infra/slot/README.md`
  - `../../modules/infra/slot/design.md`
  - `../../modules/infra/l2cache/README.md` if parent routing changes
- Design docs to update:
  - `../../modules/infra/slot/design.md`
- Additional playbooks to update:
  - this file when slot workflow or risk gates change

## Open Questions

- whether slot should eventually get a dedicated mixed-version rollout playbook if manifest or index evolution becomes
  frequent.
