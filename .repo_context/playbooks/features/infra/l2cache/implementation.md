# L2 Cache Feature Implementation Playbook

## Metadata

- Status:
  - `active`
- Feature scope:
  - `mixed`
- Owning module or area:
  - `modules/infra/l2cache/`
- Primary code paths:
  - `src/datasystem/common/l2cache/persistence_api.*`
  - `src/datasystem/common/l2cache/object_persistence_api.*`
  - `src/datasystem/common/l2cache/aggregated_persistence_api.*`
  - `src/datasystem/common/l2cache/l2_storage.*`
  - `src/datasystem/common/l2cache/obs_client/*`
  - `src/datasystem/common/l2cache/sfs_client/*`
  - `src/datasystem/worker/worker_oc_server.cpp`
- Related module docs:
  - `../../modules/infra/l2cache/README.md`
  - `../../modules/infra/l2cache/l2-cache-type.md`
  - `../../modules/infra/slot/README.md`
- Related design docs:
  - `../../modules/infra/l2cache/design.md`
  - `../../modules/infra/slot/design.md`
- Related tests or validation entrypoints:
  - `../../modules/quality/build-test-debug.md`
  - `../../modules/quality/tests-and-reproduction.md`
- Last verified against source:
  - `2026-04-17`

## Purpose

- Why this playbook exists:
  - standardize how to change backend selection and shared L2 persistence behavior without accidentally mixing parent
    backend-routing work with standalone slot-format work.
- What change class it standardizes:
  - feature additions or compatibility-sensitive behavior changes under `common/l2cache` that affect backend selection,
    `PersistenceApi`, OBS, SFS, or shared cross-backend semantics.
- What risks it is meant to reduce:
  - wrong backend dispatch, path drift, unsupported-operation confusion, and accidental assumptions that OBS, SFS, and
    distributed-disk share identical semantics.

## When To Use This Playbook

- Use when:
  - adding or changing `l2_cache_type` behavior;
  - modifying `PersistenceApi`, `ObjectPersistenceApi`, `AggregatedPersistenceApi`, `ObsClient`, `SfsClient`, or
    shared L2 config semantics;
  - introducing new config flags or operational semantics for backend selection or object-per-version persistence.
- Do not use when:
  - the task is only to answer a code-reading question;
  - the change is primarily about slot manifest, replay, compaction, takeover, or recovery behavior.
- Prefer the slot playbook when:
  - `SlotClient`, slot manifest/index format, `MergeSlot()`, `PreloadSlot()`, restart recovery, or slot DFX are
    actually in scope.
- Escalate to design-first review when:
  - adding a new backend type;
  - changing `PersistenceApi::Create()` dispatch rules;
  - changing object-key encoding or backend root-path derivation;
  - changing caller-visible semantics that differ by backend.

## Preconditions

- Required context to read first:
  - `../../modules/infra/l2cache/design.md`
  - `../../modules/infra/l2cache/README.md`
  - `../../modules/infra/l2cache/l2-cache-type.md`
  - `../../modules/infra/slot/design.md` if distributed-disk slot internals are even potentially affected
- Required source files to inspect first:
  - `src/datasystem/common/l2cache/persistence_api.cpp`
  - `src/datasystem/common/l2cache/object_persistence_api.cpp`
  - `src/datasystem/common/l2cache/aggregated_persistence_api.cpp`
  - `src/datasystem/common/l2cache/l2_storage.cpp`
  - `src/datasystem/worker/worker_oc_server.cpp`
- Required assumptions to verify before coding:
  - which backend family the feature really belongs to;
  - whether the change is object-per-version, shared dispatch, or actually slot-specific;
  - whether the feature needs to preserve compatibility for existing OBS objects, SFS layout, or slot-facing caller
    contracts.

## Request Intake

- Requested behavior change:
  - write the target L2 behavior in one sentence using backend names and API terms from source.
- Explicit non-goals:
  - record what must not change, especially backend dispatch, unsupported-operation boundaries, or existing stored-data
    layout.
- Affected users, processes, or services:
  - workers, operators configuring L2 backends, and any caller depending on shared persistence semantics.
- Backward-compatibility expectations:
  - note whether existing OBS object keys, SFS directory layout, or distributed-disk caller contracts must remain
    compatible.

## Risk Classification

| Risk Area | Question to answer before implementation | Low-risk signal | Escalation signal |
| --- | --- | --- | --- |
| backend selection | does the change stay within one existing backend family? | only one backend implementation changes | `PersistenceApi::Create()` or validation rules change |
| compatibility | does any stored-path or caller contract change? | no path or contract changes | object key encoding, root path rules, or backend-facing API semantics change |
| recovery coupling | does the change alter slot recovery semantics? | no slot internals touched | `MergeSlot`, `PreloadSlot`, or slot lifecycle changes |
| operations | does the feature add new flags or deployment assumptions? | no new operator-facing knobs | new required flags, paths, or auth assumptions |
| performance | does the feature add synchronous work on request paths? | cost stays bounded or backend-local | extra scans, fsyncs, listing, or remote round-trips on hot paths |

## Source Verification Checklist

- [ ] confirm which backend types are accepted today
- [ ] confirm whether the change lands in `ObjectPersistenceApi`, `AggregatedPersistenceApi`, or only in dispatch
- [ ] confirm whether `distributed_disk_path`, `sfs_path`, or OBS config is the real path of impact
- [ ] confirm whether the slot playbook is actually the right workflow instead
- [ ] confirm whether tests already exist for the affected backend path
- [ ] confirm whether stored data layout or caller contracts must stay backward-compatible

## Design Checklist

- [ ] choose the narrowest backend or dispatch layer that can implement the feature
- [ ] classify the change as one of: backend selection, object-per-version behavior, shared persistence semantics
- [ ] record invariants that must stay unchanged across backends
- [ ] decide whether the feature needs a new flag, config gate, or migration note
- [ ] decide whether slot-facing workflow must be split out into the slot playbook

## Implementation Plan

1. Write down the target behavior and backend scope in source-backed terms.
2. Read `l2cache/design.md` plus the smallest relevant backend-specific doc.
3. Verify the actual dispatch and lifecycle entrypoints in source before editing.
4. Classify the change using the risk table above.
5. If the change touches backend selection or shared L2 semantics, update the parent design note first.
6. Implement the smallest viable code change at the correct backend layer.
7. Validate the target backend path and one negative or unsupported path.
8. If slot internals become part of the change, switch to the slot playbook and update that module's docs too.

## Guardrails

- Must preserve:
  - the distinction between object-per-version backends and the standalone slot module;
  - `distributed_disk_path` as the root for distributed-disk storage;
  - distributed-disk-only ownership of `MergeSlot()` and `PreloadSlot()` unless caller contracts change.
- Must not change without explicit review:
  - accepted `l2_cache_type` values;
  - key encoding rules for OBS and SFS persistence;
  - backend dispatch semantics in `PersistenceApi::Create()`.
- Must verify in source before claiming:
  - actual backend selection path in `PersistenceApi::Create()`;
  - whether worker lifecycle hooks stay unchanged or require the slot module workflow;
  - whether timeout, metrics, or success-rate reporting is really implemented for the affected backend.

## Validation Plan

- Fast checks:
  - build the affected `common/l2cache` target and any touched worker target
  - verify the touched code path compiles in the worker startup or request path that really uses it
- Representative tests:
  - `tests/st/common/sfs_client/sfs_persistence_api_test.cpp`
  - `tests/st/common/sfs_client/sfs_end2end_test.cpp`
  - `tests/st/common/obs_client/obs_persistence_api_test.cpp`
  - slot tests only when the parent contract to distributed-disk changes
- Manual verification:
  - confirm the selected backend is the one actually initialized;
  - confirm the intended save/get/delete path still works for the target backend;
  - confirm one unsupported or mismatched backend path still fails in the expected way.
- Negative-path verification:
  - switch to the slot playbook if distributed-disk slot behavior changed;
  - check one delete/list/get path if OBS or SFS behavior changed.

## Review Checklist

- [ ] backend family and dispatch coupling were verified against source
- [ ] any stored-path or caller-contract change was treated as compatibility-sensitive
- [ ] unsupported operations for OBS and SFS remain unsupported unless explicitly redesigned
- [ ] slot-specific changes were routed to the slot module workflow when relevant
- [ ] `.repo_context` docs were updated if module structure, design, or workflow changed

## Context Update Requirements

- Module docs to update:
  - `../../modules/infra/l2cache/README.md`
  - `../../modules/infra/l2cache/l2-cache-type.md`
  - `../../modules/infra/slot/README.md` if slot routing changes
- Design docs to update:
  - `../../modules/infra/l2cache/design.md`
- Additional playbooks to update:
  - `../slot/implementation.md` when slot internals are part of the change

## Open Questions

- whether the async send/delete pipeline should eventually get a dedicated companion playbook for end-to-end L2
  feature work.
