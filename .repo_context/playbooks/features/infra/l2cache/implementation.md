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
  - `src/datasystem/common/l2cache/l2_storage.*`
  - `src/datasystem/common/l2cache/obs_client/*`
  - `src/datasystem/common/l2cache/sfs_client/*`
  - `src/datasystem/worker/worker_oc_server.cpp`
- Related module docs:
  - `../../modules/infra/l2cache/README.md`
  - `../../modules/infra/l2cache/l2-cache-type.md`
- Related design docs:
  - `../../modules/infra/l2cache/design.md`
- Related tests or validation entrypoints:
  - `../../modules/quality/build-test-debug.md`
  - `../../modules/quality/tests-and-reproduction.md`
- Last verified against source:
  - `2026-11-03`

## Purpose

- Why this playbook exists:
  - standardize backend selection and shared OBS/SFS persistence changes.
- What change class it standardizes:
  - feature additions or compatibility-sensitive behavior changes under `common/l2cache` that affect backend selection,
    `PersistenceApi`, OBS, SFS, or shared cross-backend semantics.
- What risks it is meant to reduce:
  - wrong backend dispatch, path drift, and incompatible stored-object layouts.

## When To Use This Playbook

- Use when:
  - adding or changing `l2_cache_type` behavior;
  - introducing new config flags or operational semantics for backend selection or object-per-version persistence.
- Do not use when:
  - the task is only to answer a code-reading question;
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
- Required source files to inspect first:
  - `src/datasystem/common/l2cache/persistence_api.cpp`
  - `src/datasystem/common/l2cache/object_persistence_api.cpp`
  - `src/datasystem/common/l2cache/l2_storage.cpp`
  - `src/datasystem/worker/worker_oc_server.cpp`
- Required assumptions to verify before coding:
  - which backend family the feature really belongs to;
  - whether stored OBS keys, SFS paths, or version/delete contracts change;

## Request Intake

- Requested behavior change:
  - write the target L2 behavior in one sentence using backend names and API terms from source.
- Explicit non-goals:
  - record what must not change, especially backend dispatch, unsupported-operation boundaries, or existing stored-data
    layout.
- Affected users, processes, or services:
  - workers, operators configuring L2 backends, and any caller depending on shared persistence semantics.
- Backward-compatibility expectations:
  - preserve existing OBS object keys and SFS directory layout.

## Risk Classification

| Risk Area | Question to answer before implementation | Low-risk signal | Escalation signal |
| --- | --- | --- | --- |
| backend selection | does the change stay within one existing backend family? | only one backend implementation changes | `PersistenceApi::Create()` or validation rules change |
| compatibility | does any stored-path or caller contract change? | no path or contract changes | object key encoding, root path rules, or backend-facing API semantics change |
| operations | does the feature add new flags or deployment assumptions? | no new operator-facing knobs | new required flags, paths, or auth assumptions |
| performance | does the feature add synchronous work on request paths? | cost stays bounded or backend-local | extra scans, fsyncs, listing, or remote round-trips on hot paths |

## Source Verification Checklist

- [ ] confirm which backend types are accepted today
- [ ] confirm whether tests already exist for the affected backend path
- [ ] confirm whether stored data layout or caller contracts must stay backward-compatible

## Design Checklist

- [ ] choose the narrowest backend or dispatch layer that can implement the feature
- [ ] classify the change as one of: backend selection, object-per-version behavior, shared persistence semantics
- [ ] record invariants that must stay unchanged across backends
- [ ] decide whether the feature needs a new flag, config gate, or migration note

## Implementation Plan

1. Write down the target behavior and backend scope in source-backed terms.
2. Read `l2cache/design.md` plus the smallest relevant backend-specific doc.
3. Verify the actual dispatch and lifecycle entrypoints in source before editing.
4. Classify the change using the risk table above.
5. If the change touches backend selection or shared L2 semantics, update the parent design note first.
6. Implement the smallest viable code change at the correct backend layer.
7. Validate the target backend path and one negative or unsupported path.

## Guardrails

- Must preserve:
  - object-per-version encoding, version fallback, and SFS rename-based publishing.
- Must not change without explicit review:
  - accepted `l2_cache_type` values;
  - key encoding rules for OBS and SFS persistence;
  - backend dispatch semantics in `PersistenceApi::Create()`.
- Must verify in source before claiming:
  - actual backend selection path in `PersistenceApi::Create()`;
  - whether timeout, metrics, or success-rate reporting is really implemented for the affected backend.

## Validation Plan

- Fast checks:
  - build the affected `common/l2cache` target and any touched worker target
  - verify the touched code path compiles in the worker startup or request path that really uses it
- Representative tests:
  - `tests/st/common/sfs_client/sfs_persistence_api_test.cpp`
  - `tests/st/common/sfs_client/sfs_end2end_test.cpp`
  - `tests/st/common/obs_client/obs_persistence_api_test.cpp`
- Manual verification:
  - confirm the selected backend is the one actually initialized;
  - confirm the intended save/get/delete path still works for the target backend;
  - confirm one unsupported or mismatched backend path still fails in the expected way.
- Negative-path verification:
  - check one delete/list/get path if OBS or SFS behavior changed.

## Review Checklist

- [ ] backend family and dispatch coupling were verified against source
- [ ] any stored-path or caller-contract change was treated as compatibility-sensitive
- [ ] unsupported operations for OBS and SFS remain unsupported unless explicitly redesigned
- [ ] `.repo_context` docs were updated if module structure, design, or workflow changed

## Context Update Requirements

- Module docs to update:
  - `../../modules/infra/l2cache/README.md`
  - `../../modules/infra/l2cache/l2-cache-type.md`
- Design docs to update:
  - `../../modules/infra/l2cache/design.md`
## Open Questions

- whether the async send/delete pipeline should eventually get a dedicated companion playbook for end-to-end L2
  feature work.

## Manual Scale-In Regression

The parameterized `ManualL2ScaleDown/LEVEL1_OCL2ScaleDownTest` cases in
`tests/st/client/object_cache/oc_client_scale_down_migrate_target_test.cpp` cover OBS and SFS write-back payloads
with and without a surviving copy. They launch three Workers and a Coordinator and exceed the default fast-test
budget; they are disabled by default and require explicit manual execution:

```bash
./build/tests/st/ds_st_object_cache --gtest_also_run_disabled_tests \
  --gtest_filter='ManualL2ScaleDown/LEVEL1_OCL2ScaleDownTest.*'
```

Use the actual ST output directory from the selected build. Check both parameter instances, source exit,
remaining membership, and post-exit payload equality. This does not replace restart recovery or persistence-failure
injection coverage. Master topology classification UTs alone are not runtime validation.
