# Decision Tree

Use this file when you know the problem type but not the owning module.

## Feature Work

- If the change starts from a public SDK call:
  - read `modules/client/client-sdk.md`
- If the change touches worker startup, request handling, or service flags:
  - read `modules/runtime/worker-runtime.md`
- If the change depends on metadata backend or multi-node behavior:
  - read `modules/runtime/cluster-management.md`
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
- If the failure is in client init, auth, or SDK binding:
  - read `modules/client/client-sdk.md`
- If the failure appears only after worker start or during client registration:
  - read `modules/runtime/worker-runtime.md`
- If the failure mentions ETCD, Metastore, hash ring, node readiness, or failover:
  - read `modules/runtime/cluster-management.md`
- If the failure looks like transport, shared memory, persistence, logging, or metrics:
  - read `modules/infra/common-infra.md`

## Code Review

- API or behavior review:
  - `modules/client/client-sdk.md`
- lifecycle or service review:
  - `modules/runtime/worker-runtime.md`
- distributed coordination review:
  - `modules/runtime/cluster-management.md`
- infra dependency or side-effect review:
  - `modules/infra/common-infra.md`
- missing tests review:
  - `modules/quality/tests-and-reproduction.md`

## When Nothing Fits Cleanly

1. Read `modules/overview/repository-overview.md`
2. Read `generated/repo_index.md`
3. Open the closest module doc
4. Verify against source
5. If needed, create or refine a narrower module doc
