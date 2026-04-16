# Context Roadmap

This file persists the rollout plan so later context expansion follows one shared path instead of starting over.

## Phase 0

Status: in progress

Goal:

- create one canonical repository context directory;
- add thin entrypoints for Codex, Claude Code, and Cursor;
- generate a coarse repository index;
- write repository-wide maintenance rules so context can self-refresh;
- add a navigation layer so large-repo context can keep scaling.

Deliverables:

- `AGENTS.md`
- `CLAUDE.md`
- `.cursor/rules/repo-context.mdc`
- `.repo_context/index.md`
- `.repo_context/decision-tree.md`
- `.repo_context/glossary.md`
- `.repo_context/README.md`
- `.repo_context/maintenance.md`
- `.repo_context/generated/repo_index.{md,json}`

## Phase 1

Status: planned

Goal:

- write coarse but source-backed module summaries for the most frequently touched areas.

Recommended order:

1. `src/datasystem/client` and `include/datasystem/*`
2. `src/datasystem/worker`
3. `src/datasystem/common`
4. `tests` and reproducibility conventions
5. `cli` and deployment-related flows
6. `python/yr/datasystem`, `java`, `go`

Definition of done for each module document:

- states responsibilities and key subdirectories;
- lists entry files and integration neighbors;
- records how to build/test/debug the module;
- records known uncertainty and files to verify against.

Completed in this phase so far:

- `client-sdk.md`
- `worker-runtime.md`
- `tests-and-reproduction.md`
- `common-infra.md`
- `cluster-management.md`
- `logging/README.md`
- `logging/trace-and-context.md`
- `logging/access-recorder.md`
- `logging/log-lifecycle-and-rotation.md`
- `metrics/README.md`
- `metrics/resource-collector.md`
- `metrics/exporters-and-buffering.md`
- `metrics/metric-families-and-registration.md`

## Phase 2

Status: planned

Goal:

- deepen context for bugfix and review use cases.

Candidates:

- request/data path maps across client, worker, master, and storage layers;
- cluster management and metastore vs ETCD flows;
- cache semantics and reliability options;
- hetero object / transfer engine relationships;
- module-level review checklists and failure hotspots.

Likely next document splits:

- `client-sdk.md` -> `cpp-api`, `python-binding`, `connect-options-auth`
- `worker-runtime.md` -> `startup-lifecycle`, `worker-service`, `embedded-worker`
- `common-infra.md` -> `rpc-and-transport`, `shared-memory`, `kvstore-backends`, `metrics-and-logging`, `device-and-rdma`
- `cluster-management.md` -> `metadata-backends`, `hash-ring-and-routing`, `deployment-config-flow`

## Phase 3

Status: planned

Goal:

- reuse curated context for official documentation and higher-precision retrieval.

Candidates:

- align `.repo_context/modules/<domain>/` with `docs/source_zh_cn/` topics;
- produce machine-friendly metadata for search and retrieval;
- optionally introduce generated cross-links from code to curated docs.

## Persistent Backlog

- Add module owner or source-of-truth file references where stable enough.
- Record common reproduction commands for representative bug classes.
- Add a glossary for repository-specific terms.
- Decide whether to expose `.repo_context/` in the published docs site.
- Evaluate whether external tooling should supplement the generated index later.
- Formalize automatic module-identification heuristics so explicit context-generation tasks split modules at the right
  granularity by default.
- Evaluate whether `.repo_context/` should gain machine-readable module metadata to support deterministic routing and
  module splitting.

## Change Policy

When a later task deepens one module, update this roadmap if:

- the recommended order changes materially;
- a new recurring context gap appears;
- a finished phase can be marked complete.
