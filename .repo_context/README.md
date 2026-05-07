# Repository Context

`.repo_context/` is the canonical, repository-local context layer for AI coding tools working in `yuanrong-datasystem`.

Its goals are to:

- help feature work align with real module boundaries, build constraints, and security expectations;
- help bugfix work locate the right code paths and reproduction entrypoints faster;
- help code review reason about module interactions with less guesswork;
- improve code and knowledge retrieval with a stable index plus source verification rules;
- reduce repeated prompt tokens by centralizing durable repository context;
- support later generation of official documentation from the same curated context.

`.repo_context/` should converge toward one formal, complete, canonical context set for the current repository state.
It is not a place for long-lived compatibility shims, redirect stubs, or partial patch notes.

## Ground Rules

- Source code is the final source of truth. `.repo_context/` is an index and working memory layer, not a replacement for reading code.
- When `.repo_context/` conflicts with code, trust the code and update `.repo_context/` in the same change when practical.
- Keep the context coarse first, then deepen module-by-module. Avoid writing broad claims that have not been verified against source.
- Every module document should record what it knows, what files back it, and what still needs confirmation.
- For explicit repo-context generation or backfill tasks, do not stop at a patch-style note. Produce a formal,
  current-state module context package that can stand on its own.
- When a module area is created or substantially deepened, explicitly assess whether it also needs a module `design.md`
  and a matching feature playbook; for shared infra or compatibility-sensitive areas, treat both as required unless a
  narrow source-backed reason says otherwise.
- If a sub-area has its own persisted format, recovery lifecycle, availability or reliability model, or DFX workflow,
  split it into a sibling module context instead of burying it only inside a parent module note.
- Do not keep compatibility redirect docs inside `.repo_context/`. When a module split or canonical path changes,
  update navigation and delete obsolete context files instead of preserving old placeholders.
- When an implementation changes a touched module's responsibilities, entrypoints, or test path, update the relevant context files before finishing.

## Who Reads What

- `AGENTS.md`: entrypoint for Codex and tools that honor repository agent instructions.
- `CLAUDE.md`: entrypoint for Claude Code.
- `.cursor/rules/repo-context.mdc`: entrypoint for Cursor.
- `.repo_context/README.md`: shared human and machine overview.
- `.repo_context/index.md`: primary navigation page for finding the right context file fast.
- `.repo_context/decision-tree.md`: quick routing by task type or question type.
- `.repo_context/glossary.md`: repository terminology and naming hints.
- `.repo_context/maintenance.md`: maintenance and self-update rules.
- `.repo_context/roadmap.md`: staged rollout plan and persistent backlog.
- `.repo_context/generated/repo_index.md`: generated coarse repository index.
- `.repo_context/modules/<domain>/*.md`: curated module context, grouped by stable domain.
- `.repo_context/modules/metadata/*.json`: machine-readable canonical module registry and routing metadata.
- `.repo_context/playbooks/<category>/...`: task-oriented guidance for feature work, bugfix, review, and upkeep.

## Structure Model

`.repo_context/` now follows a five-layer model:

1. Entry layer: `AGENTS.md`, `CLAUDE.md`, `.cursor/rules/repo-context.mdc`
2. Navigation layer: `index.md`, `decision-tree.md`, `glossary.md`
3. Global index layer: `generated/repo_index.*`, `modules/overview/repository-overview.md`
4. Domain/module layer: `modules/<domain>/*.md`
5. Task/playbook layer: `playbooks/<category>/...`

## Current Structure

```text
.repo_context/
├── index.md
├── decision-tree.md
├── glossary.md
├── README.md
├── maintenance.md
├── roadmap.md
├── generated/
│   ├── repo_index.json
│   └── repo_index.md
├── modules/
│   ├── overview/
│   │   ├── README.md
│   │   ├── repository-overview.md
│   │   └── repository-skills.md
│   ├── client/
│   │   ├── README.md
│   │   └── client-sdk.md
│   ├── metadata/
│   │   ├── README.md
│   │   ├── client.client-sdk.json
│   │   ├── infra.common-infra.json
│   │   ├── infra.l2cache.json
│   │   ├── infra.logging.json
│   │   ├── infra.metrics.json
│   │   ├── infra.observability.json
│   │   ├── infra.slot.json
│   │   ├── overview.repository-overview.json
│   │   ├── overview.repository-skills.json
│   │   ├── quality.build-test-debug.json
│   │   ├── quality.cmake-build.json
│   │   ├── quality.tests-and-reproduction.json
│   │   ├── runtime.cluster-management.json
│   │   └── runtime.worker-runtime.json
│   ├── runtime/
│   │   ├── README.md
│   │   ├── worker-runtime.md
│   │   └── cluster-management.md
│   ├── infra/
│   │   ├── README.md
│   │   ├── common-infra.md
│   │   ├── l2cache/
│   │   │   ├── README.md
│   │   │   ├── design.md
│   │   │   ├── l2-cache-type.md
│   │   ├── slot/
│   │   │   ├── README.md
│   │   │   └── design.md
│   │   ├── logging/
│   │   │   ├── README.md
│   │   │   ├── design.md
│   │   │   ├── trace-and-context.md
│   │   │   ├── access-recorder.md
│   │   │   └── log-lifecycle-and-rotation.md
│   │   ├── metrics/
│   │   │   ├── README.md
│   │   │   ├── design.md
│   │   │   ├── resource-collector.md
│   │   │   ├── exporters-and-buffering.md
│   │   │   └── metric-families-and-registration.md
│   │   └── observability/
│   │       ├── README.md
│   │       ├── diagnosis-and-operations.md
│   │       ├── signal-map.md
│   │       ├── performance-troubleshooting.md
│   │       └── runtime-health-and-runbook.md
│   └── quality/
│       ├── README.md
│       ├── build-test-debug.md
│       ├── cmake-build/
│       │   ├── README.md
│       │   └── design.md
│       ├── tests-and-reproduction.md
│       └── test-suite-design.md
├── playbooks/
│   ├── README.md
│   ├── operations/
│   │   ├── README.md
│   │   ├── incident-triage.md
│   │   └── performance-investigation.md
│   ├── upkeep/
│   │   ├── README.md
│   │   ├── module-context-generation.md
│   │   └── skill-trigger-routing.md
│   └── features/
│       ├── README.md
│       ├── quality/
│       │   ├── README.md
│       │   ├── cmake-build-optimization.md
│       │   └── test-implementation.md
│       └── infra/
│           ├── README.md
│           ├── l2cache/
│           │   ├── README.md
│           │   └── implementation.md
│           ├── slot/
│           │   ├── README.md
│           │   └── implementation.md
│           └── logging/
│               ├── README.md
│               └── implementation.md
└── templates/
    ├── module-template.md
    ├── module-design-template.md
    ├── module-metadata-template.json
    └── feature-playbook-template.md
```

## Suggested Usage Flow For AI Tools

1. Read this file.
2. Read `.repo_context/index.md` to find the right domain document quickly.
3. Read `.repo_context/maintenance.md`.
4. Read `.repo_context/generated/repo_index.md` for coarse orientation if needed.
5. Read the relevant `.repo_context/modules/<domain>/*.md` or `.repo_context/playbooks/<category>/...`.
6. Confirm the answer or change against the actual source files before implementing, reviewing, or asserting behavior.
7. If you discover stale or missing context in the touched area, update the relevant context file and regenerate the index if structure changed.

## Current Coverage

The first pass is intentionally coarse. It currently covers:

- repository-wide governance for AI context maintenance;
- navigation for moving from broad repository orientation to domain documents;
- coarse module boundaries across source, SDK, CLI, docs, and tests;
- build, test, and debug entrypoints already used by this repository;
- CMake build-system context covering the supported source-build platform boundary, third-party dependency rules,
  compiler flags, install/package outputs, and target graph for compile-speed optimization;
- current `tests/` layout, CMake/gtest-to-CTest registration, CTest labels, Python unittest orchestration, example
  smoke-test flow, and test implementation workflow guidance;
- a generated file-tree index to support fast orientation;
- first-pass domain documents grouped under `overview`, `client`, `runtime`, `infra`, and `quality`;
- repository-local skill inventory and trigger routing for `.skills/`;
- a machine-readable module metadata layer describing canonical module ids, entry docs, source roots, and split signals;
- secondary-storage parent routing, backend selection, and a standalone slot storage and recovery module;
- an upkeep playbook for formal module-context generation and backfill;
- feature workflow guidance for l2 cache and secondary-storage changes;
- cross-module observability and operations guidance for diagnosis, health checks, and performance troubleshooting.

The next recommended deep dives are recorded in `.repo_context/roadmap.md`.
