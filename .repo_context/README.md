# Repository Context

`.repo_context/` is the canonical, repository-local context layer for AI coding tools working in `yuanrong-datasystem`.

Its goals are to:

- help feature work align with real module boundaries, build constraints, and security expectations;
- help bugfix work locate the right code paths and reproduction entrypoints faster;
- help code review reason about module interactions with less guesswork;
- improve code and knowledge retrieval with a stable index plus source verification rules;
- reduce repeated prompt tokens by centralizing durable repository context;
- support later generation of official documentation from the same curated context.

## Ground Rules

- Source code is the final source of truth. `.repo_context/` is an index and working memory layer, not a replacement for reading code.
- When `.repo_context/` conflicts with code, trust the code and update `.repo_context/` in the same change when practical.
- Keep the context coarse first, then deepen module-by-module. Avoid writing broad claims that have not been verified against source.
- Every module document should record what it knows, what files back it, and what still needs confirmation.
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
│   │   └── repository-overview.md
│   ├── client/
│   │   ├── README.md
│   │   └── client-sdk.md
│   ├── runtime/
│   │   ├── README.md
│   │   ├── worker-runtime.md
│   │   └── cluster-management.md
│   ├── infra/
│   │   ├── README.md
│   │   ├── common-infra.md
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
│       └── tests-and-reproduction.md
├── playbooks/
│   ├── README.md
│   ├── operations/
│   │   ├── README.md
│   │   ├── incident-triage.md
│   │   └── performance-investigation.md
│   └── features/
│       ├── README.md
│       └── infra/
│           ├── README.md
│           └── logging/
│               ├── README.md
│               └── implementation.md
└── templates/
    ├── module-template.md
    ├── module-design-template.md
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
- a generated file-tree index to support fast orientation;
- first-pass domain documents grouped under `overview`, `client`, `runtime`, `infra`, and `quality`;
- cross-module observability and operations guidance for diagnosis, health checks, and performance troubleshooting.

The next recommended deep dives are recorded in `.repo_context/roadmap.md`.
