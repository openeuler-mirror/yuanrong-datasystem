# Context Maintenance Rules

This file defines how `.repo_context/` stays useful without drifting away from the source tree.

## Core Principles

- Treat `.repo_context/` as indexed working memory, not immutable truth.
- Verify behavior against source files, build scripts, tests, or generated artifacts before making strong claims.
- Prefer small, source-backed updates over large speculative writeups.
- If you touch a module and notice stale context, update it as part of the same task when practical.
- For explicit context-generation or backfill tasks, prefer a complete formalization pass over incremental patch notes.

## Source-Of-Truth Order

Use this priority order when resolving uncertainty:

1. Source code and build/test scripts in this repository.
2. Checked-in repository docs that are clearly tied to current code.
3. Generated repository index under `.repo_context/generated/`.
4. Curated module notes under `.repo_context/modules/<domain>/`.

If lower-priority context disagrees with higher-priority evidence, fix the lower-priority context.

Note:

- `.repo_context/generated/repo_index.*` is only a coarse source-tree index. It does not track whether a module's
  context coverage is complete or whether a module backfill already includes the right `design.md` and playbook.
- Treat coverage completeness as a responsibility of `.repo_context/README.md`, `.repo_context/index.md`, local module
  routing files, templates, and the backfill rules in this file.
- `.repo_context/modules/metadata/*.json` is the machine-readable canonical module registry. Keep it aligned with the
  canonical docs and module boundary decisions.

## When Updates Are Required

Update `.repo_context/` when a change does any of the following:

- adds, removes, renames, or meaningfully repurposes a module or major directory;
- changes public API entrypoints, service boot paths, or deployment paths;
- changes build flags, required dependencies, or test execution commands;
- changes bug reproduction steps, observability/debug workflow, or review-critical interactions;
- adds, removes, renames, or materially repurposes a repository-local skill under `.skills/`;
- changes which natural-language requests should auto-trigger a repository-local skill versus ask for confirmation first;
- reveals that existing context is outdated, ambiguous, or misleading.

## Minimum Update Policy

When a task affects repository understanding, do the minimum relevant update:

- structural change: regenerate `.repo_context/generated/repo_index.md` and `.json`;
- module behavior change: update the touched file in `.repo_context/modules/<domain>/`;
- new module with repeated future value: create a new `.repo_context/modules/<domain>/<module>.md` from the template;
- module needs stable architecture guidance for future feature work or refactors: create or update a `design.md` using `.repo_context/templates/module-design-template.md`;
- canonical module boundary or routing change: create or update `.repo_context/modules/metadata/<module-id>.json`;
- non-trivial or compatibility-sensitive feature work: reuse an existing feature playbook under `.repo_context/playbooks/<category>/...` or create/update one from `.repo_context/templates/feature-playbook-template.md`;
- navigation change: update `.repo_context/index.md` and related routing files when the best entrypoint changes;
- repository-local skill or routing change: update `modules/overview/repository-skills.md` and the matching upkeep playbook;
- discovered stale claim: correct it and keep the statement narrow and source-backed.

Exception:

- if the user explicitly asks to "generate context", "generate repo context", "补齐上下文", or perform a similar module
  context backfill task, treat it as a formalization task rather than a minimum-update task.

## Module Backfill And Deep-Dive Rule

When you create a new module context area or substantially deepen an existing one, do not stop at a single module note.
Explicitly assess all three layers below:

1. module overview or deep-dive doc under `.repo_context/modules/<domain>/...`
2. module-level `design.md` for architecture, invariants, and design-first change boundaries
3. matching feature playbook under `.repo_context/playbooks/<category>/...` for implementation workflow and risk gates
4. machine-readable metadata under `.repo_context/modules/metadata/<module-id>.json`

Required behavior:

- if the touched area is shared infra, persistence-sensitive, recovery-sensitive, compatibility-sensitive, or likely to
  be revisited for feature work, create or update both `design.md` and the matching playbook in the same task when
  practical;
- if one sub-area has its own persisted format, recovery lifecycle, availability or reliability model, or DFX workflow,
  split it into a sibling module context instead of leaving it only as a subsection inside the parent module;
- if you decide one of those layers is not needed yet, record a narrow, source-backed reason in the touched context
  instead of silently omitting it;
- if the canonical module boundary changes, update the matching metadata record in the same task;
- when a new `design.md` or playbook becomes the best entrypoint, update `README.md`, `index.md`, and the relevant
  local routing file together.

## Formalization Rule

`.repo_context/` should describe the current canonical repository state, not preserve compatibility breadcrumbs.

Required behavior:

- do not keep redirect-only or compatibility-only context files after a canonical module split or path change;
- when a new canonical module layout is chosen, update routing docs and remove obsolete context files in the same task;
- explicit module-context generation tasks should aim for a complete, source-backed module package rather than a
  partially useful intermediate state.

## Module Identification And Split Rule

When the user asks for context about a named area, do not assume the requested phrase is already the correct module
boundary. Re-evaluate the module boundary from source.

Promote a sub-area to a sibling module context when it satisfies multiple signals such as:

- a distinct code root or stable directory family;
- its own persisted format, wire format, or schema;
- its own startup, recovery, failover, or long-running lifecycle;
- its own config surface or operator-facing knobs;
- its own DFX workflow, logs, metrics, inject hooks, or debugging path;
- its own focused test families;
- design decisions that are meaningfully different from the parent module.

Do not split purely because a class is large. Keep a sub-area inside the parent module when it is only an internal
implementation detail and does not provide independent retrieval or design value.

## Context Generation Trigger Rule

Treat requests like the following as formal module-context generation tasks:

- "为 xxx 模块生成上下文"
- "补齐 xxx 的 repo context"
- "生成 xxx 的仓库上下文"
- "generate repo context for xxx"
- similar natural-language requests that clearly ask for repository context creation or backfill

Default required outputs for such tasks:

1. module boundary assessment
2. sibling-module split assessment when needed
3. canonical module `README.md`
4. canonical module `design.md` when the area is architecture-meaningful
5. matching playbook when the area has recurring implementation or review workflow
6. index, routing, and template updates when the best entrypoint changes

Do not stop after writing one coarse document if the resulting context would not satisfy the goals in
`.repo_context/README.md`.

## Definition Of Done For Explicit Context Generation

For explicit repo-context generation or module backfill tasks, done means:

- the canonical module boundary is explicit;
- sibling modules were created where the source structure requires them;
- the resulting docs are source-backed and usable for retrieval, design, bugfix, and review tasks;
- navigation points to the canonical docs;
- no obsolete compatibility context files remain.

## Authoring Rules

- Use explicit repository paths whenever possible.
- Separate verified facts from pending confirmation.
- Prefer short bullets and tables over long prose.
- Record commands exactly when describing build, test, or reproduction flows.
- Mark uncertain conclusions as `Pending verification` instead of overstating confidence.

## Recommended Update Workflow

1. Read the relevant context files for orientation.
2. Inspect source files for the touched area.
3. Identify the canonical module boundary from source, not only from the user phrase.
4. Split sibling modules when the module-identification rule says they deserve independent context.
5. Make the code or documentation change.
6. Update the relevant module or playbook context if responsibilities, flows, or commands changed.
7. Create or update the matching module metadata JSON when the canonical module boundary or routing changes.
8. Explicitly assess whether the touched module now also needs a `design.md` and a matching playbook.
9. Update `.repo_context/index.md` if the best navigation route changed.
10. Run `python3 scripts/ai_context/generate_repo_index.py` if directory structure or indexable entrypoints changed.
11. Run `python3 scripts/ai_context/validate_module_metadata.py` if module metadata changed.
12. Sanity-check generated diffs before finishing.

## Review-Time Rules

During review or question answering:

- use `.repo_context/` to narrow the search space;
- quote or summarize source-backed findings, not only context summaries;
- if context appears stale, say so and fix it when that is within scope.

## Scope Boundaries

`.repo_context/` should not:

- duplicate full API references already generated elsewhere;
- replace code comments, design docs, or user-facing manuals;
- invent architecture claims that are not yet confirmed from source.
