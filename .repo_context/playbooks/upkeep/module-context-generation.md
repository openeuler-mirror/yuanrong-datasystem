# Module Context Generation Playbook

## Metadata

- Status:
  - `active`
- Feature scope:
  - `mixed`
- Owning module or area:
  - `.repo_context/`
- Primary code paths:
  - `.repo_context/README.md`
  - `.repo_context/index.md`
  - `.repo_context/maintenance.md`
  - `.repo_context/modules/*`
  - `.repo_context/playbooks/*`
  - `scripts/ai_context/generate_repo_index.py`
- Related module docs:
  - `../../README.md`
  - `../../index.md`
  - `../../decision-tree.md`
  - `../../maintenance.md`
- Related design docs:
  - none separate today; use `README.md` plus `maintenance.md`
- Related tests or validation entrypoints:
  - manual source-backed verification
  - `python3 scripts/ai_context/generate_repo_index.py` when source-tree index regeneration is needed
- Last verified against source:
  - `2026-04-17`

## Purpose

- Why this playbook exists:
  - standardize how to generate or backfill repo context so the result is formal, modular, and useful for retrieval,
    design, bugfix, and review work.
- What change class it standardizes:
  - explicit requests to generate module context, generate repo context, or backfill context for an existing area.
- What risks it is meant to reduce:
  - coarse one-off notes, under-split module boundaries, missing design/playbook layers, and partial context that fails
    the goals stated in `.repo_context/README.md`.

## When To Use This Playbook

- Use when:
  - the user asks to generate module context, repo context, or repository context for a named area;
  - the user asks to backfill or formalize context for an existing area;
  - a previous context pass was too coarse and needs to be split into canonical sibling modules.
- Do not use when:
  - the task is an ordinary code change with only incidental context edits;
  - the task is only to read or explain code without creating or restructuring `.repo_context/`.
- Escalate to design-first review when:
  - module boundaries are ambiguous across multiple domains;
  - a split would materially change navigation used by many existing docs;
  - the repository likely needs a new domain bucket rather than a new module inside an existing domain.

## Preconditions

- Required context to read first:
  - `../../README.md`
  - `../../index.md`
  - `../../maintenance.md`
  - relevant existing module docs for the requested area
- Required source files to inspect first:
  - the real source directories and tests for the requested area
  - relevant build files and startup or recovery entrypoints when present
- Required assumptions to verify before coding:
  - whether the user-named area is actually the right module boundary;
  - whether one or more sibling modules should be created instead;
  - whether the resulting context package would satisfy the goals in `.repo_context/README.md`.

## Request Intake

- Requested behavior change:
  - restate the requested context area in source-backed terms.
- Explicit non-goals:
  - record what the task does not need, such as editing product code or preserving obsolete context paths.
- Affected users, processes, or services:
  - AI coding tools and humans relying on `.repo_context/` for retrieval and design understanding.
- Backward-compatibility expectations:
  - `.repo_context/` itself should not keep compatibility redirects; navigation should move to the new canonical path.

## Risk Classification

| Risk Area | Question to answer before implementation | Low-risk signal | Escalation signal |
| --- | --- | --- | --- |
| module boundary | is the requested area already a clean module boundary? | one stable area with shared semantics | multiple sub-areas with distinct lifecycle or format semantics |
| completeness | would one module note alone satisfy retrieval and design goals? | small area with no separate lifecycle or DFX path | coarse note would hide real structure or design information |
| navigation | can canonical entrypoints be updated cleanly? | only a few local routing docs change | many docs, domains, or playbooks would need rerouting |
| maintenance | will the result be stable and reusable? | canonical docs replace temporary notes | result would leave placeholders, redirects, or duplicated truths |

## Source Verification Checklist

- [ ] confirm the actual source roots and key tests for the requested area
- [ ] confirm whether the area has its own persisted format, lifecycle, config, DFX, or test family
- [ ] confirm whether sibling modules are required by the module-identification rule
- [ ] confirm the canonical parent domain under `.repo_context/modules/<domain>/`
- [ ] confirm whether a matching feature playbook is required
- [ ] confirm whether index and routing docs must change

## Module Identification Checklist

- [ ] list candidate module roots from source paths, not only from the user phrase
- [ ] identify persisted formats, wire formats, schemas, or on-disk models
- [ ] identify startup, recovery, failover, or long-running lifecycle boundaries
- [ ] identify distinct config surfaces and operator-facing knobs
- [ ] identify distinct DFX workflows, logs, metrics, inject hooks, or test families
- [ ] split sibling modules when multiple signals cluster around one sub-area
- [ ] avoid splitting pure helper internals that add no independent retrieval value

## Formal Output Package

For an explicit context-generation task, aim to produce:

1. canonical module `README.md`
2. canonical module `design.md` when the area is architecture-meaningful
3. matching feature playbook when the area has recurring implementation or review workflow
4. sibling modules where the source structure requires them
5. machine-readable metadata under `.repo_context/modules/metadata/<module-id>.json`
6. updated navigation in `README.md`, `index.md`, local domain `README.md`, and playbook indexes
7. template or maintenance-rule updates if the task reveals a systemic gap

Do not leave compatibility redirects or partial placeholder docs behind.

## Implementation Plan

1. Restate the requested area and inspect its real source roots.
2. Run the module-identification checklist before writing any context file.
3. Choose the canonical module boundary and decide whether sibling modules are required.
4. Write the canonical module docs and matching playbooks needed for the chosen boundary.
5. Create or update the matching machine-readable module metadata record.
6. Update repository-level and local navigation to point to the canonical docs.
7. Remove obsolete context files instead of preserving compatibility shims.
8. Regenerate the source-tree index if required.
9. Validate module metadata and review the result against the goals in `.repo_context/README.md`.

## Guardrails

- Must preserve:
  - source-backed truthfulness;
  - clear canonical module boundaries;
  - retrievability from `README.md`, `index.md`, and local routing files.
- Must not do:
  - leave redirect-only compatibility docs inside `.repo_context/`;
  - stop after one coarse document when the area clearly contains multiple meaningful submodules;
  - preserve obsolete context paths only to avoid editing navigation.
- Must verify in source before claiming:
  - module boundaries;
  - build/test entrypoints;
  - lifecycle, recovery, and DFX claims.

## Validation Plan

- Fast checks:
  - verify the canonical docs are reachable from `README.md`, `index.md`, and the local domain `README.md`
  - verify each new module doc names real source files and tests
  - run `python3 scripts/ai_context/validate_module_metadata.py`
- Representative checks:
  - search `.repo_context/` for stale references to removed or replaced canonical docs
  - regenerate `generated/repo_index.*` when required by the maintenance rules
- Manual verification:
  - confirm a new reader can find the canonical doc without already knowing the old path
  - confirm the result is fine-grained enough for design and review, but not fragmented into helper-level noise
- Negative-path verification:
  - check that no redirect-only files remain after a canonical module split
  - check that the parent module no longer claims child-module details as its primary design content

## Review Checklist

- [ ] the module boundary was derived from source, not only from the request phrase
- [ ] sibling modules were created where lifecycle, format, or DFX boundaries required them
- [ ] the result includes `design.md` and matching playbook where needed
- [ ] the result includes canonical machine-readable module metadata
- [ ] navigation points to canonical docs only
- [ ] no obsolete compatibility context files remain
- [ ] the result satisfies the goals in `.repo_context/README.md`

## Context Update Requirements

- Module docs to update:
  - requested area plus any newly split sibling modules
- Metadata docs to update:
  - `.repo_context/modules/metadata/<module-id>.json`
- Design docs to update:
  - canonical `design.md` for architecture-meaningful areas
- Additional playbooks to update:
  - feature playbooks for resulting modules
  - this file if the workflow or rules evolve

## Open Questions

- whether `.repo_context/` should later gain machine-readable module metadata to support even more deterministic
  module splitting and retrieval.
