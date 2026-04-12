# Context Maintenance Rules

This file defines how `.repo_context/` stays useful without drifting away from the source tree.

## Core Principles

- Treat `.repo_context/` as indexed working memory, not immutable truth.
- Verify behavior against source files, build scripts, tests, or generated artifacts before making strong claims.
- Prefer small, source-backed updates over large speculative writeups.
- If you touch a module and notice stale context, update it as part of the same task when practical.

## Source-Of-Truth Order

Use this priority order when resolving uncertainty:

1. Source code and build/test scripts in this repository.
2. Checked-in repository docs that are clearly tied to current code.
3. Generated repository index under `.repo_context/generated/`.
4. Curated module notes under `.repo_context/modules/<domain>/`.

If lower-priority context disagrees with higher-priority evidence, fix the lower-priority context.

## When Updates Are Required

Update `.repo_context/` when a change does any of the following:

- adds, removes, renames, or meaningfully repurposes a module or major directory;
- changes public API entrypoints, service boot paths, or deployment paths;
- changes build flags, required dependencies, or test execution commands;
- changes bug reproduction steps, observability/debug workflow, or review-critical interactions;
- reveals that existing context is outdated, ambiguous, or misleading.

## Minimum Update Policy

When a task affects repository understanding, do the minimum relevant update:

- structural change: regenerate `.repo_context/generated/repo_index.md` and `.json`;
- module behavior change: update the touched file in `.repo_context/modules/<domain>/`;
- new module with repeated future value: create a new `.repo_context/modules/<domain>/<module>.md` from the template;
- module needs stable architecture guidance for future feature work or refactors: create or update a `design.md` using `.repo_context/templates/module-design-template.md`;
- non-trivial or compatibility-sensitive feature work: reuse an existing feature playbook under `.repo_context/playbooks/<category>/...` or create/update one from `.repo_context/templates/feature-playbook-template.md`;
- navigation change: update `.repo_context/index.md` and related routing files when the best entrypoint changes;
- discovered stale claim: correct it and keep the statement narrow and source-backed.

## Authoring Rules

- Use explicit repository paths whenever possible.
- Separate verified facts from pending confirmation.
- Prefer short bullets and tables over long prose.
- Record commands exactly when describing build, test, or reproduction flows.
- Mark uncertain conclusions as `Pending verification` instead of overstating confidence.

## Recommended Update Workflow

1. Read the relevant context files for orientation.
2. Inspect source files for the touched area.
3. Make the code or documentation change.
4. Update the relevant module or playbook context if responsibilities, flows, or commands changed.
5. Update `.repo_context/index.md` if the best navigation route changed.
6. Run `python3 scripts/ai_context/generate_repo_index.py` if directory structure or indexable entrypoints changed.
7. Sanity-check generated diffs before finishing.

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
