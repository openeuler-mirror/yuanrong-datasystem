---
name: ds-resolve-issue
description: Resolve a concrete GitCode issue end-to-end in openeuler/yuanrong-datasystem, from authenticated intake and sanitized task specification through source-backed diagnosis, implementation, review, validation, commit, fork push, and a conflict-checked PR. Use when the user asks to solve or fix a specific issue number or URL and carry it through to a PR.
---

# DS Resolve Issue

## Purpose

Treat issue resolution as one stateful workflow with explicit gates. Start from a concrete GitCode issue and finish
after an open PR is verified against the intended base branch. Do not continue into CI monitoring or review-comment
processing unless the user asks separately.

This skill is the canonical orchestrator for issue-to-PR work. It composes existing repository skills while owning the
cross-stage state, Git safety checks, stop conditions, and completion contract.

## Required Skill Sequence

Run the internal intake script during the intake stage, then read and follow each applicable child skill before executing
its stage:

1. `$ds-infra-engineering` for source-backed analysis, risk classification, implementation, and context upkeep.
2. `$ds-pr-review` for local review of the completed diff.
3. `$ds-test` for validation selection and any user-authorized test execution.
4. `$ds-self-verify` before declaring the branch or PR ready.
5. `$ds-create-pr` for template-compliant GitCode PR creation and conflict verification.

Do not invoke `$ds-pr-comment-proc` or monitor CI as part of this workflow.

## Visible Orchestration Contract

Skill references do not invoke themselves. Make every child-skill handoff visible and execute that skill's workflow
explicitly.

Before starting a stage, send:

```text
[ds-resolve-issue][<state>] invoking $<skill> — <one-line purpose>
```

After completing it, update `state.json`, then send:

```text
[ds-resolve-issue][<state>] complete — evidence: <artifact or command result>; next: <next state and skill>
```

When blocked, update `state.json` and send:

```text
[ds-resolve-issue][<state>] blocked — <reason>; next action: <one safe action>
```

Do not collapse multiple child skills into one generic progress update. Use this stage mapping:

| State | Child skill |
| --- | --- |
| `intake` | internal issue intake script |
| `diagnosis`, `implementation` | `$ds-infra-engineering` |
| `review` | `$ds-pr-review` |
| `validation` | `$ds-test` |
| `self-verification` | `$ds-self-verify` |
| `delivery` | `$ds-create-pr` |

At the start of a new or resumed run, report the issue, base branch, topic branch, completed state, and next child skill.

## Authorization Boundary

An explicit request to use this skill, solve an issue end-to-end, or fix an issue and submit a PR authorizes:

- creating a topic branch
- changing repository files required by the issue
- creating or amending one commit
- pushing the topic branch to the configured fork
- creating or updating one PR for that branch

It never authorizes pushing to `upstream`, exposing credentials or private infrastructure, changing unrelated files,
waiting for CI, merging the PR, or processing review comments.

## Workflow State Machine

Advance through
`preflight -> intake -> diagnosis -> implementation -> review -> validation -> self-verification -> delivery -> verified`.
Do not skip a state unless its output already exists and current source-backed evidence proves it is still valid.

Keep a sanitized `state.json` beside the intake output under the Git-internal task directory. Record only the issue
number, base branch, topic branch, completed state, active child skill, last evidence, blocker, last verified local/fork
SHA, and PR URL. Update it immediately after every completed or blocked state so an interrupted run can resume without
making task-owned work look like user-owned changes.

### 1. Preflight

1. Confirm that the working repository is `yuanrong-datasystem` and resolve the issue number or URL.
2. Run `git status --short`. On a new run, stop on any pre-existing change. On a resumed run, continue only when the
   saved state, current branch, and diff prove that every change belongs to this workflow.
3. Inspect remotes, confirm that `upstream` is the main repository and that a writable personal fork exists.
4. Verify GitCode access using the credential setup guidance from the invoked GitCode skill.
5. Fetch the latest `upstream/master` ref without modifying the current worktree.

Do not stash, reset, overwrite, or absorb user work. Never print, copy, or persist a token in tracked files.

### 2. Intake

Run the internal intake script and fetch only the requested issue. Save its sanitized task JSON under a Git-internal path,
for example:

```bash
python3 .skills/ds-resolve-issue/scripts/issue_intake.py \
  --issue <ISSUE_NUMBER_OR_URL> \
  --output "$(git rev-parse --git-path codex/ds-resolve-issue/issue-<number>/task.json)"
```

Keep raw issue data, sensitive logs, personal paths, and credentials out of the worktree. Stop and ask one focused
question if the issue is closed, contradictory, security-sensitive, or lacks enough information to choose a safe
behavior.

After intake:

1. Use an explicit user-selected base first, then a valid issue target branch, otherwise `upstream/master`.
2. Fetch the selected base ref without modifying the current worktree.
3. Derive a unique topic branch:
   - bug or unspecified issue: `fix/issue-<number>-<slug>`
   - documentation issue: `docs/issue-<number>-<slug>`
   - test-only issue: `test/issue-<number>-<slug>`
   - build-only issue: `build/issue-<number>-<slug>`
4. Stop if the local or fork branch name already exists and the saved state cannot prove that it belongs to this issue.

### 3. Diagnose

Announce the `diagnosis` handoff to `$ds-infra-engineering`.

Invoke `$ds-infra-engineering` before editing. Load the required `.repo_context` guidance, use CodeGraph first when the
repository is indexed, and verify important claims against source.

Record a compact working model:

- observed symptom and trigger
- root cause with source evidence
- owning module and smallest fix point
- regression guard and validation target
- hot-path, concurrency, persistence, recovery, security, and compatibility risk

Stop without creating a PR when the issue is already fixed on the base branch, cannot be reproduced or supported by
source evidence, or requires a materially different product decision.

### 4. Implement

Announce the `implementation` handoff to `$ds-infra-engineering`.

Create the topic branch from the verified base and implement the smallest source-backed change. Reuse existing helpers
and repository patterns. Add or update tests, public documentation, and `.repo_context` only when the behavior or
repository guidance changes.

Keep the diff scoped to the issue. Do not perform opportunistic refactors or modify unrelated user files.

### 5. Review

Announce the `review` handoff to `$ds-pr-review`.

Invoke `$ds-pr-review` on the local branch diff before validation. Review at least:

- correctness against the issue acceptance criteria
- API, ownership, lifecycle, concurrency, persistence, recovery, and hot-path effects where applicable
- build and test integration
- diagnosability and sensitive-information exposure
- `.repo_context` freshness

Resolve all blocking findings and repeat the review after material corrections.

### 6. Validate

Announce the `validation` handoff to `$ds-test`.

Invoke `$ds-test` to choose the narrowest relevant validation, then apply this static-first policy:

1. Always run `git diff --check`, relevant schema or metadata validators, and focused script checks.
2. Run compilation or tests only when the user explicitly requests them for this task.
3. Record unrun compilation and tests honestly in the PR; an unrun check does not block PR creation.
4. Treat every executed check failure as blocking. Diagnose and fix it, or stop with the exact failure and safe
   repository state.

Never describe an unrun check as passing.

### 7. Self-Verify

Announce the `self-verification` handoff to `$ds-self-verify`. Invoke it after the final diff and validation results are
available. Resolve every blocking finding before delivery and record the resulting evidence in `state.json`.

### 8. Commit And Push

Create one English conventional commit using the repository format, such as
`fix(worker): handle <specific issue behavior>`. If later corrections are required, amend the commit and push with
`--force-with-lease`.

Push only the topic branch to the personal fork. Verify that the fork branch SHA matches the local commit before
creating the PR.

### 9. Create And Verify The PR

Announce the `delivery` handoff to `$ds-create-pr`.

Invoke `$ds-create-pr` with conflict checking enabled. Build the PR body from the repository template and include:

- a concise issue-linked problem and fix summary
- the required `/kind` marker
- `Fixes #<number>`
- exact validation commands and outcomes
- explicitly unrun checks or known limitations

Create at most one PR for the topic branch. If one already exists, update and verify it instead of creating a duplicate.
If baseline drift causes a conflict, refresh from the base at most once, rerun required static validation and
self-verification, amend the commit, push with `--force-with-lease`, and recheck the PR.

Finish only after verifying that the PR is open, its base and head branches are correct, its head SHA matches the fork,
and it reports no merge conflict.

## Stop And Recovery Contract

Stop before the next side effect when any of these occurs:

- dirty starting worktree or ambiguous branch ownership
- missing GitCode credentials or writable fork
- closed, ambiguous, security-sensitive, already-fixed, or unsupported issue
- empty or unrelated diff
- unresolved review, static validation, sensitive-data, or executed-test failure
- commit, push, API, PR verification, or one-time conflict refresh failure

Preserve the safest recoverable state and report the current state, completed evidence, exact blocker, and one
actionable next step. Never conceal a failure to keep the workflow moving.

## Completion Report

Return a compact handoff containing:

- issue URL and root-cause/fix summary
- topic branch and commit SHA
- exact validation run, results, and unrun checks
- PR URL, base/head branches, and conflict status
- remaining limitations, if any

Stop after this report. Do not wait for CI or start processing review comments.
