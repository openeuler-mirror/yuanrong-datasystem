# Infrastructure Engineering Workflow

## Metadata

- Status:
  - `active`
- Feature scope:
  - repository-wide feature, bugfix, refactor, and design work in infrastructure-sensitive code
- Owning module or area:
  - `.repo_context/modules/overview/engineering-principles.md`
- Primary source paths:
  - `AGENTS.md`
  - `CLAUDE.md`
  - `.cursor/rules/repo-context.mdc`
  - `.repo_context/modules/overview/engineering-principles.md`
  - `.repo_context/modules/**`
  - `.repo_context/playbooks/**`
- Last verified against source:
  - `2026-05-14`

## Purpose

Use this playbook when a task touches product code, module behavior, test behavior, build behavior, or a design decision
in `yuanrong-datasystem`.

This is the AI-native workflow for making small, source-backed changes without losing infrastructure discipline.

## Required Entry Flow

1. Read the active tool entrypoint:
   - Codex or compatible agents: `AGENTS.md`
   - Claude Code: `CLAUDE.md`
   - Cursor: `.cursor/rules/repo-context.mdc`
2. Read `.repo_context/README.md`, `.repo_context/index.md`, `.repo_context/maintenance.md`, and
   `.repo_context/generated/repo_index.md`.
3. Read `.repo_context/modules/overview/engineering-principles.md`.
4. Read the smallest relevant module document and feature/operation playbook from `.repo_context/index.md`.
5. Confirm behavior against source before making claims or edits.

## Pre-Change Checklist

- [ ] Identify the touched module owner and nearest source files.
- [ ] Decide whether the path is foreground, metadata coordination, data movement, background, or test-only.
- [ ] Search for existing helpers, status/error style, IO/persistence wrappers, thread pools, background task helpers,
      recovery helpers, and test utilities.
- [ ] Identify whether the change touches a hot path.
- [ ] Identify shared mutable state, ownership, and lifecycle assumptions.
- [ ] Identify whether persistence, recovery, compaction, failover, migration, or startup rebuild is affected.
- [ ] Choose the minimum test level that can catch the intended behavior.
- [ ] Decide whether `.repo_context` must be updated with the code change.

Useful search patterns:

```bash
rtk rg -n "Status |RETURN_IF_NOT_OK|CHECK_FAIL|INJECT_POINT|PerfPoint|ThreadPool|Timer" src tests
rtk rg -n "Recover|Recovery|Persist|Persistence|Compact|Replay|Restart|Failover|Preload" src tests
rtk rg -n "std::mutex|shared_mutex|lock_guard|unique_lock|atomic|shared_ptr|unique_ptr" src/datasystem
```

## Design Notes Before Editing

For non-trivial changes, write down the following in the working notes, PR body, or implementation plan:

| Question | Required answer |
| --- | --- |
| What path is touched? | foreground, metadata, transfer, background, test, or docs |
| What existing helper is reused? | exact helper or reason none fits |
| What is the hot-path impact? | allocation/copy/lock/IO/logging/complexity impact |
| What state is shared? | owner, lock, lifetime, shutdown behavior |
| What state is durable? | storage target, write order, recovery path |
| What verifies it? | exact test, build, benchmark, or reason for limited verification |

## Implementation Rules

- Keep changes small and traceable to the user request.
- Match existing style and ownership boundaries.
- Prefer local helpers over broad abstractions.
- Avoid adding new global state unless the lifecycle and shutdown path are explicit.
- Avoid logging in request paths unless it is rate-limited, already conventional, or error-only.
- Avoid blocking IO under locks.
- Avoid changing persistence formats without a compatibility and recovery plan.
- Avoid changing retry behavior without a bound and idempotency argument.
- Avoid test-only shortcuts leaking into production code.

## Verification Flow

1. Run the smallest targeted test that should catch the change.
2. Run broader tests when the change touches shared contracts, recovery, threading, or hot paths.
3. For context-only changes, run metadata and index checks when structure or metadata changed.
4. Use `.repo_context/playbooks/upkeep/ai-self-verification.md` before final status.

## Context Update Rules

Update `.repo_context` in the same task when the change affects:

- module responsibilities or boundaries
- startup, shutdown, recovery, or failover behavior
- public API or protocol behavior
- build flags, test commands, or reproduction workflow
- repository-local skills or their trigger routing
- a stale or misleading context claim discovered during the task

## Stop And Escalate Conditions

Stop and ask for direction, or explicitly narrow scope, when:

- the requested change requires a persistence format change without migration expectations;
- the change creates a new cross-module lifecycle dependency;
- correctness requires performance or recovery testing that cannot be run locally;
- source inspection contradicts `.repo_context` and the correct module boundary is unclear;
- the implementation requires broad refactoring unrelated to the requested behavior.

## Done Criteria

- Source-backed implementation or documentation change is complete.
- Relevant tests or validation commands were run, or the limitation is recorded.
- Hot-path, concurrency, and recovery risks were checked for the touched area.
- `.repo_context` navigation and skill routing are updated when needed.
- Final response reports exact verification evidence.
