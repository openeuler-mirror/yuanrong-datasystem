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
  - `2026-06-09`

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
- [ ] Map the user request, PR description, design document, or review comment to the exact code, test, docs, config, and
      build artifacts that must change.
- [ ] Decide whether the path is foreground, metadata coordination, data movement, background, or test-only.
- [ ] Search for existing helpers, status/error style, IO/persistence wrappers, thread pools, background task helpers,
      recovery helpers, and test utilities.
- [ ] Identify the owning abstraction for the behavior and whether the change keeps responsibilities high-cohesion and
      low-coupling.
- [ ] Search for repeated or scattered logic in the touched module and decide whether it should be reused, deleted, or
      centralized before adding new code.
- [ ] Identify every affected config, startup/runtime update, local/remote, SDK, worker, embedded, dscli, K8s/Helm, and
      documentation surface.
- [ ] Identify affected build systems and packaging paths, including Bazel, CMake, installed/public headers, generated
      files, and package manifests.
- [ ] Identify whether the change touches a hot path.
- [ ] Identify shared mutable state, ownership, and lifecycle assumptions.
- [ ] Identify whether persistence, recovery, compaction, failover, migration, or startup rebuild is affected.
- [ ] Choose the minimum test level that can catch the intended behavior.
- [ ] Estimate whether new or modified default tests can complete within 8 seconds and whether they introduce sleep,
      randomness, wall-clock, scheduling, or shared-global-state flakiness.
- [ ] Check that planned source, tests, scripts, docs, PR text, and generated artifacts will not expose secrets, private
      endpoints, local paths, account data, or sensitive payloads.
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
| What request, design, PR, or review claim is being satisfied? | exact claim and source-backed artifact that proves it |
| What path is touched? | foreground, metadata, transfer, background, test, or docs |
| What abstraction owns the behavior? | owning class/module/helper and why the caller should or should not see the detail |
| What existing helper is reused? | exact helper or reason none fits |
| What duplicate or scattered logic is removed or avoided? | exact repeated code, or reason no duplication exists |
| What public/config/docs surfaces change? | API, embedded, dscli, K8s/Helm, docs, compatibility, or none |
| What build and packaging surfaces change? | CMake, Bazel, public headers, generated files, package manifests, or none |
| What is the hot-path impact? | allocation/copy/lock/IO/logging/complexity impact |
| What state is shared? | owner, lock, lifetime, shutdown behavior |
| What state is durable? | storage target, write order, recovery path |
| What verifies it? | exact test, build, benchmark, test runtime expectation, or reason for limited verification |

## Implementation Rules

- Keep changes small and traceable to the user request.
- Match existing style and ownership boundaries.
- Maintain high cohesion and low coupling: keep module responsibilities explicit, keep infrastructure details inside the
  owning abstraction, and do not make unrelated callers coordinate internal policy.
- Reuse before adding new mechanisms: prefer existing helpers, status patterns, config plumbing, thread pools, test
  fixtures, and build targets unless source inspection shows they do not fit.
- Remove or centralize duplicated logic when it belongs to the touched behavior. Do not leave scattered patch-like code,
  dead compatibility shims, or unused wrappers around the new implementation.
- Prefer local helpers over broad abstractions.
- Keep design and config semantics complete across relevant paths: startup, dynamic update, local/remote, SDK, worker,
  embedded, explicit defaults, derived defaults, validation, idempotency, and compatibility.
- Keep public interfaces and operator surfaces complete in the same change: public C++/embedded APIs, dscli, K8s/Helm,
  config docs, examples, generated docs, and compatibility notes must describe effective semantics, not only names.
- Keep build systems and packaging synchronized. When a touched area supports both CMake and Bazel, update both; do not
  rely on transitive headers or header-only dependencies when implementation objects are required.
- Avoid adding new global state unless the lifecycle and shutdown path are explicit.
- Avoid logging in request paths unless it is rate-limited, already conventional, or error-only.
- Avoid blocking IO under locks.
- Avoid changing persistence formats without a compatibility and recovery plan.
- Avoid changing retry behavior without a bound and idempotency argument.
- Avoid test-only shortcuts leaking into production code.
- Keep default tests short and deterministic. Do not add or leave default tests expected to run longer than 8 seconds
  unless they are explicitly manual/performance-only and documented. Avoid uncontrolled sleeps, randomness, scheduling
  assumptions, and shared global flag leakage.
- Do not introduce secrets, private endpoints, personal paths, account identifiers, raw sensitive payloads, or sensitive
  logs into source, tests, scripts, docs, examples, generated artifacts, PR text, or review comments.
- When implementing fixes from review comments, verify the current source before marking a discussion resolved. If the
  code still violates the comment, keep it unresolved and state the source-backed reason.

## Verification Flow

1. Run the smallest targeted test that should catch the change.
2. Run broader tests when the change touches shared contracts, recovery, threading, or hot paths.
3. Run the relevant build-system checks when build files, public headers, generated files, package manifests, or dual
   CMake/Bazel surfaces changed.
4. For performance-sensitive changes, run the benchmark or measurement that matches the stated target. If it cannot be
   run locally, record the limitation instead of claiming performance is verified.
5. For context-only changes, run metadata and index checks when structure or metadata changed.
6. Use `.repo_context/playbooks/upkeep/ai-self-verification.md` before final status.

## Context Update Rules

Update `.repo_context` in the same task when the change affects:

- module responsibilities or boundaries
- startup, shutdown, recovery, or failover behavior
- public API or protocol behavior
- build flags, test commands, or reproduction workflow
- dscli, K8s/Helm, generated docs, or public configuration semantics
- repository-local skills or their trigger routing
- a stale or misleading context claim discovered during the task

## Stop And Escalate Conditions

Stop and ask for direction, or explicitly narrow scope, when:

- the requested change requires a persistence format change without migration expectations;
- the change creates a new cross-module lifecycle dependency;
- correctness requires performance or recovery testing that cannot be run locally;
- a default test would exceed 8 seconds and cannot be made targeted, deterministic, or manual/performance-only;
- source inspection contradicts `.repo_context` and the correct module boundary is unclear;
- the implementation requires broad refactoring unrelated to the requested behavior.

## Done Criteria

- Source-backed implementation or documentation change is complete.
- User, design, PR, or review claims are traceable to code, tests, docs, config, build files, or recorded limitations.
- The implementation keeps the touched module high-cohesion and low-coupling, reuses existing infrastructure where
  practical, and does not leave duplicated or patch-like logic behind.
- Relevant tests or validation commands were run, or the limitation is recorded.
- Default tests stay within the 8-second expectation or are explicitly manual/performance-only and documented.
- Public API, config, dscli, K8s/Helm, docs, CMake, Bazel, packaging, and generated artifacts are updated when affected.
- Hot-path, concurrency, recovery, compatibility, and sensitive-information risks were checked for the touched area.
- `.repo_context` navigation and skill routing are updated when needed.
- Final response reports exact verification evidence.
