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
  - `2026-06-13`

## Purpose

Use this playbook when a task touches product code, module behavior, test behavior, build behavior, or a design decision
in `yuanrong-datasystem`.

This is the AI-native workflow for making small, source-backed changes without losing infrastructure discipline.
Use it to front-load design, API, performance, safety, test, build, rollout, and operability constraints before code is
written. Development should make the intended boundary, risk, validation evidence, and production behavior explicit
before implementation starts.

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
- [ ] Map the user request, issue, design document, incident note, or planned change to the exact code, test, docs,
      config, and build artifacts that must change.
- [ ] Decide whether the work is one coherent change. If it mixes unrelated behavior, unrelated modules, independent
      risk surfaces, or multiple rollout plans, split it or record why a narrow combined change is required.
- [ ] Identify qualified owners or domain experts for high-risk surfaces before coding: public/internal APIs, C API, JNI,
      pybind, RPC/protobuf, Bazel/CMake, K8s/Helm, persistence, recovery, concurrency, memory ownership, hot paths, and
      security boundaries.
- [ ] Classify the implementation risk as docs-only, low, medium, or high, and choose the validation evidence before
      writing code.
- [ ] Decide whether the path is foreground, metadata coordination, data movement, background, or test-only.
- [ ] Search for existing helpers, status/error style, IO/persistence wrappers, thread pools, background task helpers,
      recovery helpers, and test utilities.
- [ ] Identify the owning abstraction for the behavior and whether the change keeps responsibilities high-cohesion and
      low-coupling.
- [ ] Search for repeated or scattered logic in the touched module and decide whether it should be reused, deleted, or
      centralized before adding new code.
- [ ] Identify any internal API, facade, helper, callback, RAII object, config object, or boundary type that other
      developers will call. Treat it as a semi-public contract even if it stays inside `src/`.
- [ ] Check whether the planned API has one obvious usage style, clear naming, compile-time misuse resistance where
      practical, and explicit ownership/lifetime semantics.
- [ ] Check whether a production bug can be localized from symptoms to the owning module, entrypoint, state owner,
      config source, request path, background task, or boundary layer without reverse-engineering unrelated code.
- [ ] Identify which logs, metrics, traces, error codes, request IDs, worker/master/client identity, state transitions,
      or config signals will make the change diagnosable without exposing sensitive data or adding hot-path overhead.
- [ ] Identify every affected config, startup/runtime update, local/remote, SDK, worker, embedded, dscli, K8s/Helm, and
      documentation surface.
- [ ] Identify affected build systems and packaging paths, including Bazel, CMake, installed/public headers, generated
      files, and package manifests.
- [ ] Identify rollout and rollback expectations when behavior, config, compatibility, persistence, packaging, or
      operators are affected: default state, feature flag or staged enablement, downgrade behavior, rollback command or
      procedure, and compatibility window.
- [ ] For production bug fixes, record the symptom, trigger condition, root cause, code owner, fix point, regression
      guard, and any follow-up observability or prevention work.
- [ ] Identify whether the change crosses a security or trust boundary: input validation, authentication/authorization,
      deserialization, external commands, path handling, dependency/script execution, tenant data, or privilege changes.
- [ ] Identify whether the change touches a hot path.
- [ ] Identify shared mutable state, ownership, and lifecycle assumptions.
- [ ] Identify whether persistence, recovery, compaction, failover, migration, or startup rebuild is affected.
- [ ] Choose the minimum test level that can catch the intended behavior.
- [ ] Estimate whether new or modified default tests can complete within 8 seconds and whether they introduce sleep,
      randomness, wall-clock, scheduling, or shared-global-state flakiness.
- [ ] Check that planned source, tests, scripts, docs, change notes, and generated artifacts will not expose secrets,
      private endpoints, local paths, account data, or sensitive payloads.
- [ ] For `.repo_context/` updates, check that validation notes, incident evidence, examples, and historical summaries
      use placeholders instead of real private hosts, IPs, ports, SSH users, personal namespaces, expanded home paths, or
      absolute private log paths.
- [ ] Decide whether `.repo_context` must be updated with the code change.

Useful search patterns:

```bash
rtk rg -n "Status |RETURN_IF_NOT_OK|CHECK_FAIL|INJECT_POINT|PerfPoint|ThreadPool|Timer" src tests
rtk rg -n "Recover|Recovery|Persist|Persistence|Compact|Replay|Restart|Failover|Preload" src tests
rtk rg -n "std::mutex|shared_mutex|lock_guard|unique_lock|atomic|shared_ptr|unique_ptr" src/datasystem
```

## Design Notes Before Editing

For non-trivial changes, write down the following in the working notes, change notes, or implementation plan:

| Question | Required answer |
| --- | --- |
| What request, issue, design, incident, or planned change is being satisfied? | exact source-backed artifact that proves it |
| Is this one coherent change? | split plan, or reason the combined change is still narrow |
| Who owns the risky surface? | module owner, domain expert, or reason no owner-specific confirmation is needed |
| What implementation risk level applies? | docs-only, low, medium, or high, with selected validation evidence |
| What path is touched? | foreground, metadata, transfer, background, test, or docs |
| What abstraction owns the behavior? | owning class/module/helper and why the caller should or should not see the detail |
| What existing helper is reused? | exact helper or reason none fits |
| What duplicate or scattered logic is removed or avoided? | exact repeated code, or reason no duplication exists |
| What interface will future developers use? | API shape, naming, one recommended usage path, and misuse prevention |
| What ownership or lifetime does the interface imply? | owned/ref/view/callback/async/thread boundary and release/completion owner |
| How will production issues be localized? | owning module, first entrypoint, correlated signal, and sensitive-data guard |
| What public/config/docs surfaces change? | API, embedded, dscli, K8s/Helm, docs, compatibility, or none |
| What build and packaging surfaces change? | CMake, Bazel, public headers, generated files, package manifests, or none |
| How will rollout and rollback work? | default behavior, staged enablement, compatibility, downgrade, rollback, or none |
| If this is a production bug fix, what prevents recurrence? | symptom, root cause, regression guard, owner, and follow-up signal |
| What security boundary is touched? | validation, auth, deserialization, command/path handling, dependency, sensitive data, or none |
| What is the hot-path impact? | allocation/copy/lock/IO/logging/complexity impact |
| What state is shared? | owner, lock, lifetime, shutdown behavior |
| What state is durable? | storage target, write order, recovery path |
| What verifies it? | exact test, build, benchmark, test runtime expectation, or reason for limited verification |

## Implementation Rules

- Keep changes small and traceable to the user request.
- Keep one change focused on one behavior, boundary, or failure mode. Split unrelated feature work, cleanup, build
  plumbing, test reshaping, and operator changes unless they are required for the same deliverable.
- Match existing style and ownership boundaries.
- Maintain high cohesion and low coupling: keep module responsibilities explicit, keep infrastructure details inside the
  owning abstraction, and do not make unrelated callers coordinate internal policy.
- Treat widely used internal interfaces as semi-public contracts. Do not expose policy, synchronization, lifecycle,
  formatting, sampling, retry, cache, or lazy/deferred mechanics to business call sites unless that is the owning
  abstraction's explicit responsibility.
- Design APIs for the next developer adding a similar feature: one obvious recommended style, names that communicate
  intent, ownership, lifetime, sync/async behavior, error semantics, and mode choices.
- Prefer interfaces that make common wrong usage difficult or impossible. Check nullability, ordering, one-shot
  semantics, move/copy/delete behavior, async capture lifetime, and thread-boundary assumptions before coding.
- Reuse before adding new mechanisms: prefer existing helpers, status patterns, config plumbing, thread pools, test
  fixtures, and build targets unless source inspection shows they do not fit.
- Remove or centralize duplicated logic when it belongs to the touched behavior. Do not leave scattered patch-like code,
  dead compatibility shims, or unused wrappers around the new implementation.
- Prefer local helpers over broad abstractions.
- Keep module locatability intact. Code should make it clear which module owns the behavior, where the first entrypoint
  is, who owns state, which boundary is crossed, and which signal path explains failures in production.
- Keep production diagnosability proportional to risk. Add or reuse low-overhead logs, metrics, traces, error codes,
  request IDs, state-transition evidence, and config-source reporting when they materially reduce incident triage time.
  Avoid sensitive payloads and avoid high-cost diagnostics in hot paths.
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
  logs into source, tests, scripts, docs, examples, generated artifacts, change notes, or development records.
- In `.repo_context/`, record private validation, incident, and operations evidence through placeholders or sanitized
  identifiers. Keep concrete private endpoints, personal accounts, expanded home directories, remote worktrees, and
  absolute private log paths in local configuration or controlled logs, not in repository context.
- When implementing requested follow-up fixes, verify the current source before declaring the item done. If the code still
  violates the requested behavior or design constraint, record the source-backed reason and keep working in the narrowest
  safe scope.

## Development Design Gates

Apply these gates while designing and coding:

1. Internal API design:
   - Keep responsibilities narrow, surfaces small, semantics stable, and one recommended usage path obvious.
   - Do not push policy, lifecycle, synchronization, formatting, or lazy/deferred mechanics into unrelated callers.
2. Developer experience and naming:
   - Calling code should express business intent.
   - Names must make ownership, lifetime, mode, sync/async behavior, lazy/deferred behavior, and error semantics clear at
     the call site.
3. Misuse prevention:
   - Consider common wrong usages before coding.
   - Prefer compile-time constraints, typed wrappers, RAII, narrow constructors, and focused invariant tests over comments
     that ask callers to remember hidden rules.
4. Ownership and lifetime:
   - Audit `Ref`, `View`, `Owned`, `Provider`, callback, handle, buffer, iterator, raw-pointer, and smart-pointer APIs.
   - References and views are acceptable only for same-scope synchronous use; async/shared use needs owned state or an
     explicit shared lifetime.
5. Hot-path argument cost:
   - For guard, lazy, deferred, sampling, cache, or fast-reject APIs, inspect caller expressions as well as callee bodies.
   - Avoid skipped-path string formatting, serialization, container construction, copies, locks, IO, time reads, atomics,
     and CAS unless measured and justified.
6. Abstraction boundaries:
   - Keep common infrastructure, client, worker, master, SDKs, storage, RPC, persistence, config, and operations
     boundaries explicit.
   - Keep business special cases local unless multiple call sites genuinely need a shared API.
7. Minimal implementation and reuse:
   - Search existing helpers, status/error patterns, thread pools, serializers, config paths, metrics, persistence
     utilities, and test harnesses before adding a new mechanism.
8. Consistency and learnability:
   - A capability should have one recommended style across the touched system.
   - Avoid mixed old/new styles, ownerless compatibility shims, duplicate helpers, and examples that teach non-recommended
     usage.
9. Boundary layers:
   - Treat C API, JNI, pybind, RPC, protobuf, CLI, K8s/Helm, config files, and generated artifacts as system boundaries.
   - Check nullability, length-delimited versus NUL-terminated data, encoding, ownership transfer, buffer copies,
     exception/error propagation, ABI/API compatibility, and documentation synchronization.
10. Build closure:
   - Update CMake and Bazel when both exist. Shared headers and facade changes require grepping call sites, language
     bindings, package targets, tests, and generated code.
11. Test contracts:
   - Encode design invariants, not only happy paths.
   - Prefer behavior/state assertions through public or semi-public APIs over brittle private call-order tests, unless
     that ordering is itself a stable contract.
12. Operability and locatability:
   - From an alert, user report, or error log, an on-call engineer should know which module owns the behavior, which
     entrypoint to inspect first, which state/config changed, and which telemetry correlates the request across
     boundaries.
13. Sensitive information:
   - Do not add secrets, private endpoints, personal paths, account identifiers, tenant data, raw object payloads, or
     sensitive request data to source, tests, logs, metrics, docs, generated artifacts, change notes, or development
     records.
14. Change decomposition:
   - Keep the default unit of work small enough to understand, test, and roll back.
   - Split changes that mix independent behavior, unrelated cleanup, unrelated build edits, or multiple rollout plans.
15. Qualified ownership:
   - For high-risk surfaces, identify the module owner or domain expert before implementation choices become expensive
     to unwind.
   - When no owner is available, record the source-backed reasoning and keep the change conservative.
16. Release readiness:
   - For externally visible behavior, define default behavior, feature flag or staged rollout needs, compatibility with
     old clients/configs, downgrade behavior, rollback path, and health signals.
17. Production incident fixes:
   - Connect the code change to the symptom, trigger condition, root cause, regression guard, and prevention signal.
   - Do not stop at making the immediate failure disappear if the same class of failure can recur silently.
18. Security boundaries:
   - Treat input validation, auth/authz, deserialization, external commands, path handling, dependencies, scripts,
     privilege changes, and sensitive logging as design surfaces, not cleanup details.

## Verification Flow

1. Run the smallest targeted test that should catch the change.
2. Run broader tests when the change touches shared contracts, recovery, threading, or hot paths.
3. Run the relevant build-system checks when build files, public headers, generated files, package manifests, or dual
   CMake/Bazel surfaces changed.
4. For performance-sensitive changes, run the benchmark or measurement that matches the stated target. If it cannot be
   run locally, record the limitation instead of claiming performance is verified.
5. For context-only changes, run metadata and index checks when structure or metadata changed.
6. Use `.repo_context/playbooks/upkeep/ai-self-verification.md` before final status.

Use this risk-based validation matrix when choosing evidence:

| Change surface | Minimum expected evidence |
| --- | --- |
| docs, context, workflow text | markdown/diff sanity check and any relevant metadata or skill validation |
| tests only | run the changed test or the narrowest test target that owns it |
| local implementation logic | targeted unit/component test and source-backed explanation for untested paths |
| public API, boundary, or generated surface | compile/build check for affected consumers, docs/config update, and compatibility note |
| hot path | targeted correctness test plus benchmark, perf test, or recorded measurement limitation |
| concurrency or lifetime | targeted stress, deterministic invariant test, sanitizer, or source-backed limitation |
| persistence, recovery, or migration | restart/recovery/failover test or explicit migration and rollback evidence |
| build, package, or toolchain | affected CMake/Bazel/package command, not only an editor or IDE build |
| config or operator surface | startup/runtime config path, default behavior, rollback, and documentation check |

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
- User request, issue, design, incident, or planned change is traceable to code, tests, docs, config, build files, or
  recorded limitations.
- The implementation keeps the touched module high-cohesion and low-coupling, reuses existing infrastructure where
  practical, and does not leave duplicated or patch-like logic behind.
- Shared internal APIs are clear, consistently named, hard to misuse, and friendly to future developers.
- Ownership, lifetime, boundary, hot-path argument cost, production locatability, and diagnosability have been checked
  for the touched area.
- Change size, qualified ownership, risk level, rollout/rollback, incident prevention, and security boundary expectations
  have been checked where applicable.
- Relevant tests or validation commands were run, or the limitation is recorded.
- Default tests stay within the 8-second expectation or are explicitly manual/performance-only and documented.
- Public API, config, dscli, K8s/Helm, docs, CMake, Bazel, packaging, and generated artifacts are updated when affected.
- Hot-path, concurrency, recovery, compatibility, and sensitive-information risks were checked for the touched area.
- `.repo_context` navigation and skill routing are updated when needed.
- Final response reports exact verification evidence.
