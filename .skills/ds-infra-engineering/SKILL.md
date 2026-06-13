---
name: ds-infra-engineering
description: Use when implementing, debugging, refactoring, designing, or answering codebase questions that touch yuanrong-datasystem runtime, client, worker, master, common infra, public/internal APIs, developer-facing abstractions, module boundaries, production diagnosability, hot-path performance, concurrency, memory safety, persistence, recovery, build/test behavior, or repo-context updates.
---

# Datasystem Infrastructure Engineering

## Purpose

Use this skill to keep Codex in infrastructure-engineering mode before changing or asserting behavior in
`yuanrong-datasystem`.

## Required Reading

1. `AGENTS.md`
2. `.repo_context/README.md`
3. `.repo_context/index.md`
4. `.repo_context/maintenance.md`
5. `.repo_context/modules/overview/engineering-principles.md`
6. `.repo_context/playbooks/features/infra-engineering-workflow.md`
7. The smallest relevant module doc and narrower playbook from `.repo_context/index.md`

## Workflow

- Classify the path: foreground request, metadata coordination, data transfer, background, test-only, or docs-only.
- Decide whether the request is one coherent change. Split unrelated behavior, build edits, cleanup, and operator changes
  unless they are required for the same deliverable.
- Identify high-risk surfaces and owners before coding: shared internal APIs, public APIs, language bindings, RPC,
  Bazel/CMake, K8s/Helm, persistence, recovery, concurrency, hot paths, and security boundaries.
- Choose risk-based validation evidence before implementation, including whether the change needs targeted tests,
  build-system checks, perf evidence, sanitizer/stress coverage, recovery tests, or config/operator validation.
- Search for existing helpers before adding logic.
- Before editing, run the pre-change checklist in `.repo_context/playbooks/features/infra-engineering-workflow.md`.
- Treat shared internal APIs as semi-public contracts: check naming clarity, one recommended usage style, misuse
  prevention, ownership/lifetime semantics, hot-path argument cost, module locatability, and production diagnosability.
- For behavior, config, packaging, or operator changes, record default behavior, rollout, rollback, compatibility, and
  health-signal expectations.
- For production bug fixes, connect symptom, trigger condition, root cause, fix point, regression guard, and prevention
  signal before declaring the implementation complete.
- Decide whether to use:
  - `.repo_context/playbooks/features/infra-engineering-workflow.md`
  - `.repo_context/playbooks/features/performance-change.md`
  - `.repo_context/playbooks/features/concurrency-and-memory-safety.md`
  - `.repo_context/playbooks/features/recovery-and-persistence.md`
- Verify important claims against source before editing or answering.
- Keep changes small and update `.repo_context` when module behavior, commands, skill routing, or workflow knowledge changes.
- Before final status, ensure the development gates were satisfied or record the source-backed limitation.

## Completion Gate

Before claiming the work is complete, use `$ds-self-verify`.
