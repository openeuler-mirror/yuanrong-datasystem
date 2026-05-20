---
name: ds-infra-engineering
description: Use when implementing, debugging, refactoring, designing, or answering codebase questions that touch yuanrong-datasystem runtime, client, worker, master, common infra, performance-sensitive, concurrency-sensitive, or recovery-sensitive paths.
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
6. The smallest relevant module doc and playbook from `.repo_context/index.md`

## Workflow

- Classify the path: foreground request, metadata coordination, data transfer, background, test-only, or docs-only.
- Search for existing helpers before adding logic.
- Decide whether to use:
  - `.repo_context/playbooks/features/infra-engineering-workflow.md`
  - `.repo_context/playbooks/features/performance-change.md`
  - `.repo_context/playbooks/features/concurrency-and-memory-safety.md`
  - `.repo_context/playbooks/features/recovery-and-persistence.md`
- Verify important claims against source before editing or answering.
- Keep changes small and update `.repo_context` when module behavior, commands, skill routing, or workflow knowledge changes.

## Completion Gate

Before claiming the work is complete, use `$ds-self-verify`.
