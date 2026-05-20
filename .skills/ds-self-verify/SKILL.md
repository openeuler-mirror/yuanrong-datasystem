---
name: ds-self-verify
description: Use when Codex is about to claim yuanrong-datasystem work is complete, tests pass, context is updated, a commit is ready, or a PR is ready after changing files or reviewing code.
---

# Datasystem Self Verification

## Required Reading

Read `.repo_context/playbooks/upkeep/ai-self-verification.md`.

## Required Actions

- Check the newest user request against the work performed.
- Inspect `git status --short`.
- Review the relevant diff.
- Run the smallest relevant validation commands, or record why they were not run.
- Check hot-path, concurrency, persistence/recovery, tests, and `.repo_context` update gates.
- Report exact commands and results in the final response.

## Rule

Do not claim "complete", "fixed", "passing", "verified", "committed", or "PR-ready" without evidence from the
self-verification playbook.
