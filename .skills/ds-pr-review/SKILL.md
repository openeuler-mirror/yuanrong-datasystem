---
name: ds-pr-review
description: Use when reviewing yuanrong-datasystem code, diffs, commits, pull requests, PR descriptions, or design changes for correctness, performance, concurrency, persistence, recovery, testing, or context-update risk.
---

# Datasystem PR Review

## Required Reading

1. `AGENTS.md`
2. `.repo_context/modules/overview/engineering-principles.md`
3. `.repo_context/playbooks/reviews/pr-review-checklist.md`
4. The touched module docs from `.repo_context/index.md`
5. The actual diff and source code being reviewed

## Review Rules

- Lead with findings, ordered by severity.
- Prioritize correctness, data integrity, recovery, availability, hot-path performance, concurrency safety, and missing
  tests.
- Use exact file and line references.
- Keep summaries secondary to findings.
- If no issues are found, say that clearly and mention any residual test or verification gap.

## Output Shape

```text
Findings:
- [P1] Title
  path:line
  Explanation and expected fix.

Open questions:
- ...

Residual risk:
- ...
```
