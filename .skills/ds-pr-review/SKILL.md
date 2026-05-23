---
name: ds-pr-review
description: Use when reviewing yuanrong-datasystem code, tests, scripts, docs, diffs, commits, pull requests, PR descriptions, or design changes for correctness, sensitive-information exposure, performance, concurrency, persistence, recovery, testing, or context-update risk.
---

# Datasystem PR Review

## Required Reading

1. `AGENTS.md`
2. `.repo_context/modules/overview/engineering-principles.md`
3. `.repo_context/playbooks/reviews/pr-review-checklist.md`
4. The touched module docs from `.repo_context/index.md`
5. The actual diff and source code being reviewed
6. PR descriptions and commit messages when they are part of the review scope

## Review Rules

- Lead with findings, ordered by severity.
- Prioritize correctness, data integrity, recovery, availability, hot-path performance, concurrency safety, and missing
  tests.
- Check source code, tests, scripts, docs, PR descriptions, and commit messages for sensitive information exposure.
- Do not quote secrets or private values in review output; identify the category and location, then ask for redaction or
  rotation when needed.
- Use exact file and line references.
- Keep summaries secondary to findings.
- If no issues are found, say that clearly and mention any residual test or verification gap.

## Sensitive Information Gate

Flag sensitive information in any reviewed artifact, including source files, test data, shell/Python/CMake/CI scripts,
docs, PR descriptions, and commit messages.

Treat the following as review findings:

- credentials, passwords, tokens, private or SSH keys, AK/SK values, access keys, secret keys, tenant/system keys, or
  other usable authentication material;
- server IPs, host:port pairs, internal endpoints, private URLs, or environment-specific service addresses that are not
  already public project documentation;
- local absolute paths, personal home directories, workspace paths, account names, user names, tenant identifiers, or raw
  log snippets that expose private environment details;
- logs, metrics, errors, tests, or scripts that newly print or persist sensitive request payloads, object data,
  credentials, tokens, or account metadata.

Use severity based on impact: `P0`/`P1` for usable secrets or credentials, `P1`/`P2` for private infrastructure or
personal environment leakage, and lower severity only when the exposure is clearly non-sensitive. In comments, cite the
file/line or PR/commit field and the sensitive category, but do not reproduce the sensitive value.

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
