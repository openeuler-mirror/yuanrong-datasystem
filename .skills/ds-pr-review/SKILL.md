---
name: ds-pr-review
description: Use when reviewing yuanrong-datasystem code, tests, scripts, docs, diffs, commits, pull requests, PR descriptions, or design changes for correctness, sensitive-information exposure, performance, concurrency, persistence, recovery, testing, context-update risk, and GitCode PR review-comment publishing.
---

# Datasystem PR Review

## GitCode PR Publishing Workflow

When the review target is a GitCode PR/MR number or URL for `openeuler/yuanrong-datasystem`, use the
`yuanrong-pr-review` workflow as part of this skill. Do not stop after producing local findings unless the user
explicitly asks for local-only review or publishing is blocked by missing credentials/API failure.

1. Run prepare first:

```bash
python3 ~/workspace/oh-my-codex/plugins/yuanrong-pr-review/skills/yuanrong-pr-review/scripts/review_pr.py prepare <PR_OR_URL>
```

2. Read the generated bundle path from the command output, then load only the needed review inputs:
   - `references/review_policy.md`
   - `references/language_policy.md`
   - `references/repo_focus.md`
   - `references/output_contract.md`
   - the generated `bundle.json`
3. Review changed files first using the bundle's annotated patch. Prefer `diff_line_index` from the bundle for comment
   anchoring. Open repository source only when the diff and snippets are not enough for a high-confidence finding.
4. Write findings as JSON matching the output contract. Each finding must include:
   - `path`
   - preferred `diff_line_index`, with `line` as human fallback
   - `type`
   - `severity`
   - `title`
   - concrete `problem`
   - concrete `suggestion`
5. Publish findings back to GitCode:

```bash
python3 ~/workspace/oh-my-codex/plugins/yuanrong-pr-review/skills/yuanrong-pr-review/scripts/review_pr.py publish \
  --bundle <BUNDLE_JSON> \
  --findings <FINDINGS_JSON>
```

Use `--dry-run` only when explicitly requested or when validating comment placement before a risky publish.

Publishing rules:

- Post one remote comment per high-confidence finding.
- Do not post a summary comment.
- If there are no findings, do not post remote comments.
- Let the publish script resolve `diff_line_index` to the absolute file line and handle deduplication.
- If publishing fails because credentials are missing, API access is unavailable, or a line anchor cannot be safely
  resolved, report that limitation and include the local findings.

## Required Reading

1. `AGENTS.md`
2. `.repo_context/modules/overview/engineering-principles.md`
3. `.repo_context/playbooks/reviews/pr-review-checklist.md`
4. For GitCode PR/MR review targets, the prepared `yuanrong-pr-review` bundle and references listed above
5. The touched module docs from `.repo_context/index.md`
6. The actual diff and source code being reviewed
7. PR descriptions and commit messages when they are part of the review scope

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
- For PR/MR review targets, publish accepted findings to the PR page through the GitCode workflow before final response.

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

For local-only reviews or publish-blocked reviews:

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

For GitCode PR/MR reviews, also report the publish summary printed by `review_pr.py publish`, including posted line
comments, posted general comments, skipped duplicates, skipped suggestions, and any warnings.
