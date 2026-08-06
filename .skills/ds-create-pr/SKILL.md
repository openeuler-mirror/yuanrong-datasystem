---
name: ds-create-pr
description: Use when the user asks to create, submit, or open a GitCode Pull Request for a committed source branch in openeuler/yuanrong-datasystem or another GitCode repository, including changes targeting doc_pages, master, or another base branch.
---

# GitCode Create PR

## Workflow

1. Confirm the source branch is committed and the worktree is clean. Do not push it yet. Fetch the latest target branch,
   identify its local or remote-tracking ref for `--base-ref`, and select a fork remote for `--push-remote`. For
   `openeuler/yuanrong-datasystem`, never select the upstream repository as the push remote.
2. Read `references/create-pull-request-api.md` when parameter details are needed.
3. Get the token from `GITCODE_TOKEN`, `GITCODE_ACCESS_TOKEN`, or `~/.local/gitcode_token`. Never print the token.
   Empty or whitespace-only token values are treated as configuration errors and will produce a clear prompt telling the caller what to fix.
4. Before submitting the PR, prepare a PR description that follows `.gitee/PULL_REQUEST_TEMPLATE/PULL_REQUEST_TEMPLATE.zh-cn.md` and fills in the current change summary, verification result, and any API-impact notes. For `openeuler/yuanrong-datasystem`, the bundled script now treats a missing or non-template PR body as an error.

   Fill in the 6 sections as follows:

   | Section | How AI should fill it |
   |---------|----------------------|
   | **1. Background / Symptom** | Summarize from `git diff` / commit messages. For bugfixes, list specific test names and error logs |
   | **2. Design / Solution** | REQUIRED: Mermaid sequence or class diagram when multiple components interact. Include a file change checklist table. For bugfixes, state whether PR-introduced or pre-existing |
   | **3. Verification Plan** | Build (bazel+cmake) + deploy steps + test matrix, with master baseline comparison |
   | **4. Verification Results** | MUST include specific numbers (PASS/FAIL counts). "Verified OK" alone is not acceptable |
   | **5. Follow-up Items** | Work planned for subsequent PRs. Bugfixes with no leftovers may delete this entire section |
   | **6. Self-Checklist** | Before checking each dimension, run the corresponding pre-flight verification. **Do not check off items mechanically** — each check must be backed by actual evidence |

   Pre-flight verification before filling the checklist:

   | Run this skill | To verify these checklist dimensions |
   |---------------|--------------------------------------|
   | `ds-self-verify` | Build (bazel + cmake pass), basic diff sanity |
   | `ds-pr-review` | Correctness, Memory, Concurrency, Performance, Security, Observability, Logging, Forward Compatibility, API Changes |

   `ds-pr-review` produces findings that feed directly into checklist items. After running both skills and addressing any findings, fill in the self-checklist with concrete results. **Remove dimensions that do not apply** to this PR.

   Mermaid notes: Forbidden — `Note over`, `::`, `()` in alt labels, `-->>` dashed arrows, Unicode special characters.

   PR title convention for `openeuler/yuanrong-datasystem`: **Chinese title** with optional English `type(scope):` prefix.
   Examples: `fix(brpc): 修复 stream close leak 模式` / `feat: 新增数据亲和路由`. Commit messages remain English.

   Commit message rules:
   - Use **Conventional Commits** format: `type: description` (type = feat / fix / docs / refactor / perf / test / build / chore).
   - **Never** include `Co-Authored-By:` in commit messages or PR descriptions.
5. Check commit messages and the PR description for sensitive or personal information before creating the PR. Do not include server IPs or ports, local absolute paths, account names, passwords, tokens, SSH/private keys, AK/SK, or similar non-public details. Redact or generalize them first; use repository-relative paths and sanitized log names instead.
6. Prefer the bundled script:

   ```bash
   python3 .skills/ds-create-pr/scripts/create_pr.py \
     --owner openeuler \
     --repo yuanrong-datasystem \
     --base doc_pages \
     --base-ref <upstream-remote>/doc_pages \
     --head <fork-owner>:<source-branch> \
     --push-remote <fork-remote> \
     --local-squash-message "docs: refresh zh-cn latest pages" \
     --title "docs: refresh zh-cn latest pages" \
     --body-file /tmp/pr-body.md
   ```

7. Before calling GitCode, the bundled script counts commits in `merge-base(--base-ref, HEAD)..HEAD`. It rejects zero
   commits; pushes one commit unchanged; and, for multiple commits, creates a `codex/pre-squash-*` recovery ref, creates
   one Conventional Commit with `--local-squash-message`, and updates an existing fork branch only with an explicit
   `--force-with-lease`. Continue only when it reports `SOURCE_COMMIT_COUNT_AFTER=1` and `SOURCE_PUSH_STATUS=verified`.
8. After GitCode creates the PR, post `/retest` as a general PR comment unless either the target `--base` is
   `doc_pages`, or every changed path is under `.repo_context/` or `docs/`. The bundled script evaluates paths with
   rename detection disabled so both sides of a rename remain visible. It reports `RETEST_COMMENT_STATUS=skipped` and
   `RETEST_SKIP_REASON=base_is_doc_pages|docs_or_repo_context_only` for an allowed skip; otherwise it posts the comment
   and reports `RETEST_COMMENT_STATUS=posted`. If a required comment fails, the PR already exists: do not rerun PR
   creation or create a duplicate PR; post `/retest` to the returned PR URL instead.
9. Keep `--check-conflicts` enabled. If the script reports `CONFLICT_STATUS=conflict`, tell the caller to refresh from the latest upstream `doc_pages` and regenerate the docs refresh commit before opening a new PR.
10. Report the source commit count, push status, returned PR URL, retest comment status, and conflict status to the user.

## Defaults For This Repository

- Owner: `openeuler`
- Repository: `yuanrong-datasystem`
- API base URL: `https://api.gitcode.com/api/v5`
- PR endpoint: `POST /repos/{owner}/{repo}/pulls`
- Retest comment endpoints: first `POST /repos/{owner}/{repo}/issues/{number}/comments`; on an unsupported-endpoint
  response, fall back to body-only `POST /repos/{owner}/{repo}/pulls/{number}/comments`, both with body `/retest`.
- Retest skip policy: skip when `--base=doc_pages`, or when all changed paths are exclusively within `.repo_context/`
  and `docs/`; any path outside those directories requires `/retest`.
- Token transport: `access_token` query parameter.
- PR body template for this repository: `.gitee/PULL_REQUEST_TEMPLATE/PULL_REQUEST_TEMPLATE.zh-cn.md`

For online documentation refreshes, use base branch `doc_pages` and the pushed documentation refresh branch as `head`.

## Safety

- Do not push local branches to the upstream `openeuler/yuanrong-datasystem` repository when preparing the PR source branch. Push to a fork or another non-upstream remote instead.
- Use an up-to-date `--base-ref` that is either identical to `--base` or ends with the complete `--base` branch name;
  do not count or squash against a stale or different target branch.
- Require a clean worktree, an attached source branch matching `--head`, and at least one source commit before rewriting or pushing history.
- When multiple source commits exist, preserve the original HEAD under the reported `codex/pre-squash-*` backup ref and use only the script's explicit `--force-with-lease`; never use plain `--force`.
- Do not create a PR until the script verifies exactly one source commit and confirms the pushed remote branch matches local HEAD.
- Check the created PR for conflicts before declaring the workflow complete.
- Do not declare the workflow complete unless the script reports `RETEST_COMMENT_STATUS=posted`, or reports
  `RETEST_COMMENT_STATUS=skipped` with one of the two documented `RETEST_SKIP_REASON` values.
- If a required `/retest` fails after PR creation, preserve and report the existing PR URL; do not create a duplicate PR.
- Do not include sensitive or personal information in command output, PR body, commit messages, or logs. This includes server IPs or ports, local absolute paths, account names, passwords, tokens, SSH/private keys, AK/SK, and similar non-public details.
- The bundled script rejects common sensitive patterns in the PR title, PR body, `--squash-commit-message`, and any
  required `--local-squash-message`; still inspect an unchanged single source commit separately because its message is
  not passed to the script.
- If the API fails, preserve the HTTP status and response body summary, but redact token-like values.
