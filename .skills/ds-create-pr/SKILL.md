---
name: ds-create-pr
description: Create GitCode Pull Requests for openeuler/yuanrong-datasystem or other GitCode repositories by calling the GitCode OpenAPI. Use when the user asks to create, submit, or open a PR for a pushed branch, especially after preparing docs or code commits that must target doc_pages, master, or another GitCode branch. For openeuler/yuanrong-datasystem, the PR body must follow .gitee/PULL_REQUEST_TEMPLATE/PULL_REQUEST_TEMPLATE.zh-cn.md.
---

# GitCode Create PR

## Workflow

1. Confirm the source branch is committed and pushed to GitCode.
   For `openeuler/yuanrong-datasystem`, push the source branch only to your fork or another non-upstream remote. Do not push local branches to `git@gitcode.com:openeuler/yuanrong-datasystem.git` or `https://gitcode.com/openeuler/yuanrong-datasystem.git`.
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
     --head <source-branch> \
     --title "docs: refresh zh-cn latest pages" \
     --body-file /tmp/pr-body.md
   ```

7. Keep `--check-conflicts` enabled. If the script reports `CONFLICT_STATUS=conflict`, tell the caller to refresh from the latest upstream `doc_pages` and regenerate the docs refresh commit before opening a new PR.
8. Report the returned `html_url`, `web_url`, or `url` to the user.

## Defaults For This Repository

- Owner: `openeuler`
- Repository: `yuanrong-datasystem`
- API base URL: `https://api.gitcode.com/api/v5`
- PR endpoint: `POST /repos/{owner}/{repo}/pulls`
- Token transport: `access_token` query parameter.
- PR body template for this repository: `.gitee/PULL_REQUEST_TEMPLATE/PULL_REQUEST_TEMPLATE.zh-cn.md`

For online documentation refreshes, use base branch `doc_pages` and the pushed documentation refresh branch as `head`.

## Safety

- Do not push local branches to the upstream `openeuler/yuanrong-datasystem` repository when preparing the PR source branch. Push to a fork or another non-upstream remote instead.
- Do not create a PR until the source branch exists on the remote.
- Check the created PR for conflicts before declaring the workflow complete.
- Do not include sensitive or personal information in command output, PR body, commit messages, or logs. This includes server IPs or ports, local absolute paths, account names, passwords, tokens, SSH/private keys, AK/SK, and similar non-public details.
- The bundled script rejects common sensitive patterns in PR title, PR body, and `--squash-commit-message`; still inspect regular commit messages separately because they may not be passed to the script.
- If the API fails, preserve the HTTP status and response body summary, but redact token-like values.
