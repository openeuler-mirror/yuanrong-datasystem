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
5. Prefer the bundled script:

   ```bash
   python3 .skills/ds-create-pr/scripts/create_pr.py \
     --owner openeuler \
     --repo yuanrong-datasystem \
     --base doc_pages \
     --head <source-branch> \
     --title "docs: refresh zh-cn latest pages" \
     --body-file /tmp/pr-body.md
   ```

6. Keep `--check-conflicts` enabled. If the script reports `CONFLICT_STATUS=conflict`, tell the caller to refresh from the latest upstream `doc_pages` and regenerate the docs refresh commit before opening a new PR.
7. Report the returned `html_url`, `web_url`, or `url` to the user.

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
- Do not include credentials in command output, PR body, commit messages, or logs.
- If the API fails, preserve the HTTP status and response body summary, but redact token-like values.
