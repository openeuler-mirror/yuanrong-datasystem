---
name: ds-create-pr
description: Create GitCode Pull Requests for openeuler/yuanrong-datasystem or other GitCode repositories by calling the GitCode OpenAPI. Use when Codex needs to open a PR from a pushed branch, especially after preparing docs or code commits that must target doc_pages, master, or another GitCode branch.
---

# GitCode Create PR

## Workflow

1. Confirm the source branch is committed and pushed to GitCode.
2. Read `references/create-pull-request-api.md` when parameter details are needed.
3. Get the token from `GITCODE_TOKEN`, `GITCODE_ACCESS_TOKEN`, or `~/.local/gitcode_token`. Never print the token.
4. Prefer the bundled script:

   ```bash
   python3 .skills/ds-create-pr/scripts/create_pr.py \
     --owner openeuler \
     --repo yuanrong-datasystem \
     --base doc_pages \
     --head <source-branch> \
     --title "docs: refresh zh-cn latest pages" \
     --body-file /tmp/pr-body.md
   ```

5. Keep `--check-conflicts` enabled. If the script reports `CONFLICT_STATUS=conflict`, tell the caller to refresh from the latest upstream `doc_pages` and regenerate the docs refresh commit before opening a new PR.
6. Report the returned `html_url`, `web_url`, or `url` to the user.

## Defaults For This Repository

- Owner: `openeuler`
- Repository: `yuanrong-datasystem`
- API base URL: `https://api.gitcode.com/api/v5`
- PR endpoint: `POST /repos/{owner}/{repo}/pulls`
- Token transport: `access_token` query parameter.

For online documentation refreshes, use base branch `doc_pages` and the pushed documentation refresh branch as `head`.

## Safety

- Do not create a PR until the source branch exists on the remote.
- Check the created PR for conflicts before declaring the workflow complete.
- Do not include credentials in command output, PR body, commit messages, or logs.
- If the API fails, preserve the HTTP status and response body summary, but redact token-like values.
