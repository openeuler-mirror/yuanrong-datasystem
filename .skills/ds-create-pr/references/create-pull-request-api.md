# GitCode Create Pull Request API

Primary source: <https://docs.atomgit.com/docs/apis/post-api-v-5-repos-owner-repo-pulls>

Verified fallback source: <https://docs.gitcode.com/v1-docs/docs/openapi/repos/pulls/>

## Endpoint

`POST https://api.gitcode.com/api/v5/repos/{owner}/{repo}/pulls?access_token=<token>`

## Required Parameters

- `access_token`: query string. User authorization token.
- `owner`: path. Repository owner namespace, such as `openeuler`.
- `repo`: path. Repository path, such as `yuanrong-datasystem`.
- `title`: JSON body. Pull Request title.
- `head`: JSON body. Source branch. Use `branch` for same-repository PRs and `username:branch` for cross-repository PRs.
- `base`: JSON body. Target branch.

## Optional JSON Body Parameters

- `body`: Pull Request description.
- `milestone_number`: milestone number.
- `labels`: comma-separated label names.
- `issue`: issue id for auto-filling title/content.
- `assignees`: comma-separated reviewer usernames.
- `testers`: comma-separated tester usernames.
- `prune_source_branch`: delete source branch after merge, default `false`.
- `draft`: create as draft, default `false`.
- `squash`: squash on merge, default `false`.
- `squash_commit_message`: squash commit message.
- `fork_path`: required for cross-repository PRs, format `owner/repo`.

## Minimal Request

```bash
curl --location --request POST \
  'https://api.gitcode.com/api/v5/repos/openeuler/yuanrong-datasystem/pulls?access_token=<token>' \
  --header 'Content-Type: application/json' \
  --data-raw '{
    "title": "docs: refresh zh-cn latest pages",
    "head": "docs-refresh-zh-cn-latest",
    "base": "doc_pages",
    "body": "Refresh online Chinese documentation."
  }'
```

## Repository-Specific PR Body Requirement

For `openeuler/yuanrong-datasystem`, prepare the PR body from `.gitee/PULL_REQUEST_TEMPLATE/PULL_REQUEST_TEMPLATE.zh-cn.md`
and fill in the current change description, verification result, fix linkage, and interface-impact notes before calling the API.
The bundled `create_pr.py` helper validates that required template sections are present when targeting this repository.
Also, do not push local source branches to the upstream `openeuler/yuanrong-datasystem` repository. Push the branch to
your fork or another non-upstream remote first, then open the PR against the upstream target branch.

## Expected Response

Successful responses include PR identifiers and URLs such as `number`, `html_url`, `web_url`, or API `url`. Prefer reporting the browser URL (`html_url` or `web_url`) when available.

## Conflict Detection

After creating the PR, inspect the returned merge fields and, when available, query the created PR detail endpoint:

`GET https://api.gitcode.com/api/v5/repos/{owner}/{repo}/pulls/{number}?access_token=<token>`

Treat values such as `has_conflicts: true` or merge states like `cannot_be_merged`/`conflict` as a conflict. For documentation refresh PRs, conflict means the caller should pull the latest upstream `doc_pages`, replace the generated `docs/zh-cn/latest/` content again, recommit, push, and recreate or update the PR.
