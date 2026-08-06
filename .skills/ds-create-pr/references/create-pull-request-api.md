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

## Source Branch Preparation

Before creating the PR, compare the current source branch with the merge-base of the up-to-date target `--base-ref`:

- zero commits: stop because there is nothing to merge;
- one commit: preserve it and push normally;
- multiple commits: preserve the original HEAD under `codex/pre-squash-*`, create one Conventional Commit with the
  same final tree and the merge-base as its parent, then update an existing fork branch with an explicit
  `--force-with-lease` bound to the observed remote object ID.

Require a clean worktree, an attached branch matching the PR `head`, a base ref matching the PR `base`, and a non-upstream
push remote. Verify the remote branch resolves to local HEAD before calling the PR API.

## Expected Response

Successful responses include PR identifiers and URLs such as `number`, `html_url`, `web_url`, or API `url`. Prefer reporting the browser URL (`html_url` or `web_url`) when available.

## Trigger The Required Gate

Skip `/retest` only when either condition is true:

- the PR target branch is `doc_pages`;
- every changed path is under `.repo_context/` or `docs/`.

Compute the changed paths from the same merge-base range used for commit normalization. Disable rename detection so a
rename from source code into a documentation directory still includes the source path and therefore still requires the
gate. Report the machine-readable skip reason as `base_is_doc_pages` or `docs_or_repo_context_only`.

For all other PRs, post the required comment after successful PR creation.

After a successful PR creation response provides the PR number, create a general PR comment through the issue-comment
endpoint used by GitCode PR timelines:

`POST https://api.gitcode.com/api/v5/repos/{owner}/{repo}/issues/{number}/comments?access_token=<token>`

Set the comment body to exactly `/retest`. If the deployment returns HTTP 404, 405, or 422 because it does not expose
the issue-comment endpoint for PRs, retry once with a body-only request to
`POST /repos/{owner}/{repo}/pulls/{number}/comments`. Do not retry ambiguous network failures because the first request
may already have created the comment. Treat failure of the supported endpoint as partial success: the PR already exists,
so report its URL and post `/retest` to that PR instead of rerunning PR creation.

## Conflict Detection

After creating the PR, inspect the returned merge fields and, when available, query the created PR detail endpoint:

`GET https://api.gitcode.com/api/v5/repos/{owner}/{repo}/pulls/{number}?access_token=<token>`

Treat values such as `has_conflicts: true` or merge states like `cannot_be_merged`/`conflict` as a conflict. For documentation refresh PRs, conflict means the caller should pull the latest upstream `doc_pages`, replace the generated `docs/zh-cn/latest/` content again, recommit, push, and recreate or update the PR.
