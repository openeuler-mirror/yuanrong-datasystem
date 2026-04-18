---
name: ds-refresh-docs
description: Build and refresh the openYuanrong datasystem online Chinese documentation. Use when the user asks to update, refresh, or publish the online Chinese docs, especially when Codex needs to compile docs/source_zh_cn from the latest upstream master, sync build_zh_cn/html into doc_pages/docs/zh-cn/latest, create a documentation refresh commit, push a refresh branch to a fork, and automatically open a GitCode PR using $ds-create-pr.
---

# Refresh Online Docs

## Workflow

1. Treat `docs/README_CN.md` as the source of truth for the publishing flow.
2. Read `references/online-docs-refresh.md` for the exact repository-specific rules.
3. Run the bundled script to refresh the docs, push the refresh branch to a fork or another non-upstream remote, and automatically open the PR:

   ```bash
   python3 .skills/ds-refresh-docs/scripts/refresh_online_docs.py
   ```

   The script must fetch the latest upstream `master` from `https://gitcode.com/openeuler/yuanrong-datasystem.git` or `git@gitcode.com:openeuler/yuanrong-datasystem.git`, checks out that upstream `master` in a separate source worktree, builds Chinese docs there, creates a branch from the latest upstream `doc_pages`, syncs the source worktree's `docs/build_zh_cn/html/` to `docs/zh-cn/latest/` in the `doc_pages` worktree with `rsync -a --delete`, commits the refresh, pushes the branch to the configured non-upstream remote unless `--no-push` is passed, and then automatically invokes `$ds-create-pr`.

4. Reuse the generated `PR_BODY_FILE`; it already follows `.gitee/PULL_REQUEST_TEMPLATE/PULL_REQUEST_TEMPLATE.zh-cn.md` and should not be replaced with an ad-hoc summary. The PR description must explicitly record which upstream `master` commit was used to generate the docs and the corresponding generation time; the script writes both fields automatically before calling `$ds-create-pr`.

5. Report the commit SHA, pushed branch, and PR URL.

## Required Checks

- Verify `docs/build_zh_cn/html/index.html` exists before syncing.
- Use a trailing slash on the source path: `build_zh_cn/html/`.
- Use `rsync -a --delete` so hidden files are copied and target-side stale files are removed.
- Do not commit from the main development branch; commit the generated pages on a branch based on `doc_pages`.
- Before building, update a dedicated source worktree from the latest upstream `master` fetched from `https://gitcode.com/openeuler/yuanrong-datasystem.git` or `git@gitcode.com:openeuler/yuanrong-datasystem.git`; do not build from a stale local development branch.
- Before committing generated pages, update the publication worktree from the latest upstream `doc_pages`.
- In the PR description, record both the upstream `master` commit used for generation and the generation timestamp.
- Push the refresh branch to a fork or another non-upstream remote; never push the branch to the upstream `openeuler/yuanrong-datasystem` repository.
- Let the script call `$ds-create-pr` automatically after the push. If `$ds-create-pr` reports conflicts, refresh from upstream `doc_pages`, regenerate the sync commit, push again, and rerun `ds-refresh-docs`.
- If there are no generated page changes, do not create a PR.

## Common Options

- Use `--skip-build` only when the dedicated upstream `master` source worktree already contains the inspected `docs/build_zh_cn/html/` artifacts that will be published.
- Use `--no-push` when the user wants to inspect the worktree commit before publishing; PR creation is skipped in that mode.
- Use `--source-worktree ../yuanrong-datasystem-docs-master` to control where the latest upstream `master` source worktree is created.
- Use `--worktree ../doc_pages` to match the path used in `docs/README_CN.md`.
- Use `--upstream-remote openeuler --upstream-url https://gitcode.com/openeuler/yuanrong-datasystem.git` when the upstream remote is not already configured.
- Use `--upstream-url git@gitcode.com:openeuler/yuanrong-datasystem.git` if the environment requires SSH, but do not use any non-upstream fork URL for the build source.
- Use `--remote <fork-remote>` when the PR source branch should be pushed through a non-default fork remote before the script opens the PR.
