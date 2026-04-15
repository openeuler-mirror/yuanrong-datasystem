---
name: ds-refresh-docs
description: Build and refresh the openYuanrong datasystem online Chinese documentation. Use when Codex needs to compile docs/source_zh_cn, sync build_zh_cn/html into doc_pages/docs/zh-cn/latest, create a documentation refresh commit, push a refresh branch, and open a GitCode PR using $ds-create-pr.
---

# Refresh Online Docs

## Workflow

1. Treat `docs/README_CN.md` as the source of truth for the publishing flow.
2. Read `references/online-docs-refresh.md` for the exact repository-specific rules.
3. Prepare the documentation refresh commit with the bundled script:

   ```bash
   python3 .skills/ds-refresh-docs/scripts/refresh_online_docs.py
   ```

   The script first fetches `https://gitcode.com/openeuler/yuanrong-datasystem.git`, checks out the latest upstream `master` in a separate source worktree, builds Chinese docs there, creates a branch from the latest upstream `doc_pages`, syncs the source worktree's `docs/build_zh_cn/html/` to `docs/zh-cn/latest/` in the `doc_pages` worktree with `rsync -a --delete`, commits the refresh, and pushes the branch unless `--no-push` is passed.

4. Use `$ds-create-pr` to create a PR:

   ```bash
   python3 .skills/ds-create-pr/scripts/create_pr.py \
     --owner openeuler \
     --repo yuanrong-datasystem \
     --base doc_pages \
     --head <printed-refresh-branch> \
     --title "docs: refresh zh-cn latest pages" \
     --body-file <printed-pr-body-file> \
     --check-conflicts
   ```

5. Report the commit SHA, pushed branch, and PR URL.

## Required Checks

- Verify `docs/build_zh_cn/html/index.html` exists before syncing.
- Use a trailing slash on the source path: `build_zh_cn/html/`.
- Use `rsync -a --delete` so hidden files are copied and target-side stale files are removed.
- Do not commit from the main development branch; commit the generated pages on a branch based on `doc_pages`.
- Before building, update a dedicated source worktree from the latest upstream `master`; do not build from a stale local development branch.
- Before committing generated pages, update the publication worktree from the latest upstream `doc_pages`.
- Let `$ds-create-pr` perform PR conflict detection. If it reports conflicts, refresh from upstream `doc_pages`, regenerate the sync commit, push again, and recreate or update the PR.
- If there are no generated page changes, do not create a PR.

## Common Options

- Use `--skip-build` when `docs/build_zh_cn/html/` has already been generated and inspected.
- Use `--no-push` when the user wants to inspect the worktree commit before publishing.
- Use `--source-worktree ../yuanrong-datasystem-docs-master` to control where the latest upstream `master` source worktree is created.
- Use `--worktree ../doc_pages` to match the path used in `docs/README_CN.md`.
- Use `--upstream-remote openeuler --upstream-url https://gitcode.com/openeuler/yuanrong-datasystem.git` when the upstream remote is not already configured.
