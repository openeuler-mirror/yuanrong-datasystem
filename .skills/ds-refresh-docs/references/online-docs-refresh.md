# Online Documentation Refresh

Source of truth: `docs/README_CN.md`

## Repository Defaults

- Repository: `https://gitcode.com/openeuler/yuanrong-datasystem`
- Allowed upstream Git URLs: `https://gitcode.com/openeuler/yuanrong-datasystem.git`, `git@gitcode.com:openeuler/yuanrong-datasystem.git`
- Online docs branch: `doc_pages`
- Generated Chinese docs: `docs/build_zh_cn/html/`
- Online target directory on `doc_pages`: `docs/zh-cn/latest/`
- Default commit message: `docs: refresh zh-cn latest pages`
- Default PR base branch: `doc_pages`

## Required Flow

1. Fetch the latest upstream `master` from `https://gitcode.com/openeuler/yuanrong-datasystem.git` or `git@gitcode.com:openeuler/yuanrong-datasystem.git`, then create or update a dedicated source worktree from that upstream `master`.
2. Build Chinese documentation from that source worktree's `docs/` directory:

   ```bash
   make html
   ```

3. Fetch `https://gitcode.com/openeuler/yuanrong-datasystem.git` and create a refresh branch based on the latest upstream `doc_pages`.
4. Copy the contents of `docs/build_zh_cn/html/` into `docs/zh-cn/latest/` on that branch:

   ```bash
   rsync -a --delete build_zh_cn/html/ <doc_pages-worktree>/docs/zh-cn/latest/
   ```

5. Commit all generated page changes on the refresh branch.
6. Push the refresh branch to your fork or another non-upstream remote, not to the upstream `openeuler/yuanrong-datasystem` repository.
7. Invoke `$ds-create-pr` immediately after the push so the workflow ends with an opened PR. The PR body must follow `.gitee/PULL_REQUEST_TEMPLATE/PULL_REQUEST_TEMPLATE.zh-cn.md`, and it must record the exact upstream `master` commit used for generation plus the generation timestamp.
8. If `$ds-create-pr` reports conflicts, refresh from the latest upstream `doc_pages`, replace `docs/zh-cn/latest/` again, recommit, push, and rerun the workflow.

## Why `rsync -a --delete`

The source path has a trailing slash, so the contents of `build_zh_cn/html/` are copied directly into `latest/`. Archive mode copies files, directories, and hidden files. `--delete` removes target files that no longer exist in the source, preventing stale online pages.

This synchronizes the generated file contents and directory structure for publishing. It does not promise that every filesystem metadata field is byte-for-byte identical across different filesystems.
