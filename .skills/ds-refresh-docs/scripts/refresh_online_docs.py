#!/usr/bin/env python3
"""Build, sync, commit, and optionally push the online Chinese docs refresh."""

from __future__ import annotations

import argparse
import datetime as dt
import subprocess
import sys
from pathlib import Path


def run(cmd: list[str], cwd: Path, check: bool = True) -> subprocess.CompletedProcess[str]:
    print(f"+ ({cwd}) {' '.join(cmd)}")
    return subprocess.run(cmd, cwd=cwd, check=check, text=True)


def capture(cmd: list[str], cwd: Path) -> str:
    return subprocess.check_output(cmd, cwd=cwd, text=True).strip()


def ensure_clean_git_worktree(path: Path) -> None:
    status = capture(["git", "status", "--short"], path)
    if status:
        raise SystemExit(f"Git worktree is not clean: {path}\n{status}")


def repo_root_from_args(value: str | None) -> Path:
    if value:
        return Path(value).resolve()
    return Path(capture(["git", "rev-parse", "--show-toplevel"], Path.cwd())).resolve()


DEFAULT_UPSTREAM_URL = "https://gitcode.com/openeuler/yuanrong-datasystem.git"
ALLOWED_UPSTREAM_URLS = {
    DEFAULT_UPSTREAM_URL,
    "git@gitcode.com:openeuler/yuanrong-datasystem.git",
}
UPSTREAM_OWNER = "openeuler"
UPSTREAM_REPO = "yuanrong-datasystem"
CREATE_PR_SCRIPT = Path(__file__).resolve().parents[2] / "ds-create-pr" / "scripts" / "create_pr.py"


def validate_upstream_url(url: str) -> str:
    normalized = url.strip()
    if normalized in ALLOWED_UPSTREAM_URLS:
        return normalized
    supported = ", ".join(sorted(ALLOWED_UPSTREAM_URLS))
    raise SystemExit(
        "Online docs refresh must build from the latest upstream master of "
        f"openeuler/yuanrong-datasystem. Use one of: {supported}."
    )


def ensure_remote(repo_root: Path, remote: str, url: str) -> str:
    requested_url = validate_upstream_url(url)
    existing = subprocess.run(
        ["git", "remote", "get-url", remote],
        cwd=repo_root,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
    )
    if existing.returncode != 0:
        run(["git", "remote", "add", remote, requested_url], repo_root)
        return requested_url
    existing_url = existing.stdout.strip()
    if existing_url not in ALLOWED_UPSTREAM_URLS:
        supported = ", ".join(sorted(ALLOWED_UPSTREAM_URLS))
        raise SystemExit(
            f"Remote '{remote}' points to {existing_url}, but docs refresh must use one of: {supported}. "
            "Use --upstream-remote with a remote that points to the upstream repository."
        )
    return existing_url


def parse_gitcode_remote(url: str) -> tuple[str, str]:
    normalized = url.strip()
    prefix = ""
    if normalized.startswith("git@gitcode.com:"):
        prefix = "git@gitcode.com:"
    elif normalized.startswith("https://gitcode.com/"):
        prefix = "https://gitcode.com/"
    else:
        raise SystemExit(
            f"Unsupported PR source remote URL: {normalized}. "
            "Use a GitCode fork remote such as git@gitcode.com:<owner>/yuanrong-datasystem.git."
        )
    path = normalized[len(prefix):]
    if path.endswith(".git"):
        path = path[:-4]
    parts = path.split("/")
    if len(parts) != 2 or not parts[0] or not parts[1]:
        raise SystemExit(f"Invalid GitCode remote URL for PR source branch: {normalized}")
    return parts[0], parts[1]


def resolve_pr_source_remote(repo_root: Path, remote: str) -> tuple[str, str, str]:
    existing = subprocess.run(
        ["git", "remote", "get-url", remote],
        cwd=repo_root,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
    )
    if existing.returncode != 0:
        raise SystemExit(f"Push remote '{remote}' is not configured.")
    remote_url = existing.stdout.strip()
    owner, repo = parse_gitcode_remote(remote_url)
    if (owner, repo) == (UPSTREAM_OWNER, UPSTREAM_REPO):
        raise SystemExit(
            f"Push remote '{remote}' points to upstream {remote_url}. "
            "Push the refresh branch to your fork or another non-upstream remote before creating the PR."
        )
    return remote_url, owner, repo


def fetch_branch(repo_root: Path, remote: str, branch: str) -> None:
    refspec = f"refs/heads/{branch}:refs/remotes/{remote}/{branch}"
    run(["git", "fetch", remote, refspec], repo_root)


def prepare_source_worktree(repo_root: Path, worktree: Path, branch: str, upstream_remote: str) -> Path:
    fetch_branch(repo_root, upstream_remote, branch)
    if worktree.exists():
        if not (worktree / ".git").exists():
            raise SystemExit(f"Source worktree path exists but is not a git worktree: {worktree}")
        ensure_clean_git_worktree(worktree)
        run(["git", "checkout", "--detach", f"{upstream_remote}/{branch}"], worktree)
    else:
        run(["git", "worktree", "add", "--detach", str(worktree), f"{upstream_remote}/{branch}"], repo_root)
    return worktree


def prepare_worktree(repo_root: Path, worktree: Path, branch: str, upstream_remote: str, target_branch: str) -> None:
    fetch_branch(repo_root, upstream_remote, target_branch)
    if worktree.exists():
        if not (worktree / ".git").exists():
            raise SystemExit(f"Worktree path exists but is not a git worktree: {worktree}")
        ensure_clean_git_worktree(worktree)
        run(["git", "checkout", "-B", branch, f"{upstream_remote}/{target_branch}"], worktree)
        return
    run(["git", "worktree", "add", "-B", branch, str(worktree), f"{upstream_remote}/{target_branch}"], repo_root)


def generated_at_text() -> str:
    return dt.datetime.now().astimezone().isoformat(timespec="seconds")


def create_pr_command(
    target_branch: str,
    branch: str,
    pr_title: str,
    pr_body: Path,
    fork_owner: str,
    fork_repo: str,
) -> list[str]:
    return [
        sys.executable,
        str(CREATE_PR_SCRIPT),
        "--owner",
        UPSTREAM_OWNER,
        "--repo",
        UPSTREAM_REPO,
        "--base",
        target_branch,
        "--head",
        f"{fork_owner}:{branch}",
        "--title",
        pr_title,
        "--body-file",
        str(pr_body),
        "--fork-path",
        f"{fork_owner}/{fork_repo}",
        "--check-conflicts",
    ]


def write_pr_body(
    path: Path,
    branch: str,
    commit: str,
    upstream_url: str,
    source_master_commit: str,
    docs_generated_at: str,
    built_with_make_html: bool,
) -> None:
    build_line = (
        "- [x] **验证**：PR描述已包含详细验证结果，文档构建产物已核对并同步到 `docs/zh-cn/latest/`"
    )
    verification_detail = (
        "- 在最新上游 `master` 工作树中执行了 `make html`，并确认 `docs/build_zh_cn/html/index.html` 存在。"
        if built_with_make_html
        else "- 使用现有 `docs/build_zh_cn/html/` 产物执行同步，并确认 `docs/build_zh_cn/html/index.html` 存在。"
    )
    path.write_text(
        "\n".join(
            [
                "**这是什么类型的PR？**",
                "",
                "/kind docs",
                "",
                "----",
                "",
                "**这个PR是做什么的/我们为什么需要它**",
                "",
                "- 刷新在线中文文档发布页 `docs/zh-cn/latest/`。",
                f"- 生成来源固定为上游仓 `{upstream_url}` 的最新 `master` 分支代码。",
                f"- 文档生成基线 `master` commit：`{source_master_commit}`。",
                f"- 文档生成时间：`{docs_generated_at}`。",
                "- 发布分支固定基于上游最新 `doc_pages`，避免从本地开发分支或过期工作树生成页面。",
                "验证结果：",
                verification_detail,
                "- 同步时使用 `rsync -a --delete`，确保隐藏文件被复制且目标目录中的陈旧页面被清理。",
                f"- 刷新分支：`{branch}`",
                f"- 刷新提交：`{commit}`",
                "",
                "----",
                "",
                "**此PR修复了哪些问题**:",
                "",
                "Fixes #",
                "",
                "----",
                "",
                "**PR对程序接口进行了哪些修改？**",
                "",
                "无。",
                "",
                "----",
                "",
                "**Self-checklist**:（**请自检，在[ ]内打上x，我们将检视你的完成情况，否则会导致pr无法合入**）",
                "",
                "+ - [ ] **设计**：PR对应的方案是否已经经过Maintainer评审，方案检视意见是否均已答复并完成方案修改",
                "+ - [ ] **测试**：PR中的代码是否已有UT/ST测试用例进行充分的覆盖，新增测试用例是否随本PR一并上库或已经上库",
                build_line,
                "+ - [ ] **接口**：是否涉及对外接口变更，相应变更已得到接口评审组织的通过，API对应的注释信息已经刷新正确",
                "+ - [x] **文档**：涉及官网文档修改，已刷新在线文档发布内容",
                "",
            ]
        ),
        encoding="utf-8",
    )


def parse_args() -> argparse.Namespace:
    timestamp = dt.datetime.now().strftime("%Y%m%d-%H%M%S")
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root")
    parser.add_argument("--source-worktree", default="../yuanrong-datasystem-docs-master")
    parser.add_argument("--worktree", default="../doc_pages")
    parser.add_argument("--branch", default=f"docs-refresh-zh-cn-latest-{timestamp}")
    parser.add_argument("--remote", default="origin")
    parser.add_argument("--upstream-remote", default="openeuler")
    parser.add_argument("--upstream-url", default=DEFAULT_UPSTREAM_URL)
    parser.add_argument("--source-branch", default="master")
    parser.add_argument("--target-branch", default="doc_pages")
    parser.add_argument("--commit-message", default="docs: refresh zh-cn latest pages")
    parser.add_argument("--pr-title", default="docs: refresh zh-cn latest pages")
    parser.add_argument("--skip-build", action="store_true")
    parser.add_argument("--no-push", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    repo_root = repo_root_from_args(args.repo_root)
    upstream_url = ensure_remote(repo_root, args.upstream_remote, args.upstream_url)
    push_remote_url, fork_owner, fork_repo = resolve_pr_source_remote(repo_root, args.remote)
    source_worktree = (repo_root / args.source_worktree).resolve()
    docs_source_root = prepare_source_worktree(repo_root, source_worktree, args.source_branch, args.upstream_remote)
    docs_dir = docs_source_root / "docs"
    source_dir = docs_dir / "build_zh_cn" / "html"
    worktree = (repo_root / args.worktree).resolve()
    target_dir = worktree / "docs" / "zh-cn" / "latest"
    source_master_commit = capture(["git", "rev-parse", "HEAD"], docs_source_root)

    if not args.skip_build:
        run(["make", "html"], docs_dir)
    docs_generated_at = generated_at_text()
    index_html = source_dir / "index.html"
    if not index_html.exists():
        raise SystemExit(f"Missing generated documentation entrypoint: {index_html}")

    prepare_worktree(repo_root, worktree, args.branch, args.upstream_remote, args.target_branch)
    target_dir.mkdir(parents=True, exist_ok=True)
    run(["rsync", "-a", "--delete", f"{source_dir}/", f"{target_dir}/"], repo_root)

    status = capture(["git", "status", "--short", "docs/zh-cn/latest"], worktree)
    if not status:
        print("No online documentation changes detected; nothing to commit.")
        return 0

    run(["git", "add", "docs/zh-cn/latest"], worktree)
    run(["git", "commit", "-m", args.commit_message], worktree)
    commit = capture(["git", "rev-parse", "HEAD"], worktree)
    if not args.no_push:
        run(["git", "push", "-u", args.remote, args.branch], worktree)

    pr_body = worktree / "pr-body.md"
    write_pr_body(
        pr_body,
        args.branch,
        commit,
        upstream_url,
        source_master_commit,
        docs_generated_at,
        not args.skip_build,
    )
    print(f"REFRESH_BRANCH={args.branch}")
    print(f"REFRESH_COMMIT={commit}")
    print(f"SOURCE_MASTER_COMMIT={source_master_commit}")
    print(f"DOCS_GENERATED_AT={docs_generated_at}")
    print(f"PR_SOURCE_REMOTE={args.remote}")
    print(f"PR_SOURCE_REMOTE_URL={push_remote_url}")
    print(f"PR_BODY_FILE={pr_body}")
    if args.no_push:
        print("PR creation skipped because --no-push was used.")
        return 0
    run(
        create_pr_command(args.target_branch, args.branch, args.pr_title, pr_body, fork_owner, fork_repo),
        repo_root,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
