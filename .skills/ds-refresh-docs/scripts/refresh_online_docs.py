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


UPSTREAM_URL = "https://gitcode.com/openeuler/yuanrong-datasystem.git"


def ensure_remote(repo_root: Path, remote: str, url: str) -> None:
    existing = subprocess.run(
        ["git", "remote", "get-url", remote],
        cwd=repo_root,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
    )
    if existing.returncode != 0:
        run(["git", "remote", "add", remote, url], repo_root)
    elif existing.stdout.strip() != url:
        raise SystemExit(
            f"Remote '{remote}' points to {existing.stdout.strip()}, expected {url}. "
            "Use --upstream-remote with a remote that points to the upstream repository."
        )


def fetch_branch(repo_root: Path, remote: str, branch: str) -> None:
    refspec = f"refs/heads/{branch}:refs/remotes/{remote}/{branch}"
    run(["git", "fetch", remote, refspec], repo_root)


def prepare_source_worktree(repo_root: Path, worktree: Path, branch: str, upstream_remote: str) -> Path:
    fetch_branch(repo_root, upstream_remote, branch)
    local_branch = f"docs-source-{branch}"
    if worktree.exists():
        if not (worktree / ".git").exists():
            raise SystemExit(f"Source worktree path exists but is not a git worktree: {worktree}")
        ensure_clean_git_worktree(worktree)
        run(["git", "checkout", "-B", local_branch, f"{upstream_remote}/{branch}"], worktree)
    else:
        run(["git", "worktree", "add", "-B", local_branch, str(worktree), f"{upstream_remote}/{branch}"], repo_root)
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


def write_pr_body(path: Path, branch: str, commit: str) -> None:
    path.write_text(
        "\n".join(
            [
                "## Summary",
                "",
                "- Refresh zh-cn latest online documentation pages.",
                "- Sync docs/build_zh_cn/html/ to docs/zh-cn/latest/ on doc_pages.",
                "",
                "## Verification",
                "",
                "- Built with `make html` unless `--skip-build` was used.",
                "- Source docs were refreshed from upstream master before building.",
                "- Generated pages were refreshed from upstream doc_pages before committing.",
                f"- Refresh branch: `{branch}`",
                f"- Refresh commit: `{commit}`",
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
    parser.add_argument("--upstream-url", default=UPSTREAM_URL)
    parser.add_argument("--source-branch", default="master")
    parser.add_argument("--target-branch", default="doc_pages")
    parser.add_argument("--commit-message", default="docs: refresh zh-cn latest pages")
    parser.add_argument("--skip-build", action="store_true")
    parser.add_argument("--no-push", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    repo_root = repo_root_from_args(args.repo_root)
    ensure_remote(repo_root, args.upstream_remote, args.upstream_url)
    source_worktree = (repo_root / args.source_worktree).resolve()
    docs_source_root = repo_root
    if not args.skip_build:
        docs_source_root = prepare_source_worktree(repo_root, source_worktree, args.source_branch, args.upstream_remote)
    docs_dir = docs_source_root / "docs"
    source_dir = docs_dir / "build_zh_cn" / "html"
    worktree = (repo_root / args.worktree).resolve()
    target_dir = worktree / "docs" / "zh-cn" / "latest"

    if not args.skip_build:
        run(["make", "html"], docs_dir)
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
    write_pr_body(pr_body, args.branch, commit)
    print(f"REFRESH_BRANCH={args.branch}")
    print(f"REFRESH_COMMIT={commit}")
    print(f"PR_BODY_FILE={pr_body}")
    print(f"Create the PR with $ds-create-pr using base={args.target_branch} and head=REFRESH_BRANCH.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
