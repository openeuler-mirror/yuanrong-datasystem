#!/usr/bin/env python3
"""Create a GitCode Pull Request with the v5 OpenAPI."""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urlencode
from urllib.request import Request, urlopen


TOKEN_ENV_NAMES = ("GITCODE_TOKEN", "GITCODE_ACCESS_TOKEN")
DEFAULT_TOKEN_FILE = Path.home() / ".local" / "gitcode_token"
DISPLAY_TOKEN_FILE = "~/.local/gitcode_token"
REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_PR_TEMPLATE_FILE = (
    REPO_ROOT / ".gitee" / "PULL_REQUEST_TEMPLATE" / "PULL_REQUEST_TEMPLATE.zh-cn.md"
)
RETEST_COMMENT = "/retest"
CONVENTIONAL_COMMIT_PATTERN = re.compile(
    r"^(?:feat|fix|docs|refactor|perf|test|build|chore)(?:\([^)]+\))?!?:\s+\S"
)
RETEST_SKIP_PATH_PREFIXES = (".repo_context/", "docs/")


def redact_text(value: str) -> str:
    redacted = value or ""
    redacted = re.sub(r"(?i)(access_token=)[^&\s]+", r"\1<REDACTED>", redacted)
    redacted = re.sub(
        r"(?i)(token|password|passwd|secret|access[_ -]?key|secret[_ -]?key|ak|sk)\s*[:=]\s*\S+",
        r"\1=<REDACTED>",
        redacted,
    )
    redacted = re.sub(
        r"(?<![\w.-])/(?:Users|home|root|tmp|var|mnt|opt|workspace|Volumes)(?:/[^\s`'\"<>]+)+",
        "<REDACTED_PATH>",
        redacted,
    )
    return redacted
REPO_TEMPLATE_TARGET = ("openeuler", "yuanrong-datasystem")
SENSITIVE_CONTENT_PATTERNS: tuple[tuple[str, re.Pattern[str]], ...] = (
    (
        "server IP or endpoint",
        re.compile(r"\b(?:25[0-5]|2[0-4]\d|1?\d?\d)(?:\.(?:25[0-5]|2[0-4]\d|1?\d?\d)){3}(?::\d{1,5})?\b"),
    ),
    (
        "local filesystem path",
        re.compile(
            r"(?<![\w.-])(?:/(?:Users|home|root|tmp|var|mnt|opt|workspace|Volumes)(?:/[^\s`'\"<>]+)+"
            r"|[A-Za-z]:\\(?:Users|workspace|tmp|temp)\\[^\s`'\"<>]+)"
        ),
    ),
    (
        "credential assignment",
        re.compile(
            r"(?i)\b(?:password|passwd|pwd|secret|token|access[_ -]?key|secret[_ -]?key|"
            r"system[_ -]?access[_ -]?key|system[_ -]?secret[_ -]?key|"
            r"tenant[_ -]?access[_ -]?key|tenant[_ -]?secret[_ -]?key|ak|sk|"
            r"username|user|account)\s*[:=]"
        ),
    ),
    (
        "private or ssh key",
        re.compile(
            r"(?i)-----BEGIN [A-Z0-9 ]*PRIVATE KEY-----|"
            r"BEGIN OPENSSH PRIVATE KEY|"
            r"ssh-rsa\s+[A-Za-z0-9+/=]{20,}"
        ),
    ),
)


def require_non_empty_token(token: str, source: str, fallback_path: Path) -> str:
    stripped = token.strip()
    if stripped:
        return stripped
    raise SystemExit(
        f"{source} is configured but empty. Set a non-empty GitCode token there, "
        f"or remove it and use GITCODE_TOKEN/GITCODE_ACCESS_TOKEN or {DISPLAY_TOKEN_FILE}."
    )


def load_token(explicit_token: str | None, token_file: Path | None) -> str:
    path = token_file or DEFAULT_TOKEN_FILE
    if explicit_token is not None:
        return require_non_empty_token(explicit_token, "--token", path)
    for name in TOKEN_ENV_NAMES:
        if name in os.environ:
            return require_non_empty_token(os.environ[name], name, path)
    if token_file is not None and not path.exists():
        raise SystemExit(
            "GitCode token file not found. Create a local token file with a non-empty token, "
            f"or set GITCODE_TOKEN/GITCODE_ACCESS_TOKEN. Default token file: {DISPLAY_TOKEN_FILE}."
        )
    if path.exists():
        return require_non_empty_token(path.read_text(encoding="utf-8"), "GitCode token file", path)
    raise SystemExit(
        "Missing GitCode token. Set GITCODE_TOKEN/GITCODE_ACCESS_TOKEN "
        f"or create {DISPLAY_TOKEN_FILE}. Do not paste the token into chat."
    )


def read_body(args: argparse.Namespace) -> str | None:
    if args.body_file:
        return Path(args.body_file).read_text(encoding="utf-8")
    return args.body


def resolve_template_file(args: argparse.Namespace) -> Path | None:
    if args.body_template_file is not None:
        return args.body_template_file.resolve()
    if (args.owner, args.repo) == REPO_TEMPLATE_TARGET and DEFAULT_PR_TEMPLATE_FILE.exists():
        return DEFAULT_PR_TEMPLATE_FILE
    return None


def template_required_sections(template_text: str) -> list[str]:
    sections: list[str] = []
    for raw_line in template_text.splitlines():
        line = raw_line.strip()
        # Match standalone **bold** headings (entire line is bold text, not inline bold within a sentence)
        match = re.match(r"^(\*\*.+?\*\*:?)$", line)
        if match:
            sections.append(match.group(1))
            continue
        # Match ## numbered section headings (new template), skip optional sections marked with (如有)/(if any)
        match = re.match(r"(##\s+\d+\.\s+.+)", line)
        if match:
            heading = match.group(1).strip()
            # Skip optional sections — template comment says "Bugfix 如无遗留可删除此章节"
            if re.search(r"[(（]如有[)）]|\(if any\)", heading):
                continue
            sections.append(heading)
    return sections


def validate_pr_body(body: str | None, template_path: Path | None) -> str | None:
    if template_path is None:
        return body
    if not template_path.exists():
        raise SystemExit(f"PR body template file not found: {template_path}")
    if body is None or not body.strip():
        raise SystemExit(
            "PR body is required for this repository. Provide --body/--body-file with the "
            "template filled in, or generate one from "
            f"{template_path}."
        )
    template_text = template_path.read_text(encoding="utf-8")
    missing_sections = [section for section in template_required_sections(template_text) if section not in body]
    if missing_sections:
        raise SystemExit(
            "PR body does not follow the required template. Missing sections: "
            + ", ".join(missing_sections)
            + f". Use {template_path}."
        )
    return body


def validate_no_sensitive_content(fields: dict[str, str | None]) -> None:
    violations: list[str] = []
    for field_name, value in fields.items():
        if not value:
            continue
        matched_categories = [
            category for category, pattern in SENSITIVE_CONTENT_PATTERNS if pattern.search(value)
        ]
        if matched_categories:
            violations.append(f"{field_name}: {', '.join(matched_categories)}")
    if violations:
        raise SystemExit(
            "Sensitive information is not allowed in commit messages or PR descriptions. "
            "Remove or redact these categories before creating the PR: "
            + "; ".join(violations)
        )


def put_if_present(payload: dict[str, Any], key: str, value: Any) -> None:
    if value is not None:
        payload[key] = value


def build_payload(args: argparse.Namespace) -> dict[str, Any]:
    body = validate_pr_body(read_body(args), resolve_template_file(args))
    validate_no_sensitive_content(
        {
            "PR title": args.title,
            "PR body": body,
            "squash commit message": args.squash_commit_message,
        }
    )
    payload: dict[str, Any] = {
        "title": args.title,
        "head": args.head,
        "base": args.base,
    }
    put_if_present(payload, "body", body)
    put_if_present(payload, "milestone_number", args.milestone_number)
    put_if_present(payload, "labels", args.labels)
    put_if_present(payload, "issue", args.issue)
    put_if_present(payload, "assignees", args.assignees)
    put_if_present(payload, "testers", args.testers)
    put_if_present(payload, "prune_source_branch", args.prune_source_branch)
    put_if_present(payload, "draft", args.draft)
    put_if_present(payload, "squash", args.squash)
    put_if_present(payload, "squash_commit_message", args.squash_commit_message)
    put_if_present(payload, "fork_path", args.fork_path)
    return payload


def request_json(method: str, url: str, payload: dict[str, Any] | None, timeout: int) -> dict[str, Any]:
    data = json.dumps(payload).encode("utf-8") if payload is not None else None
    request = Request(
        url,
        data=data,
        headers={"Content-Type": "application/json"},
        method=method,
    )
    try:
        with urlopen(request, timeout=timeout) as response:
            raw = response.read().decode("utf-8")
    except HTTPError as exc:
        body = redact_text(exc.read().decode("utf-8", errors="replace"))
        raise SystemExit(f"GitCode API failed: HTTP {exc.code}\n{body}") from exc
    except URLError as exc:
        raise SystemExit(f"GitCode API request failed: {exc}") from exc
    return json.loads(raw) if raw else {}


def run_git(worktree: Path, *args: str) -> str:
    try:
        result = subprocess.run(
            ["git", "-C", str(worktree), *args],
            check=True,
            capture_output=True,
            text=True,
        )
    except subprocess.CalledProcessError as exc:
        detail = redact_text((exc.stderr or exc.stdout or "").strip())
        command = " ".join(args)
        raise SystemExit(f"Git command failed: git {command}\n{detail}".rstrip()) from exc
    return result.stdout.strip()


def is_upstream_repository_url(url: str, owner: str, repo: str) -> bool:
    normalized = url.strip().rstrip("/")
    return bool(
        re.search(
            rf"gitcode\.com[:/]{re.escape(owner)}/{re.escape(repo)}(?:\.git)?$",
            normalized,
            re.IGNORECASE,
        )
    )


def validate_local_squash_message(message: str | None) -> str:
    normalized = (message or "").strip()
    if not normalized:
        raise SystemExit(
            "Multiple source commits require --local-squash-message with one Conventional Commit message."
        )
    if not CONVENTIONAL_COMMIT_PATTERN.match(normalized):
        raise SystemExit(
            "--local-squash-message must use Conventional Commits: "
            "type(scope): description, where type is feat/fix/docs/refactor/perf/test/build/chore."
        )
    if re.search(r"(?im)^Co-Authored-By:", normalized):
        raise SystemExit("--local-squash-message must not include Co-Authored-By trailers.")
    validate_no_sensitive_content({"local squash commit message": normalized})
    return normalized


def retest_skip_reason(base: str, changed_files: list[str]) -> str | None:
    if base == "doc_pages":
        return "base_is_doc_pages"
    if changed_files and all(
        path in {".repo_context", "docs"}
        or path.startswith(RETEST_SKIP_PATH_PREFIXES)
        for path in changed_files
    ):
        return "docs_or_repo_context_only"
    return None


def prepare_branch_for_pr(args: argparse.Namespace) -> dict[str, Any]:
    worktree = Path(args.git_worktree).resolve()
    if run_git(worktree, "status", "--porcelain"):
        raise SystemExit("The source worktree must be clean before preparing and pushing a PR branch.")

    branch = run_git(worktree, "branch", "--show-current")
    if not branch:
        raise SystemExit("Cannot prepare a PR branch from detached HEAD.")
    remote_branch = str(args.head).rsplit(":", 1)[-1]
    if branch != remote_branch:
        raise SystemExit(
            f"Current branch {branch!r} does not match the PR head branch {remote_branch!r}."
        )
    base_ref = str(args.base_ref).rstrip("/")
    if base_ref != args.base and not base_ref.endswith(f"/{args.base}"):
        raise SystemExit(
            f"Base ref {args.base_ref!r} does not match the PR base branch {args.base!r}."
        )

    push_url = run_git(worktree, "remote", "get-url", "--push", args.push_remote)
    if (args.owner, args.repo) == REPO_TEMPLATE_TARGET and is_upstream_repository_url(
        push_url, args.owner, args.repo
    ):
        raise SystemExit(
            f"Refusing to push the source branch to upstream remote {args.push_remote!r}; use a fork remote."
        )

    base_commit = run_git(worktree, "rev-parse", "--verify", f"{args.base_ref}^{{commit}}")
    merge_base = run_git(worktree, "merge-base", base_commit, "HEAD")
    commit_count_before = int(run_git(worktree, "rev-list", "--count", f"{merge_base}..HEAD"))
    if commit_count_before == 0:
        raise SystemExit(f"The source branch has no commits to merge relative to {args.base_ref!r}.")

    old_head = run_git(worktree, "rev-parse", "HEAD")
    backup_ref: str | None = None
    squashed = commit_count_before > 1
    if squashed:
        message = validate_local_squash_message(args.local_squash_message)
        branch_slug = re.sub(r"[^A-Za-z0-9._-]+", "-", branch).strip("-") or "branch"
        backup_ref = f"refs/heads/codex/pre-squash-{branch_slug}-{old_head[:12]}"
        run_git(worktree, "update-ref", backup_ref, old_head)
        tree = run_git(worktree, "rev-parse", "HEAD^{tree}")
        new_commit = run_git(worktree, "commit-tree", tree, "-p", merge_base, "-m", message)
        run_git(worktree, "update-ref", f"refs/heads/{branch}", new_commit, old_head)
        if run_git(worktree, "status", "--porcelain"):
            raise SystemExit(
                f"Squash created an unexpected dirty worktree. Restore with: git reset --hard {backup_ref}"
            )

    commit_count_after = int(run_git(worktree, "rev-list", "--count", f"{merge_base}..HEAD"))
    if commit_count_after != 1:
        raise SystemExit(
            f"Expected exactly one source commit after preparation, found {commit_count_after}."
        )

    changed_files = run_git(
        worktree,
        "diff",
        "--name-only",
        "--no-renames",
        f"{merge_base}..HEAD",
    ).splitlines()
    skip_reason = retest_skip_reason(args.base, changed_files)

    remote_ref = f"refs/heads/{remote_branch}"
    remote_line = run_git(worktree, "ls-remote", "--heads", args.push_remote, remote_ref)
    remote_oid = remote_line.split()[0] if remote_line else ""
    push_args = ["push", "--set-upstream"]
    if squashed and remote_oid:
        push_args.append(f"--force-with-lease={remote_ref}:{remote_oid}")
    push_args.extend([args.push_remote, f"HEAD:{remote_ref}"])
    run_git(worktree, *push_args)

    head = run_git(worktree, "rev-parse", "HEAD")
    pushed_line = run_git(worktree, "ls-remote", "--heads", args.push_remote, remote_ref)
    pushed_oid = pushed_line.split()[0] if pushed_line else ""
    if pushed_oid != head:
        raise SystemExit("Remote branch verification failed after push; do not create the PR.")

    return {
        "backup_ref": backup_ref,
        "commit_count_before": commit_count_before,
        "commit_count_after": commit_count_after,
        "changed_files": changed_files,
        "retest_skip_reason": skip_reason,
        "squashed": squashed,
    }


def pr_number(result: dict[str, Any]) -> Any:
    return result.get("number") or result.get("iid") or result.get("id")


def detect_conflict(result: dict[str, Any]) -> bool:
    merge_status = str(result.get("merge_status", "")).lower()
    has_conflicts = result.get("has_conflicts")
    if has_conflicts is True:
        return True
    return merge_status in {"cannot_be_merged", "conflict", "unchecked_conflict", "cannot_merge"}


def check_pr_conflict(args: argparse.Namespace, token: str, result: dict[str, Any]) -> dict[str, Any] | None:
    number = pr_number(result)
    if not number:
        return None
    owner = quote(args.owner, safe="")
    repo = quote(args.repo, safe="")
    base_url = args.api_base.rstrip("/")
    query = urlencode({"access_token": token})
    url = f"{base_url}/repos/{owner}/{repo}/pulls/{quote(str(number), safe='')}?{query}"
    try:
        return request_json("GET", url, None, args.timeout)
    except SystemExit:
        # Some GitCode deployments return enough merge information from create.
        return result


def post_retest_comment(args: argparse.Namespace, token: str, result: dict[str, Any]) -> dict[str, Any]:
    number = pr_number(result)
    pr_url = result.get("html_url") or result.get("web_url") or result.get("url") or "<unknown>"
    if not number:
        raise SystemExit(
            f"PR created at {pr_url}, but the response did not contain a PR number; "
            f"post {RETEST_COMMENT} manually to trigger the required gate."
        )
    owner = quote(args.owner, safe="")
    repo = quote(args.repo, safe="")
    base_url = args.api_base.rstrip("/")
    query = urlencode({"access_token": token})
    url = f"{base_url}/repos/{owner}/{repo}/issues/{quote(str(number), safe='')}/comments?{query}"
    try:
        return request_json("POST", url, {"body": RETEST_COMMENT}, args.timeout)
    except SystemExit as exc:
        if re.search(r"GitCode API failed: HTTP (?:404|405|422)\b", str(exc)):
            fallback_url = (
                f"{base_url}/repos/{owner}/{repo}/pulls/"
                f"{quote(str(number), safe='')}/comments?{query}"
            )
            try:
                return request_json("POST", fallback_url, {"body": RETEST_COMMENT}, args.timeout)
            except SystemExit as fallback_exc:
                exc = fallback_exc
        raise SystemExit(
            f"PR created at {pr_url}, but posting {RETEST_COMMENT} failed: {exc}. "
            "Do not create a duplicate PR; post the comment to the existing PR."
        ) from exc


def create_pr(args: argparse.Namespace) -> tuple[dict[str, Any], dict[str, Any] | None]:
    token = load_token(args.token, args.token_file)
    owner = quote(args.owner, safe="")
    repo = quote(args.repo, safe="")
    base_url = args.api_base.rstrip("/")
    query = urlencode({"access_token": token})
    url = f"{base_url}/repos/{owner}/{repo}/pulls?{query}"
    result = request_json("POST", url, build_payload(args), args.timeout)
    if not getattr(args, "skip_retest", False):
        post_retest_comment(args, token, result)
    conflict_info = check_pr_conflict(args, token, result) if args.check_conflicts else None
    return result, conflict_info


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--owner", required=True)
    parser.add_argument("--repo", required=True)
    parser.add_argument("--title", required=True)
    parser.add_argument("--head", required=True)
    parser.add_argument("--base", required=True)
    parser.add_argument("--base-ref", required=True, help="Local or remote-tracking ref for the target base branch.")
    parser.add_argument("--push-remote", required=True, help="Fork remote that receives the prepared source branch.")
    parser.add_argument("--git-worktree", type=Path, default=Path.cwd())
    parser.add_argument("--local-squash-message")
    parser.add_argument("--body")
    parser.add_argument("--body-file")
    parser.add_argument("--milestone-number", type=int)
    parser.add_argument("--labels")
    parser.add_argument("--issue")
    parser.add_argument("--assignees")
    parser.add_argument("--testers")
    parser.add_argument("--prune-source-branch", action="store_true")
    parser.add_argument("--draft", action="store_true")
    parser.add_argument("--squash", action="store_true")
    parser.add_argument("--squash-commit-message")
    parser.add_argument("--fork-path")
    parser.add_argument("--api-base", default="https://api.gitcode.com/api/v5")
    parser.add_argument("--token")
    parser.add_argument("--token-file", type=Path)
    parser.add_argument("--body-template-file", type=Path)
    parser.add_argument("--timeout", type=int, default=30)
    parser.add_argument("--check-conflicts", action=argparse.BooleanOptionalAction, default=True)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    build_payload(args)
    load_token(args.token, args.token_file)
    branch_info = prepare_branch_for_pr(args)
    print(f"SOURCE_COMMIT_COUNT_BEFORE={branch_info['commit_count_before']}")
    print(f"SOURCE_COMMIT_COUNT_AFTER={branch_info['commit_count_after']}")
    print(f"SOURCE_SQUASH_STATUS={'squashed' if branch_info['squashed'] else 'unchanged'}")
    if branch_info["backup_ref"]:
        print(f"SOURCE_SQUASH_BACKUP_REF={branch_info['backup_ref']}")
    print("SOURCE_PUSH_STATUS=verified")
    args.skip_retest = branch_info["retest_skip_reason"] is not None
    result, conflict_info = create_pr(args)
    print(json.dumps(result, ensure_ascii=False, indent=2))
    url = result.get("html_url") or result.get("web_url") or result.get("url")
    if url:
        print(f"PR URL: {url}")
    if args.skip_retest:
        print("RETEST_COMMENT_STATUS=skipped")
        print(f"RETEST_SKIP_REASON={branch_info['retest_skip_reason']}")
    else:
        print("RETEST_COMMENT_STATUS=posted")
    info = conflict_info or result
    if detect_conflict(info):
        print("CONFLICT_STATUS=conflict")
        print(
            "The PR appears to have conflicts. Refresh doc_pages from upstream "
            "and regenerate the docs refresh commit."
        )
        return 2
    if conflict_info is not None:
        print("CONFLICT_STATUS=clean")
    return 0


if __name__ == "__main__":
    sys.exit(main())
