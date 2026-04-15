#!/usr/bin/env python3
"""Create a GitCode Pull Request with the v5 OpenAPI."""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urlencode
from urllib.request import Request, urlopen


TOKEN_ENV_NAMES = ("GITCODE_TOKEN", "GITCODE_ACCESS_TOKEN")
DEFAULT_TOKEN_FILE = Path.home() / ".local" / "gitcode_token"


def load_token(explicit_token: str | None, token_file: Path | None) -> str:
    if explicit_token:
        return explicit_token.strip()
    for name in TOKEN_ENV_NAMES:
        value = os.environ.get(name)
        if value:
            return value.strip()
    path = token_file or DEFAULT_TOKEN_FILE
    if path.exists():
        return path.read_text(encoding="utf-8").strip()
    raise SystemExit(
        "Missing GitCode token. Set GITCODE_TOKEN/GITCODE_ACCESS_TOKEN "
        f"or create {path}."
    )


def read_body(args: argparse.Namespace) -> str | None:
    if args.body_file:
        return Path(args.body_file).read_text(encoding="utf-8")
    return args.body


def put_if_present(payload: dict[str, Any], key: str, value: Any) -> None:
    if value is not None:
        payload[key] = value


def build_payload(args: argparse.Namespace) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "title": args.title,
        "head": args.head,
        "base": args.base,
    }
    put_if_present(payload, "body", read_body(args))
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
        body = exc.read().decode("utf-8", errors="replace")
        raise SystemExit(f"GitCode API failed: HTTP {exc.code}\n{body}") from exc
    except URLError as exc:
        raise SystemExit(f"GitCode API request failed: {exc}") from exc
    return json.loads(raw) if raw else {}


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


def create_pr(args: argparse.Namespace) -> tuple[dict[str, Any], dict[str, Any] | None]:
    token = load_token(args.token, args.token_file)
    owner = quote(args.owner, safe="")
    repo = quote(args.repo, safe="")
    base_url = args.api_base.rstrip("/")
    query = urlencode({"access_token": token})
    url = f"{base_url}/repos/{owner}/{repo}/pulls?{query}"
    result = request_json("POST", url, build_payload(args), args.timeout)
    conflict_info = check_pr_conflict(args, token, result) if args.check_conflicts else None
    return result, conflict_info


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--owner", required=True)
    parser.add_argument("--repo", required=True)
    parser.add_argument("--title", required=True)
    parser.add_argument("--head", required=True)
    parser.add_argument("--base", required=True)
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
    parser.add_argument("--timeout", type=int, default=30)
    parser.add_argument("--check-conflicts", action=argparse.BooleanOptionalAction, default=True)
    return parser.parse_args()


def main() -> int:
    result, conflict_info = create_pr(parse_args())
    print(json.dumps(result, ensure_ascii=False, indent=2))
    url = result.get("html_url") or result.get("web_url") or result.get("url")
    if url:
        print(f"PR URL: {url}")
    info = conflict_info or result
    if detect_conflict(info):
        print("CONFLICT_STATUS=conflict")
        print("The PR appears to have conflicts. Refresh doc_pages from upstream and regenerate the docs refresh commit.")
        return 2
    if conflict_info is not None:
        print("CONFLICT_STATUS=clean")
    return 0


if __name__ == "__main__":
    sys.exit(main())
