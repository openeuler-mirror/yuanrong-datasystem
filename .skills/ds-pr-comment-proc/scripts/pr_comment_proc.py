#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import sys
import tomllib
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any


DEFAULT_OWNER = "openeuler"
DEFAULT_REPO = "yuanrong-datasystem"
DEFAULT_API_BASE = "https://api.gitcode.com/api/v5"
DEFAULT_FALLBACK_API_BASE = "https://api.atomgit.com/api/v5"
DEFAULT_TOKEN_FILE = "~/.local/gitcode_token"


class ProcError(RuntimeError):
    pass


def expand_path(value: str | None) -> Path | None:
    if not value:
        return None
    return Path(os.path.expandvars(os.path.expanduser(value))).resolve()


def find_default_config(start: Path) -> Path | None:
    for root in [start, *start.parents]:
        candidate = root / ".skills" / "ds-pr-review" / "assets" / "default_config.toml"
        if candidate.exists():
            return candidate
    return None


def load_config(args: argparse.Namespace) -> dict[str, Any]:
    config_path = expand_path(args.config) if args.config else find_default_config(Path.cwd())
    settings: dict[str, Any] = {
        "gitcode": {
            "owner": DEFAULT_OWNER,
            "repo": DEFAULT_REPO,
            "api_base_url": DEFAULT_API_BASE,
            "fallback_api_base_url": DEFAULT_FALLBACK_API_BASE,
            "token_file": DEFAULT_TOKEN_FILE,
        }
    }
    if config_path and config_path.exists():
        settings = tomllib.loads(config_path.read_text(encoding="utf-8"))
    gitcode = settings.setdefault("gitcode", {})
    for attr, key in (
        ("owner", "owner"),
        ("repo", "repo"),
        ("api_base", "api_base_url"),
        ("fallback_api_base", "fallback_api_base_url"),
        ("token_file", "token_file"),
    ):
        value = getattr(args, attr, None)
        if value:
            gitcode[key] = value
    return settings


def load_token(settings: dict[str, Any]) -> str:
    for name in ("GITCODE_TOKEN", "GITCODE_ACCESS_TOKEN", "GITCODE_TOEKEN"):
        token = os.environ.get(name, "").strip()
        if token:
            return token
    token_file = expand_path(str(settings["gitcode"].get("token_file") or DEFAULT_TOKEN_FILE))
    if token_file and token_file.exists():
        token = token_file.read_text(encoding="utf-8").strip()
        if token:
            return token
    raise ProcError(
        "No GitCode token found. Set GITCODE_TOKEN or GITCODE_ACCESS_TOKEN, "
        "or put the token in ~/.local/gitcode_token. Do not paste the token into chat."
    )


def parse_pr(value: str | int) -> int:
    text = str(value).strip()
    if text.isdigit():
        return int(text)
    for marker in ("/pull/", "/pulls/", "/merge_requests/"):
        if marker in text:
            tail = text.split(marker, 1)[1].split("/", 1)[0].split("?", 1)[0]
            if tail.isdigit():
                return int(tail)
    raise ProcError(f"Unsupported PR reference: {value}")


class GitCodeClient:
    def __init__(self, settings: dict[str, Any]):
        gitcode = settings["gitcode"]
        self.owner = gitcode.get("owner") or DEFAULT_OWNER
        self.repo = gitcode.get("repo") or DEFAULT_REPO
        self.token = load_token(settings)
        self.base_urls = [
            str(gitcode.get("api_base_url") or DEFAULT_API_BASE).rstrip("/"),
            str(gitcode.get("fallback_api_base_url") or "").rstrip("/"),
        ]
        self.base_urls = [url for url in self.base_urls if url]

    def headers(self) -> dict[str, str]:
        return {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self.token}",
            "PRIVATE-TOKEN": self.token,
            "User-Agent": "ds-pr-comment-proc/0.1",
        }

    def request(self, method: str, path: str, data: dict[str, Any] | None = None) -> Any:
        body = None if data is None else json.dumps(data).encode("utf-8")
        last_error: Exception | None = None
        for base_url in self.base_urls:
            request = urllib.request.Request(f"{base_url}{path}", data=body, headers=self.headers(), method=method)
            try:
                with urllib.request.urlopen(request, timeout=30) as response:
                    payload = response.read()
                    if not payload:
                        return None
                    return json.loads(payload.decode("utf-8"))
            except urllib.error.HTTPError as exc:
                last_error = exc
                if exc.code not in {401, 403, 404, 405, 422, 500, 502, 503}:
                    raise
            except Exception as exc:
                last_error = exc
        if isinstance(last_error, urllib.error.HTTPError):
            detail = last_error.read().decode("utf-8", errors="replace")
            raise ProcError(f"GitCode API {method} {path} failed: HTTP {last_error.code}: {detail}") from last_error
        if last_error:
            raise ProcError(f"GitCode API {method} {path} failed: {last_error}") from last_error
        raise ProcError(f"GitCode API {method} {path} failed")

    def paginate(self, path: str) -> list[dict[str, Any]]:
        page = 1
        items: list[dict[str, Any]] = []
        while True:
            query = urllib.parse.urlencode({"page": page, "per_page": 100})
            payload = self.request("GET", f"{path}?{query}")
            if not payload:
                break
            if not isinstance(payload, list):
                raise ProcError(f"Expected list response for {path}, got {type(payload).__name__}")
            items.extend(payload)
            if len(payload) < 100:
                break
            page += 1
        return items

    def list_comments(self, pr: int) -> list[dict[str, Any]]:
        return self.paginate(f"/repos/{self.owner}/{self.repo}/pulls/{pr}/comments")

    def reply_discussion(self, pr: int, discussion_id: str, body: str) -> Any:
        return self.request("POST", f"/repos/{self.owner}/{self.repo}/pulls/{pr}/discussions/{discussion_id}/comments",
                            {"body": body})

    def resolve_discussion(self, pr: int, discussion_id: str, resolved: bool = True) -> Any:
        return self.request("PUT", f"/repos/{self.owner}/{self.repo}/pulls/{pr}/comments/{discussion_id}",
                            {"resolved": resolved})


def body_from_item(item: dict[str, Any]) -> str | None:
    if item.get("reply") is not None:
        return str(item["reply"])
    if item.get("body") is not None:
        return str(item["body"])
    reply_file = item.get("reply_file") or item.get("body_file")
    if reply_file:
        path = expand_path(str(reply_file))
        if not path:
            raise ProcError(f"Invalid reply file: {reply_file}")
        return path.read_text(encoding="utf-8").strip()
    return None


def find_comment(comments: list[dict[str, Any]], *, comment_id: int | None = None,
                 discussion_id: str | None = None) -> dict[str, Any]:
    if comment_id is None and discussion_id is None:
        raise ProcError("A comment_id or discussion_id is required")
    for comment in comments:
        if comment_id is not None and int(comment.get("id", -1)) == comment_id:
            return comment
        if discussion_id is not None and str(comment.get("discussion_id") or "") == discussion_id:
            return comment
    raise ProcError(f"Cannot find comment_id={comment_id} discussion_id={discussion_id}")


def item_ids(item: dict[str, Any]) -> tuple[int | None, str | None]:
    comment_id = item.get("comment_id") or item.get("id")
    return (int(comment_id) if comment_id is not None else None, item.get("discussion_id"))


def resolve_discussion_id(comments: list[dict[str, Any]], item: dict[str, Any]) -> str:
    comment_id, discussion_id = item_ids(item)
    if discussion_id:
        return str(discussion_id)
    if comment_id is None:
        raise ProcError("Each item must provide comment_id, id, or discussion_id")
    comment = find_comment(comments, comment_id=comment_id)
    discussion_id = comment.get("discussion_id")
    if not discussion_id:
        raise ProcError(f"Comment {comment_id} has no discussion_id")
    return str(discussion_id)


def compact_comment(comment: dict[str, Any], with_replies: bool = False) -> dict[str, Any]:
    body = str(comment.get("body") or "")
    out = {
        "id": comment.get("id"),
        "discussion_id": comment.get("discussion_id"),
        "type": comment.get("comment_type"),
        "resolved": comment.get("resolved"),
        "created_at": comment.get("created_at"),
        "updated_at": comment.get("updated_at"),
        "body": body,
        "reply_count": len(comment.get("reply") or []),
    }
    if comment.get("diff_position"):
        out["diff_position"] = comment.get("diff_position")
    if with_replies:
        out["reply"] = comment.get("reply") or []
    return out


def load_plan(path: str) -> dict[str, Any]:
    plan_path = expand_path(path)
    if not plan_path or not plan_path.exists():
        raise ProcError(f"Plan file does not exist: {path}")
    plan = json.loads(plan_path.read_text(encoding="utf-8"))
    if "items" not in plan or not isinstance(plan["items"], list):
        raise ProcError("Plan must contain an items array")
    plan["pr"] = parse_pr(plan.get("pr"))
    return plan


def command_list(args: argparse.Namespace, client: GitCodeClient) -> int:
    pr = parse_pr(args.pr)
    comments = client.list_comments(pr)
    if args.ids:
        wanted = {int(x) for raw in args.ids for x in str(raw).split(",") if x}
        comments = [comment for comment in comments if int(comment.get("id", -1)) in wanted]
    if args.unresolved:
        comments = [comment for comment in comments if comment.get("resolved") is False]
    payload = [compact_comment(comment, args.with_replies) for comment in comments]
    if args.json:
        print(json.dumps(payload, ensure_ascii=False, indent=2))
    else:
        for item in payload:
            status = "resolved" if item["resolved"] is True else "open" if item["resolved"] is False else "n/a"
            first_line = str(item["body"]).splitlines()[0] if item["body"] else ""
            print(f"{item['id']} discussion={item['discussion_id']} {status} replies={item['reply_count']}")
            if first_line:
                print(f"  {first_line}")
    return 0


def command_reply(args: argparse.Namespace, client: GitCodeClient) -> int:
    pr = parse_pr(args.pr)
    body = args.body or (expand_path(args.body_file).read_text(encoding="utf-8").strip() if args.body_file else None)
    if not body:
        raise ProcError("reply requires --body or --body-file")
    comments = client.list_comments(pr)
    discussion_id = resolve_discussion_id(comments, {"comment_id": args.comment_id, "discussion_id": args.discussion_id})
    print(json.dumps({"action": "reply", "pr": pr, "discussion_id": discussion_id, "apply": args.apply},
                     ensure_ascii=False))
    if args.apply:
        client.reply_discussion(pr, discussion_id, body)
    return 0


def command_resolve(args: argparse.Namespace, client: GitCodeClient) -> int:
    pr = parse_pr(args.pr)
    comments = client.list_comments(pr)
    discussion_id = resolve_discussion_id(comments, {"comment_id": args.comment_id, "discussion_id": args.discussion_id})
    print(json.dumps({"action": "resolve", "pr": pr, "discussion_id": discussion_id, "apply": args.apply},
                     ensure_ascii=False))
    if args.apply:
        client.resolve_discussion(pr, discussion_id, True)
    return 0


def command_process(args: argparse.Namespace, client: GitCodeClient) -> int:
    plan = load_plan(args.plan)
    pr = int(plan["pr"])
    comments = client.list_comments(pr)
    actions: list[dict[str, Any]] = []
    for item in plan["items"]:
        discussion_id = resolve_discussion_id(comments, item)
        reply = body_from_item(item)
        if reply:
            actions.append({"type": "reply", "discussion_id": discussion_id, "body_len": len(reply)})
            if args.apply:
                client.reply_discussion(pr, discussion_id, reply)
        if item.get("resolve", True):
            actions.append({"type": "resolve", "discussion_id": discussion_id})
            if args.apply:
                client.resolve_discussion(pr, discussion_id, True)
    print(json.dumps({"pr": pr, "apply": args.apply, "actions": actions}, ensure_ascii=False, indent=2))
    return 0


def command_verify(args: argparse.Namespace, client: GitCodeClient) -> int:
    plan = load_plan(args.plan)
    pr = int(plan["pr"])
    comments = client.list_comments(pr)
    failed = False
    results = []
    for item in plan["items"]:
        comment_id, discussion_id = item_ids(item)
        comment = find_comment(comments, comment_id=comment_id, discussion_id=discussion_id)
        replies = comment.get("reply") or []
        expect_reply = body_from_item(item) is not None
        expect_resolved = bool(item.get("resolve", True))
        ok = not (expect_resolved and comment.get("resolved") is not True) and not (expect_reply and not replies)
        failed = failed or not ok
        results.append({
            "comment_id": comment.get("id"),
            "discussion_id": comment.get("discussion_id"),
            "resolved": comment.get("resolved"),
            "reply_count": len(replies),
            "ok": ok,
        })
    print(json.dumps({"pr": pr, "ok": not failed, "results": results}, ensure_ascii=False, indent=2))
    return 1 if failed else 0


def add_common(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--config", help="Path to ds-pr-review default_config.toml or compatible GitCode config.")
    parser.add_argument("--owner", help="Override GitCode owner.")
    parser.add_argument("--repo", help="Override GitCode repo.")
    parser.add_argument("--api-base", help="Override GitCode API base URL.")
    parser.add_argument("--fallback-api-base", help="Override fallback API base URL.")
    parser.add_argument("--token-file", help="Override GitCode token file.")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Fetch, reply to, resolve, and verify GitCode PR review comments.")
    add_common(parser)
    sub = parser.add_subparsers(dest="command", required=True)
    list_parser = sub.add_parser("list", help="List PR review comments.")
    list_parser.add_argument("--pr", required=True)
    list_parser.add_argument("--unresolved", action="store_true")
    list_parser.add_argument("--with-replies", action="store_true")
    list_parser.add_argument("--json", action="store_true")
    list_parser.add_argument("--ids", nargs="*", help="Comment ids, comma-separated or space-separated.")
    list_parser.set_defaults(func=command_list)
    reply_parser = sub.add_parser("reply", help="Reply under one PR review discussion.")
    reply_parser.add_argument("--pr", required=True)
    reply_parser.add_argument("--comment-id", type=int)
    reply_parser.add_argument("--discussion-id")
    reply_parser.add_argument("--body")
    reply_parser.add_argument("--body-file")
    reply_parser.add_argument("--apply", action="store_true")
    reply_parser.set_defaults(func=command_reply)
    resolve_parser = sub.add_parser("resolve", help="Mark one PR review discussion resolved.")
    resolve_parser.add_argument("--pr", required=True)
    resolve_parser.add_argument("--comment-id", type=int)
    resolve_parser.add_argument("--discussion-id")
    resolve_parser.add_argument("--apply", action="store_true")
    resolve_parser.set_defaults(func=command_resolve)
    process_parser = sub.add_parser("process", help="Reply and resolve multiple comments from a JSON plan.")
    process_parser.add_argument("--plan", required=True)
    process_parser.add_argument("--apply", action="store_true")
    process_parser.set_defaults(func=command_process)
    verify_parser = sub.add_parser("verify", help="Verify planned comments are replied and resolved.")
    verify_parser.add_argument("--plan", required=True)
    verify_parser.set_defaults(func=command_verify)
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    try:
        client = GitCodeClient(load_config(args))
        return args.func(args, client)
    except ProcError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
