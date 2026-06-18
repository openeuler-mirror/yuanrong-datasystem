#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import re
import sys
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


DEFAULT_OWNER = "openeuler"
DEFAULT_REPO = "yuanrong-datasystem"
DEFAULT_API_BASE = "https://api.gitcode.com/api/v5"
DEFAULT_TOKEN_FILE = "~/.local/gitcode_token"


class IntakeError(RuntimeError):
    pass


def expand_path(value: str) -> Path:
    return Path(os.path.expandvars(os.path.expanduser(value))).resolve()


def load_token(token_file: str | None) -> str | None:
    for name in ("GITCODE_TOKEN", "GITCODE_ACCESS_TOKEN", "GITCODE_TOEKEN"):
        token = os.environ.get(name, "").strip()
        if token:
            return token
    path = expand_path(token_file or DEFAULT_TOKEN_FILE)
    if path.exists():
        token = path.read_text(encoding="utf-8").strip()
        if token:
            return token
    return None


def display_output_path(value: str) -> str:
    text = value.strip()
    if not text:
        return "<output>"
    if re.match(r"(?<![\w.-])(?:/(?:Users|home|root|tmp|var|mnt|opt|workspace|Volumes)(?:/.*)?|[A-Za-z]:\\.*)", text):
        return "<local task spec path>"
    return text


def parse_issue_ref(value: str) -> tuple[str | None, str | None, int]:
    text = value.strip()
    if text.isdigit():
        return None, None, int(text)
    match = re.search(r"gitcode\.com/([^/]+)/([^/]+)/issues/(\d+)", text)
    if match:
        return match.group(1), match.group(2), int(match.group(3))
    match = re.search(r"/issues/(\d+)", text)
    if match:
        return None, None, int(match.group(1))
    raise IntakeError(f"Unsupported issue reference: {value}")


def redact_text(value: str) -> tuple[str, list[str]]:
    rules: list[tuple[str, str, str]] = [
        (r"(?i)(access[_-]?token|gitcode[_-]?token|password|passwd|secret|ak|sk)\s*[:=]\s*\S+", r"\1=<REDACTED>", "secret-like value"),
        (r"-----BEGIN [A-Z0-9 ]*PRIVATE KEY-----.*?-----END [A-Z0-9 ]*PRIVATE KEY-----", "<REDACTED_PRIVATE_KEY>", "private key"),
        (r"\b(?:\d{1,3}\.){3}\d{1,3}:\d{2,5}\b", "<REDACTED_HOST_PORT>", "host:port"),
        (r"\b(?:\d{1,3}\.){3}\d{1,3}\b", "<REDACTED_IP>", "ip address"),
        (r"\b[a-zA-Z0-9_.-]+@(?:[a-zA-Z0-9_.-]+|\<REDACTED_IP\>)\b", "<REDACTED_SSH_TARGET>", "ssh target"),
        (r"(?<![\w.-])/(?:Users|home)/[^\s`'\"]+", "<REDACTED_LOCAL_PATH>", "local absolute path"),
    ]
    found: list[str] = []
    redacted = value or ""
    for pattern, replacement, label in rules:
        redacted, count = re.subn(pattern, replacement, redacted, flags=re.DOTALL)
        if count:
            found.append(label)
    return redacted, sorted(set(found))


def request_json(api_base: str, owner: str, repo: str, issue: int, token: str | None) -> dict[str, Any]:
    path = f"/repos/{owner}/{repo}/issues/{issue}"
    url = api_base.rstrip("/") + path
    headers = {"Accept": "application/json", "User-Agent": "ds-issue-intake/0.1"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
        headers["PRIVATE-TOKEN"] = token
    request = urllib.request.Request(url, headers=headers, method="GET")
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            return json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        if exc.code in {401, 403, 404} and not token:
            raise IntakeError(
                "GitCode issue fetch needs credentials or the issue is not public. "
                "Set GITCODE_TOKEN or GITCODE_ACCESS_TOKEN, or create ~/.local/gitcode_token. "
                "Do not paste the token into chat."
            ) from exc
        raise IntakeError(f"GitCode issue fetch failed: HTTP {exc.code}: {detail}") from exc
    except urllib.error.URLError as exc:
        raise IntakeError(f"GitCode issue fetch failed: {exc}") from exc


def likely_kind(title: str, labels: list[str]) -> str:
    lower = (title + " " + " ".join(labels)).lower()
    if "bug" in lower or "缺陷" in lower:
        return "/kind fix"
    if "rfc" in lower or "feat" in lower or "需求" in lower:
        return "/kind feat"
    if "doc" in lower or "文档" in lower:
        return "/kind docs"
    return "/kind chore"


def search_terms(title: str, body: str) -> list[str]:
    text = title + "\n" + body
    candidates = re.findall(r"`([^`]{2,80})`", text)
    candidates += re.findall(r"-{1,2}([A-Za-z_][A-Za-z0-9_]{2,80})", text)
    candidates += re.findall(r"\b[A-Za-z_][A-Za-z0-9_]{2,80}\b", text)
    stop = {
        "bug", "issue", "test", "true", "false", "actual", "expected", "image", "png", "python", "pytest",
        "time", "start_time", "end_time", "config", "value", "print", "format", "path", "worker_out",
        "images", "disable", "warnings", "https", "raw", "atomgit", "com", "user", "assets", "namespace",
    }
    terms: list[str] = []
    for item in candidates:
        term = item.strip(" -:：,，。")
        if len(term) < 3 or term.lower() in stop:
            continue
        if len(term) >= 8 and re.fullmatch(r"[0-9a-fA-F_-]+", term):
            continue
        if term not in terms:
            terms.append(term)
    return terms[:20]


def summarize(body: str) -> str:
    lines = [line.strip() for line in body.splitlines() if line.strip()]
    return "\n".join(lines[:12])


def build_task(issue_payload: dict[str, Any], owner: str, repo: str, redacted_body: str, redactions: list[str]) -> dict[str, Any]:
    labels = [str(item.get("name", item)) for item in issue_payload.get("labels") or []]
    title = str(issue_payload.get("title") or "")
    number = int(issue_payload.get("number") or issue_payload.get("id"))
    url = issue_payload.get("html_url") or f"https://gitcode.com/{owner}/{repo}/issues/{number}"
    return {
        "version": 1,
        "generated_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        "source": {"owner": owner, "repo": repo, "issue": number, "url": url},
        "issue": {
            "number": number,
            "state": issue_payload.get("state"),
            "title": title,
            "labels": labels,
            "body_redacted": redacted_body,
            "summary": summarize(redacted_body),
        },
        "classification": {
            "pr_kind": likely_kind(title, labels),
            "source_search_terms": search_terms(title, redacted_body),
            "redactions_applied": redactions,
        },
        "next_steps": [
            "Confirm the issue claim against repository source before editing.",
            "Use the infrastructure engineering workflow before code changes.",
            "Choose targeted validation from touched source paths.",
            "Keep PR body and review replies free of private hosts, paths, tokens, and raw sensitive logs.",
        ],
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fetch and sanitize a GitCode issue into a development task spec.")
    parser.add_argument("--issue", required=True, help="Issue number or GitCode issue URL.")
    parser.add_argument("--owner", default=DEFAULT_OWNER)
    parser.add_argument("--repo", default=DEFAULT_REPO)
    parser.add_argument("--api-base", default=DEFAULT_API_BASE)
    parser.add_argument("--token-file", default=DEFAULT_TOKEN_FILE)
    parser.add_argument("--output", help="Write JSON task spec to this path.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    try:
        ref_owner, ref_repo, issue_number = parse_issue_ref(args.issue)
        owner = ref_owner or args.owner
        repo = ref_repo or args.repo
        token = load_token(args.token_file)
        payload = request_json(args.api_base, owner, repo, issue_number, token)
        body, redactions = redact_text(str(payload.get("body") or ""))
        task = build_task(payload, owner, repo, body, redactions)
        rendered = json.dumps(task, ensure_ascii=False, indent=2) + "\n"
        if args.output:
            output = expand_path(args.output)
            output.parent.mkdir(parents=True, exist_ok=True)
            output.write_text(rendered, encoding="utf-8")
            print(f"Wrote sanitized task spec: {display_output_path(args.output)}")
        else:
            print(rendered, end="")
        return 0
    except IntakeError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
