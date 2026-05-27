#!/usr/bin/env python3
from __future__ import annotations

import argparse
import ast
import json
from pathlib import Path
from typing import Any

from comment_formatter import format_comment
from common import (
    ReviewError,
    ensure_dir,
    ensure_local_repo,
    expand_path,
    load_settings,
    load_token,
    now_iso,
    parse_pr_ref,
    read_json,
    write_json,
)
from context_builder import build_context_snippets, focus_tags_for_path
from dedupe import compress_suggestions, extract_fingerprint, fingerprint_for_finding
from diff_position import absolute_line_for_position, find_position, parse_patch, render_annotated_patch
from gitcode_api import GitCodeClient
from language_detect import detect_review_language


def _client(settings: dict[str, Any]) -> GitCodeClient:
    token = load_token(settings)
    gitcode = settings["gitcode"]
    return GitCodeClient(
        owner=gitcode["owner"],
        repo=gitcode["repo"],
        token=token,
        base_urls=[gitcode["api_base_url"], gitcode.get("fallback_api_base_url", "")],
    )


def _normalize_patch_payload(file_info: dict[str, Any]) -> tuple[str, dict[str, Any]]:
    raw_patch = file_info.get("patch")
    if isinstance(raw_patch, dict):
        return str(raw_patch.get("diff") or ""), raw_patch

    if isinstance(raw_patch, str):
        stripped = raw_patch.strip()
        if stripped.startswith("{") and stripped.endswith("}"):
            try:
                parsed = ast.literal_eval(stripped)
                if isinstance(parsed, dict):
                    return str(parsed.get("diff") or ""), parsed
            except (SyntaxError, ValueError):
                pass
        return raw_patch, {}

    return "", {}


def _prepare(args: argparse.Namespace) -> int:
    settings = load_settings()
    pr_number = parse_pr_ref(args.pr_ref)
    client = _client(settings)
    repo_path, warnings = ensure_local_repo(settings)

    pull = client.get_pull(pr_number)
    files = client.list_pull_files(pr_number)
    comments = client.list_pull_comments(pr_number)
    language = detect_review_language(str(pull.get("body") or pull.get("description") or ""))

    review_files = []
    review_settings = settings["review"]

    for file_info in files:
        path = str(file_info.get("filename") or file_info.get("path") or "")
        patch, patch_meta = _normalize_patch_payload(file_info)
        position_map = parse_patch(patch)
        local_file = repo_path / path if repo_path else None
        snippets = build_context_snippets(
            local_file=local_file,
            position_map=position_map,
            snippet_radius=int(review_settings["snippet_radius"]),
            max_snippets_per_file=int(review_settings["max_snippets_per_file"]),
            max_chars_per_snippet=int(review_settings["max_chars_per_snippet"]),
        )

        if not patch:
            warnings.append(f"{path} has no patch payload from GitCode; review quality may rely on local context only.")

        review_files.append(
            {
                "path": path,
                "status": file_info.get("status"),
                "additions": file_info.get("additions"),
                "deletions": file_info.get("deletions"),
                "changes": file_info.get("changes"),
                "patch": patch,
                "patch_meta": patch_meta,
                "annotated_patch": render_annotated_patch(position_map),
                "position_map": position_map,
                "focus_tags": focus_tags_for_path(path),
                "local_path": str(local_file) if local_file and local_file.exists() else None,
                "context_snippets": snippets,
            }
        )

    normalized_comments = []
    for comment in comments:
        body = str(comment.get("body") or "")
        normalized_comments.append(
            {
                "id": comment.get("id"),
                "path": comment.get("path"),
                "position": comment.get("position"),
                "body": body,
                "fingerprint": extract_fingerprint(body),
            }
        )

    cache_root = ensure_dir(expand_path(settings["cache"]["root"]))
    bundle_dir = ensure_dir(cache_root / f"pr-{pr_number}")
    bundle_path = bundle_dir / "bundle.json"
    findings_template_path = bundle_dir / "findings.template.json"

    bundle = {
        "generated_at": now_iso(),
        "pr": {
            "number": pr_number,
            "url": pull.get("html_url") or f"{settings['gitcode']['pr_url_prefix']}{pr_number}",
            "title": pull.get("title"),
            "description": pull.get("body") or pull.get("description") or "",
            "state": pull.get("state"),
            "author": (pull.get("user") or {}).get("login"),
            "base_ref": (pull.get("base") or {}).get("ref"),
            "head_ref": (pull.get("head") or {}).get("ref"),
            "language": language,
        },
        "warnings": warnings,
        "repo": {
            "local_path": str(repo_path) if repo_path else None,
            "available": bool(repo_path and repo_path.exists()),
        },
        "review_policy": {
            "need_to_resolve": bool(review_settings["need_to_resolve"]),
            "suggestion_limit_per_file": int(review_settings["suggestion_limit_per_file"]),
        },
        "files": review_files,
        "existing_comments": normalized_comments,
    }
    write_json(bundle_path, bundle)
    write_json(findings_template_path, {"overall_risk": "low", "findings": []})

    output = {
        "bundle_path": str(bundle_path),
        "findings_template_path": str(findings_template_path),
        "pr_number": pr_number,
        "language": language,
        "warnings": warnings,
        "file_count": len(review_files),
        "comment_count": len(normalized_comments),
    }
    print(json.dumps(output, ensure_ascii=False, indent=2))
    return 0


def _bundle_file(bundle: dict[str, Any], path: str) -> dict[str, Any] | None:
    for file_entry in bundle.get("files", []):
        if file_entry.get("path") == path:
            return file_entry
    return None


def _publish(args: argparse.Namespace) -> int:
    settings = load_settings()
    bundle = read_json(Path(args.bundle))
    findings_doc = read_json(Path(args.findings))

    findings = list(findings_doc.get("findings") or [])
    if not findings:
        summary = {
            "overall_risk": findings_doc.get("overall_risk", "low"),
            "posted_line_comments": 0,
            "posted_general_comments": 0,
            "skipped_duplicates": 0,
            "skipped_suggestions": 0,
            "warnings": bundle.get("warnings", []),
        }
        print(json.dumps(summary, ensure_ascii=False, indent=2))
        return 0

    findings = compress_suggestions(findings, int(bundle["review_policy"]["suggestion_limit_per_file"]))

    pr_number = int(bundle["pr"]["number"])
    if args.dry_run:
        existing_comments = bundle.get("existing_comments", [])
        client = None
    else:
        client = _client(settings)
        existing_comments = client.list_pull_comments(pr_number)

    existing_fingerprints = set()
    for comment in existing_comments:
        body = str(comment.get("body") or "")
        fingerprint = extract_fingerprint(body) or comment.get("fingerprint")
        if fingerprint:
            existing_fingerprints.add(str(fingerprint))

    language = bundle["pr"]["language"]
    posted_line_comments = 0
    posted_general_comments = 0
    skipped_duplicates = 0
    skipped_suggestions = 0
    dry_run_comments: list[dict[str, Any]] = []

    for finding in findings:
        fingerprint = fingerprint_for_finding(finding)
        if fingerprint in existing_fingerprints:
            skipped_duplicates += 1
            continue

        body = format_comment(finding, language, fingerprint)
        file_entry = _bundle_file(bundle, str(finding.get("path", "")))
        position_entry = None
        absolute_line = None

        if file_entry:
            position_entry = find_position(
                position_map=file_entry.get("position_map", []),
                diff_line_index=finding.get("diff_line_index"),
                line=finding.get("line"),
                match_text=finding.get("match_text"),
            )
            absolute_line = absolute_line_for_position(position_entry)

        try:
            if file_entry and position_entry and absolute_line is not None:
                if args.dry_run:
                    dry_run_comments.append(
                        {
                            "mode": "line",
                            "path": file_entry["path"],
                            "position": absolute_line,
                            "position_kind": "absolute_line",
                            "resolved_from_diff_position": int(position_entry["position"]),
                            "resolved_old_line": position_entry.get("old_line"),
                            "resolved_new_line": position_entry.get("new_line"),
                            "body": body,
                        }
                    )
                else:
                    assert client is not None
                    client.post_pull_comment(
                        number=pr_number,
                        body=body,
                        path=file_entry["path"],
                        absolute_line=absolute_line,
                        need_to_resolve=bool(bundle["review_policy"]["need_to_resolve"]),
                    )
                posted_line_comments += 1
            else:
                if args.dry_run:
                    dry_run_comments.append(
                        {
                            "mode": "general",
                            "path": finding.get("path"),
                            "body": body,
                        }
                    )
                else:
                    assert client is not None
                    client.post_general_comment(pr_number, body)
                posted_general_comments += 1
        except Exception as exc:
            if finding.get("severity") == "suggestion":
                skipped_suggestions += 1
                continue
            raise ReviewError(f"Failed to publish finding for {finding.get('path')}: {exc}") from exc

        existing_fingerprints.add(fingerprint)

    summary = {
        "overall_risk": findings_doc.get("overall_risk", "low"),
        "posted_line_comments": posted_line_comments,
        "posted_general_comments": posted_general_comments,
        "skipped_duplicates": skipped_duplicates,
        "skipped_suggestions": skipped_suggestions,
        "warnings": bundle.get("warnings", []),
        "dry_run": bool(args.dry_run),
    }
    if args.dry_run:
        summary["planned_comments"] = dry_run_comments
    summary_path = Path(args.bundle).with_name("publish-summary.json")
    write_json(summary_path, summary)
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Prepare and publish YuanRong GitCode PR reviews.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    prepare_parser = subparsers.add_parser("prepare", help="Fetch PR data and build a review bundle.")
    prepare_parser.add_argument("pr_ref", help="PR number or GitCode PR URL.")
    prepare_parser.set_defaults(func=_prepare)

    publish_parser = subparsers.add_parser("publish", help="Publish findings back to GitCode.")
    publish_parser.add_argument("--bundle", required=True, help="Path to bundle.json produced by prepare.")
    publish_parser.add_argument("--findings", required=True, help="Path to findings JSON matching the output contract.")
    publish_parser.add_argument("--dry-run", action="store_true", help="Render the comments without posting them.")
    publish_parser.set_defaults(func=_publish)

    return parser


def main() -> int:
    parser = _build_parser()
    args = parser.parse_args()
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
