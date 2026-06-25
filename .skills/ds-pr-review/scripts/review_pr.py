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
from finding_validator import validate_findings
from gitcode_api import GitCodeClient
from language_detect import detect_review_language
from sensitive_scan import format_sensitive_scan_failure, scan_changed_file, scan_text


REVIEW_LINE_THRESHOLD = 100

PARALLEL_REVIEW_ROUNDS: list[dict[str, Any]] = [
    {
        "id": "traceability_scope_and_discussion",
        "gates": [
            "Claim traceability",
            "Whole-module cohesion",
            "Discussion lifecycle",
            "Sensitive Information Gate",
        ],
        "instructions": (
            "Map PR claims, changed modules, existing comments, and sensitive-information scope "
            "before publishing findings."
        ),
    },
    {
        "id": "correctness_exception_and_lifecycle",
        "gates": [
            "Functional and design-contract correctness",
            "C++ safety and concurrency",
            "Ownership and lifetime",
        ],
        "instructions": (
            "Review correctness, exception safety, multi-step state updates, callback failure paths, "
            "ownership, and lifecycle."
        ),
    },
    {
        "id": "concurrency_and_reliability",
        "gates": ["C++ safety and concurrency", "Operability"],
        "instructions": (
            "Review locks, atomics, Start/Stop races, shutdown, retry budgets, weak references, "
            "and reliability signals."
        ),
    },
    {
        "id": "performance_scale_and_microarchitecture",
        "gates": ["Hot-path performance gate", "Hot-path argument cost"],
        "instructions": (
            "Quantify hot-path cost, Big-O behavior, fanout, memory footprint, cache-line effects, "
            "futex wakes, and branch cost."
        ),
    },
    {
        "id": "api_boundary_and_maintainability",
        "gates": [
            "Internal API design",
            "Developer experience",
            "Misuse prevention",
            "Abstraction boundaries",
            "Consistency and learnability",
            "Cross-language and boundary layers",
        ],
        "instructions": (
            "Review API clarity, misuse prevention, cross-boundary contracts, locatability, "
            "and maintainability."
        ),
    },
    {
        "id": "build_test_docs_and_packaging",
        "gates": ["Public interface and docs", "Build and packaging", "Build closure", "Tests", "Test contracts"],
        "instructions": "Review Bazel/CMake closure, docs/config/package updates, and risk-proportional tests.",
    },
]


def _to_int(value: Any) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def build_change_stats(files: list[dict[str, Any]]) -> dict[str, Any]:
    additions = sum(_to_int(file_info.get("additions")) for file_info in files)
    deletions = sum(_to_int(file_info.get("deletions")) for file_info in files)
    return {
        "file_count": len(files),
        "total_additions": additions,
        "total_deletions": deletions,
        "total_changed_lines": additions + deletions,
        "threshold_for_parallel_multi_round": REVIEW_LINE_THRESHOLD,
    }


def build_review_plan(change_stats: dict[str, Any]) -> dict[str, Any]:
    changed_lines = _to_int(change_stats.get("total_changed_lines"))
    if changed_lines >= REVIEW_LINE_THRESHOLD:
        return {
            "mode": "parallel_multi_round",
            "parallelizable": True,
            "reason": f"additions + deletions >= {REVIEW_LINE_THRESHOLD}",
            "rounds": PARALLEL_REVIEW_ROUNDS,
        }

    return {
        "mode": "single_integrated_pass",
        "parallelizable": False,
        "reason": f"additions + deletions < {REVIEW_LINE_THRESHOLD}",
        "rounds": [
            {
                "id": "single_integrated_pass",
                "gates": ["all triggered existing gates"],
                "instructions": (
                    "Run one integrated pass, but still cover every triggered existing gates before "
                    "publishing findings."
                ),
            }
        ],
    }


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


def prepare(args: argparse.Namespace) -> int:
    settings = load_settings()
    pr_number = parse_pr_ref(args.pr_ref)
    client = _client(settings)
    repo_path, warnings = ensure_local_repo(settings)

    pull = client.get_pull(pr_number)
    files = client.list_pull_files(pr_number)
    comments = client.list_pull_comments(pr_number)
    change_stats = build_change_stats(files)
    review_plan = build_review_plan(change_stats)
    pr_description = str(pull.get("body") or pull.get("description") or "")
    language = detect_review_language(f"{pull.get('title') or ''}\n{pr_description}")

    review_files = []
    prepared_files = []
    sensitive_matches = []
    sensitive_matches.extend(scan_text("PR title", str(pull.get("title") or "")))
    sensitive_matches.extend(scan_text("PR description", pr_description))
    sensitive_scan_files = []
    review_settings = settings["review"]

    for file_index, file_info in enumerate(files, start=1):
        path = str(file_info.get("filename") or file_info.get("path") or "")
        patch, patch_meta = _normalize_patch_payload(file_info)
        position_map = parse_patch(patch)
        file_matches, scanned_lines = scan_changed_file(
            path=path,
            patch=patch,
            position_map=position_map,
            file_index=file_index,
        )
        sensitive_matches.extend(file_matches)
        sensitive_scan_files.append(
            {
                "path": path,
                "status": file_info.get("status"),
                "scanned_lines": scanned_lines,
            }
        )
        prepared_files.append(
            {
                "file_info": file_info,
                "path": path,
                "patch": patch,
                "patch_meta": patch_meta,
                "position_map": position_map,
            }
        )

    if sensitive_matches:
        raise ReviewError(format_sensitive_scan_failure(sensitive_matches))

    for prepared_file in prepared_files:
        file_info = prepared_file["file_info"]
        path = prepared_file["path"]
        patch = prepared_file["patch"]
        patch_meta = prepared_file["patch_meta"]
        position_map = prepared_file["position_map"]
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
            "description": pr_description,
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
        "change_stats": change_stats,
        "review_plan": review_plan,
        "review_policy": {
            "need_to_resolve": bool(review_settings["need_to_resolve"]),
            "suggestion_limit_per_file": int(review_settings["suggestion_limit_per_file"]),
        },
        "sensitive_scan": {
            "status": "passed",
            "metadata_fields": ["PR title", "PR description"],
            "files": sensitive_scan_files,
        },
        "files": review_files,
        "existing_comments": normalized_comments,
    }
    write_json(bundle_path, bundle)
    write_json(
        findings_template_path,
        {
            "overall_risk": "low",
            "_contract": {
                "required_fields": [
                    "path",
                    "diff_line_index or line",
                    "type",
                    "severity",
                    "title",
                    "evidence",
                    "problem",
                    "impact",
                    "suggestion",
                ],
                "optional_fields": ["example_code", "verification", "match_text"],
                "note": "Use example_code for multi-line code; publish validates language and Markdown quality.",
            },
            "findings": [],
        },
    )

    output = {
        "bundle_path": str(bundle_path),
        "findings_template_path": str(findings_template_path),
        "pr_number": pr_number,
        "language": language,
        "warnings": warnings,
        "file_count": len(review_files),
        "comment_count": len(normalized_comments),
        "total_changed_lines": change_stats["total_changed_lines"],
        "review_plan_mode": review_plan["mode"],
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

    language = str(bundle["pr"].get("language") or "zh")
    validation_errors = validate_findings(findings, language)
    if validation_errors:
        formatted = "\n".join(f"- {error}" for error in validation_errors)
        raise ReviewError(f"Finding validation failed:\n{formatted}")

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
    prepare_parser.set_defaults(func=prepare)

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
