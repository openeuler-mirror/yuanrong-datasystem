#!/usr/bin/env python3
from __future__ import annotations

import hashlib
import re
from collections import defaultdict
from typing import Any

from common import normalize_ws


FINGERPRINT_RE = re.compile(r"<!--\s*yuanrong-pr-review:(?P<fp>[a-f0-9]{12,64})\s*-->")


def extract_fingerprint(body: str) -> str | None:
    match = FINGERPRINT_RE.search(body or "")
    return match.group("fp") if match else None


def fingerprint_for_finding(finding: dict[str, Any]) -> str:
    parts = [
        normalize_ws(str(finding.get("path", ""))),
        str(finding.get("diff_line_index", "")),
        str(finding.get("line", "")),
        normalize_ws(str(finding.get("type", ""))),
        normalize_ws(str(finding.get("severity", ""))),
        normalize_ws(str(finding.get("title", ""))),
    ]
    return hashlib.sha1("||".join(parts).encode("utf-8")).hexdigest()[:16]


def compress_suggestions(findings: list[dict[str, Any]], limit_per_file: int) -> list[dict[str, Any]]:
    kept: list[dict[str, Any]] = []
    suggestion_count: defaultdict[str, int] = defaultdict(int)
    suggestion_keys: set[str] = set()

    for finding in findings:
        severity = finding.get("severity")
        if severity != "suggestion":
            kept.append(finding)
            continue

        path = str(finding.get("path", ""))
        suggestion_key = f"{path}:{normalize_ws(str(finding.get('title', '')))}"
        if suggestion_key in suggestion_keys:
            continue
        if suggestion_count[path] >= limit_per_file:
            continue

        suggestion_keys.add(suggestion_key)
        suggestion_count[path] += 1
        kept.append(finding)

    return kept
