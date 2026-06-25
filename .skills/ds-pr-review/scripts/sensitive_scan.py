#!/usr/bin/env python3
from __future__ import annotations

from dataclasses import dataclass
import re
from typing import Any


COMPANY_LABEL_RE = re.compile(r"(?i)\bhua\s*wei\b")
COMPANY_ALLOWED_LINE_PATTERNS: tuple[re.Pattern[str], ...] = (
    re.compile(r"(?i)\bcopyright\b.*\bhua\s*wei\b|\bhua\s*wei\b.*\bcopyright\b"),
    re.compile(
        r"(?i)(?:\b(?:third[- ]party|librar(?:y|ies)|dependenc(?:y|ies)|vendor|upstream|"
        r"open[- ]source|derived from|based on|forked from|source from|reference(?:d)?|"
        r"depends on|import(?:s|ed)? from)\b|"
        r"(?:\u7b2c\u4e09\u65b9|\u5f00\u6e90|\u5f15\u7528|\u4f9d\u8d56|\u6765\u81ea|\u5e93))"
        r".*\bhua\s*wei\b|"
        r"\bhua\s*wei\b.*"
        r"(?:\b(?:third[- ]party|librar(?:y|ies)|dependenc(?:y|ies)|vendor|upstream|"
        r"open[- ]source|derived from|based on|forked from|source from|reference(?:d)?|"
        r"depends on|import(?:s|ed)? from)\b|"
        r"(?:\u7b2c\u4e09\u65b9|\u5f00\u6e90|\u5f15\u7528|\u4f9d\u8d56|\u6765\u81ea|\u5e93))"
    ),
)

CREDENTIAL_KEY_RE = re.compile(
    r"(?i)\b(?P<key>password|passwd|pwd|secret|token|access[_ -]?key|secret[_ -]?key|"
    r"system[_ -]?access[_ -]?key|system[_ -]?secret[_ -]?key|"
    r"tenant[_ -]?access[_ -]?key|tenant[_ -]?secret[_ -]?key|ak|sk|"
    r"username|user|account|login|tenant|namespace)\s*[:=]"
)
SAFE_CREDENTIAL_RHS_RE = re.compile(
    r"(?x)"
    r"(?:"
    r"None|null|''|\"\"|"
    r"<[^>]+>|"
    r"[A-Z][A-Z0-9_]+|"
    r"[A-Za-z_][A-Za-z0-9_]*(?:\s*\([^)]*\)|\[[^\]]+\]|\.[A-Za-z_][A-Za-z0-9_]*)+"
    r")"
)
SAFE_CREDENTIAL_WORDS = {
    "branch",
    "bool",
    "bytes",
    "dict",
    "example",
    "list",
    "none",
    "null",
    "optional",
    "path",
    "placeholder",
    "safe",
    "str",
    "tuple",
    "value",
}

SENSITIVE_LINE_PATTERNS: tuple[tuple[str, re.Pattern[str]], ...] = (
    (
        "server IP or endpoint",
        re.compile(
            r"\b(?:25[0-5]|2[0-4]\d|1?\d?\d)(?:\.(?:25[0-5]|2[0-4]\d|1?\d?\d)){3}"
            r"(?::\d{1,5})?\b|"
            r"\b[a-z0-9][a-z0-9.-]{1,253}:\d{2,5}\b",
            re.IGNORECASE,
        ),
    ),
    (
        "local filesystem path",
        re.compile(
            r"(?<![\w.-])(?:/(?:Users|home|root|tmp|var|mnt|opt|workspace|Volumes)(?:/[^\s`'\"<>]+)+"
            r"|[A-Za-z]:\\(?:Users|workspace|tmp|temp)\\[^\s`'\"<>]+)"
        ),
    ),
    (
        "private or ssh key",
        re.compile(
            r"(?i)-----BEGIN [A-Z0-9 ]*PRIVATE KEY-----|"
            r"BEGIN OPENSSH "
            r"PRIVATE KEY|"
            r"ssh-rsa\s+[A-Za-z0-9+/=]{20,}"
        ),
    ),
    (
        "employee identifier",
        re.compile(
            r"(?i)(?:\u5de5\s*\u53f7|employee\s*id\b|staff\s*id\b|"
            r"badge\s*(?:id|number)\b|job\s*(?:id|number)\b|work\s*(?:id|number)\b)"
            r"\s*[:=\uff1a#-]?\s*"
            r"[A-Za-z0-9][A-Za-z0-9_.-]{2,}"
        ),
    ),
    (
        "personal contact or identity number",
        re.compile(r"(?<!\d)(?:1[3-9]\d{9}|\d{17}[\dXx])(?!\w)"),
    ),
    (
        "personal name or account",
        re.compile(r"(?i)(?:\u59d3\s*\u540d|real\s*name|full\s*name|owner\s*name)\s*[:=\uff1a]\s*\S+"),
    ),
)


@dataclass(frozen=True)
class SensitiveMatch:
    location: str
    category: str
    line: int | None = None


def _is_allowed_company_reference(line: str) -> bool:
    if not COMPANY_LABEL_RE.search(line):
        return True
    return any(pattern.search(line) for pattern in COMPANY_ALLOWED_LINE_PATTERNS)


def _normalize_key(value: str) -> str:
    return re.sub(r"[^a-z0-9]", "", value.lower())


def _credential_assignment_is_sensitive(line: str) -> bool:
    for match in CREDENTIAL_KEY_RE.finditer(line):
        raw_value = line[match.end():].strip()
        if not raw_value:
            continue

        value = raw_value.split(",", maxsplit=1)[0].strip()
        trimmed_value = value.rstrip(")]}")
        if not value:
            continue

        normalized_key = _normalize_key(match.group("key"))
        normalized_value = _normalize_key(trimmed_value.strip("'\""))
        if normalized_value == normalized_key:
            continue
        if normalized_value in SAFE_CREDENTIAL_WORDS:
            continue
        if value[:1].isidentifier() and "(" in value:
            continue
        if SAFE_CREDENTIAL_RHS_RE.fullmatch(value) or SAFE_CREDENTIAL_RHS_RE.fullmatch(trimmed_value):
            continue
        return True
    return False


def _scan_line(location: str, line: str, line_number: int | None) -> list[SensitiveMatch]:
    matches: list[SensitiveMatch] = []
    if not _is_allowed_company_reference(line):
        matches.append(SensitiveMatch(location=location, category="company identifier", line=line_number))
    if _credential_assignment_is_sensitive(line):
        matches.append(SensitiveMatch(location=location, category="credential or account assignment", line=line_number))

    for category, pattern in SENSITIVE_LINE_PATTERNS:
        if pattern.search(line):
            matches.append(SensitiveMatch(location=location, category=category, line=line_number))

    deduped: list[SensitiveMatch] = []
    seen: set[tuple[str, str, int | None]] = set()
    for match in matches:
        key = (match.location, match.category, match.line)
        if key in seen:
            continue
        seen.add(key)
        deduped.append(match)
    return deduped


def scan_text(location: str, text: str | None) -> list[SensitiveMatch]:
    if not text:
        return []

    lines = text.splitlines() or [text]
    matches: list[SensitiveMatch] = []
    for line_number, line in enumerate(lines, start=1):
        matches.extend(_scan_line(location, line, line_number if len(lines) > 1 else None))
    return matches


def scan_changed_file(
    path: str,
    patch: str,
    position_map: list[dict[str, Any]],
    file_index: int,
) -> tuple[list[SensitiveMatch], int]:
    path_matches = scan_text(f"changed file path #{file_index}", path)
    content_location = f"changed file #{file_index}" if path_matches else path
    matches = list(path_matches)

    if not patch or not position_map:
        matches.append(SensitiveMatch(location=content_location, category="unscannable changed file"))
        return matches, 0

    scanned_lines = 0
    for entry in position_map:
        if entry.get("line_type") not in {"add", "delete", "context"}:
            continue
        scanned_lines += 1
        line_number = entry.get("new_line") or entry.get("old_line")
        matches.extend(_scan_line(content_location, str(entry.get("text") or ""), line_number))

    if scanned_lines == 0:
        matches.append(SensitiveMatch(location=content_location, category="unscannable changed file"))
    return matches, scanned_lines


def format_sensitive_scan_failure(matches: list[SensitiveMatch]) -> str:
    deduped: list[SensitiveMatch] = []
    seen: set[tuple[str, str, int | None]] = set()
    for match in matches:
        key = (match.location, match.category, match.line)
        if key in seen:
            continue
        seen.add(key)
        deduped.append(match)

    entries = []
    for match in deduped:
        suffix = f":{match.line}" if match.line is not None else ""
        entries.append(f"- {match.location}{suffix}: {match.category}")

    return (
        "Sensitive information scan failed. Remove or redact the flagged categories before generating "
        "a review bundle; do not paste raw values into findings.\n"
        + "\n".join(entries)
    )
