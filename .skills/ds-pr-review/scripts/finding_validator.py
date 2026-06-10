#!/usr/bin/env python3
from __future__ import annotations

import re
from typing import Any


CHINESE_RE = re.compile(r"[\u4e00-\u9fff]")
REQUIRED_FIELDS = ("path", "type", "severity", "title", "evidence", "problem", "impact", "suggestion")
TEXT_FIELDS = ("title", "evidence", "problem", "impact", "suggestion", "verification")
FREE_TEXT_FIELDS = ("evidence", "problem", "impact", "suggestion", "verification")
VALID_TYPES = {
    "bug",
    "build",
    "compatibility",
    "correctness",
    "security",
    "performance",
    "design",
    "documentation",
    "test",
}
VALID_SEVERITIES = {"critical", "warning", "suggestion"}


def _text(finding: dict[str, Any], field: str) -> str:
    return str(finding.get(field) or "").strip()


def _finding_label(index: int, finding: dict[str, Any]) -> str:
    path = _text(finding, "path") or "<missing path>"
    title = _text(finding, "title") or "<missing title>"
    return f"finding[{index}] {path} {title}"


def _contains_plain_multiline_code(text: str) -> bool:
    if "\n" not in text:
        return False
    if "```" in text:
        return True
    codeish_lines = 0
    for line in text.splitlines():
        stripped = line.strip()
        if not stripped:
            continue
        if stripped.startswith(("#include", "if ", "for ", "while ", "return ", "class ", "struct ")):
            codeish_lines += 1
        elif any(token in stripped for token in (";", "{", "}", "->", "::", " = ")):
            codeish_lines += 1
    return codeish_lines >= 2


def validate_findings(findings: list[dict[str, Any]], language: str) -> list[str]:
    """Return user-facing validation errors for review findings."""
    language = "zh" if language == "zh" else "en"
    errors: list[str] = []

    for index, finding in enumerate(findings):
        label = _finding_label(index, finding)

        for field in REQUIRED_FIELDS:
            if not _text(finding, field):
                errors.append(f"{label}: missing required field `{field}`.")

        kind = _text(finding, "type")
        if kind and kind not in VALID_TYPES:
            errors.append(f"{label}: unknown `type` {kind!r}; expected one of {sorted(VALID_TYPES)}.")

        severity = _text(finding, "severity")
        if severity and severity not in VALID_SEVERITIES:
            errors.append(
                f"{label}: unknown `severity` {severity!r}; expected one of {sorted(VALID_SEVERITIES)}."
            )

        natural_text = " ".join(_text(finding, field) for field in TEXT_FIELDS)
        if language == "zh":
            if natural_text and not CHINESE_RE.search(natural_text):
                errors.append(f"{label}: PR language is zh, but finding text does not contain Chinese.")
        elif CHINESE_RE.search(natural_text):
            errors.append(f"{label}: PR language is en, but finding text contains Chinese.")

        for field in FREE_TEXT_FIELDS:
            text = _text(finding, field)
            if "```" in text:
                errors.append(f"{label}: field `{field}` contains a fenced code block; move code to `example_code`.")
            elif _contains_plain_multiline_code(text):
                errors.append(f"{label}: field `{field}` looks like multi-line code; move it to `example_code`.")

        example_code = str(finding.get("example_code") or "")
        if "```" in example_code:
            errors.append(f"{label}: `example_code` must contain raw code without Markdown fences.")

    return errors
