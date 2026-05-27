#!/usr/bin/env python3
from __future__ import annotations

import re
from typing import Any


HUNK_RE = re.compile(r"^@@ -(?P<old_start>\d+)(?:,(?P<old_count>\d+))? \+(?P<new_start>\d+)(?:,(?P<new_count>\d+))? @@")


def parse_patch(patch: str) -> list[dict[str, Any]]:
    if not patch:
        return []

    lines: list[dict[str, Any]] = []
    old_line = None
    new_line = None
    position = 0

    for raw in patch.splitlines():
        hunk_match = HUNK_RE.match(raw)
        if hunk_match:
            old_line = int(hunk_match.group("old_start"))
            new_line = int(hunk_match.group("new_start"))
            continue

        if raw.startswith("\\ No newline at end of file"):
            position += 1
            lines.append(
                {
                    "position": position,
                    "line_type": "meta",
                    "old_line": old_line,
                    "new_line": new_line,
                    "content": raw,
                }
            )
            continue

        if old_line is None or new_line is None:
            continue

        prefix = raw[:1]
        body = raw[1:] if raw else ""
        position += 1

        entry: dict[str, Any] = {
            "position": position,
            "content": raw,
            "text": body,
        }

        if prefix == "+":
            entry.update({"line_type": "add", "old_line": None, "new_line": new_line})
            new_line += 1
        elif prefix == "-":
            entry.update({"line_type": "delete", "old_line": old_line, "new_line": None})
            old_line += 1
        else:
            entry.update({"line_type": "context", "old_line": old_line, "new_line": new_line})
            old_line += 1
            new_line += 1

        lines.append(entry)

    return lines


def render_annotated_patch(position_map: list[dict[str, Any]]) -> str:
    rendered: list[str] = []
    for entry in position_map:
        tags = [f"diff={entry['position']}"]
        if entry.get("old_line") is not None:
            tags.append(f"old={entry['old_line']}")
        if entry.get("new_line") is not None:
            tags.append(f"new={entry['new_line']}")
        tags.append(f"type={entry['line_type']}")
        rendered.append(f"[{' '.join(tags)}] {entry['content']}")
    return "\n".join(rendered)


def find_position(
    position_map: list[dict[str, Any]],
    diff_line_index: int | None = None,
    line: int | None = None,
    match_text: str | None = None,
) -> dict[str, Any] | None:
    if diff_line_index is not None:
        for entry in position_map:
            if entry["position"] == diff_line_index:
                return entry

    candidates: list[dict[str, Any]] = []
    if line is not None:
        for entry in position_map:
            if entry.get("new_line") == line or entry.get("old_line") == line:
                candidates.append(entry)
        if candidates:
            candidates.sort(
                key=lambda item: (
                    0 if item["line_type"] in {"add", "context"} else 1,
                    abs((item.get("new_line") or item.get("old_line") or 0) - line),
                    item["position"],
                )
            )
            return candidates[0]

    if match_text:
        needle = match_text.strip()
        if needle:
            lower_needle = needle.lower()
            for entry in position_map:
                hay = (entry.get("text") or entry.get("content") or "").strip().lower()
                if lower_needle in hay:
                    return entry

    return None


def absolute_line_for_position(entry: dict[str, Any] | None) -> int | None:
    if not entry:
        return None
    if entry.get("new_line") is not None:
        return int(entry["new_line"])
    if entry.get("old_line") is not None:
        return int(entry["old_line"])
    return None
