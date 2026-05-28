#!/usr/bin/env python3
from __future__ import annotations

from pathlib import Path
from typing import Any


FOCUS_RULES: list[tuple[str, list[str]]] = [
    ("include/datasystem/kv_client.h", ["public-api", "kv-hot-path"]),
    ("include/datasystem/object_client.h", ["public-api", "object-hot-path"]),
    ("src/datasystem/client/kv_cache/", ["kv-hot-path"]),
    ("src/datasystem/client/object_cache/", ["object-hot-path"]),
    ("src/datasystem/worker/object_cache/", ["worker-object-cache", "hot-path"]),
    ("src/datasystem/worker/cluster_manager/", ["cluster-metadata"]),
    ("src/datasystem/c_api/", ["public-api", "ffi-layer"]),
    ("src/datasystem/java_api/", ["public-api", "java-api"]),
    ("python/yr/", ["public-api", "python-api"]),
    ("tests/ut/", ["tests"]),
    ("tests/st/", ["tests"]),
    ("tests/python/", ["tests"]),
]


def focus_tags_for_path(path: str) -> list[str]:
    tags: list[str] = []
    for prefix, values in FOCUS_RULES:
        if path.startswith(prefix):
            tags.extend(values)
    if not tags:
        tags.append("general")
    return sorted(set(tags))


def _group_lines(lines: list[int], radius: int) -> list[tuple[int, int]]:
    if not lines:
        return []

    sorted_lines = sorted(set(lines))
    groups: list[tuple[int, int]] = []
    start = sorted_lines[0]
    end = sorted_lines[0]

    for line in sorted_lines[1:]:
        if line <= end + radius:
            end = line
            continue
        groups.append((start, end))
        start = line
        end = line

    groups.append((start, end))
    return groups


def build_context_snippets(
    local_file: Path | None,
    position_map: list[dict[str, Any]],
    snippet_radius: int,
    max_snippets_per_file: int,
    max_chars_per_snippet: int,
) -> list[dict[str, Any]]:
    if not local_file or not local_file.exists() or not local_file.is_file():
        return []

    changed_lines = [entry["new_line"] for entry in position_map if entry.get("new_line") is not None]
    groups = _group_lines(changed_lines, snippet_radius)
    if not groups:
        return []

    text = local_file.read_text(encoding="utf-8", errors="replace").splitlines()
    snippets: list[dict[str, Any]] = []

    for start, end in groups[:max_snippets_per_file]:
        snippet_start = max(1, start - snippet_radius)
        snippet_end = min(len(text), end + snippet_radius)
        lines = []
        for lineno in range(snippet_start, snippet_end + 1):
            lines.append(f"{lineno:>6} | {text[lineno - 1]}")
        content = "\n".join(lines)
        if len(content) > max_chars_per_snippet:
            content = content[: max_chars_per_snippet - 20] + "\n... [truncated]"
        snippets.append(
            {
                "start_line": snippet_start,
                "end_line": snippet_end,
                "content": content,
            }
        )

    return snippets
