#!/usr/bin/env python3
"""Small, deterministic Codex hook helpers for repository context survival."""

from __future__ import annotations

import json
import os
import re
import subprocess
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


MAX_CONTEXT_DOCS = 6
MAX_CONTEXT_CHARS = 5000
MAX_WORKING_STATE_LINES = 80
MAX_RECENT_TOOLS = 8

DEFAULT_CONTEXT_DOCS = [
    ".repo_context/modules/overview/engineering-principles.md",
    ".repo_context/playbooks/features/infra-engineering-workflow.md",
    ".repo_context/modules/quality/tests-and-reproduction.md",
]

DEFAULT_ROUTE_MAP = [
    (
        ["src/datasystem/worker/object_cache", "src/datasystem/worker"],
        [".repo_context/modules/runtime/worker-runtime.md"],
    ),
    (["src/datasystem/master"], [".repo_context/modules/overview/repository-overview.md"]),
    (
        ["src/datasystem/common/kvstore/etcd", "third_party/protos/etcd"],
        [".repo_context/modules/runtime/etcd-metadata/README.md"],
    ),
    (["src/datasystem/worker/cluster_manager"], [".repo_context/modules/runtime/cluster-manager/README.md"]),
    (["src/datasystem/worker/hash_ring"], [".repo_context/modules/runtime/hash-ring/README.md"]),
    (["src/datasystem/common/l2cache"], [".repo_context/modules/infra/l2cache/design.md"]),
    (
        ["src/datasystem/common/l2cache/slot_client", "src/datasystem/worker/object_cache/slot_recovery"],
        [".repo_context/modules/infra/slot/design.md"],
    ),
    (["src/datasystem/common/log"], [".repo_context/modules/infra/logging/design.md"]),
    (["src/datasystem/common/metrics"], [".repo_context/modules/infra/metrics/design.md"]),
    (
        ["src/datasystem/client", "include/datasystem", "python/yr/datasystem", "java", "go"],
        [".repo_context/modules/client/client-sdk.md"],
    ),
    (
        ["tests"],
        [
            ".repo_context/modules/quality/tests-and-reproduction.md",
            ".repo_context/modules/quality/test-suite-design.md",
        ],
    ),
    (
        ["build.sh", "CMakeLists.txt", "cmake", "src/datasystem/CMakeLists.txt"],
        [".repo_context/modules/quality/cmake-build/README.md"],
    ),
    ([".skills"], [".repo_context/modules/overview/repository-skills.md"]),
    ([".repo_context"], [".repo_context/README.md", ".repo_context/index.md", ".repo_context/maintenance.md"]),
]


@dataclass(frozen=True)
class CommandDecision:
    blocked: bool = False
    reason: str = ""
    advisory: str = ""


@dataclass(frozen=True)
class WorkingState:
    repo_root: Path
    event_name: str
    touched_paths: list[str]
    recent_tools: list[str]
    last_assistant_message: str | None = None


def load_stdin_json() -> dict[str, Any]:
    raw = sys.stdin.read()
    if not raw.strip():
        return {}
    return json.loads(raw)


def emit_json(payload: dict[str, Any]) -> None:
    print(json.dumps(payload, ensure_ascii=True, separators=(",", ":")))


def find_repo_root(cwd: str | None = None) -> Path:
    current = Path(cwd or os.getcwd()).resolve()
    for candidate in [current, *current.parents]:
        if (candidate / ".repo_context").is_dir() or (candidate / ".git").exists():
            return candidate
    return current


def run_git(repo_root: Path, args: list[str], timeout_s: int = 3) -> str:
    try:
        result = subprocess.run(
            ["git", *args],
            cwd=repo_root,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            check=False,
            timeout=timeout_s,
        )
    except Exception:
        return ""
    return result.stdout.strip()


def unique_preserve_order(items: list[str]) -> list[str]:
    seen: set[str] = set()
    ordered: list[str] = []
    for item in items:
        if item and item not in seen:
            seen.add(item)
            ordered.append(item)
    return ordered


def normalize_path(path: str) -> str:
    normalized = path.strip().strip("\"'")
    while normalized.startswith("./"):
        normalized = normalized[2:]
    return normalized


def load_route_map(repo_root: Path | None = None) -> tuple[list[str], list[tuple[list[str], list[str]]]]:
    root = repo_root or find_repo_root()
    map_path = root / ".codex" / "context" / "module-map.json"
    if not map_path.exists():
        return DEFAULT_CONTEXT_DOCS, DEFAULT_ROUTE_MAP
    try:
        data = json.loads(map_path.read_text(encoding="utf-8"))
    except Exception:
        return DEFAULT_CONTEXT_DOCS, DEFAULT_ROUTE_MAP

    defaults = [str(item) for item in data.get("defaults", DEFAULT_CONTEXT_DOCS)]
    routes: list[tuple[list[str], list[str]]] = []
    for route in data.get("routes", []):
        prefixes = [normalize_path(str(item)) for item in route.get("prefixes", [])]
        docs = [normalize_path(str(item)) for item in route.get("docs", [])]
        if prefixes and docs:
            routes.append((prefixes, docs))
    return defaults, routes or DEFAULT_ROUTE_MAP


def route_context_docs(paths: list[str], repo_root: Path | None = None) -> list[str]:
    defaults, routes = load_route_map(repo_root)
    docs: list[str] = []
    for path in [normalize_path(item) for item in paths]:
        for prefixes, route_docs in routes:
            if any(path == prefix or path.startswith(prefix.rstrip("/") + "/") for prefix in prefixes):
                docs.extend(route_docs)
    docs.extend(defaults)
    return unique_preserve_order(docs)[:MAX_CONTEXT_DOCS]


PATH_RE = re.compile(
    r"(?P<path>(?:src/datasystem|include/datasystem|python/yr/datasystem|tests|cmake|docs|\.repo_context|\.skills|"
    r"third_party/protos|transfer_engine)[A-Za-z0-9_./+-]*|build\.sh|CMakeLists\.txt)"
)

KEYWORD_PATHS = [
    (
        re.compile(r"\bworker\b|object[_ -]?cache|batch\s*get|batchget", re.IGNORECASE),
        "src/datasystem/worker/object_cache",
    ),
    (re.compile(r"\bmaster\b|metadata", re.IGNORECASE), "src/datasystem/master"),
    (re.compile(r"\betcd\b|keep\s*alive|lease", re.IGNORECASE), "src/datasystem/common/kvstore/etcd"),
    (re.compile(r"\bclient\b|\bsdk\b|python\s*api", re.IGNORECASE), "src/datasystem/client"),
    (re.compile(r"\btest\b|测试|用例|ctest|gtest", re.IGNORECASE), "tests"),
    (re.compile(r"\bcmake\b|\bbuild\b|编译|构建", re.IGNORECASE), "CMakeLists.txt"),
]


def extract_paths_from_text(text: str) -> list[str]:
    paths = [normalize_path(match.group("path")) for match in PATH_RE.finditer(text or "")]
    for line in (text or "").splitlines():
        marker_match = re.match(r"^\*\*\* (?:Add|Update|Delete) File: (.+)$", line.strip())
        if marker_match:
            paths.append(normalize_path(marker_match.group(1)))
    for pattern, synthetic_path in KEYWORD_PATHS:
        if pattern.search(text or ""):
            paths.append(synthetic_path)
    return unique_preserve_order(paths)


def extract_tool_paths(payload: dict[str, Any]) -> list[str]:
    tool_input = payload.get("tool_input") or {}
    if isinstance(tool_input, dict):
        pieces = [str(value) for value in tool_input.values() if isinstance(value, (str, int, float))]
    else:
        pieces = [str(tool_input)]
    return extract_paths_from_text("\n".join(pieces))


DESTRUCTIVE_PATTERNS = [
    (re.compile(r"\bgit\s+reset\s+--hard\b"), "destructive git reset is blocked"),
    (re.compile(r"\bgit\s+checkout\s+--\s+"), "destructive git checkout is blocked"),
    (re.compile(r"\bgit\s+clean\s+-[A-Za-z]*[fdx][A-Za-z]*\b"), "destructive git clean is blocked"),
    (
        re.compile(r"\brm\s+-[A-Za-z]*r[A-Za-z]*f[A-Za-z]*\s+(?:/|\.|\*|~|\$HOME|\.git)(?:\s|$)"),
        "destructive rm is blocked",
    ),
    (re.compile(r"\bsudo\b"), "sudo is blocked by repository hook policy"),
    (re.compile(r"chmod\s+-R\s+777\b"), "world-writable recursive chmod is blocked"),
]

RTK_EXPECTED = re.compile(r"^\s*(?:git|rg|sed|cat|ls|find|python3?|cmake|bazel|ctest|pytest|npm|node|make|ninja)\b")


def evaluate_bash_command(command: str) -> CommandDecision:
    for pattern, reason in DESTRUCTIVE_PATTERNS:
        if pattern.search(command):
            return CommandDecision(blocked=True, reason=reason)
    if RTK_EXPECTED.search(command) and not command.lstrip().startswith("rtk "):
        return CommandDecision(
            advisory="Repository convention: prefix shell commands with `rtk` to reduce transcript noise."
        )
    return CommandDecision()


def additional_context_for_paths(paths: list[str], repo_root: Path | None = None, advisory: str = "") -> str:
    docs = route_context_docs(paths, repo_root)
    lines = [
        "Context survival router:",
        "- Keep durable context out of the chat; load only the smallest source-backed docs needed.",
        "- Source code is final truth; verify important claims against touched files.",
    ]
    if advisory:
        lines.append(f"- {advisory}")
    lines.append("")
    lines.append("Relevant repo context:")
    for doc in docs:
        lines.append(f"- `{doc}`")
    if paths:
        lines.append("")
        lines.append("Detected touched/search paths:")
        for path in paths[:8]:
            lines.append(f"- `{path}`")
    return "\n".join(lines)[:MAX_CONTEXT_CHARS]


def hook_specific(event_name: str, additional_context: str = "", extra: dict[str, Any] | None = None) -> dict[str, Any]:
    output: dict[str, Any] = {"hookEventName": event_name}
    if additional_context:
        output["additionalContext"] = additional_context
    if extra:
        output.update(extra)
    return {"hookSpecificOutput": output}


def handle_pre_tool_use(payload: dict[str, Any]) -> dict[str, Any]:
    event = "PreToolUse"
    repo_root = find_repo_root(payload.get("cwd"))
    tool_name = str(payload.get("tool_name") or "")
    tool_input = payload.get("tool_input") or {}
    command = str(tool_input.get("command", "")) if isinstance(tool_input, dict) else str(tool_input)
    paths = extract_tool_paths(payload)

    if tool_name == "Bash":
        decision = evaluate_bash_command(command)
        if decision.blocked:
            return hook_specific(
                event,
                extra={
                    "permissionDecision": "deny",
                    "permissionDecisionReason": f"{decision.reason}. Ask the user before running it.",
                },
            )
        advisory = decision.advisory
    else:
        advisory = ""

    return hook_specific(event, additional_context_for_paths(paths, repo_root, advisory))


def read_working_state(repo_root: Path) -> str:
    path = repo_root / ".codex" / "context" / "working-state.md"
    if not path.exists():
        return ""
    try:
        return path.read_text(encoding="utf-8")[:2500]
    except Exception:
        return ""


def git_touched_paths(repo_root: Path) -> list[str]:
    outputs = [
        run_git(repo_root, ["diff", "--name-only"]),
        run_git(repo_root, ["diff", "--cached", "--name-only"]),
        run_git(repo_root, ["ls-files", "--others", "--exclude-standard", ".codex"]),
    ]
    paths: list[str] = []
    for output in outputs:
        paths.extend(line.strip() for line in output.splitlines() if line.strip())
    return unique_preserve_order(paths)


def summarize_git(repo_root: Path) -> list[str]:
    branch = run_git(repo_root, ["branch", "--show-current"]) or "(detached or unknown)"
    status = run_git(repo_root, ["status", "--short"])
    lines = [f"- Branch: `{branch}`"]
    if status:
        changed = [line for line in status.splitlines() if line.strip()]
        lines.append("- Changed files:")
        for line in changed[:12]:
            lines.append(f"  - `{line}`")
        if len(changed) > 12:
            lines.append(f"  - ... {len(changed) - 12} more")
    else:
        lines.append("- Changed files: none")
    return lines


def handle_session_start(payload: dict[str, Any]) -> dict[str, Any]:
    repo_root = find_repo_root(payload.get("cwd"))
    touched = git_touched_paths(repo_root)
    docs = route_context_docs(touched, repo_root)
    working_state = read_working_state(repo_root)
    lines = [
        "Context survival is active for this repository.",
        "",
        "Core rules:",
        "- Treat `.repo_context/` as an index; load only the smallest relevant docs.",
        "- Before implementation, bugfix, refactor, design, or codebase Q&A: use "
        "`.repo_context/playbooks/features/infra-engineering-workflow.md`.",
        "- Before large code work: read `.repo_context/README.md`, `index.md`, `maintenance.md`, "
        "generated repo index, then route to module docs.",
        "- Prefix shell commands with `rtk` in this repository.",
        "- Keep `.codex/context/working-state.md` current across compaction or long sessions.",
        "",
        "Current git snapshot:",
        *summarize_git(repo_root),
        "",
        "Likely relevant repo context:",
    ]
    lines.extend(f"- `{doc}`" for doc in docs)
    if working_state:
        lines.extend(["", "Existing working-state excerpt:", working_state])
    return hook_specific("SessionStart", "\n".join(lines)[:MAX_CONTEXT_CHARS])


def handle_user_prompt_submit(payload: dict[str, Any]) -> dict[str, Any]:
    prompt = str(payload.get("prompt") or "")
    if not re.search(r"实现|修复|改|review|检查|分析|bug|feature|implement|fix|debug", prompt, re.IGNORECASE):
        return {"continue": True}
    repo_root = find_repo_root(payload.get("cwd"))
    context = additional_context_for_paths(extract_paths_from_text(prompt), repo_root)
    return hook_specific("UserPromptSubmit", context)


def render_working_state(state: WorkingState) -> str:
    touched = unique_preserve_order(state.touched_paths)
    docs = route_context_docs(touched, state.repo_root)
    lines = [
        "# Codex Working State",
        "",
        f"- Updated UTC: `{datetime.now(timezone.utc).replace(microsecond=0).isoformat()}`",
        f"- Last hook event: `{state.event_name}`",
        "",
        "## Task Snapshot",
        "",
        "- Current objective: continue the latest user-requested repository task.",
        "- Source of truth: repository source files, then `.repo_context/`.",
        "- Before broad edits: reload the relevant docs below and verify against source.",
        "",
        "## Touched Paths",
        "",
    ]
    if touched:
        lines.extend(f"- `{path}`" for path in touched[:20])
    else:
        lines.append("- None detected.")
    lines.extend(["", "## Relevant Repo Context", ""])
    lines.extend(f"- `{doc}`" for doc in docs)
    if state.recent_tools:
        lines.extend(["", "## Recent Tools", ""])
        lines.extend(f"- `{tool}`" for tool in state.recent_tools[-MAX_RECENT_TOOLS:])
    if state.last_assistant_message:
        excerpt = " ".join(state.last_assistant_message.split())[:900]
        lines.extend(["", "## Last Assistant Message Excerpt", "", excerpt])
    lines.extend(
        [
            "",
            "## Resume Checklist",
            "",
            "- Re-read only the relevant docs listed above.",
            "- Inspect touched source files before asserting behavior.",
            "- Run targeted verification before claiming completion.",
        ]
    )
    return "\n".join(lines[:MAX_WORKING_STATE_LINES]) + "\n"


def write_working_state(state: WorkingState) -> Path:
    output_path = state.repo_root / ".codex" / "context" / "working-state.md"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(render_working_state(state), encoding="utf-8")
    return output_path


def compact_tool_summary(payload: dict[str, Any]) -> list[str]:
    tool_name = payload.get("tool_name")
    tool_input = payload.get("tool_input")
    if not tool_name:
        return []
    command = ""
    if isinstance(tool_input, dict):
        command = str(tool_input.get("command") or tool_input.get("cmd") or "")
    return [f"{tool_name}: {command[:160]}".strip(": ")]


def build_state_from_payload(payload: dict[str, Any], event_name: str) -> WorkingState:
    repo_root = find_repo_root(payload.get("cwd"))
    paths = unique_preserve_order(git_touched_paths(repo_root) + extract_tool_paths(payload))
    return WorkingState(
        repo_root=repo_root,
        event_name=event_name,
        touched_paths=paths,
        recent_tools=compact_tool_summary(payload),
        last_assistant_message=payload.get("last_assistant_message"),
    )


def handle_post_tool_use(payload: dict[str, Any]) -> dict[str, Any]:
    state = build_state_from_payload(payload, "PostToolUse")
    write_working_state(state)
    paths = extract_tool_paths(payload)
    return hook_specific("PostToolUse", additional_context_for_paths(paths, state.repo_root))


def handle_pre_compact(payload: dict[str, Any]) -> dict[str, Any]:
    state = build_state_from_payload(payload, "PreCompact")
    path = write_working_state(state)
    return {"continue": True, "systemMessage": f"Saved Codex working state to {path}"}


def handle_post_compact(payload: dict[str, Any]) -> dict[str, Any]:
    repo_root = find_repo_root(payload.get("cwd"))
    working_state = read_working_state(repo_root)
    if working_state:
        return {
            "continue": True,
            "systemMessage": "Codex working state is available at .codex/context/working-state.md after compaction.",
        }
    return {"continue": True}


def handle_stop(payload: dict[str, Any]) -> dict[str, Any]:
    state = build_state_from_payload(payload, "Stop")
    write_working_state(state)
    return {"continue": True}


HANDLERS = {
    "SessionStart": handle_session_start,
    "UserPromptSubmit": handle_user_prompt_submit,
    "PreToolUse": handle_pre_tool_use,
    "PostToolUse": handle_post_tool_use,
    "PreCompact": handle_pre_compact,
    "PostCompact": handle_post_compact,
    "Stop": handle_stop,
}


def safe_fallback(event_name: str, error: Exception) -> dict[str, Any]:
    message = f"Codex context-survival hook failed open for {event_name}: {error}"
    if event_name in {"PreToolUse", "PostToolUse", "SessionStart", "UserPromptSubmit", "PostCompact"}:
        return hook_specific(event_name, message)
    return {"continue": True, "systemMessage": message}


def run_event(event_name: str) -> None:
    try:
        payload = load_stdin_json()
        handler = HANDLERS[event_name]
        emit_json(handler(payload))
    except Exception as exc:  # Hooks should help the agent, not brick the session.
        emit_json(safe_fallback(event_name, exc))


if __name__ == "__main__":
    event = sys.argv[1] if len(sys.argv) > 1 else ""
    if event not in HANDLERS:
        emit_json({"continue": True, "systemMessage": f"Unknown context-survival event: {event}"})
    else:
        run_event(event)
