#!/usr/bin/env python3
from __future__ import annotations

from pathlib import Path
from typing import Any


TYPE_LABELS = {
    "zh": {
        "bug": "Bug",
        "security": "安全",
        "performance": "性能",
        "design": "设计",
        "documentation": "文档",
        "test": "测试",
    },
    "en": {
        "bug": "Bug",
        "security": "Security",
        "performance": "Performance",
        "design": "Design",
        "documentation": "Documentation",
        "test": "Test",
    },
}

SEVERITY_LABELS = {
    "zh": {"critical": "严重", "warning": "警告", "suggestion": "建议"},
    "en": {"critical": "Critical", "warning": "Warning", "suggestion": "Suggestion"},
}


def _code_lang_for_path(path: str) -> str:
    suffix = Path(path).suffix.lower()
    return {
        ".cc": "cpp",
        ".cpp": "cpp",
        ".cxx": "cpp",
        ".h": "cpp",
        ".hpp": "cpp",
        ".py": "python",
        ".java": "java",
        ".go": "go",
        ".sh": "bash",
        ".md": "markdown",
    }.get(suffix, "text")


def format_comment(finding: dict[str, Any], language: str, fingerprint: str) -> str:
    language = "zh" if language == "zh" else "en"
    severity = finding.get("severity", "warning")
    kind = finding.get("type", "bug")
    title = str(finding.get("title", "")).strip()
    problem = str(finding.get("problem", "")).strip()
    suggestion = str(finding.get("suggestion", "")).strip()
    example_code = str(finding.get("example_code", "")).rstrip()

    header = f"**[{SEVERITY_LABELS[language][severity]}][{TYPE_LABELS[language][kind]}] {title}**"
    if language == "zh":
        parts = [
            header,
            f"问题：{problem}",
            f"建议：{suggestion}",
        ]
        if example_code:
            parts.append("示例：")
            parts.append(f"```{_code_lang_for_path(str(finding.get('path', '')))}\n{example_code}\n```")
    else:
        parts = [
            header,
            f"Issue: {problem}",
            f"Suggestion: {suggestion}",
        ]
        if example_code:
            parts.append("Example:")
            parts.append(f"```{_code_lang_for_path(str(finding.get('path', '')))}\n{example_code}\n```")

    parts.append(f"<!-- yuanrong-pr-review:{fingerprint} -->")
    return "\n\n".join(parts).strip() + "\n"
