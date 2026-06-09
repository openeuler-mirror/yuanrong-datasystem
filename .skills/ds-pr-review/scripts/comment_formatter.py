#!/usr/bin/env python3
from __future__ import annotations

from pathlib import Path
from typing import Any


TYPE_LABELS = {
    "zh": {
        "bug": "Bug",
        "build": "构建",
        "compatibility": "兼容性",
        "correctness": "正确性",
        "security": "安全",
        "performance": "性能",
        "design": "设计",
        "documentation": "文档",
        "test": "测试",
    },
    "en": {
        "bug": "Bug",
        "build": "Build",
        "compatibility": "Compatibility",
        "correctness": "Correctness",
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
    evidence = str(finding.get("evidence", "")).strip()
    problem = str(finding.get("problem", "")).strip()
    impact = str(finding.get("impact", "")).strip()
    suggestion = str(finding.get("suggestion", "")).strip()
    example_code = str(finding.get("example_code", "")).rstrip()
    verification = str(finding.get("verification", "")).strip()

    severity_label = SEVERITY_LABELS[language].get(str(severity), str(severity))
    type_label = TYPE_LABELS[language].get(str(kind), str(kind))
    header = f"**[{severity_label}][{type_label}] {title}**"
    if language == "zh":
        parts = [header]
        if evidence:
            parts.append(f"证据：{evidence}")
        parts.append(f"问题：{problem}")
        if impact:
            parts.append(f"影响：{impact}")
        parts.append(f"建议：{suggestion}")
        if example_code:
            parts.append("示例：")
            parts.append(f"```{_code_lang_for_path(str(finding.get('path', '')))}\n{example_code}\n```")
        if verification:
            parts.append(f"验证：{verification}")
    else:
        parts = [header]
        if evidence:
            parts.append(f"Evidence: {evidence}")
        parts.append(f"Issue: {problem}")
        if impact:
            parts.append(f"Impact: {impact}")
        parts.append(f"Suggestion: {suggestion}")
        if example_code:
            parts.append("Example:")
            parts.append(f"```{_code_lang_for_path(str(finding.get('path', '')))}\n{example_code}\n```")
        if verification:
            parts.append(f"Verification: {verification}")

    parts.append(f"<!-- yuanrong-pr-review:{fingerprint} -->")
    return "\n\n".join(parts).strip() + "\n"
