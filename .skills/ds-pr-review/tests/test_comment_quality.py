#!/usr/bin/env python3
from __future__ import annotations

import unittest

from script_imports import load_script_module


format_comment = load_script_module("comment_formatter").format_comment
validate_findings = load_script_module("finding_validator").validate_findings


def _valid_finding() -> dict[str, object]:
    return {
        "path": "src/datasystem/common/log/access_recorder.h",
        "line": 181,
        "type": "design",
        "severity": "critical",
        "title": "收敛采样细节",
        "evidence": "`AccessRecorderGuard` 在调用点暴露 `ShouldRecord()`。",
        "problem": "采样判断和 `RequestParam` 构造散落在业务调用点。",
        "impact": "这会削弱内聚性，并让 sampled-out 热路径重新引入字符串构造风险。",
        "suggestion": "把延迟构造收敛到 `AccessRecorder::Record(...)`。",
        "example_code": (
            "AccessRecorder accessPoint(AccessRecorderKey::DS_KV_CLIENT_GET);\n"
            "Status rc = impl_->Get(keys, subTimeoutMs, buffers);\n"
            "accessPoint.Record(rc, buffers.size(), [&](RequestParam &req) {\n"
            "    req.objectKey = objectKeysToString(keys);\n"
            "});"
        ),
        "verification": "执行 dry-run，确认示例代码渲染为 Markdown 代码块。",
    }


class CommentQualityTests(unittest.TestCase):
    def test_zh_formatter_renders_structured_sections_and_code_block(self) -> None:
        body = format_comment(_valid_finding(), "zh", "fp")

        self.assertIn("证据：", body)
        self.assertIn("影响：", body)
        self.assertIn("建议：", body)
        self.assertIn("验证：", body)
        self.assertIn("```cpp\nAccessRecorder accessPoint", body)

    def test_validator_accepts_valid_zh_finding(self) -> None:
        self.assertEqual(validate_findings([_valid_finding()], "zh"), [])

    def test_validator_rejects_wrong_language(self) -> None:
        finding = _valid_finding()
        finding.update(
            {
                "title": "Centralize sampling",
                "evidence": "`AccessRecorderGuard` exposes `ShouldRecord()`.",
                "problem": "Sampling decisions are spread across call sites.",
                "impact": "The hot path can regress.",
                "suggestion": "Use lazy `AccessRecorder::Record(...)`.",
                "verification": "Run dry-run.",
            }
        )

        errors = validate_findings([finding], "zh")
        self.assertTrue(any("does not contain Chinese" in error for error in errors))

    def test_validator_rejects_multiline_code_in_suggestion(self) -> None:
        finding = _valid_finding()
        finding["example_code"] = ""
        finding["suggestion"] = "建议改成：\nif (ok) {\n    return;\n}"

        errors = validate_findings([finding], "zh")
        self.assertTrue(any("looks like multi-line code" in error for error in errors))

    def test_validator_rejects_fenced_example_code(self) -> None:
        finding = _valid_finding()
        finding["example_code"] = "```cpp\nint value = 1;\n```"

        errors = validate_findings([finding], "zh")
        self.assertTrue(any("without Markdown fences" in error for error in errors))


if __name__ == "__main__":
    unittest.main()
