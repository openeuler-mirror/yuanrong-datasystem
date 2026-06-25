#!/usr/bin/env python3
from __future__ import annotations

import argparse
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from script_imports import load_script_module


ReviewError = load_script_module("common").ReviewError
parse_patch = load_script_module("diff_position").parse_patch
review_pr = load_script_module("review_pr")
sensitive_scan = load_script_module("sensitive_scan")
format_sensitive_scan_failure = sensitive_scan.format_sensitive_scan_failure
scan_changed_file = sensitive_scan.scan_changed_file
scan_text = sensitive_scan.scan_text


COMPANY = "hua" + "wei"


class SensitiveScanTests(unittest.TestCase):
    def test_blocks_company_label_outside_allowed_attribution(self) -> None:
        matches = scan_text("PR description", f"verified in {COMPANY} internal environment")

        self.assertEqual([match.category for match in matches], ["company identifier"])

    def test_allows_company_label_in_copyright_and_library_attribution(self) -> None:
        text = "\n".join(
            [
                f"Copyright (c) 2024 {COMPANY} Technologies Co., Ltd.",
                f"Third-party library derived from {COMPANY} open source runtime.",
            ]
        )

        self.assertEqual(scan_text("source", text), [])

    def test_detects_employee_identifier_without_echoing_value(self) -> None:
        private_value = "A123456"
        employee_id_label = "employee" + " id"
        matches = scan_text("PR body", "owner " + employee_id_label + ": " + private_value)

        self.assertEqual([match.category for match in matches], ["employee identifier"])
        message = format_sensitive_scan_failure(matches)
        self.assertNotIn(private_value, message)

    def test_detects_credential_literal_but_allows_code_variable_forwarding(self) -> None:
        credential_key = "to" + "ken"
        secret_value = "abc123"
        matches = scan_text("PR body", credential_key + "=" + secret_value)

        self.assertEqual([match.category for match in matches], ["credential or account assignment"])
        self.assertEqual(scan_text("source", credential_key + " = load_token(settings)"), [])
        self.assertEqual(scan_text("source", "client = ApiClient(" + credential_key + "=" + credential_key + ")"), [])
        self.assertEqual(scan_text("source", credential_key + " = os.environ.get(name, '').strip()"), [])
        self.assertEqual(scan_text("source", credential_key + ": str"), [])

    def test_scans_every_changed_file_patch_line(self) -> None:
        private_endpoint = "10." + "1.2.3"
        patch = "@@ -0,0 +1,2 @@\n+safe line\n+build host: " + private_endpoint + "\n"

        matches, scanned_lines = scan_changed_file(
            path="docs/runbook.md",
            patch=patch,
            position_map=parse_patch(patch),
            file_index=1,
        )

        self.assertEqual(scanned_lines, 2)
        self.assertEqual([match.category for match in matches], ["server IP or endpoint"])

    def test_missing_patch_blocks_prepare_because_file_was_not_scanned(self) -> None:
        matches, scanned_lines = scan_changed_file(
            path="docs/binary.bin",
            patch="",
            position_map=[],
            file_index=1,
        )

        self.assertEqual(scanned_lines, 0)
        self.assertEqual([match.category for match in matches], ["unscannable changed file"])

    def test_prepare_fails_before_writing_bundle_when_sensitive_content_is_found(self) -> None:
        class FakeClient:
            @staticmethod
            def get_pull(number: int) -> dict[str, object]:
                return {
                    "number": number,
                    "title": "safe title",
                    "body": "safe body",
                    "state": "open",
                    "base": {"ref": "master"},
                    "head": {"ref": "topic"},
                    "user": {"login": "contributor"},
                }

            @staticmethod
            def list_pull_files(_number: int) -> list[dict[str, object]]:
                return [
                    {
                        "filename": "docs/leak.md",
                        "status": "modified",
                        "patch": "@@ -0,0 +1 @@\n+internal company: " + COMPANY + "\n",
                    }
                ]

            @staticmethod
            def list_pull_comments(_number: int) -> list[dict[str, object]]:
                return []

        with tempfile.TemporaryDirectory() as tmpdir:
            settings = {
                "gitcode": {"pr_url_prefix": "https://example.invalid/pull/"},
                "repo": {},
                "review": {
                    "snippet_radius": 1,
                    "max_snippets_per_file": 1,
                    "max_chars_per_snippet": 200,
                    "suggestion_limit_per_file": 2,
                    "need_to_resolve": True,
                },
                "cache": {"root": tmpdir},
            }

            with (
                mock.patch.object(review_pr, "load_settings", return_value=settings),
                mock.patch.object(review_pr, "_client", return_value=FakeClient()),
                mock.patch.object(review_pr, "ensure_local_repo", return_value=(None, [])),
            ):
                with self.assertRaises(ReviewError) as exc:
                    review_pr.prepare(argparse.Namespace(pr_ref="42"))

            self.assertIn("Sensitive information scan failed", str(exc.exception))
            self.assertNotIn(COMPANY, str(exc.exception))
            self.assertFalse((Path(tmpdir) / "pr-42" / "bundle.json").exists())


if __name__ == "__main__":
    unittest.main()
