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
        self.assertEqual(scan_text("source", credential_key + " = 0"), [])
        self.assertEqual(scan_text("source", credential_key + " = 0xC0FFEE01ULL"), [])
        self.assertEqual(scan_text("source", credential_key + " = kToken"), [])
        self.assertEqual(scan_text("source", credential_key + " = load_token(settings)"), [])
        self.assertEqual(scan_text("source", "client = ApiClient(" + credential_key + "=" + credential_key + ")"), [])
        self.assertEqual(scan_text("source", credential_key + " = os.environ.get(name, '').strip()"), [])
        self.assertEqual(scan_text("source", credential_key + ": str"), [])
        self.assertEqual(scan_text("source", 'LOG(INFO) << "token=" << token'), [])

    def test_allows_code_syntax_that_contains_credential_words(self) -> None:
        self.assertEqual(scan_text("source", '+#include "datasystem/common/token/client_access_token.h"'), [])
        self.assertEqual(scan_text("source", "+using namespace datasystem::token;"), [])
        self.assertEqual([match.category for match in scan_text("source", "token=abc123")],
                         ["credential or account assignment"])

    def test_allows_repo_safe_test_endpoints_and_blocks_private_endpoint(self) -> None:
        self.assertEqual(scan_text("test", "127.0.0.1:8080"), [])
        self.assertEqual(scan_text("test", "urma-mock-peer-a:9090"), [])
        self.assertEqual(scan_text("test", "example-peer:9090"), [])

        private_endpoint = "10." + "1.2.3:8080"
        self.assertEqual([match.category for match in scan_text("source", private_endpoint)], ["server IP or endpoint"])

    def test_allows_tmp_test_paths_but_blocks_sensitive_filesystem_paths(self) -> None:
        self.assertEqual(scan_text("test", "/tmp/datasystem_urma_mock_1000/uds.sock"), [])

        sensitive_root = "/" + "home/test"
        sensitive_file = "." + "ssh/id_rsa"
        sensitive_path = sensitive_root + "/" + sensitive_file
        self.assertEqual(
            [match.category for match in scan_text("source", sensitive_path)],
            ["local filesystem path with sensitive content"],
        )

    def test_scans_every_changed_file_patch_line(self) -> None:
        private_endpoint = "10." + "20.30.40"
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

    def test_does_not_flag_localhost_and_wildcard_ips(self) -> None:
        """127.0.0.1 and 0.0.0.0 are safe in any context (test/bind addresses)."""
        self.assertEqual(scan_text("test", 'HostPort("127.0.0.1", port)'), [])
        self.assertEqual(scan_text("test", 'server.start("0.0.0.0:9090")'), [])
        self.assertEqual(scan_text("source", 'auto addr = HostPort("0.0.0.0", 8080);'), [])

    def test_does_not_flag_link_local_ips(self) -> None:
        """169.254.x.x (link-local, APIPA) is commonly used in test/auto-config environments."""
        self.assertEqual(scan_text("test", 'auto ip = "169.254.1.1";'), [])
        self.assertEqual(scan_text("test", 'server.start("169.254.100.50:9999")'), [])

    def test_does_not_flag_rfc5737_documentation_ips(self) -> None:
        """RFC 5737 TEST-NET IPs (192.0.2.x, 198.51.100.x, 203.0.113.x) are documentation-only."""
        self.assertEqual(scan_text("test", 'client->Connect("192.0.2.1:8080")'), [])
        self.assertEqual(scan_text("test", 'auto ip = "198.51.100.1";'), [])
        self.assertEqual(scan_text("test", 'auto ip = "203.0.113.5";'), [])

    def test_does_not_flag_cxx_type_declarations_with_credential_param_names(self) -> None:
        """C++ function signatures with credential-like parameter names should not be flagged."""
        self.assertEqual(scan_text("source",
            'Status UpdateAkSk(const std::string &accessKey, SensitiveValue secretKey) = 0;'), [])
        self.assertEqual(scan_text("source",
            'DefaultClientRequestAuth(std::string clientId = "", std::string accessKey = "",'), [])
        self.assertEqual(scan_text("source",
            'void SetAccount(std::string account) { account_ = std::move(account); }'), [])

    def test_still_flags_real_private_network_ips(self) -> None:
        """Real private network IPs (10.x, 172.16.x, 192.168.x) must still be flagged."""
        real_lines = [
            'workerAddr = "10.20.30.40:8443";',
            'auto host = "192.168.50.100";',
            'auto endpoint = "10.30.40.50:443";',
            'auto addr = "172.30.40.10:8443";',
        ]
        for line in real_lines:
            matches = scan_text("source", line)
            self.assertTrue(
                any(m.category == "server IP or endpoint" for m in matches),
                f"Should flag real IP in: {line}",
            )

    def test_does_not_flag_plain_local_paths_without_sensitive_dirs(self) -> None:
        """Plain paths without .ssh/.config/secrets should not be flagged."""
        plain_paths = [
            '/a/b/c/logs/server.log',
            '/x/y/output/data.bin',
            '/p/q/r/config/settings.yaml',
            '/m/n/build/output/main.cpp',
        ]
        for line in plain_paths:
            self.assertEqual(scan_text("source", line), [],
                f"Should not flag plain path: {line}")

    def test_flags_local_paths_containing_sensitive_directories(self) -> None:
        """Paths containing .ssh/.config/.aws/.kube/secrets/credentials/tokens/.env should be flagged."""
        sensitive_paths = [
            '/a/b/.ssh/id_rsa',
            '/x/y/.config/credentials/db.json',
            '/p/q/.aws/credentials',
            '/m/secrets/api_token',
            '/r/s/.env',
            '/t/.kube/config',
        ]
        for line in sensitive_paths:
            matches = scan_text("source", line)
            self.assertTrue(
                any("filesystem" in m.category for m in matches),
                f"Should flag sensitive path: {line}",
            )


if __name__ == "__main__":
    unittest.main()
