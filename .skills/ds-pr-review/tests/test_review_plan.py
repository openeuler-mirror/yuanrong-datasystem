#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from script_imports import load_script_module


review_pr = load_script_module("review_pr")
REVIEW_LINE_THRESHOLD = review_pr.REVIEW_LINE_THRESHOLD
build_change_stats = review_pr.build_change_stats
build_review_plan = review_pr.build_review_plan


class ReviewPlanTests(unittest.TestCase):
    def test_small_change_uses_single_integrated_pass(self) -> None:
        stats = build_change_stats(
            [
                {"filename": "src/a.cpp", "additions": 40, "deletions": 20},
                {"filename": "tests/a_test.cpp", "additions": "30", "deletions": 9},
            ]
        )
        plan = build_review_plan(stats)

        self.assertEqual(stats["total_changed_lines"], REVIEW_LINE_THRESHOLD - 1)
        self.assertEqual(plan["mode"], "single_integrated_pass")
        self.assertFalse(plan["parallelizable"])
        self.assertEqual(len(plan["rounds"]), 1)
        self.assertIn("existing gates", plan["rounds"][0]["instructions"])

    def test_large_change_requires_parallel_multi_round_review(self) -> None:
        stats = build_change_stats(
            [
                {"filename": "src/a.cpp", "additions": 80, "deletions": 20},
                {"filename": "src/b.cpp", "additions": 1, "deletions": 0},
            ]
        )
        plan = build_review_plan(stats)

        self.assertEqual(stats["total_changed_lines"], REVIEW_LINE_THRESHOLD + 1)
        self.assertEqual(plan["mode"], "parallel_multi_round")
        self.assertTrue(plan["parallelizable"])
        self.assertGreaterEqual(len(plan["rounds"]), 5)
        all_gates = " ".join(" ".join(round_info["gates"]) for round_info in plan["rounds"])
        self.assertIn("Functional and design-contract correctness", all_gates)
        self.assertIn("Hot-path performance gate", all_gates)
        self.assertIn("Build and packaging", all_gates)
        self.assertNotIn("R1-R7", all_gates)

    def test_prepare_writes_change_stats_and_review_plan_to_bundle(self) -> None:
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
                        "filename": "src/a.cpp",
                        "status": "modified",
                        "additions": 80,
                        "deletions": 20,
                        "patch": "@@ -1,1 +1,1 @@\n-int old_value;\n+int new_value;\n",
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
                mock.patch("builtins.print"),
            ):
                review_pr.prepare(argparse.Namespace(pr_ref="42"))

            bundle = json.loads((Path(tmpdir) / "pr-42" / "bundle.json").read_text(encoding="utf-8"))

        self.assertEqual(bundle["change_stats"]["total_changed_lines"], REVIEW_LINE_THRESHOLD)
        self.assertEqual(bundle["review_plan"]["mode"], "parallel_multi_round")
        self.assertTrue(bundle["review_plan"]["parallelizable"])


if __name__ == "__main__":
    unittest.main()
