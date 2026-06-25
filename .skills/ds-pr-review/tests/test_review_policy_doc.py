#!/usr/bin/env python3
from __future__ import annotations

import re
import unittest
from pathlib import Path


SKILL_TEXT = (Path(__file__).resolve().parents[1] / "SKILL.md").read_text(encoding="utf-8")


def _section(title: str) -> str:
    pattern = re.compile(rf"^## {re.escape(title)}\n(?P<body>.*?)(?=^## |\Z)", re.MULTILINE | re.DOTALL)
    match = pattern.search(SKILL_TEXT)
    if not match:
        raise AssertionError(f"missing section {title!r}")
    return match.group("body")


class ReviewPolicyDocTests(unittest.TestCase):
    def test_commercial_infra_checks_are_integrated_into_existing_gates(self) -> None:
        strict_passes = _section("Strict Review Passes")
        system_gates = _section("System-Wide Design Gates")
        combined = strict_passes + "\n" + system_gates

        self.assertIn(">=100", strict_passes)
        self.assertIn("existing gates", strict_passes)
        self.assertIn("review_plan.mode", strict_passes)
        self.assertIn("parallel_multi_round", strict_passes)
        self.assertIn("deduplicated", strict_passes)
        self.assertIn("exception safety", combined)
        self.assertIn("erase-then-insert", combined)
        self.assertIn("AB/BA", combined)
        self.assertIn("Start/Stop", combined)
        self.assertIn("QPS", combined)
        self.assertIn("Big-O", combined)
        self.assertIn("cache line", combined)
        self.assertIn("fault injection", combined)

    def test_policy_does_not_create_standalone_r_dimension_checklist(self) -> None:
        self.assertIsNone(re.search(r"^##\s+R[1-7]\b", SKILL_TEXT, re.MULTILINE))
        self.assertNotIn("R1-R7", SKILL_TEXT)


if __name__ == "__main__":
    unittest.main()
