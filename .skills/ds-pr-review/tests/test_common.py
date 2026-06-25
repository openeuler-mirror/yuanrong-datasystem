#!/usr/bin/env python3
from __future__ import annotations

import unittest

from script_imports import load_script_module


parse_pr_ref = load_script_module("common").parse_pr_ref


class CommonTests(unittest.TestCase):
    def test_parse_plain_number(self) -> None:
        self.assertEqual(parse_pr_ref("1041"), 1041)

    def test_parse_gitcode_merge_request_url(self) -> None:
        self.assertEqual(
            parse_pr_ref("https://gitcode.com/openeuler/yuanrong-datasystem/merge_requests/1041"),
            1041,
        )

    def test_parse_pull_url(self) -> None:
        self.assertEqual(parse_pr_ref("https://gitcode.com/openeuler/yuanrong-datasystem/pull/1041"), 1041)


if __name__ == "__main__":
    unittest.main()
