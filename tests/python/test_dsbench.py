# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Tests for dsbench child-process environment generation."""

import argparse
import os
import unittest
from unittest.mock import patch

from yr.datasystem.cli.benchmark.common import BenchTestCase
from yr.datasystem.cli.benchmark.task import BenchArgs


def make_test_case():
    args = argparse.Namespace(min_log_level=2, log_monitor_enable=False)
    bench_args = BenchArgs(
        name="bench_kv",
        start_time="20260724160000",
        cwd="/tmp",
        log_dir="/tmp/dsbench-test",
        result_csv_file="result.csv",
        args=args,
    )
    return BenchTestCase("set", bench_args, None)


class DsbenchEnvironmentTest(unittest.TestCase):
    def test_use_brpc_environment_is_forwarded(self):
        test_case = make_test_case()
        for value in ("true", "false"):
            with self.subTest(value=value), patch.dict(os.environ, {"DATASYSTEM_USE_BRPC": value}):
                self.assertEqual(test_case.generate_env()["DATASYSTEM_USE_BRPC"], value)

    def test_use_brpc_environment_is_omitted_when_unset(self):
        test_case = make_test_case()
        with patch.dict(os.environ, {}, clear=True):
            self.assertNotIn("DATASYSTEM_USE_BRPC", test_case.generate_env())


if __name__ == "__main__":
    unittest.main()
