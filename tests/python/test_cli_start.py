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
"""Tests for dscli start argument validation."""

import argparse
import importlib.util
import unittest
from pathlib import Path

START_PY = Path(__file__).resolve().parents[2] / "cli" / "start.py"
spec = importlib.util.spec_from_file_location("datasystem_cli_start_under_test", START_PY)
assert spec is not None
assert spec.loader is not None
start = importlib.util.module_from_spec(spec)
spec.loader.exec_module(start)


class CliStartTest(unittest.TestCase):
    def setUp(self):
        self.command = start.Command.__new__(start.Command)
        self.command._base_dir = "/opt/yuanrong"

    def test_build_command_accepts_valid_kv_events_json(self):
        config = (
            '{"bind_endpoint":"tcp://0.0.0.0:5557",'
            '"backend_id":"127.0.0.1:31501",'
            '"tenant_id":"default","dp_rank":0}'
        )

        command = self.command.build_command(
            {"kv_events_config": config}, use_ums=False
        )

        self.assertIn(f"--kv_events_config={config}", command)

    def test_process_params_rejects_injection_in_kv_events_string_value(self):
        config = (
            '{"bind_endpoint":"tcp://0.0.0.0:5557",'
            '"backend_id":"worker-0;touch-invalid"}'
        )

        with self.assertRaisesRegex(
            ValueError, "potential command-injection characters"
        ):
            dict(self.command.process_params({"kv_events_config": config}))

    def parse_start_args(self, args):
        parser = argparse.ArgumentParser(allow_abbrev=False)
        self.command.add_arguments(parser)
        return parser.parse_args(args)

    def test_worker_config_aliases_select_worker_config_path(self):
        for option in ("-W", "--worker_config_path", "-f", "--config_path"):
            with self.subTest(option=option):
                args = self.parse_start_args([option, "worker_config.json"])
                self.assertEqual(args.worker_config_path, "worker_config.json")
                self.assertIsNone(args.coordinator_config_path)

    def test_coordinator_config_uses_uppercase_c(self):
        args = self.parse_start_args(["-C", "coordinator_config.json"])

        self.assertEqual(args.coordinator_config_path, "coordinator_config.json")
        self.assertIsNone(args.worker_config_path)

    def test_physcpubind_keeps_long_option_only(self):
        args = self.parse_start_args(
            ["--physcpubind", "0-7", "-w", "--worker_address", "127.0.0.1:31501"]
        )

        self.assertEqual(args.physcpubind, "0-7")
        self.assertEqual(args.worker_args, ["--worker_address", "127.0.0.1:31501"])

    def test_direct_start_short_aliases_capture_service_arguments(self):
        worker_args = self.parse_start_args(["-w", "--worker_address", "127.0.0.1:31501"])
        coordinator_args = self.parse_start_args(["-c", "--coordinator_address", "127.0.0.1:31511"])
        combined_args = self.parse_start_args(["-a", "--worker_address", "127.0.0.1:31501"])

        self.assertEqual(worker_args.worker_args, ["--worker_address", "127.0.0.1:31501"])
        self.assertEqual(coordinator_args.coordinator_args, ["--coordinator_address", "127.0.0.1:31511"])
        self.assertEqual(combined_args.coordinator_worker_args, ["--worker_address", "127.0.0.1:31501"])


if __name__ == "__main__":
    unittest.main()
