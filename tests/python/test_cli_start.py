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

import unittest

from yr.datasystem.cli import start


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


if __name__ == "__main__":
    unittest.main()
