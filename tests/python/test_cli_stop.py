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
"""Tests for dscli stop process-exit confirmation."""

import importlib.util
import sys
import types
import unittest
from pathlib import Path
from unittest import mock

def install_cli_import_stubs_if_needed():
    try:
        import yr.datasystem.cli.common.util  # noqa: F401
        from yr.datasystem.cli.command import BaseCommand  # noqa: F401
        return
    except ModuleNotFoundError:
        pass

    logger = types.SimpleNamespace(
        info=lambda *args, **kwargs: None,
        warning=lambda *args, **kwargs: None,
        error=lambda *args, **kwargs: None,
    )

    class BaseCommand:
        SUCCESS = 0
        FAILURE = 1

        def __init__(self):
            self.logger = logger

    yr = types.ModuleType("yr")
    datasystem = types.ModuleType("yr.datasystem")
    cli = types.ModuleType("yr.datasystem.cli")
    common = types.ModuleType("yr.datasystem.cli.common")
    util = types.ModuleType("yr.datasystem.cli.common.util")
    command = types.ModuleType("yr.datasystem.cli.command")
    util.valid_safe_path = lambda path: path
    util.is_valid_address_port = lambda address: None
    command.BaseCommand = BaseCommand

    yr.datasystem = datasystem
    datasystem.cli = cli
    cli.common = common
    cli.command = command
    common.util = util
    sys.modules.setdefault("yr", yr)
    sys.modules.setdefault("yr.datasystem", datasystem)
    sys.modules.setdefault("yr.datasystem.cli", cli)
    sys.modules.setdefault("yr.datasystem.cli.common", common)
    sys.modules.setdefault("yr.datasystem.cli.common.util", util)
    sys.modules.setdefault("yr.datasystem.cli.command", command)


install_cli_import_stubs_if_needed()

STOP_PY = Path(__file__).resolve().parents[2] / "cli" / "stop.py"
spec = importlib.util.spec_from_file_location("datasystem_cli_stop_under_test", STOP_PY)
assert spec is not None
assert spec.loader is not None
stop = importlib.util.module_from_spec(spec)
spec.loader.exec_module(stop)


class CliStopTest(unittest.TestCase):
    def setUp(self):
        self.command = stop.Command.__new__(stop.Command)
        self.command.logger = mock.Mock()
        self.command.get_unique_pid = mock.Mock(return_value=1234)
        self.command.calculate_stop_timeout = mock.Mock(return_value=180)
        self.command.graceful_kill = mock.Mock()
        self.command.force_kill = mock.Mock(return_value=True)

    def test_force_kill_waits_for_process_exit_before_success(self):
        self.command.wait_exit = mock.Mock(side_effect=[False, True])

        self.command.stop_service(self.command._worker_service, "127.0.0.1:31501", {})

        self.command.graceful_kill.assert_called_once_with(1234)
        self.command.force_kill.assert_called_once_with(1234)
        self.assertEqual(self.command.wait_exit.call_count, 2)

    def test_force_kill_fails_when_process_still_exists(self):
        self.command.wait_exit = mock.Mock(side_effect=[False, False])

        with self.assertRaisesRegex(RuntimeError, "process still exists"):
            self.command.stop_service(self.command._worker_service, "127.0.0.1:31501", {})

        self.command.force_kill.assert_called_once_with(1234)
        self.assertEqual(self.command.wait_exit.call_count, 2)


if __name__ == "__main__":
    unittest.main()
