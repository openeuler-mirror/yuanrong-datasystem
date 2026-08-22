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
import json
from pathlib import Path
import sys
import tempfile
import types
import unittest
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

        @staticmethod
        def valid_safe_path(path):
            return path

    def validate_no_injection(value):
        text = str(value)
        if any(token in text for token in (";", "`", "$(", "\n")):
            raise ValueError("potential command-injection characters")
        return text

    yr = types.ModuleType("yr")
    datasystem = types.ModuleType("yr.datasystem")
    cli = types.ModuleType("yr.datasystem.cli")
    common = types.ModuleType("yr.datasystem.cli.common")
    util = types.ModuleType("yr.datasystem.cli.common.util")
    command = types.ModuleType("yr.datasystem.cli.command")
    util.valid_safe_path = lambda path: path
    util.compare_and_process_config = lambda home_dir, config, default_config: {}
    util.get_timestamped_path = lambda path: path
    util.is_valid_address_port = lambda address: None
    util.validate_no_injection = validate_no_injection
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

START_PY = Path(__file__).resolve().parents[2] / "cli" / "start.py"
spec = importlib.util.spec_from_file_location("datasystem_cli_start_under_test", START_PY)
assert spec is not None
assert spec.loader is not None
start = importlib.util.module_from_spec(spec)
spec.loader.exec_module(start)

LOCK_OUTPUT = (
    "KV store error. Cannot open the key/value store. "
    "Cannot create/open database: lock file: /tmp/rocksdb/LOCK: "
    "Resource temporarily unavailable"
)


class ExitedWorker:
    pid = 1001

    def __init__(self, output):
        self.output = output

    def poll(self):
        return 255

    def communicate(self, timeout=10):
        del timeout
        return "", self.output


class RunningWorker:
    pid = 1002

    def poll(self):
        return None


class CliStartTest(unittest.TestCase):
    def setUp(self):
        self.command = start.Command.__new__(start.Command)
        self.command._base_dir = "/opt/yuanrong"
        self.command.logger = types.SimpleNamespace(
            info=lambda *args, **kwargs: None,
            warning=lambda *args, **kwargs: None,
            error=lambda *args, **kwargs: None,
        )

    @staticmethod
    def worker_params():
        return {
            "worker_address": "127.0.0.1:31501",
            "coordinator_address": "127.0.0.1:31511",
            "ready_check_path": "/tmp/worker.ready",
            "rocksdb_store_dir": "/tmp/rocksdb",
        }

    @staticmethod
    def make_start_command(timeout=10):
        command = start.Command()
        command._base_dir = "/opt/yuanrong"
        command._timeout = timeout
        return command

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

    def test_coordinator_config_passes_log_filename_to_process(self):
        config = {
            "service_type": {"value": "coordinator"},
            "log_filename": {"value": "kvcache_coordinator"},
            "log_dir": {"value": "/tmp/datasystem/c1/logs"},
        }
        with tempfile.NamedTemporaryFile("w", encoding="utf-8", delete=False) as config_file:
            json.dump(config, config_file)
            config_path = config_file.name
        self.addCleanup(lambda: Path(config_path).unlink(missing_ok=True))

        params = self.command.load_config(config_path, start.Command._COORDINATOR_SERVICE)
        command = self.command.build_coordinator_command(params)

        self.assertEqual(params["log_filename"], "kvcache_coordinator")
        self.assertIn("--log_filename=kvcache_coordinator", command)
        self.assertIn("--log_dir=/tmp/datasystem/c1/logs", command)
        self.assertNotIn("--service_type=coordinator", command)

    def test_coordinator_config_declares_metrics_monitor_flags(self):
        self.command._base_dir = str(START_PY.parent / "deploy" / "conf")

        keys = self.command.get_default_config_keys("coordinator_config.json")

        self.assertTrue(
            {"log_monitor", "json_log_monitor", "log_monitor_interval_ms"}.issubset(keys)
        )

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
        self.assertEqual(
            coordinator_args.coordinator_args,
            ["--coordinator_address", "127.0.0.1:31511"],
        )
        self.assertEqual(
            combined_args.coordinator_worker_args,
            ["--worker_address", "127.0.0.1:31501"],
        )

    def test_worker_start_error_classifiers_cover_issue976_boundaries(self):
        address_outputs = [
            "Port 31501 on 127.0.0.1 is already in use",
            "Socket bind failed: errno = 98",
            "worker failed: EADDRINUSE",
        ]

        self.assertTrue(start.Command.is_retryable_worker_store_lock_error(LOCK_OUTPUT))
        for output in address_outputs:
            with self.subTest(output=output):
                self.assertTrue(start.Command.is_worker_address_conflict_error(output))
        self.assertFalse(
            start.Command.is_retryable_worker_store_lock_error(
                "Worker runtime error: RPC deadline exceeded"
            )
        )
        self.assertFalse(
            start.Command.is_worker_address_conflict_error(
                "mbind failed after trying all NUMA nodes"
            )
        )

    def test_worker_store_lock_file_preflight_distinguishes_held_and_available_lock(self):
        fake_fcntl = types.SimpleNamespace(LOCK_EX=1, LOCK_NB=2, LOCK_UN=4)
        err = OSError()
        err.errno = start.errno.EAGAIN

        with mock.patch.object(start.os.path, "exists", return_value=True), \
            mock.patch("builtins.open", mock.mock_open()), \
            mock.patch.dict(sys.modules, {"fcntl": fake_fcntl}):
            fake_fcntl.lockf = mock.Mock(side_effect=err)
            self.assertTrue(
                start.Command.is_worker_store_lock_unavailable(
                    {"rocksdb_store_dir": "/tmp/rocksdb"}
                )
            )
            fake_fcntl.lockf = mock.Mock(return_value=None)
            self.assertFalse(
                start.Command.is_worker_store_lock_unavailable(
                    {"rocksdb_store_dir": "/tmp/rocksdb"}
                )
            )

    def test_start_worker_reports_preflight_address_conflict_without_launching_worker(self):
        command = self.make_start_command()
        params = self.worker_params()

        with mock.patch.object(command, "is_tcp_ready", return_value=True), \
            mock.patch.object(start.subprocess, "Popen") as popen:
            with self.assertRaisesRegex(
                RuntimeError, "Worker address 127.0.0.1:31501 is already in use"
            ):
                command.start_worker(params, use_ums=False)

        popen.assert_not_called()

    def test_start_worker_reports_child_bind_conflict(self):
        command = self.make_start_command()
        params = self.worker_params()

        with mock.patch.object(command, "is_tcp_ready", return_value=False), \
            mock.patch.object(command, "build_command", return_value=["datasystem_worker"]), \
            mock.patch.object(start.os.path, "exists", return_value=False), \
            mock.patch.object(
                start.subprocess,
                "Popen",
                return_value=ExitedWorker("Port 31501 on 127.0.0.1 is already in use"),
            ) as popen:
            with self.assertRaisesRegex(
                RuntimeError, "Worker address 127.0.0.1:31501 is already in use"
            ) as caught:
                command.start_worker(params, use_ums=False)

        self.assertEqual(popen.call_count, 1)
        message = str(caught.exception)
        self.assertIn("worker_address/bind_address", message)
        self.assertNotIn("metadata store lock", message)

    def test_start_worker_reports_preflight_store_lock_without_launching_worker(self):
        command = self.make_start_command(timeout=0.15)
        params = self.worker_params()

        with mock.patch.object(command, "is_tcp_ready", return_value=False), \
            mock.patch.object(command, "build_command", return_value=["datasystem_worker"]), \
            mock.patch.object(command, "is_worker_store_lock_unavailable", return_value=True), \
            mock.patch.object(start.os.path, "exists", return_value=False), \
            mock.patch.object(start.subprocess, "Popen") as popen, \
            mock.patch.object(start.time, "monotonic", side_effect=[0, 0, 0.05, 0.2]), \
            mock.patch.object(start.time, "sleep"):
            with self.assertRaisesRegex(
                RuntimeError, "Worker metadata store lock is unavailable"
            ) as caught:
                command.start_worker(params, use_ums=False)

        popen.assert_not_called()
        message = str(caught.exception)
        self.assertIn("worker_address=127.0.0.1:31501", message)
        self.assertIn("rocksdb_store_dir=/tmp/rocksdb", message)
        self.assertIn("after 1 startup retries", message)

    def test_start_worker_reports_store_lock_conflict_after_retry_deadline(self):
        command = self.make_start_command(timeout=0.15)
        params = self.worker_params()

        with mock.patch.object(command, "is_tcp_ready", return_value=False), \
            mock.patch.object(command, "build_command", return_value=["datasystem_worker"]), \
            mock.patch.object(command, "is_worker_store_lock_unavailable", return_value=False), \
            mock.patch.object(start.os.path, "exists", return_value=False), \
            mock.patch.object(
                start.subprocess,
                "Popen",
                side_effect=[ExitedWorker(LOCK_OUTPUT), ExitedWorker(LOCK_OUTPUT)],
            ) as popen, \
            mock.patch.object(
                start.time,
                "monotonic",
                side_effect=[0, 0, 0, 0.05, 0.05, 0.05, 0.2, 0.2],
            ), \
            mock.patch.object(start.time, "sleep"), \
            mock.patch.object(start.os, "kill"):
            with self.assertRaisesRegex(
                RuntimeError, "Worker metadata store lock is unavailable"
            ) as caught:
                command.start_worker(params, use_ums=False)

        self.assertEqual(popen.call_count, 2)
        message = str(caught.exception)
        self.assertIn("worker_address=127.0.0.1:31501", message)
        self.assertIn("rocksdb_store_dir=/tmp/rocksdb", message)
        self.assertIn("after 2 startup retries", message)

    def test_start_worker_succeeds_after_store_lock_retry_when_ready_file_appears(self):
        command = self.make_start_command()
        params = self.worker_params()

        with mock.patch.object(command, "is_tcp_ready", return_value=False), \
            mock.patch.object(command, "build_command", return_value=["datasystem_worker"]), \
            mock.patch.object(command, "is_worker_store_lock_unavailable", return_value=False), \
            mock.patch.object(start.os.path, "exists", side_effect=[False, True]), \
            mock.patch.object(
                start.subprocess,
                "Popen",
                side_effect=[ExitedWorker(LOCK_OUTPUT), RunningWorker()],
            ) as popen, \
            mock.patch.object(start.time, "monotonic", side_effect=[0, 0, 0, 0.1, 0.1, 0.1]), \
            mock.patch.object(start.time, "sleep"):
            command.start_worker(params, use_ums=False)

        self.assertEqual(popen.call_count, 2)


if __name__ == "__main__":
    unittest.main()
