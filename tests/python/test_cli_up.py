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
"""Tests for dscli up command construction."""

import argparse
import importlib.util
from pathlib import Path
import shlex
import sys
import types
import unittest


def install_cli_import_stubs_if_needed():
    try:
        import yr.datasystem.cli.common.util  # noqa: F401
        from yr.datasystem.cli.common.constant import ClusterConfig  # noqa: F401
        from yr.datasystem.cli.common.parallel import ParallelMixin  # noqa: F401
        from yr.datasystem.cli.command import BaseCommand  # noqa: F401
        return
    except ModuleNotFoundError:
        pass

    class BaseCommand:
        SUCCESS = 0
        FAILURE = 1

        def __init__(self):
            self.logger = types.SimpleNamespace(
                info=lambda *args, **kwargs: None,
                warning=lambda *args, **kwargs: None,
                error=lambda *args, **kwargs: None,
            )

        @staticmethod
        def valid_safe_path(path):
            return path

    class ClusterConfig:
        SSH_PRIVATE_KEY = "ssh_private_key"
        SSH_USER_NAME = "ssh_user_name"
        WORKER_NODES = "worker_nodes"
        WORKER_PORT = "worker_port"
        WORKER_CONFIG_PATH = "worker_config_path"
        METASTORE_HEAD_NODE = "metastore_head_node"

    class ParallelMixin:
        pass

    yr = types.ModuleType("yr")
    datasystem = types.ModuleType("yr.datasystem")
    cli = types.ModuleType("yr.datasystem.cli")
    common = types.ModuleType("yr.datasystem.cli.common")
    util = types.ModuleType("yr.datasystem.cli.common.util")
    command = types.ModuleType("yr.datasystem.cli.command")
    constant = types.ModuleType("yr.datasystem.cli.common.constant")
    parallel = types.ModuleType("yr.datasystem.cli.common.parallel")

    def validate_no_injection(value):
        text = str(value)
        if any(token in text for token in (";", "`", "$(", "\n")):
            raise ValueError("potential command-injection characters")
        return text

    util.valid_safe_path = lambda path: path
    util.compare_and_process_config = lambda home_dir, config, default_config: {}
    util.get_timestamped_path = lambda path: path
    util.is_valid_address_port = lambda address: None
    util.validate_no_injection = validate_no_injection
    command.BaseCommand = BaseCommand
    constant.ClusterConfig = ClusterConfig
    parallel.ParallelMixin = ParallelMixin

    yr.datasystem = datasystem
    datasystem.cli = cli
    cli.common = common
    cli.command = command
    common.util = util
    common.constant = constant
    common.parallel = parallel
    sys.modules.setdefault("yr", yr)
    sys.modules.setdefault("yr.datasystem", datasystem)
    sys.modules.setdefault("yr.datasystem.cli", cli)
    sys.modules.setdefault("yr.datasystem.cli.common", common)
    sys.modules.setdefault("yr.datasystem.cli.common.util", util)
    sys.modules.setdefault("yr.datasystem.cli.command", command)
    sys.modules.setdefault("yr.datasystem.cli.common.constant", constant)
    sys.modules.setdefault("yr.datasystem.cli.common.parallel", parallel)


install_cli_import_stubs_if_needed()

UP_PY = Path(__file__).resolve().parents[2] / "cli" / "up.py"
spec = importlib.util.spec_from_file_location("datasystem_cli_up_under_test", UP_PY)
assert spec is not None
assert spec.loader is not None
up = importlib.util.module_from_spec(spec)
spec.loader.exec_module(up)


class CliUpTest(unittest.TestCase):
    def setUp(self):
        self.command = up.Command.__new__(up.Command)
        self.command._timeout = 90

    def parse_up_args(self, args):
        parser = argparse.ArgumentParser(allow_abbrev=False)
        self.command.add_arguments(parser)
        return parser.parse_args(args)

    def test_jemalloc_prof_conf_argument_is_available(self):
        args = self.parse_up_args(
            [
                "--jemalloc_prof_conf",
                "prof_final:true,lg_prof_sample:20",
                "-f",
                "cluster_config.json",
            ]
        )

        self.assertEqual(
            args.jemalloc_prof_conf, "prof_final:true,lg_prof_sample:20"
        )

    def test_remote_start_command_forwards_conf_as_single_argument(self):
        conf = "prof_final:true,prof_prefix:/tmp/heap dir/worker"
        command = self.command.build_remote_start_cmd(
            "/tmp/worker config.json",
            use_ums=False,
            use_numactl=False,
            numactl_opts={},
            jemalloc_prof_conf=conf,
        )

        self.assertEqual(
            shlex.split(command),
            [
                "dscli",
                "start",
                "-t",
                "90",
                "--jemalloc_prof_conf",
                conf,
                "-f",
                "/tmp/worker config.json",
            ],
        )

    def test_remote_start_command_keeps_conf_quoted_with_numactl(self):
        conf = "prof_final:true,prof_prefix:/tmp/$(invalid)/worker"
        command = self.command.build_remote_start_cmd(
            "/tmp/worker.json",
            use_ums=True,
            use_numactl=True,
            numactl_opts={"cpunodebind": "0"},
            jemalloc_prof_conf=conf,
        )

        self.assertEqual(
            shlex.split(command),
            [
                "numactl",
                "--cpunodebind=0",
                "dscli",
                "start",
                "-t",
                "90",
                "--enable_ums",
                "--jemalloc_prof_conf",
                conf,
                "-f",
                "/tmp/worker.json",
            ],
        )


if __name__ == "__main__":
    unittest.main()
