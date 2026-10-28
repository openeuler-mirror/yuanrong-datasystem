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
"""Tests for dscli Docker entryfile generation."""

import argparse
import importlib.util
import stat
import sys
import tempfile
import types
import unittest
from pathlib import Path
from types import SimpleNamespace


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

ENTRYFILE_PY = Path(__file__).resolve().parents[2] / "cli" / "generate_docker_entryfile.py"
spec = importlib.util.spec_from_file_location("datasystem_cli_generate_docker_entryfile_under_test", ENTRYFILE_PY)
assert spec is not None
assert spec.loader is not None
generate_docker_entryfile = importlib.util.module_from_spec(spec)
spec.loader.exec_module(generate_docker_entryfile)


class CliGenerateDockerEntryfileTest(unittest.TestCase):
    def setUp(self):
        self.command = generate_docker_entryfile.Command.__new__(generate_docker_entryfile.Command)
        self.command.logger = types.SimpleNamespace(
            info=lambda *args, **kwargs: None,
            warning=lambda *args, **kwargs: None,
            error=lambda *args, **kwargs: None,
        )

    def test_mode_argument_is_not_available(self):
        parser = argparse.ArgumentParser(allow_abbrev=False)
        self.command.add_arguments(parser)

        with self.assertRaises(SystemExit):
            parser.parse_args(["-m", "deployment"])

    def test_generation_uses_daemonset_entryfiles(self):
        temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(temp_dir.cleanup)
        base_dir = Path(temp_dir.name)
        source_dir = base_dir / "docker_entryfile" / "daemonset"
        source_dir.mkdir(parents=True)
        (source_dir / "worker_entry.sh").write_text("# daemonset entry\n")
        output_dir = base_dir / "output"

        self.command._base_dir = str(base_dir)
        result = self.command.run(SimpleNamespace(output_path=str(output_dir)))

        output = output_dir / "worker_entry.sh"
        self.assertEqual(result, self.command.SUCCESS)
        self.assertEqual(output.read_text(), "# daemonset entry\n")
        self.assertEqual(output.stat().st_mode & 0o777, stat.S_IRUSR | stat.S_IXUSR)


if __name__ == "__main__":
    unittest.main()
