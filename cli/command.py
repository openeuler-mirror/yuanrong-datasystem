# Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
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
"""YuanRong datasystem CLI command module."""

import argparse
import logging
import os
import stat
import sys
from importlib import import_module

import importlib.resources as resources
import yr.datasystem.cli.common.util as util

from yr.datasystem.cli import __version__


class BaseCommand:
    """Base command class."""

    name = ""
    description = ""

    logger = None

    SUCCESS = 0
    FAILURE = 1

    def __init__(self):
        """Initialize of command"""
        if BaseCommand.logger is None:
            BaseCommand._configure_logging()
        self._base_dir = str(resources.files("yr.datasystem"))
        self._base_dir = self.valid_safe_path(self._base_dir)

    @staticmethod
    def valid_safe_path(path: str):
        """Validate the legality of the input path."""
        unsafe_dirs = ["/bin", "/sbin", "/lib", "/lib64", "/", "/boot", "/dev", "/etc", "/sys", "/proc"]
        norm_path = os.path.normpath(os.path.abspath(path))
        if norm_path in unsafe_dirs:
            raise ValueError(f"Path {path} is outside allowed directory")
        for parent in unsafe_dirs:
            if parent == "/":
                continue
            if norm_path.startswith(parent + os.sep):
                raise ValueError(f"Path {path} is outside allowed directory")
        return norm_path

    @staticmethod
    def add_arguments(parser):
        """
        Add arguments to parser.

        Args:
            parser (ArgumentParser): specify parser to which arguments are added.
        """

    @classmethod
    def _configure_logging(cls):
        """Configure logging format and handlers."""
        if cls.logger is not None:
            return

        cls.logger = logging.getLogger("dscli")
        formatter = logging.Formatter("[%(levelname)s] %(message)s")

        # Console handler (shows INFO and above)
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        console_handler.setFormatter(formatter)

        cls.logger.setLevel(logging.INFO)
        cls.logger.addHandler(console_handler)

    def run(self, args):
        """
        Implementation of command logic.

        Args:
            args (Namespace): parsed arguments to hold customized parameters.
        """
        raise NotImplementedError(
            "subclasses of BaseCommand must provide a run() method"
        )

    def invoke(self, args):
        """
        Invocation of command.

        Args:
            args (Namespace): parsed arguments to hold customized parameters.
        """
        return self.run(args)


def main():
    """Entry point for YuanRong datasystem CLI."""
    if (sys.version_info.major, sys.version_info.minor) < (3, 8):
        logging.error("Python version should be at least 3.8")
        sys.exit(1)

    # set umask to 0o077
    os.umask(stat.S_IRWXG | stat.S_IRWXO)

    version_str = f"{__version__}"
    commit_id = util.get_commit_id()
    if commit_id != "unknown":
        version_str += f" (commit: {commit_id})"

    parser = argparse.ArgumentParser(
        prog="dscli",
        description="YuanRong datasystem CLI entry point (version: {})".format(
            version_str
        ),
        allow_abbrev=False,
    )

    parser.add_argument(
        "--version", action="version", version="%(prog)s {}".format(version_str)
    )

    subparsers = parser.add_subparsers(
        dest="cli",
        title="subcommands",
        description="the following subcommands are supported",
    )

    commands = {}
    modules = [
        "start",
        "stop",
        "up",
        "down",
        "runscript",
        "generate_helm_chart",
        "generate_cpp_template",
        "generate_config",
        "collect_log",
    ]
    for m in modules:
        module = import_module(f"yr.datasystem.cli.{m}")
        command_cls = getattr(module, "Command", None)
        if command_cls is None or not issubclass(command_cls, BaseCommand):
            continue

        command = command_cls()
        command_parser = subparsers.add_parser(
            command.name, help=command.description, allow_abbrev=False
        )
        command.add_arguments(command_parser)
        commands[command.name] = command
    argv = sys.argv[1:]
    if not argv or argv[0] == "help":
        argv = ["-h"]
    args = parser.parse_args(argv)
    cli = args.__dict__.pop("cli")
    command = commands[cli]
    if command.invoke(args) != BaseCommand.SUCCESS:
        sys.exit(1)
