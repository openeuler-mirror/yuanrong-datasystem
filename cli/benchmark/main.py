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

import argparse
import sys
from importlib import import_module

from yr.datasystem.cli.benchmark.executor import executor

COMMAND_REGISTRY = {
    "kv": "yr.datasystem.cli.benchmark.kv.command.KVCommand",
}


def main():
    """Entry point for the application."""
    if sys.version_info < (3, 9):
        sys.exit(1)

    try:
        parser = create_main_parser()
        args = parser.parse_args()
        command_name = getattr(args, "command", None)

        if not command_name or command_name not in COMMAND_REGISTRY:
            parser.print_help(file=sys.stderr)
            sys.exit(1)
    except Exception:
        import traceback

        traceback.print_exc()
        sys.exit(1)

    ssh_config_path = getattr(args, "ssh_config", None)
    if ssh_config_path:
        try:
            executor.load_ssh_config(ssh_config_path)
        except Exception as e:
            print(
                f"CRITICAL ERROR: Failed to load SSH configuration: {e}",
                file=sys.stderr,
            )
            sys.exit(1)

    try:
        full_class_path = COMMAND_REGISTRY[command_name]
        module_path, class_name = full_class_path.rsplit(".", 1)
        command_module = import_module(module_path)
        command_class = getattr(command_module, class_name)
        command_instance = command_class()
        exit_code = command_instance.run(args)
        sys.exit(exit_code)
    except ImportError:
        sys.exit(1)
    except Exception:
        import traceback

        traceback.print_exc()
        sys.exit(1)


def create_main_parser() -> argparse.ArgumentParser:
    """
    Create the main argument parser, and dynamically add a subparser for each command in the registry.
    """
    parser = argparse.ArgumentParser(
        prog="dsbench",
        description="YuanRong datasystem benchmark tool",
        allow_abbrev=False,
    )

    parser.add_argument(
        "--min_log_level",
        type=int,
        default=2,
        help="Set the min log level, INFO: 0, WARNING: 1, ERROR: 2 (default: 2).",
    )

    parser.add_argument(
        "--log_monitor_enable",
        type=bool,
        default=False,
        help="Enable log monitor (default: false).",
    )

    subparsers = parser.add_subparsers(
        dest="command",
        title="Available Commands",
        description="Please specify a subcommand for testing, e.g., `dsbench kv -h`.",
        required=True,
    )

    for cmd_name, class_path in COMMAND_REGISTRY.items():
        _add_subparser_for_command(cmd_name, class_path, subparsers)

    return parser


def _add_subparser_for_command(cmd_name: str, class_path: str, subparsers):
    """
    Handles the logic of importing a command class, instantiating it,
    and adding its corresponding subparser and arguments.
    """
    module_path, class_name = class_path.rsplit(".", 1)
    try:
        command_module = import_module(module_path)
        command_class = getattr(command_module, class_name)

        # Use a single instance to get help and add arguments
        command_instance = command_class()

        # Create the subparser for this command
        cmd_parser = subparsers.add_parser(
            cmd_name,
            help=command_instance.description,
            description=command_instance.description,
            allow_abbrev=False,
        )
        # Add the command-specific arguments
        command_instance.add_arguments(cmd_parser)

    except ImportError:
        print(
            f"\nCRITICAL ERROR: Failed to import module '{module_path}' for command '{cmd_name}'.",
            file=sys.stderr,
        )
        import traceback

        traceback.print_exc(file=sys.stderr)
    except AttributeError:
        print(
            f"\nCRITICAL ERROR: Class or attribute '{class_name}' not found in module '{module_path}'.",
            file=sys.stderr,
        )
    except Exception:
        print(
            f"\nCRITICAL ERROR: An unexpected error occurred while processing command '{cmd_name}'.",
            file=sys.stderr,
        )
        import traceback

        traceback.print_exc(file=sys.stderr)
