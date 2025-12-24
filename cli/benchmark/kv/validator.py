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
import logging
import os
import re
import sys
from typing import Any

logger = logging.getLogger("dsbench")


def check_duplicate_args(args: Any):
    """Ensure that unique command-line arguments are not specified multiple times."""
    raw_args = sys.argv[1:]
    unique_params = [
        "-t",
        "--thread_num",
        "-b",
        "--batch_num",
        "-n",
        "--num",
        "-s",
        "--size",
        "-p",
        "--prefix",
        "-f",
        "--testcase_file",
        "--owner_worker",
        "--numa",
        "--access_key",
        "--secret_key",
        "-w",
        "--worker_address_list",
    ]

    for param in unique_params:
        count = raw_args.count(param)
        if count > 1:
            logger.error(
                f"argument '{param}' can only be specified once. Current args: {args}"
            )
            return False
    return True


def validate_range_arguments(args: Any):
    """Validates the range of numerical command-line arguments."""
    if not 1 <= args.thread_num <= 128:
        logger.error(f"thread_num must be between 1 and 128. Current args: {args}")
        return False

    if not 1 <= args.batch_num <= 10000:
        logger.error(f"batch_num must be between 1 and 10000. Current args: {args}")
        return False

    if args.num <= 0:
        logger.error(f"num must be greater than 0. Current args: {args}")
        return False

    return True


def validate_format_arguments(args: Any):
    """Validates the format of various command-line arguments."""
    size_pattern = r"^\d+(?:B|KB|MB|GB)?$"
    if not re.match(size_pattern, args.size.upper()):
        logger.error(
            f"size format is incorrect, should be nB/nKB/nMB/nGB or just n. Current args: {args}"
        )
        return False

    if not re.match(r"^[a-zA-Z0-9_]+$", args.prefix):
        logger.error(
            f"prefix must be letters, digits, or underscores. Current args: {args}"
        )
        return False

    worker_address_pattern = r"^([a-zA-Z0-9.]+:\d+)(,[a-zA-Z0-9.]+:\d+)*$"
    if not re.match(worker_address_pattern, args.set_worker_addresses):
        logger.error(f"set_worker_addresses format is incorrect. Current args: {args}")
        return False
    if not re.match(worker_address_pattern, args.get_worker_addresses):
        logger.error(f"get_worker_addresses format is incorrect. Current args: {args}")
        return False

    if args.owner_worker and not re.match(r"^[a-zA-Z0-9.:]+$|^\s*$", args.owner_worker):
        logger.error(f"owner_worker format is incorrect. Current args: {args}")
        return False

    if args.numa and not re.match(r"^(\d+(-\d+)?)(,(\d+(-\d+)?))*$", args.numa):
        logger.error(f"numa format is incorrect. Current args: {args}")
        return False

    return True


def validate_file_arguments(args: Any):
    """Validates if testcase_file path and its format are correct."""
    if args.testcase_file:
        if not os.path.exists(args.testcase_file):
            logger.error(f"testcase_file does not exist. Current args: {args}")
            return False
        if not args.testcase_file.endswith(".json"):
            logger.error(f"testcase_file must be a .json file. Current args: {args}")
            return False

    return True


def validate_mutex_arguments(args: Any):
    """Ensures that mutually exclusive command-line arguments are not used together."""
    if args.all and args.testcase_file:
        logger.error(
            f"Cannot specify both --all and --testcase_file. Current args: {args}"
        )
        return False

    # Define a set of arguments that conflict with --all and --testcase_file
    conflicting_run_args = {"thread_num", "batch_num", "num", "size"}

    # Helper function to check if any argument in the set is set
    def _is_any_conflicting_arg_set():
        return any(
            _is_argument_set(args, arg_name) for arg_name in conflicting_run_args
        )

    # Use the helper function in simplified if conditions
    if args.all and _is_any_conflicting_arg_set():
        logger.error(
            f"When using --all, cannot specify thread_num, batch_num, num, or size. Current args: {args}"
        )
        return False

    if args.testcase_file and _is_any_conflicting_arg_set():
        logger.error(
            f"When using --testcase_file, cannot specify thread_num, batch_num, num, or size. Current args: {args}"
        )
        return False

    return True


def _is_argument_set(args, arg_name):
    """Check if an argument was explicitly set by the user, not using its default."""
    default_values = {"thread_num": 8, "batch_num": 1, "num": 1, "size": "1MB"}
    if arg_name in default_values:
        return args.__dict__[arg_name] != default_values[arg_name]
    return args.__dict__[arg_name] is not None
