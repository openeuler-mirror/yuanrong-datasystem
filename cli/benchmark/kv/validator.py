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

# Common regex patterns
# IPv4:PORT pattern
IPV4_PORT_PATTERN = (
    r"^((25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?):"
    r"([1-9]\d{0,3}|[1-5]\d{4}|6[0-4]\d{3}|65[0-4]\d{2}|655[0-2]\d|6553[0-5])$"
)
# IPv6:PORT pattern (with brackets)
IPV6_PORT_PATTERN = r"^\[([0-9a-fA-F:.]+)\]:([1-9]\d{0,3}|[1-5]\d{4}|6[0-4]\d{3}|65[0-4]\d{2}|655[0-2]\d|6553[0-5])$"
# Combined IP:PORT pattern (supports both IPv4 and IPv6)
IP_PORT_PATTERN = f"({IPV4_PORT_PATTERN})|({IPV6_PORT_PATTERN})"


def check_duplicate_args(args: Any):
    """Ensure that unique command-line arguments are not specified multiple times."""
    raw_args = sys.argv[1:]
    unique_params = [
        "-c",
        "--client_num",
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
        "--concurrent",
        "-w",
        "--worker_address_list",
    ]

    for param in unique_params:
        count = raw_args.count(param)
        if count > 1:
            logger.error(
                f"argument '{param}' can only be specified once."
            )
            return False
    return True


def validate_range_arguments(args: Any):
    """Validates the range of numerical command-line arguments."""
    if args.client_num is not None and not 1 <= args.client_num <= 128:
        logger.error(f"client_num must be between 1 and 128.")
        return False

    if args.thread_num is not None and not 1 <= args.thread_num <= 128:
        logger.error(f"thread_num must be between 1 and 128.")
        return False

    if (
        args.client_num is not None
        and args.thread_num is not None
        and args.client_num * args.thread_num > 128
    ):
        logger.error(f"client_num * thread_num must be between 1 and 128.")
        return False

    if args.batch_num is not None and not 1 <= args.batch_num <= 10000:
        logger.error(f"batch_num must be between 1 and 10000.")
        return False

    if args.num is not None and args.num <= 0:
        logger.error(f"num must be greater than 0.")
        return False

    return True


def validate_format_arguments(args: Any):
    """Validates the format of various command-line arguments."""
    # Validate size format and value
    if args.size is not None:
        size_pattern = r"^(\d+)(B|KB|MB|GB)?$"
        size_match = re.match(size_pattern, args.size.upper())
        if not size_match:
            logger.error(
                f"size format is incorrect, should be nB/nKB/nMB/nGB or just n."
            )
            return False

        # Ensure size value is greater than 0
        size_value = int(size_match.group(1))
        if size_value <= 0:
            logger.error(f"size value must be greater than 0.")
            return False

    # Validate prefix format and length
    if not re.match(r"^[a-zA-Z0-9_]+$", args.prefix):
        logger.error(
            f"prefix must be letters, digits, or underscores."
        )
        return False

    # Ensure prefix length is between 1 and 64 characters
    if not 1 <= len(args.prefix) <= 64:
        logger.error(f"prefix length must be between 1 and 64 characters.")
        return False

    # Validate worker addresses format
    worker_address_pattern = r"^([a-zA-Z0-9.]+:\d+)(,[a-zA-Z0-9.]+:\d+)*$"
    if not re.match(worker_address_pattern, args.set_worker_addresses):
        logger.error(f"set_worker_addresses format is incorrect.")
        return False
    if not re.match(worker_address_pattern, args.get_worker_addresses):
        logger.error(f"get_worker_addresses format is incorrect.")
        return False

    # Validate owner_worker format
    if args.owner_worker and not re.match(IP_PORT_PATTERN, args.owner_worker):
        logger.error(f"owner_worker format error. Use IP:PORT.")
        return False

    # Validate numa format
    if args.numa and not re.match(r"^(\d+(-\d+)?)(,(\d+(-\d+)?))*$", args.numa):
        logger.error(f"numa format is incorrect.")
        return False

    return True


def validate_file_arguments(args: Any):
    """Validates if testcase_file path and its format are correct."""
    if args.testcase_file:
        if not os.path.exists(args.testcase_file):
            logger.error(f"testcase_file does not exist.")
            return False
        if not args.testcase_file.endswith(".json"):
            logger.error(f"testcase_file must be a .json file.")
            return False

    return True


def validate_mutex_arguments(args: Any):
    """Ensures that mutually exclusive command-line arguments are not used together."""
    if args.all and args.testcase_file:
        logger.error(
            f"Cannot specify both --all and --testcase_file."
        )
        return False

    # Define a set of arguments that conflict with --all and --testcase_file
    conflicting_run_args = {"client_num", "thread_num", "batch_num", "num", "size"}

    # Helper function to check if any argument in the set is set
    def _is_any_conflicting_arg_set():
        return any(
            _is_argument_set(args, arg_name) for arg_name in conflicting_run_args
        )

    # Use the helper function in simplified if conditions
    if args.all and _is_any_conflicting_arg_set():
        logger.error(
            f"When using --all, cannot specify client_num, thread_num, batch_num, num, or size."
        )
        return False

    if args.testcase_file and _is_any_conflicting_arg_set():
        logger.error(
            f"When using --testcase_file, cannot specify client_num, thread_num, batch_num, num, or size."
        )
        return False

    return True


def _is_argument_set(args, arg_name):
    """Check if an argument was explicitly set by the user, not using its default."""
    return args.__dict__[arg_name] is not None
