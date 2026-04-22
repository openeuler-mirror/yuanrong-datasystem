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
import csv
import json
import logging
import os
import re
from typing import Any

from yr.datasystem.cli.benchmark.common import (
    BenchSuite,
    BenchTestCase,
)
from yr.datasystem.cli.benchmark.kv.bench_test_case import (
    KVArgs,
    KVBenchOutputHandler,
    KVBenchTestCase,
    KVMode,
)
from yr.datasystem.cli.benchmark.task import BenchArgs

logger = logging.getLogger("dsbench")

_SIZE_PATTERN = r"^\d+(?:B|KB|MB|GB)?$"
_NUM_PATTERN = r"^[1-9]\d*$"


def get_kv_mode(args: Any) -> KVMode:
    """Determine the execution mode based on provided arguments."""
    if args.all:
        return KVMode.FULL
    elif args.testcase_file:
        return KVMode.CUSTOMIZED
    else:
        return KVMode.SINGLE


def validate_kv_args_config(tc_config: dict):
    """
    Validates the configuration for KVArgs from a test case.
    Checks for the presence and format/validity of 'num', 'size', 'client_num', 'thread_num', and 'batch_num'.

    Args:
        tc_config: The dictionary representation of a single test case.

    Returns:
        bool: True if the configuration is valid, False otherwise.
    """
    # 1. Check for required parameters
    required_params = ["num", "size", "client_num", "thread_num", "batch_num"]
    for param in required_params:
        if param not in tc_config:
            logger.error(
                f"Missing required parameter '{param}' in test case configuration: {tc_config}"
            )
            return False

    # 2. Validate formats
    num_val = tc_config.get("num")
    if not re.match(_NUM_PATTERN, str(num_val)):
        logger.error(
            f"Parameter 'num' ('{num_val}') must be a positive integer. Config: {tc_config}"
        )
        return False

    size_val = tc_config.get("size")
    if not re.match(_SIZE_PATTERN, str(size_val).upper()):
        logger.error(
            f"Parameter 'size' ('{size_val}') format is incorrect. Expected nB/nKB/nMB/nGB. Config: {tc_config}"
        )
        return False

    client_val = tc_config.get("client_num")
    if not re.match(_NUM_PATTERN, str(client_val)):
        logger.error(
            f"Parameter 'client_num' ('{client_val}') must be a positive integer. Config: {tc_config}"
        )
        return False

    thread_val = tc_config.get("thread_num")
    if not re.match(_NUM_PATTERN, str(thread_val)):
        logger.error(
            f"Parameter 'thread_num' ('{thread_val}') must be a positive integer. Config: {tc_config}"
        )
        return False

    batch_val = tc_config.get("batch_num")
    if not re.match(_NUM_PATTERN, str(batch_val)):
        logger.error(
            f"Parameter 'batch_num' ('{batch_val}') must be a positive integer. Config: {tc_config}"
        )
        return False

    # 3. Validate ranges
    try:
        client_num_int = int(client_val)
        thread_num_int = int(thread_val)
        batch_num_int = int(batch_val)
        num_int = int(num_val)

        if not (1 <= client_num_int <= 128):
            logger.error(
                f"Parameter 'client_num' value '{client_val}' must be between 1 and 128. Config: {tc_config}"
            )
            return False

        if not (1 <= thread_num_int <= 128):
            logger.error(
                f"Parameter 'thread_num' value '{thread_val}' must be between 1 and 128. Config: {tc_config}"
            )
            return False

        if client_num_int * thread_num_int > 128:
            logger.error(
                "Parameter 'client_num * thread_num' must be between 1 and 128. "
                f"Config: {tc_config}"
            )
            return False

        if not (1 <= batch_num_int <= 10000):
            logger.error(
                f"Parameter 'batch_num' value '{batch_val}' must be between 1 and 10000. Config: {tc_config}"
            )
            return False

        if num_int <= 0:
            logger.error(
                f"Parameter 'num' value '{num_val}' must be greater than 0. Config: {tc_config}"
            )
            return False
    except ValueError:
        # This path should ideally not be reached due to previous regex checks
        logger.error(
            f"Numeric parameter validation failed. Possible internal error. Config: {tc_config}"
        )
        return False

    return True


class KVBenchSuite(BenchSuite):
    def run(self):
        logger.info(f"Running test cases...")
        logger.info("=" * 175)

        # Print header only if there are test cases
        if self.testcases:
            # Use the first test case's handler to print the header
            first_handler = self.testcases[0].handler
            first_handler.print_table_header()

        for testcase in self.testcases:
            testcase.run()
        logger.info("=" * 175)


class KVBenchSuiteBuilder:
    bench_args: BenchArgs
    testcases: list[KVArgs]
    final_csv_filepath: str

    def __init__(self, bench_args: BenchArgs) -> None:
        self.bench_args = bench_args
        self.final_csv_filepath = None
        args = bench_args.args
        mode = get_kv_mode(args)
        self.testcases = []
        if mode == KVMode.SINGLE:
            kv_args = KVArgs(
                num=args.num if args.num is not None else 100,
                size=args.size if args.size is not None else "1MB",
                client_num=args.client_num if args.client_num is not None else 8,
                thread_num=args.thread_num if args.thread_num is not None else 1,
                batch_num=args.batch_num if args.batch_num is not None else 1,
            )
            self.testcases.append(kv_args)
            return

        testcase_file_path: str
        if mode == KVMode.FULL:
            current_dir = os.path.dirname(os.path.abspath(__file__))
            testcase_file_path = os.path.join(current_dir, "kv.json")
        else:
            testcase_file_path = args.testcase_file

        testcases = self.load_json(testcase_file_path)
        for tc in testcases:
            if not validate_kv_args_config(tc):
                raise ValueError(f"Invalid testcase configuration: {tc}")
            kv_args = KVArgs(
                num=int(tc["num"]),
                size=tc["size"],
                client_num=int(tc["client_num"]),
                thread_num=int(tc["thread_num"]),
                batch_num=int(tc["batch_num"]),
            )
            self.testcases.append(kv_args)

    def load_json(self, file_path: str) -> Any:
        try:
            with open(file_path, "r") as f:
                return json.load(f)
        except FileNotFoundError as e:
            logger.error(f"kv.json template not found at {file_path}")
            raise e
        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON format in {file_path}")
            raise e

    def build(self) -> BenchSuite:
        suite = KVBenchSuite()

        # Initialize CSV file directly in the builder
        self._initialize_csv_file()

        for i, kv_args in enumerate(self.testcases, start=1):
            testcase = self.create_testcase_from_args(kv_args, testcase_index=i)
            suite.add_testcase(testcase)

        return suite

    def _initialize_csv_file(self):
        """Initialize the CSV file for storing benchmark results."""
        try:
            log_dir = self.bench_args.log_dir
            if not log_dir:
                raise ValueError("bench_args.log_dir is not set or is empty.")

            file_name = self.bench_args.result_csv_file
            self.final_csv_filepath = os.path.join(log_dir, file_name)

            # Get the CSV headers directly from the class attribute
            headers = KVBenchOutputHandler.screen_headers

            with open(
                self.final_csv_filepath, "w", newline="", encoding="utf-8"
            ) as csvfile:
                writer = csv.writer(csvfile)
                writer.writerow(headers)

        except IOError as e:
            logger.error(f"Failed to create or write to CSV file: {e}")
            logger.error(
                f"Details - Filename: {self.bench_args.result_csv_file}, "
                f"Target Path: {self.final_csv_filepath}, "
                f"Directory: {self.bench_args.log_dir}"
            )
            self.final_csv_filepath = None
        except ValueError as e:
            logger.error(f"Configuration Error: {e}")
            self.final_csv_filepath = None
        except Exception as e:
            logger.error(f"An unknown error occurred while creating the CSV file: {e}")
            self.final_csv_filepath = None

    def create_testcase_from_args(
        self, kv_args: KVArgs, testcase_index: int
    ) -> BenchTestCase:
        args = self.bench_args.args
        name = (
            f"{args.prefix}_n{kv_args.num}_{kv_args.size}"
            f"_c{kv_args.client_num}_t{kv_args.thread_num}_b{kv_args.batch_num}"
        )
        if args.concurrent:
            name = f"{name}_concurrent"

        # Create a separate handler for each test case
        testcase_handler = KVBenchOutputHandler(self.bench_args, testcase_index, kv_args)

        # Share the CSV file path from the builder to avoid overwriting
        if self.final_csv_filepath:
            testcase_handler.final_csv_filepath = self.final_csv_filepath

        testcase = KVBenchTestCase(name, self.bench_args, testcase_handler, testcase_index)
        if args.concurrent:
            testcase.add_prefill_task(kv_args)
            testcase.add_concurrent_task(kv_args)
        else:
            testcase.add_set_task(kv_args)
            testcase.add_get_task(kv_args)
        testcase.add_del_task(kv_args)
        return testcase
