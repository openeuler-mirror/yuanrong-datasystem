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
from __future__ import annotations

import datetime
import logging
import os
from abc import ABC, abstractmethod
from argparse import Namespace
from typing import Any

from yr.datasystem.cli.benchmark.task import BenchArgs, BenchTask


class BenchOutputHandler(ABC):
    bench_args: BenchArgs

    def __init__(self, bench_args: BenchArgs) -> None:
        super().__init__()
        self.bench_args = bench_args

    @abstractmethod
    def handle(self, task: BenchTask) -> None:
        """Processes a single benchmark task and handles its output."""
        pass


class BenchTestCase:
    name: str
    tasks: list[BenchTask]
    bench_args: BenchArgs
    handler: BenchOutputHandler
    index: int

    def __init__(self, name: str, bench_args: BenchArgs, handler: BenchOutputHandler, index: int = 0):
        self.name = name
        self.bench_args = bench_args
        self.tasks = []
        self.handler = handler
        self.index = index

    def add_task(self, task: BenchTask):
        """Add a new task to the list of tasks to be executed."""
        self.tasks.append(task)

    def generate_env(self):
        """Generates the environment variables for the benchmark task."""
        return {
            "DATASYSTEM_CLIENT_LOG_DIR": f"{self.bench_args.log_dir}/{self.name}",
            "DATASYSTEM_MIN_LOG_LEVEL": str(self.bench_args.args.min_log_level),
            "DATASYSTEM_LOG_MONITOR_ENABLE": str(
                self.bench_args.args.log_monitor_enable
            ).lower(),
        }

    def run(self):
        """Iterate through all tasks, execute each one, and then handle its output."""
        for task in self.tasks:
            task.run()
            self.handler.handle(task)


class BenchSuite:
    testcases: list[BenchTestCase]

    def __init__(self):
        self.testcases = []

    def add_testcase(self, testcase: BenchTestCase):
        """Add the new test case to the list of test cases to be executed."""
        self.testcases.append(testcase)

    def run(self):
        """Execute the run method for each test case in the suite."""
        for testcase in self.testcases:
            testcase.run()


class BaseCommand(ABC):
    SUCCESS = 0
    FAILURE = 1
    logger = None

    def __init__(self):
        """Initialize of command"""
        if BaseCommand.logger is None:
            BaseCommand._configure_logging()

    @staticmethod
    def _configure_logging():
        """Configure logging format and handlers."""
        if BaseCommand.logger is not None:
            return

        BaseCommand.logger = logging.getLogger("dsbench")
        formatter = logging.Formatter("%(message)s")

        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        console_handler.setFormatter(formatter)

        BaseCommand.logger.setLevel(logging.DEBUG)
        BaseCommand.logger.addHandler(console_handler)

    @staticmethod
    def _configure_logging_with_file(log_path):
        if BaseCommand.logger is None:
            BaseCommand._configure_logging()

        file_formatter = logging.Formatter("[%(asctime)s] [%(levelname)s] %(message)s")

        file_handler = logging.FileHandler(log_path)
        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(file_formatter)

        BaseCommand.logger.addHandler(file_handler)

    @abstractmethod
    def validate(self, args: Any) -> bool:
        """Validate command-line arguments. Return False if invalid."""
        pass

    @abstractmethod
    def initialize(self, args: Any) -> bool:
        """Initializes the benchmark runner with the provided arguments."""
        pass

    @abstractmethod
    def pre_run(self) -> bool:
        """Performs pre-execution checks and setup tasks."""
        pass

    @abstractmethod
    def build_suite(self, bench_args: BenchArgs) -> BenchSuite:
        """Constructs and returns a benchmark suite object."""
        pass

    def generate_bench_args(self, args: Namespace) -> BenchArgs:
        """Creates and returns BenchArgs from parsed command-line arguments."""
        name = f"bench_{args.command}"
        start_time = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%d%H%M%S")
        return BenchArgs(
            name=name,
            start_time=start_time,
            cwd=os.getcwd(),
            log_dir=f"result_{name}_{start_time}",
            result_csv_file=f"bench_result_{name}_{start_time}.csv",
            args=args,
        )

    def run(self, args: Any) -> int:
        """Executes the complete benchmark lifecycle from setup to tear-down."""
        try:

            bench_args = self.generate_bench_args(args)
            os.makedirs(bench_args.log_dir, exist_ok=True)
            log_file_name = "run.log"
            full_log_file_path = os.path.join(bench_args.log_dir, log_file_name)
            BaseCommand._configure_logging_with_file(full_log_file_path)

            if not self.validate(args):
                return self.FAILURE

            if not self.initialize(args):
                return self.FAILURE

            if not self.pre_run():
                return self.FAILURE

            suite = self.build_suite(bench_args)
            suite.run()

            return self.SUCCESS

        except Exception as e:
            self.logger.critical(
                f"A critical unhandled exception occurred during command execution: {e}"
            )
            return self.FAILURE
