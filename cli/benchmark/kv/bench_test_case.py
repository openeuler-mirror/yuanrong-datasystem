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
import logging
import os
from dataclasses import dataclass
from enum import Enum
from typing import Any, Union

from yr.datasystem.cli.benchmark.common import BenchOutputHandler, BenchTestCase
from yr.datasystem.cli.benchmark.executor import executor
from yr.datasystem.cli.benchmark.task import (
    BenchArgs,
    BenchCommandTask,
    BenchRemoteInfo,
    BenchTask,
)

logger = logging.getLogger("dsbench")


class KVMode(Enum):
    SINGLE = 1
    CUSTOMIZED = 2
    FULL = 3


@dataclass
class KVArgs:
    num: int
    size: str
    thread_num: int
    batch_num: int


class KVBenchTestCase(BenchTestCase):
    program: str
    command: str

    def __init__(
        self,
        name: str,
        bench_args: BenchArgs,
        handler: BenchOutputHandler,
        index: int,
    ):
        super().__init__(name, bench_args, handler, index)
        self.program = "dsbench_cpp"
        self.command = "kv"

    def to_base_command_args(self, kv_args: KVArgs):
        """Builds a base dictionary of command-line arguments for the benchmark."""
        args = self.bench_args.args
        perf_path = f"{self.bench_args.log_dir}/{self.name}/perf.log"

        raw_workers = f"{args.set_worker_addresses},{args.get_worker_addresses}"
        unique_workers = (
            sorted(list(set(raw_workers.split(",")))) if raw_workers else []
        )
        perf_workers = ",".join(unique_workers)

        command_args = {
            "owner_worker": args.owner_worker,
            "prefix": args.prefix,
            "num": kv_args.num,
            "size": kv_args.size,
            "thread_num": kv_args.thread_num,
            "batch_num": kv_args.batch_num,
            "perf_path": perf_path,
            "perf_workers": perf_workers,
            "access_key": args.access_key,
            "secret_key": args.secret_key,
        }
        return command_args

    def generate_commands(self, command_args: dict[str, Any], bin_path: str) -> str:
        """Formats a full command string from arguments and binary path."""
        command_args_str = BenchCommandTask.concat_args("--", command_args)
        command = [bin_path, self.command, command_args_str]
        return " ".join(command)

    def generate_remote_info(self, worker_address: str) -> Union[BenchRemoteInfo, None]:
        """Fetches remote node configuration details from the executor."""
        return executor.get_remote_info(target_address=worker_address)

    def add_task_from_command_args(self, command_args: dict[str, Any]):
        """Creates and adds a benchmark task based on the provided command arguments."""
        worker_address = command_args.get("worker_address")
        if not worker_address:
            logger.error(
                "Error: 'worker_address' is missing in command_args, cannot determine target node location."
            )
            return

        env = self.generate_env()
        datasystem_location = executor.get_datasystem_pkg_location(worker_address)

        if not datasystem_location:
            raise RuntimeError(
                f"Required datasystem package not found on worker: {worker_address}. "
                "Benchmark task creation aborted."
            )

        lib_path = f"{datasystem_location}/yr/datasystem/lib"
        if "LD_LIBRARY_PATH" in env:
            env["LD_LIBRARY_PATH"] = f"{lib_path}:{env['LD_LIBRARY_PATH']}"
        else:
            env["LD_LIBRARY_PATH"] = lib_path

        executable_bin_path = f"{datasystem_location}/yr/datasystem/dsbench_cpp"

        command_str = self.generate_commands(command_args, bin_path=executable_bin_path)

        remote = self.generate_remote_info(worker_address)

        task = BenchCommandTask(command_str, env, remote, worker_address)
        self.add_task(task)

    def add_set_task(self, kv_args: KVArgs):
        """Adds 'set' tasks for all configured 'set' worker nodes."""
        for index, worker_address in enumerate(
            self.bench_args.args.set_worker_addresses.split(",")
        ):
            command_args = self.to_base_command_args(kv_args)
            command_args["action"] = "set"
            command_args["worker_address"] = worker_address
            command_args["worker_num"] = index
            self.add_task_from_command_args(command_args)

    def add_get_task(self, kv_args: KVArgs):
        """Adds 'get' tasks for all configured 'get' worker nodes."""
        for worker_address in self.bench_args.args.get_worker_addresses.split(","):
            command_args = self.to_base_command_args(kv_args)
            command_args["action"] = "get"
            command_args["worker_num"] = (
                len(self.bench_args.args.set_worker_addresses.split(",")) - 1
            )
            command_args["worker_address"] = worker_address
            self.add_task_from_command_args(command_args)

    def add_del_task(self, kv_args: KVArgs):
        """Adds 'del' tasks for all configured 'del' worker nodes."""
        for worker_address in self.bench_args.args.get_worker_addresses.split(","):
            command_args = self.to_base_command_args(kv_args)
            command_args["action"] = "del"
            command_args["worker_num"] = (
                len(self.bench_args.args.set_worker_addresses.split(",")) - 1
            )
            command_args["worker_address"] = worker_address
            self.add_task_from_command_args(command_args)
            # only run on the first worker from get_worker_addresses
            break

    def run(self):
        """Iterate through all tasks, execute each one, and then handle its output."""
        for task in self.tasks:
            task.run()
            self.handler.handle(task, self.index)


class KVBenchOutputHandler(BenchOutputHandler):
    bench_args: BenchArgs

    def __init__(self, bench_args: BenchArgs) -> None:
        super().__init__(bench_args)
        self.printed_header = False

        self.column_definitions = {
            "index": {"width": 5, "align": ">"},
            "action": {"width": 8, "align": ">"},
            "size": {"width": 10, "align": ">"},
            "count": {"width": 8, "align": ">"},
            "batch": {"width": 8, "align": ">"},
            "thread": {"width": 6, "align": ">"},
            "worker": {"width": 21, "align": ">"},
            "avg[ms]": {"width": 14, "align": ">"},
            "min[ms]": {"width": 13, "align": ">"},
            "p90[ms]": {"width": 14, "align": ">"},
            "p99[ms]": {"width": 14, "align": ">"},
            "max[ms]": {"width": 14, "align": ">"},
            "tps[count/sec]": {"width": 15, "align": ">"},
            "throughput[MB/sec]": {
                "width": 17,
                "align": ">",
            },
        }
        self.screen_headers = [
            col_key
            for col_key, _ in self.column_definitions.items()
        ]

        self.final_csv_filepath = None

    def set_test_suite_info(self):
        """Configures the handler with necessary information before the run begins."""
        self.printed_header = False
        try:
            log_dir = self.bench_args.log_dir
            if not log_dir:
                raise ValueError("bench_args.log_dir is not set or is empty.")

            file_name = self.bench_args.result_csv_file
            self.final_csv_filepath = os.path.join(log_dir, file_name)

            with open(
                self.final_csv_filepath, "w", newline="", encoding="utf-8"
            ) as csvfile:
                writer = csv.writer(csvfile)
                writer.writerow(self.screen_headers)

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

    def handle(self, task: BenchTask, testcase_index: int):
        """Processes a single benchmark task and formats its results for output."""
        if not isinstance(task, BenchCommandTask):
            raise TypeError(
                f"Internal Error: The handler requires an instance of BenchCommandTask, "
                f"but an instance of {type(task).__name__} was received. "
                f"This indicates a logic error in task creation or handling."
            )
        command_task: BenchCommandTask = task

        if not self.printed_header:
            self._format_table_line(line_type="header")
            self._format_table_line(line_type="separator")
            self.printed_header = True

        output_str = command_task.get_output().stdout.strip()
        if not output_str:
            return

        parsed_result = self._parse_benchmark_result(
            output_str, command_task.worker_address
        )
        if not parsed_result:
            return

        screen_data = [str(testcase_index)]
        for col_key, _ in self.column_definitions.items():
            if col_key != "index":
                screen_data.append(parsed_result.get(col_key, ""))

        self._format_table_line(screen_data, line_type="data")

        if self.final_csv_filepath:
            self._append_to_csv(screen_data)

    def _append_to_csv(self, csv_data: list):
        """Helper method to append data to the CSV file with error handling."""
        try:
            with open(
                self.final_csv_filepath, "a", newline="", encoding="utf-8"
            ) as csvfile:
                writer = csv.writer(csvfile)
                writer.writerow(csv_data)
        except IOError as e:
            logger.error(f"Failed to write to CSV file: {e}")

    def _parse_benchmark_result(self, line_str, worker_host):
        """Parse a single benchmark result line from dsbench_cpp output."""
        if not line_str.strip():
            return None

        parts = line_str.strip().split(",")
        if len(parts) != 8:
            return None

        metadata_str = parts[0]
        if metadata_str.startswith("BENCHMARK-RESULT:"):
            metadata_str = metadata_str[len("BENCHMARK-RESULT:"):]
        metrics = [p.strip() for p in parts[1:]]

        try:
            meta_parts = metadata_str.split("-")
            if len(meta_parts) < 5:
                logger.error(
                    f"Failed to parse metadata: {metadata_str} from line: {line_str}"
                )
                return None

            parsed_meta = {
                "action": "-".join(meta_parts[:-4]).strip(),
                "thread": meta_parts[-4].strip(),
                "num": meta_parts[-3].strip(),
                "size": meta_parts[-2].strip(),
                "batch": meta_parts[-1].strip(),
            }
        except Exception as e:
            logger.error(f"Error occurred while parsing metadata '{metadata_str}': {e}")
            return None

        result_data = self._format_benchmark_values(metrics)
        formatted_values = result_data[1] if result_data[0] else metrics

        return self._build_final_result_dict(parsed_meta, worker_host, formatted_values)

    def _build_final_result_dict(self, parsed_meta, worker_host, values_source):
        """Helper method to build the final result dictionary from a common structure."""
        if isinstance(values_source, list):
            def value_getter(i):
                return values_source[i]
        else:
            def value_getter(k):
                return values_source.get(f"{k}_str")

        return {
            "action": parsed_meta["action"],
            "size": parsed_meta["size"],
            "count": parsed_meta["num"],
            "batch": parsed_meta["batch"],
            "thread": parsed_meta["thread"],
            "worker": worker_host,
            "avg[ms]": (
                value_getter("avg")
                if isinstance(values_source, dict)
                else value_getter(0)
            ),
            "min[ms]": (
                value_getter("min")
                if isinstance(values_source, dict)
                else value_getter(1)
            ),
            "p90[ms]": (
                value_getter("p90")
                if isinstance(values_source, dict)
                else value_getter(2)
            ),
            "p99[ms]": (
                value_getter("p99")
                if isinstance(values_source, dict)
                else value_getter(3)
            ),
            "max[ms]": (
                value_getter("max")
                if isinstance(values_source, dict)
                else value_getter(4)
            ),
            "tps[count/sec]": (
                value_getter("tps")
                if isinstance(values_source, dict)
                else value_getter(5)
            ),
            "throughput[MB/sec]": (
                value_getter("throughput")
                if isinstance(values_source, dict)
                else value_getter(6)
            ),
        }

    def _format_benchmark_values(
        self, value_list: list[str]
    ) -> tuple[bool, dict | list]:
        """
        Tries to format a list of benchmark value strings into a dictionary
        with proper types. Returns a tuple: (success_flag, result_dict_or_raw_list).
        Logs the warning on failure.
        """
        try:
            formatted_values = {
                "avg_str": f"{float(value_list[0]):.3f}",
                "min_val_str": f"{float(value_list[1]):.3f}",
                "p90_str": f"{float(value_list[2]):.3f}",
                "p99_str": f"{float(value_list[3]):.3f}",
                "max_val_str": f"{float(value_list[4]):.3f}",
                "tps_str": f"{int(round(float(value_list[5])))}",
                "throughput_str": f"{float(value_list[6]):.1f}",
            }
            return True, formatted_values
        except (ValueError, TypeError) as e:
            logger.warning(
                f"Failed to format numerical values. Error: {e}. Using original values."
            )
            return False, value_list

    def _format_table_line(self, items=None, line_type="data"):
        """Internal helper to format a table row based on its type."""
        formatted_cells = []

        if line_type in ("header", "separator"):
            for col_key, col_def in self.column_definitions.items():
                title = col_key
                width = col_def["width"]
                if line_type == "separator":
                    formatted_cells.append(f"{'-' * width}")
                else:
                    formatted_cells.append(f"{title:^{width}}")
        elif items:
            data_items = iter(items)

            for col_def in self.column_definitions.values():
                cell_content = next(data_items)
                width = col_def["width"]

                if self._is_right_aligned_cell(col_def, cell_content):
                    alignment = ">"
                else:
                    alignment = col_def["align"]

                formatted_cells.append(f"{str(cell_content):{alignment}{width}}")

        logger.info(" ".join(formatted_cells))

    def _is_right_aligned_cell(self, col_def, cell_content):
        """
        Determines if a table cell should be right-aligned.
        Right alignment is used for 'action' column or for numeric/certain unit columns.
        """
        # Check if the column is already configured for right alignment
        if col_def["align"] != ">":
            return False

        # Convert content to string once for further checks
        content_str = str(cell_content)

        # Check for numeric content
        if any(char.isdigit() for char in content_str):
            return True

        # Check for common performance/unit suffixes
        if content_str.endswith(("ms", "sec", "B", "count/sec")):
            return True
        return False
