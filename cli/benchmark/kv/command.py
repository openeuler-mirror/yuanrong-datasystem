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
import re
from typing import Any, Union

from yr.datasystem.cli.benchmark.common import (
    BaseCommand,
    BenchOutputHandler,
    BenchSuite,
)
from yr.datasystem.cli.benchmark.executor import executor
from yr.datasystem.cli.benchmark.kv import validator
from yr.datasystem.cli.benchmark.kv.bench_suite_builder import KVBenchSuiteBuilder
from yr.datasystem.cli.benchmark.kv.bench_test_case import KVBenchOutputHandler
from yr.datasystem.cli.benchmark.task import BenchArgs, BenchCommandOutput


class KVCommand(BaseCommand):
    name = "kv"
    description = "KV Performance Benchmarking"
    handler: Union[BenchOutputHandler, None]

    def __init__(self):
        """
        Initializes the Command instance.
        """
        super().__init__()
        self.handler = None
        self.mode = None
        self.args = None

    def add_arguments(self, parser: argparse.ArgumentParser):
        """
        Adds command-specific arguments to the parser.
        Arguments are grouped by their logical purpose (test, run, cluster).
        """
        self._add_test_config_arguments(parser)
        self._add_run_config_arguments(parser)
        self._add_cluster_config_arguments(parser)

    def validate(self, args: Any) -> bool:
        """Validate all provided command-line arguments."""
        if not validator.check_duplicate_args(args):
            return False
        if not validator.validate_range_arguments(args):
            return False
        if not validator.validate_format_arguments(args):
            return False
        if not validator.validate_file_arguments(args):
            return False
        if not validator.validate_mutex_arguments(args):
            return False
        return True

    def initialize(self, args: Any) -> bool:
        """Initializes the command with parsed arguments."""
        self.args = args

        return True

    def pre_run(self) -> bool:
        """Logs hardware and software configuration summary before running tests."""

        header_start = "=" * 30
        header_text = " Print Hardware & Software Configuration Summary "
        header_end = "=" * 30

        full_header = f"{header_start}{header_text}{header_end}"
        self.logger.info(full_header)

        raw_workers = (
            f"{self.args.set_worker_addresses},{self.args.get_worker_addresses}"
        )
        all_worker_addresses = sorted(list(set(raw_workers.split(","))))

        if not all_worker_addresses:
            self.logger.info(
                "  * No worker addresses are configured. Configuration check completed."
            )
            return True

        for idx, worker_address in enumerate(all_worker_addresses, 1):
            self._log_system_info_for_node(worker_address)

            self._print_worker_params(
                worker_address,
                idx,
            )

            if idx < len(all_worker_addresses):
                self.logger.info("=" * 109)

        return True

    def build_suite(self, bench_args: BenchArgs) -> BenchSuite:
        """Builds a benchmark suite for KV tests."""
        self.handler = KVBenchOutputHandler(bench_args)
        builder = KVBenchSuiteBuilder(bench_args, self.handler)
        return builder.build()

    def _add_test_config_arguments(self, parser: argparse.ArgumentParser):
        """Adds arguments related to test suite and testcase file configuration."""
        parser.add_argument(
            "--testsuite-name",
            default="kv_run",
            help="Name for the test suite (default: kv_run).",
        )

        parser.add_argument(
            "-a",
            "--all",
            action="store_true",
            help="Run all test cases defined in kv.json.",
        )
        parser.add_argument(
            "-f",
            "--testcase_file",
            type=str,
            help="""
            Testcase file (must be .json).
            Format example:
            [
                {"num": 1000, "size": "1MB", "thread_num": 1, "batch_num": 1},
                {"num": 250, "size": "1MB", "thread_num": 4, "batch_num": 1},
                {"num": 125, "size": "1MB", "thread_num": 8, "batch_num": 1}
            ]
            """,
        )

    def _add_run_config_arguments(self, parser: argparse.ArgumentParser):
        """Adds arguments related to the benchmark runtime configuration."""
        parser.add_argument(
            "-t",
            "--thread_num",
            type=int,
            default=8,
            help="Number of threads to use (default: 8, max 128)",
        )
        parser.add_argument(
            "-b",
            "--batch_num",
            type=int,
            default=1,
            help="Batch number (default: 1, max 10000)",
        )
        parser.add_argument(
            "-n", "--num", type=int, default=1, help="Number of keys (default: 1)"
        )
        parser.add_argument(
            "-s",
            "--size",
            type=str,
            default="1MB",
            help="Data size of key (e.g. 2B/4KB/8MB/1GB or just 2 for 2B)",
        )

        parser.add_argument(
            "-p",
            "--prefix",
            type=str,
            default="Bench",
            help="Prefix of the key (default: Bench)",
        )
        parser.add_argument(
            "--owner_worker",
            type=str,
            default="",
            help="Owner worker of the object metadata (ip:port)",
        )
        parser.add_argument(
            "--numa",
            type=str,
            default="",
            help="Bind to specific numa (e.g. 0-3,10-20,1,2,3)",
        )

    def _add_cluster_config_arguments(self, parser: argparse.ArgumentParser):
        """Adds arguments related to cluster setup, authentication, and tools."""
        parser.add_argument(
            "-S",
            "--set_worker_addresses",
            required=True,
            help="Comma-separated list of worker addresses (e.g., ip1:port1,ip2:port2).",
        )
        parser.add_argument(
            "-G",
            "--get_worker_addresses",
            required=True,
            help="Comma-separated list of worker addresses (e.g., ip1:port1,ip2:port2).",
        )
        parser.add_argument("--access_key", type=str, default="", help="Access key")
        parser.add_argument("--secret_key", type=str, default="", help="Secret key")

        parser.add_argument(
            "--ssh_config",
            type=str,
            default=None,
            help="""
            Path to an optional SSH config JSON file for defining custom usernames
            and private key paths for specific hosts.

            Default behavior: Username 'root', IdentityFile '~/.ssh/id_rsa'.

            The config file should only contain a 'hosts' map. If a host is not
            found in the map, the built-in defaults will be used.

            Example file format:
            {
                "hosts": {
                    "127.0.0.1:31501": { "username": "deploy", "identity_file": "~/.ssh/deploy_key", ssh_port: "8822" }
                }
            }
            """,
        )
        parser.add_argument(
            "--dsbench_path",
            type=str,
            default="dsbench",
            help="Absolute path to the dsbench tool (e.g., /opt/bin/dsbench).",
        )

    def _log_system_info_for_node(self, worker_address: str):
        """Helper to log all system information for a given node (IP or localhost)."""
        self.logger.info(f"  * System Information for {worker_address}:")

        mem_result = executor.execute("free -h", worker_address)
        self._print_mem_info_table(result=mem_result, node_id=worker_address)
        cpu_raw_result = executor.execute("lscpu", worker_address)
        self._print_cpu_info_summary(result=cpu_raw_result)
        thp_result = executor.execute(
            "cat /sys/kernel/mm/transparent_hugepage/enabled", worker_address
        )
        self._print_tlp_status(result=thp_result, node_id=worker_address)
        huge_result = executor.execute("grep Huge /proc/meminfo", worker_address)
        self._print_hugepages_status(result=huge_result)

        self.logger.info(f"  * Software Version Information for {worker_address}:")
        self._log_sdk_version(worker_address)

        worker_result = executor.execute(
            "source ~/.bashrc && dscli --version", worker_address
        )
        self._log_worker_version(worker_result)

    def _log_sdk_version(self, worker_address: str):
        """Handles printing the Client SDK (dsbench_cpp) version information.

        Dynamically locates its path and ensures execution permissions."""
        # 1. Get the installation location of openyuanrong-datasystem
        datasystem_location = executor.get_datasystem_pkg_location(worker_address)

        if not datasystem_location:
            self.logger.error(
                f"      Error: Could not find the installation location for 'openyuanrong-datasystem' "
                f"on node {worker_address}. Cannot perform Client SDK version check."
            )
            return

        # 2. Set permissions for dsbench_cpp
        dsbench_cpp_executable = f"{datasystem_location}/yr/datasystem/dsbench_cpp"
        chmod_command = f"chmod +x {dsbench_cpp_executable}"
        chmod_result = executor.execute(chmod_command, worker_address, env=None)
        if isinstance(chmod_result, str):
            self.logger.error(
                f"      Error: Failed to set execute permissions for dsbench_cpp "
                f"on node {worker_address}. Reason: {chmod_result}"
            )
            self.logger.error(f"      Error: Skipping Client SDK version check.")
            return

        # 3. Prepare and execute the version check command
        command_to_run = f"{dsbench_cpp_executable} -v"
        ld_library_path = f"{datasystem_location}/yr/datasystem/lib"
        env = {"LD_LIBRARY_PATH": ld_library_path}

        sdk_result = executor.execute(command_to_run, worker_address, env)

        # 4. Delegate the result processing to a dedicated method
        self._process_sdk_version_result(sdk_result, worker_address)

    def _process_sdk_version_result(self, sdk_result: Any, worker_address: str):
        """Processes the SDK version check result and logs the outcome."""
        if isinstance(sdk_result, BenchCommandOutput):
            stdout = sdk_result.stdout.strip()
            stderr = sdk_result.stderr.strip()
            sdk_name = "Client SDK"

            if stdout:
                version = "unknown"
                commit = "unknown"

                version_match = re.search(r"Version:\s*(.+)", stdout)
                if version_match:
                    version = version_match.group(1).strip()

                commit_match = re.search(r"Git Commit:\s*\[?([a-f0-9]+)", stdout)
                if commit_match:
                    commit = commit_match.group(1).strip()

                self.logger.info(
                    f"      - Client SDK Version: {version} (commit: {commit})"
                )

                if stderr:
                    self.logger.warning(
                        f"      Warning: The '{sdk_name} version check' command "
                        f"produced standard error output: {stderr}"
                    )
            elif not stdout and not stderr:
                self.logger.warning(
                    f"      Warning: The '{sdk_name} version check' command "
                    f"did not return any standard output or error information."
                )

        elif isinstance(sdk_result, str):
            self.logger.error(
                f"      Error: A fatal error occurred while executing the 'Client SDK version check' "
                f"on node {worker_address}. Reason: {sdk_result}"
            )
        else:
            self.logger.error(
                f"      Error: Encountered an unexpected data type while getting the Client SDK version: "
                f"{type(sdk_result)}"
            )

    def _log_worker_version(self, result: Any):
        if isinstance(result, BenchCommandOutput):
            try:
                stdout = result.stdout.strip().decode("utf-8")
            except (AttributeError, UnicodeDecodeError):
                stdout = str(result.stdout).strip()

            if not stdout:
                self.logger.warning(
                    f"      Warning: The 'Worker version check' command did not return any standard output."
                )
                return
            try:
                version_str = stdout
                if version_str.startswith("dscli "):
                    version_str = version_str[len("dscli ") :]
                if version_str.endswith(")"):
                    version_str = version_str[:-1]
                parts = version_str.split(" (commit: ")
                if len(parts) == 2:
                    version = parts[0].strip()
                    commit_id = parts[1].strip()
                    self.logger.info(
                        f"      - Worker Version: {version} (commit: {commit_id})"
                    )
                else:
                    self.logger.warning(
                        f"    Worker Version: Output format is unexpected. Cannot parse from stdout. - {stdout}"
                    )
            except Exception as e:
                self.logger.error(f"    Failed to get worker version from stdout: {e}")

    def _print_mem_info_table(self, result: Any, node_id: str):
        """Refer to the logic in kv.py.
        Print a memory information table using fixed column widths and right-aligned formatting.
        """
        if isinstance(result, BenchCommandOutput):
            cmd_output = result
            stdout = cmd_output.stdout.strip()

            if stdout:
                lines = stdout.split("\n")
                if len(lines) > 2:
                    self._print_mem_table_header()
                    self._print_mem_data(lines[1], lines[2])
                    self._print_mem_table_footer()
                else:
                    self.logger.warning(
                        f"(Node {node_id}) Insufficient lines in memory info output."
                    )

            self._log_mem_command_warnings(cmd_output, node_id)

        elif isinstance(result, str):
            self.logger.error(f"(Node {node_id}) Failed to get memory info: {result}")
        else:
            self.logger.error(
                f"(Node {node_id}) Received unexpected result type for memory info: "
                f"{type(result)}. Content: {str(result)[:200]}"
            )

    def _print_mem_table_header(self):
        """Prints the header for the memory information table."""
        self.logger.info("    Memory Information:")
        self.logger.info(
            f"    +----------------+---------+---------+---------+----------+"
        )
        self.logger.info(
            f"    | Type           |  Total  |  Used   |  Free   | Available|"
        )
        self.logger.info(
            f"    +----------------+---------+---------+---------+----------+"
        )

    def _print_mem_table_footer(self):
        """Prints the footer for the memory information table."""
        self.logger.info(
            f"    +----------------+---------+---------+---------+----------+"
        )

    def _print_mem_data(self, mem_line: str, swap_line: str):
        """Parses and prints the data for memory and swap space."""
        # Parse and print memory data
        mem_parts = mem_line.split()
        total_mem = mem_parts[1] if 1 < len(mem_parts) else "N/A"
        used_mem = mem_parts[2] if 2 < len(mem_parts) else "N/A"
        free_mem = mem_parts[3] if 3 < len(mem_parts) else "N/A"
        avail_mem = mem_parts[6] if 6 < len(mem_parts) else "N/A"
        self.logger.info(
            f"    | Memory         | {total_mem:>6}  | {used_mem:>6}  | {free_mem:>6}  | {avail_mem:>6}   |"
        )

        # Parse and print swap data
        swap_parts = swap_line.split()
        total_swap = swap_parts[1] if 1 < len(swap_parts) else "N/A"
        used_swap = swap_parts[2] if 2 < len(swap_parts) else "N/A"
        free_swap = "N/A"
        self.logger.info(
            f"    | Swap           | {total_swap:>6}  | {used_swap:>6}  | {free_swap:>6}  | {'N/A':>6}   |"
        )

    def _log_mem_command_warnings(self, cmd_output: BenchCommandOutput, node_id: str):
        """Logs warnings related to the memory command execution."""
        if cmd_output.stderr.strip():
            self.logger.warning(
                f"(Node {node_id}) 'free -h' command produced stderr: {cmd_output.stderr.strip()}"
            )
        if not cmd_output.stdout.strip() and not cmd_output.stderr.strip():
            self.logger.warning(
                f"(Node {node_id}) Command 'free -h' produced no stdout or stderr."
            )

    def _print_cpu_info_summary(self, result: Any):
        """
        Formats and prints CPU information summary.
        This new version directly processes the full output of the lscpu command.
        """
        if isinstance(result, BenchCommandOutput):
            self._parse_and_print_cpu_details(result)
        elif isinstance(result, str):
            self.logger.error(f"    Failed to get CPU information: {result}")
        else:
            self.logger.error(
                f"    Encountered an unexpected data type while getting CPU information: {type(result)}"
            )

    def _parse_and_print_cpu_details(self, cmd_output: BenchCommandOutput):
        """Helper method to parse lscpu output and print CPU information."""
        lscpu_full_output = cmd_output.stdout.strip()

        if lscpu_full_output:
            self.logger.info("    CPU Information:")
            patterns = {
                "Model name": r"Model name:\s*([^\n]+)",
                "CPU(s)": r"CPU$s$:\s*([^\n]+)",
                "Thread(s) per core:": r"Thread$s$ per core:\s*([^\n]+)",
                "Core(s) per socket:": r"Core$s$ per socket:\s*([^\n]+)",
                "Socket(s)": r"Socket$s$:\s*([^\n]+)",
                "CPU max MHz": r"CPU max MHz:\s*([^\n]+)",
            }

            found_info = False
            for field_name, regex_pattern in patterns.items():
                match = re.search(regex_pattern, lscpu_full_output, re.MULTILINE)
                if match:
                    value = match.group(1).strip()
                    self.logger.info(f"      - {field_name}: {value}")
                    found_info = True

            if not found_info:
                self.logger.warning(
                    "      - No known CPU information could be parsed from the lscpu output."
                )
                self.logger.debug(
                    f"        [DEBUG] Raw lscpu output:\n{lscpu_full_output}"
                )

        if cmd_output.stderr.strip():
            self.logger.warning(
                f"      The 'lscpu' command produced standard error output when executed remotely:\n"
                f"{cmd_output.stderr.strip()}"
            )

        if not lscpu_full_output and not cmd_output.stderr.strip():
            self.logger.warning(
                "      - The 'lscpu' command did not return any standard output or standard error."
            )

    def _print_tlp_status(self, result: Any, node_id: str):
        """Formats and prints THP status."""
        if isinstance(result, BenchCommandOutput):
            self.logger.info(f"    Transparent HugePages: {result.stdout.strip()}")
            if result.stderr.strip():
                self.logger.warning(
                    f"(Node {node_id}) 'cat transparent_hugepage/enabled' produced stderr: {result.stderr.strip()}"
                )
        elif isinstance(result, str):
            self.logger.error(f"(Node {node_id}) Failed to get THP status: {result}")
        else:
            self.logger.error(
                f"(Node {node_id}) Received unexpected result type for THP status: {type(result)}."
            )

    def _print_hugepages_status(self, result: Any):
        """Formats and prints HugePages status."""
        if isinstance(result, BenchCommandOutput):
            cmd_output = result
            total, free, size = "N/A", "N/A", "N/A"
            for line in cmd_output.stdout.split("\n"):
                if "HugePages_Total:" in line:
                    total = line.split(":")[1].strip()
                elif "HugePages_Free:" in line:
                    free = line.split(":")[1].strip()
                elif "Hugepagesize:" in line:
                    size = line.split(":")[1].strip()
            self.logger.info(
                f"    System HugePages: Total={total}, Free={free}, Size={size}"
            )

            if cmd_output.stderr.strip():
                self.logger.warning(
                    f"    'grep Huge /proc/meminfo' produced stderr: {cmd_output.stderr.strip()}"
                )
        elif isinstance(result, str):
            self.logger.error(f"    Failed to get HugePages information: {result}")
        else:
            self.logger.error(
                f"    Received unexpected result type for HugePages info: {type(result)}."
            )

    def _print_worker_params(self, worker_addr: str, worker_idx: int):
        """Parses and prints key startup parameters for a single Worker process."""

        param_defaults = {
            "shared_memory_size_mb": "1024",
            "enable_urma": "false",
            "enable_thp": "false",
        }

        param_display_map = {
            "shared_memory_size_mb": "Shared Memory",
            "enable_urma": "URMA enabled",
            "numa": "NUMA binding",
            "enable_thp": "THP enabled",
        }

        parsed_params = param_defaults.copy()

        for key in param_display_map:
            if key not in parsed_params:
                parsed_params[key] = "N/A"

        ps_cmd = f"ps -ww -C datasystem_worker -f | grep -F 'worker_address={worker_addr}' | grep -v grep"

        worker_pid, has_valid_output, extracted_params = (
            self._parse_and_extract_worker_params(
                ps_cmd, worker_addr, param_display_map.keys()
            )
        )

        if extracted_params:
            parsed_params.update(
                (k, v) for k, v in extracted_params.items() if k in param_display_map
            )

        self.logger.info(
            f"  * Worker Process {worker_idx} (Address: {worker_addr}, PID: {worker_pid}):"
        )

        if has_valid_output:
            log_params = {
                param_display_map["shared_memory_size_mb"]: f"{parsed_params['shared_memory_size_mb']} MB",
                param_display_map["enable_urma"]: parsed_params["enable_urma"],
                param_display_map["enable_thp"]: parsed_params["enable_thp"],
                param_display_map["numa"]: "N/A",
            }
            for name, value in log_params.items():
                self.logger.info(f"      - {name}: {value}")
        else:
            self.logger.info(
                "      - Could not retrieve parameters for worker. See above errors for details."
            )

    def _parse_and_extract_worker_params(
        self, full_ps_command: str, worker_addr: str, param_names_to_find: set[str]
    ) -> tuple[str, bool, dict[str, str]]:
        """
        Executes the 'ps' command, parses its output, and extracts the worker's pid
        and specified parameters.
        Returns a tuple: (pid, has_valid_output_flag, extracted_params_dict)
        """
        ps_result = executor.execute(full_ps_command, worker_addr)

        if not isinstance(ps_result, BenchCommandOutput):
            error_msg = str(ps_result)
            self.logger.error(
                f"    Failed to execute 'ps' command for worker {worker_addr}. Reason: {error_msg}"
            )
            return "N/A", False, {}

        cmd_output = ps_result
        if cmd_output.stderr.strip():
            self.logger.warning(
                f"    'ps' command for worker {worker_addr} produced stderr: {cmd_output.stderr.strip()}"
            )
        if not cmd_output.stdout.strip() and not cmd_output.stderr.strip():
            self.logger.warning(
                f"    'ps' command for worker {worker_addr} produced no output."
            )

        raw_output = cmd_output.stdout.strip()
        if not raw_output:
            return "N/A", False, {}

        pid, extracted_params = self._parse_ps_output_string(
            raw_output, param_names_to_find
        )

        return pid, True, extracted_params

    def _parse_ps_output_string(
        self, raw_output: str, param_names_to_find: set[str]
    ) -> tuple[str, dict[str, str]]:
        pid = "N/A"
        extracted_params = {}

        lines = raw_output.split("\n")
        for line in lines:
            line = line.strip()
            if not line:
                continue

            if pid == "N/A":
                parts = line.split()
                if len(parts) > 1:
                    pid = parts[1]

            for param_name in param_names_to_find:
                match = re.search(rf"--{param_name}=(\S+)", line)
                if match:
                    extracted_params[param_name] = match.group(1)

        return pid, extracted_params
