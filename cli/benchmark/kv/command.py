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
    BenchmarkBaseCommand,
    BenchSuite,
)
from yr.datasystem.cli.benchmark.executor import executor
from yr.datasystem.cli.benchmark.kv import validator
from yr.datasystem.cli.benchmark.kv.bench_suite_builder import KVBenchSuiteBuilder
from yr.datasystem.cli.benchmark.task import BenchArgs, BenchCommandOutput

from yr.datasystem.cli.benchmark.system_info import SystemInfoCollector


class KVCommand(BenchmarkBaseCommand):
    name = "kv"
    description = "KV Performance Benchmarking"

    def __init__(self):
        """
        Initializes the Command instance.
        """
        super().__init__()
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

        raw_workers = (
            f"{self.args.set_worker_addresses},{self.args.get_worker_addresses}"
        )
        all_worker_addresses = sorted(list(set(raw_workers.split(","))))

        if not all_worker_addresses:
            self.logger.info(
                "  * No worker addresses are configured. Configuration check completed."
            )
            return True
        ip_to_address_map = {addr.split(":")[0]: addr for addr in all_worker_addresses}
        # Get unique IP list from all worker addresses
        unique_ips = sorted(list(set(addr.split(":")[0] for addr in all_worker_addresses)))
        # Print node information table
        self._print_node_info_table(unique_ips, ip_to_address_map)

        # Print worker parameters table
        self._print_worker_params_table(all_worker_addresses)

        return True

    def _print_node_info_table(self, unique_ips: list[str], ip_to_address_map: dict[str, str]):
        """Print node information table for all unique IPs."""
        self.logger.info("Node Information:")
        col_widths = {
            "ip": 15,
            "version": 10,
            "commit_id": 40,
            "total_mem": 15,
            "free_mem": 15,
            "thp": 10,
            "hugepages": 15,
            "cpu_mhz": 10,
        }

        header = (
            f"{'IP':<{col_widths['ip']}}"
            f"{'Version':<{col_widths['version']}}"
            f"{'Commit ID':<{col_widths['commit_id']}}"
            f"{'Total Memory':>{col_widths['total_mem']}}"
            f"{'Free Memory':>{col_widths['free_mem']}}"
            f"{'THP':>{col_widths['thp']}}"
            f"{'HugePages':>{col_widths['hugepages']}}"
            f"{'CPU MHz':>{col_widths['cpu_mhz']}}"
        )
        self.logger.info("=" * len(header))
        self.logger.info(header)
        self.logger.info("-" * len(header))
        for idx, ip in enumerate(unique_ips, 1):
            original_address = ip_to_address_map.get(ip, ip)

            if ip in executor.local_ips_cache:
                node_info = SystemInfoCollector.get_node_info(ip)
            else:
                node_info = self._get_remote_node_info(original_address)
            row = (
                f"{ip:<{col_widths['ip']}}"
                f"{node_info['Version']:<{col_widths['version']}}"
                f"{node_info['Commit ID']:>{col_widths['commit_id']}}"
                f"{node_info['Total Memory']:>{col_widths['total_mem']}}"
                f"{node_info['Free Memory']:>{col_widths['free_mem']}}"
                f"{node_info['THP']:>{col_widths['thp']}}"
                f"{node_info['HugePages']:>{col_widths['hugepages']}}"
                f"{node_info['CPU MHz']:>{col_widths['cpu_mhz']}}"
            )
            self.logger.info(row)

        self.logger.info("=" * len(header))

    def _get_remote_node_info(self, address: str) -> dict[str, any]:
        """Get node information from remote IP using dsbench show command."""
        result = executor.execute('bash -l -c "dsbench show"', address)
        node_info = {}

        if hasattr(result, 'stderr'):
            for line in result.stderr.strip().split('\n'):
                if ':' in line:
                    key, value = line.split(':', 1)
                    node_info[key.strip()] = value.strip()

        # Ensure all required fields are present
        required_fields = ["IP", "Version", "Commit ID", "Total Memory", "Free Memory", "THP", "HugePages", "CPU MHz"]
        for field in required_fields:
            if field not in node_info:
                node_info[field] = ""

        return node_info

    def build_suite(self, bench_args: BenchArgs) -> BenchSuite:
        """Builds a benchmark suite for KV tests."""
        builder = KVBenchSuiteBuilder(bench_args)
        return builder.build()

    def _add_test_config_arguments(self, parser: argparse.ArgumentParser):
        """Adds arguments related to test suite and testcase file configuration."""
        parser.add_argument(
            "-a",
            "--all",
            action="store_true",
            help="Run all test cases using built-in configuration. Requires at least 25GB shared memory per worker.",
        )
        parser.add_argument(
            "-f",
            "--testcase_file",
            type=str,
            help="""
            Testcase file (must be .json).
            Format example:
            [
                {"num": 1000, "size": "1MB", "client_num": 1, "thread_num": 1, "batch_num": 1},
                {"num": 250, "size": "1MB", "client_num": 4, "thread_num": 1, "batch_num": 1},
                {"num": 125, "size": "1MB", "client_num": 8, "thread_num": 1, "batch_num": 1}
            ]
            """,
        )

    def _add_run_config_arguments(self, parser: argparse.ArgumentParser):
        """Adds arguments related to the benchmark runtime configuration."""
        parser.add_argument(
            "-c",
            "--client_num",
            type=int,
            default=None,
            help="Number of clients per worker (default: 8, range: 1-128). "
            "client_num * thread_num must be between 1 and 128.",
        )
        parser.add_argument(
            "-t",
            "--thread_num",
            type=int,
            default=None,
            help="Number of threads per client (default: 1, range: 1-128). "
            "client_num * thread_num must be between 1 and 128.",
        )
        parser.add_argument(
            "-b",
            "--batch_num",
            type=int,
            default=None,
            help="Batch number (default: 1, max 10000)",
        )
        parser.add_argument(
            "-n",
            "--num",
            type=int,
            default=None,
            help="Number of keys per worker process, distributed across all clients,threads (default: 100)"
        )
        parser.add_argument(
            "-s",
            "--size",
            type=str,
            default=None,
            help="Data size of key (e.g. 2B/4KB/8MB/1GB or just 2 for 2B) (default: 1MB)",
        )

        parser.add_argument(
            "-p",
            "--prefix",
            type=str,
            default="Bench",
            help="Prefix of the key (default: Bench, length: 1-64, only letters, numbers and underscores)",
        )
        parser.add_argument(
            "--owner_worker",
            type=str,
            default="",
            metavar="",
            help="Owner worker of the object metadata (ip:port) (default: empty)",
        )
        parser.add_argument(
            "--numa",
            type=str,
            default="",
            metavar="",
            help="Bind to specific numa (e.g. 0-3,10-20,1,2,3) (default: empty)",
        )
        parser.add_argument(
            "--concurrent",
            action="store_true",
            help="Run concurrent set/get workload. Instead of the default sequential flow "
            "(set -> get -> del), this mode runs: prefill -> concurrent set/get -> del. "
            "Use this to stress-test concurrent read/write performance on the same data.",
        )

    def _add_cluster_config_arguments(self, parser: argparse.ArgumentParser):
        """Adds arguments related to cluster setup, authentication, and tools."""
        parser.add_argument(
            "-S",
            "--set_worker_addresses",
            required=True,
            help="Comma-separated list of worker addresses (e.g., ip1:port1,ip2:port2) for executing set operations. "
                 "dsbench will create a task for each worker to write test data. (required)",
        )
        parser.add_argument(
            "-G",
            "--get_worker_addresses",
            required=True,
            help="Comma-separated list of worker addresses (e.g., ip1:port1,ip2:port2) for executing get operations. "
                 "dsbench will create a task for each worker to read test data, and use the first worker "
                 "to delete test data. (required)",
        )
        parser.add_argument("--access_key", type=str, default="", metavar="", help="Access key (default: empty)")
        parser.add_argument("--secret_key", type=str, default="", metavar="", help="Secret key (default: empty)")

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
                    "        [DEBUG] Raw lscpu output:\n%s", lscpu_full_output
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

    def _get_worker_params(self, worker_addr: str) -> dict:
        """Parses and returns key startup parameters for a single Worker process."""

        # Parameter defaults according to the requirements
        param_defaults = {
            "shared_memory_size_mb": "1024",
            "enable_urma": "false",
            "enable_thp": "false",
            "enable_huge_tlb": "false",
            "minloglevel": "0",
            "numa": "N/A",
        }

        # Map between parameter names in the command line and display names
        param_display_map = {
            "shared_memory_size_mb": "Shared Memory",
            "enable_urma": "URMA",
            "enable_thp": "THP",
            "enable_huge_tlb": "HugePages",
            "minloglevel": "Log Level",
            "numa": "NUMA Node",
        }

        parsed_params = param_defaults.copy()

        for key in param_display_map:
            if key not in parsed_params:
                parsed_params[key] = "N/A"

        worker_pid, has_valid_output, extracted_params = (
            self._parse_and_extract_worker_params(
                worker_addr, param_display_map.keys()
            )
        )

        if extracted_params:
            parsed_params.update(
                (k, v) for k, v in extracted_params.items() if k in param_display_map
            )

        # Convert boolean values to "enable"/"disable" format
        def bool_to_enable_disable(value):
            return "enable" if value.lower() == "true" else "disable"

        # Map minloglevel to log level names
        def minloglevel_to_name(value):
            log_level_map = {
                "0": "INFO",
                "1": "WARNING",
                "2": "ERROR"
            }
            return log_level_map.get(value, "UNKNOWN")

        # Get CPU affinity using taskset if we have a valid PID
        cpu_affinity = "N/A"
        if worker_pid and worker_pid != "N/A":
            taskset_cmd = f"taskset -cp {worker_pid}"
            taskset_result = self._execute_taskset_command(taskset_cmd, worker_addr)
            if taskset_result:
                cpu_affinity = taskset_result

        # Return parameters in table row format
        return {
            "Worker address": worker_addr,
            "Shared Memory": f"{parsed_params['shared_memory_size_mb']}MB",
            "URMA": bool_to_enable_disable(parsed_params["enable_urma"]),
            "THP": bool_to_enable_disable(parsed_params["enable_thp"]),
            "HugePages": bool_to_enable_disable(parsed_params["enable_huge_tlb"]),
            "Affinity CPU": cpu_affinity,
            "Log Level": minloglevel_to_name(parsed_params["minloglevel"]),
            "PID": worker_pid,
            "has_valid_output": has_valid_output
        }

    def _execute_taskset_command(self, taskset_cmd: str, worker_addr: str) -> str:
        """Executes taskset command to get CPU affinity for a given PID."""
        result = executor.execute(taskset_cmd, worker_addr)
        if isinstance(result, BenchCommandOutput):
            stdout = result.stdout.strip()
            if stdout:
                # Extract CPU affinity from taskset output
                # Example output: "pid 1234's current affinity list: 0,1,2,3"
                affinity_match = re.search(r"affinity list:\s*(.+)", stdout)
                if affinity_match:
                    return affinity_match.group(1).strip()
            if result.stderr.strip():
                self.logger.warning(
                    f"    'taskset' command for worker {worker_addr} produced stderr: {result.stderr.strip()}"
                )
        return "N/A"

    def _print_worker_params_table(self, all_worker_addresses: list[str]):
        """Prints worker parameters in a formatted table."""
        self.logger.info("Worker Parameters:")
        self.logger.info("=" * 100)

        # Print table header
        self.logger.info(f"{'Worker address':<20} {'Shared Memory':<15} {'URMA':<10} {'THP':<10} {'HugePages':<12} {'Affinity CPU':<15} {'Log Level':<12}")
        self.logger.info("-" * 100)

        # Collect and print worker parameters line by line
        for worker_address in all_worker_addresses:
            worker_params = self._get_worker_params(worker_address)
            if worker_params and worker_params["has_valid_output"]:
                # Build row data
                row_line = f"{worker_params['Worker address']:<20} "
                row_line += f"{worker_params['Shared Memory']:<15} "
                row_line += f"{worker_params['URMA']:<10} "
                row_line += f"{worker_params['THP']:<10} "
                row_line += f"{worker_params['HugePages']:<12} "
                row_line += f"{worker_params['Affinity CPU']:<15} "
                row_line += f"{worker_params['Log Level']:<12}"
                self.logger.info(row_line)
            else:
                # Print row with default values if parameters cannot be retrieved
                row_line = f"{worker_address:<20} "
                row_line += f"{'N/A':<15} "
                row_line += f"{'N/A':<10} "
                row_line += f"{'N/A':<10} "
                row_line += f"{'N/A':<12} "
                row_line += f"{'N/A':<15} "
                row_line += f"{'N/A':<12}"
                self.logger.info(row_line)

        self.logger.info("=" * 100)

    def _parse_and_extract_worker_params(
        self, worker_addr: str, param_names_to_find: set[str]
    ) -> tuple[str, bool, dict[str, str]]:
        """
        Executes the 'ps' command, parses its output, and extracts the worker's pid
        and specified parameters.
        Returns a tuple: (pid, has_valid_output_flag, extracted_params_dict)
        """
        # Use simple ps command without pipes
        ps_command = "ps -C datasystem_worker -o pid,cmd"
        ps_result = executor.execute(ps_command, worker_addr)

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

        # Filter output in Python
        filtered_lines = []
        for line in raw_output.split('\n'):
            line = line.strip()
            if f'worker_address={worker_addr}' in line:
                filtered_lines.append(line)

        if not filtered_lines:
            return "N/A", False, {}

        # Parse the first matching line
        pid, extracted_params = self._parse_ps_output_string(
            '\n'.join(filtered_lines), param_names_to_find
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
                if len(parts) > 0:
                    pid = parts[0]

            for param_name in param_names_to_find:
                match = re.search(rf"--{param_name}=(\S+)", line)
                if match:
                    extracted_params[param_name] = match.group(1)

        return pid, extracted_params
