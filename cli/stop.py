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
"""YuanRong datasystem CLI stop command."""

import json
import os
import re
import signal
import subprocess
import time

import yr.datasystem.cli.common.util as util
from yr.datasystem.cli.command import BaseCommand


class Command(BaseCommand):
    """
    Stop yuanrong datasystem worker service.
    """

    name = "stop"
    description = "stop yuanrong datasystem worker service"

    _base_timeout = 180
    _timeout = _base_timeout
    _check_interval = 0.2
    _default_shared_memory_size_mb = 1024
    _default_data_migrate_rate_limit_mb = 40

    def add_arguments(self, parser):
        """
        Add arguments to parser.

        Args:
            parser (ArgumentParser): Specify parser to which arguments are added.
        """
        group = parser.add_mutually_exclusive_group(required=True)
        group.add_argument(
            "-f", "--worker_config_path", metavar="FILE",
            help=(
                "stop worker by using configuration file (JSON format), "
                "which can be obtained through the generate_config command"
            ),
        )

        group.add_argument(
            "-w", "--worker_address", metavar="ADDR",
            help=("stop worker by specifying the worker address(ip:port), e.g., 127.0.0.1:31501"),
        )

    def run(self, args):
        """
        Execute for stop command.

        Args:
            args (Namespace): Parsed arguments to hold customized parameters.

        Returns:
            int: Exit code, 0 for success, 1 for failure.
        """
        try:
            worker_config = None
            if args.worker_config_path:
                worker_config = self.load_worker_config(args.worker_config_path)
            address = self.get_worker_address(args, worker_config)
            pid = self.get_unique_pid(address)
            self._timeout = self.calculate_stop_timeout(pid, worker_config)
            self.graceful_kill(pid)
            if self.wait_exit(pid):
                self.logger.info(f"[  OK  ] Stop worker service @ {address} normally, PID: {pid}")
                return self.SUCCESS
            if self.force_kill(pid):
                self.logger.info(f"[  OK  ] Force stop worker service @ {address}, PID: {pid}")
                return self.SUCCESS
            raise RuntimeError(f"[  FAILED  ] Force stop worker failed @ {address}, PID: {pid}")
        except Exception as e:
            self.logger.error(f"Stop failed: {e}")
            return self.FAILURE

    def load_worker_config(self, worker_config_path):
        """
        Load worker configuration file.

        Args:
            worker_config_path (str): Worker configuration path.

        Returns:
            dict: Parsed configuration dictionary.

        Raises:
            ValueError: If the configuration file format is incorrect.
        """
        config_path = os.path.realpath(os.path.expanduser(worker_config_path))
        config_path = util.valid_safe_path(config_path)
        try:
            with open(config_path, "r") as f:
                return json.load(f)
        except json.JSONDecodeError as e:
            raise ValueError("The configuration file format is incorrect.") from e

    def get_worker_address(self, args, worker_config=None):
        """
        Obtain the address of the worker to be stopped.

        Args:
            args (Namespace): Parsed arguments containing worker configuration or address.
            worker_config (dict): Parsed worker config, optional.

        Returns:
            str: The worker address.

        Raises:
            ValueError: If the configuration file format is incorrect.
            RuntimeError: If the worker_address is missing or invalid in the configuration.
        """
        if args.worker_address:
            return args.worker_address

        config = worker_config if worker_config is not None else self.load_worker_config(args.worker_config_path)

        if "worker_address" not in config:
            raise RuntimeError("The configuration file is missing worker_address")

        address = self.get_config_value(config, "worker_address")
        if not address:
            raise RuntimeError("Invalid worker_address value")

        return address

    def get_config_value(self, worker_config, key):
        """
        Get value from worker config item.

        Args:
            worker_config (dict): Parsed worker config.
            key (str): Config key.

        Returns:
            Optional[str]: Config value if present.
        """
        if not worker_config:
            return None
        config_item = worker_config.get(key)
        if isinstance(config_item, dict):
            value = config_item.get("value")
            if value not in ("", None):
                return value
            return config_item.get("default")
        return config_item

    def get_value_from_process_cmdline(self, pid, key):
        """
        Read the value of a flag from process cmdline.

        Args:
            pid (int): Process ID.
            key (str): Flag key.

        Returns:
            Optional[str]: Flag value if present.
        """
        try:
            with open(f"/proc/{pid}/cmdline", "rb") as f:
                raw_cmdline = f.read()
        except OSError:
            return None

        if not raw_cmdline:
            return None

        args = [arg.decode("utf-8", errors="ignore") for arg in raw_cmdline.split(b"\x00") if arg]

        prefixes = (f"--{key}=", f"-{key}=")
        for arg in args:
            for prefix in prefixes:
                if arg.startswith(prefix):
                    return arg[len(prefix):]

        options = (f"--{key}", f"-{key}")
        for i, arg in enumerate(args[:-1]):
            if arg in options:
                return args[i + 1]
        return None

    def resolve_flag_positive_int(self, pid, worker_config, key, default_value):
        """
        Resolve a positive integer value by priority: process cmdline -> worker config -> default.

        Args:
            pid (int): Process ID.
            worker_config (dict): Parsed worker config.
            key (str): Config key.
            default_value (int): Fallback value.

        Returns:
            int: Resolved value.
        """
        raw_value = self.get_value_from_process_cmdline(pid, key)
        value_source = "process cmdline"
        if raw_value in (None, ""):
            raw_value = self.get_config_value(worker_config, key)
            value_source = "worker config"
        if raw_value in (None, ""):
            return default_value
        try:
            parsed = int(str(raw_value).strip())
            if parsed > 0:
                return parsed
        except (TypeError, ValueError):
            pass
        self.logger.warning(
            f"Invalid {key}={raw_value} from {value_source}, fallback to default({default_value})."
        )
        return default_value

    def calculate_stop_timeout(self, pid, worker_config=None):
        """
        Calculate stop timeout with formula:
        timeout = 180 + shared_memory_size_mb / data_migrate_rate_limit_mb

        Args:
            pid (int): Process ID.
            worker_config (dict): Parsed worker config.

        Returns:
            float: Stop timeout in seconds.
        """
        shared_memory_size_mb = self.resolve_flag_positive_int(
            pid, worker_config, "shared_memory_size_mb", self._default_shared_memory_size_mb
        )
        data_migrate_rate_limit_mb = self.resolve_flag_positive_int(
            pid, worker_config, "data_migrate_rate_limit_mb", self._default_data_migrate_rate_limit_mb
        )
        return self._base_timeout + (shared_memory_size_mb / data_migrate_rate_limit_mb)

    def get_unique_pid(self, address):
        """
        Get the unique process PID of the worker service.

        Args:
            address (str): The worker address to find the corresponding process.

        Returns:
            int: The process ID (PID) of the worker service.

        Raises:
            RuntimeError: If no matching process or multiple processes are found.
        """
        util.is_valid_address_port(address)
        target_arg = f"-worker_address={address}"
        target_arg = re.escape(target_arg)
        cmd = ["pgrep", "-fl", "--", target_arg]
        try:
            output = subprocess.check_output(
                cmd,
                stderr=subprocess.STDOUT,
                timeout=5,
                text=True
            )

        except subprocess.CalledProcessError as e:
            raise RuntimeError(f"No matching process found for {target_arg}") from e

        pids = []
        for line in output.strip().splitlines():
            current_pid, pid_name = line.split(' ')
            if pid_name != "dscli":
                pids.append(int(current_pid))

        if not pids:
            raise RuntimeError(f"No matching process found for {target_arg}")
        if len(pids) > 1:
            raise RuntimeError(f"Multiple matching processes found for {target_arg}: {pids}")

        return pids[0]

    def graceful_kill(self, pid):
        """
        Gracefully terminate the process.

        Args:
            pid (int): The process ID (PID) to terminate.

        Raises:
            RuntimeError: If the process does not exist or insufficient permissions.
        """
        try:
            os.kill(pid, signal.SIGTERM)
        except ProcessLookupError as e:
            raise RuntimeError("The process no longer exists") from e
        except PermissionError as e:
            raise RuntimeError("Insufficient permissions to operate the process") from e

    def force_kill(self, pid):
        """
        Forcefully terminate a process.

        Args:
            pid (int): The process ID (PID) to terminate.

        Returns:
            bool: True if the process was successfully terminated, False otherwise.
        """
        try:
            os.kill(pid, signal.SIGKILL)
        except ProcessLookupError:
            return False
        return True

    def wait_exit(self, pid):
        """
        Wait for the process to exit.

        Args:
            pid (int): The process ID (PID) to monitor.

        Returns:
            bool: True if the process exits within the timeout, False otherwise.
        """
        start_time = time.time()
        while time.time() - start_time < self._timeout:
            try:
                with open(f'/proc/{pid}/stat', 'r') as f:
                    stat_info = f.read().split()
                    if stat_info[2] == 'Z':
                        return True
            except (FileNotFoundError, ProcessLookupError):
                return True
            try:
                os.kill(pid, 0)
            except ProcessLookupError:
                return True
            time.sleep(self._check_interval)
        return False
