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
    Stop yuanrong datasystem worker or coordinator service.
    """

    name = "stop"
    description = "stop yuanrong datasystem worker or coordinator service"

    _base_timeout = 180
    _timeout = _base_timeout
    _check_interval = 0.2
    _default_shared_memory_size_mb = 1024
    _default_data_migrate_rate_limit_mb = 40
    _service_type_key = "service_type"
    _worker_service = "worker"
    _coordinator_service = "coordinator"

    def add_arguments(self, parser):
        """
        Add arguments to parser.

        Args:
            parser (ArgumentParser): Specify parser to which arguments are added.
        """
        parser.add_argument(
            "-f",
            "--config_path",
            "--worker_config_path",
            dest="config_path",
            metavar="FILE",
            help=(
                "stop service by using configuration file (JSON format), "
                "which can be obtained through the generate_config command"
            ),
        )

        parser.add_argument(
            "-w",
            "--worker_address",
            metavar="ADDR",
            help="stop worker by specifying the worker address(ip:port), e.g., 127.0.0.1:31501",
        )
        parser.add_argument(
            "--coordinator_address",
            metavar="ADDR",
            help="stop coordinator by specifying the coordinator address(ip:port), e.g., 127.0.0.1:31511",
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
            stopped = False
            if args.config_path:
                config = self.load_config(args.config_path)
                service_type = self.get_service_type(config)
                if service_type == self._coordinator_service:
                    address = self.get_address(config, "coordinator_address")
                    self.stop_service(self._coordinator_service, address, config)
                else:
                    address = self.get_address(config, "worker_address")
                    self.stop_service(self._worker_service, address, config)
                return self.SUCCESS

            if args.worker_address:
                self.stop_service(self._worker_service, args.worker_address)
                stopped = True
            if args.coordinator_address:
                self.stop_service(self._coordinator_service, args.coordinator_address)
                stopped = True
            if not stopped:
                raise RuntimeError(
                    "worker_address, coordinator_address, or config_path must be specified"
                )
            return self.SUCCESS
        except Exception as e:
            self.logger.error(f"Stop failed: {e}")
            return self.FAILURE

    def load_config(self, config_path):
        """
        Load service configuration file.

        Args:
            config_path (str): Service configuration path.

        Returns:
            dict: Parsed configuration dictionary.

        Raises:
            ValueError: If the configuration file format is incorrect.
        """
        config_path = os.path.realpath(os.path.expanduser(config_path))
        config_path = util.valid_safe_path(config_path)
        try:
            with open(config_path, "r") as f:
                return json.load(f)
        except json.JSONDecodeError as e:
            raise ValueError("The configuration file format is incorrect.") from e

    def load_worker_config(self, worker_config_path):
        """Backward-compatible wrapper for old worker-only helpers."""
        return self.load_config(worker_config_path)

    def get_service_type(self, config):
        """Resolve service_type. Missing or empty service_type means worker."""
        service_type = self.get_config_value(config, self._service_type_key)
        if service_type in (None, ""):
            return self._worker_service
        service_type = str(service_type).strip().lower()
        if service_type not in (self._worker_service, self._coordinator_service):
            raise ValueError("service_type must be coordinator or worker")
        return service_type

    def get_address(self, config, key):
        """Get a required service address from config."""
        if key not in config:
            raise RuntimeError(f"The configuration file is missing {key}")
        address = self.get_config_value(config, key)
        if not address:
            raise RuntimeError(f"Invalid {key} value")
        return address

    def get_worker_address(self, args, worker_config=None):
        """
        Obtain the address of the worker to be stopped.

        Args:
            args (Namespace): Parsed arguments containing worker configuration or address.
            worker_config (dict): Parsed worker config, optional.

        Returns:
            str: The worker address.
        """
        if args.worker_address:
            return args.worker_address

        config = (
            worker_config
            if worker_config is not None
            else self.load_config(args.config_path)
        )
        return self.get_address(config, "worker_address")

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

        args = [
            arg.decode("utf-8", errors="ignore")
            for arg in raw_cmdline.split(b"\x00")
            if arg
        ]

        prefixes = (f"--{key}=", f"-{key}=")
        for arg in args:
            for prefix in prefixes:
                if arg.startswith(prefix):
                    return arg[len(prefix) :]

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
            pid,
            worker_config,
            "shared_memory_size_mb",
            self._default_shared_memory_size_mb,
        )
        data_migrate_rate_limit_mb = self.resolve_flag_positive_int(
            pid,
            worker_config,
            "data_migrate_rate_limit_mb",
            self._default_data_migrate_rate_limit_mb,
        )
        return self._base_timeout + (shared_memory_size_mb / data_migrate_rate_limit_mb)

    def stop_service(self, service_type, address, config=None):
        """Stop one service by address."""
        flag_name = (
            "coordinator_address"
            if service_type == self._coordinator_service
            else "worker_address"
        )
        binary_name = (
            "datasystem_coordinator"
            if service_type == self._coordinator_service
            else "datasystem_worker"
        )
        pid = self.get_unique_pid(address, flag_name, binary_name)
        self._timeout = self._base_timeout
        if service_type == self._worker_service:
            self._timeout = self.calculate_stop_timeout(pid, config)
        self.graceful_kill(pid)
        if self.wait_exit(pid):
            self.logger.info(
                f"[  OK  ] Stop {service_type} service @ {address} normally, PID: {pid}"
            )
            return
        if self.force_kill(pid):
            self.logger.info(
                f"[  OK  ] Force stop {service_type} service @ {address}, PID: {pid}"
            )
            return
        raise RuntimeError(
            f"[  FAILED  ] Force stop {service_type} failed @ {address}, PID: {pid}"
        )

    def get_unique_pid(self, address, flag_name="worker_address", binary_name=None):
        """
        Get the unique process PID of a service.

        Args:
            address (str): The service address to find the corresponding process.
            flag_name (str): Address flag name.
            binary_name (str): Expected binary name.

        Returns:
            int: The process ID (PID) of the service.

        Raises:
            RuntimeError: If no matching process or multiple processes are found.
        """
        util.is_valid_address_port(address)
        target_arg = f"{flag_name}={address}"
        cmd = ["pgrep", "-fl", "--", re.escape(target_arg)]
        try:
            output = subprocess.check_output(
                cmd, stderr=subprocess.STDOUT, timeout=5, text=True
            )

        except subprocess.CalledProcessError as e:
            raise RuntimeError(f"No matching process found for {target_arg}") from e

        pids = []
        for line in output.strip().splitlines():
            parts = line.split(" ", 1)
            if not parts:
                continue
            current_pid = int(parts[0])
            if self.is_expected_process(current_pid, flag_name, address, binary_name):
                pids.append(current_pid)

        if not pids:
            raise RuntimeError(f"No matching process found for {target_arg}")
        if len(pids) > 1:
            raise RuntimeError(
                f"Multiple matching processes found for {target_arg}: {pids}"
            )

        return pids[0]

    def is_expected_process(self, pid, flag_name, address, binary_name=None):
        """Verify pgrep candidate against /proc cmdline."""
        try:
            with open(f"/proc/{pid}/cmdline", "rb") as f:
                raw_cmdline = f.read()
        except OSError:
            return False
        args = [
            arg.decode("utf-8", errors="ignore")
            for arg in raw_cmdline.split(b"\x00")
            if arg
        ]
        if not args or os.path.basename(args[0]) == "dscli":
            return False
        if binary_name and not any(
            os.path.basename(arg) == binary_name for arg in args
        ):
            return False
        return self.get_value_from_process_cmdline(pid, flag_name) == address

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
                with open(f"/proc/{pid}/stat", "r") as f:
                    stat_info = f.read().split()
                    if stat_info[2] == "Z":
                        return True
            except (FileNotFoundError, ProcessLookupError):
                return True
            try:
                os.kill(pid, 0)
            except ProcessLookupError:
                return True
            time.sleep(self._check_interval)
        return False
