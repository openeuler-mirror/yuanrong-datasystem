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

import json
import logging
import os
import subprocess
from typing import Any, Optional, Union

from yr.datasystem.cli.benchmark.task import (
    BenchCommandOutput,
    BenchCommandTask,
    BenchRemoteInfo,
)

logger = logging.getLogger("dsbench")





def _get_local_ips() -> list[str]:
    """
    Safely retrieves a list of all valid local IP addresses.
    It's used to determine if a target host is local or remote.
    """
    local_ips = {"127.0.0.1", "::1"}  # Add local loopback addresses
    try:
        # On Linux, `hostname -I` returns all non-loopback IP addresses
        local_ip_raw = (
            subprocess.check_output(["hostname", "-I"], stderr=subprocess.PIPE)
            .decode()
            .strip()
        )
        if local_ip_raw:
            ips_from_command = [
                ip.split("%")[0].split("/")[0]
                for ip in local_ip_raw.split()
            ]
            local_ips.update(ips_from_command)
    except FileNotFoundError:
        logger.warning(
            "warning: `hostname` command not found. Cannot auto-detect local IPs."
        )
    except subprocess.CalledProcessError as e:
        logger.warning(
            f"warning: `hostname -I` command failed. Cannot auto-detect local IPs. Error: {e.stderr.decode().strip()}"
        )
    except Exception as e:
        logger.warning(f"warning: Failed to get local IPs automatically. Error: {e}")

    local_ips.discard("")
    local_ips.discard(None)
    return list(local_ips)


class Executor:
    """
    A singleton class to manage SSH configurations and execute BenchCommandTasks.
    It replaces direct calls to subprocess with a more structured approach.
    """

    _class_instance: Optional["Executor"] = None

    def __new__(cls, *args, **kwargs):
        if not cls._class_instance:
            cls._class_instance = super(Executor, cls).__new__(cls)
            cls._class_instance.initialized = False
        return cls._class_instance

    def __init__(self):
        if self.initialized:
            return
        self.initialized = True

        self.pkg_location_cache: dict[str, Optional[str]] = {}
        self.dsbench_cpp_permissions_cache: dict[str, bool] = {}

        # Cache local IPs on initialization for performance
        self.local_ips_cache: list[str] = _get_local_ips()

    @classmethod
    def get_instance(cls) -> "Executor":
        """Returns the singleton instance of the Executor class."""
        if not cls._class_instance:
            cls._class_instance = Executor()
        return cls._class_instance



    def get_remote_info(self, target_address: str) -> Optional[BenchRemoteInfo]:
        """
        **INTERNAL** helper method to retrieve SSH information.
        Uses default SSH parameters: current user, default identity file (~/.ssh/id_rsa), and port 22.
        """
        import getpass

        # Extract hostname/IP from target_address (handle port if present)
        target_host = target_address.split(":")[0]

        # If target is local, return None (local execution)
        if target_host in self.local_ips_cache:
            return None

        # Use default SSH parameters
        current_user = getpass.getuser()
        identity_file_path = "~/.ssh/id_rsa"
        ssh_port = 22

        expanded_identity_file = os.path.expanduser(identity_file_path)
        if not os.path.isfile(expanded_identity_file):
            logger.warning(
                f"warning: Identity file '{expanded_identity_file}' for target '{target_address}' does not exist."
            )
            return None

        return BenchRemoteInfo(
            host=target_host,
            username=current_user,
            ssh_config_path=expanded_identity_file,
            ssh_port=ssh_port,
        )

    def get_datasystem_pkg_location(self, worker_address: str) -> Union[str, None]:
        """Retrieves the installation path of openyuanrong-datasystem."""
        if worker_address in self.pkg_location_cache:
            return self.pkg_location_cache[worker_address]

        pip_show_result = self.execute(
            'bash -l -c "source ~/.bashrc && pip show openyuanrong-datasystem"', worker_address
        )

        location = None
        if isinstance(pip_show_result, BenchCommandOutput):
            stdout = pip_show_result.stdout.strip()
            if not stdout:
                logger.error(
                    f"    [DEBUG] 'pip show' on {worker_address} executed successfully but had no output; "
                    f"'openyuanrong-datasystem' may not be installed."
                )
                self.pkg_location_cache[worker_address] = None
                return None

            for line in stdout.split("\n"):
                if line.startswith("Location:"):
                    location = line.split(":", 1)[1].strip()
                    break

            if not location:
                logger.error(
                    f"    [DEBUG] 'Location:' field not found in 'pip show' output from {worker_address}."
                )
                self.pkg_location_cache[worker_address] = None
                return None
        elif isinstance(pip_show_result, str):
            logger.error(
                f"    [DEBUG] Failed to get openyuanrong-datasystem location from {worker_address}: {pip_show_result}"
            )
            self.pkg_location_cache[worker_address] = None
            return None

        self.pkg_location_cache[worker_address] = location
        return location

    def ensure_dsbench_cpp_executable(self, worker_address: str) -> Union[str, None]:
        """Ensures dsbench_cpp has execute permissions on the specified worker.
        Only sets permissions once per worker address.

        Returns:
            str: The full path to dsbench_cpp if successful, None otherwise.
        """
        # Check if we've already set permissions for this worker
        if worker_address in self.dsbench_cpp_permissions_cache:
            if self.dsbench_cpp_permissions_cache[worker_address]:
                # We've already successfully set permissions, return the path
                location = self.get_datasystem_pkg_location(worker_address)
                if location:
                    return f"{location}/yr/datasystem/dsbench_cpp"
                return None
            else:
                # We tried before and failed, don't try again
                return None

        # Get the installation location
        datasystem_location = self.get_datasystem_pkg_location(worker_address)
        if not datasystem_location:
            logger.error(
                f"    [DEBUG] Could not find openyuanrong-datasystem location on {worker_address}."
            )
            self.dsbench_cpp_permissions_cache[worker_address] = False
            return None

        # Set permissions for dsbench_cpp
        dsbench_cpp_executable = f"{datasystem_location}/yr/datasystem/dsbench_cpp"
        chmod_command = f"chmod +x {dsbench_cpp_executable}"
        chmod_result = self.execute(chmod_command, worker_address, env=None)

        if isinstance(chmod_result, str):
            logger.error(
                f"    [DEBUG] Failed to set execute permissions for dsbench_cpp on {worker_address}: {chmod_result}"
            )
            self.dsbench_cpp_permissions_cache[worker_address] = False
            return None

        # Successfully set permissions, cache the result
        self.dsbench_cpp_permissions_cache[worker_address] = True
        logger.debug(
            "    [DEBUG] Successfully set execute permissions for dsbench_cpp on %s.", worker_address
        )

        return dsbench_cpp_executable

    def execute(
        self, command_str: str, target_address: str, env=None
    ) -> Union[BenchCommandOutput, str]:
        """
        Public interface to execute a command.
        It intelligently decides whether to run the command locally or remotely.
        """
        is_local = target_address.split(":")[0] in self.local_ips_cache
        remote_info = self.get_remote_info(target_address)

        # For remote targets, we need valid SSH configuration
        if not is_local and not remote_info:
            return f"Error: SSH configuration for '{target_address}' is missing or invalid."

        # For local targets, use remote_info if available (e.g., connecting to other user on same machine)
        # Otherwise, use local execution
        task = BenchCommandTask(command=command_str, env=env, remote=remote_info)
        task.run()

        if task.output is None:
            # This case should ideally not happen if run() is always called
            return f"Error: Execution of '{command_str}' on '{target_address}' did not produce an output object."

        return task.output


# --- Global Executor Instance ---
# This instance should be imported in other modules
executor = Executor.get_instance()
