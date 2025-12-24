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


def _load_ssh_config_from_json(ssh_config_path: str) -> dict[str, dict[str, Any]]:
    """
    Load and parse the SSH configuration JSON file.
    """
    if not ssh_config_path:
        logger.warning("warning: SSH config path is not provided.")
        return {}

    try:
        expanded_path = os.path.expanduser(ssh_config_path)
        with open(expanded_path, "r", encoding="utf-8") as f:
            config = json.load(f)

        if "hosts" in config and isinstance(config["hosts"], dict):
            return config["hosts"]
        logger.error(
            f"error: Invalid SSH config format in {expanded_path}. Missing or non-dictionary 'hosts' section."
        )
        return {}
    except FileNotFoundError:
        logger.error(f"error: SSH config file not found at {ssh_config_path}.")
        return {}
    except json.JSONDecodeError:
        logger.error(
            f"error: Invalid JSON format in SSH config file {ssh_config_path}."
        )
        return {}
    except Exception as e:
        logger.error(f"error: Failed to load SSH config file {ssh_config_path}: {e}")
        return {}


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

        # Core attributes
        self.ssh_config_map: dict[str, dict[str, Any]] = {}
        self.ssh_config_path: Optional[str] = None

        self.pkg_location_cache: dict[str, Optional[str]] = {}

        # Cache local IPs on initialization for performance
        self._local_ips_cache: list[str] = _get_local_ips()

    @classmethod
    def get_instance(cls) -> "Executor":
        """Returns the singleton instance of the Executor class."""
        if not cls._class_instance:
            cls._class_instance = Executor()
        return cls._class_instance

    def load_ssh_config(self, config_path: str):
        """
        Loads SSH configurations from a specified JSON file and caches them.
        This is the public method to configure the Executor.
        """
        self.ssh_config_path = config_path
        self.ssh_config_map = _load_ssh_config_from_json(config_path)

    def get_remote_info(self, target_address: str) -> Optional[BenchRemoteInfo]:
        """
        **INTERNAL** helper method to retrieve SSH information.
        It looks up in the cached `ssh_config_map`.
        """
        if not self.ssh_config_map:
            logger.warning(
                f"warning: SSH configuration map is empty. Ensure load_ssh_config() was called."
            )
            return None

        config = self.ssh_config_map.get(target_address)

        if not config:
            return None

        username = config.get("username")
        identity_file_path = config.get("identity_file")
        port_str = config.get("port", "22")

        if not username:
            logger.warning(
                f"warning: 'username' not found for target '{target_address}'."
            )
            return None
        if not identity_file_path:
            logger.warning(
                f"warning: 'identity_file' not found for target '{target_address}'."
            )
            return None

        expanded_identity_file = os.path.expanduser(identity_file_path)
        if not os.path.isfile(expanded_identity_file):
            logger.warning(
                f"warning: Identity file '{expanded_identity_file}' for target '{target_address}' does not exist."
            )
            return None

        try:
            ssh_port = int(port_str)
        except ValueError:
            logger.warning(
                f"warning: Invalid 'port' value '{port_str}' for target '{target_address}'."
            )
            return None

        return BenchRemoteInfo(
            host=target_address.split(":")[0],
            username=username,
            ssh_config_path=expanded_identity_file,
            ssh_port=ssh_port,
        )

    def get_datasystem_pkg_location(self, worker_address: str) -> Union[str, None]:
        """Retrieves the installation path of openyuanrong-datasystem."""
        if worker_address in self.pkg_location_cache:
            return self.pkg_location_cache[worker_address]

        pip_show_result = self.execute(
            "source ~/.bashrc && pip show openyuanrong-datasystem", worker_address
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

    def execute(
        self, command_str: str, target_address: str, env=None
    ) -> Union[BenchCommandOutput, str]:
        """
        Public interface to execute a command.
        It intelligently decides whether to run the command locally or remotely.
        """
        is_local = target_address.split(":")[0] in self._local_ips_cache

        remote_info = None
        if not is_local:
            remote_info = self.get_remote_info(target_address)
            if not remote_info:
                return f"Error: SSH configuration for '{target_address}' is missing or invalid."

        task = BenchCommandTask(command=command_str, env=env, remote=remote_info)
        task.run()

        if task.output is None:
            # This case should ideally not happen if run() is always called
            return f"Error: Execution of '{command_str}' on '{target_address}' did not produce an output object."

        return task.output


# --- Global Executor Instance ---
# This instance should be imported in other modules
executor = Executor.get_instance()
