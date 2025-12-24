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
"""YuanRong datasystem CLI util module."""

import ipaddress
import importlib.resources
import json
import os
import re
import subprocess
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from yr.datasystem.cli.common.constant import ClusterConfig

DANGER = re.compile(r'''[;&|`$()<>{}!*?\n\t\r\x0b\x0c'"\\]''')
UNSAFE = ["/bin", "/sbin", "/lib", "/lib64", "/",
          "/boot", "/dev", "/etc", "/sys", "/proc"]


def valid_safe_path(path: str):
    """
    Validate the path is safe.

    Args:
        path: Input path.

    Raises:
        ValueError: If input contains potential command-injection characters.
    """
    norm_path = os.path.normpath(os.path.abspath(path))
    if "../" in norm_path:
        raise ValueError(f"Path {path} contains directory traversal sequences (e.g., ../)")
    if norm_path in UNSAFE:
        raise ValueError(f"Path {path} is outside allowed directory")
    for parent in UNSAFE:
        if parent == "/":
            continue
        if norm_path.startswith(parent + os.sep):
            raise ValueError(f"Path {path} is outside allowed directory")
    return norm_path


def is_valid_ip(addr: str) -> bool:
    """
    Validate the addr is valid IP address. Checks both IPv4 and IPv6
    Returns True if it was valid and is an IPv6 address

    Args:
        addr: IP address

    Raises:
        ValueError: If input contains potential command-injection characters.
    """
    try:
        ipaddress.IPv4Address(addr)
        return False
    except ipaddress.AddressValueError:
        try:
            ipaddress.IPv6Address(addr)
            return True
        except ipaddress.AddressValueError as e2:
            raise ValueError(f"Invalid address {addr}") from e2


def is_valid_ipv4(addr: str):
    """
    Validate the addr is valid IPV4 address.

    Args:
        addr: IP address

    Raises:
        ValueError: If input contains potential command-injection characters.
    """
    try:
        ipaddress.IPv4Address(addr)
    except ipaddress.AddressValueError as e:
        raise ValueError(f"Invalid address {addr}") from e


def is_valid_ipv6(addr: str):
    """
    Validate the addr is valid IPV6 address.

    Args:
        addr: IP address

    Raises:
        ValueError: If input contains potential command-injection characters.
    """
    try:
        ipaddress.IPv6Address(addr)
    except ipaddress.AddressValueError as e:
        raise ValueError(f"Invalid IPv6 address {addr}") from e


def is_valid_port(port):
    """
    Validate the port is valid IPV4 port.

    Args:
        port: IP port

    Raises:
        ValueError: If input contains potential command-injection characters.
    """
    port = int(port)
    if port < 0 or port > 65535:
        raise ValueError(f"port {port} is not in valid range")


def is_valid_address_port(addr_port: str):
    """
    Validate the addr_port is valid IP address with port.
    Allowed input formats:
    ipv4_format:port
    [ipv6_format]:port
    Note that ipv6_format internally contains multiple ':' characters

    Args:
        addr_port: IP address with port

    Raises:
        ValueError: If input contains potential command-injection characters.
    """
    try:
        port_delim_pos = addr_port.rfind(":")
        addr, port = addr_port[:port_delim_pos], addr_port[port_delim_pos + 1:]
    except ValueError as e:
        raise ValueError(f"Invalid address:port {addr_port}") from e

    if addr[0] == '[':
        if addr[-1] == ']':
            addr = addr[1:-1]
            is_valid_ipv6(addr)
        else:
            raise ValueError(f"Invalid address:port {addr_port}") from e
    else:
        is_valid_ipv4(addr)

    is_valid_port(port)


def validate_no_injection(user_input: str) -> str:
    """
    Validate the user input is no command injection.

    Args:
        user_input: user input

    Raises:
        ValueError: If input contains potential command-injection characters.
    """
    if not isinstance(user_input, str):
        return user_input
    if DANGER.search(user_input):
        raise ValueError("Input contains potential command-injection characters")
    return user_input


def get_required_config(config, key):
    """Get a required configuration value from a dictionary.

    Args:
        config: The configuration dictionary.
        key: The key to retrieve from the dictionary.

    Raises:
        ValueError: If the key is not found in the configuration.
    """
    value = config.get(key)
    if value is None:
        raise ValueError(f"{key} not found in config")
    return value


def load_cluster_config(
    path: str, keys: Optional[List[ClusterConfig]] = None
) -> Dict[str, str]:
    """Load cluster configuration from a JSON file and extract specific keys.

    Args:
        path: Path to the JSON configuration file.
        keys: Optional list of ClusterConfig keys to extract. If None, all keys are used.

    Returns:
        A dictionary containing the extracted configuration values.

    Raises:
        ValueError: If the configuration file format is incorrect.
    """
    try:
        path = os.path.abspath(path)
        with open(path) as f:
            config = json.load(f)
    except json.JSONDecodeError as e:
        raise ValueError(f"The configuration file {path} format is incorrect.") from e

    if keys is None:
        keys = list(ClusterConfig)

    result = {}
    for key in keys:
        keys_list = key.value.split(".")
        value = config
        for k in keys_list:
            value = get_required_config(value, k)
        result[key] = value
    return result


def ssh_execute(host, username, private_key, command):
    """Execute a command on a remote host via SSH.

    Args:
        host: The remote host to connect to.
        username: The username for SSH authentication.
        private_key: Path to the private key for SSH authentication.
        command: The command to execute on the remote host.

    Returns:
        The output of the executed command.

    Raises:
        RuntimeError: If the SSH connection fails or the command execution fails.
    """
    ssh_command = ["ssh", "-q", "-i", private_key, f"{username}@{host}", command]
    try:
        process = subprocess.Popen(
            ssh_command, stdout=subprocess.PIPE, stderr=subprocess.PIPE
        )
        stdout, stderr = process.communicate(timeout=300)
        if process.returncode != 0:
            raise RuntimeError(
                f"Error executing on {host} (exit code {process.returncode}): {stderr.decode()}"
            )
        return stdout.decode()
    except Exception as e:
        raise RuntimeError(f"SSH connection to {host} failed: {e}") from e


def scp_upload(local_file, remote_host, remote_path, user_name, private_key):
    """Upload a file to a remote host via SCP.

    Args:
        local_file: Path to the local file to upload.
        remote_host: The remote host to upload the file to.
        remote_path: The destination path on the remote host.
        user_name: The username for SCP authentication.
        private_key: Path to the private key for SCP authentication.

    Raises:
        RuntimeError: If the file upload fails.
    """
    remote_path = os.path.normpath(remote_path)

    if "../" in remote_path:
        raise ValueError(f"Remote path {remote_path} contains directory traversal sequences (e.g., ../)")

    if remote_path in UNSAFE:
        raise ValueError(f"Remote path {remote_path} is outside allowed directory")

    scp_command = [
        "scp",
        "-i",
        private_key,
        local_file,
        f"{user_name}@{remote_host}:{remote_path}",
    ]
    try:
        subprocess.check_call(scp_command, stdout=subprocess.DEVNULL)
    except subprocess.CalledProcessError as e:
        raise RuntimeError(f"Failed to upload file to {remote_host}: {e}") from e


def scp_download(remote_host, remote_path, local_path, user_name, private_key):
    """Download a file/directory from a remote host via SCP.

    Args:
        remote_host: The remote host IP or hostname.
        remote_path: The source path on the remote host.
        local_path: The destination path on local machine.
        user_name: The username for SCP authentication.
        private_key: Path to the private key for SCP authentication.

    Raises:
        RuntimeError: If the file download fails.
    """
    os.makedirs(local_path, exist_ok=True)
    scp_command = [
        "scp",
        "-r",
        "-i",
        private_key,
        f"{user_name}@{remote_host}:{remote_path}",
        local_path,
    ]
    try:
        subprocess.check_call(scp_command, stdout=subprocess.DEVNULL)
    except subprocess.CalledProcessError as e:
        raise RuntimeError(f"Failed to download from {remote_host}: {e}") from e


def get_timestamped_path(original_path: str) -> str:
    """
    Generate a timestamped version of the given path if it starts with "./yr_datasystem".

    Args:
        original_path (str): The original path string to process.

    Returns:
        str: The modified path with a timestamp appended if it starts with "./yr_datasystem",
             otherwise returns the original path.
    """
    if original_path.startswith("./yr_datasystem"):
        return original_path.replace(
            "./yr_datasystem",
            f"./yr_datasystem{datetime.now(tz=timezone.utc).strftime('%Y%m%d%H%M%S')}",
            1,
        )
    return original_path


def compare_and_process_config(
    home_dir: str, config: Dict[str, Any], default_config: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Compare and process the user configuration with the default configuration.

    Args:
        home_dir (str): The home directory path used to resolve relative paths.
        config (Dict[str, Any]): The user configuration dictionary to be processed.
        default_config (Dict[str, Any]): The default configuration dictionary for comparison.

    Returns:
        Dict[str, Any]: A dictionary containing only the keys that were modified by the user.
    """
    modified = {}
    for key, item in default_config.items():
        default_value = str(item.get("value", ""))
        user_item = config.setdefault(key, {})
        user_value = str(user_item.get("value", ""))

        is_modified = user_value.strip() and str(default_value) != str(user_value)
        if is_modified:
            modified[key] = user_value
        else:
            user_item["value"] = default_value
            user_value = default_value

        if user_value.startswith("./"):
            if "../" in user_value:
                raise RuntimeError(f"Invalid path in config value: {user_value}")
            if home_dir:
                user_item["value"] = os.path.join(home_dir, user_value[2:])
            elif is_modified:
                user_item["value"] = os.path.realpath(user_value)
            else:
                user_item["value"] = os.path.realpath(get_timestamped_path(user_value))

    return modified


def get_commit_id():
    """Reads and returns the git commit ID from a file."""
    try:
        with importlib.resources.path("yr.datasystem", ".commit_id") as commit_file_path:
            with open(commit_file_path, "r") as f:
                content = f.read().strip()
                return _extract_commit_id(content)
        return "unknown"
    except Exception:
        return "unknown"


def _extract_commit_id(content):
    """Extract commit ID from content string."""
    if not content:
        return "unknown"
    parts = content.split("=", 1)
    if len(parts) != 2:
        return "unknown"
    commit_info_str = parts[1].strip()
    if not commit_info_str or commit_info_str == "unknown":
        return "unknown"
    is_single_quoted = commit_info_str.startswith("'") and commit_info_str.endswith("'")
    is_double_quoted = commit_info_str.startswith('"') and commit_info_str.endswith('"')
    if is_single_quoted or is_double_quoted:
        commit_info_str = commit_info_str[1:-1]
    start_idx = commit_info_str.find("[")
    end_idx = commit_info_str.find("]", start_idx)
    if start_idx != -1 and end_idx != -1:
        return commit_info_str[start_idx + 1 : end_idx]
    return "unknown"
