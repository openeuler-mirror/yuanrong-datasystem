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
"""YuanRong datasystem CLI start command."""

import argparse
import json
import os
import shutil
import signal
import socket
import subprocess
import time
from typing import Any, Dict, Optional

import yr.datasystem.cli.common.util as util
from yr.datasystem.cli.command import BaseCommand


class Command(BaseCommand):
    """
    Start yuanrong datasystem worker or coordinator service.
    """

    name = "start"
    description = "startup yuanrong datasystem worker or coordinator service"

    _DEFAULT_WORKER_ADDRESS = "127.0.0.1:31501"
    _DEFAULT_TIMEOUT = 90
    _SERVICE_TYPE_KEY = "service_type"
    _WORKER_SERVICE = "worker"
    _COORDINATOR_SERVICE = "coordinator"
    _DSCLI_ONLY_PARAMS = {_SERVICE_TYPE_KEY}
    _COORDINATOR_FILTERED_PARAMS = {_SERVICE_TYPE_KEY, "worker_address"}
    _NUMACTL_OPTION_TOKENS = {
        "-N",
        "-C",
        "-m",
        "-i",
        "-p",
        "-l",
        "--cpunodebind",
        "--physcpubind",
        "--membind",
        "--interleave",
        "--preferred",
        "--localalloc",
    }
    _READY_CONNECT_TIMEOUT_SECONDS = 1.0
    _CLEANUP_WAIT_SECONDS = 2.0
    _CLEANUP_CHECK_INTERVAL_SECONDS = 0.1
    _KV_EVENTS_CONFIG_PARAM = "kv_events_config"

    def __init__(self):
        """Initialize command instance."""
        super().__init__()
        self._home_dir = ""
        self._timeout = self._DEFAULT_TIMEOUT

    def add_arguments(self, parser):
        """
        Add arguments to parser.

        Args:
            parser (ArgumentParser): Specify parser to which arguments are added.
        """
        group = parser.add_mutually_exclusive_group(required=True)
        parser.add_argument(
            "-t",
            "--timeout",
            type=int,
            default=self._DEFAULT_TIMEOUT,
            metavar="SECONDS",
            help=(
                "Maximum time to wait for service to be ready "
                f"(default: {self._DEFAULT_TIMEOUT} seconds)"
            ),
        )

        group.add_argument(
            "-f",
            "--config_path",
            "--worker_config_path",
            dest="config_path",
            metavar="FILE",
            help=(
                "start service by using configuration file (JSON format), "
                "which can be obtained through the generate_config command"
            ),
        )

        group.add_argument(
            "-w",
            "--worker_args",
            nargs=argparse.REMAINDER,
            help=(
                "start worker by using command line arguments, "
                "e.g, --worker_address '127.0.0.1:31501' --coordinator_address '127.0.0.1:31511'"
            ),
        )
        group.add_argument(
            "--coordinator_args",
            nargs=argparse.REMAINDER,
            help=(
                "start coordinator by using command line arguments, "
                "e.g, --coordinator_address '127.0.0.1:31511'"
            ),
        )
        group.add_argument(
            "--coordinator_worker_args",
            dest="coordinator_worker_args",
            nargs=argparse.REMAINDER,
            help=(
                "start coordinator and worker by using command line arguments, "
                "e.g, --worker_address '127.0.0.1:31501' --coordinator_address '127.0.0.1:31511'"
            ),
        )

        parser.add_argument(
            "-d",
            "--datasystem_home_dir",
            metavar="DIR",
            help=(
                "replace leading '.' in default configuration paths with this directory, "
                "e.g. if the configuration is './yr_datasystem/log_dir', "
                "the '.' will be replaced with the datasystem_home_dir."
            ),
        )

        parser.add_argument(
            "--enable_ums",
            action="store_true",
            default=False,
            help=(
                "Enable UMS, if enabled, the RPC messages between datasystem workers will be transmitted through ub "
                "(default: False)"
            ),
        )

        ng = parser.add_argument_group(
            "numactl options (optional, passed straight to numactl)"
        )
        ng.add_argument(
            "-N",
            "--cpunodebind",
            metavar="NODES",
            help="Restricts process execution to only the CPUs belonging to the specified NUMA node(s).",
        )
        ng.add_argument(
            "-C",
            "--physcpubind",
            metavar="CPUS",
            help="Binds the process to specific physical CPU cores by their numeric IDs.",
        )
        ng.add_argument(
            "-i",
            "--interleave",
            metavar="NODES",
            help="Sets a memory interleaving policy that round-robins page allocations "
            "across the specified NUMA node(s) in numeric order.",
        )
        ng.add_argument(
            "-p",
            "--preferred",
            metavar="NODE",
            help="Establishes a preferred NUMA node for memory allocation. The kernel will "
            "first attempt to allocate memory on this node, but will fall back to other "
            "nodes if insufficient memory is available.",
        )
        ng.add_argument(
            "-m",
            "--membind",
            metavar="NODES",
            help="Enforces a strict memory binding policy that permits allocation only from "
            "the specified NUMA node(s). If memory cannot be allocated on these nodes, "
            "the allocation fails.",
        )
        ng.add_argument(
            "-l",
            "--localalloc",
            action="store_true",
            default=None,
            help="Sets memory allocation to occur on the NUMA node where the allocating CPU "
            'resides (the "local node"). If the local node has no free memory, the '
            "kernel will fall back to nearby nodes.",
        )

    def run(self, args):
        """
        Execute for start command.

        Args:
            args (Namespace): Parsed arguments to hold customized parameters.

        Raises:
            Exception: If any error occurs during startup, an exception is raised with error details.
        """
        numactl_opts = self.collect_numactl_options(args)
        use_numactl = any(v is not None for v in numactl_opts.values())
        try:
            self.reject_numactl_options_after_service_args(args)
            if args.datasystem_home_dir:
                home_dir = os.path.abspath(os.path.expanduser(args.datasystem_home_dir))
                self._home_dir = util.valid_safe_path(home_dir)
            self._timeout = args.timeout

            if args.config_path:
                service_type = self.load_service_type(args.config_path)
                params = self.load_config(args.config_path, service_type)
                if service_type == self._COORDINATOR_SERVICE:
                    self.start_coordinator(params)
                else:
                    params.setdefault("worker_address", self._DEFAULT_WORKER_ADDRESS)
                    self.start_worker(
                        params, args.enable_ums, use_numactl, numactl_opts
                    )
            elif args.coordinator_args:
                params = self.parse_cli_args(
                    args.coordinator_args, fill_worker_defaults=False
                )
                self.start_coordinator(params)
            elif args.coordinator_worker_args:
                params = self.parse_cli_args(
                    args.coordinator_worker_args, fill_worker_defaults=False
                )
                coordinator_params, worker_params = (
                    self.split_coordinator_worker_params(params)
                )
                coordinator_pid = self.start_coordinator(coordinator_params)
                try:
                    self.start_worker(
                        worker_params, args.enable_ums, use_numactl, numactl_opts
                    )
                except Exception:
                    self.kill_started_process(coordinator_pid)
                    raise
            elif args.worker_args:
                params = self.parse_cli_args(args.worker_args)
                params.setdefault("worker_address", self._DEFAULT_WORKER_ADDRESS)
                self.start_worker(params, args.enable_ums, use_numactl, numactl_opts)
        except Exception as e:
            self.logger.error(f"Start failed: {e}")
            return self.FAILURE
        return self.SUCCESS

    def collect_numactl_options(self, args):
        """Collect numactl options from parsed args."""
        numactl_opts = {}
        for k in [
            "cpunodebind",
            "physcpubind",
            "interleave",
            "preferred",
            "membind",
            "localalloc",
        ]:
            v = getattr(args, k)
            if v is not None:
                numactl_opts[k] = v
        return numactl_opts

    def reject_numactl_options_after_service_args(self, args):
        """Reject numactl tokens placed after REMAINDER service argument markers."""
        illegal = []
        for service_args in (
            args.worker_args,
            args.coordinator_args,
            args.coordinator_worker_args,
        ):
            for tok in service_args or []:
                if tok in self._NUMACTL_OPTION_TOKENS:
                    illegal.append(tok)
        if illegal:
            self.logger.error(
                "numactl options must be placed before service arguments. "
                f"Found illegal token(s): {', '.join(illegal)}"
            )
            raise ValueError("numactl options must be placed before service arguments")

    def load_json_config(self, config_path: str) -> Dict[str, Any]:
        """Load a JSON config file after path validation."""
        config_path = os.path.realpath(os.path.expanduser(config_path))
        config_path = util.valid_safe_path(config_path)
        try:
            with open(config_path, "r") as f:
                return json.load(f)
        except json.JSONDecodeError as e:
            raise ValueError("The configuration file format is incorrect.") from e

    def load_service_type(self, config_path: str) -> str:
        """Load service_type from config. Missing or empty service_type means worker."""
        config = self.load_json_config(config_path)
        return self.get_service_type(config)

    def get_service_type(self, config: Dict[str, Any]) -> str:
        """Resolve service type from a parsed config object."""
        service_type = self.get_config_value(config, self._SERVICE_TYPE_KEY)
        if service_type in (None, ""):
            return self._WORKER_SERVICE
        service_type = str(service_type).strip().lower()
        if service_type not in (self._WORKER_SERVICE, self._COORDINATOR_SERVICE):
            raise ValueError("service_type must be coordinator or worker")
        return service_type

    def get_config_value(self, config: Dict[str, Any], key: str):
        """Get a config value supporting both raw and {'value': ...} forms."""
        config_item = config.get(key)
        if isinstance(config_item, dict):
            value = config_item.get("value")
            if value not in ("", None):
                return value
            return config_item.get("default")
        return config_item

    def load_config(
        self, config_path: str, service_type: Optional[str] = None
    ) -> Dict[str, str]:
        """
        Load the configuration file and extract necessary parameters.

        Args:
            config_path (str): Path to the configuration file.
            service_type (str): Resolved service type. Missing means infer from config.

        Returns:
            Dict[str, str]: Dictionary containing extracted parameters.

        Raises:
            ValueError: If the configuration file format is incorrect.
        """
        config = self.load_json_config(config_path)
        service_type = service_type or self.get_service_type(config)
        if service_type == self._COORDINATOR_SERVICE:
            return self.config_to_params(config, fill_defaults=False)

        default_config_path = os.path.join(self._base_dir, "worker_config.json")
        default_config_path = util.valid_safe_path(default_config_path)
        try:
            with open(default_config_path, "r") as f:
                default_config = json.load(f)
        except json.JSONDecodeError as e:
            raise ValueError("The configuration file format is incorrect.") from e

        modified = util.compare_and_process_config(
            self._home_dir, config, default_config
        )
        for key, _ in modified.items():
            self.logger.info(f"Modifed config - {key}")
        return self.config_to_params(config, fill_defaults=False)

    def config_to_params(
        self, config: Dict[str, Any], fill_defaults: bool = False
    ) -> Dict[str, str]:
        """Convert dscli JSON config entries to process parameters."""
        params = {}
        for flag, conf in config.items():
            if flag in self._DSCLI_ONLY_PARAMS or not str(flag).strip():
                continue
            if isinstance(conf, dict):
                value = conf.get("value", "")
                if fill_defaults and value in (None, ""):
                    value = conf.get("default", "")
            else:
                value = conf
            params[flag] = str(value).strip()
            if flag == "log_dir" and params[flag]:
                self.logger.info(f"Log directory configured at: {params[flag]}")
        return params

    def get_default_config_keys(self, config_file: str) -> set:
        """Load process parameter keys from a default dscli config file."""
        config_path = os.path.join(self._base_dir, config_file)
        config = self.load_json_config(config_path)
        return {
            str(key)
            for key in config.keys()
            if key not in self._DSCLI_ONLY_PARAMS and str(key).strip()
        }

    def split_coordinator_worker_params(self, params: Dict[str, str]):
        """Split combined CLI params by coordinator and worker config definitions."""
        coordinator_keys = self.get_default_config_keys("coordinator_config.json")
        worker_keys = self.get_default_config_keys("worker_config.json")
        unknown_keys = sorted(
            key
            for key in params.keys()
            if key not in self._DSCLI_ONLY_PARAMS
            and key not in coordinator_keys
            and key not in worker_keys
        )
        if unknown_keys:
            raise ValueError(
                f"Unknown coordinator/worker parameter(s): {', '.join(unknown_keys)}"
            )

        coordinator_params = {
            key: value for key, value in params.items() if key in coordinator_keys
        }
        worker_params = {
            key: value for key, value in params.items() if key in worker_keys
        }
        self.fill_params(worker_params)
        worker_params.setdefault("worker_address", self._DEFAULT_WORKER_ADDRESS)
        return coordinator_params, worker_params

    def parse_cli_args(
        self, cli_args: list, fill_worker_defaults: bool = True
    ) -> Dict[str, str]:
        """
        Parse command line arguments into a dictionary.

        Args:
            cli_args (list): List of command line arguments.
            fill_worker_defaults (bool): Whether to fill worker defaults.

        Returns:
            Dict[str, str]: Dictionary containing parsed parameters.

        Raises:
            ValueError: If there is a mismatch between parameter names and values.
        """
        params = {}
        current_flag = None

        for arg in cli_args:
            if arg.startswith("--"):
                if current_flag:
                    raise ValueError(f"Param {current_flag} is missing a value")
                current_flag = arg[2:]
            else:
                if not current_flag:
                    raise ValueError(f"No parameter name specified: {arg}")
                params[current_flag] = arg
                current_flag = None

        if current_flag:
            raise ValueError(f"Param {current_flag} is missing a value")

        if fill_worker_defaults:
            self.fill_params(params)
        return params

    def fill_params(self, params: Dict[str, str]):
        """Fill the parameters with default values from the worker configuration file.

        Args:
            params: Dictionary to be filled with default parameters.

        Raises:
            ValueError: If the configuration file format is incorrect.
        """
        default_config_path = os.path.join(self._base_dir, "worker_config.json")
        try:
            with open(default_config_path, "r") as f:
                default_config = json.load(f)
        except json.JSONDecodeError as e:
            raise ValueError("The configuration file format is incorrect.") from e
        for key, item in default_config.items():
            if key in self._DSCLI_ONLY_PARAMS or key in params:
                continue
            params[key] = str(item.get("value", ""))
            if not params[key].startswith("./"):
                continue
            if self._home_dir:
                params[key] = os.path.join(self._home_dir, params[key][2:])
            else:
                params[key] = os.path.realpath(util.get_timestamped_path(params[key]))
        if params.get("log_dir"):
            self.logger.info(f"Log directory configured at: {params['log_dir']}")

    def validate_worker_backend_params(self, params: Dict[str, str]):
        """Validate the worker coordination backend selection."""
        configured_backends = [
            name
            for name in ("coordinator_address", "etcd_address", "metastore_address")
            if params.get(name)
        ]
        if not configured_backends:
            raise ValueError(
                "Missing required: coordinator_address, etcd_address, or metastore_address must be specified"
            )
        if len(configured_backends) > 1:
            raise ValueError("Only one coordination backend can be specified")

    def start_worker(
        self,
        params: Dict[str, str],
        use_ums: bool,
        use_numactl: bool = False,
        numactl_opts: Optional[Dict[str, Any]] = None,
    ):
        """
        Start the datasystem worker service with specified parameters.

        Args:
            params (Dict[str, str]): Dictionary containing worker configuration parameters.
            use_ums: bool , true when params contain numactl parameters
            numactl_opts: numactl options dict
        Raises:
            ValueError: If required parameters are missing.
            RuntimeError: If the worker service fails to start or exits abnormally.
        """
        self.validate_worker_backend_params(params)

        cmd = self.build_command(params, use_ums, use_numactl, numactl_opts)
        lib_dir = os.path.join(self._base_dir, "lib")
        env = os.environ.copy()
        env["LD_LIBRARY_PATH"] = f"{lib_dir}:{env.get('LD_LIBRARY_PATH', '')}"
        try:
            ready_check_path = params.get("ready_check_path")
            if not ready_check_path:
                raise RuntimeError("ready_check_path is empty")
            ready_check_path = os.path.abspath(ready_check_path)
            ready_check_path = self.valid_safe_path(ready_check_path)
            if os.path.exists(ready_check_path) and os.path.isfile(ready_check_path):
                os.remove(ready_check_path)
            process = subprocess.Popen(
                cmd,
                env=env,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                start_new_session=True,
            )
            self.logger.info(f"Starting worker service with PID: {process.pid}")
            for _ in range(self._timeout):
                return_code = process.poll()
                if return_code is not None:
                    stdout, stderr = process.communicate(timeout=10)
                    self.logger.error(
                        f"[  FAILED  ] Worker exited with code {return_code}\n output: {stdout + stderr}"
                    )
                    raise RuntimeError(
                        f"Worker service exited abnormally with code {return_code}"
                    )
                if os.path.exists(ready_check_path):
                    self.logger.info(
                        "[  OK  ] Start worker service @ {} success, PID: {}".format(
                            params["worker_address"], process.pid
                        )
                    )
                    break
                time.sleep(1)
            else:
                self.logger.error(
                    f"[  FAILED  ] Worker service is not ready within {self._timeout} seconds"
                )
                try:
                    os.kill(process.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
                raise RuntimeError("Worker service startup timeout")

        except Exception as e:
            self.logger.error(
                "[  FAILED  ] Start worker service @ {} failed: {}".format(
                    params["worker_address"], e
                )
            )
            raise RuntimeError("The worker service exited abnormally") from e

    def start_coordinator(self, params: Dict[str, str]) -> int:
        """Start the datasystem coordinator service and wait until it accepts connections."""
        coordinator_address = params.get("coordinator_address")
        if not coordinator_address:
            raise ValueError("Missing required: coordinator_address must be specified")
        util.is_valid_address_port(coordinator_address)

        if self.is_tcp_ready(coordinator_address):
            raise RuntimeError(f"Coordinator address {coordinator_address} is already in use")

        cmd = self.build_coordinator_command(params)
        lib_dir = os.path.join(self._base_dir, "lib")
        env = os.environ.copy()
        env["LD_LIBRARY_PATH"] = f"{lib_dir}:{env.get('LD_LIBRARY_PATH', '')}"
        try:
            process = subprocess.Popen(
                cmd,
                env=env,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                start_new_session=True,
            )
            self.logger.info(f"Starting coordinator service with PID: {process.pid}")
            for _ in range(self._timeout):
                return_code = process.poll()
                if return_code is not None:
                    stdout, stderr = process.communicate(timeout=10)
                    self.logger.error(
                        f"[  FAILED  ] Coordinator exited with code {return_code}\n output: {stdout + stderr}"
                    )
                    raise RuntimeError(
                        f"Coordinator service exited abnormally with code {return_code}"
                    )
                if self.is_tcp_ready(coordinator_address):
                    return_code = process.poll()
                    if return_code is not None:
                        stdout, stderr = process.communicate(timeout=10)
                        self.logger.error(
                            f"[  FAILED  ] Coordinator exited with code {return_code}\n output: {stdout + stderr}"
                        )
                        raise RuntimeError(
                            f"Coordinator service exited abnormally with code {return_code}"
                        )
                    self.logger.info(
                        "[  OK  ] Start coordinator service @ {} success, PID: {}".format(
                            coordinator_address, process.pid
                        )
                    )
                    return process.pid
                time.sleep(1)
            self.logger.error(
                f"[  FAILED  ] Coordinator service is not ready within {self._timeout} seconds"
            )
            try:
                os.kill(process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            raise RuntimeError("Coordinator service startup timeout")
        except Exception as e:
            self.logger.error(
                "[  FAILED  ] Start coordinator service @ {} failed: {}".format(
                    coordinator_address, e
                )
            )
            raise RuntimeError("The coordinator service exited abnormally") from e

    def is_tcp_ready(self, address: str) -> bool:
        """Return whether a host:port address accepts a TCP connection."""
        host, port = self.split_address(address)
        try:
            with socket.create_connection(
                (host, port), timeout=self._READY_CONNECT_TIMEOUT_SECONDS
            ):
                return True
        except OSError:
            return False

    def split_address(self, address: str):
        """Split an IPv4 host:port address."""
        pos = address.rfind(":")
        return address[:pos].strip("[]"), int(address[pos + 1 :])

    def kill_started_process(self, pid: Optional[int]):
        """Best-effort cleanup for a process started by this command."""
        if not pid:
            return
        try:
            os.kill(pid, signal.SIGTERM)
        except ProcessLookupError:
            return
        except PermissionError as e:
            self.logger.warning(
                f"Failed to stop coordinator after worker startup failure: {e}"
            )
            return

        deadline = time.monotonic() + self._CLEANUP_WAIT_SECONDS
        while time.monotonic() < deadline:
            try:
                os.kill(pid, 0)
            except ProcessLookupError:
                return
            except PermissionError:
                break
            time.sleep(self._CLEANUP_CHECK_INTERVAL_SECONDS)

        try:
            os.kill(pid, signal.SIGKILL)
        except ProcessLookupError:
            return
        except PermissionError as e:
            self.logger.warning(
                f"Failed to force stop coordinator after worker startup failure: {e}"
            )

    def process_params(self, params: Dict[str, str], excluded_params=None):
        """Yield validated process parameters excluding dscli-only keys."""
        excluded = set(excluded_params or set()) | self._DSCLI_ONLY_PARAMS
        for k, v in params.items():
            if k in excluded or not str(v).strip():
                continue
            yield util.validate_no_injection(k), util.validate_no_injection(v)

    def build_command(
        self,
        params: Dict[str, str],
        use_ums: bool,
        use_numactl: bool = False,
        numactl_opts: Optional[Dict[str, Any]] = None,
    ) -> list:
        """
        Construct the command line parameters for starting the worker.

        Args:
            params (Dict[str, str]): Dictionary containing worker configuration parameters.

        Returns:
            list: List of command line arguments.
        """
        cmd = []
        if use_ums:
            ums_run_path = shutil.which("ums_run")
            if not ums_run_path:
                raise RuntimeError(
                    "ums_run is not installed on this host. Please install it first"
                )
            cmd.append(ums_run_path)
        if use_numactl:
            numactl_path = shutil.which("numactl")
            if not numactl_path:
                raise RuntimeError(
                    "numactl is not installed on this host. Please install it first"
                )
            cmd.append("numactl")
            for key in [
                "cpunodebind",
                "physcpubind",
                "interleave",
                "preferred",
                "membind",
            ]:
                val = (numactl_opts or {}).get(key)
                if val is not None:
                    val = util.validate_no_injection(str(val))
                    cmd.append(f"--{key}={val}")
            if (numactl_opts or {}).get("localalloc"):
                cmd.append("--localalloc")
        worker_bin = util.validate_no_injection(
            os.path.abspath(os.path.join(self._base_dir, "datasystem_worker"))
        )
        cmd.append(worker_bin)

        for k, v in self.process_params(params):
            if k == self._KV_EVENTS_CONFIG_PARAM:
                v = self.validate_worker_param_value(k, v)
            cmd.append(f"--{k}={v}")
        cmd_str = " ".join(cmd)
        if use_numactl:
            self.logger.info(f"Starting with numactl command: {cmd_str}")
        return cmd

    def build_coordinator_command(self, params: Dict[str, str]) -> list:
        """Construct the command line parameters for starting the coordinator."""
        coordinator_bin = util.validate_no_injection(
            os.path.abspath(os.path.join(self._base_dir, "datasystem_coordinator"))
        )
        cmd = [coordinator_bin]
        for k, v in self.process_params(params, self._COORDINATOR_FILTERED_PARAMS):
            cmd.append(f"--{k}={v}")
        return cmd

    def validate_worker_param_value(self, key: str, value: str) -> str:
        if key != self._KV_EVENTS_CONFIG_PARAM:
            return util.validate_no_injection(value)
        try:
            config = json.loads(value)
        except json.JSONDecodeError as e:
            raise ValueError("kv_events_config must be a valid JSON object string") from e
        if not isinstance(config, dict):
            raise ValueError("kv_events_config must be a JSON object")
        for item in config.values():
            if isinstance(item, str):
                util.validate_no_injection(item)
        return value
