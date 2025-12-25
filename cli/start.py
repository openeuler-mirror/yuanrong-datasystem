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
import signal
import subprocess
import time
import shutil
from typing import Dict, Optional, Any

import yr.datasystem.cli.common.util as util
from yr.datasystem.cli.command import BaseCommand


class Command(BaseCommand):
    """
    Start yuanrong datasystem worker service.
    """

    name = "start"
    description = "startup yuanrong datasystem worker service"

    _REQUIRED_PARAMS = ["etcd_address"]
    _DEFAULT_WORKER_ADDRESS = "127.0.0.1:31501"
    _DEFAULT_TIMEOUT = 90

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
                f"Maximum time to wait for worker service to be ready (default: {self._DEFAULT_TIMEOUT} seconds)"
            ),
        )

        group.add_argument(
            "-f",
            "--worker_config_path",
            metavar="FILE",
            help=(
                "start worker by using configuration file (JSON format), "
                "which can be obtained through the generate_config command"
            ),
        )

        group.add_argument(
            "-w",
            "--worker_args",
            nargs=argparse.REMAINDER,
            help=(
                "start worker by using command line arguments, "
                "e.g, --worker_address '127.0.0.1:31501' --etcd_address '127.0.0.1:2379'"
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

        ng = parser.add_argument_group("numactl options (optional, passed straight to numactl)")
        ng.add_argument(
            "-N", "--cpunodebind",
            metavar="NODES",
            help="Restricts process execution to only the CPUs belonging to the specified NUMA node(s)."
        )
        ng.add_argument(
            "-C", "--physcpubind",
            metavar="CPUS",
            help="Binds the process to specific physical CPU cores by their numeric IDs."
        )
        ng.add_argument(
            "-i", "--interleave",
            metavar="NODES",
            help="Sets a memory interleaving policy that round-robins page allocations "
                "across the specified NUMA node(s) in numeric order."
        )
        ng.add_argument(
            "-p", "--preferred",
            metavar="NODE",
            help="Establishes a preferred NUMA node for memory allocation. The kernel will "
                "first attempt to allocate memory on this node, but will fall back to other "
                "nodes if insufficient memory is available."
        )
        ng.add_argument(
            "-m", "--membind",
            metavar="NODES",
            help="Enforces a strict memory binding policy that permits allocation only from "
                "the specified NUMA node(s). If memory cannot be allocated on these nodes, "
                "the allocation fails."
        )
        ng.add_argument(
            "-l", "--localalloc",
            action="store_true",
            default=None,
            help="Sets memory allocation to occur on the NUMA node where the allocating CPU "
                "resides (the \"local node\"). If the local node has no free memory, the "
                "kernel will fall back to nearby nodes."
        )

    def run(self, args):
        """
        Execute for start command.

        Args:
            args (Namespace): Parsed arguments to hold customized parameters.

        Raises:
            Exception: If any error occurs during worker startup, an exception is raised with error details.
        """
        numactl_opts = {}
        for k in [
            "cpunodebind", "physcpubind", "interleave",
            "preferred", "membind", "localalloc"
        ]:
            v = getattr(args, k)
            if v is not None:
                numactl_opts[k] = v
        use_numactl = any(v is not None for v in numactl_opts.values())

        illegal = []
        for tok in args.worker_args or []:
            if tok in {"-N", "-C", "-m", "-i", "-p", "-l", "--cpunodebind",
                       "--physcpubind", "--membind", "--interleave",
                       "--preferred", "--localalloc"}:
                illegal.append(tok)
        if illegal:
            self.logger.error(
                "numactl options must be placed *before* -w/--worker_args.  "
                f"Found illegal token(s) in -w: {', '.join(illegal)}"
            )
            raise ValueError(f"numactl options must be placed before worker arguments")

        final_params = {}
        try:
            if args.datasystem_home_dir:
                home_dir = os.path.abspath(os.path.expanduser(args.datasystem_home_dir))
                self._home_dir = util.valid_safe_path(home_dir)
            if args.worker_config_path:
                final_params = self.load_config(args.worker_config_path)
            elif args.worker_args:
                final_params = self.parse_cli_args(args.worker_args)
            final_params.setdefault("worker_address", self._DEFAULT_WORKER_ADDRESS)
            self._timeout = args.timeout
            self.start_worker(final_params, use_numactl, numactl_opts)
        except Exception as e:
            self.logger.error(f"Start failed: {e}")
            return self.FAILURE
        return self.SUCCESS

    def load_config(self, config_path: str) -> Dict[str, str]:
        """
        Load the configuration file and extract necessary parameters.

        Args:
            config_path (str): Path to the configuration file.

        Returns:
            Dict[str, str]: Dictionary containing extracted parameters.

        Raises:
            ValueError: If the configuration file format is incorrect.
        """
        config_path = os.path.realpath(os.path.expanduser(config_path))
        config_path = util.valid_safe_path(config_path)

        default_config_path = os.path.join(self._base_dir, "worker_config.json")
        default_config_path = util.valid_safe_path(default_config_path)
        try:
            with open(config_path, "r") as f:
                config = json.load(f)
            with open(default_config_path, "r") as f:
                default_config = json.load(f)
        except json.JSONDecodeError as e:
            raise ValueError("The configuration file format is incorrect.") from e

        modified = util.compare_and_process_config(self._home_dir, config, default_config)
        for key, _ in modified.items():
            self.logger.info(f"Modifed config - {key}")
        params = {}
        for flag, conf in config.items():
            if not str(flag).strip():
                continue
            params[flag] = str(conf.get("value", "")).strip()
            if flag == "log_dir":
                self.logger.info(f"Log directory configured at: {params[flag]}")
        return params

    def parse_cli_args(self, cli_args: list) -> Dict[str, str]:
        """
        Parse command line arguments into a dictionary.

        Args:
            cli_args (list): List of command line arguments.

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

        self.fill_params(params)
        return params

    def fill_params(self, params: Dict[str, str]):
        """Fill the parameters with default values from the configuration file.

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
            if key in params:
                continue
            params[key] = str(item.get("value", ""))
            if not params[key].startswith("./"):
                continue
            if self._home_dir:
                params[key] = os.path.join(self._home_dir, params[key][2:])
            else:
                params[key] = os.path.realpath(util.get_timestamped_path(params[key]))
        self.logger.info(f"Log directory configured at: {params['log_dir']}")

    def start_worker(self, params: Dict[str, str],
                     use_numactl: bool = False,
                     numactl_opts: Optional[Dict[str, Any]] = None):
        """
        Start the datasystem worker service with specified parameters.

        Args:
            params (Dict[str, str]): Dictionary containing worker configuration parameters.
            use_numactl: bool , true when params contain numactl parameters
            numactl_opts: numactl options dict
        Raises:
            ValueError: If required parameters are missing.
            RuntimeError: If the worker service fails to start or exits abnormally.
        """
        for param in self._REQUIRED_PARAMS:
            if not params.get(param):
                raise ValueError(f"Missing required parameters: {param}")

        cmd = self.build_command(params, use_numactl, numactl_opts)
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
                    self.logger.error(f"[  FAILED  ] Worker exited with code {return_code}\n output: {stdout + stderr}")
                    raise RuntimeError(f"Worker service exited abnormally with code {return_code}")
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
                raise RuntimeError(f"Worker service startup timeout")

        except Exception as e:
            self.logger.error("[  FAILED  ] Start worker service @ {} failed: {}".format(params["worker_address"], e))
            raise RuntimeError("The worker service exited abnormally") from e

    def build_command(self, params: Dict[str, str],
                        use_numactl: bool = False,
                        numactl_opts: Optional[Dict[str, Any]] = None) -> list:
        """
        Construct the command line parameters for starting the worker.

        Args:
            params (Dict[str, str]): Dictionary containing worker configuration parameters.

        Returns:
            list: List of command line arguments.
        """
        cmd = []
        if use_numactl:
            numactl_path = shutil.which("numactl")
            if not numactl_path:
                raise RuntimeError(
                    "numactl is not installed on this host. "
                    "Please install it first"
                )
            cmd.append("numactl")
            for key in ["cpunodebind", "physcpubind", "interleave",
                        "preferred", "membind"]:
                val = (numactl_opts or {}).get(key)
                if val is not None:
                    val = util.validate_no_injection(str(val))
                    cmd.append(f"--{key}={val}")
            if numactl_opts.get("localalloc"):
                cmd.append("--localalloc")
        worker_bin = util.validate_no_injection(
            os.path.abspath(os.path.join(self._base_dir, "datasystem_worker")))
        cmd.append(worker_bin)

        for k, v in params.items():
            if str(v).strip():
                k = util.validate_no_injection(k)
                v = util.validate_no_injection(v)
                cmd.append(f"--{k}={v}")
        return cmd
