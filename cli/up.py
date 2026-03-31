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
"""YuanRong datasystem CLI up command."""

import json
import os
import shlex
from typing import Any, Dict

import yr.datasystem.cli.common.util as util
from yr.datasystem.cli.command import BaseCommand
from yr.datasystem.cli.common.constant import ClusterConfig
from yr.datasystem.cli.common.parallel import ParallelMixin


class Command(BaseCommand, ParallelMixin):
    """
    Startup yuanrong datasystem worker on cluster nodes.
    """

    name = "up"
    description = "startup yuanrong datasystem worker on cluster nodes"

    _DEFAULT_TIMEOUT = 90

    def __init__(self):
        """Initialize command instance."""
        super().__init__()
        self._config = {}
        self._home_dir = ""
        self._hidden_config_path = ""
        self._timeout = self._DEFAULT_TIMEOUT

    def add_arguments(self, parser):
        """
        Add arguments to parser.

        Args:
            parser (ArgumentParser): Specify parser to which arguments are added.
        """
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

        parser.add_argument(
            "-f",
            "--cluster_config_path",
            metavar="FILE",
            required=True,
            help=(
                "path of cluster configuration file (JSON format), "
                "which can be obtained through the generate_config command"
            ),
        )

        parser.add_argument(
            "-d",
            "--datasystem_home_dir",
            metavar="DIR",
            help=(
                "directory to replace the current paths in the configuration, "
                "e.g. if the config contains './yr_datasystem/log_dir', "
                "'.' will be replaced with the datasystem_home_dir."
            ),
        )

        parser.add_argument(
            "--enable_ums",
            action="store_true",
            default=False,
            help=(
                "Enable UMS, if enabled, the RPC messages between datasystem workers will be transmitted through ub (default: False)"
            ),
        )

        parser.add_argument(
            "--metastore_head_node",
            metavar="NODE",
            help=(
                "Specify which node is the head node (starts metastore service). "
                "Metastore is used to replace external etcd for metadata storage. "
                "If not specified, use external etcd for cluster coordination."
            ),
        )

        ng = parser.add_argument_group("numactl options (optional, passed straight to numactl)")
        ng.add_argument(
            "-N", "--cpunodebind",
            metavar="NODES",
            help="Restricts process execution to only the CPUs belonging to the specified NUMA node(s)."
        )
        ng.add_argument(
            "-C",
            "--physcpubind",
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
        Execute for up command.

        Args:
            args (Namespace): Parsed arguments to hold customized parameters.

        Returns:
            int: Exit code, 0 for success, 1 for failure.
        """
        try:
            self._config = util.load_cluster_config(args.cluster_config_path)
            ssh_key_path = os.path.realpath(
                os.path.expanduser(self._config[ClusterConfig.SSH_PRIVATE_KEY])
            )
            self._config[ClusterConfig.SSH_PRIVATE_KEY] = util.valid_safe_path(ssh_key_path)
            if args.datasystem_home_dir:
                home_dir = os.path.realpath(
                    os.path.expanduser(args.datasystem_home_dir)
                )
                self._home_dir = util.valid_safe_path(home_dir)

            metastore_head_node = self._validate_metastore_config(args)

            numactl_opts = {}
            for k in [
                "cpunodebind", "physcpubind", "interleave",
                "preferred", "membind", "localalloc"
            ]:
                v = getattr(args, k)
                if v is not None:
                    numactl_opts[k] = v
            use_numactl = any(v is not None for v in numactl_opts.values())
            self.update_worker_config()
            self._timeout = args.timeout

            worker_nodes = self._config[ClusterConfig.WORKER_NODES]

            # If using metastore mode, start head node first, then start other nodes in parallel
            if metastore_head_node:
                self.logger.info(f"Starting metastore head node: {metastore_head_node}")
                self.process_node(
                    metastore_head_node,
                    use_ums=args.enable_ums,
                    use_numactl=use_numactl,
                    numactl_opts=numactl_opts,
                )

                # Get worker nodes excluding head node
                other_nodes = [node for node in worker_nodes if node != metastore_head_node]
                if other_nodes:
                    self.logger.info(f"Starting other worker nodes in parallel: {other_nodes}")
                    self.execute_parallel(
                        other_nodes,
                        use_ums=args.enable_ums,
                        use_numactl=use_numactl,
                        numactl_opts=numactl_opts,
                    )
            else:
                # Using etcd mode, start all nodes in parallel
                self.execute_parallel(
                    worker_nodes,
                    use_ums=args.enable_ums,
                    use_numactl=use_numactl,
                    numactl_opts=numactl_opts,
                )
        except Exception as e:
            self.logger.error(f"Up cluster failed: {e}")
            return self.FAILURE
        return self.SUCCESS

    def _validate_metastore_config(self, args):
        """
        Validate metastore configuration.

        Gets metastore_head_node from CLI args or cluster config and validates:
        - metastore_head_node must be one of the worker_nodes
        - metastore_address must be configured in worker_config.json

        Args:
            args (Namespace): Parsed arguments.

        Returns:
            str: The metastore_head_node value (empty string if not specified).

        Raises:
            ValueError: If metastore configuration validation fails.
        """
        metastore_head_node = getattr(args, "metastore_head_node", None)
        if metastore_head_node is None:
            metastore_head_node = self._config.get(ClusterConfig.METASTORE_HEAD_NODE, "")
        self._config["metastore_head_node"] = metastore_head_node

        # Validation: if metastore_head_node_node is specified
        if metastore_head_node:
            worker_nodes = self._config[ClusterConfig.WORKER_NODES]
            if metastore_head_node not in worker_nodes:
                raise ValueError(
                    f"metastore_head_node '{metastore_head_node}' must be one of worker_nodes: {worker_nodes}"
                )

            # Validation: metastore_address must be configured in worker_config.json
            worker_config_path = os.path.realpath(
                os.path.expanduser(self._config[ClusterConfig.WORKER_CONFIG_PATH])
            )
            try:
                with open(worker_config_path, "r") as f:
                    worker_config = json.load(f)
            except (json.JSONDecodeError, IOError) as e:
                raise ValueError(f"Failed to read worker_config.json: {e}") from e

            metastore_address = worker_config.get("metastore_address", {}).get("value", "")
            if not metastore_address:
                raise ValueError(
                    "metastore_address must be configured in worker_config.json "
                    "when metastore_head_node is specified"
                )

        return metastore_head_node

    def process_node(self, node, **kwargs):
        """
        Process startup of worker on a single node.

        Args:
            node (str): The node to start the worker on.
        """
        user_name = self._config[ClusterConfig.SSH_USER_NAME]
        private_key = self._config[ClusterConfig.SSH_PRIVATE_KEY]
        worker_port = self._config[ClusterConfig.WORKER_PORT]

        use_numactl = kwargs.get("use_numactl", False)
        numactl_opts = kwargs.get("numactl_opts") or {}
        use_ums = kwargs.get("use_ums", False)

        self._hidden_config_path = util.validate_no_injection(self._hidden_config_path)
        util.ssh_execute(
            node,
            user_name,
            private_key,
            f"mkdir -p -- {shlex.quote(os.path.dirname(self._hidden_config_path))}",
        )

        # Upload the modified worker config to remote
        util.scp_upload(
            self._hidden_config_path,
            node,
            self._hidden_config_path,
            user_name,
            private_key,
        )

        # Update worker_address
        is_ipv6 = util.is_valid_ip(node)
        node_arg = node
        if is_ipv6:
            node_arg = "[" + node + "]"
        util.is_valid_port(worker_port)
        sed_command = (
            r"sed -i "
            r'"/\"worker_address\"/,/}/ s/\"value\"\s*:\s*\"[^\"]*\"/\"value\": \"%s\"/g" '
            r"%s"
        ) % (f"{node_arg}:{worker_port}", self._hidden_config_path)
        util.ssh_execute(
            node,
            user_name,
            private_key,
            sed_command,
        )

        # Process metastore_head_node configuration (using sed to modify config file)
        metastore_head_node = self._config.get("metastore_head_node", "")
        if metastore_head_node:
            # Determine if current node is the head node
            start_metastore_service = "true" if node == metastore_head_node else "false"
            # Use sed to modify start_metastore_service value in the remote config file
            sed_command = (
                r"sed -i "
                r'"/\"start_metastore_service\"/,/}/ s/\"value\"\s*:\s*\"[^\"]*\"/\"value\": \"%s\"/g" '
                r"%s"
            ) % (start_metastore_service, self._hidden_config_path)
            util.ssh_execute(node, user_name, private_key, sed_command)
            self.logger.info(f"Setting start_metastore_service={start_metastore_service} for node: {node}")

        remote_cmd = self.build_remote_start_cmd(
            self._hidden_config_path, use_ums, use_numactl, numactl_opts
        )
        if use_numactl:
            self.logger.info(f"Starting with numactl command: {remote_cmd}")
        util.ssh_execute(node, user_name, private_key, f"bash -l -c {shlex.quote(remote_cmd)}")
        self.logger.info(f"Start worker service @ {node}:{worker_port} success.")

    def build_remote_start_cmd(
        self,
        config_path: str,
        use_ums: bool,
        use_numactl: bool,
        numactl_opts: Dict[str, Any],
    ) -> str:
        """
        Update the remote cmd command to execute.
        """
        if use_ums:
            base_cmd = f"dscli start -t {self._timeout} --enable_ums -f {shlex.quote(config_path)}"
        else:
            base_cmd = f"dscli start -t {self._timeout} -f {shlex.quote(config_path)}"

        if not use_numactl:
            return base_cmd

        cmd_parts = ["numactl"]
        for key in ["cpunodebind", "physcpubind", "interleave",
                    "preferred", "membind"]:
            val = numactl_opts.get(key)
            if val is not None:
                val = util.validate_no_injection(str(val))
                cmd_parts.append(f"--{key}={val}")
        if numactl_opts.get("localalloc"):
            cmd_parts.append("--localalloc")
        cmd_parts.append(base_cmd)
        return " ".join(cmd_parts)

    def update_worker_config(self):
        """
        Update the worker configuration.

        Raises:
            ValueError: If the configuration file format is incorrect.
        """
        config_path = os.path.realpath(
            os.path.expanduser(self._config[ClusterConfig.WORKER_CONFIG_PATH])
        )
        config_path = util.valid_safe_path(config_path)
        default_config_path = os.path.join(self._base_dir, "worker_config.json")
        try:
            with open(config_path, "r") as f:
                config = json.load(f)
            with open(default_config_path, "r") as f:
                default_config = json.load(f)
        except json.JSONDecodeError as e:
            raise ValueError(
                f"The configuration file {config_path} format is incorrect."
            ) from e

        modified = util.compare_and_process_config(self._home_dir, config, default_config)
        for key, _ in modified.items():
            self.logger.info(f"Modifed config - {key}")
        log_dir = config.get("log_dir", {}).get("value", "")
        self.logger.info(f"Log directory configured at: {log_dir}")

        dir_name = os.path.dirname(config_path)
        base_name = os.path.basename(config_path)
        self._hidden_config_path = os.path.join(dir_name, f".{base_name}")
        try:
            with open(self._hidden_config_path, "w") as f:
                json.dump(config, f, indent=4)
        except IOError as e:
            raise ValueError(f"Failed to write to {self._hidden_config_path}.") from e
