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
import os

import yr.datasystem.cli.common.util as util
from yr.datasystem.cli.command import BaseCommand
from yr.datasystem.cli.common.constant import ClusterConfig
from yr.datasystem.cli.common.parallel import ParallelMixin


class Command(BaseCommand, ParallelMixin):
    """
    Stop yuanrong datasystem worker on cluster nodes.
    """

    name = "down"
    description = "stop yuanrong datasystem worker on cluster nodes"

    _config = {}

    def add_arguments(self, parser):
        """
        Add arguments to parser.

        Args:
            parser (ArgumentParser): Specify parser to which arguments are added.
        """
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

    def run(self, args):
        """
        Execute for down command.

        Args:
            args (Namespace): Parsed arguments to hold customized parameters.

        Returns:
            int: Exit code, 0 for success, 1 for failure.
        """
        try:
            self._config = util.load_cluster_config(
                args.cluster_config_path,
                [
                    ClusterConfig.WORKER_NODES,
                    ClusterConfig.WORKER_PORT,
                    ClusterConfig.SSH_USER_NAME,
                    ClusterConfig.SSH_PRIVATE_KEY,
                ],
            )
            ssh_key_path = os.path.abspath(
                os.path.expanduser(self._config[ClusterConfig.SSH_PRIVATE_KEY])
            )
            self._config[ClusterConfig.SSH_PRIVATE_KEY] = util.valid_safe_path(ssh_key_path)

            self.execute_parallel(self._config[ClusterConfig.WORKER_NODES])
        except Exception as e:
            self.logger.error(f"Down cluster failed: {e}")
            return self.FAILURE
        return self.SUCCESS

    def process_node(self, node):
        """
        Process stopping of worker on a single node.

        Args:
            node (str): The node to stop the worker on.
        """
        user_name = self._config[ClusterConfig.SSH_USER_NAME]
        private_key = self._config[ClusterConfig.SSH_PRIVATE_KEY]
        worker_port = self._config[ClusterConfig.WORKER_PORT]
        is_ipv6 = util.is_valid_ip(node)
        node_arg = node
        if is_ipv6:
            node_arg = "[" + node + "]"
        util.is_valid_port(worker_port)
        util.ssh_execute(
            node,
            user_name,
            private_key,
            f"bash -l -c 'dscli stop -w {node_arg}:{worker_port}' 2>/dev/null",
        )
        self.logger.info(f"Stop worker service @ {node}:{worker_port} success.")
