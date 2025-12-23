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
"""YuanRong datasystem CLI collect_log command."""


import os
from datetime import datetime, timezone

import yr.datasystem.cli.common.util as util
from yr.datasystem.cli.command import BaseCommand
from yr.datasystem.cli.common.constant import ClusterConfig
from yr.datasystem.cli.common.parallel import ParallelMixin


class Command(BaseCommand, ParallelMixin):
    """
    Collect logs on cluster nodes.
    """

    name = "collect_log"
    description = "collect logs on cluster nodes"

    _config = {}
    _remote_log_path = ""
    _output_dir = ""

    @staticmethod
    def add_arguments(parser):
        """Add arguments to parser.

        Args:
            parser (ArgumentParser): Specify parser to which arguments are added.
        """
        parser.add_argument(
            "-f",
            "--cluster_config_path",
            metavar="FILE",
            required=True,
            help="path of cluster configuration file (JSON format)",
        )
        parser.add_argument(
            "-d",
            "--log_path",
            metavar="PATH",
            required=True,
            help="remote path to the logs directory on each cluster node that needs to be collected",
        )
        parser.add_argument(
            "-o",
            "--output_path",
            default=f"log_{datetime.now(tz=timezone.utc).strftime('%Y%m%d_%H%M%S')}",
            help="local directory path where collected logs will be stored, "
            "defaults to 'log_<timestamp>' in the current directory",
        )

    def run(self, args):
        """Execute for collect_log command.

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
                    ClusterConfig.SSH_USER_NAME,
                    ClusterConfig.SSH_PRIVATE_KEY,
                ],
            )
            self._config[ClusterConfig.SSH_PRIVATE_KEY] = os.path.realpath(
                os.path.expanduser(self._config[ClusterConfig.SSH_PRIVATE_KEY])
            )

            self._remote_log_path = args.log_path

            self._output_dir = os.path.realpath(args.output_path)
            os.makedirs(self._output_dir, exist_ok=True)

            self.execute_parallel(self._config[ClusterConfig.WORKER_NODES])
        except Exception as e:
            self.logger.error(f"Collect log failed: {e}")
            return self.FAILURE
        return self.SUCCESS

    def process_node(self, node):
        """Collect logs from a single cluster node.

        Args:
            node (str): The IP address of the worker node.

        Raises:
            RuntimeError: If log collection from the node fails.
        """
        try:
            user_name = self._config[ClusterConfig.SSH_USER_NAME]
            private_key = self._config[ClusterConfig.SSH_PRIVATE_KEY]
            local_path = os.path.normpath(os.path.join(self._output_dir, str(node)))
            local_path = util.valid_safe_path(local_path)
            node = util.is_valid_ipv4(node)

            util.scp_download(
                node,
                self._remote_log_path,
                local_path,
                user_name,
                private_key,
            )
            self.logger.info(f"Collect logs from {node} successfully.")
        except Exception as e:
            raise RuntimeError(f"Failed to collect logs from {node}: {e}") from e
