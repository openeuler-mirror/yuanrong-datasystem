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
"""YuanRong datasystem CLI runscript command."""

import os
import shlex

import yr.datasystem.cli.common.util as util
from yr.datasystem.cli.command import BaseCommand
from yr.datasystem.cli.common.constant import ClusterConfig
from yr.datasystem.cli.common.parallel import ParallelMixin


class Command(BaseCommand, ParallelMixin):
    """
    Run script on cluster nodes.
    """

    name = "runscript"
    description = "run script on cluster nodes"

    _config = {}
    _script_path = ""

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
            help="path of cluster configuration file (JSON format)",
        )
        parser.add_argument("script", help="script to execute, e.g., install.sh")

    def run(self, args):
        """
        Execute for runscript command.

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
            ssh_key_path = os.path.realpath(
                os.path.expanduser(self._config[ClusterConfig.SSH_PRIVATE_KEY])
            )
            self._config[ClusterConfig.SSH_PRIVATE_KEY] = util.valid_safe_path(ssh_key_path)

            self._script_path = os.path.realpath(os.path.expanduser(args.script))
            if not os.path.exists(self._script_path):
                raise FileNotFoundError(
                    f"Script file {self._script_path} does not exist."
                )

            self.execute_parallel(self._config[ClusterConfig.WORKER_NODES])
        except Exception as e:
            self.logger.error(f"Run script failed: {e}")
            return self.FAILURE
        return self.SUCCESS

    def process_node(self, worker_node):
        """
        Execute a script on a single worker node.

        Args:
            worker_node (str): The IP address of the worker node.
        """
        remote_script_path = os.path.normpath(f"~/{os.path.basename(self._script_path)}")
        remote_script_path = util.valid_safe_path(remote_script_path)

        try:
            user_name = self._config[ClusterConfig.SSH_USER_NAME]
            private_key = self._config[ClusterConfig.SSH_PRIVATE_KEY]
            util.scp_upload(
                self._script_path,
                worker_node,
                remote_script_path,
                user_name,
                private_key,
            )
            remote_script_path = util.validate_no_injection(remote_script_path)
            util.ssh_execute(
                worker_node, user_name, private_key, f"bash {shlex.quote(remote_script_path)}"
            )
            self.logger.info(f"Script executed successfully on {worker_node}.")

            util.ssh_execute(
                worker_node, user_name, private_key, f"rm -f {shlex.quote(remote_script_path)}"
            )
        except Exception as e:
            raise RuntimeError(f"Error executing script on {worker_node}: {e}") from e
