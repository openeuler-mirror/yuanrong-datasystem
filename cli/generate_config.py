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
"""YuanRong datasystem CLI generate_config command."""

import os
import shutil

from yr.datasystem.cli.command import BaseCommand
import yr.datasystem.cli.common.util as util


class Command(BaseCommand):
    """
    Generate yuanrong datasystem configuration files.
    """

    name = "generate_config"
    description = "generate yuanrong datasystem cluster and worker configuration files"

    @staticmethod
    def add_arguments(parser):
        """
        Add arguments to parser.

        Args:
            parser (ArgumentParser): Specify parser to which arguments are added.
        """
        parser.add_argument(
            "-o", "--output_path",
            default=os.getcwd(),
            help="path to save the generated configuration files, default path is the current directory"
        )

    def run(self, args):
        """
        Execute for generate_config command.

        Args:
            args (Namespace): Parsed arguments containing the output path.

        Raises:
            FileNotFoundError: If the source configuration file does not exist.
            NotADirectoryError: If the output path is not a directory.
        """
        try:
            output_dir = os.path.normpath(os.path.realpath(args.output_path))
            output_dir = util.valid_safe_path(output_dir)
            if not os.path.isdir(output_dir):
                raise NotADirectoryError(f"Path is not a directory: {output_dir}")
            os.makedirs(output_dir, exist_ok=True)

            cluster_file = "cluster_config.json"
            src_cluster = os.path.join(self._base_dir, cluster_file)
            if not os.path.exists(src_cluster):
                raise FileNotFoundError(f"Source cluster configuration file {src_cluster} does not exist")
            dest_cluster = os.path.join(output_dir, cluster_file)
            shutil.copyfile(src_cluster, dest_cluster)
            self.logger.info(f"Cluster configuration file has been generated to {dest_cluster}")

            worker_file = "worker_config.json"
            src_worker = os.path.join(self._base_dir, worker_file)
            if not os.path.exists(src_worker):
                raise FileNotFoundError(f"Source worker configuration file {src_worker} does not exist")
            dest_worker = os.path.join(output_dir, worker_file)
            shutil.copyfile(src_worker, dest_worker)
            self.logger.info(f"Worker configuration file has been generated to {dest_worker}")

            self.logger.info("Configuration generation completed successfully")
        except Exception as e:
            self.logger.error(f"Generate failed: {e}")
            return self.FAILURE
        return self.SUCCESS
    