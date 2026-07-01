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

import yr.datasystem.cli.common.util as util
from yr.datasystem.cli.command import BaseCommand


class Command(BaseCommand):
    """
    Generate yuanrong datasystem configuration files.
    """

    name = "generate_config"
    description = "generate yuanrong datasystem cluster, worker and coordinator configuration files"
    _CONFIG_FILES = (
        "cluster_config.json",
        "worker_config.json",
        "coordinator_config.json",
    )

    @staticmethod
    def add_arguments(parser):
        """
        Add arguments to parser.

        Args:
            parser (ArgumentParser): Specify parser to which arguments are added.
        """
        parser.add_argument(
            "-o",
            "--output_path",
            default=os.getcwd(),
            help="path to save the generated configuration files, default path is the current directory",
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

            for config_file in self._CONFIG_FILES:
                src_config = os.path.join(self._base_dir, config_file)
                if not os.path.exists(src_config):
                    raise FileNotFoundError(
                        f"Source configuration file {src_config} does not exist"
                    )
                dest_config = os.path.join(output_dir, config_file)
                shutil.copyfile(src_config, dest_config)
                self.logger.info(
                    f"Configuration file {config_file} has been generated to {dest_config}"
                )

            self.logger.info("Configuration generation completed successfully")
        except Exception as e:
            self.logger.error(f"Generate failed: {e}")
            return self.FAILURE
        return self.SUCCESS
