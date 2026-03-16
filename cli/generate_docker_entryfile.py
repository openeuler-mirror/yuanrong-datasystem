# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
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
"""YuanRong datasystem CLI command to generate docker entrypoint files."""

import os
import shutil
import stat
from pathlib import Path
import yr.datasystem.cli.common.util as util
from yr.datasystem.cli.command import BaseCommand


class Command(BaseCommand):
    """
    Generate docker entrypoint and exitpoint files for yuanrong datasystem.
    """

    name = 'generate_docker_entryfile'
    description = 'generate docker entrypoint and exitpoint files for yuanrong datasystem'

    @staticmethod
    def add_arguments(parser):
        """
        Add arguments to parser.

        Args:
            parser (ArgumentParser): Specify parser to which arguments are added.
        """
        parser.add_argument(
            '-o', '--output_path',
            type=str,
            metavar='OUTPUT_PATH',
            default=os.getcwd(),
            help='path to save the generated docker entrypoint files, default path is the current directory. \
                Example: dscli generate_docker_entryfile --output_path /home/user/docker_entryfiles'
        )
        parser.add_argument(
            '-m', '--mode',
            type=str,
            metavar='MODE',
            default="daemonset",
            help='mode to save the generated docker entrypoint files, default mode is daemonset. \
                Optional parameter: daemonset, deployment. \
                Example: dscli generate_docker_entryfile --output_path /home/user/docker_entryfiles'
        )

    def run(self, args):
        """
        Execute for generate_docker_entryfile command.

        Args:
            args (Namespace): Parsed command-line arguments.
        """
        
        # Validate mode
        mode = util.validate_mode(args.mode)
        
        # Process output path
        output_path = Path(args.output_path)
        output_path = output_path.resolve()
        util.valid_safe_path(output_path)

        # Create output directory
        output_path.mkdir(parents=True, exist_ok=True)

        # Get entrypoint and exitpoint paths
        entryfile_path = os.path.join(self._base_dir, 'docker_entryfile', mode)

        try:
            for item in os.listdir(entryfile_path):
                src_file = os.path.join(entryfile_path, item)
                if os.path.isfile(src_file):
                    dest_file = os.path.join(output_path, item)
                    shutil.copy2(src_file, dest_file)
                    os.chmod(dest_file, stat.S_IRUSR | stat.S_IXUSR)
        except OSError as e:
            self.logger.error(f"Error copying entrypoint files: {e}")
            return self.FAILURE

        # Provide feedback for successful generation
        self.logger.info(f"Docker entryfile generated successfully at: {output_path}")
        return self.SUCCESS