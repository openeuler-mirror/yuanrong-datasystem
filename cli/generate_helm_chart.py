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
"""YuanRong datasystem CLI command to generate Helm chart."""

import os
import shutil
from pathlib import Path
import yr.datasystem.cli.common.util as util
from yr.datasystem.cli.command import BaseCommand


class Command(BaseCommand):
    """
    Generate Helm chart for yuanrong datasystem deployment.
    """


    name = 'generate_helm_chart'
    description = 'generate Helm chart for yuanrong datasystem deployment'

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
            help='path to save the generated Helm chart, default path is the current directory. \
                Example: dscli generate_helm_chart --output_path /home/user/helmCharts'
        )

    def run(self, args):
        """
        Execute for generate_helm_chart command.

        Args:
            args (Namespace): Parsed command-line arguments.
        """

        # Process output path
        output_path = Path(args.output_path) / 'datasystem-helm-chart'
        output_path = output_path.resolve()
        util.valid_safe_path(output_path)

        # Create output directory
        output_path.mkdir(parents=True, exist_ok=True)

        # Get Helm chart template path
        helm_chart_template_path = os.path.join(self._base_dir, 'helm_chart')

        # Copy template to output directory
        try:
            shutil.copytree(helm_chart_template_path, output_path, dirs_exist_ok=True)
        except shutil.Error as e:
            self.logger.error(f"Error copying files: {e}")
            return self.FAILURE
        except OSError as e:
            self.logger.error(f"OS error occurred: {e}")
            return self.FAILURE

        # Provide feedback for successful generation
        self.logger.info(f"\nHelm chart generated successfully at: {output_path}")
        self.logger.info("You can now use this chart for deployment with Helm.")
        return self.SUCCESS
