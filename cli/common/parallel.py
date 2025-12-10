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
"""YuanRong datasystem CLI parallel module."""

from concurrent.futures import ThreadPoolExecutor, as_completed


class ParallelMixin:
    """Mixin to run tasks in parallel across nodes."""

    def execute_parallel(self, nodes, **kwargs):
        """Execute tasks on all nodes in parallel."""
        if not hasattr(self, "process_node"):
            raise NotImplementedError(
                "Class must implement process_node() to use execute_parallel"
            )
        is_error = False

        with ThreadPoolExecutor(max_workers=len(nodes)) as executor:
            futures = {executor.submit(self.process_node, node, **kwargs): node for node in nodes}

            for future in as_completed(futures):
                node = futures[future]
                try:
                    future.result()
                except Exception as e:
                    is_error = True
                    self.logger.error(f"Node {node} execute failed: {e}")

            if is_error:
                raise RuntimeError("Some nodes failed during processing")
