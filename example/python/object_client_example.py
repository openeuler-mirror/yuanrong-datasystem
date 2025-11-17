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

"""
Object client python interface test.
"""

from __future__ import absolute_import

import argparse

from datasystem.object_client import ObjectClient


class ObjectClientExample:
    """This class shows the SDK usage example of the ObjectClient."""

    def __init__(self):
        parser = argparse.ArgumentParser(description="Object client python interface Test")
        parser.add_argument("--host", required=True, help="The IP of worker service")
        parser.add_argument("--port", required=True, type=int, help="The port of worker service")
        args = parser.parse_args()
        self._host = args.host
        self._port = args.port

    def set_data_example(self):
        """This function shows the basic usage of g_increase_ref/create/get/g_decrease_ref."""
        client = ObjectClient(self._host, self._port)
        # Init object client
        client.init()

        # Increase the key's global reference
        key = "key"
        client.g_increase_ref([key])

        # Create shared memory buffer for key.
        value = bytes("val", encoding="utf8")
        size = len(value)
        buf = client.create(key, size)

        # Lock shared memory buffer. NOTE: Lock protection for shared memory data
        # access is only required in scenarios involving concurrent access from
        # multiple instances on a single node.
        buf.wlatch()

        # Copy data to shared memory buffer.
        buf.memory_copy(value)

        # Publish the key.
        buf.publish()

        # Unlock shared memory buffer.
        buf.unwlatch()

        # Get the key.
        buffer_list = client.get([key], True)
        if value != buffer_list[0].immutable_data():
            raise RuntimeError(f"Assert failed, expect {value}, but got {buffer_list[0].immutable_data()}")

        # Decrease the key's global reference, the lifecycle of this key will end afterwards.
        client.g_decrease_ref([key])


if __name__ == "__main__":
    ObjectClientExample().set_data_example()
