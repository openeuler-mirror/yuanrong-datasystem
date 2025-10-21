# Copyright (c) Huawei Technologies Co., Ltd. 2023. All rights reserved.
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
State cache client python interface Test.
"""

from __future__ import absolute_import
import time

from datasystem.kv_client import KVClient


def assert_eq(value, expected_value):
    """Compare two values for equality."""
    if value != expected_value:
        raise RuntimeError(f"Assert failed, expect {expected_value}, but got {value}")


class KVClientExample:
    """This class shows the SDK usage example of the KVClient."""

    def __init__(self, host, port):
        self._host = host
        self._port = port

    def set_data_example(self):
        """This function shows the basic usage of set/get/del."""
        client = KVClient(self._host, self._port)
        client.init()
        key = "key"
        expected_val = "value"
        client.set(key, expected_val)

        val = client.get([key], True)
        assert_eq(val[0], expected_val)

        client.delete([key])

    def set_value_with_generate_key_example(self):
        """This function shows the basic usage of set_value/generate_key."""
        client = KVClient(self._host, self._port)
        client.init()
        expected_val = "value"
        key = client.set_value(expected_val)

        val = client.get([key], True)
        assert_eq(val[0], expected_val)

        client.delete(["key"])

        key = client.generate_key()
        client.set(key, expected_val)
        client.delete([key])

    def set_data_with_ttl_example(self):
        """This function shows the basic usage of TTL(time to live).
        This value is valid only for the TTL time and will be deleted after the time expires."""
        client = KVClient(self._host, self._port)
        client.init()
        key = "key"
        expected_val = "value"
        client.set(key, expected_val, ttl_second=2)

        val = client.get([key])
        decode_val = val[0].decode()
        if decode_val != expected_val:
            raise RuntimeError(f"[set_data_with_ttl] Get value failed, expect {expected_val}, but got {val}")

        time.sleep(3)
        try:
            client.get([key])
        except RuntimeError as e:
            if "Cannot get objects from worker" not in str(e):
                raise RuntimeError('get failed ') from e

    def set_data_with_l2_cache_example(self):
        """This function shows the basic usage of write_mode. If the L2Cache is configured on the server,
        data can be written to the L2Cache to improve data reliability."""
        client = KVClient(self._host, self._port)
        client.init()
        expected_val = "value"
        client.set("key", expected_val)


if __name__ == '__main__':
    example = KVClientExample('127.0.0.1', 31501)
    example.set_data_example()
    example.set_value_with_generate_key_example()
    example.set_data_with_ttl_example()
    example.set_data_with_l2_cache_example()
