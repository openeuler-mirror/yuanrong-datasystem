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
Ds client python interface Test.
"""
from __future__ import absolute_import
import os
import random
import json
import time
import unittest

from yr.datasystem.ds_client import DsClient
from yr.datasystem.object_client import WriteMode
from yr.datasystem.util import Context


class TestDsClientMethods(unittest.TestCase):
    """
    Features: DsClient python interface test.
    """

    @staticmethod
    def random_str(slen=10):
        """
        Features: Generate a random string for test.
        """
        seed = "1234567890abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ!@#%^*()_+=-"
        sa = []
        for _ in range(slen):
            sa.append(random.choice(seed))
        return ''.join(sa)

    @classmethod
    def setUpClass(cls):
        root_dir = os.path.dirname(os.path.abspath('..'))
        worker_env_path = os.path.join(root_dir, 'output', 'datasystem', 'service', 'worker_config.json')
        with open(worker_env_path, "r") as f:
            config = json.load(f)

        work_address = config.get("worker_address", {})
        work_addr = work_address.get("value")
        cls.host, cls.port = work_addr.split(":")
        cls.port = int(cls.port)

    def test_kv_client(self):
        """
        Features: Test DsClient kv client api.
        """
        Context.set_trace_id("ds_client_test_kv_client")
        client1 = DsClient(self.host, self.port)
        client2 = DsClient(self.host, self.port)
        client1.init()
        client2.init()

        key1 = self.random_str()
        val1 = self.random_str(20)
        write_mode = WriteMode.NONE_L2_CACHE
        client1.kv().set(key1, val1, write_mode)

        key_list = [key1]
        get_value_list = client2.kv().get(key_list)
        self.assertEqual(len(get_value_list), 1)
        self.assertEqual(str(get_value_list[0], encoding='utf-8'), val1)

        client1.kv().delete(key_list)
        with self.assertRaisesRegex(RuntimeError, r"Key not found"):
            client2.kv().get(key_list)

    def test_object_client(self):
        """
        Features: Test DsClient object client api.
        """
        Context.set_trace_id("ds_client_test_object_client")
        client1 = DsClient(self.host, self.port)
        client2 = DsClient(self.host, self.port)
        client1.init()
        client2.init()

        object_key = self.random_str(10)
        client1.object().g_increase_ref([object_key])
        value = bytes(self.random_str(50), encoding='utf8')
        buffer1 = client1.object().create(object_key, len(value))
        buffer1.wlatch()
        buffer1.memory_copy(value)
        buffer1.seal()
        buffer1.unwlatch()

        self.assertEqual(client2.object().g_increase_ref([object_key]), [])
        buffer_list = client2.object().get([object_key], 5)
        self.assertEqual(value, buffer_list[0].immutable_data().tobytes())
        self.assertEqual(client1.object().g_decrease_ref([object_key]), [])
        self.assertEqual(client2.object().g_decrease_ref([object_key]), [])
        time.sleep(1)
        with self.assertRaisesRegex(RuntimeError, r"Key not found"):
            client2.object().get([object_key], 5)

    def test_hetero_client(self):
        """
        Features: Test DsClient hetero client api.
        """
        Context.set_trace_id("ds_client_test_hetero_client")
        client1 = DsClient(self.host, self.port)
        client1.init()
        client1.hetero()
