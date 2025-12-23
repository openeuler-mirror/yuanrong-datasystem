# Copyright (c) Huawei Technologies Co., Ltd. 2022. All rights reserved.
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
Object cache client python interface Test.
"""
from __future__ import absolute_import
import math
import os
import random
import json
import threading
import time
import unittest

from yr.datasystem.object_client import Buffer, ConsistencyType, ObjectClient


class TestOcClientMethods(unittest.TestCase):
    """
    Features: Object cache client python interface test.
    """

    @classmethod
    def setUpClass(cls):
        root_dir = os.path.dirname(os.path.abspath('..'))
        worker_env_path = os.path.join(root_dir, "output", "datasystem", "service", "worker_config.json")
        with open(worker_env_path, "r") as f:
            config = json.load(f)

        work_address = config.get("worker_address", {})
        TestOcClientMethods.work_addr = work_address.get("value")
        TestOcClientMethods.host, port = TestOcClientMethods.work_addr.split(":")
        TestOcClientMethods.port = int(port)
        TestOcClientMethods.BUFFER = Buffer()

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

    @staticmethod
    def write_buffer(value):
        """
        Features: Write data to the buffer, for concurrency test only.
        """
        TestOcClientMethods.BUFFER.wlatch()
        TestOcClientMethods.BUFFER.memory_copy(value)
        TestOcClientMethods.BUFFER.unwlatch()

    @staticmethod
    def read_buffer():
        """
        Features: Read data from the buffer, for concurrency test only.
        """
        TestOcClientMethods.BUFFER.rlatch()
        value = TestOcClientMethods.BUFFER.immutable_data()
        TestOcClientMethods.BUFFER.unrlatch()
        return value

    @staticmethod
    def init_test_client():
        """
        Features: Init test client.
        """
        client = ObjectClient(TestOcClientMethods.host, TestOcClientMethods.port)
        client.init()
        return client

    def test_client_put_get_succeed(self):
        """
        Features: Put an object successfully.
        """
        client = self.init_test_client()
        object_key = self.random_str(10)
        value = bytes(self.random_str(50), encoding='utf8')
        param = {"consistency_type": ConsistencyType.PRAM}
        client.put(object_key, value, param)
        buffer_list = client.get([object_key], 5)
        self.assertEqual(value, buffer_list[0].immutable_data())

    def test_client_get_immutable_and_release_buffer(self):
        """
        Features: Put an object successfully.
        """
        client = self.init_test_client()
        object_key = self.random_str(10)
        value = bytes(self.random_str(50), encoding='utf8')
        param = {"consistency_type": ConsistencyType.PRAM}
        client.put(object_key, value, param)
        buffer_list = client.get([object_key], 5)
        read_data = buffer_list[0].immutable_data()
        buffer_list = None
        self.assertEqual(value, read_data)

    def test_client_create_publish_succeed(self):
        """
        Features: Put an object successfully.
        """
        client = self.init_test_client()
        object_key = self.random_str(10)
        value = bytes(self.random_str(50), encoding='utf8')
        size = len(value)
        param = {"consistency_type": ConsistencyType.PRAM}
        buffer = client.create(object_key, size, param)
        self.assertEqual(buffer.get_size(), size)
        buffer.wlatch()
        buffer.memory_copy(value)
        buffer.publish()
        buffer.unwlatch()

    def test_create_publish_get_bytearray_succeed(self):
        """
        Features: Put and get a bytearray type data successfully.
        """
        client = self.init_test_client()
        object_key = self.random_str(10)
        value = bytearray(self.random_str(50), encoding='utf8')
        size = len(value)
        buffer = client.create(object_key, size)
        buffer.wlatch()
        buffer.memory_copy(value)
        buffer.seal()
        buffer.unwlatch()
        buffer_list = client.get([object_key], 5)
        self.assertEqual(value, buffer_list[0].immutable_data())

    def test_client_put_get_memoryview_succeed(self):
        """
        Features: Put and get a memoryview type data successfully.
        """
        client = self.init_test_client()
        object_key = self.random_str(10)
        byte_value = bytes(self.random_str(50), encoding='utf8')
        value = memoryview(byte_value)
        size = len(value)
        buffer = client.create(object_key, size)
        buffer.wlatch()
        buffer.memory_copy(value)
        buffer.seal()
        buffer.unwlatch()
        buffer_list = client.get([object_key], 5)
        self.assertEqual(value, buffer_list[0].immutable_data())

    def test_client_get_succeed(self):
        """
        Features: Get some objects successfully.
        """
        client = self.init_test_client()

        object_key = self.random_str(10)
        value = bytes(self.random_str(50), encoding='utf8')
        buffer1 = client.create(object_key, len(value))
        buffer1.wlatch()
        buffer1.memory_copy(value)
        buffer1.seal()
        buffer1.unwlatch()

        object_key2 = self.random_str(10)
        value2 = bytes(self.random_str(100), encoding='utf8')
        buffer2 = client.create(object_key2, len(value2))
        buffer2.wlatch()
        buffer2.memory_copy(value2)
        buffer2.seal()
        buffer2.unwlatch()

        buffer_list = client.get([object_key, object_key2], 5)
        self.assertEqual(value, buffer_list[0].immutable_data().tobytes())
        self.assertEqual(value2, buffer_list[1].immutable_data().tobytes())

        client2 = self.init_test_client()
        buffer_list2 = client2.get([object_key, object_key2], 5)
        self.assertEqual(value, buffer_list2[0].immutable_data().tobytes())
        self.assertEqual(value2, buffer_list2[1].immutable_data().tobytes())

    def test_client_get_failed(self):
        """
        Features: Get an object that does not exist
        """
        client = self.init_test_client()
        object_key = self.random_str(10)
        value = bytes(self.random_str(50), encoding='utf8')
        buffer = client.create(object_key, len(value))
        buffer.wlatch()
        buffer.memory_copy(value)
        buffer.seal()
        buffer.unwlatch()
        buffer_list = client.get([object_key, self.random_str(10)], 5)
        self.assertEqual(value, buffer_list[0].immutable_data().tobytes())
        self.assertIsNone(buffer_list[1])

    @staticmethod
    def init_client_timeout_with_interval(client) -> int:
        """
        Features: Configure the client timeout interval, run and return the duration
        """
        timeout_start = time.time()
        try:
            client.init()
        except RuntimeError:
            timeout_end = time.time()
        else:
            timeout_end = time.time()
        return math.floor(timeout_end - timeout_start)  # Ignore values after decimal points

    def test_client_init_timeout(self):
        """
        Features: Test timeout case of client init
        """
        host, port = "127.0.0.1", 18888
        client = ObjectClient(host, port, 5 * 1000, "")
        duration = self.init_client_timeout_with_interval(client)
        self.assertLessEqual(duration, 6, "duration is {time} sec, want {interval}".format(time=duration, interval=5))

    def test_client_increase_decrease_global_ref(self):
        """
        Features: Test increase and decrease ref count
        """
        client = self.init_test_client()
        object_key = self.random_str(10)
        client.g_increase_ref([object_key])
        self.assertEqual(client.g_decrease_ref([object_key, "not_exist_key"]), [])

    def test_client_put_get_big_bytes_succeed(self):
        """
        Features: Put and get a big bytes object successfully.
        """
        client = self.init_test_client()
        value_size = 5 * 1024 * 1024
        object_key1 = self.random_str(10)
        value1 = bytes("x" * value_size, encoding='utf8')
        buffer1 = client.create(object_key1, len(value1))
        buffer1.wlatch()
        buffer1.memory_copy(value1)
        buffer1.seal()
        buffer1.unwlatch()
        buffer_list1 = client.get([object_key1], 5)
        self.assertEqual(value1, buffer_list1[0].immutable_data())

    def test_client_put_get_big_bytearray_succeed(self):
        """
        Features: Put and get a big bytearray object successfully.
        """
        client = self.init_test_client()
        value_size = 5 * 1024 * 1024
        object_key2 = self.random_str(10)
        value2 = bytearray("x" * value_size, encoding='utf8')
        buffer2 = client.create(object_key2, len(value2))
        buffer2.wlatch()
        buffer2.memory_copy(value2)
        buffer2.seal()
        buffer2.unwlatch()
        buffer_list2 = client.get([object_key2], 5)
        self.assertEqual(value2, buffer_list2[0].immutable_data())

    def test_client_put_get_big_memoryview_succeed(self):
        """
        Features: Put and get a big memoryview object successfully.
        """
        client = self.init_test_client()
        value_size = 5 * 1024 * 1024
        object_key3 = self.random_str(10)
        byte_value = bytes("x" * value_size, encoding='utf8')
        value3 = memoryview(byte_value)
        buffer3 = client.create(object_key3, len(value3))
        buffer3.wlatch()
        buffer3.memory_copy(value3)
        buffer3.seal()
        buffer3.unwlatch()
        buffer_list3 = client.get([object_key3], 5)
        self.assertEqual(value3, buffer_list3[0].immutable_data())

    def test_concurrent_write_read_buffer(self):
        """
        Features: Write data concurrently to the buffer.
        """
        client = self.init_test_client()
        object_key = self.random_str(10)
        value = bytes(self.random_str(50), encoding='utf8')
        TestOcClientMethods.BUFFER = client.create(object_key, len(value))
        thread_num = 5
        threads = []
        i = 0
        while i in range(thread_num):
            t = threading.Thread(target=self.write_buffer, args=(value,))
            threads.append(t)
            i += 1
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        buffer_val = self.read_buffer()
        self.assertEqual(value, buffer_val)

    def test_invalidate_buffer(self):
        """
        Features: Invalidate the buffer.
        """
        client = self.init_test_client()
        object_key = self.random_str(10)
        value = bytes(self.random_str(50), encoding='utf8')
        buffer = client.create(object_key, len(value))
        buffer.wlatch()
        buffer.memory_copy(value)
        buffer.publish()
        buffer.unwlatch()
        buffer.invalidate_buffer()
        self.assertRaises(RuntimeError, client.get, [object_key], 5)

    def test_buffer_immutable_data(self):
        """
        Features: Get immutable memoryview from the buffer.
        """
        client = self.init_test_client()
        object_key = self.random_str(10)
        value = bytearray(self.random_str(50), encoding='utf8')
        buffer = client.create(object_key, len(value))
        immutable_value = buffer.immutable_data()
        self.assertTrue(immutable_value.readonly)

    def test_buffer_mutable_data(self):
        """
        Features: Get mutable memoryview from the buffer.
        """
        client = self.init_test_client()
        object_key = self.random_str(10)
        value = bytearray(self.random_str(50), encoding='utf8')
        buffer = client.create(object_key, len(value))
        mutable_value = buffer.mutable_data()
        self.assertFalse(mutable_value.readonly)

    def test_nest_publish(self):
        """
        Features: Test nested publish.
        """
        client = self.init_test_client()
        object1 = "key_1"
        object2 = "key_2"
        object3 = "key_3"
        value = bytes(self.random_str(50), encoding='utf8')
        client.g_increase_ref([object1, object2, object3])

        self.assertEqual(client.query_global_ref_num(object1), 1)
        self.assertEqual(client.query_global_ref_num(object2), 1)
        self.assertEqual(client.query_global_ref_num(object3), 1)

        param = {"consistency_type": ConsistencyType.PRAM}
        client.put(object2, value, param)
        client.put(object3, value, param)

        buffer = client.create(object1, len(value))
        buffer.wlatch()
        buffer.memory_copy(value)
        buffer.publish([object2, object3])
        buffer.unwlatch()

        buffer_list = client.get([object1, object2, object3], 0)
        self.assertEqual(len(buffer_list), 3)
        self.assertEqual(value, buffer_list[0].immutable_data())
        self.assertEqual(value, buffer_list[1].immutable_data())
        self.assertEqual(value, buffer_list[2].immutable_data())

        client.g_decrease_ref([object2, object3])
        self.assertEqual(client.query_global_ref_num(object1), 1)
        self.assertEqual(client.query_global_ref_num(object2), 0)
        self.assertEqual(client.query_global_ref_num(object3), 0)

        self.assertEqual(len(buffer_list), 3)
        buffer_list = client.get([object1, object2, object3], 0)
        self.assertEqual(value, buffer_list[0].immutable_data())
        self.assertEqual(value, buffer_list[1].immutable_data())
        self.assertEqual(value, buffer_list[2].immutable_data())

        client.g_decrease_ref([object1])
        self.assertEqual(client.query_global_ref_num(object1), 0)
        self.assertEqual(client.query_global_ref_num(object2), 0)
        self.assertEqual(client.query_global_ref_num(object3), 0)

        self.assertRaises(RuntimeError, client.get, [object1, object2, object3], 0)

    def test_generate_object_id(self):
        """
        Features: Test generate object key.
        """
        client = self.init_test_client()
        object_key = client.generate_object_id("abc")

        self.assertEqual(client.g_increase_ref([object_key]), [])
        value = bytes(self.random_str(50), encoding='utf8')
        buffer1 = client.create(object_key, len(value))
        buffer1.wlatch()
        buffer1.memory_copy(value)
        buffer1.seal()
        buffer1.unwlatch()
        self.assertEqual(client.g_decrease_ref([object_key]), [])
