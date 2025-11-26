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
State cache client python interface Test.
"""
from __future__ import absolute_import
import os
import random
import json
import time
import unittest

from yr.datasystem.object_client import WriteMode, ObjectClient
from yr.datasystem.kv_client import KVClient, ExistenceOpt, ReadParam
from yr.datasystem.util import Context


class TestKVClientMethods(unittest.TestCase):
    """
    Features: State cache client python interface test.
    """
    host = ""
    port = 0

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

    def test_state_init_by_env(self):
        """
        Features: Success to init state client test.
        """
        Context.set_trace_id("test_state_init_by_env")
        os.environ['DATASYSTEM_HOST'] = self.host
        os.environ['DATASYSTEM_PORT'] = str(self.port)
        client = KVClient(host="", port=0, timeout_ms=5000)
        client.init()

    def test_state_set_succeed(self):
        """
        Features: Success to set a data cache test.
        """
        Context.set_trace_id("test_state_set_succeed")
        key1 = self.random_str()
        val1 = self.random_str(20)
        client1 = KVClient(self.host, self.port)
        client1.init()
        param1 = WriteMode.NONE_L2_CACHE
        client1.set(key1, val1, param1)

        key2 = self.random_str()
        val2 = self.random_str(20)
        client2 = KVClient(self.host, self.port)
        client2.init()
        param2 = WriteMode.NONE_L2_CACHE
        client1.set(key2, val2, param2)

    @unittest.skip("redis server problem")
    def test_state_set_l2_cache_succeed(self):
        """
        Features: Success to set a data cache and async send to L2 cache test.
        """
        Context.set_trace_id("test_state_set_l2_cache_succeed")
        key1 = "key1"
        val1 = "value1"
        client1 = KVClient(self.host, self.port)
        client1.init()
        param1 = WriteMode.WRITE_THROUGH_L2_CACHE
        client1.set(key1, val1, param1)

        key2 = "key2"
        val2 = "value2"
        param2 = WriteMode.WRITE_BACK_L2_CACHE
        client1.set(key2, val2, param2)

    def test_state_set_ttl(self):
        """
        Features: Success to set a data cache test.
        """
        Context.set_trace_id("test_state_set_ttl")
        key1 = self.random_str()
        val1 = self.random_str(20)
        client1 = KVClient(self.host, self.port)
        client1.init()
        param1 = WriteMode.NONE_L2_CACHE
        ttl_second = 1
        client1.set(key1, val1, param1, ttl_second)

        key2 = self.random_str()
        val2 = self.random_str(20)
        client2 = KVClient(self.host, self.port)
        client2.init()
        param2 = WriteMode.NONE_L2_CACHE
        client1.set(key2, val2, param2, ttl_second)
        self.assertEqual(client1.get([key1, key2], True), [val1, val2])
        time.sleep(2)
        with self.assertRaises(RuntimeError):
            client1.get([key1, key2], True)

    def test_state_set_ttl_invalid_args(self):
        """
        Features: Success to set a data cache test.
        """
        Context.set_trace_id("test_state_set_ttl_invalid_args")
        key1 = self.random_str()
        val1 = self.random_str(20)
        client1 = KVClient(self.host, self.port)
        client1.init()
        param1 = WriteMode.NONE_L2_CACHE

        with self.assertRaises(TypeError):
            client1.set(key1, val1, param1, -1)
        with self.assertRaises(TypeError):
            client1.set(key1, val1, param1, 4294967296)

        ttl_second = 4294967295
        client1.set(key1, val1, param1, ttl_second)
        time.sleep(2)
        self.assertEqual(client1.get([key1], True), [val1])

    def test_state_set_failed(self):
        """
        Features: Fail to set a data cache test.
        """
        Context.set_trace_id("test_state_set_failed")
        key1 = ''
        val1 = self.random_str(20)
        key2 = self.random_str()
        val2 = ''
        client = KVClient(self.host, self.port)
        client.init()
        self.assertRaisesRegex(RuntimeError, "The objectKey should not be empty.", client.set, key1, val1)
        self.assertRaisesRegex(RuntimeError, "The dataSize value should be bigger than zero.", client.set, key2, val2)

    def test_state_get_read_only_buffers(self):
        """
        Features: Success to get read only buffers and then get memory view from the buffers.
        """
        key_list = [self.random_str(), self.random_str(), self.random_str()]
        val_list = [self.random_str(20), self.random_str(20), self.random_str(1_000_000)]
        client = KVClient(self.host, self.port)
        client.init()
        client.set(key_list[0], val_list[0])
        client.set(key_list[1], val_list[1])
        client.set(key_list[2], val_list[2])
        read_only_buffers = client.get_read_only_buffers(key_list, 0)
        views = [buffer.immutable_data() for buffer in read_only_buffers]
        for i, mem_view in enumerate(views):
            read_only_buffers[i].rlatch()
            self.assertEqual(val_list[i].encode(), mem_view.tobytes())
            read_only_buffers[i].unrlatch()

    def test_state_get_read_only_buffers_with_latch(self):
        """
        Features: Success to get read only buffers and then get memory view from the buffers with latch.
        """
        key_list = [self.random_str(), self.random_str(), self.random_str()]
        val_list = [self.random_str(20), self.random_str(20), self.random_str(1_000_000)]
        client = KVClient(self.host, self.port)
        client.init()
        client.set(key_list[0], val_list[0])
        client.set(key_list[1], val_list[1])
        client.set(key_list[2], val_list[2])
        read_only_buffers = client.get_read_only_buffers(key_list, 0)
        views = [buffer.immutable_data(with_latch=True) for buffer in read_only_buffers]
        for i, mem_view in enumerate(views):
            self.assertEqual(val_list[i].encode(), mem_view.tobytes())

    def test_state_get_read_only_buffers_latch_fail(self):
        """
        Features: Success to get read only buffers and then get memory view from the buffers with latch.
        Before that, object client get a buffer and wlatch.
        """
        key_list = [self.random_str(), self.random_str()]
        val_list = [self.random_str(20), self.random_str(1_000_000)]
        oc_client = ObjectClient(self.host, self.port)
        oc_client.init()
        client = KVClient(self.host, self.port)
        client.init()

        client.set(key_list[0], val_list[0])
        client.set(key_list[1], val_list[1])
        rw_buffers = oc_client.get([key_list[0], key_list[1]], timeout_ms=1000)

        # small element case.
        read_only_buffers = client.get_read_only_buffers(key_list, 0)
        mem_view = read_only_buffers[0].immutable_data(with_latch=True, timeout_sec=1)
        self.assertEqual(mem_view.tobytes(), val_list[0].encode())

        # big element case.
        rw_buffers[1].wlatch(timeout_sec=1)
        read_only_buffers = client.get_read_only_buffers(key_list, 0)
        is_runtime_error = False
        try:
            read_only_buffers[1].immutable_data(with_latch=True, timeout_sec=1)
        except RuntimeError:
            is_runtime_error = True
        self.assertTrue(is_runtime_error)
        rw_buffers[1].unwlatch()
        mem_view = read_only_buffers[1].immutable_data(with_latch=True, timeout_sec=1)
        self.assertEqual(mem_view.tobytes(), val_list[1].encode())

    def test_read(self):
        """
        Features: Test read.
        """
        Context.set_trace_id("test_read")
        client = KVClient(self.host, self.port)
        client.init()

        key_num = 3
        value_size = 100 * 1024
        append_value = "abcdefg"
        key_list = [self.random_str() for _ in range(key_num)]
        val_list = [self.random_str(value_size) + append_value for _ in range(key_num)]
        write_mode = WriteMode.NONE_L2_CACHE
        existence_opt = ExistenceOpt.NONE
        self.assertEqual(client.mset(key_list, val_list, write_mode, 0, existence_opt), [])

        read_param_list = []
        for i in range(key_num):
            read_param_list.append(ReadParam(key=key_list[i], offset=value_size, size=len(append_value)))
        buffers = client.read(read_param_list)
        self.assertEqual(len(buffers), key_num)
        for i in range(key_num):
            mem_view = buffers[i].immutable_data(with_latch=True, timeout_sec=1)
            bytes_data = mem_view.tobytes()
            result_str = bytes_data.decode('utf-8', errors='replace')
            self.assertEqual(result_str, append_value)

        self.assertEqual(client.delete(key_list), [])

    def test_state_get_succeed(self):
        """
        Features: Success to get a data cache test.
        """
        Context.set_trace_id("test_state_get_succeed")
        key_list = [self.random_str(), self.random_str(), self.random_str()]
        val_list = [self.random_str(20), self.random_str(20), self.random_str(20)]
        client = KVClient(self.host, self.port)
        client.init()
        client.set(key_list[0], val_list[0])
        client.set(key_list[1], val_list[1])
        client.set(key_list[2], val_list[2])
        self.assertEqual(client.get(key_list, True), val_list)

    def test_state_get_failed(self):
        """
        Features: Fail to get a data cache test.
        """
        Context.set_trace_id("test_state_get_failed")
        key1 = self.random_str()
        val1 = self.random_str()
        key2 = self.random_str()
        client = KVClient(self.host, self.port)
        client.init()
        client.set(key1, val1)
        self.assertEqual(client.get([key1], True), [val1])
        self.assertRaisesRegex(RuntimeError, "Cannot get objects from worker", client.get, [key2], False, 5)
        self.assertEqual(client.get([key1, key2], True), [val1, None])
        self.assertEqual(client.get([key1, key2], False), [val1.encode(), None])

    def test_state_del_succeed(self):
        """
        Features: Success to delete a data cache test.
        """
        Context.set_trace_id("test_state_del_succeed")
        key1 = self.random_str()
        key2 = self.random_str()
        key3 = self.random_str()
        key_list = [key1, key2, key3]
        client = KVClient(self.host, self.port)
        client.init()
        client.set(key1, '33')
        client.set(key2, '44')
        client.set(key3, '55')
        self.assertEqual(client.get(key_list, True), ['33', '44', '55'])
        self.assertEqual(client.delete(key_list), [])

    def test_state_del_failed(self):
        """
        Features: Fail to delete a data cache test.
        """
        Context.set_trace_id("test_state_del_failed")
        key = self.random_str()
        val = self.random_str(20)
        client = KVClient(self.host, self.port)
        client.init()
        client.set(key, val)
        self.assertEqual(client.get([key], True), [val])
        self.assertEqual(client.delete([key]), [])
        self.assertEqual(client.delete([key]), [])

    def test_set_value(self):
        """
        Features: Set value without key.
        """
        Context.set_trace_id("test_set_value")
        client = KVClient(self.host, self.port)
        client.init()
        val = "qqqqq"
        key = client.set_value(val)
        self.assertEqual(client.get([key], True), [val])

    def test_mset_values(self):
        """
        Feature: Test mset values.
        """
        Context.set_trace_id("test_m_set_value")
        client1 = KVClient(self.host, self.port)
        client2 = KVClient(self.host, self.port)
        client1.init()
        client2.init()

        key_num = 10
        key_list = [self.random_str(15) for _ in range(key_num)]
        val_list1 = [self.random_str(20) for _ in range(key_num)]
        write_mode = WriteMode.NONE_L2_CACHE
        existence_opt = ExistenceOpt.NONE
        self.assertEqual(client1.mset(key_list, val_list1, write_mode, 0, existence_opt), [])
        get_vals1 = client2.get(key_list, True)
        self.assertListEqual(val_list1, get_vals1)

        val_list2 = [self.random_str(20) for _ in range(key_num)]
        self.assertEqual(client1.mset(key_list, val_list2, write_mode, 0, existence_opt), [])
        get_vals2 = client2.get(key_list, True)
        self.assertListEqual(get_vals2, val_list2)

        self.assertEqual(client1.delete(key_list), [])

    def test_mset_nx_mode(self):
        """
        Feature: Test mset values by nx existence_opt.
        """
        Context.set_trace_id("test_m_set_value")
        client1 = KVClient(self.host, self.port)
        client1.init()

        key_num = 10
        key_list = [self.random_str(15) for _ in range(key_num)]
        val_list1 = [self.random_str(20) for _ in range(key_num)]
        write_mode = WriteMode.NONE_L2_CACHE
        existence_opt = ExistenceOpt.NX
        with self.assertRaisesRegex(RuntimeError, r"The MSet doesn't support ntx_1"):
            client1.mset(key_list, val_list1, write_mode, 0, existence_opt)

        self.assertEqual(client1.delete(key_list), [])

    def test_mset_tx(self):
        """
        Feature: Test msettx values.
        """
        Context.set_trace_id("test_m_set_value")
        client1 = KVClient(self.host, self.port)
        client2 = KVClient(self.host, self.port)
        client1.init()
        client2.init()

        key_num = 5
        key_list = [self.random_str(15) for _ in range(key_num)]
        val_list1 = [self.random_str(20) for _ in range(key_num)]
        write_mode = WriteMode.NONE_L2_CACHE
        existence_opt = ExistenceOpt.NX
        client1.msettx(key_list, val_list1, write_mode, 0, existence_opt)
        get_vals1 = client2.get(key_list, True)
        self.assertListEqual(get_vals1, val_list1)

        val_list2 = [self.random_str(20) for _ in range(key_num)]
        with self.assertRaisesRegex(RuntimeError, r"already exists in local worker"):
            client1.msettx(key_list, val_list2, write_mode, 0, existence_opt)

        self.assertEqual(client1.delete(key_list), [])

    def test_mset_tx_none_mode(self):
        """
        Feature: Test msettx none exist opt.
        """
        Context.set_trace_id("test_m_set_tx_value")
        client1 = KVClient(self.host, self.port)
        client1.init()

        key_num = 5
        key_list = [self.random_str(15) for _ in range(key_num)]
        val_list1 = [self.random_str(20) for _ in range(key_num)]
        write_mode = WriteMode.NONE_L2_CACHE
        existence_opt = ExistenceOpt.NONE
        with self.assertRaisesRegex(RuntimeError, r"The MSetTx only supports set not existence key now"):
            client1.msettx(key_list, val_list1, write_mode, 0, existence_opt)

        self.assertEqual(client1.delete(key_list), [])

    def test_mset_tx_exceed_max_num(self):
        """
        Feature: Test msettx exceed max key num.
        """
        Context.set_trace_id("test_m_set_tx_value")
        client1 = KVClient(self.host, self.port)
        client1.init()

        key_num = 10
        key_list = [self.random_str(15) for _ in range(key_num)]
        val_list1 = [self.random_str(20) for _ in range(key_num)]
        write_mode = WriteMode.NONE_L2_CACHE
        existence_opt = ExistenceOpt.NX
        with self.assertRaisesRegex(RuntimeError, r"The maximum size of keys in single operation is 8"):
            client1.msettx(key_list, val_list1, write_mode, 0, existence_opt)

        self.assertEqual(client1.delete(key_list), [])

    def test_generate_key(self):
        """
        Features: Generate a unique key for SET.
        """
        Context.set_trace_id("test_generate_key")
        client = KVClient(self.host, self.port)
        client.init()
        generate_key_len = 73
        key = client.generate_key()
        self.assertEqual(len(key), generate_key_len)
        self.assertNotEqual(key.find(';'), -1)

    def test_set_evictable_value(self):
        """
        Features: Set evictable value.
        """
        Context.set_trace_id("test_set_evictable_value")
        key = self.random_str()
        val = self.random_str(20)
        client = KVClient(self.host, self.port)
        client.init()
        param = WriteMode.NONE_L2_CACHE_EVICT
        client.set(key, val, param)

    def test_exist(self):
        """
        Features: Check the existence of the given keys in the data system.
        """
        Context.set_trace_id("test_exist")
        client = KVClient(self.host, self.port)
        client.init()

        key1 = self.random_str()
        key2 = self.random_str()
        key3 = self.random_str()
        val = self.random_str(20)
        keys = [key1, key2, key3]

        self.assertEqual(client.exist(keys), [False, False, False])

        client.set(key1, val)
        self.assertEqual(client.exist(keys), [True, False, False])

        client.set(key3, val)
        self.assertEqual(client.exist(keys), [True, False, True])

        client.delete([key1])
        self.assertEqual(client.exist([key1]), [False])

        client.delete([key3])
        self.assertEqual(client.exist(keys), [False, False, False])

        with self.assertRaisesRegex(RuntimeError, r"Invalid parameter"):
            client.exist([])

        max_keys = 10000
        keys_exceed = [self.random_str() for _ in range(max_keys + 1)]
        with self.assertRaisesRegex(RuntimeError, r"The objectKeys size exceed 10000"):
            client.exist(keys_exceed)

    def test_expire(self):
        """
        Feature: Set expiration time for keys.
        """
        Context.set_trace_id("test_expire")
        key1 = self.random_str()
        val1 = self.random_str(20)
        client1 = KVClient(self.host, self.port)
        client1.init()
        param1 = WriteMode.NONE_L2_CACHE
        ttl_second = 100
        client1.set(key1, val1, param1, ttl_second)

        key2 = self.random_str()
        val2 = self.random_str(20)
        client2 = KVClient(self.host, self.port)
        client2.init()
        param2 = WriteMode.NONE_L2_CACHE
        client1.set(key2, val2, param2, ttl_second)
        self.assertEqual(client1.exist([key1, key2]), [True, True])
        new_ttl = 1
        failed_key = client1.expire([key2], new_ttl)
        self.assertEqual(failed_key, [])
        time.sleep(2)
        self.assertEqual(client1.exist([key1, key2]), [True, False])
