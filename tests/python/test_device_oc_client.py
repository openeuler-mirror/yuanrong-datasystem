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
Object cache client python interface Test.
"""
from __future__ import absolute_import
import os
import random
import json
import time
import unittest

from yr.datasystem.hetero_client import (
    HeteroClient,
    Blob,
    DeviceBlobList,
)

try:
    import acl
except ImportError:
    acl = None


def check_acl():
    """
    Features: Return False if import acl failed or build without hetero.
    """
    return acl is not None and os.getenv("BUILD_HETERO", "off") == "on"


class TestDeviceOcClientMethods(unittest.TestCase):
    """
    Features: Object cache client python interface test.
    """

    @classmethod
    def setUpClass(cls):
        root_dir = os.path.dirname(os.path.abspath(".."))
        worker_env_path = os.path.join(root_dir, 'output', 'datasystem', 'service', 'worker_config.json')
        with open(worker_env_path, "r") as f:
            config = json.load(f)

        work_address = config.get("worker_address", {})
        TestDeviceOcClientMethods.work_addr = work_address.get("value")
        (
            TestDeviceOcClientMethods.host,
            port,
        ) = TestDeviceOcClientMethods.work_addr.split(":")
        TestDeviceOcClientMethods.port = int(port)

    @staticmethod
    def random_str(slen=10):
        """
        Features: Generate a random string for test.
        """
        seed = "1234567890abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ!@#%^*()_+=-"
        sa = []
        for _ in range(slen):
            sa.append(random.choice(seed))
        return "".join(sa)

    @staticmethod
    def init_test_hetero_client():
        """
        Features: Init test client.
        """
        client = HeteroClient(
            TestDeviceOcClientMethods.host,
            TestDeviceOcClientMethods.port,
            60000,
            "",
            "",
            "",
            "",
            "QTWAOYTTINDUT2QVKYUC",
            "MFyfvK41ba2giqM7**********KGpownRZlmVmHc",
        )
        client.init()
        return client

    def get_all_futures(self, future_lists):
        """Function Waiting for all futures to end."""
        timeout = 30000
        for future_list in future_lists:
            for future in future_list:
                future.get(timeout)

    def check_ptr_content(self, dev_ptr: int, value: str):
        """
        Features: Check the device pointer content
        """
        size = len(value)
        output_byte = bytes("0", "utf-8").zfill(size)
        output_ptr = acl.util.bytes_to_ptr(output_byte)
        ret = acl.rt.memcpy(output_ptr, size, dev_ptr, size, 2)
        self.assertEqual(ret, 0)
        self.assertEqual(output_byte, value.encode())

    def batch_data_check(self, out_data_blob_list, test_value):
        """Features: check mget result with mset"""
        for batch_info in out_data_blob_list:
            for info in batch_info.blob_list:
                self.check_ptr_content(info.dev_ptr, test_value)

    @unittest.skip # need center master
    def test_mset_and_mget(self):
        """
        Features: Test mset and mget device object.
        """
        device_id = 0
        acl.init()
        # device id default set 0
        acl.rt.set_device(device_id)
        client = self.init_test_hetero_client()
        obj_count = 128
        data_size = 1024 * 1024

        test_value = self.random_str(data_size)

        object_key_list = [self.random_str(10) for _ in range(obj_count)]

        in_data_blob_list = self._create_blob_list(device_id, obj_count, data_size, test_value)
        out_data_blob_list = self._create_empty_blob_list(device_id, obj_count, data_size)

        client.mset_d2h(object_key_list, in_data_blob_list)
        client.mget_h2d(object_key_list, out_data_blob_list, 60000)
        client.delete(object_key_list)

        self.batch_data_check(out_data_blob_list, test_value)
        acl.finalize()

    @unittest.skip
    def test_async_dev_delete(self):
        """
        Features: Test mset and mget device object.
        """
        src_device_id, dest_device_id = 6, 5
        obj_count = 128
        data_size = 1024 * 1024
        test_value = self.random_str(data_size)
        object_key_list = [self.random_str(10) for _ in range(obj_count)]

        child1 = os.fork()
        if child1 == 0:
            acl.init()
            acl.rt.set_device(src_device_id)
            in_data_blob_list = self._create_blob_list(src_device_id, obj_count, data_size, test_value)
            client = self.init_test_hetero_client()
            client.dev_mset(object_key_list, in_data_blob_list)
            time.sleep(2)
            acl.finalize()
        else:
            child2 = os.fork()
            if child2 == 0:
                acl.init()
                acl.rt.set_device(dest_device_id)
                out_data_blob_list = self._create_empty_blob_list(dest_device_id, obj_count, data_size)
                client = self.init_test_hetero_client()
                failed_keys = client.dev_mget(object_key_list, out_data_blob_list, 10 * 1000)
                self.batch_data_check(out_data_blob_list, test_value)
                self.assertEqual(len(failed_keys), 0)
                future = client.async_dev_delete(object_key_list)
                failed_keys = future.get()
                self.assertEqual(len(failed_keys), 0)
                acl.finalize()
            else:
                os.waitpid(child1, 0)
                os.waitpid(child2, 0)

    @unittest.skip
    def test_async_mset_and_async_mget(self):
        """
        Features: Test async_mset and async_mget device object.
        """
        device_id = 0
        acl.init()
        acl.rt.set_device(device_id)
        client = self.init_test_hetero_client()
        obj_count = 128
        data_size = 1024 * 1024
        timeout = 30 * 1000

        test_value = self.random_str(data_size)
        object_key_list = [self.random_str(10) for _ in range(obj_count)]
        in_data_blob_list = self._create_blob_list(device_id, obj_count, data_size, test_value)
        out_data_blob_list = self._create_empty_blob_list(device_id, obj_count, data_size)

        mset_future = client.async_mset_d2h(object_key_list, in_data_blob_list)
        failed_keys = mset_future.get(timeout)
        self.assertEqual(len(failed_keys), 0)
        mget_future = client.async_mget_h2d(object_key_list, out_data_blob_list, 60000)
        failed_keys = mget_future.get(timeout)
        self.assertEqual(len(failed_keys), 0)
        client.delete(object_key_list)
        self.batch_data_check(out_data_blob_list, test_value)
        acl.finalize()

    @unittest.skip
    def test_async_mget_fail(self):
        """
        Features: Test async_mset_d2h and async_mget_h2d device object.
        """
        device_id = 0
        acl.init()
        acl.rt.set_device(device_id)
        client = self.init_test_hetero_client()
        data_size = 1024 * 1024
        obj_count = 128
        test_value = self.random_str(data_size)
        object_key_list = [self.random_str(10) for _ in range(obj_count)]
        in_data_blob_list = self._create_blob_list(device_id, obj_count, data_size, test_value)
        out_data_blob_list = self._create_empty_blob_list(device_id, obj_count, data_size)
        not_exit_key = self.random_str(10)
        mset_future = client.async_mset_d2h(object_key_list, in_data_blob_list)

        # add not exit key
        object_key_list[0] = not_exit_key
        mget_future = client.async_mget_h2d(object_key_list, out_data_blob_list, 60000)

        failed_keys = mset_future.get()
        self.assertEqual(len(failed_keys), 0)
        failed_keys = mget_future.get()
        self.assertEqual(len(failed_keys), 1)
        self.assertEqual(failed_keys, [not_exit_key])

        client.delete(object_key_list)
        acl.finalize()

    def _create_blob_list(self, device_id, obj_count, data_size, test_value):
        blob_list = []
        for _ in range(obj_count):
            tmp_batch_list = []
            for _ in range(4):
                dev_ptr, _ = acl.rt.malloc(data_size, 0)
                acl.rt.memcpy(dev_ptr, data_size,
                              acl.util.bytes_to_ptr(test_value.encode()),
                              data_size, 1)
                blob = Blob(dev_ptr, data_size)
                tmp_batch_list.append(blob)
            blob_list.append(DeviceBlobList(device_id, tmp_batch_list))
        return blob_list

    def _create_empty_blob_list(self, device_id, obj_count, data_size):
        blob_list = []
        for _ in range(obj_count):
            tmp_batch_list = []
            for _ in range(4):
                dev_ptr, _ = acl.rt.malloc(data_size, 0)
                blob = Blob(dev_ptr, data_size)
                tmp_batch_list.append(blob)
            blob_list.append(DeviceBlobList(device_id, tmp_batch_list))
        return blob_list
