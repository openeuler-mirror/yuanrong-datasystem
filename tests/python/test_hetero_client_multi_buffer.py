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
"""Unit tests for the HeteroClient synchronous multi-buffer APIs."""

import unittest

from yr.datasystem.hetero_client import HeteroClient
from yr.datasystem.kv_client import SetParam
from yr.datasystem.lib import libds_client_py as ds


class _Status:
    def __init__(self, message=""):
        self._message = message

    def is_error(self):
        return bool(self._message)

    def to_string(self):
        return self._message


class _NativeClient:
    def __init__(self):
        self.get_args = None
        self.set_args = None
        self.get_result = (_Status(), [])
        self.set_result = _Status()
        self.exist_args = None
        self.exist_result = (_Status(), [1, 0])

    def mget_h2d_from_multi_buffers(self, *args):
        self.get_args = args
        return self.get_result

    def mset_d2h_from_multi_buffers(self, *args):
        self.set_args = args
        return self.set_result

    def batch_is_exist(self, *args):
        self.exist_args = args
        return self.exist_result


class TestHeteroClientMultiBuffer(unittest.TestCase):
    def setUp(self):
        self.native_client = _NativeClient()
        self.client = HeteroClient.__new__(HeteroClient)
        self.client._client = self.native_client
        self.keys = ["key0", "key1"]
        self.addrs = [[100, 200], [300]]
        self.sizes = [[10, 20], [30]]

    def test_mget_forwards_multi_buffer_arguments(self):
        failed_keys = self.client.mget_h2d_from_multi_buffers(
            self.keys, self.addrs, self.sizes, 1234
        )

        self.assertEqual(failed_keys, [])
        self.assertEqual(
            self.native_client.get_args,
            (self.keys, self.addrs, self.sizes, 1234),
        )

    def test_mget_preserves_failed_keys(self):
        self.native_client.get_result = (_Status(), ["key1"])

        failed_keys = self.client.mget_h2d_from_multi_buffers(
            self.keys, self.addrs, self.sizes
        )

        self.assertEqual(failed_keys, ["key1"])

    def test_mset_forwards_multi_buffer_arguments_and_set_param(self):
        set_param = SetParam()
        set_param.ttl_second = 60

        self.client.mset_d2h_from_multi_buffers(
            self.keys, self.addrs, self.sizes, set_param
        )

        keys, addrs, sizes, native_set_param = self.native_client.set_args
        self.assertEqual(keys, self.keys)
        self.assertEqual(addrs, self.addrs)
        self.assertEqual(sizes, self.sizes)
        self.assertEqual(native_set_param.write_mode, ds.WriteMode(set_param.write_mode.value))
        self.assertEqual(native_set_param.existence, ds.ExistenceOpt(set_param.existence.value))
        self.assertEqual(native_set_param.ttl_second, 60)
        self.assertEqual(native_set_param.cache_type, ds.CacheType(set_param.cache_type.value))

    def test_native_errors_are_raised(self):
        self.native_client.get_result = (_Status("get failed"), [])
        with self.assertRaisesRegex(RuntimeError, "get failed"):
            self.client.mget_h2d_from_multi_buffers(self.keys, self.addrs, self.sizes)

        self.native_client.set_result = _Status("set failed")
        with self.assertRaisesRegex(RuntimeError, "set failed"):
            self.client.mset_d2h_from_multi_buffers(self.keys, self.addrs, self.sizes)

    def test_batch_is_exist_returns_native_integer_list(self):
        result = self.client.batch_is_exist(self.keys)

        self.assertEqual(result, [1, 0])
        self.assertIs(result, self.native_client.exist_result[1])
        self.assertEqual(self.native_client.exist_args, (self.keys,))

    def test_batch_is_exist_raises_native_error(self):
        self.native_client.exist_result = (_Status("exist failed"), [])

        with self.assertRaisesRegex(RuntimeError, "exist failed"):
            self.client.batch_is_exist(self.keys)

    def test_invalid_shapes_are_rejected_before_native_call(self):
        invalid_args = [
            ([], [], []),
            (self.keys, self.addrs[:1], self.sizes),
            (self.keys, [[], [300]], self.sizes),
            (self.keys, [[100], [300]], self.sizes),
        ]

        for keys, addrs, sizes in invalid_args:
            with self.subTest(keys=keys, addrs=addrs, sizes=sizes):
                with self.assertRaises(ValueError):
                    self.client.mget_h2d_from_multi_buffers(keys, addrs, sizes)

        self.assertIsNone(self.native_client.get_args)


if __name__ == "__main__":
    unittest.main()
