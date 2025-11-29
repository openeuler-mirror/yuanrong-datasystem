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
Test datasystem tensor client python interface.
"""
import logging
from multiprocessing import Process, Barrier
import json
import os
import random
import time
import unittest

is_torch_exist = True
is_mindspore_exist = True
is_tensor_client_exist = True

try:
    import acl
    import numpy as np
    import torch
    import torch_npu
except ImportError:
    is_torch_exist = False

try:
    import mindspore as ms
except ImportError:
    is_mindspore_exist = False

try:
    from datasystem import DsTensorClient, CopyRange
except ImportError:
    is_tensor_client_exist = False

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(message)s',
)
logger = logging.getLogger(__name__)


class TestDsTensorClient(unittest.TestCase):
    """
    Features: Ds Tensor client python interface test.
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
        return "".join(sa)

    @staticmethod
    def batch_tensors_check(actual_tensors, expected_tensors):
        """Check tensor data"""
        assert len(actual_tensors) == len(expected_tensors), "Tensor count mismatch"

        for actual, expected in zip(actual_tensors, expected_tensors):
            cpu_actual = actual.cpu()
            cpu_expected = expected.cpu()
            assert torch.allclose(cpu_actual, cpu_expected), "Tensor data mismatch"

    @staticmethod
    def init_test_tensor_client(device_id):
        """
        Features: Init test client.
        """
        client = DsTensorClient(TestDsTensorClient.work_ip, TestDsTensorClient.work_port, device_id)
        client.init()
        return client

    @classmethod
    def setUpClass(cls):
        root_dir = os.path.dirname(os.path.abspath('..'))
        worker_env_path = os.path.join(root_dir, 'output', 'service', 'worker_config.json')
        with open(worker_env_path, "r") as f:
            config = json.load(f)

        work_address = config.get("worker_address", {})
        work_addr = work_address.get("value")
        cls.host, cls.port = work_addr.split(":")
        cls.port = int(cls.port)

    def run_send_kvcache(self, device_id, keys, send_tensors_cpu):
        """Function to run dev_send."""
        acl.init()
        acl.rt.set_device(device_id)

        client = self.init_test_tensor_client(device_id)

        torch_npu.npu.set_device(f'npu:{device_id}')
        send_tensors_npu = [tensor.to('npu') for tensor in send_tensors_cpu]

        futures = client.dev_send(keys, send_tensors_npu)
        timeout_ms = 10 * 1000
        for future in futures:
            future.get(timeout_ms)
        acl.finalize()

    def run_recv_kvcache(self, device_id, keys, expect_tensors):
        """Function to run dev_recv."""
        acl.init()
        acl.rt.set_device(device_id)

        client = self.init_test_tensor_client(device_id)

        torch_npu.npu.set_device(f'npu:{device_id}')
        recv_tensors = [torch.empty((expect_tensors[0].shape[0], expect_tensors[0].shape[1]), dtype=torch.float,
                                    device=f'npu:{device_id}') for _ in expect_tensors]
        futures = client.dev_recv(keys, recv_tensors)
        timeout_ms = 10 * 1000
        for future in futures:
            future.get(timeout_ms)
        self.batch_tensors_check(recv_tensors, expect_tensors)
        acl.finalize()

    @unittest.skipUnless(is_torch_exist and is_tensor_client_exist, "Run when dependency is exist")
    def test_dev_recv_and_dev_send(self):
        """Test dev_send and dev_recv"""
        src_device_id, dest_device_id = 6, 7
        key_num = 1
        keys = [self.random_str(10) for _ in range(key_num)]
        send_tensors_cpu = [torch.rand(10, 20, dtype=torch.float) for _ in range(key_num)]
        child1 = os.fork()
        if child1 == 0:
            self.run_send_kvcache(src_device_id, keys, send_tensors_cpu)
        else:
            child2 = os.fork()
            if child2 == 0:
                self.run_recv_kvcache(dest_device_id, keys, send_tensors_cpu)
            else:
                os.waitpid(child1, 0)
                os.waitpid(child2, 0)

    @unittest.skipUnless(is_torch_exist and is_tensor_client_exist, "Run when dependency is exist")
    def test_async_mset_d2h_and_async_mget_h2d(self):
        """Test async_mset_d2h and async_mget_h2d."""
        device_id = 0
        acl.init()
        acl.rt.set_device(device_id)
        client = self.init_test_tensor_client(device_id)
        key_num = 2
        keys = [self.random_str(10) for _ in range(key_num)]

        torch_npu.npu.set_device(f'npu:{device_id}')
        swap_out_tensors = [
            torch.rand((1, 1024), dtype=torch.float16, device=f'npu:{device_id}')
            for _ in range(key_num)
        ]
        swap_in_tensors = [
            torch.zeros((1, 1024), dtype=torch.float16, device=f'npu:{device_id}')
            for _ in range(key_num)
        ]

        timeout_ms = 10 * 1000

        mset_future = client.async_mset_d2h(keys, swap_out_tensors)
        failed_keys = mset_future.get(timeout_ms)
        self.assertEqual(len(failed_keys), 0)

        mget_future = client.async_mget_h2d(keys, swap_in_tensors)
        failed_keys = mget_future.get(timeout_ms)
        self.assertEqual(len(failed_keys), 0)

        client.delete(keys)

        self.batch_tensors_check(swap_in_tensors, swap_out_tensors)

    @unittest.skipUnless(is_torch_exist and is_tensor_client_exist, "Run when dependency is exist")
    def test_mset_d2h_and_mget_h2d(self):
        """Test mset_d2h and mget_h2d device object."""
        device_id = 7
        acl.init()
        acl.rt.set_device(device_id)
        client = self.init_test_tensor_client(device_id)
        key_num = 2
        keys = [self.random_str(10) for _ in range(key_num)]

        torch_npu.npu.set_device(f'npu:{device_id}')
        swap_out_tensors = [
            torch.rand((1, 1024), dtype=torch.float16, device=f'npu:{device_id}')
            for _ in range(key_num)
        ]
        swap_in_tensors = [
            torch.zeros((1, 1024), dtype=torch.float16, device=f'npu:{device_id}')
            for _ in range(key_num)
        ]
        client.mset_d2h(keys, swap_out_tensors)
        client.mget_h2d(keys, swap_in_tensors)

        client.delete(keys)

        self.batch_tensors_check(swap_in_tensors, swap_out_tensors)

    @unittest.skipUnless(is_mindspore_exist and is_tensor_client_exist, "Run when dependency is exist")
    def test_mset_and_mget_with_mindspore_tensor(self):
        """Test mset and mget device object."""
        device_id = 7
        acl.init()
        acl.rt.set_device(device_id)
        client = self.init_test_tensor_client(device_id)
        key_num = 2
        keys = [self.random_str(10) for _ in range(key_num)]

        # vllm-mindspore to set_device
        device = torch.device(f"cuda:{device_id}")
        torch.cuda.set_device(device)
        swap_out_tensors = [torch.rand((1, 1024), dtype=torch.float16) for _ in range(key_num)]
        swap_in_tensors = [torch.zeros((1, 1024), dtype=torch.float16) for _ in range(key_num)]
        client.mset_d2h(keys, swap_out_tensors)
        client.mget_h2d(keys, swap_in_tensors)

        client.delete(keys)

        self.batch_tensors_check(swap_in_tensors, swap_out_tensors)

    @unittest.skipUnless(is_mindspore_exist and is_tensor_client_exist, "Run when dependency is exist")
    def test_dev_mset_and_dev_mget_with_mindspore_tensor(self):
        """Test dev_mset and dev_mget device object."""
        src_device_id, dest_device_id = 0, 1
        key_num = 1
        keys = [self.random_str(10) for _ in range(key_num)]
        datas = [np.random.rand(2, 3) for _ in range(key_num)]
        child1 = os.fork()
        if child1 == 0:
            acl.init()
            acl.rt.set_device(src_device_id)
            ms.set_device(device_target="Ascend", device_id=src_device_id)
            send_tensors_npu = [ms.Tensor(data, dtype=ms.float32) + 0 for data in datas]

            client = self.init_test_tensor_client(src_device_id)
            failed_keys = client.dev_mset(keys, send_tensors_npu)
            self.assertEqual(len(failed_keys), 0)
            time.sleep(4)
            acl.finalize()
        else:
            child2 = os.fork()
            if child2 == 0:
                acl.init()
                acl.rt.set_device(dest_device_id)
                ms.set_device(device_target="Ascend", device_id=dest_device_id)
                recv_tensors = [ms.Tensor(np.ones(shape=[2, 3]), dtype=ms.float32) + 0 for _ in range(key_num)]

                client = self.init_test_tensor_client(dest_device_id)
                failed_keys = client.dev_mget(keys, recv_tensors, 20 * 1000)
                self.assertEqual(len(failed_keys), 0)
                failed_keys = client.dev_delete(keys)
                self.assertEqual(len(failed_keys), 0)

                expect_tensors_npu = [ms.Tensor(data, dtype=ms.float32) + 0 for data in datas]
                self.batch_tensors_check(expect_tensors_npu, recv_tensors)
                acl.finalize()
            else:
                os.waitpid(child1, 0)
                os.waitpid(child2, 0)

    def run_put_page_attn_layerwise_d2d(self, device_id, keys, send_tensors_cpu, block_ids):
        """Function to run put_page_attn_layerwise_d2d."""
        acl.init()
        acl.rt.set_device(device_id)
        client = self.init_test_tensor_client(device_id)
        client.dev_delete(keys)

        torch_npu.npu.set_device(f'npu:{device_id}')
        send_tensors_npu = [tensor.to('npu') for tensor in send_tensors_cpu]
        futures = client.put_page_attn_layerwise_d2d(keys, send_tensors_npu, block_ids)
        timeout_ms = 10 * 1000
        for future in futures:
            future.get(timeout_ms)
        acl.finalize()

    def run_get_page_attn_layerwise_d2d(self, device_id, keys, expect_tensors, block_ids):
        """Function to run get_page_attn_layerwise_d2d."""
        acl.init()
        acl.rt.set_device(device_id)
        client = self.init_test_tensor_client(device_id)

        torch_npu.npu.set_device(f'npu:{device_id}')
        recv_tensors = [
            torch.empty_like(expect_tensors[0], device=f'npu:{device_id}')
            for _ in expect_tensors
        ]
        futures = client.get_page_attn_layerwise_d2d(keys, recv_tensors, block_ids)
        timeout_ms = 10 * 1000
        for future in futures:
            future.get(timeout_ms)
        self.batch_tensors_check(recv_tensors, expect_tensors)
        acl.finalize()

    @unittest.skipUnless(is_torch_exist and is_tensor_client_exist, "Run when dependency is exist")
    def test_put_page_attn_layerwise_d2d_and_get_page_attn_layerwise_d2d(self):
        """Test put_page_attn_layerwise_d2d and get_page_attn_layerwise_d2d."""
        src_device_id, dest_device_id = 6, 7
        key_num = 1
        keys = [self.random_str(10) for _ in range(key_num)]
        send_tensors_cpu = [torch.rand(4, 32, 128, 16, dtype=torch.float32) for _ in range(key_num)]
        block_ids = [0, 1, 2, 3]
        child1 = os.fork()
        if child1 == 0:
            self.run_put_page_attn_layerwise_d2d(src_device_id, keys, send_tensors_cpu, block_ids)
        else:
            child2 = os.fork()
            if child2 == 0:
                self.run_get_page_attn_layerwise_d2d(dest_device_id, keys, send_tensors_cpu, block_ids)
            else:
                os.waitpid(child1, 0)
                os.waitpid(child2, 0)

    @unittest.skipUnless(is_torch_exist and is_tensor_client_exist, "Run when dependency is exist")
    def test_mset_page_attn_blockwise_d2h_and_mget_page_attn_blockwise_h2d(self):
        """Test mset_page_attn_blockwise_d2h and mget_page_attn_blockwise_h2d."""
        device_id = 7
        acl.init()
        acl.rt.set_device(device_id)
        client = self.init_test_tensor_client(device_id)
        key_num = 4
        keys = [self.random_str(10) for _ in range(key_num)]

        torch_npu.npu.set_device(f'npu:{device_id}')
        swap_out_tensors = [
            torch.rand((4, 32, 128, 16), dtype=torch.float16, device=f'npu:{device_id}')
            for _ in range(key_num)
        ]
        swap_in_tensors = [
            torch.zeros((4, 32, 128, 16), dtype=torch.float16, device=f'npu:{device_id}')
            for _ in range(key_num)
        ]
        block_ids = [0, 1, 2, 3]

        timeout_ms = 10 * 1000

        mset_future = client.mset_page_attn_blockwise_d2h(keys, swap_out_tensors, block_ids)
        failed_keys = mset_future.get(timeout_ms)
        self.assertEqual(len(failed_keys), 0)

        mget_future = client.mget_page_attn_blockwise_h2d(keys, swap_in_tensors, block_ids)
        failed_keys = mget_future.get(timeout_ms)
        self.assertEqual(len(failed_keys), 0)

        client.delete(keys)

        self.batch_tensors_check(swap_in_tensors, swap_out_tensors)

    @unittest.skipUnless(is_mindspore_exist and is_tensor_client_exist, "Run when dependency is exist")
    def test_async_dev_delete(self):
        """Test async_dev_delete device object."""
        src_device_id, dest_device_id = 4, 5
        key_num = 100
        keys = [self.random_str(10) for _ in range(key_num)]
        datas = [np.random.rand(2, 3) for _ in range(key_num)]
        child1 = os.fork()
        if child1 == 0:
            acl.init()
            acl.rt.set_device(src_device_id)
            ms.set_device(device_target="Ascend", device_id=src_device_id)
            send_tensors_npu = [ms.Tensor(data, dtype=ms.float32) + 0 for data in datas]

            client = self.init_test_tensor_client(src_device_id)
            failed_keys = client.dev_mset(keys, send_tensors_npu)
            self.assertEqual(len(failed_keys), 0)
            time.sleep(4)
            acl.finalize()
        else:
            child2 = os.fork()
            if child2 == 0:
                acl.init()
                acl.rt.set_device(dest_device_id)
                ms.set_device(device_target="Ascend", device_id=dest_device_id)
                recv_tensors = [ms.Tensor(np.ones(shape=[2, 3]), dtype=ms.float32) + 0 for _ in range(key_num)]

                client = self.init_test_tensor_client(dest_device_id)
                failed_keys = client.dev_mget(keys, recv_tensors, 20 * 1000)
                self.assertEqual(len(failed_keys), 0)
                future = client.async_dev_delete(keys)
                failed_keys = future.get()
                self.assertEqual(len(failed_keys), 0)

                expect_tensors_npu = [ms.Tensor(data, dtype=ms.float32) + 0 for data in datas]
                self.batch_tensors_check(expect_tensors_npu, recv_tensors)
                acl.finalize()
            else:
                os.waitpid(child1, 0)
                os.waitpid(child2, 0)

    @unittest.skipUnless(is_tensor_client_exist, "Run when dependency is exist")
    def test_invalid_input(self):
        """Test invalid input."""
        device_id = 0
        client = self.init_test_tensor_client(device_id)
        with self.assertRaises(TypeError):
            client.dev_send("only_key", [])

    @unittest.skipUnless(is_mindspore_exist and is_tensor_client_exist, "Run when dependency is exist")
    def test_tensor_is_not_contiguous(self):
        """Test non-contiguous tensor."""
        device_id = 7
        acl.init()
        acl.rt.set_device(device_id)
        ms.set_device(device_target="Ascend", device_id=device_id)

        client = self.init_test_tensor_client(device_id)
        x = ms.Tensor([[1, 2, 3], [4, 5, 6]], dtype=ms.float32)
        y = ms.ops.transpose(x, (1, 0))
        with self.assertRaises(TypeError):
            client.dev_mset(["key"], [y])
        acl.finalize()

    @unittest.skipUnless(is_torch_exist and is_tensor_client_exist, "Run when dependency is exist")
    def test_page_attn_layerwise_dbls(self):
        """Test page_attn_layerwise_dbls."""
        device_id = 7
        acl.init()
        acl.rt.set_device(device_id)
        client = self.init_test_tensor_client(device_id)

        t0 = torch.randn((20, 3, 4), dtype=torch.float16, device=f"npu:{device_id}")
        t1 = torch.randn((20, 3, 4), dtype=torch.float16, device=f"npu:{device_id}")

        dbls = client.page_attn_layerwise_dbls([t0, t1], [0, 1, 2])
        for dbl in dbls:
            blobs_list = dbl.get_blobs()
            for blob in blobs_list:
                size = blob.get_size()
                self.assertEqual(size, 24)

    @unittest.skipUnless(is_torch_exist and is_tensor_client_exist, "Run when dependency is exist")
    def test_page_attn_blockwise_dbls(self):
        """Test page_attn_blockwise_dbls."""
        device_id = 7
        acl.init()
        acl.rt.set_device(device_id)
        client = self.init_test_tensor_client(device_id)

        t0 = torch.randn((20, 3, 4), dtype=torch.float16, device=f"npu:{device_id}")
        t1 = torch.randn((20, 3, 4), dtype=torch.float16, device=f"npu:{device_id}")

        dbls = client.page_attn_blockwise_dbls([t0, t1], [0, 1, 2], device_id)
        for dbl in dbls:
            blobs_list = dbl.get_blobs()
            for blob in blobs_list:
                size = blob.get_size()
                self.assertEqual(size, 24)

    @unittest.skipUnless(is_mindspore_exist and is_tensor_client_exist, "Run when dependency is exist")
    def test_dev_mget_single_tensor_about_different_rank(self):
        """Test dev_mget_single_tensor."""
        src_device_id, dest_device_id = 5, 6
        column = 16

        key1 = [self.random_str(10)]
        child1 = os.fork()
        if child1 == 0:
            acl.init()
            acl.rt.set_device(src_device_id)
            ms.set_device(device_target="Ascend", device_id=src_device_id)

            client = self.init_test_tensor_client(src_device_id)
            key1_send_tensors_npu = [ms.Tensor(np.arange(column).reshape(2, int(column / 2)), dtype=ms.float32) + 0, ]
            failed_keys = client.dev_mset(key1, key1_send_tensors_npu)
            self.assertEqual(len(failed_keys), 0)
            time.sleep(4)
            acl.finalize()
        else:
            child2 = os.fork()
            if child2 == 0:
                acl.init()
                acl.rt.set_device(dest_device_id)
                ms.set_device(device_target="Ascend", device_id=dest_device_id)

                client = self.init_test_tensor_client(dest_device_id)
                recv_tensor = ms.Tensor(np.ones(shape=[1, int(column / 2)]), dtype=ms.float32) + 0
                data_size_byte = (int)(column / 2) * recv_tensor.itemsize
                copy_ranges = [
                    CopyRange(src_offset=0, dst_offset=0, length=data_size_byte)
                ]
                failed_keys = client.dev_mget_into_tensor(key1, recv_tensor, copy_ranges, 20 * 1000)
                self.assertEqual(len(failed_keys), 0)

                failed_keys = client.dev_delete(key1)
                self.assertEqual(len(failed_keys), 0)

                acl.finalize()
            else:
                os.waitpid(child1, 0)
                os.waitpid(child2, 0)

    @unittest.skipUnless(is_mindspore_exist and is_tensor_client_exist, "Run when dependency is exist")
    def test_dev_mget_into_tensor_in_same_rank(self):
        """Test dev_mget_into_tensor."""
        device_id = 5
        column = 50

        acl.init()
        acl.rt.set_device(device_id)
        ms.set_device(device_target="Ascend", device_id=device_id)

        client = self.init_test_tensor_client(device_id)
        key1 = [self.random_str(10)]
        key2 = [self.random_str(10)]
        key1_send_tensors_npu = [ms.Tensor(np.arange(column), dtype=ms.float32) + 0, ]
        key2_send_tensors_npu = [ms.Tensor(np.arange(100, column + 100), dtype=ms.float32) + 0, ]
        failed_keys = client.dev_mset(key1, key1_send_tensors_npu)
        self.assertEqual(len(failed_keys), 0)
        failed_keys = client.dev_mset(key2, key2_send_tensors_npu)
        self.assertEqual(len(failed_keys), 0)

        recv_tensor = ms.Tensor(np.ones(shape=[column]), dtype=ms.float32) + 0

        keys = key1 + key2
        data_size_byte = (int)(column / 2) * recv_tensor.itemsize
        copy_ranges = [
            CopyRange(src_offset=0, dst_offset=0, length=data_size_byte),
            CopyRange(src_offset=data_size_byte, dst_offset=data_size_byte, length=data_size_byte)
        ]
        failed_keys = client.dev_mget_into_tensor(keys, recv_tensor, copy_ranges, 20 * 1000)
        self.assertEqual(len(failed_keys), 0)

        failed_keys = client.dev_delete(keys)
        self.assertEqual(len(failed_keys), 0)

        acl.finalize()

    @unittest.skipUnless(is_mindspore_exist and is_tensor_client_exist, "Run when dependency is exist")
    def test_dev_d2d_dead_lock1(self):
        """Test the d2d deadlock."""
        local_rank_num = 8
        dtype = ms.float32
        shape = (2, 3)

        def task(i, barrier, local_rank_num):
            acl.init()
            acl.rt.set_device(i)
            ms.set_device(device_target="Ascend", device_id=i)
            client = self.init_test_tensor_client(i)
            keys = [f'{i}_{j}' for j in range(local_rank_num)]
            send_tensors = [ms.Tensor(np.ones(shape), dtype) + 0 for i in range(local_rank_num)]

            failed_keys = client.dev_mset(keys, send_tensors)
            assert len(failed_keys) == 0
            logger.info(f"device {i} set key_list:{keys} success")
            barrier.wait()

            get_keys = [f'{j}_{i}' for j in range(local_rank_num)]
            recv_tensors = [ms.Tensor(np.zeros(shape), dtype) + 0 for i in range(local_rank_num)]
            failed_keys = client.dev_mget(get_keys, recv_tensors, 60 * 1000)
            assert len(failed_keys) == 0

            self.batch_tensors_check(recv_tensors, send_tensors)
            logger.info(f"device {i} get key_list:{get_keys} success")

            barrier.wait()
            failed_keys = client.dev_delete(keys)
            assert len(failed_keys) == 0

        processes = []
        barrier = Barrier(local_rank_num)
        for i in range(local_rank_num):
            p = Process(target=task, args=(i, barrier, local_rank_num))
            processes.append(p)
            p.start()

        for p in processes:
            p.join()

    @unittest.skipUnless(is_mindspore_exist and is_tensor_client_exist, "Run when dependency is exist")
    def test_dev_d2d_dead_lock2(self):
        """Test the d2d deadlock."""
        local_rank_num = 8
        dtype = ms.float32
        shape = (2, 3)
        key_lists_formal = [f"device_id_{i}" for i in range(local_rank_num)]
        array_lists_formal = [np.random.randn(*shape) for _ in range(local_rank_num)]

        def task(i, barrier, local_rank_num):
            acl.init()
            acl.rt.set_device(i)
            ms.set_device(device_target="Ascend", device_id=i)
            client = self.init_test_tensor_client(i)
            send_tensors = [ms.Tensor(array_lists_formal[i], dtype) + 0]
            failed_keys = client.dev_mset([key_lists_formal[i]], send_tensors)
            assert len(failed_keys) == 0
            logger.info(f"device {i} set key_list:{key_lists_formal[i]} success")
            barrier.wait()

            key_lists = key_lists_formal[0: i] + key_lists_formal[i + 1::]
            recv_tensors = [ms.Tensor(np.ones(shape), dtype) + 0 for _ in range(local_rank_num - 1)]
            failed_keys = client.dev_mget(key_lists, recv_tensors, 60 * 1000)
            assert len(failed_keys) == 0
            logger.info(f"device {i} get key_list:{key_lists} success")

            barrier.wait()
            failed_keys = client.dev_delete(key_lists_formal)
            assert len(failed_keys) == 0

        processes = []
        barrier = Barrier(local_rank_num)
        for i in range(local_rank_num):
            p = Process(target=task, args=(i, barrier, local_rank_num))
            processes.append(p)
            p.start()

        for p in processes:
            p.join()

    @unittest.skipUnless(is_mindspore_exist and is_tensor_client_exist, "Run when dependency is exist")
    def test_sub_timeout_ms_error(self):
        """
        Test dev_mget_into_tensor with sub_timeout_ms errors.
        """
        device_id = 7
        acl.init()
        acl.rt.set_device(device_id)
        ms.set_context(device_target="Ascend", device_id=device_id)

        client = self.init_test_tensor_client(device_id)

        key1 = self.random_str(10)
        key_send_tensor_npu1 = [ms.Tensor(np.random.rand(1, 1024), dtype=ms.float32) + 0]

        failed_keys = client.dev_mset([key1], key_send_tensor_npu1)
        assert len(failed_keys) == 0

        recv_tensor = ms.Tensor(np.zeros((1, 1024)), dtype=ms.float32) + 0

        data_size_byte = (int)(1024) * recv_tensor.itemsize
        copy_ranges = [CopyRange(src_offset=0, dst_offset=0, length=data_size_byte)]

        sub_timeout_cases = [True, "1"]

        for sub_timeout in sub_timeout_cases:
            with self.assertRaises(TypeError):
                client.dev_mget_into_tensor([key1], recv_tensor, copy_ranges, sub_timeout)
        acl.finalize()
