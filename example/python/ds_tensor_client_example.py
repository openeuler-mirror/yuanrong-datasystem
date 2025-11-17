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
Hetero client python interface test.
"""

from __future__ import absolute_import

import argparse
import logging

from datasystem import DsTensorClient

is_torch_exist = True
is_mindspore_exist = True

try:
    import acl
    import numpy
except ImportError:
    is_torch_exist = False
    is_mindspore_exist = False

try:
    import torch
    import torch_npu
except ImportError:
    is_torch_exist = False

try:
    import mindspore
except ImportError:
    is_mindspore_exist = False


class DsTensorClientExample():
    """This class shows the SDK usage example of the HeteroClient."""

    def __init__(self):
        parser = argparse.ArgumentParser(description="DsTensorClient python interface Test")
        parser.add_argument("--host", required=True, help="The IP of worker service")
        parser.add_argument("--port", required=True, type=int, help="The port of worker service")
        parser.add_argument("--device_id", type=int, default=0, help="The device id")
        args = parser.parse_args()
        self._host = args.host
        self._port = args.port
        self._device_id = args.device_id

        logging.basicConfig(level=logging.INFO)

    def torch_dev_mset_and_dev_mget_example(self):
        """test pytorch tensor"""
        logging.info("Start executing torch_dev_mset_and_dev_mget_example...")
        acl.init()
        acl.rt.set_device(self._device_id)
        torch_npu.npu.set_device(f'npu:{self._device_id}')

        key = "key"
        in_tensors = [torch.rand((2, 3), dtype=torch.float16, device=f'npu:{self._device_id}')]
        client = DsTensorClient(self._host, self._port, self._device_id)
        client.init()
        failed_keys = client.dev_mset([key], in_tensors)
        if failed_keys:
            raise RuntimeError(f"dev_mset failed, failed keys: {failed_keys}")

        out_tensors = [torch.zeros((2, 3), dtype=torch.float16, device=f'npu:{self._device_id}')]
        sub_timeout_ms = 30_000
        failed_keys = client.dev_mget([key], out_tensors, sub_timeout_ms)
        if failed_keys:
            raise RuntimeError(f"dev_mget failed, failed keys: {failed_keys}")
        acl.finalize()
        logging.info("Execute torch_dev_mset_and_dev_mget_example success.")

    def mindspore_dev_mset_and_dev_mget_example(self):
        """test mindspore tensor"""
        logging.info("Start executing mindspore_dev_mset_and_dev_mget_example...")
        acl.init()
        acl.rt.set_device(self._device_id)
        mindspore.set_device(device_target="Ascend", device_id=self._device_id)

        key = "key"
        data = numpy.random.rand(2, 3)
        in_tensors = [mindspore.Tensor(data, dtype=mindspore.float32) + 0]
        client = DsTensorClient(self._host, self._port, self._device_id)
        client.init()
        failed_keys = client.dev_mset([key], in_tensors)
        if failed_keys:
            raise RuntimeError(f"dev_mset failed, failed keys: {failed_keys}")

        out_tensors = [mindspore.Tensor(numpy.ones(shape=[2, 3]), dtype=mindspore.float32) + 0]
        sub_timeout_ms = 30_000
        failed_keys = client.dev_mget([key], out_tensors, sub_timeout_ms)
        if failed_keys:
            raise RuntimeError(f"dev_mget failed, failed keys: {failed_keys}")
        acl.finalize()
        logging.info("Execute mindspore_dev_mset_and_dev_mget_example success.")


if __name__ == '__main__':
    example = DsTensorClientExample()
    excute = False
    if is_torch_exist:
        example.torch_dev_mset_and_dev_mget_example()
        excute = True
    if is_mindspore_exist:
        example.mindspore_dev_mset_and_dev_mget_example()
        excute = True
    if not excute:
        logging.warning("No examples were executed.")
