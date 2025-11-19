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

import acl
from datasystem.hetero_client import (
    HeteroClient,
    Blob,
    DeviceBlobList,
)


class HeteroClientExample():
    """This class shows the SDK usage example of the HeteroClient."""

    def __init__(self):
        parser = argparse.ArgumentParser(description="Hetero client python interface Test")
        parser.add_argument("--host", required=True, help="The IP of worker service")
        parser.add_argument("--port", required=True, type=int, help="The port of worker service")
        parser.add_argument("--device_id", type=int, default=0, help="The device id")
        args = parser.parse_args()
        self._host = args.host
        self._port = args.port
        self._device_id = args.device_id

    def dev_mset_and_dev_mget_example(self):
        """test dev_mset and dev_mget"""
        acl.init()
        acl.rt.set_device(self._device_id)
        client = HeteroClient(self._host, self._port)
        client.init()
        key = "key"
        value = bytes("val", encoding='utf8')
        size = len(value)
        in_dev_ptr, _ = acl.rt.malloc(size, 0)
        acl.rt.memcpy(in_dev_ptr, size, acl.util.bytes_to_ptr(value), size, 1)
        in_blob = Blob(in_dev_ptr, size)
        in_blob_list = [DeviceBlobList(self._device_id, [in_blob])]
        failed_keys = client.dev_mset([key], in_blob_list)
        if failed_keys:
            raise RuntimeError(f"dev_mset failed, failed keys: {failed_keys}")

        out_dev_ptr, _ = acl.rt.malloc(size, 0)
        out_blob = Blob(out_dev_ptr, size)
        out_blob_list = [DeviceBlobList(self._device_id, [out_blob])]
        sub_timeout_ms = 30_000
        failed_keys = client.dev_mget([key], out_blob_list, sub_timeout_ms)
        if failed_keys:
            raise RuntimeError(f"dev_mget failed, failed keys: {failed_keys}")
        acl.rt.free(in_dev_ptr)
        acl.rt.free(out_dev_ptr)
        acl.finalize()


if __name__ == '__main__':
    example = HeteroClientExample()
    example.dev_mset_and_dev_mget_example()
