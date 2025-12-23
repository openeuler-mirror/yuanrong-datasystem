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
Python module init.
"""

__all__ = [
    "Buffer",
    "ConsistencyType",
    "DsClient",
    "HeteroClient",
    "ObjectClient",
    "KVClient",
    "Status",
    "StreamClient",
    "SubconfigType",
    "WriteMode",
    "Context",
    "FutureTimeoutException",
    "Blob",
    "DeviceBlobList",
    "DsTensorClient",
    "CopyRange"
    "MetaInfo",
    "Future"
]

from yr.datasystem.object_client import Buffer, ConsistencyType
from yr.datasystem.object_client import ObjectClient, WriteMode
from yr.datasystem.lib.libds_client_py import FutureTimeoutException
from yr.datasystem.stream_client import SubconfigType, StreamClient
from yr.datasystem.ds_client import DsClient
from yr.datasystem.kv_client import KVClient
from yr.datasystem.hetero_client import HeteroClient, Blob, DeviceBlobList, MetaInfo, Future
from yr.datasystem.util import Status, Context


# Dynamically load DsTensorClient
# Delay dependency checking until the class is actually used to avoid forcing dependency on torch
def __getattr__(name):
    if name == "DsTensorClient":
        from yr.datasystem.ds_tensor_client import DsTensorClient
        return DsTensorClient
    if name == "CopyRange":
        from yr.datasystem.ds_tensor_client import CopyRange
        return CopyRange
    raise AttributeError(f"module {__name__} has no attribute {name}")
