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
State cache client python interface.
"""
from __future__ import absolute_import
from collections import namedtuple
from enum import Enum
from typing import List

from yr.datasystem.lib import libds_client_py as ds
from yr.datasystem.util import Validator as Validator

class EmbClient:
    """
    Features: Data system State Cache Client management for python.
    """
 
    def __init__(
        self,
        host: str = "",
        port: int = 0,
        timeout_ms=60000,
        token: str = '',
        client_public_key: str = "",
        client_private_key: str = "",
        server_public_key: str = "",
        access_key="",
        secret_key="",
        tenant_id="",
        enable_cross_node_connection=False,
        req_timeout_ms=0,
        enable_exclusive_connection=False
    ):
        self._client = ds.EmbClient(
            host,
            port,
            timeout_ms,
            token,
            client_public_key,
            client_private_key,
            server_public_key,
            access_key,
            secret_key,
            tenant_id,
            enable_cross_node_connection,
            req_timeout_ms,
            enable_exclusive_connection
        )
 
    def init(self, tableKey: str, table_index: int, table_name: str, dim_size: int, tableCapacity: int, bucketNum: int,
                hashFunction: int, bucketCapacity: int, etcdserver: str, localPath: str):
        """ Init a client to connect to a worker.
 
        Raises:
            RuntimeError: Raise a runtime error if the client fails to connect to the worker.
        """
        status = self._client.Init(tableKey, table_index, table_name, dim_size, tableCapacity, bucketNum, hashFunction, bucketCapacity, etcdserver, localPath)
        if status.is_error():
            raise RuntimeError(status.to_string())
    
 
    def shutDown(self):
        status = self._client.ShutDown()
        if status.is_error():
            raise RuntimeError(status.to_string())
 
    def insert(self, keys: List[str], values: List[bytes | bytearray | str]):
        status = self._client.Insert(keys, values)
        if status.is_error():
            raise RuntimeError(status.to_string())
 
    def find(self, keys: List[int]) -> List[bytes]:
        status, values = self._client.Find(keys)
        if status.is_error():
            raise RuntimeError(status.to_string())
        return values

    def buildIndex(self):
        status = self._client.BuildIndex()
        if status.is_error():
            raise RuntimeError(status.to_string())
 
    def load(self, key_paths: List[str], value_paths: List[str], file_format: str):
        status = self._client.Load(key_paths, value_paths, file_format)
        if status.is_error():
            raise RuntimeError(status.to_string())