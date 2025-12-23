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
Datasystem client python interface.
"""
from __future__ import absolute_import

from yr.datasystem.util import Validator as validator
from yr.datasystem.object_client import ObjectClient
from yr.datasystem.kv_client import KVClient
from yr.datasystem.hetero_client import HeteroClient


class DsClient:
    """
    Features: Datasystem Client management for python.
    """

    def __init__(
        self,
        host,
        port,
        connect_timeout_ms=60000,
        token="",
        client_public_key="",
        client_private_key="",
        server_public_key="",
        access_key="",
        secret_key="",
        tenant_id="",
        enable_cross_node_connection=False,
        req_timeout_ms=0,
        enable_exclusive_connection=False
    ):
        """Constructor of the DsClient class

        Args:
            host(str): The host of the worker address.
            port(int): The port of the worker address.
            connect_timeout_ms(int): The timeout_ms interval for the connection between the client and worker.
            token(str): A string used for authentication.
            client_public_key(str): The client's public key, for curve authentication.
            client_private_key(str): The client's private key, for curve authentication.
            server_public_key(str): The worker server's public key, for curve authentication.
            access_key(str): The access key used by AK/SK authorize.
            secret_key(str): The secret key for AK/SK authorize.
            tenant_id(str): The tenant ID.
            enable_cross_node_connection(bool): Indicates whether the client can connect to the standby node.
            req_timeout_ms(int): The timeout of request, when req_timeout_ms<=0, req_timeout_ms is the same with
            connect_timeout_ms.
            enable_exclusive_connection(bool): Experimental feature: improves IPC performance between client and
            datasystem_worker.

        Raises:
            TypeError: Raise a type error if the input parameter is invalid.
        """
        args = [
            ["host", host, str],
            ["port", port, int],
            ["connect_timeout_ms", connect_timeout_ms, int],
            ["token", token, str],
            ["client_public_key", client_public_key, str],
            ["client_private_key", client_private_key, str],
            ["server_public_key", server_public_key, str],
            ["access_key", access_key, str],
            ["secret_key", secret_key, str],
            ["tenant_id", tenant_id, str],
            ["enable_cross_node_connection", enable_cross_node_connection, bool],
            ["req_timeout_ms", req_timeout_ms, int],
        ]
        validator.check_args_types(args)
        self._kv_client = KVClient(
            host,
            port,
            connect_timeout_ms,
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
        self._hetero_client = HeteroClient(
            host,
            port,
            connect_timeout_ms,
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
        self._object_client = ObjectClient(
            host,
            port,
            connect_timeout_ms,
            token,
            client_public_key,
            client_private_key,
            server_public_key,
            access_key,
            secret_key,
            tenant_id,
            req_timeout_ms,
            enable_exclusive_connection
        )

    def init(self):
        """Init a client to connect to a worker.

        Raises:
            RuntimeError: Raise a runtime error if the client fails to connect to the worker.
        """
        self._kv_client.init()
        self._hetero_client.init()
        self._object_client.init()

    def kv(self):
        """Obtain the kv client instance. """
        return self._kv_client

    def hetero(self):
        """Obtain the hetero client instance. """
        return self._hetero_client

    def object(self):
        """Obtain the object client instance. """
        return self._object_client
