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
Object cache client python interface.
"""
from __future__ import absolute_import

from enum import Enum
from typing import Dict
from yr.datasystem.lib import libds_client_py as ds
from yr.datasystem.util import Validator as validator


class WriteMode(Enum):
    """
    The `WriteMode` class defines the reliability of object.

    Currently, the following 'WriteMode' are supported:

    ===================================  ==================================================================
    Definition                            Description
    ===================================  ==================================================================
    `WriteMode.NONE_L2_CACHE`            Object only store in cache. Default mode.
    `WriteMode.WRITE_THROUGH_L2_CACHE`   Object store in cache and synchronized to L2.
    `WriteMode.WRITE_BACK_L2_CACHE`      Object store in cache and asynchronized to L2.
    `WriteMode.NONE_L2_CACHE_EVICT`      Object is volatile, if cache resource is lack it would be delete.
    ===================================  ==================================================================
    """

    NONE_L2_CACHE = ds.WriteMode.NONE_L2_CACHE
    WRITE_THROUGH_L2_CACHE = ds.WriteMode.WRITE_THROUGH_L2_CACHE
    WRITE_BACK_L2_CACHE = ds.WriteMode.WRITE_BACK_L2_CACHE
    NONE_L2_CACHE_EVICT = ds.WriteMode.NONE_L2_CACHE_EVICT


class ConsistencyType(Enum):
    """
    The `ConsistencyType` class defines the consistency of object.

    Currently, the following 'ConsistencyType' are supported:

    ===================================  ==================================================================
    Definition                            Description
    ===================================  ==================================================================
    `ConsistencyType.PRAM`               PRAM (Pipelined RAM) consistency.
    `ConsistencyType.CAUSAL`             Causal consistency.
    ===================================  ==================================================================
    """

    PRAM = ds.ConsistencyType.PRAM
    CAUSAL = ds.ConsistencyType.CAUSAL


class CacheType(Enum):
    """
    The `CacheType` class defines the consistency of object.

    Currently, the following 'CacheType' are supported:

    ===================================  ==================================================================
    Definition                            Description
    ===================================  ==================================================================
    `CacheType.MEMEORY`               Cache to memory.
    `CacheType.DISK`                  Cache to disk.
    ===================================  ==================================================================
    """

    MEMEORY = ds.CacheType.MEMORY
    DISK = ds.CacheType.DISK


class Buffer:
    """
    The `Buffer` class defines a Buffer in object cache.
    """

    def __init__(self):
        self._buffer = None
        self._dev_buffer = None

    def wlatch(self, timeout_sec=60):
        """
        Acquire the write-lock to protect the buffer from concurrent reads and writes.

        Args:
            timeout_sec(int): The try-lock `timeout_sec`, the default value is 60 seconds.

        Raises:
            TypeError: Raise a type error if `timeout_sec` is invalid.
            RuntimeError: Raise a runtime error if acquire write latch fails.
        """
        args = [["timeout_sec", timeout_sec, int]]
        validator.check_args_types(args)
        validator.check_param_range(
            "timeout_sec", timeout_sec, 1, validator.INT32_MAX_SIZE
        )
        self._check_buffer()
        latch_status = self._buffer.wlatch(timeout_sec)
        if latch_status.is_error():
            raise RuntimeError(latch_status.to_string())

    def unwlatch(self):
        """
        Release the write-lock.

        Raises:
            RuntimeError: Raise a runtime error if release write latch fails.
        """
        self._check_buffer()
        unlatch_status = self._buffer.unwlatch()
        if unlatch_status.is_error():
            raise RuntimeError(unlatch_status.to_string())

    def rlatch(self, timeout_sec=60):
        """
        Acquire the read-lock to protect the buffer from concurrent writes.

        Args:
            timeout_sec(int): The try-lock `timeout_sec`, the default value is 60 seconds.

        Raises:
            TypeError: Raise a type error if `timeout_sec` is invalid.
            RuntimeError: Raise a runtime error if acquire read latch fails.
        """
        args = [["timeout_sec", timeout_sec, int]]
        validator.check_args_types(args)
        validator.check_param_range(
            "timeout_sec", timeout_sec, 1, validator.INT32_MAX_SIZE
        )
        self._check_buffer()
        latch_status = self._buffer.rlatch(timeout_sec)
        if latch_status.is_error():
            raise RuntimeError(latch_status.to_string())

    def unrlatch(self):
        """
        Release the read-lock.

        Raises:
            RuntimeError: Raise a runtime error if release read latch fails.
        """
        self._check_buffer()
        unlatch_status = self._buffer.unrlatch()
        if unlatch_status.is_error():
            raise RuntimeError(unlatch_status.to_string())

    def mutable_data(self):
        """
        Get a mutable data memory view.

        Returns:
            The mutable memory view of the buffer.
        """
        self._check_buffer()
        return self._buffer.mutable_data()

    def immutable_data(self):
        """
        Get an immutable data memory view.

        Returns:
            The immutable memory view of the buffer.
        """
        self._check_buffer()
        return memoryview(self._buffer.immutable_data())

    def memory_copy(self, value):
        """
        Write data to the buffer.

        Args:
            value(memoryview, bytes or bytearray): the data to be copied to the buffer

        Raises:
            TypeError: Raise a type error if `value` is invalid.
            RuntimeError: Raise a runtime error if the copy fails.
        """
        args = [["value", value, memoryview, bytes, bytearray]]
        validator.check_args_types(args)
        self._check_buffer()
        copy_status = self._buffer.memory_copy(value)
        if copy_status.is_error():
            raise RuntimeError(copy_status.to_string())

    def publish(self, nested_object_keys=None):
        """
        Publish mutable data to the server.

        Args:
            nested_object_keys(list): Nested object key list that buffer object depending on.

        Raises:
            TypeError: Raise a type error if the input parameter is invalid.
            RuntimeError: Raise a runtime error if publish fails.
        """
        if nested_object_keys is None:
            nested_object_keys = []
        args = [["nested_object_keys", nested_object_keys, list]]
        validator.check_args_types(args)
        if not all(map(lambda id: isinstance(id, str), nested_object_keys)):
            raise TypeError(
                r"The input of nested_object_keys should be a list of strings. There exists error type."
            )
        self._check_buffer()
        pub_status = self._buffer.publish(nested_object_keys)
        if pub_status.is_error():
            raise RuntimeError(pub_status.to_string())

    def seal(self, nested_object_keys=None):
        """
        Publish immutable data to the server.

        Args:
            nested_object_keys(list):  Objects that depend on objectKey.

        Raises:
            TypeError: Raise a type error if the input parameter is invalid.
            RuntimeError: Raise a runtime error if the seal fails.
        """
        if nested_object_keys is None:
            nested_object_keys = []
        args = [["nested_object_keys", nested_object_keys, list]]
        validator.check_args_types(args)
        if not all(map(lambda id: isinstance(id, str), nested_object_keys)):
            raise TypeError(
                r"The input of nested_object_keys should be a list of strings. There exists error type."
            )
        self._check_buffer()
        seal_status = self._buffer.seal(nested_object_keys)
        if seal_status.is_error():
            raise RuntimeError(seal_status.to_string())

    def invalidate_buffer(self):
        """
        Invalidate data on the current host.

        Raises:
            RuntimeError: Raise a runtime error if invalidate fails.
        """
        self._check_buffer()
        invalidate_status = self._buffer.invalidate_buffer()
        if invalidate_status.is_error():
            raise RuntimeError(invalidate_status.to_string())

    def get_size(self):
        """
        Get the size of the buffer.

        Returns:
            size(int): data size of the buffer.
        """
        self._check_buffer()
        return self._buffer.get_size()

    def _set_buffer(self, buffer):
        """
        Set buffer.

        Args:
            buffer: The buffer created by the client
        """
        self._buffer = buffer

    def _check_buffer(self):
        """
        Check to make sure that self._buffer is not None.

        Raises:
            RuntimeError: Raise a runtime error if self._buffer is None
        """
        if self._buffer is None:
            raise RuntimeError(r"The buffer is None, please create it first.")


class ObjectClient:
    """
    The `ObjectClient` class defines a object cache client.

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
        oauth_client_id(str): The client id for tenant.
        oauth_client_secret(str): The client secret for tenant.
        oauth_url(str): The auth url of IAM.
        tenant_id(str): The tenant ID.
        enable_exclusive_connection(bool): Experimental feature: improves IPC performance between client and
        datasystem_worker

    Raises:
        TypeError: Raise a type error if the input parameter is invalid.


    Examples:
        >>>
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
        req_timeout_ms=0,
        enable_exclusive_connection=False
    ):
        """
        Constructor of the ObjectClient class

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
            ["enable_exclusive_connection", enable_exclusive_connection, bool]
        ]
        validator.check_args_types(args)
        self.client = ds.ObjectClient(
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

    @staticmethod
    def _check_or_set_default_create_param(param: Dict):
        key_consistency_type = "consistency_type"
        if param is None:
            param = {
                key_consistency_type: ConsistencyType.PRAM,
            }
        validator.check_args_types([["param", param, dict]])

        return validator.check_key_exists(param, [key_consistency_type])

    def init(self):
        """
        Init a client to connect to a worker.

        Raises:
            RuntimeError: Raise a runtime error if the client fails to connect to the worker.
        """
        init_status = self.client.init()
        if init_status.is_error():
            raise RuntimeError(init_status.to_string())

    def create(self, object_key, size, param=None):
        """
        Create an object buffer

        Args:
            object_key(str): The id of the object to be created.
            size(int): The size in bytes of object.
            param(dict): which contains the following three "key: value" pairs successively:
            (1) "consistency_type": consistency_type(Enum): Indicating which consistency type will be used.
                              There are 2 options:
                              1) ConsistencyType.PRAM;
                              2) ConsistencyType.CAUSAL;

        Returns:
            buffer: The object buffer.

        Raises:
            TypeError: Raise a type error if the input parameter is invalid.
            RuntimeError: Raise a runtime error if the client fails to connect to the worker.
        """
        params = self._check_or_set_default_create_param(param)
        consistency_type = params[0]

        args = [
            ["object_key", object_key, str],
            ["size", size, int],
            ["consistency_type", consistency_type, ConsistencyType],
        ]
        validator.check_args_types(args)
        create_status, buffer = self.client.create(
            object_key, size, consistency_type.value
        )
        if create_status.is_error():
            raise RuntimeError(create_status.to_string())
        buf = Buffer()
        buf._set_buffer(buffer)
        return buf

    def put(self, object_key, value, param=None, nested_object_keys=None):
        """
        Put the object data to the data system.

        Args:
            object_key(str): The id of the object to be created.
            value(memoryview, bytes or bytearray): the data to be put
            param(dict): which contains the following three "key: value" pairs successively:
            (1) "consistency_type": consistency_type(Enum): Indicating which consistency type will be used.
                               There are 2 options:
                               1) ConsistencyType.PRAM;
                               2) ConsistencyType.CAUSAL;
            nested_object_keys(list):  Objects that depend on objectKey.

        Raises:
            TypeError: Raise a type error if the input parameter is invalid.
            RuntimeError: Raise a runtime error if the put fails.
        """
        params = self._check_or_set_default_create_param(param)
        consistency_type = params[0]

        if nested_object_keys is None:
            nested_object_keys = []

        args = [
            ["object_key", object_key, str],
            ["value", value, memoryview, bytes, bytearray],
            ["consistency_type", consistency_type, ConsistencyType],
            ["nested_object_keys", nested_object_keys, list],
        ]
        validator.check_args_types(args)
        put_status = self.client.put(
            object_key,
            value,
            consistency_type.value,
            nested_object_keys,
        )
        if put_status.is_error():
            raise RuntimeError(put_status.to_string())

    def get(self, object_keys, timeout_ms):
        """
        Get the buffers corresponding to the designated object keys

        Args:
            object_keys(list): The ids of the objects to get.
            timeout_ms(int): The `timeout_ms` of the get operation.

        Returns:
            buffers(list): list of buffers for the given `object_keys`.

        Raises:
            TypeError: Raise a type error if the input parameter is invalid.
            RuntimeError: Raise a runtime error if failed to get all objects.
        """
        buffer_list = []
        args = [["object_keys", object_keys, list], ["timeout_ms", timeout_ms, int]]
        validator.check_args_types(args)
        validator.check_param_range(
            "timeout_ms", timeout_ms, 0, validator.INT32_MAX_SIZE
        )
        if not object_keys:
            raise RuntimeError(r"The input of object_keys list should not be empty")
        get_status, buffer_array = self.client.get(object_keys, timeout_ms)
        if get_status.is_error():
            raise RuntimeError(get_status.to_string())
        for buffer in buffer_array:
            buf = None
            if buffer is not None:
                buf = Buffer()
                buf._set_buffer(buffer)
            buffer_list.append(buf)
        return buffer_list

    def g_increase_ref(self, object_keys):
        """
        Increase the global reference of the given objects.

        Args:
            object_keys(list): The ids of the objects to be increased. It cannot be empty.

        Returns:
            failed_object_keys(list): list of failed object key.

        Raises:
            TypeError: Raise a type error if the input parameter is invalid.
            RuntimeError: Raise a runtime error if the increase fails for all objects.
        """
        args = [["object_keys", object_keys, list]]
        validator.check_args_types(args)
        if not object_keys:
            raise RuntimeError(r"The input of object_keys list should not be empty")
        g_inc_ref_status, failed_object_keys = self.client.g_increase_ref(object_keys)
        if g_inc_ref_status.is_error():
            raise RuntimeError(g_inc_ref_status.to_string())
        return failed_object_keys

    def g_decrease_ref(self, object_keys):
        """
        Decrease the global reference of the given objects.

        Args:
            object_keys(list): The ids of the objects to be decreased. It cannot be empty.

        Returns:
            failed_object_keys(list): list of failed object key.

        Raises:
            TypeError: Raise a type error if the input parameter is invalid.
            RuntimeError: Raise a runtime error if the decrease fails for all objects.
        """
        args = [["object_keys", object_keys, list]]
        validator.check_args_types(args)
        if not object_keys:
            raise RuntimeError(r"The input of object_keys list should not be empty")
        g_dec_ref_status, failed_object_keys = self.client.g_decrease_ref(object_keys)
        if g_dec_ref_status.is_error():
            raise RuntimeError(g_dec_ref_status.to_string())
        return failed_object_keys

    def query_global_ref_num(self, object_key):
        """Query object global reference in the cluster.

        Args:
            object_key(str): The id of the object to be queried. It cannot be empty.

        Returns:
            ref_num(int): The object's global reference num; -1 in case of failure.
        """
        args = [["object_key", object_key, str]]
        validator.check_args_types(args)
        ref_num = self.client.query_global_ref_num(object_key)
        return ref_num

    def generate_object_id(self, prefix: str = ''):
        """Add the workerUuid as a suffix to the objectKey.

        Args:
            prefix(str): The objectKey generated by user.

        Returns:
            object_key(str): The key with workerUuid.

        Raise:
            TypeError: Raise a type error if the input parameter is invalid.
            RuntimeError: Raise a runtime error if fails to generate object key.
        """
        args = [["prefix", prefix, str]]
        validator.check_args_types(args)
        status, object_key = self.client.generate_object_id(prefix)
        if status.is_error():
            raise RuntimeError(status.to_string())
        return object_key
