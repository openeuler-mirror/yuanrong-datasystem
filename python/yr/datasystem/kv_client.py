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
from yr.datasystem.object_client import WriteMode, CacheType
from yr.datasystem.util import Validator as validator


ReadParam = namedtuple(
    "ReadParam",
    ["key", "offset", "size"]
)
"""
Declare the attribute of each value to be read when using 'read' interface.

Args:
    key(str): Key of the value to be read.
    offset(int): Read the value and skip the offset length.
    size(int): Size of the value to be read.
"""


class ExistenceOpt(Enum):
    """
    Features: Wrapping the ExistenceOpt.
    """

    NONE = ds.ExistenceOpt.NONE
    NX = ds.ExistenceOpt.NX


class SetParam:
    """
    The set property of key
    """

    def __init__(self) -> None:
        self.write_mode = WriteMode.NONE_L2_CACHE
        self.ttl_second = 0
        self.existence = ExistenceOpt.NONE
        self.cache_type = CacheType.MEMEORY

    # Defines the reliability of object
    write_mode: WriteMode
    # Time-To-Live,unit is second
    ttl_second: int
    # Determines if setting is allowed when the key already exists.
    existence: ExistenceOpt
    # Decide where to store data
    cache_type: CacheType


class ReadOnlyBuffer:
    """
    Features: Wrapping a state value, i.e., a read only buffer.
    """

    def __init__(self, state_val_buffer: ds.StateValueBuffer):
        self._state_val_buffer = state_val_buffer

    def immutable_data(self, with_latch=False, timeout_sec=60) -> memoryview:
        """ Get an immutable data memory view.
        Args:
            with_latch(bool): Whether acquiring the latch before buffer getting.
            timeout_sec(int): The try-lock timeout_sec, the default value is 60 seconds.

        Returns:
            The immutable memory view of the buffer.

        Raises:
            TypeError: Raise a type error if the input parameter is invalid.
            RuntimeError: Raise a runtime error if acquire read latch fails.
        """
        args = [["with_latch", with_latch, bool], ["timeout_sec", timeout_sec, int]]
        validator.check_args_types(args)
        validator.check_param_range("timeout_sec", timeout_sec, 1, validator.INT32_MAX_SIZE)
        self._check_buffer()
        mem_view_buffer = self._state_val_buffer.ImmutableData(with_latch, timeout_sec)
        if mem_view_buffer is None:
            raise RuntimeError(r"The memory view buffer is none, we fail in rlatch")
        return memoryview(mem_view_buffer)

    def rlatch(self, timeout_sec=60):
        """ Acquire the read-lock to protect the buffer from concurrent writes.

        Args:
            timeout_sec(int): The try-lock timeout_sec, the default value is 60 seconds.

        Raises:
            TypeError: Raise a type error if the input parameter is invalid.
            RuntimeError: Raise a runtime error if acquire read latch fails.
        """
        args = [["timeout_sec", timeout_sec, int]]
        validator.check_args_types(args)
        validator.check_param_range("timeout_sec", timeout_sec, 1, validator.INT32_MAX_SIZE)
        self._check_buffer()
        latch_status = self._state_val_buffer.RLatch(timeout_sec)
        if latch_status.is_error():
            raise RuntimeError(latch_status.to_string())

    def unrlatch(self):
        """ Release the read-lock.

        Raises:
            RuntimeError: Raise a runtime error if release read latch fails.
        """
        self._check_buffer()
        unlatch_status = self._state_val_buffer.UnRLatch()
        if unlatch_status.is_error():
            raise RuntimeError(unlatch_status.to_string())

    def _check_buffer(self):
        """ Check to make sure that self._buffer is not None.

        Raises:
            RuntimeError: Raise a runtime error if self._buffer is None
        """
        if self._state_val_buffer is None:
            raise RuntimeError(r"The buffer is None, please create it first.")
        if not isinstance(self._state_val_buffer, ds.StateValueBuffer):
            raise TypeError(r"The state value buffer is incorrect.")


class KVClient:
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
        """Constructor of the KVClient class

        Args:
            host(str): The host of the worker. If the host is not filled in, we will initialize it from the environment
                       variable, and the initialization of other parameters (such as port, token, etc) is also similar.
            port(int): The port of the worker.
            timeout_ms(int): The timeout interval for the connection between the client and worker.
            token(str): A string used for authentication.
            client_public_key(str): The client's public key, for curve authentication.
            client_private_key(str): The client's private key, for curve authentication.
            server_public_key(str): The worker server's public key, for curve authentication.
            access_key(str): The access key used by AK/SK authorize.
            secret_key(str): The secret key for AK/SK authorize.
            tenant_id(str): The tenant ID.
            enable_cross_node_connection(bool): Indicates whether the client can connect to the standby node.
            req_timeout_ms(int): The timeout of request, when req_timeout_ms<=0, req_timeout_ms is the same
            with timeout_ms.
            enable_exclusive_connection(bool): Experimental feature: improves IPC performance between client and
            datasystem_worker.

        Raises:
            RuntimeError: Raise a runtime error if the client fails to connect to the worker.
            TypeError: Raise a type error if the input parameter is invalid.
        """
        args = [
            ["host", host, str], ["port", port, int], ["timeout_ms", timeout_ms, int],
            ["client_public_key", client_public_key, str], ["client_private_key", client_private_key, str],
            ["server_public_key", server_public_key, str], ["access_key", access_key, str],
            ["secret_key", secret_key, str],
            ["tenant_id", tenant_id, str], ["enable_cross_node_connection", enable_cross_node_connection, bool],
            ["enable_exclusive_connection", enable_exclusive_connection, bool]
        ]
        validator.check_args_types(args)
        validator.check_param_range("timeout_ms", timeout_ms, 0, validator.INT32_MAX_SIZE)
        self._client = ds.KVClient(
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

    def init(self):
        """ Init a client to connect to a worker.

        Raises:
            RuntimeError: Raise a runtime error if the client fails to connect to the worker.
        """
        status = self._client.Init()
        if status.is_error():
            raise RuntimeError(status.to_string())

    def set(self, key, val, write_mode=WriteMode.NONE_L2_CACHE, ttl_second=0):
        """ Invoke worker client to set the value of a key.

        Args:
            key(str): The key of string data.
            val(memoryview, bytes, bytearray, str): The data to be set.
            write_mode(WriteMode): controls whether data is written to the L2 cache to enhance data reliability.
                The options are as follows:
                WriteMode.NONE_L2_CACHE: indicates that data reliability is not required,
                WriteMode.WRITE_THROUGH_L2_CACHE: indicates that data is synchronously written to the L2 cache
                WriteMode.WRITE_BACK_L2_CACHE: indicates that data is asynchronously written to the L2 cache
                to improve data reliability.
                WriteMode.NONE_L2_CACHE_EVICT: indicates that data reliability is not required and evictable.
            ttl_second(uint32): controls the expire time of the data:
                If the value is greater than 0, the data will be deleted automatically after expired.
                If set to 0, the data need to be manually deleted.
        Raises:
            RuntimeError: Raise a runtime error if fails to set the value of the key.
            TypeError: Raise a type error if the input parameter is invalid.
        """
        args = [
            ["key", key, str],
            ["val", val, memoryview, bytes, bytearray, str],
            [
                "write_mode", write_mode, type(WriteMode.NONE_L2_CACHE), type(WriteMode.WRITE_BACK_L2_CACHE),
                type(WriteMode.WRITE_THROUGH_L2_CACHE), type(WriteMode.NONE_L2_CACHE_EVICT),
            ],
            ["ttl_second", ttl_second, int]
        ]
        validator.check_args_types(args)

        if isinstance(val, str):
            val = str.encode(val)
        status = self._client.Set(key, val, write_mode.value, ttl_second)
        if status.is_error():
            raise RuntimeError(status.to_string())

    def set_value(self, val, write_mode=WriteMode.NONE_L2_CACHE, ttl_second=0):
        """ Invoke worker client to set the value of a key.

        Args:
            val(memoryview, bytes, bytearray, str): The data to be set.
            write_mode(WriteMode): controls whether data is written to the L2 cache to enhance data reliability.
                The options are as follows:
                WriteMode.NONE_L2_CACHE: indicates that data reliability is not required,
                WriteMode.WRITE_THROUGH_L2_CACHE: indicates that data is synchronously written to the L2 cache
                WriteMode.WRITE_BACK_L2_CACHE: indicates that data is asynchronously written to the L2 cache
                to improve data reliability.
                WriteMode.NONE_L2_CACHE_EVICT: indicates that data reliability is not required and evictable.
            ttl_second(uint32): controls the expire time of the data:
                If the value is greater than 0, the data will be deleted automatically after expired.
                If set to 0, the data need to be manually deleted.

        Returns:
            key: The key of object. If set failed, it will return empty string.

        Raises:
            RuntimeError: Raise a runtime error if fails to set the value of the key.
            TypeError: Raise a type error if the input parameter is invalid.
        """
        args = [
            ["val", val, memoryview, bytes, bytearray, str],
            [
                "write_mode", write_mode, type(WriteMode.NONE_L2_CACHE), type(WriteMode.WRITE_THROUGH_L2_CACHE),
                type(WriteMode.WRITE_BACK_L2_CACHE)
            ],
            ["ttl_second", ttl_second, int]
        ]
        validator.check_args_types(args)
        if isinstance(val, str):
            val = str.encode(val)
        key = self._client.SetValue(val, write_mode.value, ttl_second)
        return key

    def mset(self, keys, vals, write_mode=WriteMode.NONE_L2_CACHE, ttl_second=0, existence_opt=ExistenceOpt.NONE):
        """Multi-key set interface, it can batch set keys and return failed keys. The max keys size < 2000 and
           the max value for key to set < 500 * 1024.

        Args:
            keys(str): The keys of objects.
            vals(memoryview, bytes, bytearray, str): The vals to set.
            write_mode(WriteMode): controls whether data is written to the L2 cache to enhance data reliability.
                The options are as follows:
                WriteMode.NONE_L2_CACHE: indicates that data reliability is not required,
                WriteMode.WRITE_THROUGH_L2_CACHE: indicates that data is synchronously written to the L2 cache
                WriteMode.WRITE_BACK_L2_CACHE: indicates that data is asynchronously written to the L2 cache
                to improve data reliability.
                WriteMode.NONE_L2_CACHE_EVICT: indicates that data reliability is not required and evictable.
            ttl_second(uint32): controls the expire time of the data:
                If the value is greater than 0, the data will be deleted automatically after expired.
                If set to 0, the data need to be manually deleted.
            existence_opt(ExistenceOpt): Controlling the behavior of Set keys.
                The options are as follows:
                ExistenceOpt.NONE: Set the key without checking whether the key exists.
                ExistenceOpt.NX(Not support): Only set the key if it does not already exist.

        Returns:
            failedKeys: Return the keys that failed to set.

        Raises:
            RuntimeError: Raise a runtime error if all the keys set fail.
            TypeError: Raise a type error if the input parameter is invalid.
        """
        args = [
            ["keys", keys, list],
            ["vals", vals, list],
            ["write_mode", write_mode, type(WriteMode.NONE_L2_CACHE), type(WriteMode.WRITE_THROUGH_L2_CACHE),
             type(WriteMode.WRITE_BACK_L2_CACHE)],
            ["ttl_second", ttl_second, int],
            ["existence_opt", existence_opt, type(ExistenceOpt.NONE), type(ExistenceOpt.NX)]
        ]
        validator.check_args_types(args)
        vals = [val.encode() if isinstance(val, str) else val for val in vals]

        status, failed_keys = self._client.MSet(keys, vals, write_mode.value, ttl_second, existence_opt.value)
        if status.is_error():
            raise RuntimeError(status.to_string())
        return failed_keys

    def msettx(self, keys, vals, write_mode=WriteMode.NONE_L2_CACHE, ttl_second=0, existence_opt=ExistenceOpt.NONE):
        """Transactional multi-key set interface, it guarantees all the keys are either successfully created or
           none of them is created. The number of keys should be in the range of 1 to 8.

        Args:
            keys(str): The keys of objects.
            vals(memoryview, bytes, bytearray, str): The vals to set.
            write_mode(WriteMode): controls whether data is written to the L2 cache to enhance data reliability.
                The options are as follows:
                WriteMode.NONE_L2_CACHE: indicates that data reliability is not required,
                WriteMode.WRITE_THROUGH_L2_CACHE: indicates that data is synchronously written to the L2 cache
                WriteMode.WRITE_BACK_L2_CACHE: indicates that data is asynchronously written to the L2 cache
                to improve data reliability.
                WriteMode.NONE_L2_CACHE_EVICT: indicates that data reliability is not required and evictable.
            ttl_second(uint32): controls the expire time of the data:
                If the value is greater than 0, the data will be deleted automatically after expired.
                If set to 0, the data need to be manually deleted.
            existence_opt(ExistenceOpt): Controlling the behavior of Set keys.
                The options are as follows:
                ExistenceOpt.NONE(Not support): Set the key without checking whether the key exists.
                ExistenceOpt.NX: Only set the key if it does not already exist.

        Raises:
            RuntimeError: Raise a runtime error if one of the keys set fail.
            TypeError: Raise a type error if the input parameter is invalid.
        """
        args = [
            ["keys", keys, list],
            ["vals", vals, list],
            ["write_mode", write_mode, type(WriteMode.NONE_L2_CACHE), type(WriteMode.WRITE_THROUGH_L2_CACHE),
             type(WriteMode.WRITE_BACK_L2_CACHE)],
            ["ttl_second", ttl_second, int],
            ["existence_opt", existence_opt, type(ExistenceOpt.NONE), type(ExistenceOpt.NX)]
        ]
        validator.check_args_types(args)
        vals = [val.encode() if isinstance(val, str) else val for val in vals]

        status = self._client.MSetTx(keys, vals, write_mode.value, ttl_second, existence_opt.value)
        if status.is_error():
            raise RuntimeError(status.to_string())

    def get_read_only_buffers(self, keys: list = None, timeout_ms=0) -> List[ReadOnlyBuffer]:
        """ Get the values of all given keys.

        Args:
            keys(list): The key list of string type.
            timeout_ms(int): TimeoutMs of waiting for the result return if object not ready.
                A positive integer number required. 0 means no waiting time allowed.
        Returns:
            values(list): The value list of keys. If the key is not found, it will raise RuntimeError.

        Raises:
            RuntimeError: Raise a runtime error if fails to get the value of all keys.
            TypeError: Raise a type error if the input parameter is invalid.
        """
        args = [["keys", keys, list], ["timeout_ms", timeout_ms, int]]
        validator.check_args_types(args)
        validator.check_param_range("timeout_ms", timeout_ms, 0, validator.INT32_MAX_SIZE)
        status, buffer_array = self._client.GetReadOnlyBuffers(keys, timeout_ms)  # returned value is bytes type
        if status.is_error():
            raise RuntimeError(status.to_string())

        buffer_list = []
        for buffer in buffer_array:
            buf = None
            if buffer is not None:
                buf = ReadOnlyBuffer(buffer)
            buffer_list.append(buf)

        return buffer_list

    def get(self, keys: list = None, convert_to_str: bool = False, sub_timeout_ms=0):
        """ Get the values of all given keys.

        Args:
            keys(list): The key list of string type.
            convert_to_str(bool): True: convert the return value to string, False: return bytes directly.
            sub_timeout_ms(int): TimeoutMs of waiting for the result return if object not ready.
                A positive integer number required. 0 means no waiting time allowed.
        Returns:
            values(list): The value list of keys. If the key is not found, it will raise RuntimeError.

        Raises:
            RuntimeError: Raise a runtime error if fails to get the value of all keys.
            TypeError: Raise a type error if the input parameter is invalid.
        """
        args = [["keys", keys, list], ["sub_timeout_ms", sub_timeout_ms, int]]
        validator.check_args_types(args)
        if keys is None:
            raise RuntimeError(r"The input of keys list should not be empty")
        validator.check_param_range("sub_timeout_ms", sub_timeout_ms, 0, validator.INT32_MAX_SIZE)
        status, values = self._client.Get(keys, sub_timeout_ms)  # returned value is bytes type
        if status.is_error():
            raise RuntimeError(status.to_string())
        if convert_to_str is False:
            return values
        ret_vals = []
        for val in values:
            ret_vals.append(val.decode() if val is not None else None)
        return ret_vals

    def read(self, read_params) -> List[ReadOnlyBuffer]:
        """Some data in an object can be read based on the specified key and parameters.
           In some scenarios, read amplification can be avoided.

        Args:
            read_params(list): List of namedtuple ReadParam.

        Returns:
            read_only_buffer_list(list): Return List[ReadOnlyBuffer]. If the key is not found,
                                         it will raise RuntimeError.

        Raises:
            RuntimeError: Raise a runtime error if fails to get the value of one key.
            TypeError: Raise a type error if the input parameter is invalid.
        """
        args = [["read_params", read_params, list]]
        validator.check_args_types(args)
        if read_params is None:
            raise RuntimeError(r"The input of read_param list should not be empty")
        cpp_read_params = []
        for read_param in read_params:
            cpp_read_params.append(ds.ReadParam.build(read_param.key, read_param.offset, read_param.size))

        status, buffer_array = self._client.ReadSpecifyOffsetData(cpp_read_params)
        if status.is_error():
            raise RuntimeError(status.to_string())

        read_only_buffer_list = []
        for buffer in buffer_array:
            buf = None
            if buffer is not None:
                buf = ReadOnlyBuffer(buffer)
            read_only_buffer_list.append(buf)

        return read_only_buffer_list

    def delete(self, keys: list = None):
        """ Delete the values of all given keys.

        Args:
            keys(list): The data list of string type.

        Returns:
            failed_keys(list): The failed delete keys.

        Raises:
            RuntimeError: Raise a runtime error if fails to delete the value of all keys.
            TypeError: Raise a type error if the input parameter is invalid.
        """
        args = [["keys", keys, list]]
        validator.check_args_types(args)
        if keys is None:
            raise RuntimeError(r"The input of keys list should not be empty")
        status, failed_keys = self._client.Del(keys)
        if status.is_error():
            raise RuntimeError(status.to_string())
        return failed_keys

    def generate_key(self, prefix: str = ''):
        """ Generate a unique key for SET.

        Returns:
            key(string): The unique key, if the key fails to be generated, an empty string is returned.

        Examples:
            >>> from yr.datasystem.kv_client import KVClient
            >>> client = KVClient('127.0.0.1', 18482)
            >>> client.init()
            >>> client.generate_key()
            '0a595240-5506-4c7c-b1f7-7abfb1eb4add;b053480f-75bf-41dd-8ce5-6f9ef58e9de4'
        """
        args = [["prefix", prefix, str]]
        validator.check_args_types(args)
        status, key = self._client.generate_key(prefix)
        if status.is_error():
            raise RuntimeError(status.to_string())
        return key

    def exist(self, keys) -> list:
        """
        Check the existence of the given keys in the data system.

        Args:
            keys(list): The data list of string type. Constraint: The number of keys cannot exceed 10,000.

        Returns:
            exists(list): The existence of the corresponding key.

        Raises:
            RuntimeError: Raise a runtime error if all keys do not exist.
            TypeError: Raise a type error if the input parameter is invalid.

        Examples:
            >>> from yr.datasystem.kv_client import KVClient
            >>> client = KVClient('127.0.0.1', 18482)
            >>> keys = [self.random_str() for _ in range(3)]
            >>> val = self.random_str(20)
            >>> client.set(keys[0], val)
            >>> client.set(keys[2], val)
            >>> client.exist(keys)
            [True, False, True]
        """
        args = [["keys", keys, list]]
        validator.check_args_types(args)
        status, exists = self._client.exist(keys)
        if status.is_error():
            raise RuntimeError(status.to_string())
        return exists

    def expire(self, keys, ttl_second) -> list:
        """
        Set the expiration time for key list (in seconds).

        Args:
            keys(list): The data list of string type. Constraint: The number of keys cannot exceed 10,000.
            ttl_second(uint32): TTL in seconds. If the value is greater than 0, the data will be deleted automatically
                after expired. If set to 0, the data need to be manually deleted.

        Returns:
            failed_keys(list): The keys that setting expiration time failed.

        Raises:
            RuntimeError: Raise a runtime error if all keys expiring failed.
            TypeError: Raise a type error if the input parameter is invalid.

        Examples:
            >>> from yr.datasystem.kv_client import KVClient
            >>> client = KVClient('127.0.0.1', 18482)
            >>> keys = [self.random_str() for _ in range(3)]
            >>> val = self.random_str(20)
            >>> ttl_second = 6000
            >>> client.set(keys[0], val)
            >>> client.set(keys[2], val)
            >>> client.expire(keys, ttl_second)
        """
        args = [["keys", keys, list], ["ttl_second", ttl_second, int]]
        validator.check_args_types(args)
        status, failed_keys = self._client.expire(keys, ttl_second)
        if status.is_error():
            raise RuntimeError(status.to_string())
        return failed_keys
