# Copyright (c) Huawei Technologies Co., Ltd. 2024. All rights reserved.
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
Hetero cache client python interface.
"""

from __future__ import absolute_import
from typing import List

from yr.datasystem.lib import libds_client_py as ds
from yr.datasystem.util import Validator as validator
from yr.datasystem.kv_client import SetParam


class Future:
    """
    Class for obtaining the execution result of an asynchronous task.

    Args:
        origin_future(ds.Future): Cpp native future.
    """

    def __init__(self, origin_future: ds.Future):
        self._future = origin_future

    def get(self, timeout_ms=60000):
        """
        To wait for and access the result of the asynchronous operation.

        Args:
            timeout_ms(int): Timeout interval for invoking.

        Returns:
            failed_keys(list): The failed keys.

        Raises:
            RuntimeError: Raise a runtime error if command execution timed out or failed.
        """
        result = self._future.get(timeout_ms)

        status = None
        failed_keys = []
        if isinstance(result, dict) and "failed_keys" in result:
            failed_keys = result["failed_keys"]
            status = result["status"]
        else:
            status = result

        if status.is_error():
            raise RuntimeError(status.to_string())

        return failed_keys


class MetaInfo:
    """Class used to describe metadata"""

    def __init__(self, blob_size_list) -> None:
        self.blob_size_list = blob_size_list

    # The deivce memory blob size list of key
    blob_size_list: list


class HeteroClient:
    """
    Data system Hetero Client management for python.

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
        enable_remote_h2d(bool): Whether the remote h2d feature is enabled or not, default off.

    Raises:
        TypeError: Raise a type error if the input parameter is invalid.
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
        enable_exclusive_connection=False,
        enable_remote_h2d=False
    ):
        """Constructor of the HeteroClient class

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
            enable_remote_h2d(bool): Whether the remote h2d feature is enabled or not, default off.

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
            ["enable_exclusive_connection", enable_exclusive_connection, bool],
            ["enable_remote_h2d", enable_remote_h2d, bool],
        ]
        validator.check_args_types(args)
        self._client = ds.HeteroClient(
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
            enable_exclusive_connection,
            enable_remote_h2d
        )

    @staticmethod
    def _change_blob_list_from_py_to_cpp(blob_lists: list):
        """
        Converts a mixed list of DeviceBlobList objects (either Python wrappers or native C++ objects)
        into a list of native C++ DeviceBlobList objects, suitable for passing to C++ APIs.

        Args:
            blob_lists (list): A list of DeviceBlobList objects. Elements can be either
                            Python wrapper instances or native C++ instances.

        Returns:
            list: A list containing only native C++ DeviceBlobList objects.
        """
        if not blob_lists:
            return []

        out_blob_lists = []
        for blob_list in blob_lists:
            if hasattr(blob_list, 'to_ds_blob_list'):
                cpp_blob_list = blob_list.to_ds_blob_list()
            else:
                cpp_blob_list = blob_list
            out_blob_lists.append(cpp_blob_list)

        return out_blob_lists

    def init(self):
        """
        Init a client to connect to a worker.

        Raises:
            RuntimeError: Raise a runtime error if the client fails to connect to the worker.
        """
        init_status = self._client.init()
        if init_status.is_error():
            raise RuntimeError(init_status.to_string())

    def mget_h2d(self, keys: list, data_blob_list: list, sub_timeout_ms: int):
        """
        Obtain data from the host and write the data to the device.
        mget_h2d and mset_d2h must be used together.
        If multiple memory addresses are combined and written to the host during mset_d2h, the host data is
        automatically split into multiple memory addresses and written to the device in mget_h2d.
        If the key of the host is no longer used, you can call the delete interface to delete it.

        Args:
            keys(list): Keys in the host. Constraint: The number of keys cannot exceed 10,000.
            data_blob_list(list): The list of data info.
            sub_timeout_ms(int): The sub_timeout_ms of the get operation.

        Returns:
            failed_keys(list): The keys that failed to get.

        Raises:
            RuntimeError: Raise a runtime error if failing to get device objects.
            TypeError: Raise a type error if the input parameter is invalid.
        """
        args = [["keys", keys, list],
                ["data_blob_list", data_blob_list, list],
                ["sub_timeout_ms", sub_timeout_ms, int]]
        validator.check_args_types(args)
        if (not keys) or (not data_blob_list):
            raise RuntimeError(r"The input of keys and data_blob_list should not be empty")
        cpp_list = self._change_blob_list_from_py_to_cpp(data_blob_list)
        status, failed_keys = self._client.mget_h2d(keys, cpp_list, sub_timeout_ms)
        if status.is_error():
            raise RuntimeError(status.to_string())
        return failed_keys

    def mset_d2h(self, keys: list, data_blob_list: list, set_param: SetParam = SetParam()) -> None:
        """
        Write the data of the device to the host. If the BLOB of the device contains multiple memory addresses,
        the device automatically combines data and writes the data to the host.
        If the key of the host is no longer used, you can call the delete interface to delete it.

        Args:
            keys(list): The data list of string type. Constraint: The number of keys cannot exceed 10,000.
            data_blob_list(list): The list of data info.
            set_param(SetParam): The set param , default value:
                write_mode = WriteMode.NONE_L2_CACHE
                ttl_second = 0
                existence = ExistenceOpt.NONE
                cache_type = CacheType.MEMEORY

        Returns:
            None.

        Raises:
            RuntimeError: Raise a runtime error if failing to put device objects.
            TypeError: Raise a type error if the input parameter is invalid.
        """
        args = [["keys", keys, list],
                ["data_blob_list", data_blob_list, list]]
        validator.check_args_types(args)
        if (not keys) or (not data_blob_list):
            raise RuntimeError(r"The input of keys and data_blob_list should not be empty")
        cpp_list = self._change_blob_list_from_py_to_cpp(data_blob_list)
        ds_set_param = ds.SetParam()
        ds_set_param.write_mode = ds.WriteMode(set_param.write_mode.value)
        ds_set_param.existence = ds.ExistenceOpt(set_param.existence.value)
        ds_set_param.ttl_second = set_param.ttl_second
        ds_set_param.cache_type = ds.CacheType(set_param.cache_type.value)
        status = self._client.mset_d2h(keys, cpp_list, ds_set_param)
        if status.is_error():
            raise RuntimeError(status.to_string())

    def async_mset_d2h(self, keys: list, data_blob_list: list, set_param: SetParam = SetParam()) -> Future:
        """
        Write the data of the device to the host. If the BLOB of the device contains multiple memory addresses,
        the device automatically combines data and writes the data to the host.
        If the key of the host is no longer used, you can call the delete interface to delete it.

        Args:
            keys(list): The data list of string type. Constraint: The number of keys cannot exceed 10,000.
            data_blob_list(list): The list of data info.
            set_param(SetParam): The set param , default value:
                write_mode = WriteMode.NONE_L2_CACHE
                ttl_second = 0
                existence = ExistenceOpt.NONE
                cache_type = CacheType.MEMEORY
        Returns:
            Future: You can use the get() method of the future object.

        Raises:
            RuntimeError: Raise a runtime error if failing to put device objects.
            TypeError: Raise a type error if the input parameter is invalid.
        """
        args = [["keys", keys, list],
                ["data_blob_list", data_blob_list, list]]
        validator.check_args_types(args)
        if (not keys) or (not data_blob_list):
            raise RuntimeError(r"The input of keys and data_blob_list should not be empty")
        cpp_list = self._change_blob_list_from_py_to_cpp(data_blob_list)
        ds_set_param = ds.SetParam()
        ds_set_param.write_mode = ds.WriteMode(set_param.write_mode.value)
        ds_set_param.existence = ds.ExistenceOpt(set_param.existence.value)
        ds_set_param.ttl_second = set_param.ttl_second
        ds_set_param.cache_type = ds.CacheType(set_param.cache_type.value)
        async_result_future = self._client.async_mset_d2h(keys, cpp_list, ds_set_param)
        return Future(async_result_future)

    def async_mget_h2d(self, keys: list, data_blob_list: list, sub_timeout_ms: int) -> Future:
        """
        Obtain data from the host and write the data to the device.
        mget_h2d and mset_d2h must be used together.
        If multiple memory addresses are combined and written to the host during mset_d2h, the host data is
        automatically split into multiple memory addresses and written to the device in mget_h2d.
        If the key of the host is no longer used, you can call the delete interface to delete it.

        Args:
            keys(list): Keys in the host. Constraint: The number of keys cannot exceed 10,000.
            data_blob_list(list): The list of data info.
            sub_timeout_ms(int): The sub_timeout_ms of the get operation.

        Returns:
            failed_keys(list): The keys that failed to get.

        Raises:
            RuntimeError: Raise a runtime error if failing to get device objects.
            TypeError: Raise a type error if the input parameter is invalid.
        """
        args = [["keys", keys, list],
                ["data_blob_list", data_blob_list, list],
                ["sub_timeout_ms", sub_timeout_ms, int]]
        validator.check_args_types(args)
        if (not keys) or (not data_blob_list):
            raise RuntimeError(r"The input of keys and data_blob_list should not be empty")
        cpp_list = self._change_blob_list_from_py_to_cpp(data_blob_list)
        async_result_future = self._client.async_mget_h2d(keys, cpp_list, sub_timeout_ms)
        return Future(async_result_future)

    def delete(self, keys: list = None) -> list:
        """
        Delete the key from the host.
        The delete interface works with mget_h2d and mset_d2h.

        Args:
            keys(list): The data list of string type. Constraint: The number of keys cannot exceed 10,000.

        Returns:
            failed_keys(list): The keys that failed to be deleted.

        Raises:
            RuntimeError: Raise a runtime error if failing to delete the value of all keys.
            TypeError: Raise a type error if the input parameter is invalid.
        """
        args = [["keys", keys, list]]
        validator.check_args_types(args)
        if keys is None:
            raise RuntimeError(r"The input of keys list should not be empty")
        status, failed_keys = self._client.delete(keys)
        if status.is_error():
            raise RuntimeError(status.to_string())
        return failed_keys

    def dev_publish(self, keys, data_blob_list) -> List[Future]:
        """
        Publish the memory on the device as a heterogeneous object of the data system.
        Heterogeneous objects can be obtained through dev_subscribe.
        dev_publish and dev_subscribe must be used together.
        The device memory addresses in the input parameters of the dev_publish and dev_subscribe interfaces cannot
        belong to the same NPU.
        After data is obtained through dev_subscribe, the data system automatically deletes the heterogeneous object
        and does not manage the device memory corresponding to the object.

        Args:
            keys(list): A list of keys corresponding to the data_blob_list.
            data_blob_list(list): A list of structures describing the Device memory.

        Returns:
            List[Future] is returned. You can use the get() method of the future object
            to wait for and access the result of HcclRecv.

        Raises:
            RuntimeError: Raise a runtime error if failing to publish the value of all keys.
            TypeError: Raise a type error if the input parameter is invalid.
        """
        args = [["keys", keys, list], ["data_blob_list", data_blob_list, list]]
        validator.check_args_types(args)
        if (not keys) or (not data_blob_list):
            raise RuntimeError(r"The input of keys and data_blob_list should not be empty")
        cpp_list = self._change_blob_list_from_py_to_cpp(data_blob_list)
        status, origin_future_list = self._client.dev_publish(keys, cpp_list)
        if status.is_error():
            raise RuntimeError(status.to_string())
        future_list = [Future(f) for f in origin_future_list]
        return future_list

    def dev_subscribe(self, keys, data_blob_list) -> List[Future]:
        """
        Subscribes to heterogeneous objects of the data system and writes data to data_blob_list.
        Data is directly transmitted through the device-to-device channel.
        dev_publish and dev_subscribe must be used together.
        The device memory addresses in the input parameters of the dev_publish and dev_subscribe interfaces cannot
        belong to the same NPU.
        After data is obtained through dev_subscribe, the data system automatically deletes the heterogeneous object
        and does not manage the device memory corresponding to the object.

        Args:
            keys(list): A list of keys corresponding to the data_blob_list.
            data_blob_list(list): A list of structures describing the Device memory.

        Returns:
            List[Future] is returned. You can use the get() method of the future object
            to wait for and access the result of HcclRecv.

        Raises:
            RuntimeError: Raise a runtime error if failing to subscribe to the values of all keys fails.
            TypeError: Raise a type error if the input parameter is invalid.
        """
        args = [["keys", keys, list], ["data_blob_list", data_blob_list, list]]
        validator.check_args_types(args)
        if (not keys) or (not data_blob_list):
            raise RuntimeError(r"The input of keys and data_blob_list should not be empty")
        cpp_list = self._change_blob_list_from_py_to_cpp(data_blob_list)
        status, origin_future_list = self._client.dev_subscribe(keys, cpp_list)
        if status.is_error():
            raise RuntimeError(status.to_string())
        future_list = [Future(f) for f in origin_future_list]
        return future_list

    def dev_mset(self, keys, data_blob_list) -> list:
        """
        The data system caches data on the device and writes the metadata of the key corresponding to
        data_blob_list to the data system so that other clients can access the data system.
        dev_mset and dev_mget must be used together. Heterogeneous objects are not automatically deleted after
        dev_mget is executed. If an object is no longer used, invoke dev_local_delete or dev_delete to delete it.
        The device memory addresses in the input parameters of the dev_mset and dev_mget interfaces cannot
        belong to the same NPU.

        Args:
            keys(list): A list of keys corresponding to the data_blob_list. Constraint: The number of keys
                        cannot exceed 10,000.
            data_blob_list(list): A list of structures describing the Device memory.

        Returns:
            failed_keys(list): The failed dev_mset keys.

        Raises:
            RuntimeError: Raise a runtime error if failing to set the value of all keys.
            TypeError: Raise a type error if the input parameter is invalid.
        """
        args = [["keys", keys, list], ["data_blob_list", data_blob_list, list]]
        validator.check_args_types(args)
        if (not keys) or (not data_blob_list):
            raise RuntimeError(r"The input of keys and data_blob_list should not be empty")
        cpp_list = self._change_blob_list_from_py_to_cpp(data_blob_list)
        status, failed_keys = self._client.dev_mset(keys, cpp_list)
        if status.is_error():
            raise RuntimeError(status.to_string())
        return failed_keys

    def dev_mget(self, keys, data_blob_list, sub_timeout_ms) -> list:
        """
        Obtains data from the device and writes the data to data_blob_list. Data is transmitted directly through
        the device-to-device channel.
        dev_mset and dev_mget must be used together. Heterogeneous objects are not automatically deleted after
        dev_mget is executed. If an object is no longer used, invoke dev_local_delete or dev_delete to delete it.
        The device memory addresses in the input parameters of the dev_mset and dev_mget interfaces cannot
        belong to the same NPU.

        Args:
            keys(list): A list of keys corresponding to the data_blob_list. Constraint: The number of keys
                        cannot exceed 10,000.
            data_blob_list(list): A list of structures describing the Device memory.
            sub_timeout_ms(int): The sub_timeout_ms of the get operation.

        Returns:
            failed_keys(list): The failed dev_mget keys.

        Raises:
            RuntimeError: Raise a runtime error if failing to get the value of all keys.
            TypeError: Raise a type error if the input parameter is invalid.
        """
        args = [["keys", keys, list], ["data_blob_list", data_blob_list, list], ["sub_timeout_ms", sub_timeout_ms, int]]
        validator.check_args_types(args)
        if (not keys) or (not data_blob_list):
            raise RuntimeError(r"The input of keys and data_blob_list should not be empty")
        cpp_list = self._change_blob_list_from_py_to_cpp(data_blob_list)
        status, failed_keys = self._client.dev_mget(keys, cpp_list, sub_timeout_ms)
        if status.is_error():
            raise RuntimeError(status.to_string())
        return failed_keys

    def dev_delete(self, keys: list = None) -> list:
        """
        Delete the device info from the host.
        The dev_delete interface is used together with the dev_mset / dev_mget interface.

        Args:
            keys(list): The data list of string type. Constraint: The number of keys cannot exceed 10,000.

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
        status, failed_keys = self._client.dev_delete(keys)
        if status.is_error():
            raise RuntimeError(status.to_string())
        return failed_keys

    def async_dev_delete(self, keys: list = None) -> Future:
        """
        Delete the device info from the host asynchronously.
        The dev_delete interface is used together with the dev_mset / dev_mget interface.

        Args:
            keys(list): The data list of string type. Constraint: The number of keys cannot exceed 10,000.

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
        async_result_future = self._client.async_dev_delete(keys)
        return Future(async_result_future)

    def dev_local_delete(self, keys) -> list:
        """
        dev_local_delete interface. After calling this interface, the data replica stored in the data system by the
        current client connection will be deleted.
        The dev_local_delete interface is used together with the dev_mset / dev_mget interface.

        Args:
            keys(list): A list of keys corresponding to the data_blob_list. Constraint: The number of keys
                        cannot exceed 10,000.

        Returns:
            failed_keys(list): The keys that failed to be deleted.

        Raises:
            RuntimeError: Raise a runtime error if fails to get the value of all keys.
            TypeError: Raise a type error if the input parameter is invalid.
        """
        args = [["keys", keys, list]]
        validator.check_args_types(args)
        if not keys:
            raise RuntimeError(r"The input of keys should not be empty")
        status, failed_keys = self._client.dev_local_delete(keys)
        if status.is_error():
            raise RuntimeError(status.to_string())
        return failed_keys

    def generate_key(self, prefix: str = '') -> str:
        """
        Generate a key with the Worker UUID of the data system.

        Returns:
            key(string): The unique key, if the key fails to be generated, an empty string is returned.

        Examples:
            >>> from yr.datasystem.hetero_client import HeteroClient
            >>> client = HeteroClient('127.0.0.1', 18482)
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
            TypeError: Raise a type error if the input parameter is invalid.

        Examples:
            >>> from yr.datasystem.hetero_client import HeteroClient
            >>> client = HeteroClient('127.0.0.1', 18482)
            >>> client.init()
            >>> client.exist(['key1', 'key2', 'key3'])
            [True, False, True]
        """
        args = [["keys", keys, list]]
        validator.check_args_types(args)
        status, exists = self._client.exist(keys)
        if status.is_error():
            raise RuntimeError(status.to_string())
        return exists

    def get_meta_info(self, keys, is_dev_key) -> list:
        """
        Get the meta info of keys.

        Args:
            keys(list): The data list of string type. Constraint: The number of keys cannot exceed 10,000.
            is_dev_key(bool):The type of keys, true represents the D2D type, while false represents the D2H type.

        Returns:
            list(list, list): return a list, include:
                - failed_keys(list): The failed keys.
                - meta_infos(list): The meta info of keys.

        Raises:
            TypeError: Raise a type error if the input parameter is invalid.
            RuntimeError: Raise a runtime error if fails to get the value of all keys.
        """

        args = [["keys", keys, list], ["is_dev_key", is_dev_key, bool]]
        validator.check_args_types(args)
        status, res = self._client.get_meta_info(keys, is_dev_key)
        if status.is_error():
            raise RuntimeError(status.to_string())
        failed_keys, raw_meta_infos = res
        meta_infos = [MetaInfo(raw_meta_info.blob_size_list) for raw_meta_info in raw_meta_infos]
        return failed_keys, meta_infos


class Blob:
    """
    Describes a memory segment on a device NPU.

    Args:
        dev_ptr(int): Blob memory address.
        size(int): Blob size in bytes.
    """

    def __init__(self, dev_ptr: int, size: int):
        self.dev_ptr = dev_ptr
        self.size = size

    def to_ds_blob(self) -> ds.Blob:
        """
        Wrap pythonic class to pybind11's Blob class.
        """
        return ds.Blob.build(self.dev_ptr, self.size)

    def set_dev_ptr(self, dev_ptr: int):
        """
        Updates the device memory pointer which stored in python class.

        Args:
            dev_ptr: The device memory pointer
        """
        self.dev_ptr = dev_ptr

    def set_size(self, size: int):
        """
        Updates the size of the device data which stored in python class.

        Args:
            size: The device data size
        """
        self.size = size


class DeviceBlobList:
    """
    Describes a group of memory on a device. Information about each memory segment is stored in a Blob.

    Args:
        dev_idx(int): Device index.
        blob_list(list): Blob list.
        src_offset(list): Expect to get data starting from the src_offset position on the sender.
    """

    def __init__(self, dev_idx: int, blob_list: list, src_offset: int = 0):
        self.dev_idx = dev_idx
        self.blob_list = blob_list
        self.src_offset = src_offset

    def to_ds_blob_list(self) -> ds.DeviceBlobList:
        """
        Wrap pythonic class to pybind11's DeviceBlobList class.
        """
        return ds.DeviceBlobList.build([blob.to_ds_blob() for blob in self.blob_list], self.dev_idx, self.src_offset)

    def append_blob(self, blob: Blob):
        """
        Add blob to bloblist.

        Args:
            blob: The device memory pointer and size data
        """
        self.blob.append(blob)

    def set_dev_idx(self, dev_idx: int):
        """
        Set the device index to the DeviceBlobList object.

        Args:
            dev_idx: The divice id
        """
        self.dev_idx = dev_idx

    def set_src_offset(self, src_offset: int):
        """
        Set the offset of src device data to the DeviceBlobList object.

        Args:
            src_offset: The starting offset of source device data
        """
        self.src_offset = src_offset

    def get_blob_list(self) -> list:
        """
        Get the list of blob.

        Returns:
            blob(list): list of blob.
        """
        return self.blob_list
