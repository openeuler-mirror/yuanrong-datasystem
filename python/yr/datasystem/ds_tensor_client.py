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
Datasystem tensor client python interface.
"""
from __future__ import absolute_import

from dataclasses import dataclass
from typing import List

from yr.datasystem.hetero_client import (
    HeteroClient,
    Blob,
    DeviceBlobList,
    Future
)
from yr.datasystem.lib import libds_client_py as ds
from yr.datasystem.kv_client import SetParam, WriteMode
from yr.datasystem.util import Validator as validator

try:
    from torch import Tensor
except ModuleNotFoundError as e:
    raise ModuleNotFoundError(
        "DsTensorClient requires either torch or mindspore to be installed. Please install the required package."
    ) from e


@dataclass
class CopyRange:
    """Represents a data copy range specification.

    Attributes:
        src_offset (int): Starting offset in the source data (in bytes).
        dst_offset (int): Starting offset in the destination Tensor (in bytes).
        length (int): Length of the data to be copied (in bytes).
    """
    src_offset: int
    dst_offset: int
    length: int


class PageAttnUtils:
    """
    Page Attention Utilities.
    """

    @staticmethod
    def blk_2_blob(ptr: int, elem_size: int, num_block_elem: int, block_id: int) -> ds.Blob:
        """
        Convert a block to a blob.

        Args:
            ptr (int): Device memory address.
            elem_size (int): Element size.
            num_block_elem (int): Number of block elements.
            block_id (int): Block id.

        Returns:
            The converted blob.
        """
        return ds.PageAttnUtils.blk_2_blob(ptr, elem_size, num_block_elem, block_id)

    @staticmethod
    def blks_2_dev_blob_list(
        device_idx: int,
        ptr: int,
        elem_size: int,
        num_block_elem: int,
        block_ids: list[int]
    ) -> ds.DeviceBlobList:
        """
        Convert a block list to a device blob list.

        Args:
            device_idx (int): Device index.
            ptr (int): Device memory address.
            elem_size (int): Element size.
            num_block_elem (int): Number of block elements.
            block_ids (list[int]): Block id list.

        Returns:
            The converted device blob list.
        """
        return ds.PageAttnUtils.blks_2_dev_blob_list(device_idx, ptr, elem_size, num_block_elem, block_ids)

    @staticmethod
    def blockwise_dev_blob_lists(
        device_idx: int,
        layer_tensors: list[ds.Tensor],
        block_ids: list[int]
    ) -> list[ds.DeviceBlobList]:
        """
        Convert a block list of layers to a device blob list, each block gives a device blob list.

        Args:
            device_idx (int): Device index.
            layer_tensors (list[ds.Tensor]): Layer tensors.
            block_ids (list[int]): block id lists.

        Returns:
            The converted device blob list.
        """
        status, dbls = ds.PageAttnUtils.blockwise_dev_blob_lists(device_idx, layer_tensors, block_ids)
        if status.is_error():
            raise RuntimeError(status.to_string())
        return dbls

    @staticmethod
    def layerwise_dev_blob_lists(
        device_idx: int,
        layer_tensors: list[ds.Tensor],
        block_ids: list[int]
    ) -> list[ds.DeviceBlobList]:
        """
        Convert a block list of layers to a device blob list, each layer gives a device blob list.

        Args:
            device_idx (int): Device index.
            layer_tensors (list[ds.Tensor]): Layer tensors.
            block_ids (list[int]): block id lists.

        Returns:
            The converted device blob list.
        """
        status, dbls = ds.PageAttnUtils.layerwise_dev_blob_lists(device_idx, layer_tensors, block_ids)
        if status.is_error():
            raise RuntimeError(status.to_string())
        return dbls


class DsTensorClient:
    """
    Data system Tensor Cache Client management for python, which provides named object support for
    efficient transfer of Tensor data between D2D or H2D/D2H.

    Args:
        host(str): The host of the worker address.
        port(int): The port of the worker address.
        device_id (int): The identifier of the device.
        connect_timeout_ms(int): The timeout_ms interval for the connection between the client and worker.
        token(str): A string used for authentication.
        client_public_key(str): The client's public key, for curve authentication.
        client_private_key(str): The client's private key, for curve authentication.
        server_public_key(str): The worker server's public key, for curve authentication.
        req_timeout_ms(int): The timeout of request, when req_timeout_ms<=0, req_timeout_ms is the same with
        connect_timeout_ms.
        enable_remote_h2d(bool): Whether the remote h2d feature is enabled or not, default off.
    Raises:
        TypeError: Raise a type error if the input parameter is invalid.
        RuntimeError: Raise a runtime error if the client failed to invoke api.
    """

    def __init__(
        self,
        host,
        port,
        device_id,
        connect_timeout_ms=60000,
        client_public_key="",
        client_private_key="",
        server_public_key="",
        req_timeout_ms=0,
        enable_remote_h2d=False
    ):
        args = [
            ["host", host, str],
            ["port", port, int],
            ["device_id", device_id, int],
            ["connect_timeout_ms", connect_timeout_ms, int],
            ["client_public_key", client_public_key, str],
            ["client_private_key", client_private_key, str],
            ["server_public_key", server_public_key, str],
            ["req_timeout_ms", req_timeout_ms, int],
        ]
        validator.check_args_types(args)
        self._hetero_client = HeteroClient(
            host=host,
            port=port,
            connect_timeout_ms=connect_timeout_ms,
            client_public_key=client_public_key,
            client_private_key=client_private_key,
            server_public_key=server_public_key,
            req_timeout_ms=req_timeout_ms,
            enable_remote_h2d=enable_remote_h2d
        )
        self._device_id = device_id

    @staticmethod
    def _get_tensor_device_type(tensor: Tensor) -> str:
        """Safely get tensor device type with proper error handling"""
        if hasattr(tensor, 'device') and isinstance(tensor.device, str):
            error_msg = ("tensor.device is a string, 'str' object has no attribute 'type'. This usually indicates that "
                        "MindSpore is being used without msadapter, or there's a version mismatch.\n "
                        "Solutions:\n "
                        "1. Install msadapter for proper tensor operations: \n"
                        "   pip install msadapter\n "
                        "2. Ensure MindSpore and msadapter version compatibility.\n")
            raise AttributeError(error_msg)

        return tensor.device.type

    @staticmethod
    def _is_ms_tensor(tensor: Tensor) -> str:
        """check if the tensor is mindspore type"""
        return DsTensorClient._get_tensor_device_type(tensor) == "Ascend"

    @staticmethod
    def _check_tensor_device_type(tensor: Tensor) -> None:
        """check the tensor type"""
        device_type = DsTensorClient._get_tensor_device_type(tensor)
        if device_type not in ["Ascend", "npu"]:
            raise ValueError(f"{device_type} tensor, not a npu/Ascend tensor")

    @staticmethod
    def _check_tensors_is_contiguous(tensors: List[Tensor]) -> None:
        """Check if the tensor memory layout is contiguous.

        Args:
            tensors: Input tensor list to be checked.

        Raises:
            TypeError: If the tensor memory is not contiguous.
        """
        for tensor in tensors:
            if not tensor.is_contiguous():
                raise TypeError(
                    f"Tensor memory is not contiguous. "
                    "Possible solutions:\n"
                    "1. Call tensor.contiguous() to create a contiguous copy\n"
                    "2. Check if the tensor was created with non-contiguous operations "
                    "(e.g. transpose, narrow, expand)\n"
                    "3. Verify tensor creation parameters (order='C' for C-contiguous)"
                )

    @staticmethod
    def _get_kv_tensor_data_ptr(tensor: Tensor) -> int:
        """Get kvtensor start data pointer"""
        if DsTensorClient._is_ms_tensor(tensor):
            element_size = tensor.element_size()
            return tensor.data_ptr() + (tensor.storage_offset() * element_size)
        return tensor.data_ptr()

    @classmethod
    def _page_attn_blockwise_dbls(
        cls,
        layer_tensors: list[Tensor],
        block_ids: list[int],
        device_id: int
    ) -> list[ds.DeviceBlobList]:
        """Convert the page attention block wise to pybind11's DeviceBlobList object"""
        kvc_tensors = cls._construct_layerwise_tensors(block_ids, layer_tensors)
        if DsTensorClient._is_ms_tensor(layer_tensors[0]):
            return PageAttnUtils.blockwise_dev_blob_lists(device_id, kvc_tensors, block_ids)
        return PageAttnUtils.blockwise_dev_blob_lists(layer_tensors[0].device.index, kvc_tensors, block_ids)

    @classmethod
    def _page_attn_layerwise_dbls(cls, layer_tensors: list[Tensor], block_ids: list[int]) -> list[ds.DeviceBlobList]:
        """Convert the page attention layer wise to pybind11's DeviceBlobList object"""
        kvc_tensors = cls._construct_layerwise_tensors(block_ids, layer_tensors)
        return PageAttnUtils.layerwise_dev_blob_lists(layer_tensors[0].device.index, kvc_tensors, block_ids)

    @classmethod
    def _construct_layerwise_tensors(cls, block_ids, layer_tensors) -> list[ds.Tensor]:
        """Construct layerwise tensors"""
        if not layer_tensors:
            raise ValueError("No layer tensor")
        if not block_ids:
            raise ValueError("No block id")
        for tensor in layer_tensors:
            cls._check_tensor_device_type(tensor)
            if tensor.device.index != layer_tensors[0].device.index:
                raise ValueError("Tensors not from a same device")

        kvc_tensors = [ds.Tensor(cls._get_kv_tensor_data_ptr(t), t.element_size(), list(t.shape))
                       for t in layer_tensors]
        return kvc_tensors

    def init(self) -> None:
        """
        Init a client to connect to a worker.

        Raises:
            RuntimeError: Raise a runtime error if the client fails to connect to the worker.
        """
        self._hetero_client.init()

    def mset_d2h(self, keys: List[str], tensors: List[Tensor], set_param: SetParam = SetParam()) -> None:
        """
        Write the tensors of the device to the host. If the data of the device contains multiple memory addresses,
        the device automatically combines data and writes the data to the host.
        If the key of the host is no longer used, you can call the delete interface to delete it.

        Args:
            keys (List[str]): List of keys associated with the tensors.
                              Constraint: The number of keys cannot exceed 10,000.
            tensors (List[Tensor]): List of tensors to send.
            set_param(SetParam): The set param , default value:
                write_mode = WriteMode.NONE_L2_CACHE
                ttl_second = 0
                existence = ExistenceOpt.NONE
                cache_type = CacheType.MEMEORY

        Returns:
            None.

        Raises:
            RuntimeError: Raise a runtime error if failing to mset_d2h the values of all keys.
            TypeError: Raise a type error if the input parameter is invalid.
        """
        args = [
            ["keys", keys, list],
            ["tensors", tensors, list]
        ]
        validator.check_args_types(args)
        self._check_tensors_is_contiguous(tensors)
        dev_blob_lists = [self._tensor_2_bloblist(tensor) for tensor in tensors]
        return self._hetero_client.mset_d2h(keys, dev_blob_lists, set_param)

    def mget_h2d(self, keys: List[str], tensors: List[Tensor], sub_timeout_ms: int = 0) -> list:
        """
        Obtain tensors from the host and write the tensors to the device.
        mget_h2d and mset_d2h must be used together.
        If multiple memory addresses are combined and written to the host during mset_d2h, the host data is
        automatically split into multiple memory addresses and written to the device in mget_h2d.
        If the key of the host is no longer used, you can call the delete interface to delete it.

        Args:
            keys (List[str]): List of keys associated with the tensors.
                              Constraint: The number of keys cannot exceed 10,000.
            tensors (List[Tensor]): List of tensors to store the retrieved data.
            sub_timeout_ms (int, optional): Timeout for the subscription in milliseconds.
                Defaults to 0.

        Returns:
            failed_keys(list): The keys that failed to get.

        Raises:
            RuntimeError: Raise a runtime error if failing to mget_h2d the values of all keys.
            TypeError: Raise a type error if the input parameter is invalid.
        """
        args = [
            ["keys", keys, list],
            ["tensors", tensors, list],
            ["sub_timeout_ms", sub_timeout_ms, int]
        ]
        validator.check_args_types(args)
        self._check_tensors_is_contiguous(tensors)
        dev_blob_lists = [self._tensor_2_bloblist(tensor) for tensor in tensors]
        return self._hetero_client.mget_h2d(keys, dev_blob_lists, sub_timeout_ms)

    def async_mset_d2h(self, keys: List[str], tensors: List[Tensor], set_param: SetParam = SetParam()) -> Future:
        """
        Write the tensors of the device to the host asynchronously. If the data of the device contains multiple memory
        addresses, the device automatically combines data and writes the data to the host.
        If the key of the host is no longer used, you can call the delete interface to delete it.

        Args:
            keys (List[str]): List of keys associated with the tensors.
                              Constraint: The number of keys cannot exceed 10,000.
            tensors (List[Tensor]): List of tensors to send.
            set_param(SetParam): The set param , default value:
                write_mode = WriteMode.NONE_L2_CACHE
                ttl_second = 0
                existence = ExistenceOpt.NONE
                cache_type = CacheType.MEMEORY

        Returns:
            Future: A Future object representing the asynchronous operations.

        Raises:
            RuntimeError: Raise a runtime error if failing to mset_d2h the values of all keys.
            TypeError: Raise a type error if the input parameter is invalid.
        """
        args = [
            ["keys", keys, list],
            ["tensors", tensors, list]
        ]
        validator.check_args_types(args)
        self._check_tensors_is_contiguous(tensors)
        dev_blob_list = [self._tensor_2_bloblist(tensor) for tensor in tensors]
        return self._hetero_client.async_mset_d2h(keys, dev_blob_list, set_param)

    def async_mget_h2d(self, keys: List[str], tensors: List[Tensor], sub_timeout_ms: int = 0) -> Future:
        """
        Obtain tensors from the host and write the tensors to the device asynchronously.
        If multiple memory addresses are combined and written to the host during async_mset_d2h, the host data is
        automatically split into multiple memory addresses and written to the device in async_mget_h2d.
        If the key of the host is no longer used, you can call the delete interface to delete it.

        Args:
            keys (List[str]): List of keys associated with the tensors.
                              Constraint: The number of keys cannot exceed 10,000.
            tensors (List[Tensor]): List of tensors to store the retrieved data.

        Returns:
            Future: A Future object representing the asynchronous operations.

        Raises:
            RuntimeError: Raise a runtime error if failing to async_mset_d2h the values of all keys.
            TypeError: Raise a type error if the input parameter is invalid.
        """
        args = [
            ["keys", keys, list],
            ["tensors", tensors, list],
            ["sub_timeout_ms", sub_timeout_ms, int]
        ]
        validator.check_args_types(args)
        self._check_tensors_is_contiguous(tensors)
        dev_blob_list = [self._tensor_2_bloblist(tensor) for tensor in tensors]
        return self._hetero_client.async_mget_h2d(keys, dev_blob_list, sub_timeout_ms)

    def delete(self, keys: list = None) -> list:
        """
        Delete the tensor datas of keys from worker.
        The delete interface works with mget_h2d and mset_d2h.

        Args:
            keys (List[str], optional): List of keys to delete. Constraint: The number of keys cannot exceed 10,000.

        Returns:
            failed_keys(list): The keys that failed to be deleted.

        Raises:
            RuntimeError: Raise a runtime error if failing to delete the values of all keys.
            TypeError: Raise a type error if the input parameter is invalid.
        """
        args = [
            ["keys", keys, list]
        ]
        validator.check_args_types(args)
        return self._hetero_client.delete(keys)

    def dev_mset(self, keys: List[str], tensors: List[Tensor]) -> list:
        """
        The data system caches data on the device and writes the metadata of the key corresponding to
        tensors to the data system so that other clients can access the data system.
        dev_mset and dev_mget must be used together. Heterogeneous objects are not automatically deleted after
        dev_mget is executed. If an object is no longer used, invoke dev_local_delete or dev_delete to delete it.
        The device memory addresses in the input parameters of the dev_mset and dev_mget interfaces cannot
        belong to the same NPU.

        Args:
            keys(list): A list of keys corresponding to the data_blob_list. Constraint: The number of keys
                        cannot exceed 10,000.
            tensors (List[Tensor]): List of tensors to store the retrieved data.

        Returns:
            failed_keys(list): The failed dev_mset keys.

        Raises:
            RuntimeError: Raise a runtime error if failing to set the value of all keys.
            TypeError: Raise a type error if the input parameter is invalid.
        """
        args = [
            ["keys", keys, list],
            ["tensors", tensors, list]
        ]
        validator.check_args_types(args)
        self._check_tensors_is_contiguous(tensors)
        dev_blob_lists = [self._tensor_2_bloblist(tensor) for tensor in tensors]
        return self._hetero_client.dev_mset(keys, dev_blob_lists)

    def dev_mget(self, keys: List[str], tensors: List[Tensor], sub_timeout_ms: int = 0) -> list:
        """
        Obtains data from the device and writes the data to tensors. Data is transmitted directly through
        the device-to-device channel.
        dev_mset and dev_mget must be used together. Heterogeneous objects are not automatically deleted after
        dev_mget is executed. If an object is no longer used, invoke dev_local_delete or dev_delete to delete it.
        The device memory addresses in the input parameters of the dev_mset and dev_mget interfaces cannot
        belong to the same NPU.
        During the execution of dev_mget, do not exit the process where dev_mset is executed. Otherwise, dev_mget fails.

        Args:
            keys(list): A list of keys corresponding to the data_blob_list. Constraint: The number of keys
                        cannot exceed 10,000.
            tensors (List[Tensor]): List of tensors to store the retrieved data.
            sub_timeout_ms(int): The sub_timeout_ms of the get operation.

        Returns:
            failed_keys(list): The failed dev_mget keys.

        Raises:
            RuntimeError: Raise a runtime error if failing to get the value of all keys.
            TypeError: Raise a type error if the input parameter is invalid.
        """
        args = [
            ["keys", keys, list],
            ["tensors", tensors, list],
            ["sub_timeout_ms", sub_timeout_ms, int]
        ]
        validator.check_args_types(args)
        self._check_tensors_is_contiguous(tensors)
        dev_blob_lists = [self._tensor_2_bloblist(tensor) for tensor in tensors]
        return self._hetero_client.dev_mget(keys, dev_blob_lists, sub_timeout_ms)

    def dev_mget_into_tensor(
            self, keys: List[str], tensor: Tensor, copy_ranges: List[CopyRange], sub_timeout_ms: int = 60000) -> list:
        """
        Retrieves data from the device for multiple keys and copies each data segment into specified
        locations within a single target tensor, based on user-defined copy ranges.
        The data is transmitted directly through the device-to-device channel.

        This method is designed to work in conjunction with dev_mset. Heterogeneous objects are not automatically
        deleted after dev_mget_into_tensor is executed. If an object is no longer needed, you should explicitly invoke
        dev_local_delete、async_dev_delete or dev_delete to remove it.

        Args:
            keys (List[str]): A list of keys corresponding to the data on the device.
                            Constraint: The number of keys must not exceed 10,000.
            tensor (Tensor): The target tensor into which the retrieved data will be copied.
                            The tensor must have sufficient size to accommodate all specified copy ranges.
            copy_ranges (List[CopyRange]): A list of CopyRange named tuples specifying the source offsets,
                                        destination offsets (within the tensor), and lengths (in bytes)
                                        for each data segment to be copied.
                                        Each CopyRange contains:
                                            - src_offset (int): Starting offset in the source (in bytes).
                                            - dst_offset (int): Starting offset in the destination tensor (in bytes).
                                            - length (int): Length of the data to be copied (in bytes).
            sub_timeout_ms (int, optional): The sub-timeout (in milliseconds) for the get operation. Default is 60s.

        Returns:
            List[str]: A list of keys for which the dev_mget_into_tensor operation failed.

        Raises:
            RuntimeError: Raised when the operation fails to retrieve the values for all specified keys.
            TypeError: Raised when one or more input parameters are of an invalid type.
        """
        args = [
            ["keys", keys, list],
            ["tensors", tensor, Tensor],
            ["copy_ranges", copy_ranges, list],
            ["sub_timeout_ms", sub_timeout_ms, int]
        ]
        validator.check_args_types(args)
        if len(keys) != len(copy_ranges):
            raise RuntimeError(
                r"The length of keys and copy_ranges do not match, keys:{}, copy_ranges:{}".format(len(keys),
                                                                                                   len(copy_ranges)))

        self._check_tensor_device_type(tensor)
        dev_blob_lists = []
        start_ptr = self._get_start_data_ptr(tensor)
        for copy_range in copy_ranges:
            blob = Blob(start_ptr + copy_range.dst_offset, copy_range.length)
            dev_blob_lists.append(DeviceBlobList(self._device_id, [blob], copy_range.src_offset))
        return self._hetero_client.dev_mget(keys, dev_blob_lists, sub_timeout_ms)

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
        args = [
            ["keys", keys, list]
        ]
        validator.check_args_types(args)
        return self._hetero_client.dev_delete(keys)

    def async_dev_delete(self, keys: list = None) -> Future:
        """
        Delete the device info from the host asynchronously.
        The async_dev_delete interface is used together with the dev_mset / dev_mget interface.

        Args:
            keys(list): The data list of string type. Constraint: The number of keys cannot exceed 10,000.

        Returns:
            Future: A Future object representing the asynchronous operations. When calling `future.get()`, if there are
                    partial failures, the keys of the failed operations are returned; if all keys fail, an RuntimeError
                    exception is thrown.

        Raises:
            TypeError: Raise a type error if the input parameter is invalid.
        """
        args = [
            ["keys", keys, list]
        ]
        validator.check_args_types(args)
        return self._hetero_client.async_dev_delete(keys)

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
        args = [
            ["keys", keys, list]
        ]
        validator.check_args_types(args)
        return self._hetero_client.dev_local_delete(keys)

    def dev_send(self, keys: List[str], tensors: List[Tensor]) -> List[Future]:
        """
        Send the tensors cache on the device as a heterogeneous object of the data system.

        Heterogeneous objects can be obtained through dev_recv.
        dev_send and dev_recv must be used together.
        The device memory addresses in the input parameters of the dev_send and dev_recv interfaces cannot
        belong to the same NPU.
        After data is obtained through dev_recv, the data system automatically deletes the heterogeneous object
        and does not manage the device memory corresponding to the object.

        Args:
            keys (List[str]): A list of keys corresponding to the tensor list.
            tensors (List[Tensor]): List of tensors corresponding to the keys.

        Returns:
            List[Future]: A list of Future objects representing the asynchronous operations.

        Raises:
            RuntimeError: Raise a runtime error if failing to dev_send the values of all keys.
            TypeError: Raise a type error if the input parameter is invalid.
        """
        args = [
            ["keys", keys, list],
            ["tensors", tensors, list]
        ]
        validator.check_args_types(args)
        self._check_tensors_is_contiguous(tensors)
        dev_blob_list = [self._tensor_2_bloblist(tensor) for tensor in tensors]
        return self._hetero_client.dev_publish(keys, dev_blob_list)

    def dev_recv(self, keys: List[str], tensors: List[Tensor]) -> List[Future]:
        """
        Receive heterogeneous objects of the data system and writes data to tensors.

        Tensor data is directly transmitted through the device-to-device channel.
        dev_send and dev_recv must be used together.
        The device memory addresses in the input parameters of the dev_send and dev_recv interfaces cannot
        belong to the same NPU.
        After data is obtained through dev_recv, the data system automatically deletes the heterogeneous object
        and does not manage the device memory corresponding to the object.
        During the execution of dev_recv, do not exit the process where dev_send is executed. Otherwise,
        dev_recv fails.

        Args:
            keys (List[str]): A list of keys corresponding to the tensor list.
            tensors (List[Tensor]): List of tensors to store the retrieved data.

        Returns:
            List[Future]: A list of Future objects representing the asynchronous operations.

        Raises:
            RuntimeError: Raise a runtime error if failing to dev_recv the values of all keys.
            TypeError: Raise a type error if the input parameter is invalid.
        """
        args = [
            ["keys", keys, list],
            ["tensors", tensors, list]
        ]
        validator.check_args_types(args)
        self._check_tensors_is_contiguous(tensors)
        dev_blob_list = [self._tensor_2_bloblist(tensor) for tensor in tensors]
        return self._hetero_client.dev_subscribe(keys, dev_blob_list)

    def exist(self, keys: list[str]) -> list[bool]:
        """
        Check the existence of the given keys in the data system.

        Args:
            keys(List[str]): The data list of string type. Constraint: The number of keys cannot exceed 10,000.

        Returns:
            exists(List[bool]): The existence of the corresponding keys.

        Raises:
            RuntimeError: Raise a runtime error if failing to exist the values of all keys.
            TypeError: Raise a type error if the input parameter is invalid.
        """
        args = [["keys", keys, list]]
        validator.check_args_types(args)
        return self._hetero_client.exist(keys)

    def put_page_attn_layerwise_d2d(
        self,
        keys: list[str],
        layer_tensors: list[Tensor],
        block_ids: list[int]
    ) -> list[Future]:
        """
        Put PagedAttention layer-wise tensors on the device as a heterogeneous object of the data system.

        Heterogeneous objects can be obtained through get_page_attn_layerwise_d2d.
        put_page_attn_layerwise_d2d and get_page_attn_layerwise_d2d must be used together.
        The device memory addresses in the input parameters of the put_page_attn_layerwise_d2d and
        get_page_attn_layerwise_d2d interfaces cannot belong to the same NPU.
        After data is obtained through get_page_attn_layerwise_d2d, the data system automatically
        deletes the heterogeneous object and does not manage the device memory corresponding to the object.

        Args:
            keys (List[str]): List of keys for the Device heterogeneous object.
                Constraint: Maximum 10,000 keys allowed.
            layer_tensors (List[Tensor]): List of tensors to put.
                Constraint: All tensors must have contiguous memory layout.
            block_ids (List[int]): List of block IDs to be published.

        Returns:
            List[Future]: A list of Future objects representing the asynchronous operations.

        Raises:
            RuntimeError: Raise a runtime error if failing to put the values of all keys.
            TypeError: Raise a type error if the input parameter is invalid.
        """
        args = [
            ["keys", keys, list],
            ["layer_tensors", layer_tensors, list],
            ["block_ids", block_ids, list],
        ]
        validator.check_args_types(args)
        self._check_tensors_is_contiguous(layer_tensors)
        dev_blob_list = self._page_attn_blockwise_dbls(layer_tensors, block_ids)
        return self._hetero_client.dev_publish(keys, dev_blob_list)

    def get_page_attn_layerwise_d2d(
        self,
        keys: list[str],
        layer_tensors: list[Tensor],
        block_ids: list[int]
    ) -> list[Future]:
        """
        Receive heterogeneous objects of the data system and writes data to PagedAttention layer-wise tensors.

        Tensor data is directly transmitted through the device-to-device channel.
        put_page_attn_layerwise_d2d and get_page_attn_layerwise_d2d must be used together.
        The device memory addresses in the input parameters of the put_page_attn_layerwise_d2d and
        get_page_attn_layerwise_d2d interfaces cannot belong to the same NPU.
        After data is obtained through get_page_attn_layerwise_d2d, the data system automatically
        deletes the heterogeneous object and does not manage the device memory corresponding to the object.
        During the execution of get_page_attn_layerwise_d2d, do not exit the process where put_page_attn_layerwise_d2d
        is executed.
        Otherwise, get_page_attn_layerwise_d2d fails.

        Args:
            keys (List[str]): List of keys for the Device heterogeneous object.
                Constraint: Maximum 10,000 keys allowed.
            layer_tensors (List[Tensor]): List of tensors corresponding to the keys.
                Constraint: All tensors must have contiguous memory layout.
            block_ids (List[int]): List of block IDs to be subscribed.

        Returns:
            List[Future]: A list of Future objects representing the asynchronous operations.

        Raises:
            RuntimeError: Raise a runtime error if failing to get the values of all keys.
            TypeError: Raise a type error if the input parameter is invalid.
        """
        args = [
            ["keys", keys, list],
            ["layer_tensors", layer_tensors, list],
            ["block_ids", block_ids, list],
        ]
        validator.check_args_types(args)
        self._check_tensors_is_contiguous(layer_tensors)
        dev_blob_list = self._page_attn_blockwise_dbls(layer_tensors, block_ids)
        return self._hetero_client.dev_subscribe(keys, dev_blob_list)

    def mset_page_attn_blockwise_d2h(
        self,
        keys: list[str],
        layer_tensors: list[Tensor],
        block_ids: list[int],
    ) -> Future:
        """
        Write the PagedAttention block-wise tensors of the device to the host asynchronously.

        If the data of the device contains multiple memory addresses, the device automatically combines data and writes
        the data to the host.
        If the key of the host is no longer used, you can call the delete interface to delete it.

        Args:
            keys (List[str]): List of keys for the Host.
                Constraint: The number of keys cannot exceed 10,000.
            layer_tensors (List[Tensor]): List of tensors to send from device to host.
            block_ids (List[int]): List of block IDs to be published.

        Returns:
            Future: A Future object representing the asynchronous operations.

        Raises:
            RuntimeError: Raise a runtime error if failing to mset_page_attn_blockwise_d2h the values of all keys.
            TypeError: Raise a type error if the input parameter is invalid.
        """
        args = [
            ["keys", keys, list],
            ["layer_tensors", layer_tensors, list],
            ["block_ids", block_ids, list],
        ]
        validator.check_args_types(args)
        self._check_tensors_is_contiguous(layer_tensors)
        dev_blob_list = self._page_attn_blockwise_dbls(layer_tensors, block_ids, self._device_id)
        set_param = SetParam()
        set_param.write_mode = WriteMode.NONE_L2_CACHE_EVICT
        return self._hetero_client.async_mset_d2h(keys, dev_blob_list, set_param)

    def mget_page_attn_blockwise_h2d(
        self,
        keys: list[str],
        layer_tensors: list[Tensor],
        block_ids: list[int],
        sub_timeout_ms: int = 0
    ) -> Future:
        """
        Obtain the PagedAttention layer-wise tensors from the host and write the tensors to the device asynchronously.

        If multiple memory addresses are combined and written to the host during mset_page_attn_blockwise_d2h,
        the host data is automatically split into multiple memory addresses and written to the device in
        mget_page_attn_blockwise_h2d.
        If the key of the host is no longer used, you can call the delete interface to delete it.

        Args:
            keys (List[str]): List of keys for the Host.
                Constraint: Maximum 10,000 keys allowed.
            layer_tensors (List[Tensor]): List of tensors to store the retrieved data.
                Constraint: All tensors must have contiguous memory layout.
            block_ids (List[int]): List of block IDs to be subscribed.
            sub_timeout_ms (int): Subscription timeout in milliseconds.
                Default: 0 (no timeout).

        Returns:
            Future: A Future object representing the asynchronous operations.

        Raises:
            RuntimeError: Raise a runtime error if failing to mget_page_attn_blockwise_h2d the values of all keys.
            TypeError: Raise a type error if the input parameter is invalid.
        """
        args = [
            ["keys", keys, list],
            ["layer_tensors", layer_tensors, list],
            ["block_ids", block_ids, list],
        ]
        validator.check_args_types(args)
        self._check_tensors_is_contiguous(layer_tensors)
        dev_blob_list = self._page_attn_blockwise_dbls(layer_tensors, block_ids, self._device_id)
        return self._hetero_client.async_mget_h2d(keys, dev_blob_list, sub_timeout_ms)

    def _get_start_data_ptr(self, tensor: Tensor) -> int:
        if self._is_ms_tensor(tensor):
            element_size = tensor.element_size()
        else:
            element_size = tensor.dtype.itemsize
        data_ptr = tensor.data_ptr()
        if data_ptr == 0:
            raise RuntimeError("The input tensor has no valid data storage (data_ptr is null). "
                               "Please ensure the tensor is properly initialized and not empty.")
        return data_ptr + (tensor.storage_offset() * element_size)

    def _tensor_2_bloblist(self, tensor: Tensor) -> DeviceBlobList:
        """
        Convert a PyTorch tensor into a DeviceBlobList.

        Args:
            tensor (Tensor): The PyTorch tensor to convert.

        Returns:
            DeviceBlobList: The converted blob list.

        Raises:
            TypeError: If the input is not a PyTorch tensor.
        """
        self._check_tensor_device_type(tensor)
        blob = Blob(self._get_start_data_ptr(tensor), tensor.nbytes)
        return DeviceBlobList(self._device_id, [blob])
