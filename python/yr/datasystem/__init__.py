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

import ctypes
import os
from pathlib import Path

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
    "CopyRange",
    "MetaInfo",
    "Future",
]

from yr.datasystem.object_client import Buffer, ConsistencyType
from yr.datasystem.object_client import ObjectClient, WriteMode
from yr.datasystem.lib.libds_client_py import FutureTimeoutException
from yr.datasystem.stream_client import SubconfigType, StreamClient
from yr.datasystem.ds_client import DsClient
from yr.datasystem.kv_client import KVClient
from yr.datasystem.hetero_client import HeteroClient, Blob, DeviceBlobList, MetaInfo, Future
from yr.datasystem.util import Status, Context


def _preload_transfer_engine_runtime_shared_libs(pkg_dir: Path) -> None:
    candidate_dirs = [pkg_dir, pkg_dir / "lib"]

    p2p_candidates = [
        path / "libp2p_transfer.so"
        for path in candidate_dirs
    ]
    for p2p_so in p2p_candidates:
        if p2p_so.exists():
            os.environ.setdefault("TRANSFER_ENGINE_P2P_SO_PATH", str(p2p_so))
            break

    rtld_global = getattr(ctypes, "RTLD_GLOBAL", 0)
    for base_dir in candidate_dirs:
        if not base_dir.exists():
            continue
        for pattern in ("libglog.so*", "libprotobuf.so*", "libabsl*.so*"):
            for so_path in sorted(base_dir.glob(pattern)):
                try:
                    ctypes.CDLL(str(so_path), mode=rtld_global)
                except OSError:
                    pass


_PKG_DIR = Path(__file__).resolve().parent
_preload_transfer_engine_runtime_shared_libs(_PKG_DIR)

try:
    from yr.datasystem._transfer_engine import ErrorCode as _TransferEngineErrorCode
    from yr.datasystem._transfer_engine import Result as _TransferEngineResult
    from yr.datasystem._transfer_engine import TransferEngine as _TransferEngine
except ImportError as exc:
    _TransferEngineErrorCode = None
    _TransferEngineResult = None
    _TransferEngine = None
    _TRANSFER_ENGINE_IMPORT_ERROR = exc
else:
    _TRANSFER_ENGINE_IMPORT_ERROR = None
    __all__.extend(["TransferEngine", "Result", "ErrorCode"])


# Dynamically load DsTensorClient
# Delay dependency checking until the class is actually used to avoid forcing dependency on torch
def __getattr__(name):
    if name == "TransferEngine":
        if _TransferEngine is not None:
            return _TransferEngine
        if _TRANSFER_ENGINE_IMPORT_ERROR is not None:
            raise AttributeError(
                f"module {__name__} has no attribute {name}; "
                f"optional transfer engine bindings failed to import: {_TRANSFER_ENGINE_IMPORT_ERROR}"
            ) from _TRANSFER_ENGINE_IMPORT_ERROR
    if name == "Result":
        if _TransferEngineResult is not None:
            return _TransferEngineResult
        if _TRANSFER_ENGINE_IMPORT_ERROR is not None:
            raise AttributeError(
                f"module {__name__} has no attribute {name}; "
                f"optional transfer engine bindings failed to import: {_TRANSFER_ENGINE_IMPORT_ERROR}"
            ) from _TRANSFER_ENGINE_IMPORT_ERROR
    if name == "ErrorCode":
        if _TransferEngineErrorCode is not None:
            return _TransferEngineErrorCode
        if _TRANSFER_ENGINE_IMPORT_ERROR is not None:
            raise AttributeError(
                f"module {__name__} has no attribute {name}; "
                f"optional transfer engine bindings failed to import: {_TRANSFER_ENGINE_IMPORT_ERROR}"
            ) from _TRANSFER_ENGINE_IMPORT_ERROR
    if name == "DsTensorClient":
        from yr.datasystem.ds_tensor_client import DsTensorClient
        return DsTensorClient
    if name == "CopyRange":
        from yr.datasystem.ds_tensor_client import CopyRange
        return CopyRange
    raise AttributeError(f"module {__name__} has no attribute {name}")
