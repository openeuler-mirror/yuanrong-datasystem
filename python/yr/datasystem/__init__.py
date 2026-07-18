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

import importlib.machinery
from importlib import import_module
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
    "ServiceDiscovery",
    "ServiceDiscoveryOptions",
    "CoordinatorServiceDiscovery",
    "CoordinatorServiceDiscoveryOptions",
    "ServiceAffinityPolicy",
]


def _configure_transfer_engine_runtime(pkg_dir: Path) -> None:
    candidate_dirs = [pkg_dir, pkg_dir / "lib"]

    p2p_candidates = [
        path / "libp2p_transfer.so"
        for path in candidate_dirs
    ]
    for p2p_so in p2p_candidates:
        if p2p_so.exists():
            os.environ.setdefault("TRANSFER_ENGINE_P2P_SO_PATH", str(p2p_so))
            break

_PKG_DIR = Path(__file__).resolve().parent
_configure_transfer_engine_runtime(_PKG_DIR)
if any((_PKG_DIR / ("_transfer_engine" + suffix)).exists() for suffix in importlib.machinery.EXTENSION_SUFFIXES):
    __all__.extend(["TransferEngine", "Result", "ErrorCode"])

# Keep public SDK symbols lazy so importing TransferEngine alone does not load
# libds_client_py/libbrpc before other native logging runtimes enter the process.
_LAZY_EXPORTS = {
    "Buffer": ("yr.datasystem.object_client", "Buffer"),
    "ConsistencyType": ("yr.datasystem.object_client", "ConsistencyType"),
    "ObjectClient": ("yr.datasystem.object_client", "ObjectClient"),
    "WriteMode": ("yr.datasystem.object_client", "WriteMode"),
    "FutureTimeoutException": ("yr.datasystem.lib.libds_client_py", "FutureTimeoutException"),
    "SubconfigType": ("yr.datasystem.stream_client", "SubconfigType"),
    "StreamClient": ("yr.datasystem.stream_client", "StreamClient"),
    "DsClient": ("yr.datasystem.ds_client", "DsClient"),
    "KVClient": ("yr.datasystem.kv_client", "KVClient"),
    "HeteroClient": ("yr.datasystem.hetero_client", "HeteroClient"),
    "Blob": ("yr.datasystem.hetero_client", "Blob"),
    "DeviceBlobList": ("yr.datasystem.hetero_client", "DeviceBlobList"),
    "MetaInfo": ("yr.datasystem.hetero_client", "MetaInfo"),
    "Future": ("yr.datasystem.hetero_client", "Future"),
    "Status": ("yr.datasystem.util", "Status"),
    "Context": ("yr.datasystem.util", "Context"),
    "ServiceDiscovery": ("yr.datasystem.service_discovery", "ServiceDiscovery"),
    "ServiceDiscoveryOptions": ("yr.datasystem.service_discovery", "ServiceDiscoveryOptions"),
    "CoordinatorServiceDiscovery": ("yr.datasystem.service_discovery", "CoordinatorServiceDiscovery"),
    "CoordinatorServiceDiscoveryOptions": ("yr.datasystem.service_discovery", "CoordinatorServiceDiscoveryOptions"),
    "ServiceAffinityPolicy": ("yr.datasystem.service_discovery", "ServiceAffinityPolicy"),
    "DsTensorClient": ("yr.datasystem.ds_tensor_client", "DsTensorClient"),
    "CopyRange": ("yr.datasystem.ds_tensor_client", "CopyRange"),
}

_TRANSFER_ENGINE_EXPORTS = {
    "TransferEngine": "TransferEngine",
    "Result": "Result",
    "ErrorCode": "ErrorCode",
}


def _load_lazy_export(name):
    module_name, attr_name = _LAZY_EXPORTS[name]
    module = import_module(module_name)
    value = getattr(module, attr_name)
    globals()[name] = value
    return value


def _load_transfer_engine_export(name):
    try:
        module = import_module("yr.datasystem._transfer_engine")
    except ImportError as exc:
        raise AttributeError(
            f"module {__name__} has no attribute {name}; "
            f"optional transfer engine bindings failed to import: {exc}"
        ) from exc
    value = getattr(module, _TRANSFER_ENGINE_EXPORTS[name])
    globals()[name] = value
    return value


def _load_perf_client():
    from yr.datasystem.lib import libds_client_py as _ds
    from yr.datasystem.util import Validator as validator

    if not hasattr(_ds, "PerfClient"):
        raise AttributeError(f"module {__name__} has no attribute PerfClient")

    class PerfClient:
        """Data system perf client for resetting and fetching perf logs.

        PerfClient is only available when the Python bindings are built with
        ENABLE_PERF enabled.
        """

        def __init__(
            self,
            host,
            port,
            connect_timeout_ms=9000,
            access_key="",
            secret_key="",
        ):
            args = [
                ["host", host, str],
                ["port", port, int],
                ["connect_timeout_ms", connect_timeout_ms, int],
                ["access_key", access_key, str],
                ["secret_key", secret_key, str],
            ]
            validator.check_args_types(args)
            self._client = _ds.PerfClient(host, port, connect_timeout_ms, access_key, secret_key)

        def init(self):
            init_status = self._client.init()
            if init_status.is_error():
                raise RuntimeError(init_status.to_string())

        def reset_perf_log(self, perf_type: str):
            status = self._client.reset_perf_log(perf_type)
            if status.is_error():
                raise RuntimeError(status.to_string())

        def get_perf_log(self, perf_type: str):
            status, perf_log = self._client.get_perf_log(perf_type)
            if status.is_error():
                raise RuntimeError(status.to_string())
            return perf_log

    globals()["PerfClient"] = PerfClient
    return PerfClient


def __getattr__(name):
    if name in _TRANSFER_ENGINE_EXPORTS:
        return _load_transfer_engine_export(name)
    if name in _LAZY_EXPORTS:
        return _load_lazy_export(name)
    if name == "PerfClient":
        return _load_perf_client()
    raise AttributeError(f"module {__name__} has no attribute {name}")
