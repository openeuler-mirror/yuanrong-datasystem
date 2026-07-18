# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
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
Service discovery python facade tests.
"""
from __future__ import absolute_import

import importlib
from pathlib import Path
import sys
import types
import unittest


PYTHON_ROOT = Path(__file__).resolve().parents[2] / "python"
if str(PYTHON_ROOT) not in sys.path:
    sys.path.insert(0, str(PYTHON_ROOT))

_MISSING = object()
_FAKE_MODULE_NAMES = [
    "yr.datasystem",
    "yr.datasystem.lib",
    "yr.datasystem.lib.libds_client_py",
    "yr.datasystem.service_discovery",
    "yr.datasystem.kv_client",
    "yr.datasystem.object_client",
]


class FakeStatus:
    def is_error(self):
        return False

    def to_string(self):
        return "OK"


class FakeServiceAffinityPolicy:
    PREFERRED_SAME_NODE = 0
    REQUIRED_SAME_NODE = 1
    RANDOM = 2


class FakeServiceDiscoveryOptions:
    __slots__ = (
        "etcd_address",
        "cluster_name",
        "etcd_ca",
        "etcd_cert",
        "etcd_key",
        "etcd_dns_name",
        "username",
        "password",
        "token_refresh_interval_sec",
        "host_id_env_name",
        "affinity_policy",
    )


class FakeCoordinatorServiceDiscoveryOptions:
    __slots__ = (
        "service_address",
        "cluster_name",
        "host_id_env_name",
        "affinity_policy",
        "coordinator_discovery",
    )


class FakeNativeDiscovery:
    def __init__(self, options):
        self.options = options
        self.init_calls = 0
        self.select_worker_calls = 0

    def init(self):
        self.init_calls += 1
        return FakeStatus()

    def select_worker(self):
        self.select_worker_calls += 1
        return FakeStatus(), "127.0.0.1", 9000, False

    def select_same_node_worker(self):
        return FakeStatus(), "127.0.0.1", 9000

    def get_all_workers(self):
        return FakeStatus(), [], ["127.0.0.1:9000"]

    def get_affinity_policy(self):
        return FakeServiceAffinityPolicy.PREFERRED_SAME_NODE

    def has_host_affinity(self):
        return True


class FakeNativeKVClient:
    instances = []

    def __init__(self, *args):
        self.args = args
        FakeNativeKVClient.instances.append(self)


def install_fake_native_module():
    saved_modules = {name: sys.modules.get(name, _MISSING) for name in _FAKE_MODULE_NAMES}
    yr_package = sys.modules.get("yr")
    saved_datasystem_attr = getattr(yr_package, "datasystem", _MISSING) if yr_package else _MISSING
    for module in _FAKE_MODULE_NAMES:
        sys.modules.pop(module, None)

    fake_ds = types.ModuleType("yr.datasystem.lib.libds_client_py")
    fake_ds.ServiceAffinityPolicy = FakeServiceAffinityPolicy
    fake_ds.ServiceDiscoveryOptions = FakeServiceDiscoveryOptions
    fake_ds.CoordinatorServiceDiscoveryOptions = FakeCoordinatorServiceDiscoveryOptions
    fake_ds.ServiceDiscovery = FakeNativeDiscovery
    fake_ds.CoordinatorServiceDiscovery = FakeNativeDiscovery
    fake_ds.KVClient = FakeNativeKVClient
    fake_ds.ExistenceOpt = type("ExistenceOpt", (), {"NONE": 0, "NX": 1})
    fake_ds.WriteMode = type(
        "WriteMode",
        (),
        {
            "NONE_L2_CACHE": 0,
            "WRITE_THROUGH_L2_CACHE": 1,
            "WRITE_BACK_L2_CACHE": 2,
            "NONE_L2_CACHE_EVICT": 3,
            "WRITE_BACK_L2_CACHE_EVICT": 4,
        },
    )
    fake_ds.ConsistencyType = type("ConsistencyType", (), {"PRAM": 0, "CAUSAL": 1})
    fake_ds.CacheType = type("CacheType", (), {"MEMORY": 0, "DISK": 1})
    fake_ds.StateValueBuffer = type("StateValueBuffer", (), {})

    fake_lib = types.ModuleType("yr.datasystem.lib")
    fake_lib.libds_client_py = fake_ds
    sys.modules["yr.datasystem.lib"] = fake_lib
    sys.modules["yr.datasystem.lib.libds_client_py"] = fake_ds

    FakeNativeKVClient.instances.clear()
    return saved_modules, yr_package, saved_datasystem_attr


def restore_native_modules(saved_modules, yr_package, saved_datasystem_attr):
    for module in _FAKE_MODULE_NAMES:
        sys.modules.pop(module, None)
    for name, module in saved_modules.items():
        if module is not _MISSING:
            sys.modules[name] = module
    if yr_package is not None:
        if saved_datasystem_attr is _MISSING:
            if hasattr(yr_package, "datasystem"):
                delattr(yr_package, "datasystem")
        else:
            setattr(yr_package, "datasystem", saved_datasystem_attr)


class TestServiceDiscoveryApi(unittest.TestCase):
    def setUp(self):
        saved_modules, yr_package, saved_datasystem_attr = install_fake_native_module()
        self.addCleanup(restore_native_modules, saved_modules, yr_package, saved_datasystem_attr)

    def test_coordinator_service_discovery_is_exported(self):
        datasystem = importlib.import_module("yr.datasystem")

        self.assertIn("CoordinatorServiceDiscovery", datasystem.__all__)
        self.assertIn("CoordinatorServiceDiscoveryOptions", datasystem.__all__)
        self.assertIsNotNone(getattr(datasystem, "CoordinatorServiceDiscovery"))
        self.assertIsNotNone(getattr(datasystem, "CoordinatorServiceDiscoveryOptions"))

    def test_fake_options_reject_unbound_attrs(self):
        options = FakeCoordinatorServiceDiscoveryOptions()

        with self.assertRaises(AttributeError):
            options.not_bound_by_pybind = "value"

    def test_kv_client_passes_coordinator_discovery_to_native_client(self):
        discovery_module = importlib.import_module("yr.datasystem.service_discovery")
        kv_module = importlib.import_module("yr.datasystem.kv_client")

        options = discovery_module.CoordinatorServiceDiscoveryOptions()
        options.service_address = "coordinator-address"
        options.cluster_name = "cluster-a"
        options.host_id_env_name = "HOST_ID"
        options.affinity_policy = discovery_module.ServiceAffinityPolicy.PREFERRED_SAME_NODE
        service_discovery = discovery_module.CoordinatorServiceDiscovery(options)
        service_discovery.init()

        kv_module.KVClient(service_discovery=service_discovery, enable_cross_node_connection=True)

        native_discovery = service_discovery.native_discovery
        self.assertTrue(service_discovery.initialized)
        self.assertEqual(native_discovery.init_calls, 1)
        self.assertEqual(native_discovery.select_worker_calls, 0)
        self.assertEqual(len(FakeNativeKVClient.instances), 1)
        self.assertIs(FakeNativeKVClient.instances[0].args[-1], native_discovery)

    def test_kv_client_passes_etcd_discovery_to_native_client(self):
        discovery_module = importlib.import_module("yr.datasystem.service_discovery")
        kv_module = importlib.import_module("yr.datasystem.kv_client")

        options = discovery_module.ServiceDiscoveryOptions()
        options.etcd_address = "etcd-address"
        options.cluster_name = "cluster-a"
        service_discovery = discovery_module.ServiceDiscovery(options)
        service_discovery.init()

        kv_module.KVClient(service_discovery=service_discovery, enable_cross_node_connection=True)

        self.assertEqual(len(FakeNativeKVClient.instances), 1)
        self.assertIs(FakeNativeKVClient.instances[0].args[-1], service_discovery.native_discovery)
        self.assertEqual(service_discovery.native_discovery.options.cluster_name, "cluster-a")

    def test_kv_client_without_discovery_uses_host_port(self):
        kv_module = importlib.import_module("yr.datasystem.kv_client")

        kv_module.KVClient(host="worker-address", port=1234)

        self.assertEqual(len(FakeNativeKVClient.instances), 1)
        self.assertEqual(FakeNativeKVClient.instances[0].args[0], "worker-address")
        self.assertEqual(FakeNativeKVClient.instances[0].args[1], 1234)
        self.assertEqual(len(FakeNativeKVClient.instances[0].args), 13)

    def test_kv_client_rejects_uninitialized_discovery(self):
        discovery_module = importlib.import_module("yr.datasystem.service_discovery")
        kv_module = importlib.import_module("yr.datasystem.kv_client")

        options = discovery_module.CoordinatorServiceDiscoveryOptions()
        options.service_address = "coordinator-address"
        service_discovery = discovery_module.CoordinatorServiceDiscovery(options)

        with self.assertRaisesRegex(RuntimeError, "call init\\(\\) before creating KVClient"):
            kv_module.KVClient(service_discovery=service_discovery, enable_cross_node_connection=True)

    def test_kv_client_discovery_with_host_port_warning(self):
        discovery_module = importlib.import_module("yr.datasystem.service_discovery")
        kv_module = importlib.import_module("yr.datasystem.kv_client")

        options = discovery_module.CoordinatorServiceDiscoveryOptions()
        options.service_address = "coordinator-address"
        service_discovery = discovery_module.CoordinatorServiceDiscovery(options)
        service_discovery.init()

        with self.assertWarnsRegex(UserWarning, "host and port are ignored"):
            kv_module.KVClient(
                host="worker-address",
                port=1234,
                service_discovery=service_discovery,
                enable_cross_node_connection=True,
            )

    def test_kv_client_discovery_warns_without_cross_node_connection(self):
        discovery_module = importlib.import_module("yr.datasystem.service_discovery")
        kv_module = importlib.import_module("yr.datasystem.kv_client")

        options = discovery_module.CoordinatorServiceDiscoveryOptions()
        options.service_address = "coordinator-address"
        service_discovery = discovery_module.CoordinatorServiceDiscovery(options)
        service_discovery.init()

        with self.assertWarnsRegex(RuntimeWarning, "enable_cross_node_connection is False"):
            kv_module.KVClient(service_discovery=service_discovery)

    def test_kv_client_discovery_host_port_mismatch(self):
        kv_module = importlib.import_module("yr.datasystem.kv_client")

        with self.assertRaisesRegex(RuntimeError, "host and port must be provided together"):
            kv_module.KVClient(host="worker-address")

        with self.assertRaisesRegex(RuntimeError, "host and port must be provided together"):
            kv_module.KVClient(port=1234)


if __name__ == "__main__":
    unittest.main()
