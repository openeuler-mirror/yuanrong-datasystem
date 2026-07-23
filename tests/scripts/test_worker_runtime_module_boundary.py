import pathlib
import unittest


REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]


class WorkerRuntimeModuleBoundaryTest(unittest.TestCase):
    def test_control_backend_scope_does_not_depend_on_object_cache_transport(self):
        build_files = {
            REPO_ROOT / "src/datasystem/worker/runtime/BUILD.bazel",
            REPO_ROOT / "src/datasystem/worker/runtime/CMakeLists.txt",
        }

        forbidden_tokens = [
            "worker_object_cache",
            "//src/datasystem/worker/object_cache:worker_worker_oc_api",
            "//src/datasystem/worker/object_cache:worker_worker_peer_state_codec",
        ]

        for build_file in build_files:
            text = build_file.read_text(encoding="utf-8")
            if build_file.name == "BUILD.bazel":
                text = text.split('name = "worker_control_backend_scope_classification"', 1)[1]
                text = text.split("ds_cc_library(", 1)[0]
            else:
                text = text.split("add_library(worker_control_backend_scope_classification", 1)[1]
                text = text.split("add_library(", 1)[0]
            for token in forbidden_tokens:
                self.assertNotIn(token, text, f"{build_file} should not expose object_cache dependencies")

    def test_control_backend_probe_is_owned_by_runtime_not_object_cache(self):
        self.assertTrue((REPO_ROOT / "src/datasystem/worker/runtime/worker_control_backend_probe.cpp").exists())
        self.assertTrue((REPO_ROOT / "src/datasystem/worker/runtime/worker_control_backend_probe.h").exists())
        self.assertFalse((REPO_ROOT / "src/datasystem/worker/object_cache/worker_control_backend_probe.cpp").exists())
        self.assertFalse((REPO_ROOT / "src/datasystem/worker/object_cache/worker_control_backend_probe.h").exists())

        cmake_file = REPO_ROOT / "src/datasystem/worker/object_cache/CMakeLists.txt"
        text = cmake_file.read_text(encoding="utf-8")
        aggregate_sources = text.split("set(WORKER_OC_SRCS", 1)[1].split(")", 1)[0]
        self.assertNotIn("worker_control_backend_probe.cpp", aggregate_sources)
        self.assertNotIn("worker_control_backend_probe", text)

        object_cache_bazel = REPO_ROOT / "src/datasystem/worker/object_cache/BUILD.bazel"
        self.assertNotIn("worker_control_backend_probe", object_cache_bazel.read_text(encoding="utf-8"))

        runtime_bazel = REPO_ROOT / "src/datasystem/worker/runtime/BUILD.bazel"
        self.assertIn('name = "worker_control_backend_probe"', runtime_bazel.read_text(encoding="utf-8"))

    def test_control_backend_probe_runtime_target_does_not_depend_on_object_cache(self):
        files = [
            REPO_ROOT / "src/datasystem/worker/runtime/BUILD.bazel",
            REPO_ROOT / "src/datasystem/worker/runtime/CMakeLists.txt",
            REPO_ROOT / "src/datasystem/worker/runtime/worker_control_backend_probe.cpp",
            REPO_ROOT / "src/datasystem/worker/runtime/worker_control_backend_probe.h",
        ]

        forbidden_tokens = [
            "worker_object_cache",
            "//src/datasystem/worker/object_cache:worker_worker_oc_api",
            "//src/datasystem/worker/object_cache:worker_worker_peer_state_codec",
            "datasystem/worker/object_cache/worker_worker_oc_api.h",
            "datasystem/worker/object_cache/worker_worker_peer_state_codec.h",
            "object_cache::",
        ]

        for file_path in files:
            text = file_path.read_text(encoding="utf-8")
            if file_path.name == "BUILD.bazel":
                text = text.split('name = "worker_control_backend_probe"', 1)[1]
                text = text.split("ds_cc_library(", 1)[0]
            elif file_path.name == "CMakeLists.txt":
                text = text.split("add_library(worker_control_backend_probe", 1)[1]
                text = text.split("add_library(worker_runtime_core", 1)[0]
            for token in forbidden_tokens:
                self.assertNotIn(token, text, f"{file_path} should use injected peer probe clients")

    def test_topology_phase_callbacks_do_not_depend_on_object_cache_service_impl(self):
        runtime_files = [
            REPO_ROOT / "src/datasystem/worker/runtime/BUILD.bazel",
            REPO_ROOT / "src/datasystem/worker/runtime/CMakeLists.txt",
            REPO_ROOT / "src/datasystem/worker/runtime/worker_topology_phase_callbacks.h",
            REPO_ROOT / "src/datasystem/worker/runtime/worker_topology_phase_callbacks.cpp",
        ]

        forbidden_tokens = [
            "worker_oc_service_impl",
            "//src/datasystem/worker/object_cache:worker_oc_service_impl",
            "datasystem/worker/object_cache/worker_oc_service_impl.h",
            "object_cache::WorkerOCServiceImpl",
        ]

        for file_path in runtime_files:
            text = file_path.read_text(encoding="utf-8")
            if file_path.name == "BUILD.bazel":
                text = text.split('name = "worker_topology_phase_callbacks"', 1)[1]
                text = text.split("ds_cc_library(", 1)[0]
            elif file_path.name == "CMakeLists.txt":
                text = text.split("add_library(worker_topology_phase_callbacks", 1)[1]
            for token in forbidden_tokens:
                self.assertNotIn(token, text, f"{file_path} should use injected topology object-cache actions")

    def test_topology_phase_callbacks_use_injected_metadata_actions(self):
        files = [
            REPO_ROOT / "src/datasystem/worker/runtime/worker_topology_phase_callbacks.h",
            REPO_ROOT / "src/datasystem/worker/runtime/worker_topology_phase_callbacks.cpp",
        ]

        forbidden_tokens = [
            "MetadataManagerHolder",
            "OCMetadataManager",
            "SCMetadataManager",
            "OCMigrateMetadataManager",
            "SCMigrateMetadataManager",
            "datasystem/master/metadata_manager_holder.h",
            "datasystem/master/object_cache/oc_metadata_manager.h",
            "datasystem/master/object_cache/oc_migrate_metadata_manager.h",
            "datasystem/master/stream_cache/sc_metadata_manager.h",
            "datasystem/master/stream_cache/sc_migrate_metadata_manager.h",
        ]

        for file_path in files:
            text = file_path.read_text(encoding="utf-8")
            for token in forbidden_tokens:
                self.assertNotIn(token, text, f"{file_path} should use injected topology metadata actions")

    def test_data_migrator_does_not_depend_on_topology_callback_executor(self):
        files = [
            REPO_ROOT / "src/datasystem/worker/object_cache/data_migrator/data_migrator.cpp",
            REPO_ROOT / "src/datasystem/worker/object_cache/data_migrator/data_migrator.h",
        ]

        forbidden_tokens = [
            "datasystem/cluster/executor/topology_phase_callbacks.h",
        ]

        for file_path in files:
            text = file_path.read_text(encoding="utf-8")
            for token in forbidden_tokens:
                self.assertNotIn(token, text, f"{file_path} should avoid topology callback executor coupling")

    def test_clear_data_flow_uses_narrow_topology_callback_inputs(self):
        files = [
            REPO_ROOT / "src/datasystem/worker/object_cache/service/worker_oc_service_clear_data_flow.h",
            REPO_ROOT / "src/datasystem/worker/object_cache/service/worker_oc_service_clear_data_flow.cpp",
        ]

        forbidden_tokens = [
            "datasystem/cluster/executor/topology_phase_callbacks.h",
        ]

        for file_path in files:
            text = file_path.read_text(encoding="utf-8")
            for token in forbidden_tokens:
                self.assertNotIn(token, text, f"{file_path} should use narrow topology callback input contracts")

    def test_worker_oc_service_impl_uses_narrow_topology_callback_inputs(self):
        files = [
            REPO_ROOT / "src/datasystem/worker/object_cache/worker_oc_service_impl.h",
        ]

        forbidden_tokens = [
            "datasystem/cluster/executor/topology_phase_callbacks.h",
        ]

        for file_path in files:
            text = file_path.read_text(encoding="utf-8")
            for token in forbidden_tokens:
                self.assertNotIn(token, text, f"{file_path} should avoid the full topology callback executor contract")

    def test_worker_oc_server_header_does_not_expose_runtime_internals(self):
        header = REPO_ROOT / "src/datasystem/worker/worker_oc_server.h"
        text = header.read_text(encoding="utf-8")
        self.assertIn("class WorkerIsolationCoordinator;", text)
        forbidden_tokens = [
            "datasystem/worker/runtime/worker_isolation_coordinator.h",
            "datasystem/worker/runtime/worker_topology_availability_admission.h",
            "datasystem/worker/runtime/worker_control_backend_probe.h",
            "datasystem/worker/runtime/worker_topology_phase_callbacks.h",
        ]
        for token in forbidden_tokens:
            self.assertNotIn(token, text, f"{header} should not expose runtime implementation headers")

    def test_master_metadata_managers_use_narrow_topology_callback_inputs(self):
        files = [
            REPO_ROOT / "src/datasystem/master/object_cache/oc_metadata_manager.h",
            REPO_ROOT / "src/datasystem/master/object_cache/oc_migrate_metadata_manager.h",
            REPO_ROOT / "src/datasystem/master/stream_cache/sc_metadata_manager.h",
            REPO_ROOT / "src/datasystem/master/stream_cache/sc_migrate_metadata_manager.h",
        ]

        forbidden_tokens = [
            "datasystem/cluster/executor/topology_phase_callbacks.h",
        ]

        for file_path in files:
            text = file_path.read_text(encoding="utf-8")
            for token in forbidden_tokens:
                self.assertNotIn(token, text, f"{file_path} should depend on metadata recovery input contracts only")

    def test_full_topology_callback_contract_is_owned_by_executor_and_runtime_adapter_only(self):
        allowed_files = {
            REPO_ROOT / "src/datasystem/cluster/executor/topology_task_executor.h",
            REPO_ROOT / "src/datasystem/worker/runtime/worker_topology_phase_callbacks.h",
        }

        include_token = "datasystem/cluster/executor/topology_phase_callbacks.h"
        matched_files = {
            path
            for path in (REPO_ROOT / "src/datasystem").rglob("*")
            if path.suffix in {".h", ".cpp", ".cc"}
            and include_token in path.read_text(encoding="utf-8", errors="ignore")
        }

        self.assertEqual(allowed_files, matched_files)

    def test_topology_engine_uses_coordination_backend_not_concrete_etcd(self):
        files = [
            REPO_ROOT / "src/datasystem/cluster/runtime/topology_engine.h",
            REPO_ROOT / "src/datasystem/cluster/runtime/topology_engine.cpp",
        ]

        forbidden_tokens = [
            "EtcdStore",
            "EtcdCoordinationBackend",
            "datasystem/common/kvstore/etcd/etcd_store.h",
            "datasystem/cluster/coordination_backend/etcd_coordination_backend.h",
            "UseEtcd(",
        ]

        for file_path in files:
            text = file_path.read_text(encoding="utf-8")
            for token in forbidden_tokens:
                self.assertNotIn(token, text, f"{file_path} should use ICoordinationBackend abstractions")

    def test_coordination_backend_interface_hides_concrete_proxy_headers(self):
        header = REPO_ROOT / "src/datasystem/cluster/coordination_backend/coordination_backend.h"
        text = header.read_text(encoding="utf-8")

        self.assertIn("class ICoordinatorServiceProxy;", text)
        forbidden_tokens = [
            "datasystem/common/coordinator/coordinator_service_proxy.h",
            "ds_coordination_backend.h",
            "etcd_coordination_backend.h",
        ]
        for token in forbidden_tokens:
            self.assertNotIn(token, text, "ICoordinationBackend should not expose concrete backend/proxy headers")

    def test_slot_recovery_store_uses_coordination_backend_not_etcd_store(self):
        files = [
            REPO_ROOT / "src/datasystem/worker/object_cache/slot_recovery/BUILD.bazel",
            REPO_ROOT / "src/datasystem/worker/object_cache/slot_recovery/slot_recovery_store.h",
            REPO_ROOT / "src/datasystem/worker/object_cache/slot_recovery/slot_recovery_store.cpp",
        ]

        forbidden_tokens = [
            "EtcdStore",
            "EtcdSlotRecoveryStore",
            "common/kvstore/etcd:etcd_store",
            "datasystem/common/kvstore/etcd/etcd_store.h",
        ]

        for file_path in files:
            text = file_path.read_text(encoding="utf-8")
            for token in forbidden_tokens:
                self.assertNotIn(token, text, f"{file_path} should go through ICoordinationBackend")

    def test_slot_recovery_store_header_uses_coordination_backend_interface_only(self):
        header = REPO_ROOT / "src/datasystem/worker/object_cache/slot_recovery/slot_recovery_store.h"
        text = header.read_text(encoding="utf-8")
        self.assertIn("coordination_backend/coordination_backend.h", text)
        forbidden_tokens = [
            "etcd_coordination_backend.h",
            "ds_coordination_backend.h",
            "EtcdCoordinationBackend",
            "DsCoordinationBackend",
        ]
        for token in forbidden_tokens:
            self.assertNotIn(token, text)

    def test_object_cache_coordination_headers_use_coordination_backend_interface_only(self):
        files = [
            REPO_ROOT / "src/datasystem/worker/object_cache/central_metadata_address_resolver.h",
            REPO_ROOT / "src/datasystem/worker/object_cache/object_metadata_coordination_reader.h",
        ]

        for file_path in files:
            text = file_path.read_text(encoding="utf-8")
            self.assertIn("coordination_backend/coordination_backend.h", text)
            forbidden_tokens = [
                "etcd_coordination_backend.h",
                "ds_coordination_backend.h",
                "EtcdCoordinationBackend",
                "DsCoordinationBackend",
            ]
            for token in forbidden_tokens:
                self.assertNotIn(token, text)

    def test_object_cache_module_does_not_use_concrete_coordination_backends(self):
        object_cache_root = REPO_ROOT / "src/datasystem/worker/object_cache"
        files = [
            path
            for path in object_cache_root.rglob("*")
            if path.suffix in {".h", ".cpp", ".cc"}
        ]

        forbidden_tokens = [
            "datasystem/common/kvstore/etcd/etcd_store.h",
            "datasystem/cluster/coordination_backend/ds_coordination_backend.h",
            "datasystem/cluster/coordination_backend/etcd_coordination_backend.h",
            "EtcdStore",
            "DsCoordinationBackend",
            "EtcdCoordinationBackend",
        ]

        for file_path in files:
            text = file_path.read_text(encoding="utf-8", errors="ignore")
            for token in forbidden_tokens:
                self.assertNotIn(token, text, f"{file_path} should use ICoordinationBackend injected abstractions")

    def test_node_selector_uses_runtime_facade_not_runtime_state_manager(self):
        files = [
            REPO_ROOT / "src/datasystem/worker/object_cache/data_migrator/strategy/node_selector.h",
            REPO_ROOT / "src/datasystem/worker/object_cache/data_migrator/strategy/node_selector.cpp",
        ]

        forbidden_tokens = [
            "SetRuntimeStateManager",
            "WorkerRuntimeStateManager *",
            "worker_runtime_state.h",
            "runtimeState_",
        ]

        for file_path in files:
            text = file_path.read_text(encoding="utf-8")
            for token in forbidden_tokens:
                self.assertNotIn(token, text, f"{file_path} should depend on WorkerRuntimeFacade only")

    def test_worker_services_use_runtime_facade_for_admission(self):
        files = [
            REPO_ROOT / "src/datasystem/worker/worker_service_impl.h",
            REPO_ROOT / "src/datasystem/worker/worker_service_impl.cpp",
            REPO_ROOT / "src/datasystem/worker/object_cache/worker_oc_service_impl.h",
            REPO_ROOT / "src/datasystem/worker/object_cache/worker_oc_service_impl.cpp",
            REPO_ROOT / "src/datasystem/worker/object_cache/master_worker_oc_service_impl.h",
            REPO_ROOT / "src/datasystem/worker/object_cache/master_worker_oc_service_impl.cpp",
            REPO_ROOT / "src/datasystem/worker/object_cache/worker_worker_oc_service_impl.cpp",
            REPO_ROOT / "src/datasystem/worker/stream_cache/client_worker_sc_service_impl.cpp",
        ]

        forbidden_tokens = [
            "datasystem/worker/runtime/worker_admission_facade.h",
            "datasystem/worker/runtime/worker_service_admission.h",
            "datasystem/worker/runtime/worker_runtime_state.h",
            "WorkerRuntimeStateReadGuard",
        ]

        for file_path in files:
            text = file_path.read_text(encoding="utf-8")
            for token in forbidden_tokens:
                self.assertNotIn(token, text, f"{file_path} should use WorkerRuntimeFacade as the runtime boundary")

    def test_object_cache_service_does_not_keep_etcd_store(self):
        files = [
            REPO_ROOT / "src/datasystem/worker/object_cache/worker_oc_service_impl.h",
            REPO_ROOT / "src/datasystem/worker/object_cache/worker_oc_service_impl.cpp",
        ]

        forbidden_tokens = [
            "EtcdStore *etcdStore",
            "etcdStore_",
            "datasystem/common/kvstore/etcd/etcd_store.h",
        ]

        for file_path in files:
            text = file_path.read_text(encoding="utf-8")
            for token in forbidden_tokens:
                self.assertNotIn(token, text, f"{file_path} should use injected coordination/metadata capabilities")

        build_file = REPO_ROOT / "src/datasystem/worker/object_cache/BUILD.bazel"
        text = build_file.read_text(encoding="utf-8")
        target_slice = text.split('name = "worker_oc_service_impl"', 1)[1]
        target_slice = target_slice.split("ds_cc_library(", 1)[0]
        self.assertNotIn("common/kvstore/etcd:etcd_store", target_slice)

    def test_get_service_uses_metadata_reader_not_coordination_backend(self):
        files = [
            REPO_ROOT / "src/datasystem/worker/object_cache/service/BUILD.bazel",
            REPO_ROOT / "src/datasystem/worker/object_cache/service/worker_oc_service_get_impl.h",
            REPO_ROOT / "src/datasystem/worker/object_cache/service/worker_oc_service_get_impl.cpp",
        ]

        forbidden_tokens = [
            "cluster::ICoordinationBackend",
            "coordination_backend/coordination_backend.h",
            "coordinationBackend_",
            "EnsureCoordinationBackendAvailable",
            "QueryObjectMetadataFromCoordination",
        ]

        for file_path in files:
            text = file_path.read_text(encoding="utf-8")
            if file_path.name == "BUILD.bazel":
                text = text.split('name = "worker_oc_service_get_impl_header"', 1)[1]
                text = text.split('name = "worker_oc_service_delete_impl_header"', 1)[0]
            for token in forbidden_tokens:
                self.assertNotIn(token, text, f"{file_path} should use object metadata reader abstraction")

    def test_worker_oc_server_uses_central_metadata_address_resolver(self):
        server = REPO_ROOT / "src/datasystem/worker/worker_oc_server.cpp"
        text = server.read_text(encoding="utf-8")
        resolver_slice = text.split("Status WorkerOCServer::ResolveCentralMetadataAddress", 1)[0]

        forbidden_tokens = [
            "ClaimOrReadCentralMetadataAddress",
            "COORDINATION_MASTER_ADDRESS_TABLE",
            "COORDINATION_MASTER_ADDRESS_KEY",
        ]

        for token in forbidden_tokens:
            self.assertNotIn(token, resolver_slice, "WorkerOCServer should delegate central metadata address policy")
        self.assertIn("CentralMetadataAddressResolver", text)

    def test_worker_oc_server_delegates_concrete_coordination_backend_construction(self):
        files = [
            REPO_ROOT / "src/datasystem/worker/worker_oc_server.cpp",
            REPO_ROOT / "src/datasystem/worker/worker_oc_server.h",
        ]

        forbidden_tokens = [
            "datasystem/cluster/coordination_backend/ds_coordination_backend.h",
            "datasystem/cluster/coordination_backend/etcd_coordination_backend.h",
            "cluster::DsCoordinationBackend",
            "cluster::EtcdCoordinationBackend",
        ]

        for file_path in files:
            text = file_path.read_text(encoding="utf-8")
            for token in forbidden_tokens:
                self.assertNotIn(token, text, f"{file_path} should use worker coordination backend factory")

        factory = REPO_ROOT / "src/datasystem/worker/worker_coordination_backend_factory.h"
        self.assertTrue(factory.exists())

    def test_worker_composition_uses_runtime_facade_methods(self):
        files = [
            REPO_ROOT / "src/datasystem/worker/worker_oc_server.cpp",
        ]

        forbidden_tokens = [
            "workerRuntime_.RuntimeState()",
        ]

        for file_path in files:
            text = file_path.read_text(encoding="utf-8")
            for token in forbidden_tokens:
                self.assertNotIn(token, text, f"{file_path} should use WorkerRuntimeFacade semantic methods")

    def test_worker_runtime_facade_does_not_expose_state_manager(self):
        header = REPO_ROOT / "src/datasystem/worker/runtime/worker_runtime_facade.h"
        text = header.read_text(encoding="utf-8")

        forbidden_tokens = [
            "worker_admission_facade.h",
            "worker_recovery_controller.h",
            "worker_runtime_state.h",
            "RuntimeState()",
            "WorkerRuntimeStateManager &",
            "const WorkerRuntimeStateManager &",
            "WorkerRuntimeStateReadGuard",
        ]

        for token in forbidden_tokens:
            self.assertNotIn(token, text, "WorkerRuntimeFacade should expose semantic runtime operations only")

    def test_topology_availability_public_header_uses_runtime_facade_only(self):
        header = REPO_ROOT / "src/datasystem/worker/runtime/worker_topology_availability_admission.h"
        text = header.read_text(encoding="utf-8")

        forbidden_tokens = [
            "worker_recovery_controller.h",
            "worker_runtime_state.h",
            "WorkerRuntimeStateManager",
            "WorkerRecoveryController",
        ]

        for token in forbidden_tokens:
            self.assertNotIn(token, text, "Topology availability admission should expose WorkerRuntimeFacade only")

    def test_worker_isolation_coordinator_bazel_declares_runtime_facade_dependency(self):
        header = REPO_ROOT / "src/datasystem/worker/runtime/worker_isolation_coordinator.h"
        build_file = REPO_ROOT / "src/datasystem/worker/runtime/BUILD.bazel"

        self.assertIn("worker_runtime_facade.h", header.read_text(encoding="utf-8"))
        text = build_file.read_text(encoding="utf-8")
        target_slice = text.split('name = "worker_isolation_coordinator"', 1)[1]
        target_slice = target_slice.split("ds_cc_library(", 1)[0]
        self.assertIn(":worker_runtime_facade", target_slice)

    def test_object_cache_public_headers_do_not_expose_runtime_internals(self):
        files = [
            REPO_ROOT / "src/datasystem/worker/object_cache/object_cache_recovery_state.h",
            REPO_ROOT / "src/datasystem/worker/object_cache/worker_oc_service_impl.h",
            REPO_ROOT / "src/datasystem/worker/object_cache/worker_worker_oc_service_impl.h",
        ]

        forbidden_tokens = [
            "datasystem/worker/runtime/worker_admission_facade.h",
            "datasystem/worker/runtime/worker_recovery_evidence_tracker.h",
            "datasystem/worker/runtime/worker_runtime_state.h",
            "datasystem/worker/runtime/worker_service_admission.h",
            "WorkerRuntimeStateReadGuard",
        ]

        for file_path in files:
            text = file_path.read_text(encoding="utf-8")
            for token in forbidden_tokens:
                self.assertNotIn(token, text, f"{file_path} should hide runtime internals behind facade methods")

    def test_stream_service_uses_runtime_facade_for_admission(self):
        header = REPO_ROOT / "src/datasystem/worker/stream_cache/client_worker_sc_service_impl.h"
        impl = REPO_ROOT / "src/datasystem/worker/stream_cache/client_worker_sc_service_impl.cpp"
        server = REPO_ROOT / "src/datasystem/worker/worker_oc_server.cpp"

        self.assertIn("SetRuntimeFacade", header.read_text(encoding="utf-8"))
        self.assertIn("AcquireStreamAdmissionGuard", impl.read_text(encoding="utf-8"))
        self.assertIn("runtime->AcquireAdmissionGuard", impl.read_text(encoding="utf-8"))
        self.assertNotIn("runtime_->CheckAdmission", impl.read_text(encoding="utf-8"))
        self.assertIn("streamCacheClientWorkerSvc_->SetRuntimeFacade(&workerRuntime_)", server.read_text(encoding="utf-8"))

    def test_object_cache_service_uses_runtime_guard_for_admission(self):
        impl = REPO_ROOT / "src/datasystem/worker/object_cache/worker_oc_service_impl.cpp"
        text = impl.read_text(encoding="utf-8")

        self.assertIn("AcquireObjectCacheAdmissionGuard", text)
        self.assertIn("runtime->AcquireAdmissionGuard", text)
        self.assertNotIn('runtime_->CheckAdmission(kind, "ObjectCacheService")', text)

    def test_stream_public_headers_do_not_expose_runtime_internals(self):
        files = [
            REPO_ROOT / "src/datasystem/worker/stream_cache/client_worker_sc_service_impl.h",
        ]

        forbidden_tokens = [
            "datasystem/worker/runtime/worker_admission_facade.h",
            "datasystem/worker/runtime/worker_recovery_evidence_tracker.h",
            "datasystem/worker/runtime/worker_runtime_state.h",
            "datasystem/worker/runtime/worker_service_admission.h",
        ]

        for file_path in files:
            text = file_path.read_text(encoding="utf-8")
            for token in forbidden_tokens:
                self.assertNotIn(token, text, f"{file_path} should hide runtime internals behind facade methods")


if __name__ == "__main__":
    unittest.main()
