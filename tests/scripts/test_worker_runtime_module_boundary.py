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

    def test_slot_recovery_store_header_hides_coordination_backend_detail(self):
        header = REPO_ROOT / "src/datasystem/worker/object_cache/slot_recovery/slot_recovery_store.h"
        text = header.read_text(encoding="utf-8")
        self.assertIn("class ICoordinationBackend;", text)
        self.assertNotIn("coordination_backend/coordination_backend.h", text)

    def test_object_cache_coordination_headers_hide_backend_detail(self):
        files = [
            REPO_ROOT / "src/datasystem/worker/object_cache/central_metadata_address_resolver.h",
            REPO_ROOT / "src/datasystem/worker/object_cache/object_metadata_coordination_reader.h",
        ]

        for file_path in files:
            text = file_path.read_text(encoding="utf-8")
            self.assertIn("class ICoordinationBackend;", text)
            self.assertNotIn("coordination_backend/coordination_backend.h", text)

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
        ]

        forbidden_tokens = [
            "datasystem/worker/runtime/worker_admission_facade.h",
            "datasystem/worker/runtime/worker_service_admission.h",
            "datasystem/worker/runtime/worker_runtime_state.h",
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

    def test_object_cache_public_headers_do_not_expose_runtime_internals(self):
        files = [
            REPO_ROOT / "src/datasystem/worker/object_cache/worker_oc_service_impl.h",
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

    def test_stream_service_uses_runtime_facade_for_admission(self):
        header = REPO_ROOT / "src/datasystem/worker/stream_cache/client_worker_sc_service_impl.h"
        impl = REPO_ROOT / "src/datasystem/worker/stream_cache/client_worker_sc_service_impl.cpp"
        server = REPO_ROOT / "src/datasystem/worker/worker_oc_server.cpp"

        self.assertIn("SetRuntimeFacade", header.read_text(encoding="utf-8"))
        self.assertIn("runtime_->CheckAdmission", impl.read_text(encoding="utf-8"))
        self.assertIn("streamCacheClientWorkerSvc_->SetRuntimeFacade(&workerRuntime_)", server.read_text(encoding="utf-8"))

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
