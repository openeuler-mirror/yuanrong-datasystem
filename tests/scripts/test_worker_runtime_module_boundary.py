import pathlib
import unittest


REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]


class WorkerRuntimeModuleBoundaryTest(unittest.TestCase):
    def test_control_backend_scope_does_not_depend_on_object_cache_transport(self):
        build_files = [
            REPO_ROOT / "src/datasystem/worker/runtime/BUILD.bazel",
            REPO_ROOT / "src/datasystem/worker/runtime/CMakeLists.txt",
        ]

        forbidden_tokens = [
            "worker_object_cache",
            "//src/datasystem/worker/object_cache:worker_worker_oc_api",
            "//src/datasystem/worker/object_cache:worker_worker_peer_state_codec",
        ]

        for build_file in build_files:
            text = build_file.read_text(encoding="utf-8")
            for token in forbidden_tokens:
                self.assertNotIn(token, text, f"{build_file} should not expose object_cache dependencies")

    def test_control_backend_probe_is_not_in_object_cache_aggregate_library(self):
        cmake_file = REPO_ROOT / "src/datasystem/worker/object_cache/CMakeLists.txt"
        text = cmake_file.read_text(encoding="utf-8")
        aggregate_sources = text.split("set(WORKER_OC_SRCS", 1)[1].split(")", 1)[0]
        self.assertNotIn("worker_control_backend_probe.cpp", aggregate_sources)

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


if __name__ == "__main__":
    unittest.main()
