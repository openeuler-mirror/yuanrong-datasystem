add_library(worker_runtime_types INTERFACE)
target_include_directories(worker_runtime_types INTERFACE ${PROJECT_SOURCE_DIR}/src)

add_library(worker_runtime_state STATIC worker_runtime_state.cpp)
target_include_directories(worker_runtime_state PUBLIC ${PROJECT_SOURCE_DIR}/src)
target_link_libraries(worker_runtime_state PUBLIC worker_runtime_types common_metrics)

add_library(worker_recovery_evidence STATIC worker_recovery_evidence.cpp)
target_include_directories(worker_recovery_evidence PUBLIC ${PROJECT_SOURCE_DIR}/src)
target_link_libraries(worker_recovery_evidence PUBLIC worker_runtime_types)

add_library(worker_recovery_controller STATIC worker_recovery_controller.cpp)
target_include_directories(worker_recovery_controller PUBLIC ${PROJECT_SOURCE_DIR}/src)
target_link_libraries(worker_recovery_controller PUBLIC worker_runtime_state worker_recovery_evidence)

add_library(worker_recovery_evidence_tracker STATIC worker_recovery_evidence_tracker.cpp)
target_include_directories(worker_recovery_evidence_tracker PUBLIC ${PROJECT_SOURCE_DIR}/src)
target_link_libraries(worker_recovery_evidence_tracker PUBLIC worker_recovery_evidence)

add_library(worker_isolation_coordinator STATIC worker_isolation_coordinator.cpp)
target_include_directories(worker_isolation_coordinator PUBLIC ${PROJECT_SOURCE_DIR}/src)
target_link_libraries(worker_isolation_coordinator PUBLIC worker_runtime_state common_inject common_log)

add_library(worker_topology_availability_admission STATIC worker_topology_availability_admission.cpp)
target_include_directories(worker_topology_availability_admission PUBLIC ${PROJECT_SOURCE_DIR}/src)
target_link_libraries(worker_topology_availability_admission PUBLIC
        worker_runtime_state
        worker_recovery_controller
        worker_runtime_facade
        cluster_topology
        )

add_library(worker_service_admission STATIC worker_service_admission.cpp)
target_include_directories(worker_service_admission PUBLIC ${PROJECT_SOURCE_DIR}/src)
target_link_libraries(worker_service_admission PUBLIC worker_runtime_state common_util)

add_library(worker_admission_facade STATIC worker_admission_facade.cpp)
target_include_directories(worker_admission_facade PUBLIC ${PROJECT_SOURCE_DIR}/src)
target_link_libraries(worker_admission_facade PUBLIC worker_service_admission)

add_library(worker_runtime_facade STATIC worker_runtime_facade.cpp)
target_include_directories(worker_runtime_facade PUBLIC ${PROJECT_SOURCE_DIR}/src)
target_link_libraries(worker_runtime_facade PUBLIC worker_admission_facade worker_recovery_controller)

add_library(worker_topology_runtime STATIC worker_topology_runtime.cpp)
target_include_directories(worker_topology_runtime PUBLIC ${PROJECT_SOURCE_DIR}/src)
target_link_libraries(worker_topology_runtime PUBLIC cluster_topology common_util)

add_library(worker_control_backend_probe STATIC worker_control_backend_probe.cpp)
target_include_directories(worker_control_backend_probe PUBLIC ${PROJECT_SOURCE_DIR}/src)
target_link_libraries(worker_control_backend_probe PRIVATE common_log common_util)

add_library(worker_runtime_core INTERFACE)
target_link_libraries(worker_runtime_core INTERFACE
        worker_admission_facade
        worker_isolation_coordinator
        worker_recovery_controller
        worker_recovery_evidence
        worker_recovery_evidence_tracker
        worker_runtime_facade
        worker_runtime_state
        worker_runtime_types
        worker_service_admission
        worker_topology_runtime
        worker_topology_availability_admission
        )

add_library(worker_topology_phase_callbacks STATIC worker_topology_phase_callbacks.cpp)
target_include_directories(worker_topology_phase_callbacks PUBLIC ${PROJECT_SOURCE_DIR}/src)
target_link_libraries(worker_topology_phase_callbacks PUBLIC
        worker_topology_availability_admission
        cluster_topology
        common_util
        )
