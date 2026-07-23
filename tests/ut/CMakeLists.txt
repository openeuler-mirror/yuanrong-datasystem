project(ut)

include(GoogleTest)

set(PROJECT_DIR "${PROJECT_SOURCE_DIR}/../..")

set(DS_UT_DEPEND_LIBS
        GTest::gtest
        GTest::gmock
        nlohmann_json::nlohmann_json
        datasystem_static
        binmock
        generic_service_protos
        slot_recovery_protos
        common_etcd
        common_log_proto
        common_coordinator_store
        static_coordinator_discovery
        coordinator_service_impl
        common_log
        common_metrics
        common_buffer
        common_util_queue
        common_util
        common_iam
        common_shared_memory
        common_ak_sk
        common_persistence_api
        common_immutable_string
        string_ref
        common_parallel
        httpclient
        master_object_cache
        worker_object_cache
        cluster_topology
        cluster_query_projector
        worker_client_manager
        datasystem_worker_static
        )

# common_util/thread_local.cpp defines thread_local ZmqMessage; minimal UT targets need
# common_rpc_zmq to resolve ZmqMessage ctor/dtor at link time.
set(DS_UT_COMMON_UTIL_EXTRA_LIBS common_rpc_zmq)
# Wrap heavy UT link lines: GNU ld scans each static archive once unless grouped.
set(DS_UT_HEAVY_LINK_START -Wl,--start-group)
set(DS_UT_HEAVY_LINK_END -Wl,--end-group)

#include dirs
include_directories(${CMAKE_CURRENT_SOURCE_DIR})
include_directories(${PROJECT_DIR}/src)
include_directories(${PROJECT_DIR}/tests)  # For #include "ut/common.h"
include_directories(${PROJECT_DIR}/tests/support)

# fetch ut test files and remove the files we don't care about.
file(GLOB_RECURSE DS_TEST_UT_SRCS CONFIGURE_DEPENDS "${CMAKE_CURRENT_SOURCE_DIR}/*.cpp")
file(GLOB_RECURSE DS_UT_STREAM_SRCS CONFIGURE_DEPENDS "${CMAKE_CURRENT_SOURCE_DIR}/**/stream_cache/*.cpp")
file(GLOB_RECURSE DS_UT_OBJECT_SRCS CONFIGURE_DEPENDS "${CMAKE_CURRENT_SOURCE_DIR}/**/object_cache/*.cpp")

set(UT_COMMON_SRCS
        test_main.cpp
        common.cpp)

set(BIN_PATH_LIST LLT_BIN_PATH="${CMAKE_CURRENT_BINARY_DIR}")

list(FILTER DS_TEST_UT_SRCS EXCLUDE REGEX .*/device/.*)
list(FILTER DS_TEST_UT_SRCS EXCLUDE REGEX .*/binmock/.*)
list(FILTER DS_TEST_UT_SRCS EXCLUDE REGEX .*/flags/.*)
list(FILTER DS_TEST_UT_SRCS EXCLUDE REGEX .*/common/l2cache/slot_store_test\.cpp$)
list(FILTER DS_TEST_UT_SRCS EXCLUDE REGEX .*/common/coordinator/coordinator_store_perf_test\.cpp$)
list(FILTER DS_TEST_UT_SRCS EXCLUDE REGEX .*/coordinator/coordinator_server_options_test\.cpp$)
list(FILTER DS_TEST_UT_SRCS EXCLUDE REGEX .*/worker/data_worker_options_test\.cpp$)
list(FILTER DS_TEST_UT_SRCS EXCLUDE REGEX .*/worker/worker_runtime_state_test\.cpp$)
list(FILTER DS_TEST_UT_SRCS EXCLUDE REGEX .*/worker/worker_service_admission_test\.cpp$)
list(FILTER DS_TEST_UT_SRCS EXCLUDE REGEX .*/worker/worker_admission_facade_test\.cpp$)
list(FILTER DS_TEST_UT_SRCS EXCLUDE REGEX .*/worker/worker_runtime_facade_test\.cpp$)
list(FILTER DS_TEST_UT_SRCS EXCLUDE REGEX .*/worker/worker_isolation_coordinator_test\.cpp$)
list(FILTER DS_TEST_UT_SRCS EXCLUDE REGEX .*/test_main\.cpp$)
list(FILTER DS_TEST_UT_SRCS EXCLUDE REGEX .*/common\\.cpp$)
list(FILTER DS_TEST_UT_SRCS EXCLUDE REGEX .*/stream_cache/.*)
list(FILTER DS_TEST_UT_SRCS EXCLUDE REGEX .*/object_cache/.*)
list(FILTER DS_TEST_UT_SRCS EXCLUDE REGEX .*/log/log_performance_test\\.cpp$)
list(FILTER DS_TEST_UT_SRCS EXCLUDE REGEX .*/operation_logger_test\\.cpp$)
list(FILTER DS_TEST_UT_SRCS EXCLUDE REGEX .*/dynamic_config_updater_test\\.cpp$)
list(FILTER DS_TEST_UT_SRCS EXCLUDE REGEX .*/flags_monitor_gate_test\\.cpp$)
list(FILTER DS_TEST_UT_SRCS EXCLUDE REGEX .*/flags_config_monitor_test\\.cpp$)
list(FILTER DS_TEST_UT_SRCS EXCLUDE REGEX .*/cluster/cluster_topology_schema_test\\.cpp$)
list(FILTER DS_TEST_UT_SRCS EXCLUDE REGEX .*/cluster/topology_key_helper_test\\.cpp$)
list(FILTER DS_TEST_UT_SRCS EXCLUDE REGEX .*/cluster/coordination_backend_contract_test\\.cpp$)
list(FILTER DS_TEST_UT_SRCS EXCLUDE REGEX .*/cluster/ds_coordination_backend_session_test\\.cpp$)
list(FILTER DS_TEST_UT_SRCS EXCLUDE REGEX .*/cluster/membership_contract_test\\.cpp$)
list(FILTER DS_TEST_UT_SRCS EXCLUDE REGEX .*/cluster/topology_snapshot_test\\.cpp$)
list(FILTER DS_TEST_UT_SRCS EXCLUDE REGEX .*/cluster/topology_repository_codec_test\\.cpp$)
list(FILTER DS_TEST_UT_SRCS EXCLUDE REGEX .*/cluster/topology_repository_test\\.cpp$)
list(FILTER DS_TEST_UT_SRCS EXCLUDE REGEX .*/cluster/topology_reader_test\\.cpp$)
list(FILTER DS_TEST_UT_SRCS EXCLUDE REGEX .*/cluster/topology_observer_test\\.cpp$)
list(FILTER DS_TEST_UT_SRCS EXCLUDE REGEX .*/cluster/coordination_event_dispatcher_test\\.cpp$)
list(FILTER DS_TEST_UT_SRCS EXCLUDE REGEX .*/cluster/hash_algorithm_test\\.cpp$)
list(FILTER DS_TEST_UT_SRCS EXCLUDE REGEX .*/cluster/placement_facade_test\\.cpp$)
list(FILTER DS_TEST_UT_SRCS EXCLUDE REGEX .*/cluster/topology_plan_builder_test\\.cpp$)
list(FILTER DS_TEST_UT_SRCS EXCLUDE REGEX .*/cluster/topology_failure_classifier_test\\.cpp$)
list(FILTER DS_TEST_UT_SRCS EXCLUDE REGEX .*/cluster/topology_task_materializer_test\\.cpp$)
list(FILTER DS_TEST_UT_SRCS EXCLUDE REGEX .*/cluster/topology_controller_test\\.cpp$)
list(FILTER DS_TEST_UT_SRCS EXCLUDE REGEX .*/cluster/topology_controller_runtime_test\\.cpp$)
list(FILTER DS_TEST_UT_SRCS EXCLUDE REGEX .*/cluster/hash_key_filter_test\\.cpp$)
list(FILTER DS_TEST_UT_SRCS EXCLUDE REGEX .*/cluster/storage_scan_plan_test\\.cpp$)
list(FILTER DS_TEST_UT_SRCS EXCLUDE REGEX .*/cluster/topology_task_executor_test\\.cpp$)
list(FILTER DS_TEST_UT_SRCS EXCLUDE REGEX .*/cluster/topology_engine_test\\.cpp$)
list(FILTER DS_TEST_UT_SRCS EXCLUDE REGEX .*/cluster/topology_runtime_composition_test\\.cpp$)
list(FILTER DS_TEST_UT_SRCS EXCLUDE REGEX .*/cluster/topology_task_janitor_test\\.cpp$)
list(FILTER DS_TEST_UT_SRCS EXCLUDE REGEX .*/cluster/topology_dfx_test\\.cpp$)
list(FILTER DS_TEST_UT_SRCS EXCLUDE REGEX .*/cluster/topology_shutdown_test\\.cpp$)
list(FILTER DS_TEST_UT_SRCS EXCLUDE REGEX .*/cluster/testing/fake_coordination_backend\\.cpp$)

# These targets need a real URMA device. Build them separately below instead of adding them to the default ds_ut
# binary.
list(FILTER DS_TEST_UT_SRCS EXCLUDE REGEX ".*/client/urma_remote_jetty_reuse_test\\.cpp$")
list(FILTER DS_TEST_UT_SRCS EXCLUDE REGEX ".*/client/urma_send_jetty_lifecycle_test\\.cpp$")
list(FILTER DS_TEST_UT_SRCS EXCLUDE REGEX ".*/client/urma_send_jetty_fault_test\\.cpp$")
list(FILTER DS_TEST_UT_SRCS EXCLUDE REGEX ".*/client/urma_jetty_gate_test\\.cpp$")

if (NOT BUILD_WITH_RDMA)
    list(FILTER DS_TEST_UT_SRCS EXCLUDE REGEX ".*/rdma/.*\\.cpp$")
endif()
if (NOT BUILD_WITH_URMA)
    list(FILTER DS_TEST_UT_SRCS EXCLUDE REGEX ".*/client/urma_send_lane_test\\.cpp$")
endif()
if (NOT BUILD_WITH_URMA_MOCK)
    list(FILTER DS_TEST_UT_SRCS EXCLUDE REGEX ".*/common/urma_mock/.*\\.cpp$")
endif()

list(APPEND DS_UT_STREAM_SRCS)

add_library(_ds_ut_main_obj OBJECT test_main.cpp)
add_library(_ds_ut_common_obj OBJECT common.cpp)
target_compile_definitions(_ds_ut_main_obj PRIVATE ${BIN_PATH_LIST})
target_compile_definitions(_ds_ut_common_obj PRIVATE ${BIN_PATH_LIST})
target_link_libraries(_ds_ut_main_obj PRIVATE GTest::gtest GTest::gmock)
target_link_libraries(_ds_ut_common_obj PRIVATE ${DS_UT_DEPEND_LIBS})

function(add_ds_ut_executable TARGET_NAME)
    add_executable(${TARGET_NAME} ${ARGN})
    target_compile_definitions(${TARGET_NAME} PRIVATE ${BIN_PATH_LIST})
endfunction()

add_ds_ut_executable(ds_ut
        ${DS_TEST_UT_SRCS}
        common/flags/flag_manager_config_test.cpp
        $<TARGET_OBJECTS:_ds_ut_main_obj>
        $<TARGET_OBJECTS:_ds_ut_common_obj>)
add_ds_ut_executable(ds_ut_stream
        ${DS_UT_STREAM_SRCS}
        $<TARGET_OBJECTS:_ds_ut_main_obj>
        $<TARGET_OBJECTS:_ds_ut_common_obj>)
add_ds_ut_executable(ds_ut_object
        ${DS_UT_OBJECT_SRCS}
        $<TARGET_OBJECTS:_ds_ut_main_obj>
        $<TARGET_OBJECTS:_ds_ut_common_obj>)
if (BUILD_WITH_URMA OR BUILD_WITH_URMA_MOCK)
    add_ds_ut_executable(ds_ut_urma_remote_jetty_reuse
            client/urma_remote_jetty_reuse_test.cpp
            $<TARGET_OBJECTS:_ds_ut_main_obj>)
    add_ds_ut_executable(ds_ut_urma_send_jetty_lifecycle
            client/urma_send_jetty_lifecycle_test.cpp
            $<TARGET_OBJECTS:_ds_ut_main_obj>)
    add_ds_ut_executable(ds_ut_urma_send_jetty_fault
            client/urma_send_jetty_fault_test.cpp
            $<TARGET_OBJECTS:_ds_ut_main_obj>)
endif()
if (BUILD_WITH_URMA_MOCK)
        foreach(_urma_ut_target ds_ut_urma_remote_jetty_reuse ds_ut_urma_send_jetty_lifecycle
                                ds_ut_urma_send_jetty_fault)
                target_compile_definitions(${_urma_ut_target} PRIVATE USE_URMA USE_URMA_MOCK)
                target_link_libraries(${_urma_ut_target} PRIVATE common_urma_mock_obj)
        endforeach()
endif()
if (BUILD_WITH_URMA)
    add_ds_ut_executable(ds_ut_urma_jetty_gate
            client/urma_jetty_gate_test.cpp
            $<TARGET_OBJECTS:_ds_ut_main_obj>)
endif()
add_executable(ds_ut_slot_store
        common/l2cache/slot_store_test.cpp
        $<TARGET_OBJECTS:_ds_ut_main_obj>)
add_executable(coordinator_store_perf_test
        common/coordinator/coordinator_store_perf_test.cpp
        $<TARGET_OBJECTS:_ds_ut_main_obj>
        $<TARGET_OBJECTS:_ds_ut_common_obj>)
add_ds_ut_executable(coordinator_server_options_test
        coordinator/coordinator_server_options_test.cpp
        $<TARGET_OBJECTS:_ds_ut_main_obj>)
add_ds_ut_executable(data_worker_options_test
        worker/data_worker_options_test.cpp
        $<TARGET_OBJECTS:_ds_ut_main_obj>
        $<TARGET_OBJECTS:_ds_ut_common_obj>)
add_ds_ut_executable(worker_runtime_state_ut
        worker/worker_runtime_state_test.cpp
        $<TARGET_OBJECTS:_ds_ut_main_obj>)
add_ds_ut_executable(worker_service_admission_ut
        worker/worker_service_admission_test.cpp
        $<TARGET_OBJECTS:_ds_ut_main_obj>)
add_ds_ut_executable(worker_admission_facade_ut
        worker/worker_admission_facade_test.cpp
        $<TARGET_OBJECTS:_ds_ut_main_obj>)
add_ds_ut_executable(worker_runtime_facade_ut
        worker/worker_runtime_facade_test.cpp
        $<TARGET_OBJECTS:_ds_ut_main_obj>)
add_ds_ut_executable(worker_isolation_coordinator_ut
        worker/worker_isolation_coordinator_test.cpp
        $<TARGET_OBJECTS:_ds_ut_main_obj>)
add_executable(cluster_topology_contract_ut
        cluster/cluster_topology_schema_test.cpp
        cluster/coordination_backend_contract_test.cpp
        cluster/ds_coordination_backend_session_test.cpp
        cluster/membership_contract_test.cpp
        cluster/topology_repository_codec_test.cpp
        cluster/topology_repository_test.cpp
        cluster/topology_snapshot_test.cpp
        cluster/topology_reader_test.cpp
        cluster/topology_observer_test.cpp
        cluster/coordination_event_dispatcher_test.cpp
        cluster/hash_algorithm_test.cpp
        cluster/placement_facade_test.cpp
        cluster/topology_plan_builder_test.cpp
        cluster/topology_failure_classifier_test.cpp
        cluster/topology_task_materializer_test.cpp
        cluster/topology_controller_test.cpp
        cluster/topology_controller_runtime_test.cpp
        cluster/hash_key_filter_test.cpp
        cluster/storage_scan_plan_test.cpp
        cluster/topology_task_executor_test.cpp
        cluster/topology_engine_test.cpp
        cluster/topology_runtime_composition_test.cpp
        cluster/topology_task_janitor_test.cpp
        cluster/topology_dfx_test.cpp
        cluster/topology_shutdown_test.cpp
        cluster/testing/fake_coordination_backend.cpp
        cluster/topology_key_helper_test.cpp
        $<TARGET_OBJECTS:_ds_ut_main_obj>)

if (BUILD_WITH_URMA_MOCK)
        target_compile_definitions(ds_ut PRIVATE USE_URMA USE_URMA_MOCK)
        target_link_libraries(ds_ut PRIVATE common_urma_mock_obj)
endif()

target_link_libraries(ds_ut PRIVATE
        ${DS_UT_HEAVY_LINK_START}
        ${DS_UT_DEPEND_LIBS}
        ${DS_UT_COMMON_UTIL_EXTRA_LIBS}
        ${DS_UT_HEAVY_LINK_END}
        -Wl,--reduce-memory-overheads)
target_link_libraries(ds_ut_stream PRIVATE
        ${DS_UT_HEAVY_LINK_START}
        ${DS_UT_DEPEND_LIBS}
        ${DS_UT_COMMON_UTIL_EXTRA_LIBS}
        worker_stream_cache
        datasystem_worker_static
        ${DS_UT_HEAVY_LINK_END}
        -Wl,--reduce-memory-overheads)
target_link_libraries(ds_ut_object PRIVATE
        ${DS_UT_HEAVY_LINK_START}
        ${DS_UT_DEPEND_LIBS}
        ${DS_UT_COMMON_UTIL_EXTRA_LIBS}
        ${DS_UT_HEAVY_LINK_END}
        -Wl,--reduce-memory-overheads)
if (BUILD_WITH_URMA OR BUILD_WITH_URMA_MOCK)
    target_link_libraries(ds_ut_urma_remote_jetty_reuse PRIVATE
            ${DS_UT_HEAVY_LINK_START}
            ${DS_UT_DEPEND_LIBS}
            ${DS_UT_COMMON_UTIL_EXTRA_LIBS}
            ${DS_UT_HEAVY_LINK_END}
            -Wl,--reduce-memory-overheads)
    target_link_libraries(ds_ut_urma_send_jetty_lifecycle PRIVATE
            ${DS_UT_HEAVY_LINK_START}
            ${DS_UT_DEPEND_LIBS}
            ${DS_UT_COMMON_UTIL_EXTRA_LIBS}
            ${DS_UT_HEAVY_LINK_END}
            -Wl,--reduce-memory-overheads)
    target_link_libraries(ds_ut_urma_send_jetty_fault PRIVATE
            ${DS_UT_HEAVY_LINK_START}
            ${DS_UT_DEPEND_LIBS}
            ${DS_UT_COMMON_UTIL_EXTRA_LIBS}
            ${DS_UT_HEAVY_LINK_END}
            -Wl,--reduce-memory-overheads)
endif()
if (BUILD_WITH_URMA)
    target_link_libraries(ds_ut_urma_jetty_gate PRIVATE
            ${DS_UT_HEAVY_LINK_START}
            ${DS_UT_DEPEND_LIBS}
            ${DS_UT_COMMON_UTIL_EXTRA_LIBS}
            ${DS_UT_HEAVY_LINK_END}
            -Wl,--reduce-memory-overheads)
endif()
target_link_libraries(ds_ut_slot_store PRIVATE
        GTest::gtest
        GTest::gmock
        binmock
        common_persistence_api
        common_slot_client
        common_log
        common_util
        common_request_context)
target_link_libraries(coordinator_store_perf_test PRIVATE ${DS_UT_DEPEND_LIBS})
target_link_libraries(coordinator_server_options_test PRIVATE
        coordinator_server
        GTest::gtest
        GTest::gmock
        common_log
        common_util)
target_link_libraries(data_worker_options_test PRIVATE
        ${DS_UT_HEAVY_LINK_START}
        ${DS_UT_DEPEND_LIBS}
        ${DS_UT_COMMON_UTIL_EXTRA_LIBS}
        ${DS_UT_HEAVY_LINK_END}
        -Wl,--reduce-memory-overheads)
target_link_libraries(worker_runtime_state_ut PRIVATE
        GTest::gtest
        GTest::gmock
        worker_runtime_state
        ${DS_UT_COMMON_UTIL_EXTRA_LIBS})
target_link_libraries(worker_service_admission_ut PRIVATE
        GTest::gtest
        GTest::gmock
        worker_service_admission
        ${DS_UT_COMMON_UTIL_EXTRA_LIBS})
target_link_libraries(worker_admission_facade_ut PRIVATE
        GTest::gtest
        GTest::gmock
        worker_admission_facade
        ${DS_UT_COMMON_UTIL_EXTRA_LIBS})
target_link_libraries(worker_runtime_facade_ut PRIVATE
        GTest::gtest
        GTest::gmock
        worker_runtime_facade
        ${DS_UT_COMMON_UTIL_EXTRA_LIBS})
target_link_libraries(worker_isolation_coordinator_ut PRIVATE
        GTest::gtest
        GTest::gmock
        worker_isolation_coordinator
        ${DS_UT_COMMON_UTIL_EXTRA_LIBS})
target_link_libraries(cluster_topology_contract_ut PRIVATE
        GTest::gtest
        GTest::gmock
        cluster_topology
        ${DS_UT_COMMON_UTIL_EXTRA_LIBS})
target_compile_definitions(ds_ut_slot_store PRIVATE ${BIN_PATH_LIST})

if (BUILD_OBSERVABILITY)
    set(TEST_ENVIRONMENT "LD_LIBRARY_PATH=${_WORKER_BIN_DIR}:${OpenSSL_LIB_PATH}:${gRPC_LIB_PATH}:${Protobuf_LIB_PATH}:${absl_LIB_PATH}:${gflags_LIB_PATH}:${brpc_LIB_PATH}:${LOGS_ROOT}/lib:${ICONV_ROOT}/lib:$ENV{LD_LIBRARY_PATH}"
        CACHE STRING "")
else()
    set(TEST_ENVIRONMENT "LD_LIBRARY_PATH=${_WORKER_BIN_DIR}:${OpenSSL_LIB_PATH}:${gRPC_LIB_PATH}:${Protobuf_LIB_PATH}:${absl_LIB_PATH}:${gflags_LIB_PATH}:${brpc_LIB_PATH}:${ICONV_ROOT}/lib:$ENV{LD_LIBRARY_PATH}"
        CACHE STRING "")
endif()

if (BUILD_WITH_RDMA)
        string(REPLACE "LD_LIBRARY_PATH=" "LD_LIBRARY_PATH=${UCX_LIB_PATH}:" TEST_ENVIRONMENT "${TEST_ENVIRONMENT}")
endif()

if (BUILD_WITH_URMA)
        get_filename_component(URMA_LIB_PATH ${URMA_LIBRARY} DIRECTORY)
        string(REPLACE "LD_LIBRARY_PATH=" "LD_LIBRARY_PATH=${URMA_LIB_PATH}:" TEST_ENVIRONMENT "${TEST_ENVIRONMENT}")
endif()

add_datasystem_test(ds_ut TEST_ENVIRONMENTS ${TEST_ENVIRONMENT})
add_datasystem_test(ds_ut_stream TEST_ENVIRONMENTS ${TEST_ENVIRONMENT})
add_datasystem_test(ds_ut_object TEST_ENVIRONMENTS ${TEST_ENVIRONMENT})
add_datasystem_test(ds_ut_slot_store TEST_ENVIRONMENTS ${TEST_ENVIRONMENT})
add_datasystem_test(coordinator_store_perf_test TEST_ENVIRONMENTS ${TEST_ENVIRONMENT})
add_datasystem_test(coordinator_server_options_test TEST_ENVIRONMENTS ${TEST_ENVIRONMENT})
add_datasystem_test(data_worker_options_test TEST_ENVIRONMENTS ${TEST_ENVIRONMENT})
add_datasystem_test(worker_runtime_state_ut TEST_ENVIRONMENTS ${TEST_ENVIRONMENT})
add_datasystem_test(worker_service_admission_ut TEST_ENVIRONMENTS ${TEST_ENVIRONMENT})
add_datasystem_test(worker_admission_facade_ut TEST_ENVIRONMENTS ${TEST_ENVIRONMENT})
add_datasystem_test(worker_runtime_facade_ut TEST_ENVIRONMENTS ${TEST_ENVIRONMENT})
add_datasystem_test(worker_isolation_coordinator_ut TEST_ENVIRONMENTS ${TEST_ENVIRONMENT})
add_datasystem_test(cluster_topology_contract_ut TEST_ENVIRONMENTS ${TEST_ENVIRONMENT})

if (BUILD_WITH_URMA OR BUILD_WITH_URMA_MOCK)
    add_datasystem_test(ds_ut_urma_remote_jetty_reuse
            TEST_ENVIRONMENTS ${TEST_ENVIRONMENT})
    add_datasystem_test(ds_ut_urma_send_jetty_lifecycle
            TEST_ENVIRONMENTS ${TEST_ENVIRONMENT})
    add_datasystem_test(ds_ut_urma_send_jetty_fault
            TEST_ENVIRONMENTS ${TEST_ENVIRONMENT})
endif()
if (BUILD_WITH_URMA)
    add_datasystem_test(ds_ut_urma_jetty_gate
            TEST_ENVIRONMENTS ${TEST_ENVIRONMENT})
endif()


add_ds_ut_executable(log_perf_ut
        "${CMAKE_CURRENT_SOURCE_DIR}/common/log/log_performance_test.cpp"
        $<TARGET_OBJECTS:_ds_ut_main_obj>
        $<TARGET_OBJECTS:_ds_ut_common_obj>)
target_link_libraries(log_perf_ut PRIVATE
        ${DS_UT_HEAVY_LINK_START}
        ${DS_UT_DEPEND_LIBS}
        ${DS_UT_COMMON_UTIL_EXTRA_LIBS}
        ${DS_UT_HEAVY_LINK_END}
        -Wl,--reduce-memory-overheads)

add_executable(flags_ut
        "${CMAKE_CURRENT_SOURCE_DIR}/common/flags/flags_test.cpp")
target_link_libraries(flags_ut
        ds_flags
        common_util
        nlohmann_json::nlohmann_json
        ${SECUREC_LIBRARY}
        GTest::gtest
        GTest::gmock
        -Wl,--whole-archive
        common_etcd
        common_shared_memory
        worker_object_cache
        datasystem_worker_static
        -Wl,--no-whole-archive)
add_datasystem_test(flags_ut TEST_ENVIRONMENTS ${TEST_ENVIRONMENT})

add_executable(util_flags_ut
        "${CMAKE_CURRENT_SOURCE_DIR}/common/flags/dynamic_flag_config_test.cpp"
        $<TARGET_OBJECTS:_ds_ut_main_obj>
        $<TARGET_OBJECTS:_ds_ut_common_obj>)
target_link_libraries(util_flags_ut PRIVATE
        ${DS_UT_HEAVY_LINK_START}
        ${DS_UT_DEPEND_LIBS}
        ${DS_UT_COMMON_UTIL_EXTRA_LIBS}
        dynamic_flag_config
        ${DS_UT_HEAVY_LINK_END}
        -Wl,--reduce-memory-overheads)
add_datasystem_test(util_flags_ut TEST_ENVIRONMENTS ${TEST_ENVIRONMENT})

add_executable(operation_logger_ut
        "${CMAKE_CURRENT_SOURCE_DIR}/common/log/operation_logger_test.cpp")
target_link_libraries(operation_logger_ut
        common_log
        ds_flags
        common_util
        ${DS_UT_COMMON_UTIL_EXTRA_LIBS}
        ${SECUREC_LIBRARY}
        GTest::gtest
        GTest::gmock)
add_datasystem_test(operation_logger_ut TEST_ENVIRONMENTS ${TEST_ENVIRONMENT})

add_executable(dynamic_config_updater_ut
        "${CMAKE_CURRENT_SOURCE_DIR}/common/flags/dynamic_config_updater_test.cpp"
        $<TARGET_OBJECTS:_ds_ut_main_obj>)
target_link_libraries(dynamic_config_updater_ut
        dynamic_flag_config
        common_log
        ds_flags
        common_util
        ${DS_UT_COMMON_UTIL_EXTRA_LIBS}
        nlohmann_json::nlohmann_json
        ${SECUREC_LIBRARY}
        GTest::gtest
        GTest::gmock)
add_datasystem_test(dynamic_config_updater_ut TEST_ENVIRONMENTS ${TEST_ENVIRONMENT})

add_executable(flags_monitor_gate_ut
        "${CMAKE_CURRENT_SOURCE_DIR}/common/flags/flags_monitor_gate_test.cpp"
        "${CMAKE_SOURCE_DIR}/src/datasystem/client/client_flags_monitor.cpp"
        $<TARGET_OBJECTS:_ds_ut_main_obj>)
target_link_libraries(flags_monitor_gate_ut
        dynamic_flag_config
        common_log
        ds_flags
        common_util
        ${DS_UT_COMMON_UTIL_EXTRA_LIBS}
        ${SECUREC_LIBRARY}
        GTest::gtest
        GTest::gmock)
add_datasystem_test(flags_monitor_gate_ut TEST_ENVIRONMENTS ${TEST_ENVIRONMENT})

add_executable(flags_config_monitor_ut
        "${CMAKE_CURRENT_SOURCE_DIR}/common/flags/flags_config_monitor_test.cpp"
        $<TARGET_OBJECTS:_ds_ut_main_obj>
        $<TARGET_OBJECTS:_ds_ut_common_obj>)
target_link_libraries(flags_config_monitor_ut PRIVATE
        ${DS_UT_HEAVY_LINK_START}
        ${DS_UT_DEPEND_LIBS}
        ${DS_UT_COMMON_UTIL_EXTRA_LIBS}
        dynamic_flag_config
        ${DS_UT_HEAVY_LINK_END}
        -Wl,--reduce-memory-overheads)
add_datasystem_test(flags_config_monitor_ut TEST_ENVIRONMENTS ${TEST_ENVIRONMENT})

if (BUILD_HETERO AND BUILD_HETERO_NPU)
    add_subdirectory(device/ascend)
endif()

set(EMBEDDED_CLIENT_WORKER_API_UT_DEPEND_LIBS
        GTest::gtest
        GTest::gmock
        embedded_client_worker_api
        common_util)
