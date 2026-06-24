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
        worker_hash_ring
        worker_topology_membership
        worker_topology_routing
        worker_topology_runtime
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

list(FILTER DS_TEST_UT_SRCS EXCLUDE REGEX .*/test_main\.cpp$)
list(FILTER DS_TEST_UT_SRCS EXCLUDE REGEX .*/common\\.cpp$)
list(FILTER DS_TEST_UT_SRCS EXCLUDE REGEX .*/stream_cache/.*)
list(FILTER DS_TEST_UT_SRCS EXCLUDE REGEX .*/object_cache/.*)
list(FILTER DS_TEST_UT_SRCS EXCLUDE REGEX .*/log/log_performance_test\\.cpp$)
list(FILTER DS_TEST_UT_SRCS EXCLUDE REGEX .*/operation_logger_test\\.cpp$)
list(FILTER DS_TEST_UT_SRCS EXCLUDE REGEX .*/dynamic_config_updater_test\\.cpp$)
list(FILTER DS_TEST_UT_SRCS EXCLUDE REGEX .*/flags_monitor_gate_test\\.cpp$)
list(FILTER DS_TEST_UT_SRCS EXCLUDE REGEX .*/flags_config_monitor_test\\.cpp$)

if (NOT BUILD_WITH_RDMA)
    list(FILTER DS_TEST_UT_SRCS EXCLUDE REGEX ".*/rdma/.*\\.cpp$")
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
add_executable(ds_ut_slot_store
        common/l2cache/slot_store_test.cpp
        $<TARGET_OBJECTS:_ds_ut_main_obj>)


target_link_libraries(ds_ut PRIVATE
        ${DS_UT_HEAVY_LINK_START}
        ${DS_UT_DEPEND_LIBS}
        ${DS_UT_COMMON_UTIL_EXTRA_LIBS}
        ${DS_UT_HEAVY_LINK_END})
target_link_libraries(ds_ut_stream PRIVATE
        ${DS_UT_HEAVY_LINK_START}
        ${DS_UT_DEPEND_LIBS}
        ${DS_UT_COMMON_UTIL_EXTRA_LIBS}
        worker_stream_cache
        datasystem_worker_static
        ${DS_UT_HEAVY_LINK_END})
target_link_libraries(ds_ut_object PRIVATE
        ${DS_UT_HEAVY_LINK_START}
        ${DS_UT_DEPEND_LIBS}
        ${DS_UT_COMMON_UTIL_EXTRA_LIBS}
        ${DS_UT_HEAVY_LINK_END})
target_link_libraries(ds_ut_slot_store PRIVATE
        GTest::gtest
        GTest::gmock
        binmock
        common_persistence_api
        common_slot_client
        common_log
        common_util)



target_compile_definitions(ds_ut_slot_store PRIVATE ${BIN_PATH_LIST})

if (BUILD_OBSERVABILITY)
    set(TEST_ENVIRONMENT "LD_LIBRARY_PATH=${_WORKER_BIN_DIR}:${OpenSSL_LIB_PATH}:${gRPC_LIB_PATH}:${LOGS_ROOT}/lib:${ICONV_ROOT}/lib:$ENV{LD_LIBRARY_PATH}"
        CACHE STRING "")
else()
    set(TEST_ENVIRONMENT "LD_LIBRARY_PATH=${_WORKER_BIN_DIR}:${OpenSSL_LIB_PATH}:${gRPC_LIB_PATH}:${ICONV_ROOT}/lib:$ENV{LD_LIBRARY_PATH}"
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


add_ds_ut_executable(log_perf_ut
        "${CMAKE_CURRENT_SOURCE_DIR}/common/log/log_performance_test.cpp"
        $<TARGET_OBJECTS:_ds_ut_main_obj>
        $<TARGET_OBJECTS:_ds_ut_common_obj>)
target_link_libraries(log_perf_ut PRIVATE
        ${DS_UT_HEAVY_LINK_START}
        ${DS_UT_DEPEND_LIBS}
        ${DS_UT_COMMON_UTIL_EXTRA_LIBS}
        ${DS_UT_HEAVY_LINK_END})

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
add_datasystem_test(flags_ut)

add_executable(util_flags_ut
        "${CMAKE_CURRENT_SOURCE_DIR}/common/util/flags_test.cpp"
        $<TARGET_OBJECTS:_ds_ut_main_obj>
        $<TARGET_OBJECTS:_ds_ut_common_obj>)
target_link_libraries(util_flags_ut PRIVATE
        ${DS_UT_HEAVY_LINK_START}
        ${DS_UT_DEPEND_LIBS}
        ${DS_UT_COMMON_UTIL_EXTRA_LIBS}
        common_util_gflag
        ${DS_UT_HEAVY_LINK_END})
add_datasystem_test(util_flags_ut)

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
add_datasystem_test(operation_logger_ut)

add_executable(dynamic_config_updater_ut
        "${CMAKE_CURRENT_SOURCE_DIR}/common/util/dynamic_config_updater_test.cpp"
        $<TARGET_OBJECTS:_ds_ut_main_obj>)
target_link_libraries(dynamic_config_updater_ut
        common_util_gflag
        common_log
        ds_flags
        common_util
        ${DS_UT_COMMON_UTIL_EXTRA_LIBS}
        nlohmann_json::nlohmann_json
        ${SECUREC_LIBRARY}
        GTest::gtest
        GTest::gmock)
add_datasystem_test(dynamic_config_updater_ut)

add_executable(flags_monitor_gate_ut
        "${CMAKE_CURRENT_SOURCE_DIR}/common/util/flags_monitor_gate_test.cpp"
        "${CMAKE_SOURCE_DIR}/src/datasystem/client/client_flags_monitor.cpp"
        $<TARGET_OBJECTS:_ds_ut_main_obj>)
target_link_libraries(flags_monitor_gate_ut
        common_util_gflag
        common_log
        ds_flags
        common_util
        ${DS_UT_COMMON_UTIL_EXTRA_LIBS}
        ${SECUREC_LIBRARY}
        GTest::gtest
        GTest::gmock)
add_datasystem_test(flags_monitor_gate_ut)

add_executable(flags_config_monitor_ut
        "${CMAKE_CURRENT_SOURCE_DIR}/common/util/flags_config_monitor_test.cpp"
        $<TARGET_OBJECTS:_ds_ut_main_obj>
        $<TARGET_OBJECTS:_ds_ut_common_obj>)
target_link_libraries(flags_config_monitor_ut PRIVATE
        ${DS_UT_HEAVY_LINK_START}
        ${DS_UT_DEPEND_LIBS}
        ${DS_UT_COMMON_UTIL_EXTRA_LIBS}
        common_util_gflag
        ${DS_UT_HEAVY_LINK_END})
add_datasystem_test(flags_config_monitor_ut)

if (BUILD_HETERO AND BUILD_HETERO_NPU)
    add_subdirectory(device/ascend)
endif()

set(EMBEDDED_CLIENT_WORKER_API_UT_DEPEND_LIBS
        GTest::gtest
        GTest::gmock
        embedded_client_worker_api
        common_util)
