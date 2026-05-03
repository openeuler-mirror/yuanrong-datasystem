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
        worker_client_manager
        datasystem_worker_static
        )

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
list(FILTER DS_TEST_UT_SRCS EXCLUDE REGEX .*/common/l2cache/slot_store_test\\.cpp$)
list(FILTER DS_TEST_UT_SRCS EXCLUDE REGEX .*/test_main\\.cpp$)
list(FILTER DS_TEST_UT_SRCS EXCLUDE REGEX .*/common\\.cpp$)
list(FILTER DS_TEST_UT_SRCS EXCLUDE REGEX .*/stream_cache/.*)
list(FILTER DS_TEST_UT_SRCS EXCLUDE REGEX .*/object_cache/.*)

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

target_link_libraries(ds_ut PRIVATE ${DS_UT_DEPEND_LIBS})
target_link_libraries(ds_ut_stream PRIVATE ${DS_UT_DEPEND_LIBS} worker_stream_cache datasystem_worker_static)
target_link_libraries(ds_ut_object PRIVATE ${DS_UT_DEPEND_LIBS})
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

add_executable(flags_ut "${CMAKE_CURRENT_SOURCE_DIR}/common/flags/flags_test.cpp")
target_link_libraries(flags_ut ds_flags GTest::gtest GTest::gmock)
add_datasystem_test(flags_ut)

set(EMBEDDED_CLIENT_WORKER_API_UT_DEPEND_LIBS
        GTest::gtest
        GTest::gmock
        embedded_client_worker_api
        common_util)
