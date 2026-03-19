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
        common_etcd
        common_log
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

# fetch ut test files and remove the files we don't care about.
file(GLOB_RECURSE DS_TEST_UT_SRCS CONFIGURE_DEPENDS "${CMAKE_CURRENT_SOURCE_DIR}/*.cpp")
file(GLOB_RECURSE DS_UT_STREAM_SRCS CONFIGURE_DEPENDS "${CMAKE_CURRENT_SOURCE_DIR}/**/stream_cache/*.cpp")
file(GLOB_RECURSE DS_UT_OBJECT_SRCS CONFIGURE_DEPENDS "${CMAKE_CURRENT_SOURCE_DIR}/**/object_cache/*.cpp")

set(UT_COMMON_SRCS
        test_main.cpp
        common.cpp)

list(FILTER DS_TEST_UT_SRCS EXCLUDE REGEX .*/device/.*)
list(FILTER DS_TEST_UT_SRCS EXCLUDE REGEX .*/binmock/.*)
list(FILTER DS_TEST_UT_SRCS EXCLUDE REGEX .*/flags/.*)
list(FILTER DS_TEST_UT_SRCS EXCLUDE REGEX .*/stream_cache/.*)
list(FILTER DS_TEST_UT_SRCS EXCLUDE REGEX .*/object_cache/.*)

if (NOT BUILD_WITH_RDMA)
    list(FILTER DS_TEST_UT_SRCS EXCLUDE REGEX ".*/rdma/.*\\.cpp$")
endif()

list(APPEND DS_UT_STREAM_SRCS)

add_executable(ds_ut ${DS_TEST_UT_SRCS})
add_executable(ds_ut_stream ${DS_UT_STREAM_SRCS} ${UT_COMMON_SRCS})
add_executable(ds_ut_object ${DS_UT_OBJECT_SRCS} ${UT_COMMON_SRCS})

target_link_libraries(ds_ut PRIVATE ${DS_UT_DEPEND_LIBS})
target_link_libraries(ds_ut_stream PRIVATE ${DS_UT_DEPEND_LIBS} worker_stream_cache datasystem_worker_static)
target_link_libraries(ds_ut_object PRIVATE ${DS_UT_DEPEND_LIBS})

set(BIN_PATH_LIST LLT_BIN_PATH="${CMAKE_CURRENT_BINARY_DIR}")

target_compile_definitions(ds_ut PRIVATE ${BIN_PATH_LIST})
target_compile_definitions(ds_ut_stream PRIVATE ${BIN_PATH_LIST})
target_compile_definitions(ds_ut_object PRIVATE ${BIN_PATH_LIST})

if (BUILD_OBSERVABILITY)
    set(TEST_ENVIRONMENT "LD_LIBRARY_PATH=${_WORKER_BIN_DIR}:${OpenSSL_LIB_PATH}:${gRPC_LIB_PATH}:${OBS_LIB_PATH}:${LOGS_ROOT}/lib:${ICONV_ROOT}/lib:$ENV{LD_LIBRARY_PATH}"
        CACHE STRING "")
else()
    set(TEST_ENVIRONMENT "LD_LIBRARY_PATH=${_WORKER_BIN_DIR}:${OpenSSL_LIB_PATH}:${gRPC_LIB_PATH}:${OBS_LIB_PATH}:${ICONV_ROOT}/lib:$ENV{LD_LIBRARY_PATH}"
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

add_executable(flags_ut "${CMAKE_CURRENT_SOURCE_DIR}/common/flags/flags_test.cpp")
target_link_libraries(flags_ut ds_flags GTest::gtest GTest::gmock)
add_datasystem_test(flags_ut)

set(EMBEDDED_CLIENT_WORKER_API_UT_DEPEND_LIBS
        GTest::gtest
        GTest::gmock
        embedded_client_worker_api
        common_util)
