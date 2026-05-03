# virtual project for common include and library file path.
project(st)

set(PROJECT_DIR "${PROJECT_SOURCE_DIR}/../..")

set(DS_ST_DEPEND_LIBS
        cluster
        common_rocksdb
        common_rpc_zmq_demo
        common_rpc_zmq
        common_log
        common_util
        common_iam
        common_shared_memory
        common_event_loop
        common_encrypt
        common_ak_sk
        common_immutable_string
        string_ref
        worker_hash_ring
        worker_client_manager
        cluster_manager
        ds_master
        datasystem_worker_static
        master_object_cache_store
        master_object_cache
        worker_object_cache
        master_stream_cache
        worker_stream_cache
        ds_router_client_static
        master_stream_cache_store
        master_stream_protos
        httpclient
        common_persistence_api
        common_slot_client)


#include dirs
include_directories(${CMAKE_CURRENT_SOURCE_DIR})
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/client/object_cache)
include_directories(${PROJECT_DIR}/src)

add_subdirectory(cluster)

# fetch st test files and remove the files we don't care about.
file(GLOB_RECURSE DS_TEST_ST_SRCS CONFIGURE_DEPENDS
        "${CMAKE_CURRENT_SOURCE_DIR}/*.cpp")

file(GLOB_RECURSE DS_ST_STREAM_CACHE_SRCS CONFIGURE_DEPENDS
        "${CMAKE_CURRENT_SOURCE_DIR}/**/stream_cache/*.cpp")

file(GLOB_RECURSE DS_ST_OBJECT_CACHE_SRCS CONFIGURE_DEPENDS
        "${CMAKE_CURRENT_SOURCE_DIR}/**/object_cache/*.cpp")

file(GLOB_RECURSE DS_ST_KV_CACHE_SRCS CONFIGURE_DEPENDS
        "${CMAKE_CURRENT_SOURCE_DIR}/**/kv_cache/*.cpp")

file(GLOB_RECURSE DS_ST_EMBEDDED_CLIENT_SRCS CONFIGURE_DEPENDS
        "${CMAKE_CURRENT_SOURCE_DIR}/embedded_client/*.cpp")

list(FILTER DS_TEST_ST_SRCS EXCLUDE REGEX .*curvekey_generator.cpp)
list(FILTER DS_TEST_ST_SRCS EXCLUDE REGEX .*encrypt_util.cpp)
list(FILTER DS_TEST_ST_SRCS EXCLUDE REGEX .*hashring_parser.cpp)
list(FILTER DS_TEST_ST_SRCS EXCLUDE REGEX .*/device/.*)
list(FILTER DS_TEST_ST_SRCS EXCLUDE REGEX .*/cluster/.*)
list(FILTER DS_TEST_ST_SRCS EXCLUDE REGEX .*/stream_cache/.*)
list(FILTER DS_TEST_ST_SRCS EXCLUDE REGEX .*/embedded_client/.*)
list(FILTER DS_TEST_ST_SRCS EXCLUDE REGEX .*/object_cache/.*)
list(FILTER DS_TEST_ST_SRCS EXCLUDE REGEX .*/kv_cache/.*)
list(FILTER DS_TEST_ST_SRCS EXCLUDE REGEX .*/test_main\\.cpp$)
list(FILTER DS_TEST_ST_SRCS EXCLUDE REGEX .*/st_oc_service_impl\\.cpp$)
list(FILTER DS_TEST_ST_SRCS EXCLUDE REGEX .*/client/object_cache/oc_client_common\\.cpp$)
list(FILTER DS_ST_OBJECT_CACHE_SRCS EXCLUDE REGEX .*/client/object_cache/oc_client_common\\.cpp$)
list(FILTER DS_ST_STREAM_CACHE_SRCS EXCLUDE REGEX .*/common/stream_cache/element_generator\\.cpp$)

if (NOT ENABLE_PERF)
    list(FILTER DS_TEST_ST_SRCS EXCLUDE REGEX .*/client/perf_client/.*)
endif()

set(ST_COMMON_SRCS
        client/object_cache/oc_client_common.cpp
        st_oc_service_impl.cpp
        common/stream_cache/element_generator.cpp)

add_library(_ds_st_main_obj OBJECT test_main.cpp)
add_library(_ds_st_group_common_obj OBJECT ${ST_COMMON_SRCS})
add_library(_ds_st_stream_cache_obj OBJECT ${DS_ST_STREAM_CACHE_SRCS})
add_library(_ds_st_object_cache_obj OBJECT ${DS_ST_OBJECT_CACHE_SRCS})
add_library(_ds_st_kv_cache_obj OBJECT ${DS_ST_KV_CACHE_SRCS})
add_library(_ds_st_other_obj OBJECT ${DS_TEST_ST_SRCS})

target_link_libraries(_ds_st_main_obj GTest::gtest GTest::gmock)
target_link_libraries(_ds_st_group_common_obj nlohmann_json::nlohmann_json generic_service_protos datasystem_worker_static datasystem)
target_link_libraries(_ds_st_stream_cache_obj nlohmann_json::nlohmann_json generic_service_protos datasystem_worker_static datasystem)
target_link_libraries(_ds_st_object_cache_obj nlohmann_json::nlohmann_json generic_service_protos datasystem_worker_static datasystem)
target_link_libraries(_ds_st_kv_cache_obj nlohmann_json::nlohmann_json generic_service_protos datasystem_worker_static datasystem)
target_link_libraries(_ds_st_other_obj nlohmann_json::nlohmann_json generic_service_protos datasystem_worker_static datasystem)

function(add_ds_st_grouped_executable TARGET_NAME OBJECT_TARGET)
    add_executable(${TARGET_NAME}
            $<TARGET_OBJECTS:${OBJECT_TARGET}>
            $<TARGET_OBJECTS:_ds_st_main_obj>
            $<TARGET_OBJECTS:_ds_st_group_common_obj>)
endfunction()

add_ds_st_grouped_executable(ds_st_stream_cache _ds_st_stream_cache_obj)
add_ds_st_grouped_executable(ds_st_object_cache _ds_st_object_cache_obj)
add_ds_st_grouped_executable(ds_st_kv_cache _ds_st_kv_cache_obj)
add_ds_st_grouped_executable(ds_st _ds_st_other_obj)
add_executable(ds_st_embedded_client ${DS_ST_EMBEDDED_CLIENT_SRCS} $<TARGET_OBJECTS:_ds_st_main_obj>)

target_link_libraries(ds_st PRIVATE ${DS_ST_DEPEND_LIBS})
target_link_libraries(ds_st_stream_cache PRIVATE ${DS_ST_DEPEND_LIBS})
target_link_libraries(ds_st_object_cache PRIVATE ${DS_ST_DEPEND_LIBS} binmock)
target_link_libraries(ds_st_kv_cache PRIVATE ${DS_ST_DEPEND_LIBS})
target_link_libraries(ds_st_embedded_client  GTest::gtest GTest::gmock common_etcd  nlohmann_json::nlohmann_json generic_service_protos datasystem cluster)
target_compile_definitions(ds_st_embedded_client PRIVATE HASH_PHRASE_PATH="${CMAKE_CURRENT_BINARY_DIR}/hashring_parser")

find_package(absl PATHS absl_PKG_PATH)

add_custom_command(
        TARGET ds_st POST_BUILD
        COMMAND ${CMAKE_COMMAND} -E copy_directory
        ${CMAKE_CURRENT_SOURCE_DIR}/data
        ${CMAKE_BINARY_DIR}/tests/st/data)

add_executable(curve_keygen common/rpc/curvekey_generator.cpp)
target_link_libraries(curve_keygen PRIVATE common_util common_rpc_zmq)

add_dependencies(ds_st curve_keygen)

add_executable(hashring_parser ./hashring_parser.cpp)
target_link_libraries(hashring_parser PRIVATE hash_ring_protos_client)
install(TARGETS hashring_parser DESTINATION tools)

add_custom_command(
        TARGET ds_st POST_BUILD
        COMMAND ${CMAKE_COMMAND} -E make_directory
        ${CMAKE_BINARY_DIR}/tests/st/data/encrypt_test/key_component_dir
        COMMAND ${CMAKE_COMMAND} -E make_directory
        ${CMAKE_BINARY_DIR}/tests/st/data/zmq_curve_test/input_dir
        COMMAND ${CMAKE_COMMAND} -E make_directory
        ${CMAKE_BINARY_DIR}/tests/st/data/zmq_curve_test/key_component_dir
        COMMAND ${CMAKE_COMMAND} -E make_directory
        ${CMAKE_BINARY_DIR}/tests/st/data/zmq_curve_test/encrypt_dir
        COMMAND ${CMAKE_COMMAND} -E make_directory
        ${CMAKE_INSTALL_PREFIX}/tools
        COMMAND ${CMAKE_COMMAND} -E copy
        ${CMAKE_BINARY_DIR}/tests/st/curve_keygen
        ${CMAKE_INSTALL_PREFIX}/tools/curve_keygen)
set(CURVE_COMMAND ${CMAKE_COMMAND} -E env LD_LIBRARY_PATH=${gflags_LIB_PATH}:$ENV{LD_LIBRARY_PATH}
        ${CMAKE_BINARY_DIR}/tests/st/curve_keygen)
add_custom_command(
        TARGET ds_st POST_BUILD
        COMMAND ${CURVE_COMMAND} -n worker
        COMMAND ${CURVE_COMMAND} -n client
        COMMAND ${CURVE_COMMAND} -n invalid_client
        COMMAND ${CURVE_COMMAND} -n invalid_service_client
        COMMAND ${CMAKE_COMMAND}  -E copy_directory
        ${CMAKE_BINARY_DIR}/tests/st/data/zmq_curve_test/input_dir/
        ${CMAKE_BINARY_DIR}/tests/st/data/zmq_curve_test/encrypt_dir/
        WORKING_DIRECTORY ${CMAKE_BINARY_DIR}/tests/st/data/zmq_curve_test/input_dir)

add_custom_command(
        TARGET ds_st POST_BUILD
        COMMAND ${CMAKE_COMMAND} -E make_directory
        ${CMAKE_BINARY_DIR}/tests/st/data/zmq_curve_test/encrypt_dir/worker_authorized_clients
        COMMAND ${CMAKE_COMMAND} -E copy
        ${CMAKE_BINARY_DIR}/tests/st/data/zmq_curve_test/encrypt_dir/client.key
        ${CMAKE_BINARY_DIR}/tests/st/data/zmq_curve_test/encrypt_dir/worker_authorized_clients/client.key
        COMMAND ${CMAKE_COMMAND} -E copy
        ${CMAKE_BINARY_DIR}/tests/st/data/zmq_curve_test/encrypt_dir/invalid_service_client.key
        ${CMAKE_BINARY_DIR}/tests/st/data/zmq_curve_test/encrypt_dir/worker_authorized_clients/invalid_service_client.key
        COMMAND ${CMAKE_COMMAND} -E copy
        ${CMAKE_BINARY_DIR}/tests/st/data/zmq_service.mapping
        ${CMAKE_BINARY_DIR}/tests/st/data/zmq_curve_test/encrypt_dir/worker_authorized_clients/service.mapping)

add_custom_command(
        TARGET ds_st POST_BUILD
        COMMAND ${CMAKE_COMMAND} -E make_directory
        ${CMAKE_BINARY_DIR}/tests/st/data/client_zmq_curve_test/input_dir
        COMMAND ${CMAKE_COMMAND} -E make_directory
        ${CMAKE_BINARY_DIR}/tests/st/data/client_zmq_curve_test/key_component_dir
        COMMAND ${CMAKE_COMMAND} -E make_directory
        ${CMAKE_BINARY_DIR}/tests/st/data/client_zmq_curve_test/encrypt_dir)

add_custom_command(
        TARGET ds_st POST_BUILD
        COMMAND ${CURVE_COMMAND} -n worker
        COMMAND ${CURVE_COMMAND} -n master
        COMMAND ${CURVE_COMMAND} -n client
        COMMAND ${CURVE_COMMAND} -n admin
        COMMAND ${CURVE_COMMAND} -n invalid_client
        WORKING_DIRECTORY ${CMAKE_BINARY_DIR}/tests/st/data/client_zmq_curve_test/input_dir)

add_custom_command(
        TARGET ds_st POST_BUILD
        COMMAND ${CMAKE_COMMAND}  -E copy_directory
        ${CMAKE_BINARY_DIR}/tests/st/data/client_zmq_curve_test/input_dir/
        ${CMAKE_BINARY_DIR}/tests/st/data/client_zmq_curve_test/encrypt_dir/
        COMMAND ${CMAKE_COMMAND} -E make_directory
        ${CMAKE_BINARY_DIR}/tests/st/data/client_zmq_curve_test/encrypt_dir/worker_authorized_clients
        COMMAND ${CMAKE_COMMAND} -E make_directory
        ${CMAKE_BINARY_DIR}/tests/st/data/client_zmq_curve_test/encrypt_dir/master_authorized_clients

        COMMAND ${CMAKE_COMMAND} -E copy
        ${CMAKE_BINARY_DIR}/tests/st/data/client_zmq_curve_test/encrypt_dir/client.key
        ${CMAKE_BINARY_DIR}/tests/st/data/client_zmq_curve_test/encrypt_dir/worker_authorized_clients/client.key
        COMMAND ${CMAKE_COMMAND} -E copy
        ${CMAKE_BINARY_DIR}/tests/st/data/client_zmq_curve_test/encrypt_dir/client.key
        ${CMAKE_BINARY_DIR}/tests/st/data/client_zmq_curve_test/encrypt_dir/master_authorized_clients/client.key
        COMMAND ${CMAKE_COMMAND} -E copy
        ${CMAKE_BINARY_DIR}/tests/st/data/client_zmq_curve_test/encrypt_dir/admin.key
        ${CMAKE_BINARY_DIR}/tests/st/data/client_zmq_curve_test/encrypt_dir/worker_authorized_clients/admin.key
        COMMAND ${CMAKE_COMMAND} -E copy
        ${CMAKE_BINARY_DIR}/tests/st/data/client_zmq_curve_test/encrypt_dir/admin.key
        ${CMAKE_BINARY_DIR}/tests/st/data/client_zmq_curve_test/encrypt_dir/master_authorized_clients/admin.key
        COMMAND ${CMAKE_COMMAND} -E copy
        ${CMAKE_BINARY_DIR}/tests/st/data/client_zmq_curve_test/encrypt_dir/master.key
        ${CMAKE_BINARY_DIR}/tests/st/data/client_zmq_curve_test/encrypt_dir/worker_authorized_clients/master.key
        COMMAND ${CMAKE_COMMAND} -E copy
        ${CMAKE_BINARY_DIR}/tests/st/data/client_zmq_curve_test/encrypt_dir/worker.key
        ${CMAKE_BINARY_DIR}/tests/st/data/client_zmq_curve_test/encrypt_dir/worker_authorized_clients/worker.key
        COMMAND ${CMAKE_COMMAND} -E copy
        ${CMAKE_BINARY_DIR}/tests/st/data/client_zmq_curve_test/encrypt_dir/worker.key
        ${CMAKE_BINARY_DIR}/tests/st/data/client_zmq_curve_test/encrypt_dir/master_authorized_clients/worker.key
        COMMAND ${CMAKE_COMMAND} -E copy
        ${CMAKE_BINARY_DIR}/tests/st/data/master_service.mapping
        ${CMAKE_BINARY_DIR}/tests/st/data/client_zmq_curve_test/encrypt_dir/master_authorized_clients/service.mapping
        WORKING_DIRECTORY ${CMAKE_BINARY_DIR}/tests/st/data/client_zmq_curve_test/input_dir)

add_custom_command(
        TARGET ds_st POST_BUILD
        COMMAND ${CMAKE_COMMAND} -E make_directory
        ${CMAKE_BINARY_DIR}/tests/st/data/client_plaintext_zmq_curve_test
        COMMAND ${CMAKE_COMMAND} -E make_directory
        ${CMAKE_BINARY_DIR}/tests/st/data/client_plaintext_zmq_curve_test/worker_authorized_clients)

add_custom_command(
        TARGET ds_st POST_BUILD
        COMMAND ${CURVE_COMMAND} -n worker
        COMMAND ${CURVE_COMMAND} -n client
        COMMAND ${CURVE_COMMAND} -n invalid_client
        WORKING_DIRECTORY ${CMAKE_BINARY_DIR}/tests/st/data/client_plaintext_zmq_curve_test)

add_custom_command(
        TARGET ds_st POST_BUILD
        COMMAND ${CMAKE_COMMAND} -E copy
        ${CMAKE_BINARY_DIR}/tests/st/data/client_plaintext_zmq_curve_test/client.key
        ${CMAKE_BINARY_DIR}/tests/st/data/client_plaintext_zmq_curve_test/worker_authorized_clients/client.key
        COMMAND ${CMAKE_COMMAND} -E copy
        ${CMAKE_BINARY_DIR}/tests/st/data/client_plaintext_zmq_curve_test/worker.key
        ${CMAKE_BINARY_DIR}/tests/st/data/client_plaintext_zmq_curve_test/worker_authorized_clients/worker.key)

if (BUILD_WITH_RDMA)
        string(REPLACE "LD_LIBRARY_PATH=" "LD_LIBRARY_PATH=${UCX_LIB_PATH}:" TEST_ENVIRONMENT "${TEST_ENVIRONMENT}")
endif()

if (BUILD_WITH_URMA)
        get_filename_component(URMA_LIB_PATH ${URMA_LIBRARY} DIRECTORY)
        string(REPLACE "LD_LIBRARY_PATH=" "LD_LIBRARY_PATH=${URMA_LIB_PATH}:" TEST_ENVIRONMENT "${TEST_ENVIRONMENT}")
endif()

add_datasystem_test(ds_st TEST_ENVIRONMENTS ${TEST_ENVIRONMENT})
add_datasystem_test(ds_st_stream_cache TEST_ENVIRONMENTS ${TEST_ENVIRONMENT})
add_datasystem_test(ds_st_object_cache TEST_ENVIRONMENTS ${TEST_ENVIRONMENT})
add_datasystem_test(ds_st_kv_cache TEST_ENVIRONMENTS ${TEST_ENVIRONMENT})
add_datasystem_test(ds_st_embedded_client TEST_ENVIRONMENTS ${TEST_ENVIRONMENT})

file(GLOB_RECURSE DS_DEVICE_TEST_SRCS CONFIGURE_DEPENDS
        "${CMAKE_CURRENT_SOURCE_DIR}/device/*.cpp"
        )
if (NOT BUILD_HETERO_GPU)
    list(FILTER DS_DEVICE_TEST_SRCS EXCLUDE REGEX ".*/hetero_d2h_test_gpu\\.cpp$")
endif()
add_executable(ds_device_llt ${DS_DEVICE_TEST_SRCS} $<TARGET_OBJECTS:_ds_st_main_obj>)

if (BUILD_HETERO)
    set(DS_DEVICE_LLT_DEPEND_LIB
            cluster
            ${ASCEND_LIBRARY}
            ${HCCL_LIBRARY}
            binmock)
    # copy acl plugin shared library to the same directory of ds_st.
    if (BUILD_HETERO_NPU)
        add_custom_command(
            TARGET ds_device_llt POST_BUILD
            COMMAND ${CMAKE_COMMAND} -E copy
            ${CMAKE_BINARY_DIR}/src/datasystem/common/device/ascend/plugin/libacl_plugin.so
            ${CMAKE_BINARY_DIR}/tests/st
        )
    endif()
    if (BUILD_HETERO_GPU)
        add_custom_command(
            TARGET ds_device_llt POST_BUILD
            COMMAND ${CMAKE_COMMAND} -E copy
            ${CMAKE_BINARY_DIR}/src/datasystem/common/device/nvidia/plugin/libcuda_plugin.so
            ${CMAKE_BINARY_DIR}/tests/st
        )
    endif()
else()
    # Keep device LLT available in non-hetero builds and rely on the mock device manager.
    set(DS_DEVICE_LLT_DEPEND_LIB
            cluster
            binmock)
endif()

target_link_libraries(ds_device_llt PRIVATE ${DS_DEVICE_LLT_DEPEND_LIB})
target_compile_definitions(ds_device_llt PRIVATE ${BIN_PATH_LIST})
add_datasystem_test(ds_device_llt TEST_ENVIRONMENTS ${TEST_ENVIRONMENT})
