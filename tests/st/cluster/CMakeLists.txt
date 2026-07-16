add_library(test_port_allocator STATIC test_port_allocator.cpp)
target_include_directories(test_port_allocator PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/..)
target_link_libraries(test_port_allocator PUBLIC common_util)

add_library(cluster STATIC external_cluster.cpp subprocess.cpp common.cpp)
set(BIN_PATH_LIST
        MASTER_BIN_PATH="${_MASTER_BIN_DIR}/master"
        WORKER_BIN_PATH="${_WORKER_BIN_DIR}/datasystem_worker"
        COORDINATOR_BIN_PATH="${CMAKE_BINARY_DIR}/src/datasystem/coordinator/datasystem_coordinator"
        LLT_BIN_PATH="${CMAKE_CURRENT_BINARY_DIR}/..")
target_compile_definitions(cluster PUBLIC ${BIN_PATH_LIST})
add_dependencies(cluster datasystem_coordinator)
target_link_libraries(cluster
    GTest::gtest
    GTest::gmock
    datasystem_static
    generic_service_protos_client
    common_etcd
    common_test
    cluster_topology_keyspace
    test_port_allocator
)
