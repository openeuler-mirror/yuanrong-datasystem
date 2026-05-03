add_library(cluster STATIC external_cluster.cpp subprocess.cpp common.cpp test_port_allocator.cpp)
set(BIN_PATH_LIST
        MASTER_BIN_PATH="${_MASTER_BIN_DIR}/master"
        WORKER_BIN_PATH="${_WORKER_BIN_DIR}/datasystem_worker"
        LLT_BIN_PATH="${CMAKE_CURRENT_BINARY_DIR}/..")
target_compile_definitions(cluster PUBLIC ${BIN_PATH_LIST})
target_link_libraries(cluster
    GTest::gtest
    GTest::gmock
    datasystem_static
    generic_service_protos_client
    common_etcd
)
