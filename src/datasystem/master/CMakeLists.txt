add_subdirectory(object_cache)

# Set source files
set(MASTER_SRCS
        master_service_impl.cpp
        node_descriptor.cpp
        worker_manager.cpp
        replica_manager.cpp
        metadata_redirect_helper.cpp
        replica_rpc_channel_impl.cpp
        replication_service_impl.cpp)

set(MASTER_DEPEND_LIBS
        common_ak_sk
        common_encrypt
        common_inject
        common_log
        common_perf
        common_rocksdb
        common_rpc_zmq
        common_util
        ds_server
        master_heartbeat_protos
        master_object_cache
        rpc_stub_cache_mgr
        worker_client_manager
        cluster_manager
        )

# add master lib
add_library(ds_master STATIC ${MASTER_SRCS})
target_link_libraries(ds_master PRIVATE ${MASTER_DEPEND_LIBS})
add_dependencies(ds_master master_heartbeat_protos)

# for ut test
if (WITH_TESTS)
    set(_MASTER_BIN_DIR ${CMAKE_CURRENT_BINARY_DIR} CACHE STRING "path of master binary file")
endif ()
