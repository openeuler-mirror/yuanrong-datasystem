set(MASTER_METADATA_REDIRECT_SRCS
        metadata_redirect_helper.cpp)

set(MASTER_METADATA_REDIRECT_DEPEND_LIBS
        ${TBB_LIBRARY}
        common_immutable_string
        common_log
        common_util
        cluster_topology
        )

add_library(master_metadata_redirect STATIC ${MASTER_METADATA_REDIRECT_SRCS})
target_link_libraries(master_metadata_redirect PRIVATE ${MASTER_METADATA_REDIRECT_DEPEND_LIBS})

add_subdirectory(object_cache)
add_subdirectory(stream_cache)

# Set source files
set(MASTER_SRCS
        master_service_impl.cpp
        node_descriptor.cpp
        worker_manager.cpp
        metadata_manager_holder.cpp
        memory_rebalance_scheduler.cpp
        resource_manager.cpp)

set(MASTER_DEPEND_LIBS
        common_ak_sk
        common_encrypt
        common_log
        common_perf
        common_rocksdb
        common_rpc_zmq
        common_util
        string_ref
        ds_server
        master_heartbeat_protos
        master_object_cache
        master_stream_cache
        master_metadata_redirect
        rpc_stub_cache_mgr
        worker_client_manager
        cluster_topology
        )

# add master lib
add_library(ds_master STATIC ${MASTER_SRCS})
target_link_libraries(ds_master PRIVATE ${MASTER_DEPEND_LIBS})
add_dependencies(ds_master master_heartbeat_protos)

# for ut test
if (WITH_TESTS)
    set(_MASTER_BIN_DIR ${CMAKE_CURRENT_BINARY_DIR} CACHE STRING "path of master binary file")
endif ()
