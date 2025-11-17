add_subdirectory(store)
set(MASTER_SC_SRCS
        master_sc_service_impl.cpp
        master_worker_sc_api.cpp
        rpc_session_manager.cpp
        stream_metadata.cpp
        topology_manager.cpp
        sc_metadata_manager.cpp
        sc_migrate_metadata_manager.cpp
        sc_notify_worker_manager.cpp
        )

set(MASTER_SC_DEPEND_LIBS
        common_log
        common_util
        common_rpc_zmq
        common_event_loop
        master_stream_cache_store
        master_stream_protos
        cluster_manager
        )

add_library(master_stream_cache STATIC ${MASTER_SC_SRCS})
target_link_libraries(master_stream_cache PRIVATE ${MASTER_SC_DEPEND_LIBS})
add_dependencies(master_stream_cache
        master_stream_protos)