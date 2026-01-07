add_subdirectory(metrics)
add_subdirectory(page_queue)

set(WORKER_SC_SRCS
        stream_manager.cpp
        subscription.cpp
        consumer.cpp
        worker_master_sc_api.cpp
        producer.cpp
        remote_worker_manager.cpp
        client_worker_sc_service_impl.cpp
        master_worker_sc_service_impl.cpp
        worker_worker_sc_service_impl.cpp
        buffer_pool.cpp
        stream_data_pool.cpp
        usage_monitor.cpp
        worker_sc_allocate_memory.cpp
        )

set(WORKER_SC_DEPEND_LIBS
        ${TBB_LIBRARY}
        ${SECUREC_LIBRARY}
        common_log
        common_util
        common_perf
        common_rpc_zmq
        common_event_loop
        common_shared_memory
        common_sc
        sc_metrics
        sc_page_queue
        posix_protos
        worker_stream_protos
        master_stream_protos
        common_encrypt
        common_ak_sk
        httpclient
        worker_health_check
        )

add_library(worker_stream_cache STATIC ${WORKER_SC_SRCS})
target_link_libraries(worker_stream_cache PRIVATE ${WORKER_SC_DEPEND_LIBS})
add_dependencies(worker_stream_cache
        posix_protos
        worker_stream_protos
        master_stream_protos)
