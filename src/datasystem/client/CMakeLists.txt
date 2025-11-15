list(APPEND CLIENT_SRCS
        datasystem.cpp
        client_worker_common_api.cpp
        listen_worker.cpp
        mmap_table.cpp
        mmap_manager.cpp
        client_flags_monitor.cpp
        context/context.cpp
        object_cache/client_worker_api.cpp
        object_cache/object_client.cpp
        object_cache/object_client_impl.cpp
        object_cache/device/client_device_object_manager.cpp
        object_cache/device/p2p_subscribe.cpp
        object_cache/device/hccl_comm_factory.cpp
        object_cache/device/future.cpp
        object_cache/device/device_memory_unit.cpp
        object_cache/device/hccl_comm_magr.cpp
        object_cache/device/page_attn_utils.cpp
        kv_cache/read_only_buffer.cpp
        kv_cache/kv_client.cpp
        hetero_cache/hetero_client.cpp
        stream_cache/client_worker_api.cpp
        stream_cache/client_base_impl.cpp
        stream_cache/consumer.cpp
        stream_cache/consumer_impl.cpp
        stream_cache/producer.cpp
        stream_cache/producer_impl.cpp
        stream_cache/stream_client.cpp
        stream_cache/stream_client_impl.cpp
        stream_cache/producer_consumer_worker_api.cpp)

list(APPEND CLIENT_DEPEND_LIBS
        ${SECUREC_LIBRARY}
        ${TBB_LIBRARY}
        protobuf::libprotobuf
        ak_sk_signature
        common_buffer
        common_event_loop
        common_inject
        common_log
        common_perf
        common_sc
        common_shm_unit_info
        common_util
        common_immutable_string
        string_ref
        nlohmann_json::nlohmann_json
        posix_protos_client
        share_memory_protos_client
        worker_object_protos_client
        worker_stream_protos_client
        common_acl_device
        common_shared_memory
        common_rdma
        common_parallel)

if (ENABLE_PERF)
    file(GLOB_RECURSE PERF_CLIENT_SRC RELATIVE ${CMAKE_CURRENT_SOURCE_DIR} perf_client/*.cpp )
    list(APPEND CLIENT_SRCS ${PERF_CLIENT_SRC})
    list(APPEND CLIENT_DEPEND_LIBS perf_posix_protos_client)
endif ()

# static library for ut.
add_library(datasystem_static STATIC ${CLIENT_SRCS})
target_link_libraries(datasystem_static PRIVATE "${CLIENT_DEPEND_LIBS}")

# shared library for user.
add_library(datasystem SHARED ${CLIENT_SRCS})
target_compile_options(datasystem PRIVATE "-fvisibility=hidden")
target_link_libraries(datasystem PRIVATE "${CLIENT_DEPEND_LIBS}")

add_custom_target(clean_datasystem_rpath
    COMMAND ${CMAKE_COMMAND} -D "BIN_FILE=$<TARGET_FILE:datasystem>" -P "${CMAKE_SOURCE_DIR}/cmake/scripts/CleanBuildRpath.cmake"
    COMMENT "Cleaning RPATH for datasystem.so"
)

add_dependencies(clean_datasystem_rpath dsbench_cpp)

list(APPEND common_etcd_client
        common_util
        hash_ring_protos_client)

add_dependencies(datasystem_static
        share_memory_protos_client
        posix_protos_client
        worker_object_protos_client
        master_object_protos_client
        worker_stream_protos_client)

add_dependencies(datasystem
        share_memory_protos_client
        posix_protos_client
        worker_object_protos_client
        master_object_protos_client
        worker_stream_protos_client)
