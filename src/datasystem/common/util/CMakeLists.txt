add_subdirectory(queue)

set(UTIL_SRCS
        uuid_generator.cpp
        ../flags/kv_client_config.cpp
        uri.cpp
        file_util.cpp
        net_util.cpp
        random_data.cpp
        status.cpp
        thread_pool.cpp
        wait_post.cpp
        fd_pass.cpp
        fd_manager.cpp
        format.cpp
        memory.cpp
        version.cpp
        compatibility_manager.cpp
        numa_util.cpp
        deadlock_util.cpp
        hash_algorithm.cpp
        ../rpc/zmq/zmq_message.cpp
        thread_local.cpp
        sensitive_value.cpp
        ssl_authorization.cpp
        safe_shm_lock.cpp
        locks.cpp
        rpc_diagnostic.cpp
        sched_runtime_linux.cpp
        string_util.cpp)

set(UTIL_LIBS
        ${ZLIB_LIBRARIES}
        ${SECUREC_LIBRARY}
        ${OPENSSL_LIBRARIES}
        ${ZeroMQ_LIBRARY}
        re2::re2
        common_log
        common_inject
        common_util_queue
        dynamic_flag_config
        common_rdma_util)

set_source_files_properties(version.cpp PROPERTIES
        COMPILE_DEFINITIONS "GIT_HASH=\"${GIT_HASH}\"")

add_library(common_util STATIC ${UTIL_SRCS})
target_link_libraries(common_util PRIVATE ${UTIL_LIBS})

# request_context.cpp depends only on bthread (bthread_key_create/getspecific/setspecific)
# and on header types from common_log (TimeCost, Trace). No link-time dependency on
# common_log — request_context.cpp does not call any function defined in common_log.
# Kept as a separate target so common_util stays brpc-free.
add_library(common_request_context STATIC request_context.cpp)
target_link_libraries(common_request_context PUBLIC ${BRPC_LIBRARIES} common_util)
