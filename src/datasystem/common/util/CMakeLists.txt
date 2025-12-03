add_subdirectory(gflag)
add_subdirectory(queue)

set(UTIL_SRCS
        uuid_generator.cpp
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
        deadlock_util.cpp
        hash_algorithm.cpp
        thread_local.cpp
        sensitive_value.cpp
        ssl_authorization.cpp
        meta_route_tool.cpp
        safe_shm_lock.cpp
        locks.cpp
        string_util.cpp)

set(UTIL_LIBS
        ${ZLIB_LIBRARIES}
        ${SECUREC_LIBRARY}
        ${OPENSSL_LIBRARIES}
        re2::re2
        common_log
        common_inject
        common_util_queue
        common_util_gflag
        common_rdma_util)

add_library(common_util STATIC ${UTIL_SRCS})
target_link_libraries(common_util PRIVATE ${UTIL_LIBS})
