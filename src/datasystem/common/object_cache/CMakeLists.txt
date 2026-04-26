set(COMMON_BUFFER_SRCS
        buffer.cpp
        lock.cpp
        shm_guard.cpp
        urma_fallback_tcp_limiter.cpp
        device_buffer.cpp
        buffer_composer.cpp)

set(COMMON_BUFFER_DEPEND_LIBS
        ${SECUREC_LIBRARY}
        ${CLIENT_LIB}
        common_log
        common_util
        )

set(COMMON_REF_SRCS object_ref_info.cpp)

set(COMMON_REF_DEPEND_LIBS
        ${TBB_LIBRARY}
        common_log
        common_util
        common_metrics
        common_shared_memory
        )

add_library(common_buffer STATIC ${COMMON_BUFFER_SRCS})
target_link_libraries(common_buffer PRIVATE ${COMMON_BUFFER_DEPEND_LIBS})
add_dependencies(common_buffer posix_protos)

add_library(common_ref STATIC ${COMMON_REF_SRCS})
target_link_libraries(common_ref PRIVATE ${COMMON_REF_DEPEND_LIBS})
