set(QUEUE_SRCS
        shm_circular_queue.cpp)

set(QUEUE_LIBS
        ${ZLIB_LIBRARIES}
        ${SECUREC_LIBRARY}
        common_log)

add_library(common_util_queue STATIC ${QUEUE_SRCS})
target_link_libraries(common_util_queue PRIVATE ${QUEUE_LIBS})