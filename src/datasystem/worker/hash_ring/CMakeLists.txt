
file(GLOB_RECURSE HASH_RING_SRCS RELATIVE ${CMAKE_CURRENT_SOURCE_DIR} "*.cpp")

set(WORKER_HASH_RING_DEPEND_LIBS
        common_util
        common_log
        common_etcd
        common_rocksdb
        posix_protos
        worker_health_check
        )

add_library(worker_hash_ring STATIC ${HASH_RING_SRCS})
target_link_libraries(worker_hash_ring PRIVATE ${WORKER_HASH_RING_DEPEND_LIBS})
