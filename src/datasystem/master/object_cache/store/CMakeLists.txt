set(MASTER_OC_STORE_SRCS
        meta_async_queue.cpp
        object_meta_store.cpp
        ${MASTER_OBJECT_CACHE_PROTO_SRCS})

set(MASTER_OC_STORE_DEPEND_LIBS
        ${SECUREC_LIBRARY}
        common_log
        common_util
        common_rocksdb
        master_object_protos
        cluster_manager)

add_library(master_object_cache_store STATIC ${MASTER_OC_STORE_SRCS})
target_link_libraries(master_object_cache_store PRIVATE ${MASTER_OC_STORE_DEPEND_LIBS})
add_dependencies(master_object_cache_store master_object_protos)