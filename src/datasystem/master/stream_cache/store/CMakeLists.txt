set(MASTER_SC_STORE_SRCS
        rocks_stream_meta_store.cpp
        )

set(MASTER_SC_STORE_DEPEND_LIBS
        RocksDB::rocksdb
        common_log
        common_rocksdb
        master_stream_protos
        posix_protos
        worker_stream_protos
        )

add_library(master_stream_cache_store STATIC ${MASTER_SC_STORE_SRCS})
target_link_libraries(master_stream_cache_store PRIVATE ${MASTER_SC_STORE_DEPEND_LIBS})
add_dependencies(master_stream_cache_store
        posix_protos
        worker_stream_protos
        master_stream_protos)