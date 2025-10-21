set(ROCKSDB_SRCS
        rocks_store.cpp
        replica.cpp
        replica_task.cpp)

set(ROCKSDB_DEPEND_LIBS
        RocksDB::rocksdb
        common_log
        common_util)

add_library(common_rocksdb STATIC ${ROCKSDB_SRCS})
target_link_libraries(common_rocksdb PRIVATE ${ROCKSDB_DEPEND_LIBS})