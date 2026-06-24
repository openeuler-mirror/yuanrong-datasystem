set(COMMON_CLUSTER_STORE_SRCS
        cluster_store.cpp
        )

add_library(cluster_store STATIC ${COMMON_CLUSTER_STORE_SRCS})
target_include_directories(cluster_store PUBLIC ${PROJECT_SOURCE_DIR}/src)
target_link_libraries(cluster_store PUBLIC
        common_etcd
        common_util
        )
