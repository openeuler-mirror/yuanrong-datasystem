add_library(cluster_query_projector STATIC cluster_query_projector.cpp)
target_include_directories(cluster_query_projector PUBLIC ${PROJECT_SOURCE_DIR}/src)
target_link_libraries(cluster_query_projector PUBLIC
        cluster_topology
        common_util)

add_library(cluster_query_client STATIC cluster_query_client.cpp)
target_include_directories(cluster_query_client PUBLIC ${PROJECT_SOURCE_DIR}/src)
target_link_libraries(cluster_query_client PUBLIC
        cluster_query_projector
        cluster_membership_codec_client
        cluster_topology_keyspace
        common_ak_sk
        common_coordinator_store
        static_coordinator_discovery
        common_etcd_client
        common_util
        rpc_stub_cache_mgr)
