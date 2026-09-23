add_library(cluster_admin_client STATIC cluster_admin_client.cpp)
target_include_directories(cluster_admin_client PUBLIC ${PROJECT_SOURCE_DIR}/src)
target_link_libraries(cluster_admin_client PUBLIC
        cluster_topology
        cluster_topology_keyspace
        common_coordinator_store
        static_coordinator_discovery
        common_etcd_client
        common_util
        rpc_stub_cache_mgr)
