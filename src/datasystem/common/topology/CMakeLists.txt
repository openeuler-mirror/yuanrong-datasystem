set(COMMON_TOPOLOGY_SRCS
        membership/cluster_membership.cpp
        membership/cluster_registry.cpp
        membership/worker_directory.cpp
        repository/topology_key_helper.cpp
        repository/topology_repository_codec.cpp
        repository/topology_repository.cpp
        runtime/topology_change_handler.cpp
        )

add_library(common_topology STATIC ${COMMON_TOPOLOGY_SRCS})
target_include_directories(common_topology PUBLIC ${PROJECT_SOURCE_DIR}/src)
target_link_libraries(common_topology PUBLIC
        common_util
        common_log
        common_etcd
        cluster_store
        hash_ring_protos
        )
add_dependencies(common_topology hash_ring_protos)
