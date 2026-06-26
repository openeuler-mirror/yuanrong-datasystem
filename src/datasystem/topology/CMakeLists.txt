add_subdirectory(coordination_backend)

set(TOPOLOGY_SRCS
        algorithm/algorithm_registry.cpp
        algorithm/hash_algorithm.cpp
        algorithm/placement_policy_engine.cpp
        membership/cluster_membership.cpp
        membership/cluster_registry.cpp
        routing/placement_directory.cpp
        routing/redirect_policy.cpp
        routing/routing_cache.cpp
        routing/routing_view.cpp
        routing/worker_locator.cpp
        routing/placement_facade.cpp
        repository/topology_key_helper.cpp
        repository/topology_repository_codec.cpp
        repository/topology_repository.cpp
        runtime/topology_change_handler.cpp
        )

add_library(topology STATIC ${TOPOLOGY_SRCS})
target_include_directories(topology PUBLIC ${PROJECT_SOURCE_DIR}/src)
target_link_libraries(topology PUBLIC
        common_util
        common_log
        common_etcd
        coordination_backend
        hash_ring_protos
        )
add_dependencies(topology hash_ring_protos)
