add_library(membership_value_codec STATIC membership/membership_value_codec.cpp)
target_include_directories(membership_value_codec PUBLIC ${PROJECT_SOURCE_DIR}/src)
target_link_libraries(membership_value_codec PUBLIC common_util coordinator_protos)
add_dependencies(membership_value_codec coordinator_protos)

add_library(worker_node_info STATIC membership/worker_node_info.cpp)
target_include_directories(worker_node_info PUBLIC ${PROJECT_SOURCE_DIR}/src)
target_link_libraries(worker_node_info PUBLIC common_util coordinator_protos)
add_dependencies(worker_node_info coordinator_protos)

add_library(worker_node_info_client STATIC membership/worker_node_info.cpp)
target_include_directories(worker_node_info_client PUBLIC ${PROJECT_SOURCE_DIR}/src)
target_link_libraries(worker_node_info_client PUBLIC common_util coordinator_protos_client)
add_dependencies(worker_node_info_client coordinator_protos_client)

add_subdirectory(coordination_backend)

set(TOPOLOGY_SRCS
        algorithm/algorithm_catalog.cpp
        algorithm/hash_algorithm.cpp
        algorithm/route_policy_engine.cpp
        executor/topology_task_executor.cpp
        membership/cluster_membership.cpp
        membership/cluster_registry.cpp
        orchestration/topology_control_loop.cpp
        orchestration/topology_mutation_applier.cpp
        orchestration/topology_plan_builder.cpp
        routing/placement_directory.cpp
        routing/membership_endpoint_view.cpp
        routing/redirect_policy.cpp
        routing/routing_cache.cpp
        routing/routing_view.cpp
        routing/owner_endpoint_resolver.cpp
        routing/worker_locator.cpp
        routing/placement_facade.cpp
        repository/topology_key_helper.cpp
        repository/topology_repository_codec.cpp
        repository/topology_repository.cpp
        runtime/coordination_event_pump.cpp
        runtime/topology_degraded_startup_checker.cpp
        runtime/topology_admin.cpp
        runtime/topology_change_handler.cpp
        runtime/topology_diagnostics.cpp
        runtime/topology_engine.cpp
        runtime/topology_local_snapshot_store.cpp
        runtime/topology_readiness.cpp
        runtime/topology_state_digest.cpp
        runtime/topology_snapshot_rebuilder.cpp
        )

add_library(topology STATIC ${TOPOLOGY_SRCS})
target_include_directories(topology PUBLIC ${PROJECT_SOURCE_DIR}/src)
target_link_libraries(topology PUBLIC
        common_util
        common_log
        common_etcd
        coordination_backend
        membership_value_codec
        worker_node_info
        coordinator_protos
        hash_ring_protos
        )
add_dependencies(topology coordinator_protos hash_ring_protos)
