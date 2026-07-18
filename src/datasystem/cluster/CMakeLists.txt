add_library(cluster_membership_codec STATIC
        membership/membership_value_codec.cpp)
target_include_directories(cluster_membership_codec PUBLIC ${PROJECT_SOURCE_DIR}/src)
target_link_libraries(cluster_membership_codec PUBLIC
        common_util
        coordinator_protos)

add_library(cluster_membership_codec_client STATIC
        membership/membership_value_codec.cpp)
target_include_directories(cluster_membership_codec_client PUBLIC ${PROJECT_SOURCE_DIR}/src)
target_link_libraries(cluster_membership_codec_client PUBLIC
        common_util
        coordinator_protos_client)

add_subdirectory(coordination_backend)

add_library(cluster_topology_keyspace STATIC
        repository/topology_key_helper.cpp)
target_include_directories(cluster_topology_keyspace PUBLIC ${PROJECT_SOURCE_DIR}/src)
target_link_libraries(cluster_topology_keyspace PUBLIC
        common_log
        common_util)

add_library(cluster_topology_scope STATIC
        executor/hash_key_filter.cpp
        executor/storage_scan_plan.cpp)
target_include_directories(cluster_topology_scope PUBLIC ${PROJECT_SOURCE_DIR}/src)
target_link_libraries(cluster_topology_scope PUBLIC common_util)

add_library(cluster_topology STATIC
        algorithm/algorithm_catalog.cpp
        algorithm/hash_algorithm.cpp
        control/topology_controller.cpp
        control/topology_controller_runtime.cpp
        control/topology_failure_classifier.cpp
        control/topology_plan_builder.cpp
        control/topology_task_materializer.cpp
        control/topology_task_janitor.cpp
        executor/topology_task_executor.cpp
        membership/membership_endpoint_view.cpp
        model/topology_snapshot.cpp
        repository/topology_repository.cpp
        repository/topology_repository_codec.cpp
        routing/placement_facade.cpp
        runtime/coordination_event_dispatcher.cpp
        runtime/topology_engine.cpp
        runtime/topology_observer.cpp
        runtime/topology_reader.cpp
        runtime/topology_role_watch_plan.cpp
        runtime/topology_snapshot_state.cpp)

target_include_directories(cluster_topology PUBLIC ${PROJECT_SOURCE_DIR}/src)
target_link_libraries(cluster_topology PUBLIC
        cluster_coordination_backend
        cluster_membership_codec
        cluster_topology_keyspace
        cluster_topology_scope
        cluster_topology_protos
        topology_recovery_reporter
        common_ak_sk
        common_log
        common_util)
add_dependencies(cluster_topology cluster_topology_protos)
