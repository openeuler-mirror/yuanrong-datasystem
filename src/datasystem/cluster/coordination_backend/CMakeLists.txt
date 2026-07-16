set(CLUSTER_COORDINATION_BACKEND_SRCS
        coordination_backend.cpp
        ds_coordination_backend.cpp
        etcd_coordination_backend.cpp)

add_library(cluster_coordination_backend STATIC ${CLUSTER_COORDINATION_BACKEND_SRCS})
target_include_directories(cluster_coordination_backend PUBLIC ${PROJECT_SOURCE_DIR}/src)
target_link_libraries(cluster_coordination_backend PUBLIC
        cluster_membership_codec
        common_coordinator_store
        common_etcd
        common_util
        coordinator_protos)

add_library(topology_recovery_reporter STATIC topology_recovery_reporter.cpp)
target_include_directories(topology_recovery_reporter PUBLIC ${PROTO_BUILD_DIR} ${CMAKE_SOURCE_DIR})
target_link_libraries(topology_recovery_reporter PUBLIC
        common_ak_sk common_coordinator_store common_log common_util coordinator_protos)
add_dependencies(topology_recovery_reporter coordinator_protos)
