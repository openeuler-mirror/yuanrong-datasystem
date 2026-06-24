add_subdirectory(coordination_backend)

set(TOPOLOGY_SRCS
        membership/cluster_membership.cpp
        membership/cluster_registry.cpp
        membership/worker_directory.cpp
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
