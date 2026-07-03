set(TOPOLOGY_COORDINATION_BACKEND_SRCS
        coordination_backend.cpp
        etcd_coordination_backend.cpp
        )

add_library(coordination_backend STATIC ${TOPOLOGY_COORDINATION_BACKEND_SRCS})
target_include_directories(coordination_backend PUBLIC ${PROJECT_SOURCE_DIR}/src)
target_link_libraries(coordination_backend PUBLIC
        common_coordinator_store
        common_etcd
        common_util
        membership_value_codec
        coordinator_protos
        )
