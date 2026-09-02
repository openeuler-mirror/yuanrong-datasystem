add_library(coordinator_store_backend STATIC coordinator_store_backend.cpp)
target_include_directories(coordinator_store_backend PUBLIC ${PROTO_BUILD_DIR} ${CMAKE_SOURCE_DIR})
target_link_libraries(coordinator_store_backend PUBLIC
    cluster_coordination_backend common_coordinator_store common_log common_util)

add_library(coordinator_topology_control_host STATIC topology_control_host.cpp)
target_include_directories(coordinator_topology_control_host PUBLIC ${PROTO_BUILD_DIR} ${CMAKE_SOURCE_DIR})
target_link_libraries(coordinator_topology_control_host PUBLIC
    cluster_topology
    common_coordinator_store
    common_inject
    common_log
    common_util
    coordinator_store_backend
    coordinator_topology_recovery_manager)

add_library(coordinator_topology_recovery_manager STATIC topology_recovery_manager.cpp)
target_include_directories(coordinator_topology_recovery_manager PUBLIC ${PROTO_BUILD_DIR} ${CMAKE_SOURCE_DIR})
target_link_libraries(coordinator_topology_recovery_manager PUBLIC
    cluster_topology common_ak_sk common_coordinator_store common_log common_util)

add_subdirectory(raft)

add_library(coordinator_service_impl STATIC coordinator_service_impl.cpp)
target_include_directories(coordinator_service_impl PUBLIC ${PROTO_BUILD_DIR} ${CMAKE_SOURCE_DIR})
target_link_libraries(coordinator_service_impl PUBLIC
    coordinator_protos
    common_coordinator_store
    cluster_membership_codec
    cluster_topology_keyspace
    common_util
    common_rpc_zmq
    common_flags
    common_log
    common_metrics
    ds_flags
    rpc_stub_cache_mgr
    coordinator_watch_dispatcher_impl
    coordinator_topology_recovery_manager
    coordinator_topology_control_host
    coordinator_election_manager
    coordinator_raft_service
    coordinator_raft_types
    ds_server
)
add_dependencies(coordinator_service_impl coordinator_protos)

add_library(coordinator_watch_dispatcher_impl STATIC watch_dispatcher_impl.cpp)
target_include_directories(coordinator_watch_dispatcher_impl PUBLIC ${PROTO_BUILD_DIR} ${CMAKE_SOURCE_DIR})
target_link_libraries(coordinator_watch_dispatcher_impl
  PUBLIC coordinator_protos common_coordinator_store common_util common_flags common_metrics rpc_stub_cache_mgr)
add_dependencies(coordinator_watch_dispatcher_impl coordinator_protos)

set_source_files_properties(coordinator_runtime.cpp PROPERTIES
    COMPILE_DEFINITIONS "GIT_HASH=\"${GIT_HASH}\";GIT_BRANCH=\"${GIT_BRANCH}\"")

add_library(coordinator_runtime STATIC coordinator_runtime.cpp)
target_include_directories(coordinator_runtime PUBLIC ${PROTO_BUILD_DIR} ${CMAKE_SOURCE_DIR})
target_link_libraries(coordinator_runtime PUBLIC
    coordinator_service_impl
    coordinator_raft_types
    static_coordinator_discovery
)
target_link_libraries(coordinator_runtime PRIVATE
    butil_log_sink
    common_flags
    common_log
    common_metrics
    common_util
    common_signal
    ds_flags
    dynamic_flag_config
)
add_dependencies(coordinator_runtime coordinator_protos)
set_target_properties(coordinator_runtime PROPERTIES POSITION_INDEPENDENT_CODE ON)

add_library(coordinator_server_obj OBJECT coordinator_server.cpp)
target_include_directories(coordinator_server_obj PUBLIC ${PROTO_BUILD_DIR} ${CMAKE_SOURCE_DIR})
target_link_libraries(coordinator_server_obj PRIVATE coordinator_runtime common_signal)
set_target_properties(coordinator_server_obj PROPERTIES POSITION_INDEPENDENT_CODE ON)

add_library(coordinator_server STATIC $<TARGET_OBJECTS:coordinator_server_obj>)
target_include_directories(coordinator_server PUBLIC ${PROTO_BUILD_DIR} ${CMAKE_SOURCE_DIR})
target_link_libraries(coordinator_server PUBLIC coordinator_runtime PRIVATE common_signal)
add_dependencies(coordinator_server coordinator_protos)

add_library(datasystem_coordinator_shared SHARED $<TARGET_OBJECTS:coordinator_server_obj>)
target_include_directories(datasystem_coordinator_shared PUBLIC ${PROTO_BUILD_DIR} ${CMAKE_SOURCE_DIR})
target_link_libraries(datasystem_coordinator_shared PUBLIC coordinator_runtime PRIVATE common_signal)
set_target_properties(datasystem_coordinator_shared PROPERTIES OUTPUT_NAME datasystem_coordinator)

add_library(datasystem_coordinator_static STATIC $<TARGET_OBJECTS:coordinator_server_obj>)
target_include_directories(datasystem_coordinator_static PUBLIC ${PROTO_BUILD_DIR} ${CMAKE_SOURCE_DIR})
target_link_libraries(datasystem_coordinator_static PUBLIC coordinator_runtime PRIVATE common_signal)
set_target_properties(datasystem_coordinator_static PROPERTIES OUTPUT_NAME datasystem_coordinator)

add_executable(datasystem_coordinator coordinator_main.cpp)
set_target_properties(datasystem_coordinator PROPERTIES LINK_FLAGS "-fPIE -pie")
target_link_libraries(datasystem_coordinator PRIVATE coordinator_server common_log common_util common_flags)
set_target_properties(datasystem_coordinator PROPERTIES OUTPUT_NAME datasystem_coordinator)
