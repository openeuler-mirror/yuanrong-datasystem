add_library(coordinator_topology_recovery_manager STATIC topology_recovery_manager.cpp)
target_include_directories(coordinator_topology_recovery_manager PUBLIC ${PROTO_BUILD_DIR} ${CMAKE_SOURCE_DIR})
target_link_libraries(coordinator_topology_recovery_manager PUBLIC
    cluster_topology common_ak_sk common_coordinator_store common_log common_util)

add_library(coordinator_service_impl STATIC coordinator_service_impl.cpp)
target_include_directories(coordinator_service_impl PUBLIC ${PROTO_BUILD_DIR} ${CMAKE_SOURCE_DIR})
target_link_libraries(coordinator_service_impl PUBLIC
    coordinator_protos
    common_coordinator_store
    cluster_topology_keyspace
    common_util
    common_rpc_zmq
    common_log
    ds_flags
    rpc_stub_cache_mgr
    coordinator_watch_dispatcher_impl
    coordinator_topology_recovery_manager
    ds_server
)
add_dependencies(coordinator_service_impl coordinator_protos)

add_library(coordinator_watch_dispatcher_impl STATIC watch_dispatcher_impl.cpp)
target_include_directories(coordinator_watch_dispatcher_impl PUBLIC ${PROTO_BUILD_DIR} ${CMAKE_SOURCE_DIR})
target_link_libraries(coordinator_watch_dispatcher_impl
  PUBLIC coordinator_protos common_coordinator_store common_util common_flags rpc_stub_cache_mgr)
add_dependencies(coordinator_watch_dispatcher_impl coordinator_protos)

add_library(coordinator_server STATIC coordinator_server.cpp)
target_include_directories(coordinator_server PUBLIC ${PROTO_BUILD_DIR} ${CMAKE_SOURCE_DIR})
target_link_libraries(coordinator_server PUBLIC
    coordinator_service_impl
)
target_link_libraries(coordinator_server PRIVATE
    common_log
    common_util
    common_signal
    dynamic_flag_config
)
add_dependencies(coordinator_server coordinator_protos)

add_library(datasystem_coordinator_shared SHARED coordinator_server.cpp)
target_include_directories(datasystem_coordinator_shared PUBLIC ${PROTO_BUILD_DIR} ${CMAKE_SOURCE_DIR})
target_link_libraries(datasystem_coordinator_shared PUBLIC
    coordinator_service_impl
)
target_link_libraries(datasystem_coordinator_shared PRIVATE
    common_log
    common_util
    common_signal
    dynamic_flag_config
)
set_target_properties(datasystem_coordinator_shared PROPERTIES OUTPUT_NAME datasystem_coordinator)

add_library(datasystem_coordinator_static STATIC coordinator_server.cpp)
target_include_directories(datasystem_coordinator_static PUBLIC ${PROTO_BUILD_DIR} ${CMAKE_SOURCE_DIR})
target_link_libraries(datasystem_coordinator_static PUBLIC
    coordinator_service_impl
)
target_link_libraries(datasystem_coordinator_static PRIVATE
    common_log
    common_util
    common_signal
    dynamic_flag_config
)
set_target_properties(datasystem_coordinator_static PROPERTIES OUTPUT_NAME datasystem_coordinator)

add_executable(datasystem_coordinator coordinator_main.cpp)
set_target_properties(datasystem_coordinator PROPERTIES LINK_FLAGS "-fPIE -pie")
target_link_libraries(datasystem_coordinator PRIVATE coordinator_server common_log common_util common_flags)
set_target_properties(datasystem_coordinator PROPERTIES OUTPUT_NAME datasystem_coordinator)
