add_library(coordinator_leader_router STATIC coordinator_leader_router.cpp)
target_include_directories(coordinator_leader_router PUBLIC ${PROTO_BUILD_DIR} ${CMAKE_SOURCE_DIR})
target_compile_features(coordinator_leader_router PUBLIC cxx_std_17)
target_link_libraries(coordinator_leader_router PUBLIC coordinator_protos common_util)

# Fixed-address Coordinator discovery shared by client, Worker, and Coordinator startup.
add_library(static_coordinator_discovery STATIC static_coordinator_discovery.cpp)
target_include_directories(static_coordinator_discovery PUBLIC ${PROTO_BUILD_DIR} ${CMAKE_SOURCE_DIR})
target_link_libraries(static_coordinator_discovery PUBLIC coordinator_leader_router common_util)

# Coordinator store library - lightweight in-memory coordinator with TTL and watch support

set(COORDINATOR_STORE_SRCS
    coordinator_service_proxy.cpp
    coordinator_store.cpp
    memory_kv_store.cpp
    ttl_manager.cpp
    watch_dispatcher.cpp
    watch_registry.cpp
)

set(COORDINATOR_STORE_HDRS
    coordinator_service_proxy.h
    coordinator_leader_router.h
    coordinator_store.h
    key_value_entry.h
    memory_kv_store.h
    steady_clock.h
    ttl_manager.h
    watch_dispatcher.h
    watch_event.h
    watch_registry.h
)

# Coordinator store static library
add_library(common_coordinator_store STATIC ${COORDINATOR_STORE_SRCS})
target_sources(common_coordinator_store PRIVATE ${COORDINATOR_STORE_HDRS})
target_include_directories(common_coordinator_store PUBLIC ${CMAKE_SOURCE_DIR})
target_compile_features(common_coordinator_store PUBLIC cxx_std_17)
target_link_libraries(common_coordinator_store PUBLIC pthread coordinator_leader_router coordinator_protos common_log common_util ds_flags rpc_stub_cache_mgr)
