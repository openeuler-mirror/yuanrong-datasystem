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
    coordinator_store.h
    key_value_entry.h
    memory_kv_store.h
    steady_clock.h
    ttl_manager.h
    watch_dispatcher.h
    watch_event.h
    watch_registry.h
)

# Coordinator store shared library
add_library(common_coordinator_store SHARED ${COORDINATOR_STORE_SRCS})
target_sources(common_coordinator_store PRIVATE ${COORDINATOR_STORE_HDRS})
target_include_directories(common_coordinator_store PUBLIC ${CMAKE_SOURCE_DIR})
target_compile_features(common_coordinator_store PUBLIC cxx_std_17)
target_link_libraries(common_coordinator_store PUBLIC pthread common_log common_util ds_flags)
