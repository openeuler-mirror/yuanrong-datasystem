# Coordinator store library - lightweight in-memory coordinator with TTL and watch support

set(COORDINATOR_STORE_SRCS
    coordinator_service_proxy.cpp
)

set(COORDINATOR_STORE_HDRS
    coordinator_service_proxy.h
)

# Coordinator store shared library
add_library(common_coordinator_store SHARED ${COORDINATOR_STORE_SRCS})
target_sources(common_coordinator_store PRIVATE ${COORDINATOR_STORE_HDRS})
target_include_directories(common_coordinator_store PUBLIC ${CMAKE_SOURCE_DIR})
target_compile_features(common_coordinator_store PUBLIC cxx_std_17)
target_link_libraries(common_coordinator_store PUBLIC pthread common_util coordinator_protos)
add_dependencies(common_coordinator_store coordinator_protos)
