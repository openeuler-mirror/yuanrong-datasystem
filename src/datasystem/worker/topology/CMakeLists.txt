add_library(worker_topology_membership STATIC
        membership/worker_directory.cpp
        )
target_link_libraries(worker_topology_membership PRIVATE
        common_util
        )

add_library(worker_topology_routing STATIC
        routing/routing_view.cpp
        routing/routing_cache.cpp
        routing/worker_locator.cpp
        routing/redirect_policy.cpp
        )
target_link_libraries(worker_topology_routing PRIVATE
        common_util
        worker_topology_membership
        )

add_library(worker_topology_runtime STATIC
        runtime/placement_facade.cpp
        )
target_link_libraries(worker_topology_runtime PRIVATE
        common_util
        worker_topology_membership
        worker_topology_routing
        )
