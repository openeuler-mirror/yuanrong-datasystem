set(ACL_DEVICE_DEPEND_LIBS
    common_util
    dl
)
add_library(common_acl_device STATIC
    acl_device_manager.cpp
    p2phccl_comm_wrapper.cpp
    ffts_dispatcher.cpp
    acl_resource_manager.cpp
    callback_thread.cpp
)
target_link_libraries(common_acl_device PRIVATE ${ACL_DEVICE_DEPEND_LIBS})

if (BUILD_HETERO AND BUILD_HETERO_NPU)
    add_subdirectory(plugin)
    # Ensure that acl_plugin_hash are generated before building datasystem.
    # Dependency: datasystem -> common_acl_device -> acl_plugin_hash -> acl_plugin
    add_dependencies(common_acl_device acl_plugin_hash)
endif()
