add_subdirectory(ascend)
if (BUILD_HETERO_GPU)
    add_subdirectory(nvidia)
endif()

add_library(common_device STATIC
    comm_wrapper.cpp
    comm_wrapper_base.cpp
    device_resource_manager.cpp
    acl_pipeline_p2p_task.cpp
)

target_link_libraries(common_device PRIVATE common_acl_device)
if (BUILD_HETERO_GPU)
    target_link_libraries(common_device PRIVATE common_cuda_device)
endif()
target_link_libraries(common_device PRIVATE common_util common_inject)
