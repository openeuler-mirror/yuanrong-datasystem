set(CUDA_DEVICE_DEPEND_LIBS
    common_util
    dl
)
add_library(common_cuda_device STATIC
    cuda_device_manager.cpp
    cuda_resource_manager.cpp
)
target_link_libraries(common_cuda_device PRIVATE ${CUDA_DEVICE_DEPEND_LIBS})

if (BUILD_HETERO AND BUILD_HETERO_GPU)
    add_subdirectory(plugin)
    # Ensure that cuda_plugin_hash are generated before building datasystem.
    # Dependency: datasystem -> common_cuda_device -> cuda_plugin_hash -> cuda_plugin
    add_dependencies(common_cuda_device cuda_plugin_hash)
endif()
