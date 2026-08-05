find_path(CUDA_RUNTIME_API_INCLUDE_DIR cuda_runtime_api.h
    HINTS
        ${CUDA_INCLUDE_DIR}
        ${Cuda_ROOT}/include
        /usr/local/cuda/include
        /usr/include)

add_library(common_cuda_host_memory STATIC cuda_host_memory.cpp)
target_link_libraries(common_cuda_host_memory PRIVATE common_util ${CMAKE_DL_LIBS})
if (CUDA_RUNTIME_API_INCLUDE_DIR)
    target_include_directories(common_cuda_host_memory PRIVATE ${CUDA_RUNTIME_API_INCLUDE_DIR})
endif()

if (BUILD_HETERO_GPU)
    set(CUDA_DEVICE_DEPEND_LIBS
        common_util
        ${CMAKE_DL_LIBS}
    )
    add_library(common_cuda_device STATIC
        cuda_device_manager.cpp
        cuda_resource_manager.cpp
    )
    target_link_libraries(common_cuda_device PRIVATE ${CUDA_DEVICE_DEPEND_LIBS})

    if (BUILD_HETERO)
        add_subdirectory(plugin)
        # Ensure that cuda_plugin_hash are generated before building datasystem.
        # Dependency: datasystem -> common_cuda_device -> cuda_plugin_hash -> cuda_plugin
        add_dependencies(common_cuda_device cuda_plugin_hash)
    endif()
endif()
