if (BUILD_PIPLN_H2D)
    set(OS_XPRT_PIPLN_SRCS
        mock_rh2d_driver.cpp
        cuda_rh2d_driver.cpp
        chunk_manager.cpp
        os_transport_pipeline_api_impl.cpp
        pipeline_notify_queue.cpp
    )

    add_library(os_transport_pipeline STATIC ${OS_XPRT_PIPLN_SRCS})
    target_compile_definitions(os_transport_pipeline PUBLIC
        BUILD_PIPLN_H2D
    )
    target_include_directories(os_transport_pipeline PUBLIC
        ${OS_TRANSPORT_INCLUDE_DIR}
    )
    # when no cuda include dir, this means build with mock api
    if(CUDA_INCLUDE_DIR)
        target_include_directories(os_transport_pipeline PUBLIC
            ${CUDA_INCLUDE_DIR}
        )
    endif()
    target_link_libraries(os_transport_pipeline PUBLIC
        mlcachedirect_os_transport_static
        common_cuda_host_memory
        common_rdma
        common_log
        common_perf
        common_util
    )
    # api_impl.cpp calls common_rdma symbols while common_rdma's urma sources call back into the
    # OsXprtPipln hooks (only when BUILD_PIPLN_H2D is globally defined). Static-archive cycle:
    # CMake resolves it by repeating both archives on link lines of every consumer.
    target_link_libraries(common_rdma PUBLIC os_transport_pipeline)
    add_dependencies(os_transport_pipeline
        mlcachedirect_os_transport_static
    )
endif()
