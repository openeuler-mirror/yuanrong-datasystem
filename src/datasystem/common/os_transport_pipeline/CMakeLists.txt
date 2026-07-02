if (BUILD_PIPLN_H2D)
    set(OS_XPRT_PIPLN_SRCS
        mock_rh2d_driver.cpp
        cuda_rh2d_driver.cpp
        chunk_manager.cpp
        dlopen_util.cpp
        os_transport_pipeline_api_impl.cpp
        pipeline_notify_queue.cpp
    )

    add_library(os_transport_pipeline STATIC ${OS_XPRT_PIPLN_SRCS})
    target_include_directories(os_transport_pipeline PUBLIC
        ${OS_TRANSPORT_INCLUDE_DIR}
    )
    # when no cuda include dir, this means build with mock api
    if(CUDA_INCLUDE_DIR)
        target_include_directories(os_transport_pipeline PUBLIC
            ${CUDA_INCLUDE_DIR}
        )
    endif()
    target_link_libraries(os_transport_pipeline PRIVATE 
        common_log
        common_perf
        dl
    )
    add_dependencies(os_transport_pipeline
        worker_object_cache
    )
endif()
