if (BUILD_PIPLN_H2D)
    set(OS_XPRT_PIPLN_SRCS
        mock_ipc.cpp
        cuda_ipc.cpp
        chunk_manager.cpp
        dlopen_util.cpp
        os_transport_pipeline_api_impl.cpp
    )

    add_library(os_transport_pipeline STATIC ${OS_XPRT_PIPLN_SRCS})
    target_include_directories(os_transport_pipeline PUBLIC
        ${OS_TRANSPORT_INCLUDE_DIR}
        ${CUDA_INCLUDE_DIR}
    )
    target_link_libraries(os_transport_pipeline PRIVATE 
        dl
    )
    add_dependencies(os_transport_pipeline
        worker_object_cache
    )
endif()

