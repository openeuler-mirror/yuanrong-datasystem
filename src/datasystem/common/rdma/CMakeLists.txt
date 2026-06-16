set(RDMA_UTIL_SRCS
    rdma_util.cpp)

add_library(common_rdma_util STATIC ${RDMA_UTIL_SRCS})

set(URMA_SRCS "")
set(RDMA_SRCS "")
set(FAST_TRANSPORT_SRCS "")
list(APPEND FAST_TRANSPORT_SRCS
    fast_transport_base.cpp
    fast_transport_manager_wrapper.cpp
)
set(URMA_DEPEND_LIBS
    common_rdma_util)
set(RDMA_DEPEND_LIBS
    common_rdma_util)
set(HCCS_EXTRA_LIBS)
if (BUILD_WITH_URMA)
    list(APPEND URMA_SRCS
        urma_manager.cpp
        urma_async_event_handler.cpp
        urma_info.cpp
        urma_resource.cpp
        urma_dlopen_util.cpp
    )
    list(APPEND URMA_DEPEND_LIBS dl)
    list(APPEND URMA_DEPEND_LIBS ${TBB_LIBRARY})
endif()

if (BUILD_HETERO)
    list(APPEND URMA_DEPEND_LIBS common_acl_device)
    if (BUILD_HETERO_GPU)
        list(APPEND URMA_DEPEND_LIBS common_cuda_device)
    endif()
    list(APPEND URMA_DEPEND_LIBS common_shared_memory)
    list(APPEND URMA_SRCS
        npu/remote_h2d_manager.cpp
        npu/roce_transport.cpp
    )
    # HCCS transport requires cann_hixl
    if (ASCEND_HIXL_FOUND)
        list(APPEND URMA_SRCS npu/hccs_transport.cpp)
        # HIXL symbols are used by HCCSTransport; common_rdma's users need to link hixl as well
        set(HCCS_EXTRA_LIBS ${ASCEND_HIXL_LIBRARIES})
    else()
        message(WARNING "cann_hixl not found, HCCS transport will be disabled")
        set(HCCS_EXTRA_LIBS "")
    endif()
endif()

if (BUILD_WITH_RDMA)
    list(APPEND RDMA_SRCS ucp_manager.cpp ucp_worker_pool.cpp ucp_worker.cpp ucp_endpoint.cpp ucp_segment.cpp ucp_dlopen_util.cpp)
    list(APPEND RDMA_DEPEND_LIBS dl)
    list(APPEND RDMA_DEPEND_LIBS ${TBB_LIBRARY})
endif()

add_library(common_rdma STATIC ${FAST_TRANSPORT_SRCS} ${URMA_SRCS} ${RDMA_SRCS})
target_link_libraries(common_rdma
    PRIVATE ${URMA_DEPEND_LIBS} ${RDMA_DEPEND_LIBS} meta_transport_protos_client share_memory_protos_client
    PUBLIC ${HCCS_EXTRA_LIBS})

# Propagate HIXL availability to source files via compile definition
if (ASCEND_HIXL_FOUND)
    target_compile_definitions(common_rdma PRIVATE ASCEND_HIXL_AVAILABLE)
endif()
