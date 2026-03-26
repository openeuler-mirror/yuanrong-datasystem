set(RDMA_UTIL_SRCS
    rdma_util.cpp)

add_library(common_rdma_util STATIC ${RDMA_UTIL_SRCS})

set(URMA_SRCS "")
set(RDMA_SRCS "")
set(FAST_TRANSPORT_SRCS
    fast_transport_manager_wrapper.cpp)
set(URMA_DEPEND_LIBS
    common_rdma_util)
set(RDMA_DEPEND_LIBS
    common_rdma_util)
if (BUILD_WITH_URMA)
    list(APPEND URMA_SRCS urma_manager.cpp urma_info.cpp urma_dlopen_util.cpp)
    list(APPEND URMA_DEPEND_LIBS dl)
    list(APPEND URMA_DEPEND_LIBS ${TBB_LIBRARY})
endif()

if (BUILD_HETERO)
    list(APPEND URMA_DEPEND_LIBS common_acl_device)
    if (BUILD_HETERO_GPU)
        list(APPEND URMA_DEPEND_LIBS common_cuda_device)
    endif()
    list(APPEND URMA_DEPEND_LIBS common_shared_memory)
    list(APPEND URMA_SRCS npu/remote_h2d_manager.cpp)
endif()

if (BUILD_WITH_RDMA)
    list(APPEND RDMA_SRCS ucp_manager.cpp ucp_worker_pool.cpp ucp_worker.cpp ucp_endpoint.cpp ucp_segment.cpp ucp_dlopen_util.cpp)
    list(APPEND RDMA_DEPEND_LIBS dl)
endif()

add_library(common_rdma STATIC ${FAST_TRANSPORT_SRCS} ${URMA_SRCS} ${RDMA_SRCS})
target_link_libraries(common_rdma PRIVATE ${URMA_DEPEND_LIBS} ${RDMA_DEPEND_LIBS} meta_transport_protos_client share_memory_protos_client)
