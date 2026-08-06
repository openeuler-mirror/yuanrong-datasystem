set(RDMA_UTIL_SRCS
    rdma_util.cpp)

add_library(common_rdma_util STATIC ${RDMA_UTIL_SRCS})
target_link_libraries(common_rdma_util PUBLIC ${BRPC_LIBRARIES})

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
if (BUILD_WITH_URMA OR BUILD_WITH_URMA_MOCK)
    list(APPEND URMA_DEPEND_LIBS common_cuda_host_memory)
    list(APPEND URMA_SRCS
        urma_manager.cpp
        urma_async_event_handler.cpp
        urma_info.cpp
        urma_resource.cpp
        urma_dlopen_util.cpp
    )
    # Real URMA: link liburma at compile time (the 44 ds_urma_* shims call
    # symbols resolved by -lurma directly). liburma_ubagg/liburma-udma are
    # liburma's transitive DT_NEEDED dependencies; they are NOT linked
    # explicitly to avoid recursively validating their own transitive deps
    # (e.g. libummu needs GLIBCXX_3.4.29 which may exceed the build host's
    # libstdc++). They are shipped in the package (see cmake/package.cmake)
    # and resolved by the runtime dynamic linker.
    # libtpsa provides ub_str_to_* symbols that liburma.so references but does
    # not declare in its DT_NEEDED, so it must be linked explicitly.
    # Mock URMA: no real library needed (weak-symbol dispatch, no dlopen).
    if (BUILD_WITH_URMA)
        list(APPEND URMA_DEPEND_LIBS ${URMA_LIBRARY})
        find_library(URMA_TPSA_LIBRARY tpsa PATHS ${URMA_LIB_LOCATION} DOC "URMA tpsa library")
        if (URMA_TPSA_LIBRARY)
            list(APPEND URMA_DEPEND_LIBS ${URMA_TPSA_LIBRARY})
        endif()
    endif()
    list(APPEND URMA_DEPEND_LIBS ${TBB_LIBRARY})
    list(APPEND URMA_DEPEND_LIBS string_ref)
    list(APPEND URMA_DEPEND_LIBS common_util)
endif()

if (BUILD_WITH_URMA OR BUILD_HETERO)
    list(APPEND URMA_DEPEND_LIBS common_shared_memory)
endif()

if (BUILD_HETERO AND BUILD_HETERO_NPU)
    list(APPEND URMA_DEPEND_LIBS common_acl_device)
    if (BUILD_HETERO_GPU)
        list(APPEND URMA_DEPEND_LIBS common_cuda_device)
    endif()
    list(APPEND URMA_SRCS
        npu/remote_h2d_manager.cpp
        npu/roce_transport.cpp
    )
    # HCCS transport requires cann_hixl with GlobalResourceConfig support.
    if (ASCEND_HIXL_HCCS_SUPPORTED)
        list(APPEND URMA_SRCS npu/hixl_transport.cpp)
        # HIXL symbols are used by HixlTransport; common_rdma's users need to link hixl as well
        set(HCCS_EXTRA_LIBS ${ASCEND_HIXL_LIBRARIES})
    else()
        message(WARNING "cann_hixl not found or HIXL version is lower than 8.5.2, HCCS transport will be disabled")
        set(HCCS_EXTRA_LIBS "")
    endif()
endif()
if (BUILD_HETERO)
    if (BUILD_HETERO_GPU)
        list(APPEND URMA_DEPEND_LIBS common_cuda_device)
    endif()
    list(APPEND URMA_DEPEND_LIBS common_shared_memory)
endif()

if (BUILD_WITH_RDMA)
    list(APPEND RDMA_SRCS ucp_manager.cpp ucp_worker_pool.cpp ucp_worker.cpp ucp_endpoint.cpp ucp_segment.cpp ucp_dlopen_util.cpp)
    list(APPEND RDMA_DEPEND_LIBS dl)
    list(APPEND RDMA_DEPEND_LIBS ${TBB_LIBRARY})
endif()

add_library(common_rdma STATIC ${FAST_TRANSPORT_SRCS} ${URMA_SRCS} ${RDMA_SRCS})
if (BUILD_WITH_URMA)
    # Let the linker locate liburma's transitive DT_NEEDED deps (liburma_common,
    # libtpsa, etc.) in the URMA lib dir without linking them explicitly.
    target_link_directories(common_rdma PRIVATE ${URMA_LIB_LOCATION})
    target_link_options(common_rdma PRIVATE "-Wl,-rpath-link,${URMA_LIB_LOCATION}")
    target_compile_definitions(common_rdma PUBLIC USE_URMA)
endif()
if (BUILD_WITH_URMA_MOCK)
    target_compile_definitions(common_rdma PUBLIC USE_URMA USE_URMA_MOCK)
endif()
target_link_libraries(common_rdma
    PRIVATE ${URMA_DEPEND_LIBS} ${RDMA_DEPEND_LIBS} meta_transport_protos_client share_memory_protos_client
    PUBLIC ${HCCS_EXTRA_LIBS} ${BRPC_LIBRARIES})

# Propagate HCCS transport availability to source files via compile definition
if (ASCEND_HIXL_HCCS_SUPPORTED)
    target_compile_definitions(common_rdma PRIVATE ASCEND_HIXL_AVAILABLE)
endif()
