set(RDMA_UTIL_SRCS
    rdma_util.cpp)

add_library(common_rdma_util STATIC ${RDMA_UTIL_SRCS})

set(URMA_SRCS
    urma_manager_wrapper.cpp)
set(URMA_DEPEND_LIBS
    common_rdma_util)

if (BUILD_WITH_URMA)
    list(APPEND URMA_SRCS urma_manager.cpp urma_info.cpp)
    list(APPEND URMA_DEPEND_LIBS ${URMA_LIBRARY})

    set(URMA_STUB_SRCS
        urma_stub.cpp
        urma_info.cpp)

    add_library(common_stub_rdma STATIC ${URMA_STUB_SRCS})
endif()

if (BUILD_WITH_RDMA)
    list(APPEND RDMA_SRCS ucp_manager.cpp)
    list(APPEND RDMA_DEPEND_LIBS ${UCX_LIBRARIES})

    # TODO: STUB RDMA_STUB_SRCS
endif()

add_library(common_rdma STATIC ${URMA_SRCS} ${RDMA_SRCS})
target_link_libraries(common_rdma PRIVATE ${URMA_DEPEND_LIBS} ${RDMA_DEPEND_LIBS})