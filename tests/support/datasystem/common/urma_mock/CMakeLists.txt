set(URMA_MOCK_SRCS
    abi/mock_abi.cpp
    abi/mock_dlopen_resolver.cpp
    transport/uds_endpoint_service.cpp
    registry/import_endpoint_registry.cpp
    segment/memfd_resolver.cpp
    segment/segment_identity.cpp
    segment/segment_memory.cpp
    segment/segment_endpoint_registry.cpp
    segment/segment_view.cpp
    transport/uds_transport.cpp
    inject/fault_inject.cpp
    mock_registry.cpp
    urma_mock_backend.cpp
    urma_mock_backend_ops.cpp
    post_send/mock_thread_pool.cpp
    post_send/post_send_wr.cpp
    objects/mock_context.cpp
    objects/mock_device.cpp
    objects/mock_jfc.cpp
    objects/mock_jetty.cpp
    objects/mock_seg.cpp
    objects/mock_tjetty.cpp
)

add_library(common_urma_mock_obj OBJECT ${URMA_MOCK_SRCS})
target_include_directories(common_urma_mock_obj
    PUBLIC
        ${PROJECT_SOURCE_DIR}/tests/support
        ${PROJECT_SOURCE_DIR}/src
)
target_link_libraries(common_urma_mock_obj
    PUBLIC common_flags common_inject common_log meta_transport_protos_client ${SECUREC_LIBRARY}
)
target_compile_definitions(common_urma_mock_obj PUBLIC USE_URMA_MOCK)

add_library(common_urma_mock STATIC $<TARGET_OBJECTS:common_urma_mock_obj>)
target_link_libraries(common_urma_mock PUBLIC common_urma_mock_obj)

foreach(_target datasystem_static datasystem datasystem_worker_static datasystem_worker_shared)
    if (TARGET ${_target})
        target_link_libraries(${_target} PRIVATE common_urma_mock_obj)
    endif()
endforeach()
