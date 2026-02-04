add_subdirectory(ascend)

add_library(common_device STATIC
    comm_wrapper.cpp
    comm_wrapper_base.cpp
)

target_link_libraries(common_device PRIVATE common_acl_device)
target_link_libraries(common_device PRIVATE common_util common_inject)