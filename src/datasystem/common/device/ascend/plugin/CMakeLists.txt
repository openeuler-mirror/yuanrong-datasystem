message("Build Ascend plugin with ${ASCEND_LIBRARY}")
set(ACL_PLUGIN_DEPEND_LIBS
    ${ASCEND_LIBRARY}
    ${HCCL_LIBRARY}
    p2p-transfer::p2ptransfer
    protobuf::libprotobuf
)
add_library(acl_plugin SHARED acl_device_context.cpp)
target_link_libraries(acl_plugin PRIVATE ${ACL_PLUGIN_DEPEND_LIBS})

add_custom_command(
    OUTPUT ${CMAKE_CURRENT_BINARY_DIR}/acl_plugin_sha256.h
    # Step 1: Clean rpath information.
    COMMAND ${CMAKE_COMMAND} 
        -D BIN_FILE=$<TARGET_FILE:acl_plugin> 
        -P ${CMAKE_SOURCE_DIR}/cmake/scripts/CleanBuildRpath.cmake
    # Step 2: Strip symbol information.
    COMMAND ${CMAKE_COMMAND} 
        -D SRC_PATH_PATTERN=$<TARGET_FILE:acl_plugin>
        -D DST_PATH=${CMAKE_CURRENT_BINARY_DIR}
        -D STRIP_LIB:BOOL=${ENABLE_STRIP}
        -P ${CMAKE_SOURCE_DIR}/cmake/scripts/StripAndGenHash.cmake
    # Step 3: Calculate the hash value and generate the header file.
    COMMAND ${CMAKE_COMMAND} -P ${CMAKE_SOURCE_DIR}/cmake/scripts/GenerateHashHeader.cmake
    DEPENDS acl_plugin
    COMMENT "Generating hash header for acl_plugin"
    VERBATIM
)

add_custom_target(acl_plugin_hash ALL DEPENDS ${CMAKE_CURRENT_BINARY_DIR}/acl_plugin_sha256.h)