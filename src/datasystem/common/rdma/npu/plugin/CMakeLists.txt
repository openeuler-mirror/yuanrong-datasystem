message("Build HIXL plugin with ${ASCEND_HIXL_LIBRARIES}")

add_library(ds_hixl_plugin SHARED hixl_plugin.cpp)
set_target_properties(ds_hixl_plugin PROPERTIES
    OUTPUT_NAME ds_hixl_plugin
    CXX_VISIBILITY_PRESET hidden
    VISIBILITY_INLINES_HIDDEN YES
    LINK_DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/hixl_plugin.map)
target_include_directories(ds_hixl_plugin PRIVATE ${ASCEND_HIXL_INCLUDE_DIR})
target_link_options(ds_hixl_plugin PRIVATE
    "LINKER:--version-script=${CMAKE_CURRENT_SOURCE_DIR}/hixl_plugin.map")
target_link_libraries(ds_hixl_plugin PRIVATE
    -Wl,--no-as-needed
    ${ASCEND_HIXL_LIBRARIES}
    -Wl,--as-needed)
add_custom_command(TARGET ds_hixl_plugin POST_BUILD
    COMMAND ${CMAKE_COMMAND}
        -D ELF_FILE=$<TARGET_FILE:ds_hixl_plugin>
        -D ELF_ROLE=PLUGIN
        -P ${CMAKE_SOURCE_DIR}/cmake/tests/check_datasystem_cann_needed.cmake
    COMMENT "Checking HIXL plugin CANN dependencies"
    VERBATIM)

add_custom_command(
    OUTPUT ${CMAKE_CURRENT_BINARY_DIR}/hixl_plugin_sha256.h
    COMMAND ${CMAKE_COMMAND}
        -D BIN_FILE=$<TARGET_FILE:ds_hixl_plugin>
        -P ${CMAKE_SOURCE_DIR}/cmake/scripts/CleanBuildRpath.cmake
    COMMAND ${CMAKE_COMMAND}
        -D SRC_PATH_PATTERN=$<TARGET_FILE:ds_hixl_plugin>
        -D DST_PATH=${CMAKE_CURRENT_BINARY_DIR}
        -D STRIP_LIB:BOOL=${ENABLE_STRIP}
        -P ${CMAKE_SOURCE_DIR}/cmake/scripts/StripAndGenHash.cmake
    COMMAND ${CMAKE_COMMAND}
        -D PATH=${CMAKE_CURRENT_BINARY_DIR}/libds_hixl_plugin.so
        -D OUTFILE=${CMAKE_CURRENT_BINARY_DIR}/hixl_plugin_sha256.h
        -D HASH_VAR=HIXL_PLUGIN_SHA256
        -D GUARD_NAME=HIXL_PLUGIN_SHA256_H
        -P ${CMAKE_SOURCE_DIR}/cmake/scripts/GenerateHashHeader.cmake
    DEPENDS ds_hixl_plugin
    COMMENT "Generating hash header for ds_hixl_plugin"
    VERBATIM)

add_custom_target(ds_hixl_plugin_hash ALL
    DEPENDS ${CMAKE_CURRENT_BINARY_DIR}/hixl_plugin_sha256.h)
