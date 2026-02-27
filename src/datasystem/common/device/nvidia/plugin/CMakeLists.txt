message("Build CUDA plugin with CUDA Toolkit: ${CUDAToolkit_VERSION}")
set(CUDA_PLUGIN_DEPEND_LIBS
    ${CUDA_LIBRARY}
    ${NCCL_LIBRARY}
    protobuf::libprotobuf
)
add_library(cuda_plugin SHARED cuda_device_context.cpp)
target_link_libraries(cuda_plugin PRIVATE ${CUDA_PLUGIN_DEPEND_LIBS})

add_custom_command(
    OUTPUT ${CMAKE_CURRENT_BINARY_DIR}/cuda_plugin_sha256.h
    # Step 1: Clean rpath information.
    COMMAND ${CMAKE_COMMAND} 
        -D BIN_FILE=$<TARGET_FILE:cuda_plugin> 
        -P ${CMAKE_SOURCE_DIR}/cmake/scripts/CleanBuildRpath.cmake
    # Step 2: Strip symbol information.
    COMMAND ${CMAKE_COMMAND} 
        -D SRC_PATH_PATTERN=$<TARGET_FILE:cuda_plugin>
        -D DST_PATH=${CMAKE_CURRENT_BINARY_DIR}
        -D STRIP_LIB:BOOL=${ENABLE_STRIP}
        -P ${CMAKE_SOURCE_DIR}/cmake/scripts/StripAndGenHash.cmake
    # Step 3: Calculate the hash value and generate the header file.
    COMMAND ${CMAKE_COMMAND} 
        -D PATH=${CMAKE_CURRENT_BINARY_DIR}/libcuda_plugin.so
        -D OUTFILE=${CMAKE_CURRENT_BINARY_DIR}/cuda_plugin_sha256.h
        -D HASH_VAR=CUDA_PLUGIN_SHA256
        -D GUARD_NAME=CUDA_PLUGIN_SHA256_H
        -P ${CMAKE_SOURCE_DIR}/cmake/scripts/GenerateHashHeader.cmake
    DEPENDS cuda_plugin
    COMMENT "Generating hash header for cuda_plugin"
    VERBATIM
)

add_custom_target(cuda_plugin_hash ALL DEPENDS ${CMAKE_CURRENT_BINARY_DIR}/cuda_plugin_sha256.h)