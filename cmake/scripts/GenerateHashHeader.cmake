# CMake script for calculating the SHA256 hash value of the file and store it in the header file for invoking during running.
# Support passing parameters via -D, or use default values for backward compatibility with ACL plugin.
if(NOT DEFINED PATH)
    set(PATH "${CMAKE_CURRENT_BINARY_DIR}/libacl_plugin.so")
endif()
if(NOT DEFINED OUTFILE)
    set(OUTFILE "${CMAKE_CURRENT_BINARY_DIR}/acl_plugin_sha256.h")
endif()
if(NOT DEFINED HASH_VAR)
    set(HASH_VAR "ACL_PLUGIN_SHA256")
endif()
if(NOT DEFINED GUARD_NAME)
    set(GUARD_NAME "ACL_PLUGIN_SHA256_H")
endif()
  
# Calculate the SHA256 hash value of a file.
file(SHA256 ${PATH} HASH_VALUE)
  
# Generate the header file content
set(HEADER_CONTENT "#ifndef ${GUARD_NAME}\n")
set(HEADER_CONTENT "${HEADER_CONTENT}#define ${GUARD_NAME}\n")
set(HEADER_CONTENT "${HEADER_CONTENT}\n")
set(HEADER_CONTENT "${HEADER_CONTENT}#define ${HASH_VAR} \"${HASH_VALUE}\"\n")
set(HEADER_CONTENT "${HEADER_CONTENT}\n")
set(HEADER_CONTENT "${HEADER_CONTENT}#endif // ${GUARD_NAME}\n")

# Write the hash value to the header file.
file(WRITE ${OUTFILE} "${HEADER_CONTENT}")