# - Find ASCEND (acl_base.h, acl.h, libascendcl.so )
# This module defines
#  ASCEND_INCLUDE_DIR, directory containing headers
#  ASCEND_LIBRARY, Location of libascendcl's shared
#  ASCEND_FOUND, whether ascend has been found
 
find_path(ASCEND_INCLUDE_DIR acl/acl.h
        DOC   "Path to the ASCEND header file"
        HINTS ${Ascend_ROOT}/include
        NO_CMAKE_SYSTEM_PATH
        NO_SYSTEM_ENVIRONMENT_PATH)
 
find_library(ASCENDCL_LIBRARY 
        ${CMAKE_SHARED_LIBRARY_PREFIX}ascendcl${CMAKE_SHARED_LIBRARY_SUFFIX}
        DOC   "Path to Ascend library"
        HINTS ${Ascend_ROOT}/lib64
        NO_CMAKE_SYSTEM_PATH
        NO_SYSTEM_ENVIRONMENT_PATH)
 
find_library(ASCEND_RUNTIME_LIBRARY 
        ${CMAKE_SHARED_LIBRARY_PREFIX}runtime${CMAKE_SHARED_LIBRARY_SUFFIX}
        DOC   "Path to Runtime library"
        HINTS ${Ascend_ROOT}/lib64
        NO_CMAKE_SYSTEM_PATH
        NO_SYSTEM_ENVIRONMENT_PATH)

find_library(ASCEND_HCCL_TRANSFER_LIBRARY 
        ${CMAKE_SHARED_LIBRARY_PREFIX}hccl_plf${CMAKE_SHARED_LIBRARY_SUFFIX}
        DOC   "Path to Hccl Transfer Library library"
        HINTS ${Ascend_ROOT}/lib64
        NO_CMAKE_SYSTEM_PATH
        NO_SYSTEM_ENVIRONMENT_PATH)

find_library(ASCEND_RA_LIBRARY 
        ${CMAKE_SHARED_LIBRARY_PREFIX}ra${CMAKE_SHARED_LIBRARY_SUFFIX}
        DOC   "Path to Ra library"
        HINTS ${Ascend_ROOT}/lib64
        NO_CMAKE_SYSTEM_PATH
        NO_SYSTEM_ENVIRONMENT_PATH)
 
message("ascend lib: ${ASCENDCL_LIBRARY}")
message("runtime lib: ${ASCEND_RUNTIME_LIBRARY}")
message("ra lib: ${ASCEND_RA_LIBRARY}")
message("tsd client lib: ${ASCEND_TSD_CLIENT_LIBRARY}")

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(Ascend REQUIRED_VARS
        ASCENDCL_LIBRARY ASCEND_RUNTIME_LIBRARY ASCEND_HCCL_TRANSFER_LIBRARY ASCEND_RA_LIBRARY ASCEND_INCLUDE_DIR)
