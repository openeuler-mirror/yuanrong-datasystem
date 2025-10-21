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

find_library(ASCEND_LIBRARY ${CMAKE_SHARED_LIBRARY_PREFIX}ascendcl${CMAKE_SHARED_LIBRARY_SUFFIX}
        ${CMAKE_SHARED_LIBRARY_PREFIX}hccl${CMAKE_SHARED_LIBRARY_SUFFIX}
        DOC   "Path to Ascend library"
        HINTS ${Ascend_ROOT}/lib64
        NO_CMAKE_SYSTEM_PATH
        NO_SYSTEM_ENVIRONMENT_PATH)

find_library(HCCL_LIBRARY 
        ${CMAKE_SHARED_LIBRARY_PREFIX}hccl${CMAKE_SHARED_LIBRARY_SUFFIX}
        DOC   "Path to HCCL library"
        HINTS ${Ascend_ROOT}/lib64
        NO_CMAKE_SYSTEM_PATH
        NO_SYSTEM_ENVIRONMENT_PATH)

message("ascend lib: ${ASCEND_LIBRARY}")
message("hccl lib: ${HCCL_LIBRARY}")

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(Ascend REQUIRED_VARS
        ASCEND_LIBRARY HCCL_LIBRARY ASCEND_INCLUDE_DIR)
