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

find_library(CANN_HIXL_LIBRARY
        ${CMAKE_SHARED_LIBRARY_PREFIX}cann_hixl${CMAKE_SHARED_LIBRARY_SUFFIX}
        DOC   "Path to CANN HIXL library (optional)"
        HINTS ${Ascend_ROOT}/lib64
        NO_CMAKE_SYSTEM_PATH
        NO_SYSTEM_ENVIRONMENT_PATH)

find_path(ASCEND_HIXL_INCLUDE_DIR hixl/hixl.h
        DOC   "Path to CANN HIXL headers (optional)"
        HINTS ${Ascend_ROOT}/include
        NO_CMAKE_SYSTEM_PATH
        NO_SYSTEM_ENVIRONMENT_PATH)

find_file(ASCEND_HIXL_TYPES_HEADER hixl/hixl_types.h
        DOC   "Path to CANN HIXL type header (optional)"
        HINTS ${Ascend_ROOT}/include
        NO_CMAKE_SYSTEM_PATH
        NO_SYSTEM_ENVIRONMENT_PATH)

find_file(ASCEND_HIXL_VERSION_HEADER version/hixl_version.h
        DOC   "Path to CANN HIXL version header (optional)"
        HINTS ${Ascend_ROOT}/include
        NO_CMAKE_SYSTEM_PATH
        NO_SYSTEM_ENVIRONMENT_PATH)

find_file(ASCEND_HIXL_VERSION_INFO version.info
        DOC   "Path to CANN HIXL version info (optional)"
        HINTS ${Ascend_ROOT}/share/info/hixl
        NO_CMAKE_SYSTEM_PATH
        NO_SYSTEM_ENVIRONMENT_PATH)

find_library(METADEF_LIBRARY
        ${CMAKE_SHARED_LIBRARY_PREFIX}metadef${CMAKE_SHARED_LIBRARY_SUFFIX}
        DOC   "Path to Ascend metadef library (optional, required by HIXL public C++ APIs)"
        HINTS ${Ascend_ROOT}/lib64
        NO_CMAKE_SYSTEM_PATH
        NO_SYSTEM_ENVIRONMENT_PATH)

set(ASCEND_HIXL_FOUND FALSE)
if (ASCEND_HIXL_INCLUDE_DIR AND ASCEND_HIXL_TYPES_HEADER AND CANN_HIXL_LIBRARY AND METADEF_LIBRARY)
    set(ASCEND_HIXL_FOUND TRUE)
    set(ASCEND_HIXL_LIBRARIES ${ASCEND_LIBRARY} ${CANN_HIXL_LIBRARY} ${METADEF_LIBRARY})
endif()

set(ASCEND_HIXL_VERSION "")
if (ASCEND_HIXL_VERSION_HEADER)
    file(READ "${ASCEND_HIXL_VERSION_HEADER}" _ASCEND_HIXL_VERSION_CONTENT)
    string(REGEX MATCH "#define[ \t]+HIXL_VERSION_STR[ \t]+\"([0-9]+\\.[0-9]+\\.[0-9]+)\""
        _ASCEND_HIXL_VERSION_MATCH "${_ASCEND_HIXL_VERSION_CONTENT}")
    if (_ASCEND_HIXL_VERSION_MATCH)
        set(ASCEND_HIXL_VERSION "${CMAKE_MATCH_1}")
    endif()
    unset(_ASCEND_HIXL_VERSION_CONTENT)
    unset(_ASCEND_HIXL_VERSION_MATCH)
endif()
if (NOT ASCEND_HIXL_VERSION AND ASCEND_HIXL_VERSION_INFO)
    file(READ "${ASCEND_HIXL_VERSION_INFO}" _ASCEND_HIXL_VERSION_INFO_CONTENT)
    string(REGEX MATCH "Version=([0-9]+\\.[0-9]+\\.[0-9]+)"
        _ASCEND_HIXL_VERSION_INFO_MATCH "${_ASCEND_HIXL_VERSION_INFO_CONTENT}")
    if (_ASCEND_HIXL_VERSION_INFO_MATCH)
        set(ASCEND_HIXL_VERSION "${CMAKE_MATCH_1}")
    endif()
    unset(_ASCEND_HIXL_VERSION_INFO_CONTENT)
    unset(_ASCEND_HIXL_VERSION_INFO_MATCH)
endif()

set(ASCEND_HIXL_HCCS_SUPPORTED FALSE)
if (ASCEND_HIXL_FOUND AND ASCEND_HIXL_VERSION AND ASCEND_HIXL_VERSION VERSION_GREATER_EQUAL "8.5.2")
    set(ASCEND_HIXL_HCCS_SUPPORTED TRUE)
endif()

message("ascend lib: ${ASCEND_LIBRARY}")
message("hccl lib: ${HCCL_LIBRARY}")
if (CANN_HIXL_LIBRARY)
    message(STATUS "cann_hixl lib: ${CANN_HIXL_LIBRARY}")
else()
    message(STATUS "cann_hixl lib: not found.")
endif()
if (ASCEND_HIXL_INCLUDE_DIR)
    message(STATUS "cann_hixl include dir: ${ASCEND_HIXL_INCLUDE_DIR}")
else()
    message(STATUS "cann_hixl include dir: not found.")
endif()
if (NOT ASCEND_HIXL_TYPES_HEADER)
    message(STATUS "cann_hixl type header: not found.")
endif()
if (ASCEND_HIXL_VERSION)
    message(STATUS "cann_hixl version: ${ASCEND_HIXL_VERSION}")
else()
    message(STATUS "cann_hixl version: unknown.")
endif()
if (ASCEND_HIXL_FOUND AND ASCEND_HIXL_VERSION AND NOT ASCEND_HIXL_HCCS_SUPPORTED)
    message(WARNING "cann_hixl found, but HIXL version ${ASCEND_HIXL_VERSION} is lower than 8.5.2. "
        "HCCS RH2D will be disabled. Build with CANN/HIXL 8.5.2+ to enable remote_h2d_link_type=HCCS.")
elseif(ASCEND_HIXL_FOUND AND NOT ASCEND_HIXL_VERSION)
    message(WARNING "cann_hixl found, but HIXL version is unknown. HCCS RH2D will be disabled. "
        "Build with CANN/HIXL 8.5.2+ to enable remote_h2d_link_type=HCCS.")
endif()
if (METADEF_LIBRARY)
    message(STATUS "metadef lib: ${METADEF_LIBRARY}")
else()
    message(STATUS "metadef lib: not found.")
endif()

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(Ascend REQUIRED_VARS
        ASCEND_LIBRARY HCCL_LIBRARY ASCEND_INCLUDE_DIR)
