# - Find UCX (ucp/api/ucp.h, libucp.so, libuct.so, libucs.so)
# This module defines
#  UCX_INCLUDE_DIR      - Directory containing ucp/api/ucp.h
#  UCX_UCP_LIBRARY      - Path to libucp
#  UCX_UCT_LIBRARY      - Path to libuct
#  UCX_UCS_LIBRARY      - Path to libucs
#  UCX_UCM_LIBRARY      - Path to libucm (optional)
#  UCX_LIBRARIES        - All required UCX libraries (ucp, uct, ucs)
#  UCX_FOUND            - True if UCX is found

set(_UCX_SEARCH_DIRS ${ucx_ROOT})

# Find include directory (look for ucp/api/ucp.h)
find_path(UCX_INCLUDE_DIR
    NAMES ucp/api/ucp.h
    PATHS ${_UCX_SEARCH_DIRS}
    PATH_SUFFIXES include
    DOC "Path to UCX include directory (containing ucp/api/ucp.h)"
    NO_CMAKE_SYSTEM_PATH
    NO_SYSTEM_ENVIRONMENT_PATH
)

# Find libraries
find_library(UCX_UCP_LIBRARY
    NAMES ucp
    PATHS ${_UCX_SEARCH_DIRS}
    PATH_SUFFIXES lib lib64
    DOC "UCX UCP library"
    NO_CMAKE_SYSTEM_PATH
    NO_SYSTEM_ENVIRONMENT_PATH
)

find_library(UCX_UCT_LIBRARY
    NAMES uct
    PATHS ${_UCX_SEARCH_DIRS}
    PATH_SUFFIXES lib lib64
    DOC "UCX UCT library"
    NO_CMAKE_SYSTEM_PATH
    NO_SYSTEM_ENVIRONMENT_PATH
)

find_library(UCX_UCS_LIBRARY
    NAMES ucs
    PATHS ${_UCX_SEARCH_DIRS}
    PATH_SUFFIXES lib lib64
    DOC "UCX UCS library"
    NO_CMAKE_SYSTEM_PATH
    NO_SYSTEM_ENVIRONMENT_PATH
)

find_library(UCX_UCM_LIBRARY
    NAMES ucm
    PATHS ${_UCX_SEARCH_DIRS}
    PATH_SUFFIXES lib lib64
    NO_CMAKE_SYSTEM_PATH
    NO_SYSTEM_ENVIRONMENT_PATH
)

# Build full library list
set(UCX_LIBRARIES ${UCX_UCP_LIBRARY} ${UCX_UCT_LIBRARY} ${UCX_UCS_LIBRARY})
if(UCX_UCM_LIBRARY)
    list(APPEND UCX_LIBRARIES ${UCX_UCM_LIBRARY})
endif()

# Standard argument handling
include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(
    UCX
    REQUIRED_VARS
    UCX_INCLUDE_DIR
    UCX_UCP_LIBRARY
    UCX_UCT_LIBRARY
    UCX_UCS_LIBRARY
)

# Set variables as advanced (hide in CMake GUI)
mark_as_advanced(
    UCX_INCLUDE_DIR
    UCX_UCP_LIBRARY
    UCX_UCT_LIBRARY
    UCX_UCS_LIBRARY
    UCX_UCM_LIBRARY
)

# Print status for debugging
if(UCX_FOUND)
    message(STATUS "UCX_INCLUDE_DIR = ${UCX_INCLUDE_DIR}")
    message(STATUS "UCX_UCP_LIBRARY = ${UCX_UCP_LIBRARY}")
    message(STATUS "UCX_UCT_LIBRARY = ${UCX_UCT_LIBRARY}")
    message(STATUS "UCX_UCS_LIBRARY = ${UCX_UCS_LIBRARY}")
    if(UCX_UCM_LIBRARY)
        message(STATUS "UCX_UCM_LIBRARY = ${UCX_UCM_LIBRARY}")
    endif()
endif()