# URMA SDK integration.
#
# Two modes controlled by URMA_PKG_URL:
#   - URMA_PKG_URL set    : download the package, auto-locate headers/libs, restructure
#                           headers into ub/umdk/urma/ to match #include <ub/umdk/urma/*.h>
#   - URMA_PKG_URL empty  : use system-installed URMA SDK (/usr/include, /usr/lib64)
#
# Exposes cache variables:
#   URMA_INCLUDE_DIR     - include root (contains ub/umdk/urma/urma_api.h)
#   URMA_LIBRARY         - liburma.so path (linked at compile time, see rdma/CMakeLists.txt)
#   URMA_LIB_LOCATION    - directory of liburma.so (used for packaging the 6 urma so files)

# Set URMA_PKG_URL/URMA_PKG_SHA256 here to enable download mode.
# Leave them empty to use the system-installed URMA SDK.
set(URMA_PKG_URL "" CACHE STRING "URMA package download URL (empty = use system URMA SDK)")
set(URMA_PKG_SHA256 "" CACHE STRING "URMA package SHA256 checksum (required when URMA_PKG_URL is set)")

if (URMA_PKG_URL)
    # --- Download package ---
    if (NOT URMA_PKG_SHA256)
        message(FATAL_ERROR "URMA_PKG_SHA256 must be set when URMA_PKG_URL is set "
            "(downloading without checksum is unsafe)")
    endif()
    download_lib_pkg(urma_pkg ${URMA_PKG_URL} ${URMA_PKG_SHA256})

    # Auto-locate include root: recursively find urma_api.h anchor
    file(GLOB_RECURSE _URMA_API_HEADER "${urma_pkg_SOURCE_DIR}/*urma_api.h")
    list(GET _URMA_API_HEADER 0 _first_header)
    if (NOT _first_header)
        message(FATAL_ERROR "urma_api.h not found in downloaded URMA package")
    endif()
    get_filename_component(_include_src_dir ${_first_header} DIRECTORY)

    # Restructure headers into ub/umdk/urma/ to match #include <ub/umdk/urma/*.h>
    set(URMA_PREFIXED_INCLUDE_DIR ${CMAKE_BINARY_DIR}/third_party/urma_pkg_prefixed_include)
    file(MAKE_DIRECTORY ${URMA_PREFIXED_INCLUDE_DIR}/ub/umdk/urma)
    file(COPY ${_include_src_dir}/
         DESTINATION ${URMA_PREFIXED_INCLUDE_DIR}/ub/umdk/urma
         FILES_MATCHING PATTERN "*.h")
    set(URMA_INCLUDE_LOCATION ${URMA_PREFIXED_INCLUDE_DIR})

    # Auto-locate lib root: recursively find liburma.so anchor
    file(GLOB_RECURSE _URMA_LIB_FILE "${urma_pkg_SOURCE_DIR}/*liburma.so")
    list(GET _URMA_LIB_FILE 0 _first_lib)
    if (NOT _first_lib)
        message(FATAL_ERROR "liburma.so not found in downloaded URMA package")
    endif()
    get_filename_component(URMA_LIB_LOCATION ${_first_lib} DIRECTORY)
    set(URMA_LIB_LOCATION ${URMA_LIB_LOCATION} CACHE PATH "Directory containing URMA shared libraries")
else()
    message(STATUS "URMA_PKG_URL not set, using system-installed URMA SDK")
endif()

find_package(URMA REQUIRED)
include_directories(${URMA_INCLUDE_DIR})

add_definitions(-DUSE_URMA)
if (URMA_OVER_UB)
    message(STATUS "Build URMA over UB")
    add_definitions(-DURMA_OVER_UB)
endif()
