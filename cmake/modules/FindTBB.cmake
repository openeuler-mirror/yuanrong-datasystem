# - Find TBB (tbb.h, libtbb.so libtbbmalloc.so libtbbmalloc_proxy.so)
# This module defines
#  TBB_INCLUDE_DIR, directory containing headers
#  TBB_LIBRARY, Location of libtbb's shared
#  TBB_FOUND, whether tbb has been found

find_path(TBB_INCLUDE_DIR tbb/tbb.h
        DOC   "Path to the TBB header file"
        NO_CMAKE_SYSTEM_PATH
        NO_SYSTEM_ENVIRONMENT_PATH)

find_library(TBB_LIBRARY tbb
        DOC   "Tbb library"
        NO_CMAKE_SYSTEM_PATH
        NO_SYSTEM_ENVIRONMENT_PATH)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(TBB REQUIRED_VARS
        TBB_LIBRARY TBB_INCLUDE_DIR)