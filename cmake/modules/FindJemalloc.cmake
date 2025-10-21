# - Find Jemalloc (jemalloc.h, libjemalloc.a)
# This module defines
#  JEMALLOC_INCLUDE_DIR, directory containing headers
#  JEMALLOC_STATIC_LIBRARY, Location of jemalloc's static library
#  JEMALLOC_FOUND, whether jemalloc has been found

find_path(JEMALLOC_INCLUDE_DIR jemalloc/jemalloc.h
        DOC   "Path to the jemalloc header file"
        NO_CMAKE_SYSTEM_PATH
        NO_SYSTEM_ENVIRONMENT_PATH)

find_library(JEMALLOC_STATIC_LIBRARY ${CMAKE_STATIC_LIBRARY_PREFIX}jemalloc${CMAKE_STATIC_LIBRARY_SUFFIX}
        DOC   "jemalloc is a general purpose malloc(3) implementation that emphasizes fragmentation avoidance and scalable concurrency support."
        NO_CMAKE_SYSTEM_PATH
        NO_SYSTEM_ENVIRONMENT_PATH)

find_library(JEMALLOC_PIC_LIBRARY jemalloc_pic
        DOC   "jemalloc is a general purpose malloc(3) implementation that emphasizes fragmentation avoidance and scalable concurrency support."
        NO_CMAKE_SYSTEM_PATH
        NO_SYSTEM_ENVIRONMENT_PATH)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(Jemalloc REQUIRED_VARS
        JEMALLOC_PIC_LIBRARY JEMALLOC_STATIC_LIBRARY JEMALLOC_INCLUDE_DIR)