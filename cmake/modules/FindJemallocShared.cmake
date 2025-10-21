# - Find Jemalloc (libjemalloc.so)
# This module defines
#  JEMALLOC_SHARED_LIBRARY, Location of jemalloc's shared library

find_library(JEMALLOC_SHARED_LIBRARY ${CMAKE_SHARED_LIBRARY_PREFIX}jemalloc${CMAKE_SHARED_LIBRARY_SUFFIX}
        DOC   "jemalloc is a general purpose malloc(3) implementation that emphasizes fragmentation avoidance and scalable concurrency support."
        NO_CMAKE_SYSTEM_PATH
        NO_SYSTEM_ENVIRONMENT_PATH)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(JemallocShared REQUIRED_VARS JEMALLOC_SHARED_LIBRARY)