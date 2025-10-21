# - Find libsodium (sodium.h, libsodium.a)
# This module defines
#  SODIUM_INCLUDE_DIR, directory containing headers
#  SODIUM_STATIC_LIBRARY, Location of libsodium's static library
#  SODIUM_FOUND, whether libsodium has been found

find_path(SODIUM_INCLUDE_DIR sodium.h
        DOC   "Path to the libsodium header file"
        NO_CMAKE_SYSTEM_PATH
        NO_SYSTEM_ENVIRONMENT_PATH)

find_library(SODIUM_STATIC_LIBRARY ${CMAKE_STATIC_LIBRARY_PREFIX}sodium${CMAKE_STATIC_LIBRARY_SUFFIX}
        DOC   "libsodium static library"
        NO_CMAKE_SYSTEM_PATH
        NO_SYSTEM_ENVIRONMENT_PATH)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(Sodium REQUIRED_VARS
        SODIUM_INCLUDE_DIR SODIUM_STATIC_LIBRARY)