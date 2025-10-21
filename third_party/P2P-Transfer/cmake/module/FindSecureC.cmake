# - Find SercureC (securec.h, libsecurec.so)
# This module defines
#  SECUREC_INCLUDE_DIR, directory containing headers
#  SECUREC_LIBRARY, Location of libsecurec's shared or static library
#  SECUREC_FOUND, whether securec has been found
 
find_path(SECUREC_INCLUDE_DIR securec.h
        DOC   "Path to the securec header file"
        NO_CMAKE_SYSTEM_PATH
        NO_SYSTEM_ENVIRONMENT_PATH)
 
find_library(SECUREC_LIBRARY securec
        DOC   "Huawei secure function library"
        NO_CMAKE_SYSTEM_PATH
        NO_SYSTEM_ENVIRONMENT_PATH)
 
include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(SecureC REQUIRED_VARS
        SECUREC_LIBRARY SECUREC_INCLUDE_DIR)