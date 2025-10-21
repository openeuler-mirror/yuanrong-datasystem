# - Find URMA (urma_types.h, liburma.so, liburma_ip.so)
# This module defines
#  URMA_INCLUDE_DIR, directory containing urma headers
#  URMA_LIBRARY, Location of liburma's shared
#  URMA_IP_LIBRARY, Location of liburma_ip's shared
#  URMA_FOUND, whether URMA has been found

find_path(URMA_INCLUDE_DIR urma_api.h
          PATHS ${URMA_INCLUDE_LOCATION}
          DOC   "Path to the urma api header file"
          NO_CMAKE_SYSTEM_PATH
          NO_SYSTEM_ENVIRONMENT_PATH)

find_path(URMA_INCLUDE_DIR urma_types.h
          PATHS ${URMA_INCLUDE_LOCATION}
          DOC   "Path to the urma header file"
          NO_CMAKE_SYSTEM_PATH
          NO_SYSTEM_ENVIRONMENT_PATH)

find_library(URMA_LIBRARY urma
             PATHS ${URMA_LIB_LOCATION}
             DOC   "URMA library"
             NO_CMAKE_SYSTEM_PATH
             NO_SYSTEM_ENVIRONMENT_PATH)

find_library(URMA_IP_LIBRARY urma_ip
             PATHS ${URMA_IP_IB_LIB_LOCATION}
             DOC   "URMA IP library"
             NO_CMAKE_SYSTEM_PATH
             NO_SYSTEM_ENVIRONMENT_PATH)

find_library(URMA_IB_LIBRARY urma_ib
             PATHS ${URMA_IP_IB_LIB_LOCATION}
             DOC   "URMA IB library"
             NO_CMAKE_SYSTEM_PATH
             NO_SYSTEM_ENVIRONMENT_PATH)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(URMA REQUIRED_VARS
    URMA_LIBRARY URMA_IP_LIBRARY URMA_IB_LIBRARY URMA_INCLUDE_DIR)

message(STATUS "URMA_LIBRARY=${URMA_LIBRARY}")
message(STATUS "URMA_IP_LIBRARY=${URMA_IP_LIBRARY}")
message(STATUS "URMA_IB_LIBRARY=${URMA_IB_LIBRARY}")
message(STATUS "URMA_INCLUDE_DIR=${URMA_INCLUDE_DIR}")
