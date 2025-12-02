# - Find URMA (urma_types.h, liburma.so)
# This module defines
#  URMA_INCLUDE_DIR, directory containing urma headers
#  URMA_LIBRARY, Location of liburma's shared
#  URMA_FOUND, whether URMA has been found

find_path(URMA_INCLUDE_DIR ub/umdk/urma/urma_api.h
          DOC   "Path to the urma api header file")

find_library(URMA_LIBRARY urma
             PATHS ${URMA_LIB_LOCATION}
             DOC   "URMA library")

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(URMA REQUIRED_VARS URMA_LIBRARY URMA_INCLUDE_DIR)

message(STATUS "URMA_LIBRARY=${URMA_LIBRARY}")
message(STATUS "URMA_INCLUDE_DIR=${URMA_INCLUDE_DIR}")
