# - Find URMA (urma_api.h, liburma.so)
# This module defines
#  URMA_INCLUDE_DIR, directory containing urma headers
#  URMA_LIBRARY, Location of liburma's shared
#  URMA_UBAGG_LIBRARY, Location of liburma_ubagg's shared
#  URMA_UDMA_LIBRARY, Location of liburma-udma's shared
#  URMA_LIB_LOCATION, directory of liburma.so (used for packaging the 6 urma so files)
#  URMA_FOUND, whether URMA has been found
#
# When URMA_INCLUDE_LOCATION/URMA_LIB_LOCATION are set (download package mode),
# search there exclusively (NO_DEFAULT_PATH) so a system-installed URMA SDK
# does not shadow the downloaded package. Otherwise fall back to system paths.

if (URMA_INCLUDE_LOCATION)
    find_path(URMA_INCLUDE_DIR ub/umdk/urma/urma_api.h
        PATHS ${URMA_INCLUDE_LOCATION}
        NO_DEFAULT_PATH
        DOC "Path to the urma api header file (download mode)")
else()
    find_path(URMA_INCLUDE_DIR ub/umdk/urma/urma_api.h
        DOC "Path to the urma api header file")
endif()

if (URMA_LIB_LOCATION)
    find_library(URMA_LIBRARY urma
        PATHS ${URMA_LIB_LOCATION}
        NO_DEFAULT_PATH
        DOC "URMA library (download mode)")
    find_library(URMA_UBAGG_LIBRARY urma_ubagg
        PATHS ${URMA_LIB_LOCATION}
        NO_DEFAULT_PATH
        DOC "URMA ubagg library (download mode)")
    find_library(URMA_UDMA_LIBRARY urma-udma
        PATHS ${URMA_LIB_LOCATION}
        NO_DEFAULT_PATH
        DOC "URMA udma library (download mode)")
else()
    find_library(URMA_LIBRARY urma
        DOC "URMA library")
    find_library(URMA_UBAGG_LIBRARY urma_ubagg
        PATHS /usr/lib64/urma
        DOC "URMA ubagg library")
    find_library(URMA_UDMA_LIBRARY urma-udma
        DOC "URMA udma library")
endif()

# Derive URMA_LIB_LOCATION from the resolved library if not already set (system mode)
if (URMA_LIBRARY AND NOT URMA_LIB_LOCATION)
    get_filename_component(URMA_LIB_LOCATION ${URMA_LIBRARY} DIRECTORY)
    set(URMA_LIB_LOCATION ${URMA_LIB_LOCATION} CACHE PATH "Directory containing URMA shared libraries")
endif()

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(URMA REQUIRED_VARS URMA_LIBRARY URMA_UBAGG_LIBRARY URMA_UDMA_LIBRARY URMA_INCLUDE_DIR)

message(STATUS "URMA_LIBRARY=${URMA_LIBRARY}")
message(STATUS "URMA_UBAGG_LIBRARY=${URMA_UBAGG_LIBRARY}")
message(STATUS "URMA_UDMA_LIBRARY=${URMA_UDMA_LIBRARY}")
message(STATUS "URMA_INCLUDE_DIR=${URMA_INCLUDE_DIR}")
message(STATUS "URMA_LIB_LOCATION=${URMA_LIB_LOCATION}")
