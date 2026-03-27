if (TARGET absl::base)
    get_property(_TRANSFER_ENGINE_ABSL_INCLUDE_DIR TARGET absl::base PROPERTY INTERFACE_INCLUDE_DIRECTORIES)
    set(TRANSFER_ENGINE_ABSL_INCLUDE_DIR "${_TRANSFER_ENGINE_ABSL_INCLUDE_DIR}" CACHE INTERNAL "")
    return()
endif()

set(absl_VERSION 20240722)
if (NOT "$ENV{DS_LOCAL_LIBS_DIR}" STREQUAL "")
    set(absl_URL "$ENV{DS_LOCAL_LIBS_DIR}/opensource_third_party/lts_2024_07_22.zip")
else()
    set(absl_URL "https://gitee.com/mirrors/abseil-cpp/repository/archive/lts_2024_07_22.zip")
endif()
set(absl_SHA256 "cf9f05c6e3216aa49d2fb3e0935ce736b1440e7879e1fde626f57f6610aa8b95")
set(absl_PATCHES
    "${CMAKE_CURRENT_LIST_DIR}/../../third_party/patches/absl/absl_failure_signal_handler.patch")

set(absl_CMAKE_OPTIONS
    -DCMAKE_BUILD_TYPE=Release
    -DBUILD_SHARED_LIBS=OFF
    -DABSL_PROPAGATE_CXX_STD=ON
    -DABSL_BUILD_TESTING=OFF
    -DABSL_ENABLE_INSTALL=ON)

TE_ADD_THIRDPARTY_LIB(absl
    URL ${absl_URL}
    SHA256 ${absl_SHA256}
    VERSION ${absl_VERSION}
    CONF_OPTIONS ${absl_CMAKE_OPTIONS}
    CXX_FLAGS ${TRANSFER_ENGINE_THIRDPARTY_SAFE_FLAGS}
    PATCHES ${absl_PATCHES})

set(absl_DIR "${absl_ROOT}/lib/cmake/absl")
if (EXISTS "${absl_ROOT}/lib64/cmake/absl")
    set(absl_DIR "${absl_ROOT}/lib64/cmake/absl")
endif()
find_package(absl REQUIRED PATHS "${absl_DIR}" NO_DEFAULT_PATH)
get_property(_TRANSFER_ENGINE_ABSL_INCLUDE_DIR TARGET absl::base PROPERTY INTERFACE_INCLUDE_DIRECTORIES)
set(TRANSFER_ENGINE_ABSL_ROOT "${absl_ROOT}" CACHE INTERNAL "")
set(TRANSFER_ENGINE_ABSL_LIB_PATH "${absl_LIB_PATH}" CACHE INTERNAL "")
set(TRANSFER_ENGINE_ABSL_INCLUDE_DIR "${_TRANSFER_ENGINE_ABSL_INCLUDE_DIR}" CACHE INTERNAL "")
