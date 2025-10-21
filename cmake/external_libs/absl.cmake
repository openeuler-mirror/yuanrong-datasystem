set(absl_COMMIT_ID "20240722")
set(absl_VERSION 20240722)
if (NOT "$ENV{DS_LOCAL_LIBS_DIR}" STREQUAL "")
    set(absl_URL "$ENV{DS_LOCAL_LIBS_DIR}/opensource_third_party/lts_2024_07_22.zip")
else()
    set(absl_URL "https://gitee.com/mirrors/abseil-cpp/repository/archive/lts_2024_07_22.zip")
endif()
set(absl_SHA256 "c1e391c517790669dfcbbfda1278a61053679c303c0fb05018bf2266197f054e")

set(absl_CMAKE_OPTIONS
        -DCMAKE_BUILD_TYPE:STRING=Release
        -DCMAKE_POSITION_INDEPENDENT_CODE:BOOL=TRUE
        -DCMAKE_CXX_STANDARD=17)

set(absl_CXX_FLAGS ${THIRDPARTY_SAFE_FLAGS})

set(absl_PATCHES ${CMAKE_SOURCE_DIR}/third_party/patches/absl/absl_failure_signal_handler.patch)

add_thirdparty_lib(absl
        URL ${absl_URL}
        SHA256 ${absl_SHA256}
        FAKE_SHA256 ${absl_FAKE_SHA256}
        VERSION ${absl_VERSION}
        CONF_OPTIONS ${absl_CMAKE_OPTIONS}
        CXX_FLAGS ${absl_CXX_FLAGS}
        PATCHES ${absl_PATCHES}
        )

set(absl_DIR ${absl_ROOT})
find_package(absl REQUIRED)
get_property(absl_INCLUDE_DIR TARGET absl::base PROPERTY INTERFACE_INCLUDE_DIRECTORIES)
include_directories(${absl_INCLUDE_DIR})

if (EXISTS ${absl_ROOT}/lib64)
    set(absl_PKG_PATH ${absl_ROOT}/lib64/cmake/absl)
else()
    set(absl_PKG_PATH ${absl_ROOT}/lib/cmake/absl)
endif()

if (DEFINED EXPORT_TO_USER_ENV_FILE)
  file(APPEND "${EXPORT_TO_USER_ENV_FILE}"
    "set(absl_PKG_PATH ${absl_PKG_PATH})" "\n"
    )
endif()
