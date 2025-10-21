# re2
set(re2_VERSION 2024-07-02)
if ("$ENV{DS_PACKAGE}" STREQUAL "")
  if (NOT "$ENV{DS_LOCAL_LIBS_DIR}" STREQUAL "")
    set(re2_URL "$ENV{DS_LOCAL_LIBS_DIR}/opensource_third_party/2024-07-02.zip")
  else()
    set(re2_URL "https://gitee.com/mirrors/re2/repository/archive/2024-07-02.zip")
  endif()
  set(re2_SHA256 "20f5af5320da5a704125eaec5965ddc0cfa86fb420575a9f9f04c5cef905ba93")
else()
  gen_thirdparty_pkg(re2 re2_URL re2_SHA256 re2_FAKE_SHA256 re2_VERSION)
endif()

if (EXISTS ${absl_ROOT}/lib64)
    set(absl_PKG_PATH ${absl_ROOT}/lib64/cmake/absl)
else()
    set(absl_PKG_PATH ${absl_ROOT}/lib/cmake/absl)
endif()

set(re2_CMAKE_OPTIONS
    -DCMAKE_BUILD_TYPE:STRING=Release
    -DCMAKE_POSITION_INDEPENDENT_CODE:BOOL=TRUE
    -DRE2_BUILD_TESTING:BOOL=OFF
    -DCMAKE_CXX_STANDARD=17
    -Dabsl_DIR:PATH=${absl_PKG_PATH})

set(re2_CXX_FLAGS ${THIRDPARTY_SAFE_FLAGS})

add_thirdparty_lib(re2 
  URL ${re2_URL}
  SHA256 ${re2_SHA256}
  FAKE_SHA256 ${re2_FAKE_SHA256}
  VERSION ${re2_VERSION}
  CONF_OPTIONS ${re2_CMAKE_OPTIONS}
  CXX_FLAGS ${re2_CXX_FLAGS})

if (EXISTS ${re2_ROOT}/lib64)
  set(re2_PKG_PATH ${re2_ROOT}/lib64/cmake/re2)
else()
  set(re2_PKG_PATH ${re2_ROOT}/lib/cmake/re2)
endif()

if (DEFINED EXPORT_TO_USER_ENV_FILE)
  file(APPEND "${EXPORT_TO_USER_ENV_FILE}"
    "set(re2_PKG_PATH ${re2_PKG_PATH})" "\n"
    )
endif()

find_package(re2 REQUIRED)
get_property(re2_INCLUDE_DIR TARGET re2::re2 PROPERTY INTERFACE_INCLUDE_DIRECTORIES)
include_directories(${re2_INCLUDE_DIR})