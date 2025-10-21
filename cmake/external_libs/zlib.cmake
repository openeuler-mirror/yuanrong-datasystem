set(zlib_VERSION 1.3.1)
if (NOT "$ENV{DS_LOCAL_LIBS_DIR}" STREQUAL "")
  set(zlib_URL "$ENV{DS_LOCAL_LIBS_DIR}/opensource_third_party/zlib-v1.3.1.tar.gz")
else()
  set(zlib_URL "https://gitee.com/mirrors/zlib/repository/archive/v1.3.1.tar.gz")
endif()
set(zlib_SHA256 "4208eb8a8ba5703831123b06c9bbadf780e678a9972eb00ecce57e6f87b30f36")

set(zlib_CMAKE_OPTIONS
    -DCMAKE_BUILD_TYPE:STRING=Release)

set(zlib_C_FLAGS ${THIRDPARTY_SAFE_FLAGS})

if (zlib_VERSION STREQUAL "1.2.13")
  set(zlib_PATCHES
          ${CMAKE_SOURCE_DIR}/third_party/patches/zlib/Backport-CVE-2023-45853-Reject-overflows-of-zip-header-fields-in-mi-c.patch)
endif()

add_thirdparty_lib(ZLIB
  URL ${zlib_URL}
  SHA256 ${zlib_SHA256}
  FAKE_SHA256 ${zlib_FAKE_SHA256}
  VERSION ${zlib_VERSION}
  CONF_OPTIONS ${zlib_CMAKE_OPTIONS}
  C_FLAGS ${zlib_C_FLAGS}
  PATCHES ${zlib_PATCHES})

set(ZLIB_ROOT ${ZLIB_ROOT})
set(ZLIB_DIR ${ZLIB_ROOT})
find_package(ZLIB ${zlib_VERSION} REQUIRED)

include_directories(${ZLIB_INCLUDE_DIRS})