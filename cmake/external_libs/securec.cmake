if (NOT "$ENV{DS_LOCAL_LIBS_DIR}" STREQUAL "")
  set(securec_URL "$ENV{DS_LOCAL_LIBS_DIR}/opensource_third_party/v1.1.16.tar.gz")
else()
  set(securec_URL "https://gitee.com/openeuler/libboundscheck/archive/refs/tags/v1.1.16.tar.gz")
endif()
set(securec_SHA256 "aee8368ef04a42a499edd5bfebce529e7f32dd138bfed383d316e48af4e45d2c")

set(securec_VERSION v1.1.16)

set(securec_C_FLAGS ${THIRDPARTY_SAFE_FLAGS})

set(securec_PATCHES ${CMAKE_SOURCE_DIR}/third_party/patches/securec/libboundscheck-cmake-support.patch)

add_thirdparty_lib(SecureC
  URL ${securec_URL}
  SHA256 ${securec_SHA256}
  VERSION ${securec_VERSION}
  C_FLAGS ${securec_C_FLAGS}
  PATCHES ${securec_PATCHES})

set(CMAKE_PREFIX_PATH ${SecureC_ROOT})
find_package(SecureC REQUIRED)

include_directories(${SECUREC_INCLUDE_DIR})