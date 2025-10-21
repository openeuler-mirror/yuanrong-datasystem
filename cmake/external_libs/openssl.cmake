set(openssl_VERSION 1.1.1wa)
if (NOT "$ENV{DS_LOCAL_LIBS_DIR}" STREQUAL "")
    set(openssl_URL "$ENV{DS_LOCAL_LIBS_DIR}/opensource_third_party/OpenSSL_1_1_1wa.tar.gz")
else()
  set(openssl_URL "https://gitee.com/openeuler/openssl/archive/refs/tags/OpenSSL_1_1_1wa.tar.gz")
endif()
set(openssl_SHA256 "09bcab2dfb4a10e425763bafe60fd253ef37a0893877cdc3982d8bbeaaaa8881")

set(openssl_CXX_FLAGS ${THIRDPARTY_SAFE_FLAGS})

set(openssl_C_FLAGS ${THIRDPARTY_SAFE_FLAGS})

set(openssl_OPTIONS shared enable-ssl3 enable-ssl3-method no-buildtest-c++)

add_thirdparty_lib(OpenSSL
  URL ${openssl_URL}
  SHA256 ${openssl_SHA256}
  FAKE_SHA256 ${openssl_FAKE_SHA256}
  VERSION ${openssl_VERSION}
  TOOLCHAIN configure
  CONF_OPTIONS ${openssl_OPTIONS}
  CXX_FLAGS ${openssl_CXX_FLAGS}
  C_FLAGS ${openssl_C_FLAGS}
  PATCHES ${openssl_PATCHES})

set(OPENSSL_ROOT_DIR "${OpenSSL_ROOT}")
find_package(OpenSSL REQUIRED)
include_directories(SYSTEM ${OPENSSL_INCLUDE_DIR})