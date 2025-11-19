set(curl_VERSION 8.8.0)
if (NOT "$ENV{DS_LOCAL_LIBS_DIR}" STREQUAL "")
  set(curl_URL "$ENV{DS_LOCAL_LIBS_DIR}/opensource_third_party/curl-8_8_0.zip")
else()
  set(curl_URL "https://gitee.com/mirrors/curl/repository/archive/curl-8_8_0.zip")
endif()
set(curl_SHA256 "73c70c94f487c5ae26f9f27094249e40bb1667ae6c0406a75c3b11f86f0c1128")

set(curl_CMAKE_OPTIONS
    -DCMAKE_CXX_STANDARD=11
    -DOPENSSL_ROOT_DIR:PATH=${OpenSSL_ROOT})

set(curl_C_FLAGS ${THIRDPARTY_SAFE_FLAGS})

if (curl_VERSION STREQUAL "8.8.0")
  set(curl_PATCHES
    ${CMAKE_SOURCE_DIR}/third_party/patches/curl/8.8.0/Backport-CVE-2024-6197-fix-CVE-2024-6197-for-curl-8.8.0-c.patch
    ${CMAKE_SOURCE_DIR}/third_party/patches/curl/8.8.0/Backport-CVE-2024-6874-fix-CVE-2024-6874-for-curl-8.8.0-c.patch
    ${CMAKE_SOURCE_DIR}/third_party/patches/curl/8.8.0/Backport-CVE-2024-7264-fix-CVE-2024-7264-for-curl-8.8.0-c.patch
    ${CMAKE_SOURCE_DIR}/third_party/patches/curl/8.8.0/Backport-CVE-2024-8096-fix-CVE-2024-8096-for-curl-8.8.0-c.patch
    ${CMAKE_SOURCE_DIR}/third_party/patches/curl/8.8.0/Backport-CVE-2024-9681-fix-CVE-2024-9681-for-curl-8.8.0-c.patch
    ${CMAKE_SOURCE_DIR}/third_party/patches/curl/8.8.0/Backport-CVE-2024-11053-fix-CVE-2024-11053-for-curl-8.8.0-c.patch
    ${CMAKE_SOURCE_DIR}/third_party/patches/curl/8.8.0/Backport-CVE-2025-0167-fix-CVE-2025-0167-for-curl-8.8.0-c.patch
    ${CMAKE_SOURCE_DIR}/third_party/patches/curl/8.8.0/Backport-CVE-2025-0725-fix-CVE-2025-0725-for-curl-8.8.0-c.patch
    ${CMAKE_SOURCE_DIR}/third_party/patches/curl/8.8.0/support_old_cmake.patch
  )
endif()

add_thirdparty_lib(CURL
        URL ${curl_URL}
        SHA256 ${curl_SHA256}
        FAKE_SHA256 ${curl_FAKE_SHA256}
        VERSION ${curl_VERSION}
        CONF_OPTIONS ${curl_CMAKE_OPTIONS}
        C_FLAGS ${curl_C_FLAGS}
        PATCHES ${curl_PATCHES})

find_package(CURL ${curl_VERSION} REQUIRED PATHS ${CURL_ROOT} CONFIG)