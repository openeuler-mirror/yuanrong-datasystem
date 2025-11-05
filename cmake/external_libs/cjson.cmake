set(cjson_VERSION 1.7.17)
if (NOT "$ENV{DS_LOCAL_LIBS_DIR}" STREQUAL "")
  set(cjson_URL "$ENV{DS_LOCAL_LIBS_DIR}/opensource_third_party/v1.7.17.zip")
else()
  set(cjson_URL "https://gitee.com/mirrors/cJSON/repository/archive/v1.7.17.zip")
endif()
set(cjson_SHA256 "6edd31f4faa373dc94b26e4ab6b4483d32fbfc003771a6d0e1a9106e0d9521f2")

set(cjson_C_FLAGS ${THIRDPARTY_SAFE_FLAGS})
set(cjson_LINK_FLAGS ${THIRDPARTY_SAFE_FLAGS})
if ("$ENV{DS_PACKAGE}" STREQUAL "")
  set(cjson_PATCHES ${CMAKE_SOURCE_DIR}/third_party/patches/cjson/1.7.17/Backport-CVE-2024-31755-Add-NULL-check-to-cJSON_SetValuestring-c.patch)
endif()

add_thirdparty_lib(CJSON
  URL ${cjson_URL}
  SHA256 ${cjson_SHA256}
  FAKE_SHA256 ${cjson_FAKE_SHA256}
  VERSION ${cjson_VERSION}
  C_FLAGS ${cjson_C_FLAGS}
  LINK_FLAGS ${cjson_LINK_FLAGS}
  PATCHES ${cjson_PATCHES})

find_library(CJSON
    NAMES cjson
    PATHS ${CJSON_ROOT}
    PATH_SUFFIXES lib lib64
    REQUIRED
    NO_DEFAULT_PATH)