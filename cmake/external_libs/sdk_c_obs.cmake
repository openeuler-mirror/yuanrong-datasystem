set(obs_VERSION 3.24.3)
if (NOT "$ENV{DS_LOCAL_LIBS_DIR}" STREQUAL "")
  set(obs_URL "$ENV{DS_LOCAL_LIBS_DIR}/opensource_third_party/v3.24.3.zip")
else()
  set(obs_URL "https://gitee.com/mirrors/huaweicloud-sdk-c-obs/repository/archive/v3.24.3.zip")
endif()
set(obs_SHA256 "ed744c92494d2d7fd171e776abfef627ac7bdd0e24910a207e83d3887cafb2bd")

set(obs_CMAKE_OPTIONS
    -DCMAKE_BUILD_TYPE:STRING=Release
    -DCURL_ROOT_DIR::PATH=${CURL_ROOT}
    -DSECUREC_ROOT_DIR:PATH=${SecureC_ROOT}
    -DOPENSSL_ROOT_DIR=${OpenSSL_ROOT}
    -DICONV_ROOT_DIR=${ICONV_ROOT}
    -DXML2_ROOT_DIR=${XML2_ROOT}
    -DPCRE_ROOT_DIR=${PCRE_ROOT}
    -DCJSON_ROOT_DIR=${CJSON_ROOT}
    -DZLIB_ROOT=${ZLIB_ROOT}
    -DSPDLOG_ROOT_DIR=${SPDLOG_ROOT})
  
set(obs_CXX_FLAGS ${THIRDPARTY_SAFE_FLAGS})
set(obs_C_FLAGS ${THIRDPARTY_SAFE_FLAGS})
set(obs_LINK_FLAGS ${THIRDPARTY_SAFE_FLAGS})

set(obs_PATCHES ${CMAKE_SOURCE_DIR}/third_party/patches/obs/3.24.3/obs-sdk-cmake-install.patch)

add_thirdparty_lib(OBS 
  URL ${obs_URL}
  SHA256 ${obs_SHA256}
  FAKE_SHA256 ${obs_FAKE_SHA256}
  VERSION ${obs_VERSION}
  CONF_OPTIONS ${obs_CMAKE_OPTIONS}
  CXX_FLAGS ${obs_CXX_FLAGS}
  C_FLAGS ${obs_C_FLAGS}
  LINK_FLAGS ${obs_LINK_FLAGS}
  PATCHES ${obs_PATCHES})

set(OBS_DIR ${OBS_ROOT})
find_package(OBS ${obs_VERSION} REQUIRED)
include_directories(${OBS_INCLUDE_DIR})