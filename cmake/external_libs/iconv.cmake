set(iconv_VERSION 1.15)
if (NOT "$ENV{DS_LOCAL_LIBS_DIR}" STREQUAL "")
  set(iconv_URL "$ENV{DS_LOCAL_LIBS_DIR}/opensource_third_party/libiconv-1.15.tar.gz")
else()
  set(iconv_URL "https://mirrors.aliyun.com/gnu/libiconv/libiconv-1.15.tar.gz")
endif()
set(iconv_SHA256 "ccf536620a45458d26ba83887a983b96827001e92a13847b45e4925cc8913178")

set(iconv_C_FLAGS ${THIRDPARTY_SAFE_FLAGS})
set(iconv_CXX_FLAGS ${THIRDPARTY_SAFE_FLAGS})
set(iconv_LINK_FLAGS ${THIRDPARTY_SAFE_FLAGS})
set(iconv_OPTIONS
  --enable-shared
  --disable-static)

add_thirdparty_lib(ICONV
  URL ${iconv_URL}
  SHA256 ${iconv_SHA256}
  FAKE_SHA256 ${iconv_FAKE_SHA256}
  VERSION ${iconv_VERSION}
  CONF_OPTIONS ${iconv_OPTIONS}
  C_FLAGS ${iconv_C_FLAGS}
  CXX_FLAGS ${iconv_CXX_FLAGS}
  LINK_FLAGS ${iconv_LINK_FLAGS}
  TOOLCHAIN configure
  PRE_CONFIGURE ${iconv_AUTOGEN})

find_library(ICONV
    NAMES iconv
    PATHS ${ICONV_ROOT}
    PATH_SUFFIXES lib lib64
    REQUIRED
    NO_DEFAULT_PATH)
