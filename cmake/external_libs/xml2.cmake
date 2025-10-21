set(xml2_VERSION 2.9.12)
if (NOT "$ENV{DS_LOCAL_LIBS_DIR}" STREQUAL "")
  set(xml2_URL "$ENV{DS_LOCAL_LIBS_DIR}/opensource_third_party/v2.9.12.zip")
else()
  set(xml2_URL "https://gitee.com/mirrors/libxml2/repository/archive/v2.9.12.zip")
endif()
set(xml2_SHA256 "e3e25e5357e896e9c5ba368d158d908027b1a84d2f700ecab60746af8c3c2bf7")

set(xml2_C_FLAGS ${THIRDPARTY_SAFE_FLAGS})
set(xml2_CXX_FLAGS ${THIRDPARTY_SAFE_FLAGS})
set(xml2_LINK_FLAGS ${THIRDPARTY_SAFE_FLAGS})
set(xml2_OPTIONS
  --enable-shared
  --disable-static
  --with-python=no)

set(xml2_AUTOGEN sh autogen.sh)

add_thirdparty_lib(XML2
  URL ${xml2_URL}
  SHA256 ${xml2_SHA256}
  FAKE_SHA256 ${xml2_FAKE_SHA256}
  VERSION ${xml2_VERSION}
  CONF_OPTIONS ${xml2_OPTIONS}
  C_FLAGS ${xml2_C_FLAGS}
  CXX_FLAGS ${xml2_CXX_FLAGS}
  LINK_FLAGS ${xml2_LINK_FLAGS}
  TOOLCHAIN configure
  PRE_CONFIGURE ${xml2_AUTOGEN})

find_library(XML2
    NAMES xml2
    PATHS ${XML2_ROOT}
    PATH_SUFFIXES lib lib64
    REQUIRED
    NO_DEFAULT_PATH)