set(pcre_VERSION 8.45)
if (NOT "$ENV{DS_LOCAL_LIBS_DIR}" STREQUAL "")
  set(pcre_URL "$ENV{DS_LOCAL_LIBS_DIR}/opensource_third_party/pcre-8.45.tar.gz")
else()
  set(pcre_URL "https://nchc.dl.sourceforge.net/project/pcre/pcre/8.45/pcre-8.45.tar.gz")
endif()
set(pcre_SHA256 "4e6ce03e0336e8b4a3d6c2b70b1c5e18590a5673a98186da90d4f33c23defc09")

set(pcre_OPTIONS
  --enable-shared
  --disable-static)

set(pcre_C_FLAGS ${THIRDPARTY_SAFE_FLAGS})
set(pcre_CXX_FLAGS ${THIRDPARTY_SAFE_FLAGS})
set(pcre_LINK_FLAGS ${THIRDPARTY_SAFE_FLAGS})

add_thirdparty_lib(PCRE
  URL ${pcre_URL}
  SHA256 ${pcre_SHA256}
  FAKE_SHA256 ${pcre_FAKE_SHA256}
  VERSION ${pcre_VERSION}
  CONF_OPTIONS ${pcre_OPTIONS}
  C_FLAGS ${pcre_C_FLAGS}
  CXX_FLAGS ${pcre_CXX_FLAGS}
  LINK_FLAGS ${pcre_LINK_FLAGS}
  TOOLCHAIN configure)

find_library(PCRE
    NAMES pcre
    PATHS ${PCRE_ROOT}
    PATH_SUFFIXES lib lib64
    REQUIRED
    NO_DEFAULT_PATH)
