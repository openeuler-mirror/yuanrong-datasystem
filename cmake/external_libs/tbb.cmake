set(tbb_VERSION 2020.3-5.oe2203sp1)
if (NOT "$ENV{DS_LOCAL_LIBS_DIR}" STREQUAL "")
  set(tbb_URL "$ENV{DS_LOCAL_LIBS_DIR}/opensource_third_party/v2020.3.zip")
else()
  set(tbb_URL "https://gitee.com/mirrors/tbb/repository/archive/v2020.3.zip")
endif()
set(tbb_SHA256 "58654b47f145e619cddfdd1fdf2dd36b6db85d7e644bce8f2b278eed8b1b28b7")

if ("${tbb_VERSION}" STREQUAL "2021.5.0")
  set(tbb_TOOLCHAIN "cmake")
else ()
  set(tbb_TOOLCHAIN "make")
endif ()

string(FIND "${tbb_VERSION}" "2020.3" tbb_need_patch)
if (NOT found EQUAL -1)
  set(tbb_PATCHES ${CMAKE_SOURCE_DIR}/third_party/patches/tbb/2020.3/soft-link.patch)
endif ()

set(tbb_CMAKE_OPTIONS
    -DTBB_TEST:BOOL=OFF
    -DCMAKE_BUILD_TYPE:STRING=Release)

set(tbb_CXX_FLAGS ${THIRDPARTY_SAFE_FLAGS})

set(tbb_LINK_FLAGS "-Wl,-z,relro,-z,now")

add_thirdparty_lib(TBB 
  URL ${tbb_URL}
  SHA256 ${tbb_SHA256}
  FAKE_SHA256 ${tbb_FAKE_SHA256}
  VERSION ${tbb_VERSION}
  CONF_OPTIONS ${tbb_CMAKE_OPTIONS}
  CXX_FLAGS ${tbb_CXX_FLAGS}
  LINK_FLAGS ${tbb_LINK_FLAGS}
  TOOLCHAIN ${tbb_TOOLCHAIN}
  PATCHES ${tbb_PATCHES})

set(TBB_DIR ${TBB_ROOT})
find_package(TBB REQUIRED)

if (EXISTS "${TBB_INCLUDE_DIR}/oneapi/tbb/version.h")
  file(STRINGS
    "${TBB_INCLUDE_DIR}/oneapi/tbb/version.h"
    TBB_VERSION_CONTENTS
    REGEX "VERSION")
else()
  #only read the start of the file
  file(STRINGS
    "${TBB_INCLUDE_DIR}/tbb/tbb_stddef.h"
    TBB_VERSION_CONTENTS
    REGEX "VERSION")
endif()

string(REGEX REPLACE
      ".*#define TBB_INTERFACE_VERSION ([0-9]+).*" "\\1"
      TBB_INTERFACE_VERSION "${TBB_VERSION_CONTENTS}")
  
add_compile_definitions(TBB_INTERFACE_VERSION=${TBB_INTERFACE_VERSION})
include_directories(SYSTEM ${TBB_INCLUDE_DIR})