set(spdlog_VERSION 1.12.0)
if (NOT "$ENV{DS_LOCAL_LIBS_DIR}" STREQUAL "")
  set(spdlog_URL "$ENV{DS_LOCAL_LIBS_DIR}/opensource_third_party/v1.12.0.zip")
else()
  set(spdlog_URL "https://gitee.com/mirror-luyi/spdlog/repository/archive/v1.12.0.zip")
endif()
set(spdlog_SHA256 "a0a22ed8e4932cf5f7abc354fc96d20217a8b45471e732d5e672cc01979fe588")

set(spdlog_CXX_FLAGS ${THIRDPARTY_SAFE_FLAGS})
set(spdlog_LINK_FLAGS ${THIRDPARTY_SAFE_FLAGS})

set(spdlog_CMAKE_OPTIONS
    -DCMAKE_BUILD_TYPE:STRING=Release
    -DSPDLOG_BUILD_SHARED:BOOL=ON)

set(spdlog_PATCHES 
    ${CMAKE_SOURCE_DIR}/third_party/patches/spdlog/change-filename.patch 
    ${CMAKE_SOURCE_DIR}/third_party/patches/spdlog/change-rotating-file-sink.patch)

add_thirdparty_lib(SPDLOG
    URL ${spdlog_URL}
    SHA256 ${spdlog_SHA256}
    FAKE_SHA256 ${spdlog_FAKE_SHA256}
    VERSION ${spdlog_VERSION}
    CXX_FLAGS ${spdlog_CXX_FLAGS}
    LINK_FLAGS ${spdlog_LINK_FLAGS}
    CONF_OPTIONS ${spdlog_CMAKE_OPTIONS}
    PATCHES ${spdlog_PATCHES})

find_library(SPDLOG
    NAMES ds-spdlog
    PATHS ${SPDLOG_ROOT}
    PATH_SUFFIXES lib lib64
    REQUIRED
    NO_DEFAULT_PATH)

set(CMAKE_PREFIX_PATH ${SPDLOG_ROOT})
find_package(spdlog ${spdlog_VERSION} REQUIRED)
get_property(spdlog_INCLUDE_DIR TARGET spdlog::spdlog PROPERTY INTERFACE_INCLUDE_DIRECTORIES)
include_directories(SYSTEM ${spdlog_INCLUDE_DIR})
