# gflags 2.2.2 - brpc dependency
set(gflags_VERSION 2.2.2)
if (NOT "$ENV{DS_LOCAL_LIBS_DIR}" STREQUAL "")
    set(gflags_URL "$ENV{DS_LOCAL_LIBS_DIR}/opensource_third_party/v2.2.2.tar.gz")
else()
    set(gflags_URL "https://gitee.com/mirrors/gflags/repository/archive/v2.2.2.zip")
endif()
set(gflags_SHA256 "dfd9d3ee0c92e92d2b550e56322d1237324e000c72306802b81c32a1231769df")

set(gflags_CMAKE_OPTIONS
    -DCMAKE_BUILD_TYPE:STRING=Release
    -DCMAKE_POSITION_INDEPENDENT_CODE:BOOL=ON
    -DGFLAGS_BUILD_STATIC_LIBS:BOOL=OFF
    -DGFLAGS_BUILD_SHARED_LIBS:BOOL=ON
    -DGFLAGS_BUILD_TESTING:BOOL=OFF
    -DGFLAGS_REGISTER_INSTALL_DIR:BOOL=OFF
    -DCMAKE_INSTALL_LIBDIR:STRING=lib)

if (KVTEST_BUILD_STATIC)
    list(APPEND gflags_CMAKE_OPTIONS
        -DGFLAGS_BUILD_STATIC_LIBS:BOOL=ON
        -DGFLAGS_BUILD_SHARED_LIBS:BOOL=OFF)
endif()

set(gflags_CXX_FLAGS ${THIRDPARTY_SAFE_FLAGS})
set(gflags_C_FLAGS ${THIRDPARTY_SAFE_FLAGS})

add_thirdparty_lib(gflags
    URL ${gflags_URL}
    SHA256 ${gflags_SHA256}
    FAKE_SHA256 ${gflags_FAKE_SHA256}
    VERSION ${gflags_VERSION}
    CONF_OPTIONS ${gflags_CMAKE_OPTIONS}
    CXX_FLAGS ${gflags_CXX_FLAGS}
    C_FLAGS ${gflags_C_FLAGS})

set(gflags_INCLUDE_DIR ${gflags_ROOT}/include)
set(gflags_LIB_DIR ${gflags_ROOT}/lib)
find_library(gflags_LIBRARY
    NAMES gflags
    PATHS ${gflags_LIB_DIR}
    REQUIRED
    NO_DEFAULT_PATH)
include_directories(SYSTEM ${gflags_INCLUDE_DIR})

# CMake config dir for downstream consumers (e.g. brpc's cmake).
set(gflags_PKG_PATH ${gflags_LIB_DIR}/cmake/gflags)
