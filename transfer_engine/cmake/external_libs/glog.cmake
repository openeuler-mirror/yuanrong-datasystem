if (TARGET glog::glog)
    return()
endif()

set(glog_VERSION 0.7.0)
if (NOT "$ENV{DS_LOCAL_LIBS_DIR}" STREQUAL "")
    set(glog_URL "$ENV{DS_LOCAL_LIBS_DIR}/opensource_third_party/v0.7.0.zip")
else()
    set(glog_URL "https://gitee.com/mirrors/glog/repository/archive/v0.7.0.zip")
endif()
set(glog_SHA256 "713188ed17bd2f50a0967bee3b418a1750664d424f81643cfa153fc09c1e323d")

set(glog_CMAKE_OPTIONS
    -DCMAKE_BUILD_TYPE=Release
    -DBUILD_SHARED_LIBS=ON
    -DWITH_GFLAGS=OFF
    -DBUILD_TESTING=OFF)

TE_ADD_THIRDPARTY_LIB(glog
    URL ${glog_URL}
    SHA256 ${glog_SHA256}
    VERSION ${glog_VERSION}
    CONF_OPTIONS ${glog_CMAKE_OPTIONS}
    CXX_FLAGS ${TRANSFER_ENGINE_THIRDPARTY_SAFE_FLAGS})

set(glog_DIR "${glog_ROOT}/lib/cmake/glog")
if (EXISTS "${glog_ROOT}/lib64/cmake/glog")
    set(glog_DIR "${glog_ROOT}/lib64/cmake/glog")
endif()
find_package(glog REQUIRED PATHS "${glog_DIR}" NO_DEFAULT_PATH)
set(TRANSFER_ENGINE_GLOG_ROOT "${glog_ROOT}" CACHE INTERNAL "")
set(TRANSFER_ENGINE_GLOG_LIB_PATH "${glog_LIB_PATH}" CACHE INTERNAL "")
