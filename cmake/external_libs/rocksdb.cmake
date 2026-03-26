set(rocksdb_VERSION 7.10.2)
if (NOT "$ENV{DS_LOCAL_LIBS_DIR}" STREQUAL "")
  set(rocksdb_URL "$ENV{DS_LOCAL_LIBS_DIR}/opensource_third_party/v7.10.2.zip")
else()
  set(rocksdb_URL "https://gitee.com/mirrors/rocksdb/repository/archive/v7.10.2.zip")
endif()
set(rocksdb_SHA256 "8f655269fac91da27032914377ecb63eeab9eccc5da5d6894482115f71287e26")

set(rocksdb_CMAKE_OPTIONS
    -DCMAKE_BUILD_TYPE:STRING=Release
    -DWITH_TESTS:BOOL=OFF
    -DFAIL_ON_WARNINGS:BOOL=OFF 
    -DFORCE_SSE42:BOOL=OFF 
    -DPORTABLE:BOOL=ON
    -DUSE_RTTI:BOOL=ON
    -DWITH_GFLAGS:BOOL=OFF)

set(rocksdb_CXX_FLAGS ${THIRDPARTY_SAFE_FLAGS})

set(rocksdb_C_FLAGS ${THIRDPARTY_SAFE_FLAGS})

set(rocksdbPATCHES
    ${CMAKE_SOURCE_DIR}/third_party/patches/rocksdb/include-algorithm-for-gcc-14.patch)

add_thirdparty_lib(RocksDB
  URL ${rocksdb_URL}
  SHA256 ${rocksdb_SHA256}
  FAKE_SHA256 ${rocksdb_FAKE_SHA256}
  VERSION ${rocksdb_VERSION}
  CONF_OPTIONS ${rocksdb_CMAKE_OPTIONS}
  CXX_FLAGS ${rocksdb_CXX_FLAGS}
  C_FLAGS ${rocksdb_C_FLAGS}
  PATCHES ${rocksdbPATCHES})

set(RocksDB_DIR ${RocksDB_ROOT})
find_package(RocksDB ${rocksdb_VERSION} REQUIRED)

get_property(RocksDB_INCLUDE_DIR TARGET RocksDB::rocksdb PROPERTY INTERFACE_INCLUDE_DIRECTORIES)
include_directories(${RocksDB_INCLUDE_DIR})