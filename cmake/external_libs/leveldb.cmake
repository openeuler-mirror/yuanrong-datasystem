# leveldb 1.23 - brpc dependency (RPC tracing)
set(leveldb_VERSION 1.23)
if (NOT "$ENV{DS_LOCAL_LIBS_DIR}" STREQUAL "")
    set(leveldb_URL "$ENV{DS_LOCAL_LIBS_DIR}/opensource_third_party/1.23.tar.gz")
else()
    set(leveldb_URL "https://gitee.com/mirrors/leveldb/repository/archive/1.23.zip")
endif()
set(leveldb_SHA256 "4ee1dab7719fb5e357854c380c0d297a5857e98102f8bff2b93639326ac030ec")

set(leveldb_CMAKE_OPTIONS
    -DCMAKE_BUILD_TYPE:STRING=Release
    -DCMAKE_POSITION_INDEPENDENT_CODE:BOOL=ON
    -DLEVELDB_BUILD_TESTS:BOOL=OFF
    -DLEVELDB_BUILD_BENCHMARKS:BOOL=OFF
    -DCMAKE_INSTALL_LIBDIR:STRING=lib)

set(leveldb_CXX_FLAGS ${THIRDPARTY_SAFE_FLAGS})
set(leveldb_C_FLAGS ${THIRDPARTY_SAFE_FLAGS})

add_thirdparty_lib(leveldb
    URL ${leveldb_URL}
    SHA256 ${leveldb_SHA256}
    FAKE_SHA256 ${leveldb_FAKE_SHA256}
    VERSION ${leveldb_VERSION}
    CONF_OPTIONS ${leveldb_CMAKE_OPTIONS}
    CXX_FLAGS ${leveldb_CXX_FLAGS}
    C_FLAGS ${leveldb_C_FLAGS})

set(leveldb_INCLUDE_DIR ${leveldb_ROOT}/include)
set(leveldb_LIB_DIR ${leveldb_ROOT}/lib)
find_library(leveldb_LIBRARY
    NAMES leveldb
    PATHS ${leveldb_LIB_DIR}
    REQUIRED
    NO_DEFAULT_PATH)
include_directories(SYSTEM ${leveldb_INCLUDE_DIR})

# CMake config dir for downstream consumers (e.g. brpc's cmake).
set(leveldb_PKG_PATH ${leveldb_LIB_DIR}/cmake/leveldb)
