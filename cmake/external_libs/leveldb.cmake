# leveldb 1.23 - brpc dependency (RPC tracing)
set(leveldb_VERSION 1.23)
if (NOT "$ENV{DS_LOCAL_LIBS_DIR}" STREQUAL "")
    set(leveldb_URL "$ENV{DS_LOCAL_LIBS_DIR}/opensource_third_party/1.23.tar.gz")
else()
    # Default: upstream GitHub. Set DATASYSTEM_GITHUB_PROXY to use gh-proxy.com mirror.
    if (NOT "$ENV{DATASYSTEM_GITHUB_PROXY}" STREQUAL "")
        set(leveldb_URL "https://gh-proxy.com/https://github.com/google/leveldb/archive/refs/tags/1.23.tar.gz")
    else()
        set(leveldb_URL "https://github.com/google/leveldb/archive/refs/tags/1.23.tar.gz")
    endif()
endif()
set(leveldb_SHA256 "9a37f8a6174f09bd622bc723b55881dc541cd50747cbd08831c2a82d620f6d76")

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
