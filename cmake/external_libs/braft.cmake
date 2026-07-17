# braft 1.1.2 - Raft consensus library built on the project's existing brpc.
set(braft_VERSION 1.1.2)
if ("$ENV{DS_PACKAGE}" STREQUAL "")
    if (NOT "$ENV{DS_LOCAL_LIBS_DIR}" STREQUAL "")
        set(braft_URL "$ENV{DS_LOCAL_LIBS_DIR}/opensource_third_party/v1.1.2.tar.gz")
    else()
        if (NOT "$ENV{DATASYSTEM_GITHUB_PROXY}" STREQUAL "")
            set(braft_URL "https://gh-proxy.com/https://github.com/baidu/braft/archive/refs/tags/v1.1.2.tar.gz")
        else()
            set(braft_URL "https://github.com/baidu/braft/archive/refs/tags/v1.1.2.tar.gz")
        endif()
    endif()
    set(braft_SHA256 "bb3705f61874f8488e616ae38464efdec1a20610ddd6cd82468adc814488f14e")
else()
    gen_thirdparty_pkg(braft braft_URL braft_SHA256 braft_FAKE_SHA256 braft_VERSION)
endif()

set(braft_EXTRA_MSGS
    ${brpc_ROOT}
    ${gflags_ROOT}
    ${leveldb_ROOT}
    ${Protobuf_ROOT}
    ${OPENSSL_ROOT_DIR}
    ${ZLIB_ROOT})

if (EXISTS ${Protobuf_ROOT}/lib64/libprotobuf.so)
    set(_BRAFT_PROTOBUF_LIB ${Protobuf_ROOT}/lib64/libprotobuf.so)
else()
    set(_BRAFT_PROTOBUF_LIB ${Protobuf_ROOT}/lib/libprotobuf.so)
endif()
set(_BRAFT_PROTOC_EXECUTABLE ${Protobuf_ROOT}/bin/protoc)

set(braft_CMAKE_OPTIONS
    -DCMAKE_BUILD_TYPE:STRING=Release
    -DCMAKE_POSITION_INDEPENDENT_CODE:BOOL=ON
    -DBRPC_WITH_GLOG:BOOL=OFF
    -DBUILD_UNIT_TESTS:BOOL=OFF
    -DBUILD_BRAFT_TOOLS:BOOL=OFF
    -DBRAFT_REVISION:STRING=v${braft_VERSION}
    -DWITH_DEBUG_SYMBOLS:BOOL=OFF
    -DLEVELDB_WITH_SNAPPY:BOOL=OFF
    -DGFLAGS_INCLUDE_PATH:PATH=${gflags_INCLUDE_DIR}
    -DGFLAGS_LIB:FILEPATH=${gflags_LIBRARY}
    -DLEVELDB_INCLUDE_PATH:PATH=${leveldb_INCLUDE_DIR}
    -DLEVELDB_LIB:FILEPATH=${leveldb_LIBRARY}
    -DBRPC_INCLUDE_PATH:PATH=${BRPC_INCLUDE_DIR}
    -DBRPC_LIB:FILEPATH=${BRPC_LIBRARY}
    -DPROTOBUF_INCLUDE_DIR:PATH=${Protobuf_INCLUDE_DIR}
    -DPROTOBUF_INCLUDE_DIRS:PATH=${Protobuf_INCLUDE_DIR}
    -DPROTOBUF_LIBRARY:FILEPATH=${_BRAFT_PROTOBUF_LIB}
    -DPROTOBUF_LIBRARIES:FILEPATH=${_BRAFT_PROTOBUF_LIB}
    -DPROTOBUF_PROTOC_EXECUTABLE:FILEPATH=${_BRAFT_PROTOC_EXECUTABLE}
    -DOPENSSL_ROOT_DIR:PATH=${OPENSSL_ROOT_DIR}
    -DZLIB_INCLUDE_PATH:PATH=${ZLIB_INCLUDE_DIRS}
    -DZLIB_LIB:FILEPATH=${ZLIB_LIBRARIES})

set(braft_CXX_FLAGS "${THIRDPARTY_SAFE_FLAGS} -I${absl_INCLUDE_DIR}")
set(braft_C_FLAGS ${THIRDPARTY_SAFE_FLAGS})
set(braft_PATCHES
    ${CMAKE_SOURCE_DIR}/third_party/patches/braft/modern-toolchain-compat.patch)

set(_BRAFT_PROTOBUF_LIB_DIR ${Protobuf_ROOT}/lib)
if (EXISTS ${Protobuf_ROOT}/lib64)
    set(_BRAFT_PROTOBUF_LIB_DIR ${Protobuf_ROOT}/lib64)
endif()
set(_BRAFT_SAVED_LD_LIBRARY_PATH "$ENV{LD_LIBRARY_PATH}")
set(ENV{LD_LIBRARY_PATH} "${_BRAFT_PROTOBUF_LIB_DIR}:$ENV{LD_LIBRARY_PATH}")

add_thirdparty_lib(braft
    URL ${braft_URL}
    SHA256 ${braft_SHA256}
    FAKE_SHA256 ${braft_FAKE_SHA256}
    VERSION ${braft_VERSION}
    CONF_OPTIONS ${braft_CMAKE_OPTIONS}
    CXX_FLAGS ${braft_CXX_FLAGS}
    C_FLAGS ${braft_C_FLAGS}
    PATCHES ${braft_PATCHES}
    EXTRA_MSGS ${braft_EXTRA_MSGS})

set(ENV{LD_LIBRARY_PATH} "${_BRAFT_SAVED_LD_LIBRARY_PATH}")

set(BRAFT_INCLUDE_DIR ${braft_ROOT}/include)
set(BRAFT_LIB_DIR ${braft_LIB_PATH})
unset(BRAFT_LIBRARY CACHE)
find_library(BRAFT_LIBRARY
    NAMES braft
    PATHS ${BRAFT_LIB_DIR}
    REQUIRED
    NO_DEFAULT_PATH)
include_directories(SYSTEM ${BRAFT_INCLUDE_DIR})

set(BRAFT_LIBRARIES
    ${BRAFT_LIBRARY}
    ${BRPC_LIBRARIES}
    OpenSSL::SSL
    OpenSSL::Crypto
    Threads::Threads
    ${CMAKE_DL_LIBS}
    ZLIB::ZLIB
    rt)

if (DEFINED EXPORT_TO_USER_ENV_FILE)
    file(APPEND "${EXPORT_TO_USER_ENV_FILE}"
        "set(BRAFT_INCLUDE_DIR ${BRAFT_INCLUDE_DIR})" "\n"
        "set(BRAFT_LIB_DIR ${BRAFT_LIB_DIR})" "\n")
endif()
