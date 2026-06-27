# brpc 1.15.0 - dual-mode RPC (alongside ZMQ)
# Depends on: gflags, leveldb, protobuf, openssl (all provided by this project's third-party tree).
set(brpc_VERSION 1.15.0)
if (NOT "$ENV{DS_LOCAL_LIBS_DIR}" STREQUAL "")
    set(brpc_URL "$ENV{DS_LOCAL_LIBS_DIR}/opensource_third_party/1.15.0.tar.gz")
else()
    set(brpc_URL "https://github.com/apache/brpc/archive/refs/tags/1.15.0.tar.gz")
endif()
set(brpc_SHA256 "f674b753af71dc313d9d2dcf34f574f0a3438c9f9bb9e7e6ca500a3b0ca7ddfb")

# Force cache invalidation when any of brpc's dependencies move.
set(brpc_EXTRA_MSGS
    ${gflags_ROOT}
    ${leveldb_ROOT}
    ${Protobuf_ROOT}
    ${OPENSSL_ROOT_DIR})

# Tell brpc's cmake to find the project's own third-party installs of gflags/leveldb/protobuf/openssl
# so brpc links against the exact same versions as the rest of the project (avoid ABI mismatches).
# The project's installed protobuf exposes protobuf-config.cmake in lib64/cmake/protobuf; without
# pointing brpc at it, brpc's find_package(Protobuf) finds the system /usr/include protobuf 3.14
# which is ABI-incompatible with the project's 4.25.5.
if (EXISTS ${Protobuf_ROOT}/lib64/cmake/protobuf)
    set(_BRPC_PROTOBUF_CMAKE_DIR ${Protobuf_ROOT}/lib64/cmake/protobuf)
else()
    set(_BRPC_PROTOBUF_CMAKE_DIR ${Protobuf_ROOT}/lib/cmake/protobuf)
endif()
set(_BRPC_PREFIX_PATH
    "${gflags_ROOT}"
    "${gflags_PKG_PATH}"
    "${leveldb_ROOT}"
    "${leveldb_PKG_PATH}"
    "${_BRPC_PROTOBUF_CMAKE_DIR}"
    "${absl_PKG_PATH}"
    "${OPENSSL_ROOT_DIR}")

# brpc regenerates its *.pb.h via PROTOBUF_PROTOC_EXECUTABLE inside its own cmake; if it falls
# back to the system /usr/bin/protoc (here 3.14, incompatible with the project's protobuf 4.25.5
# headers) the installed brpc/*.pb.h files end up using removed macros like PROTOBUF_NAMESPACE_OPEN
# and fail to compile downstream. Pin it to the project's protoc explicitly.
set(_BRPC_PROTOC_EXECUTABLE ${Protobuf_ROOT}/bin/protoc)
if (EXISTS ${Protobuf_ROOT}/lib64/libprotoc.so)
    set(_BRPC_PROTOC_LIB ${Protobuf_ROOT}/lib64/libprotoc.so)
else()
    set(_BRPC_PROTOC_LIB ${Protobuf_ROOT}/lib/libprotoc.so)
endif()
if (EXISTS ${Protobuf_ROOT}/lib64/libprotobuf.so)
    set(_BRPC_PROTOBUF_LIB ${Protobuf_ROOT}/lib64/libprotobuf.so)
else()
    set(_BRPC_PROTOBUF_LIB ${Protobuf_ROOT}/lib/libprotobuf.so)
endif()

set(brpc_CMAKE_OPTIONS
    -DCMAKE_BUILD_TYPE:STRING=Release
    -DCMAKE_POSITION_INDEPENDENT_CODE:BOOL=ON
    -DBUILD_SHARED_LIBS:BOOL=ON
    -DWITH_BRPC_SHARED_LIB:BOOL=ON
    -DWITH_GLOG:BOOL=OFF
    -DBUILD_UNIT_TESTS:BOOL=OFF
    -DBUILD_BENCHMARK:BOOL=OFF
    -DBUILD_BRPC_TOOLS:BOOL=OFF
    -DDOWNLOAD_GTEST:BOOL=OFF
    -DWITH_DEBUG_SYMBOL:BOOL=OFF
    # brpc's CMakeLists.txt uses bare find_path/find_library (no PATHS arg) for
    # GFLAGS (FindGFLAGS.cmake:20-25), LEVELDB (CMakeLists.txt:226-227),
    # and PROTOC_LIB (CMakeLists.txt:270). On clean CI (no system packages),
    # these fail. Pass CMAKE_INCLUDE_PATH/CMAKE_LIBRARY_PATH — standard vars
    # that ALL find_path/find_library always search.
    -DCMAKE_INCLUDE_PATH:PATH=${gflags_INCLUDE_DIR};${leveldb_INCLUDE_DIR}
    -DCMAKE_LIBRARY_PATH:PATH=${gflags_LIB_DIR};${leveldb_LIB_DIR}
    -DGFLAGS_INCLUDE_PATH:PATH=${gflags_INCLUDE_DIR}
    -DGFLAGS_LIBRARY:FILEPATH=${gflags_LIBRARY}
    -DGFLAGS_ROOT:PATH=${gflags_ROOT}
    -DLEVELDB_INCLUDE_PATH:PATH=${leveldb_INCLUDE_DIR}
    -DLEVELDB_LIB:FILEPATH=${leveldb_LIBRARY}
    -DCMAKE_PREFIX_PATH:PATH="${_BRPC_PREFIX_PATH}"
    -DProtobuf_DIR:PATH=${_BRPC_PROTOBUF_CMAKE_DIR}
    -Dabsl_DIR:PATH=${absl_PKG_PATH}
    -Dgflags_DIR:PATH=${gflags_PKG_PATH}
    -Dleveldb_DIR:PATH=${leveldb_PKG_PATH}
    -DOPENSSL_ROOT_DIR:PATH=${OPENSSL_ROOT_DIR}
    # brpc's CMakeLists.txt reads the lowercase PROTOBUF_INCLUDE_DIRS / PROTOBUF_LIBRARIES / PROTOC_LIB
    # (set by FindProtobuf.cmake). The project's installed protobuf uses the modern target-based
    # protobuf-config.cmake that does NOT define those lowercase vars, so brpc ends up with empty
    # protobuf include paths and a system /usr/lib64 protoc. Pass them explicitly so brpc picks
    # the project's 4.25.5 protobuf instead of the system 3.14 (otherwise its regenerated *.pb.h
    # files reference headers like generated_message_tctable_decl.h that only exist in 4.x).
    -Dprotobuf_MODULE_COMPATIBLE:BOOL=ON
    -DCMAKE_DISABLE_FIND_PACKAGE_Protobuf:BOOL=OFF
    -DPROTOBUF_INCLUDE_DIRS:PATH=${Protobuf_INCLUDE_DIR}
    -DPROTOBUF_LIBRARIES:FILEPATH=${_BRPC_PROTOBUF_LIB}
    -DPROTOBUF_PROTOC_EXECUTABLE:FILEPATH=${_BRPC_PROTOC_EXECUTABLE}
    -DPROTOBUF_PROTOC_LIBRARY:FILEPATH=${_BRPC_PROTOC_LIB}
    -DPROTOC_LIB:FILEPATH=${_BRPC_PROTOC_LIB}
    -DProtobuf_PROTOC_EXECUTABLE:FILEPATH=${_BRPC_PROTOC_EXECUTABLE}
    -DProtobuf_INCLUDE_DIR:PATH=${Protobuf_INCLUDE_DIR}
    -DProtobuf_LIBRARY:FILEPATH=${_BRPC_PROTOBUF_LIB}
    -DProtobuf_LIBRARY_RELEASE:FILEPATH=${_BRPC_PROTOBUF_LIB}
    -DProtobuf_PROTOC_LIBRARY:FILEPATH=${_BRPC_PROTOC_LIB}
    -DCMAKE_CPP_FLAGS:STRING=-I${absl_INCLUDE_DIR})

# brpc's CMakeLists.txt unconditionally overwrites CMAKE_CXX_FLAGS (line 149), so passing -I for
# absl via CMAKE_CXX_FLAGS would be discarded. Instead, pass it via CMAKE_CPP_FLAGS which brpc
# accumulates into its final CMAKE_CXX_FLAGS (lines 136-175). absl is needed because protobuf 4.25.5
# headers transitively include absl/strings/string_view.h.
set(brpc_CXX_FLAGS ${THIRDPARTY_SAFE_FLAGS})
set(brpc_C_FLAGS ${THIRDPARTY_SAFE_FLAGS})

set(brpc_PATCHES
    ${CMAKE_SOURCE_DIR}/third_party/patches/brpc/fix-boringssl-compat.patch)

# brpc's build runs the project's protoc which dlopen-s libprotoc.so.<ver> at runtime.
# Without LD_LIBRARY_PATH the protoc invocation fails with "cannot open shared object file".
# Prepend the project's protobuf lib dir for the duration of this add_thirdparty_lib.
if (EXISTS ${Protobuf_ROOT}/lib64)
    set(_BRPC_PROTOC_LIB_DIR ${Protobuf_ROOT}/lib64)
else()
    set(_BRPC_PROTOC_LIB_DIR ${Protobuf_ROOT}/lib)
endif()
set(_BRPC_SAVED_LD_LIBRARY_PATH "$ENV{LD_LIBRARY_PATH}")
set(ENV{LD_LIBRARY_PATH} "${_BRPC_PROTOC_LIB_DIR}:$ENV{LD_LIBRARY_PATH}")

add_thirdparty_lib(brpc
    URL ${brpc_URL}
    SHA256 ${brpc_SHA256}
    FAKE_SHA256 ${brpc_FAKE_SHA256}
    VERSION ${brpc_VERSION}
    CONF_OPTIONS ${brpc_CMAKE_OPTIONS}
    CXX_FLAGS ${brpc_CXX_FLAGS}
    C_FLAGS ${brpc_C_FLAGS}
    PATCHES ${brpc_PATCHES}
    EXTRA_MSGS ${brpc_EXTRA_MSGS})

# Restore the previous LD_LIBRARY_PATH so we don't leak the protobuf lib path into later steps.
set(ENV{LD_LIBRARY_PATH} "${_BRPC_SAVED_LD_LIBRARY_PATH}")

# Expose brpc's include and lib paths for downstream CMakeLists.txt.
# brpc's cmake build packs bthread/butil/bvar into libbrpc.{a,so}; they are not
# separately installed, so a single link to libbrpc exposes all symbols.
set(BRPC_INCLUDE_DIR ${brpc_ROOT}/include)
set(BRPC_LIB_DIR ${brpc_ROOT}/lib)
if (EXISTS ${brpc_ROOT}/lib64)
    set(BRPC_LIB_DIR ${brpc_ROOT}/lib64)
endif()
find_library(BRPC_LIBRARY
    NAMES brpc
    PATHS ${BRPC_LIB_DIR}
    REQUIRED
    NO_DEFAULT_PATH)
include_directories(SYSTEM ${BRPC_INCLUDE_DIR})

# Convenience aggregate for targets that need brpc (incl. bthread/butil/bvar) + their deps.
# protobuf::libprotobuf is needed because libbrpc.so links against it at build time
# but find_library above returns a raw .so path without its NEEDED transitive deps.
# Adding it explicitly ensures consumers get -L<protobuf_lib_dir> -lprotobuf.
# protobuf::libprotobuf also transitively brings in absl (protobuf was built with
# -Dprotobuf_ABSL_PROVIDER=package), resolving libbrpc.so's absl symbols.
set(BRPC_LIBRARIES
    ${BRPC_LIBRARY}
    ${gflags_LIBRARY}
    ${leveldb_LIBRARY}
    protobuf::libprotobuf)

if (DEFINED EXPORT_TO_USER_ENV_FILE)
    file(APPEND "${EXPORT_TO_USER_ENV_FILE}"
        "set(BRPC_INCLUDE_DIR ${BRPC_INCLUDE_DIR})" "\n"
        "set(BRPC_LIB_DIR ${BRPC_LIB_DIR})" "\n")
endif()
