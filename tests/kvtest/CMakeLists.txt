cmake_minimum_required(VERSION 3.14)
project(kvtest C CXX)
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
# Use -std=c++17 (not gnu++17) to avoid GNU extension conflicts with the SDK's
# spdlog/logging initialization in ServiceDiscovery::Init().
set(CMAKE_CXX_EXTENSIONS OFF)

set(DATASYSTEM_SDK_DIR "" CACHE PATH "Path to datasystem SDK installation")
set(REPO_ROOT "${CMAKE_CURRENT_SOURCE_DIR}/../.." CACHE PATH "Path to yuanrong-datasystem repo root (for third-party cmake + patches)")

# Read version from VERSION file
file(STRINGS "${CMAKE_SOURCE_DIR}/VERSION" BUILD_VERSION LIMIT_COUNT 1)
add_definitions(-DBUILD_VERSION="${BUILD_VERSION}")

# Embed git commit hash at compile time
execute_process(
    COMMAND git rev-parse --short HEAD
    WORKING_DIRECTORY "${CMAKE_SOURCE_DIR}"
    OUTPUT_VARIABLE BUILD_COMMIT
    OUTPUT_STRIP_TRAILING_WHITESPACE
    ERROR_QUIET
)
if(NOT BUILD_COMMIT)
    set(BUILD_COMMIT "unknown")
endif()
add_definitions(-DBUILD_COMMIT="${BUILD_COMMIT}")

# ---------------------------------------------------------------------------
# Backend selection: brpc+bthread (default, ON) or httplib+std::thread (OFF).
# When ON, the main repo's third-party cmake scripts (cmake/external_libs/*.cmake)
# are reused to download/build brpc+protobuf+gflags+absl+leveldb+openssl+zlib
# into $DS_OPENSOURCE_DIR (cached; first build ~5-10min, subsequent seconds).
# kvtest then links libbrpc.so/libprotobuf.so/etc directly — no SDK packaging
# changes needed, no SDK symbol-export changes. The user need NOT pre-install
# any third-party dev packages; only the pre-installed datasystem SDK (for
# libdatasystem.so + datasystem headers) is required.
# ---------------------------------------------------------------------------
option(KVTEST_USE_BRPC "Use brpc control plane + bthread workers (ON) or httplib + std::thread (OFF)" ON)

# Common sources (compiled in both backends).
set(KVTEST_COMMON_SOURCES
    src/main.cpp
    src/common/config.cpp
    src/metrics/metrics.cpp
    src/pipeline/pipeline.cpp
    src/pipeline/kv_worker.cpp
    src/pipeline/cache_reader.cpp
    src/pipeline/stop.cpp
    src/rpc/notify_dispatcher.cpp
    src/rpc/peer_client.cpp
    src/benchmark/benchmark_runner.cpp)

if(KVTEST_USE_BRPC)
    # Build all third-party libs (brpc/protobuf/gflags/absl/openssl/zlib) as
    # STATIC archives and link them into the kvtest binary. This avoids vtable
    # interposition: libdatasystem.so exports _ZTV* (all C++ vtables via
    # export_datasystem_symbols.lds); if kvtest dynamically links libprotobuf.so,
    # the dynamic linker resolves google::protobuf::* vtable symbols to
    # libdatasystem.so's internal protobuf → crash. With static linking, vtable
    # references are direct (link-time resolved, no GOT) → no interposition.
    # This mirrors bazel mode (self-contained binary, static brpc/protobuf).
    set(KVTEST_BUILD_STATIC ON)

    # The main repo's external_libs/*.cmake reference ${CMAKE_SOURCE_DIR}/third_party/patches/
    # to locate patch files. When kvtest is the top-level project, CMAKE_SOURCE_DIR is
    # tests/kvtest/, not the repo root — so save/restore CMAKE_SOURCE_DIR around the
    # includes so the included .cmake files see the repo root for patch resolution.
    set(_KVTEST_SAVED_CMAKE_SOURCE_DIR ${CMAKE_SOURCE_DIR})
    set(CMAKE_SOURCE_DIR ${REPO_ROOT})

    # Include order mirrors cmake/dependency.cmake's relevant subset (brpc depends on
    # gflags/leveldb/protobuf/openssl/zlib/absl; protobuf depends on absl/zlib).
    include(${REPO_ROOT}/cmake/util.cmake)
    include(${REPO_ROOT}/cmake/external_libs/absl.cmake)
    include(${REPO_ROOT}/cmake/external_libs/zlib.cmake)
    include(${REPO_ROOT}/cmake/external_libs/openssl.cmake)
    include(${REPO_ROOT}/cmake/external_libs/leveldb.cmake)
    include(${REPO_ROOT}/cmake/external_libs/gflags.cmake)
    # Use protobuf 28.3 (matching bazel-built SDK's internal protobuf version).
    set(protobuf_VERSION 28.3)
    include(${REPO_ROOT}/cmake/external_libs/protobuf.cmake)
    include(${REPO_ROOT}/cmake/external_libs/brpc.cmake)

    set(CMAKE_SOURCE_DIR ${_KVTEST_SAVED_CMAKE_SOURCE_DIR})

    # Generate kvtest_control.pb.cc/.h from the proto using the project's protoc.
    # protoc is dynamically linked against libprotoc.so.<ver> which lives in
    # ${Protobuf_ROOT}/lib[64]; inject LD_LIBRARY_PATH via `cmake -E env`.
    if (EXISTS ${Protobuf_ROOT}/lib64)
        set(_KVTEST_PROTOC_LIB_DIR ${Protobuf_ROOT}/lib64)
    else()
        set(_KVTEST_PROTOC_LIB_DIR ${Protobuf_ROOT}/lib)
    endif()
    set(PROTO_FILE ${CMAKE_CURRENT_SOURCE_DIR}/kvtest_control.proto)
    set(GEN_DIR ${CMAKE_BINARY_DIR}/gen)
    file(MAKE_DIRECTORY ${GEN_DIR})
    set(PB_CC ${GEN_DIR}/kvtest_control.pb.cc)
    set(PB_H  ${GEN_DIR}/kvtest_control.pb.h)
    add_custom_command(
        OUTPUT ${PB_CC} ${PB_H}
        COMMAND ${CMAKE_COMMAND} -E env "LD_LIBRARY_PATH=${_KVTEST_PROTOC_LIB_DIR}:$ENV{LD_LIBRARY_PATH}"
                ${Protobuf_ROOT}/bin/protoc
                --proto_path=${CMAKE_CURRENT_SOURCE_DIR}
                --cpp_out=${GEN_DIR}
                ${PROTO_FILE}
        DEPENDS ${PROTO_FILE}
        COMMENT "Generating kvtest_control.pb.cc/.h via main repo's protoc")

    add_executable(kvtest
        ${KVTEST_COMMON_SOURCES}
        src/rpc/brpc_server.cpp
        ${PB_CC})

    # Force brpc_server.cpp.o and peer_client.cpp.o to wait for the proto
    # header to be generated (CMake's Make generator doesn't know about the
    # generated header dependency on first build).
    set_source_files_properties(src/rpc/brpc_server.cpp src/rpc/peer_client.cpp PROPERTIES
        OBJECT_DEPENDS "${PB_H}")

    target_include_directories(kvtest PRIVATE
        ${DATASYSTEM_SDK_DIR}/include
        ${CMAKE_CURRENT_SOURCE_DIR}/src
        ${GEN_DIR})

    target_compile_definitions(kvtest PRIVATE KVTEST_USE_BRPC)

    # Statically link all third-party archives + dynamically link libdatasystem.so.
    # BRPC_LIBRARIES carries libbrpc.a + libgflags.a + libleveldb.a + protobuf::libprotobuf
    # + Threads. OpenSSL::SSL/Crypto and ZLIB::ZLIB are cmake imported targets that
    # handle transitive deps. -Wl,--whole-archive for absl ensures all small .a
    # components are pulled in (absl has many fine-grained static libs; without
    # whole-archive, the linker may miss template/inline-defined symbols).
    target_link_libraries(kvtest PRIVATE
        ${DATASYSTEM_SDK_DIR}/lib/libdatasystem.so
        -Wl,--whole-archive
        ${BRPC_LIBRARY}
        ${gflags_LIBRARY}
        ${leveldb_LIBRARY}
        -Wl,--no-whole-archive
        protobuf::libprotobuf
        OpenSSL::SSL
        OpenSSL::Crypto
        ZLIB::ZLIB
        Threads::Threads
        pthread
        dl
        rt)

    # Only libdatasystem.so is dynamic — BUILD_RPATH points at the SDK lib dir;
    # INSTALL_RPATH uses $ORIGIN/lib for portable deployment (patchelf in
    # `make package` rewrites the binary's RPATH at packaging time).
    set_target_properties(kvtest PROPERTIES
        BUILD_RPATH "${DATASYSTEM_SDK_DIR}/lib"
        INSTALL_RPATH "$ORIGIN/lib")
else()
    # httplib backend — header-only, no third-party deps; std::thread workers
    # (KVTEST_USE_BRPC undefined → kvtest::thread == std::thread, httplib path
    # in main.cpp/peer_client.cpp is selected at compile time).
    add_executable(kvtest
        ${KVTEST_COMMON_SOURCES}
        src/rpc/http_server.cpp)

    target_include_directories(kvtest PRIVATE
        ${DATASYSTEM_SDK_DIR}/include
        ${CMAKE_CURRENT_SOURCE_DIR}/src)

    set_target_properties(kvtest PROPERTIES
        BUILD_RPATH "${DATASYSTEM_SDK_DIR}/lib"
        INSTALL_RPATH "${DATASYSTEM_SDK_DIR}/lib")

    target_link_libraries(kvtest PRIVATE
        ${DATASYSTEM_SDK_DIR}/lib/libdatasystem.so
        pthread)
endif()

# Optional NUMA support — link libnuma if available (header detection by __has_include)
find_library(LIBNUMA_LIB numa)
if(LIBNUMA_LIB)
    target_compile_definitions(kvtest PRIVATE HAS_LIBNUMA=1)
    target_link_libraries(kvtest PRIVATE ${LIBNUMA_LIB})
    message(STATUS "NUMA support enabled: ${LIBNUMA_LIB}")
else()
    message(STATUS "NUMA not found — numa_node config will be ignored")
endif()

# Statically link libstdc++ and libgcc so the binary runs on systems with
# older GCC (e.g. openEuler 22.04 with GCC 10 / GLIBCXX_3.4.29).
target_link_options(kvtest PRIVATE -static-libstdc++ -static-libgcc)

# ---------------------------------------------------------------------------
# coordinator_test: standalone Coordinator deployment test program
# Links against libdatasystem_coordinator.so from the SDK.
# ---------------------------------------------------------------------------
add_executable(coordinator_test src/coordinator_main.cpp)
target_include_directories(coordinator_test PRIVATE
    ${DATASYSTEM_SDK_DIR}/include
    ${CMAKE_CURRENT_SOURCE_DIR}/src)
target_link_libraries(coordinator_test PRIVATE
    ${DATASYSTEM_SDK_DIR}/lib/libdatasystem_coordinator.so
    pthread)
set_target_properties(coordinator_test PROPERTIES
    BUILD_RPATH "${DATASYSTEM_SDK_DIR}/lib"
    INSTALL_RPATH "$ORIGIN/lib")
target_link_options(coordinator_test PRIVATE -static-libstdc++ -static-libgcc)

# ---------------------------------------------------------------------------
# worker_test: standalone Worker deployment test program
# Links against libdatasystem_worker.so from the SDK.
# ---------------------------------------------------------------------------
add_executable(worker_test src/data_worker_main.cpp)
target_include_directories(worker_test PRIVATE
    ${DATASYSTEM_SDK_DIR}/include
    ${CMAKE_CURRENT_SOURCE_DIR}/src)
target_link_libraries(worker_test PRIVATE
    ${DATASYSTEM_SDK_DIR}/lib/libdatasystem_worker.so
    pthread)
set_target_properties(worker_test PROPERTIES
    BUILD_RPATH "${DATASYSTEM_SDK_DIR}/lib"
    INSTALL_RPATH "$ORIGIN/lib")
target_link_options(worker_test PRIVATE -static-libstdc++ -static-libgcc)
