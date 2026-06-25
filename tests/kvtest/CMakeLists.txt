cmake_minimum_required(VERSION 3.14)
project(kvtest CXX)
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
# Use -std=c++17 (not gnu++17) to avoid GNU extension conflicts with the SDK's
# spdlog/logging initialization in ServiceDiscovery::Init().
set(CMAKE_CXX_EXTENSIONS OFF)

set(DATASYSTEM_SDK_DIR "" CACHE PATH "Path to datasystem SDK installation")

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

add_executable(kvtest
    src/main.cpp
    src/common/config.cpp
    src/metrics/metrics.cpp
    src/pipeline/pipeline.cpp
    src/pipeline/kv_worker.cpp
    src/pipeline/cache_reader.cpp
    src/pipeline/stop.cpp
    src/rpc/http_server.cpp
    src/benchmark/benchmark_runner.cpp)

target_include_directories(kvtest PRIVATE
    ${DATASYSTEM_SDK_DIR}/include
    ${CMAKE_SOURCE_DIR}/src)

# Set RPATH so the binary finds libdatasystem.so and its dependencies at runtime
set_target_properties(kvtest PROPERTIES
    BUILD_RPATH "${DATASYSTEM_SDK_DIR}/lib"
    INSTALL_RPATH "${DATASYSTEM_SDK_DIR}/lib"
)

target_link_libraries(kvtest PRIVATE
    ${DATASYSTEM_SDK_DIR}/lib/libdatasystem.so
    pthread)

# Optional NUMA support — check both header and library for consistency
include(CheckIncludeFile)
check_include_file(numa.h HAS_NUMA_H)
find_library(LIBNUMA_LIB numa)
if(HAS_NUMA_H AND LIBNUMA_LIB)
    target_compile_definitions(kvtest PRIVATE HAS_LIBNUMA=1)
    target_link_libraries(kvtest PRIVATE ${LIBNUMA_LIB})
    message(STATUS "NUMA support enabled: ${LIBNUMA_LIB}")
else()
    message(STATUS "NUMA not found — numa_node config will be ignored")
endif()

# Statically link libstdc++ and libgcc so the binary runs on systems with
# older GCC (e.g. openEuler 22.04 with GCC 10 / GLIBCXX_3.4.29).
target_link_options(kvtest PRIVATE -static-libstdc++ -static-libgcc)
