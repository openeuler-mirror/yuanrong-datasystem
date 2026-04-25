cmake_minimum_required(VERSION 3.14)
project(kvclient_standalone_test CXX)
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
# Use -std=c++17 (not gnu++17) to avoid GNU extension conflicts with the SDK's
# spdlog/logging initialization in ServiceDiscovery::Init().
set(CMAKE_CXX_EXTENSIONS OFF)

set(DATASYSTEM_SDK_DIR "" CACHE PATH "Path to datasystem SDK installation")

# Read version from VERSION file
file(STRINGS "${CMAKE_SOURCE_DIR}/VERSION" BUILD_VERSION LIMIT_COUNT 1)
add_definitions(-DBUILD_VERSION="${BUILD_VERSION}")

add_executable(kvclient_standalone_test
    src/main.cpp src/config.cpp src/kv_worker.cpp src/http_server.cpp src/metrics.cpp src/stop.cpp src/pipeline.cpp)

# Use the shared libds-spdlog.so instead of header-only spdlog to avoid
# conflicting singleton registries between the binary and the SDK.
target_compile_definitions(kvclient_standalone_test PRIVATE SPDLOG_COMPILED_LIB SPDLOG_SHARED_LIB)

target_include_directories(kvclient_standalone_test PRIVATE
    ${DATASYSTEM_SDK_DIR}/include
    ${CMAKE_SOURCE_DIR}/src
    ${CMAKE_SOURCE_DIR}/third_party/spdlog-include
    ${CMAKE_SOURCE_DIR}/third_party/json-include)

# Set RPATH so the binary finds libdatasystem.so and its dependencies at runtime
set_target_properties(kvclient_standalone_test PROPERTIES
    BUILD_RPATH "${DATASYSTEM_SDK_DIR}/lib"
    INSTALL_RPATH "${DATASYSTEM_SDK_DIR}/lib"
)

target_link_libraries(kvclient_standalone_test PRIVATE
    ${DATASYSTEM_SDK_DIR}/lib/libdatasystem.so
    pthread)

# Statically link libstdc++ and libgcc so the binary runs on systems with
# older GCC (e.g. openEuler 22.04 with GCC 10 / GLIBCXX_3.4.29).
target_link_options(kvclient_standalone_test PRIVATE -static-libstdc++ -static-libgcc)
