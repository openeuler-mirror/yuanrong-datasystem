cmake_minimum_required(VERSION 3.14)
project(kvclient_standalone_test CXX)
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

set(DATASYSTEM_SDK_DIR "" CACHE PATH "Path to datasystem SDK installation")

include(FetchContent)
FetchContent_Declare(spdlog
    GIT_REPOSITORY https://github.com/gabime/spdlog.git
    GIT_TAG v1.12.0)
FetchContent_Declare(nlohmann_json
    GIT_REPOSITORY https://github.com/nlohmann/json.git
    GIT_TAG v3.11.2)
FetchContent_MakeAvailable(spdlog nlohmann_json)

add_executable(kvclient_standalone_test
    main.cpp config.cpp kv_worker.cpp http_server.cpp metrics.cpp stop.cpp)

target_include_directories(kvclient_standalone_test PRIVATE
    ${DATASYSTEM_SDK_DIR}/include
    ${CMAKE_SOURCE_DIR})

target_link_libraries(kvclient_standalone_test PRIVATE
    ${DATASYSTEM_SDK_DIR}/lib/libdatasystem.so
    spdlog::spdlog
    nlohmann_json::nlohmann_json
    pthread)
