cmake_minimum_required(VERSION 3.14)
project(kvtest_tests CXX)
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

if(NOT KVTEST_ROOT)
    set(KVTEST_ROOT "${CMAKE_CURRENT_SOURCE_DIR}/../..")
endif()

file(STRINGS "${KVTEST_ROOT}/VERSION" BUILD_VERSION LIMIT_COUNT 1)
add_definitions(-DBUILD_VERSION="${BUILD_VERSION}")

# Phase 1: zero-dependency tests (always available)
set(PHASE1_SOURCES
    test_main.cpp
    test_data_pattern.cpp
    test_cpu_affinity.cpp
    test_thread_pool.cpp
)

# Phase 2+: tests that need production sources (added incrementally)
set(PHASE2_SOURCES "")
set(PHASE3_SOURCES "")
set(PHASE4_SOURCES "")

if(EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/test_config.cpp")
    list(APPEND PHASE2_SOURCES
        test_config.cpp
        ${KVTEST_ROOT}/src/common/config.cpp
    )
endif()

if(EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/test_metrics.cpp")
    list(APPEND PHASE3_SOURCES
        test_metrics.cpp
        ${KVTEST_ROOT}/src/metrics/metrics.cpp
    )
endif()

if(EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/test_pipeline.cpp")
    list(APPEND PHASE4_SOURCES
        test_pipeline.cpp
        ${KVTEST_ROOT}/src/pipeline/pipeline.cpp
    )
endif()

add_executable(kvtest_tests
    ${PHASE1_SOURCES}
    ${PHASE2_SOURCES}
    ${PHASE3_SOURCES}
    ${PHASE4_SOURCES}
)

target_include_directories(kvtest_tests PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/stubs
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${KVTEST_ROOT}/src
)

target_link_libraries(kvtest_tests PRIVATE pthread)
