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
        ${KVTEST_ROOT}/src/pipeline/cuda_workload.cpp
    )
endif()

if(EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/test_rate_gate.cpp")
    list(APPEND PHASE4_SOURCES
        test_rate_gate.cpp
    )
endif()

if(EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/test_benchmark.cpp")
    list(APPEND PHASE4_SOURCES
        test_benchmark.cpp
        ${KVTEST_ROOT}/src/benchmark/benchmark_runner.cpp
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
    ${KVTEST_ROOT}/../../include
)

# Optional NUMA support — match main CMakeLists.txt detection
find_library(LIBNUMA_LIB numa)
if(LIBNUMA_LIB)
    target_compile_definitions(kvtest_tests PRIVATE HAS_LIBNUMA=1)
    target_link_libraries(kvtest_tests PRIVATE ${LIBNUMA_LIB})
    message(STATUS "NUMA support enabled for tests: ${LIBNUMA_LIB}")
endif()

target_link_libraries(kvtest_tests PRIVATE pthread ${CMAKE_DL_LIBS})

enable_testing()
add_library(kvtest_fake_cudart SHARED fake_cudart.cpp)
target_link_libraries(kvtest_fake_cudart PRIVATE pthread)
add_executable(kvtest_cuda_tests test_cuda_workload.cpp
    ${KVTEST_ROOT}/src/pipeline/cuda_workload.cpp
    ${KVTEST_ROOT}/src/pipeline/pipeline.cpp
    ${KVTEST_ROOT}/src/common/config.cpp
    ${KVTEST_ROOT}/src/metrics/metrics.cpp)
target_include_directories(kvtest_cuda_tests PRIVATE stubs . ${KVTEST_ROOT}/src ${KVTEST_ROOT}/../../include)
target_link_libraries(kvtest_cuda_tests PRIVATE pthread ${CMAKE_DL_LIBS} kvtest_fake_cudart)
foreach(mode cpu required_no_gpu load_fail pin_only pin_on pin_off partial_fail fragment_fail event_fail capacity verify_corrupt
             concurrent verify_off verify_sample warmup warmup_fail warmup_pin_off)
    add_test(NAME cuda_${mode} COMMAND kvtest_cuda_tests $<TARGET_FILE:kvtest_fake_cudart> ${mode})
    set_tests_properties(cuda_${mode} PROPERTIES TIMEOUT 8)
endforeach()
