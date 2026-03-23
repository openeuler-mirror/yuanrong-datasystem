cmake_minimum_required(VERSION 3.14)
project(transfer_engine LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

include(FetchContent)
set(_te_prev_build_shared_libs ${BUILD_SHARED_LIBS})
set(BUILD_SHARED_LIBS OFF CACHE BOOL "Build static glog" FORCE)
FetchContent_Declare(
    glog
    URL https://github.com/google/glog/archive/refs/tags/v0.7.0.zip
)
set(WITH_GFLAGS OFF CACHE BOOL "" FORCE)
set(BUILD_TESTING OFF CACHE BOOL "" FORCE)
FetchContent_MakeAvailable(glog)
set(BUILD_SHARED_LIBS ${_te_prev_build_shared_libs} CACHE BOOL "Restore shared library mode" FORCE)

option(TRANSFER_ENGINE_ENABLE_P2P_THIRD_PARTY "Enable P2P-Transfer backend" ON)
option(TRANSFER_ENGINE_BUILD_TESTS "Build transfer_engine tests" ON)
option(TRANSFER_ENGINE_BUILD_BUNDLED_P2P_SO "Build bundled libp2ptransfer.so with transfer_engine" ON)
option(TRANSFER_ENGINE_BUILD_BUNDLED_PROTOBUF "Build bundled protobuf from source" ON)
option(TRANSFER_ENGINE_BUILD_PYTHON "Build transfer_engine python bindings" OFF)
set(TRANSFER_ENGINE_PYTHON_OUTPUT_DIR "" CACHE PATH "Output directory of python extension module")

set(TRANSFER_ENGINE_SRCS
    src/transfer_engine.cpp
    src/internal/log/logging.cpp
    src/internal/log/environment_dump.cpp
    src/internal/runtime/acl_runtime_helper.cpp
    src/internal/connection/connection_manager.cpp
    src/internal/control_plane/control_plane.cpp
    src/internal/control_plane/control_plane_codec.cpp
    src/internal/memory/registered_memory_table.cpp
    src/internal/control_plane/socket_rpc_transport.cpp
    src/internal/control_plane/transfer_control_dispatcher.cpp
    src/internal/backend/mock_data_plane_backend.cpp
    src/internal/control_plane/transfer_control_service.cpp
)

if (TRANSFER_ENGINE_ENABLE_P2P_THIRD_PARTY)
    list(APPEND TRANSFER_ENGINE_SRCS src/internal/backend/p2p_transfer_backend.cpp)
endif()

add_library(transfer_engine STATIC ${TRANSFER_ENGINE_SRCS})
set_target_properties(transfer_engine PROPERTIES POSITION_INDEPENDENT_CODE ON)

target_include_directories(transfer_engine
    PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}/include
    PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/src
)

target_link_libraries(transfer_engine PUBLIC glog::glog PRIVATE dl)

if (TRANSFER_ENGINE_ENABLE_P2P_THIRD_PARTY)
    if (TRANSFER_ENGINE_BUILD_BUNDLED_P2P_SO)
        if (TRANSFER_ENGINE_BUILD_BUNDLED_PROTOBUF)
            include(${CMAKE_CURRENT_SOURCE_DIR}/cmake/external_libs/protobuf.cmake)
            te_prepare_protobuf()
        else()
            find_package(Protobuf QUIET)
            if (NOT Protobuf_FOUND)
                message(FATAL_ERROR
                    "Bundled P2P-Transfer requires Protobuf but it was not found. "
                    "Install protobuf dev package, configure -DProtobuf_ROOT_DIR=<path>, "
                    "or enable -DTRANSFER_ENGINE_BUILD_BUNDLED_PROTOBUF=ON."
                )
            endif()
        endif()

        # Build shared p2p-transfer from bundled third_party source to ensure libp2ptransfer.so exists.
        set(BUILD_SHARED_LIBS ON CACHE BOOL "Build shared libs for bundled third party" FORCE)
        set(_te_prev_skip_install_rules ${CMAKE_SKIP_INSTALL_RULES})
        set(CMAKE_SKIP_INSTALL_RULES ON)
        add_subdirectory(
            ${CMAKE_CURRENT_SOURCE_DIR}/third_party/P2P-Transfer
            ${CMAKE_CURRENT_BINARY_DIR}/third_party_p2p_build
            EXCLUDE_FROM_ALL
        )
        # Ensure libp2ptransfer.so can resolve bundled deps (e.g. libprotobuf.so)
        # from the same directory after being copied into python package.
        set_target_properties(p2p-transfer_p2p-transfer PROPERTIES
            BUILD_RPATH "$ORIGIN"
            INSTALL_RPATH "$ORIGIN"
        )
        set(CMAKE_SKIP_INSTALL_RULES ${_te_prev_skip_install_rules})
        add_dependencies(transfer_engine p2p-transfer_p2p-transfer)

        # Keep a stable output location for runtime loading/tests.
        add_custom_target(copy_p2ptransfer_so ALL
            COMMAND ${CMAKE_COMMAND} -E make_directory ${CMAKE_CURRENT_BINARY_DIR}/lib
            COMMAND ${CMAKE_COMMAND} -E copy_if_different
                $<TARGET_FILE:p2p-transfer_p2p-transfer>
                ${CMAKE_CURRENT_BINARY_DIR}/lib/libp2ptransfer.so
            DEPENDS p2p-transfer_p2p-transfer
            VERBATIM
        )
        add_dependencies(transfer_engine copy_p2ptransfer_so)
        message(STATUS "Bundled p2p-transfer shared library output: ${CMAKE_CURRENT_BINARY_DIR}/lib")
    endif()

    target_include_directories(transfer_engine PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/third_party/P2P-Transfer/include
    )

    if (DEFINED ENV{ASCEND_HOME_PATH})
        set(_ascend_root $ENV{ASCEND_HOME_PATH})
    elseif(DEFINED ENV{ASCEND_CUSTOM_PATH})
        set(_ascend_root $ENV{ASCEND_CUSTOM_PATH}/latest)
    else()
        set(_ascend_root /usr/local/Ascend/ascend-toolkit/latest)
    endif()

    target_include_directories(transfer_engine PRIVATE
        ${_ascend_root}/include
        ${_ascend_root}/include/experiment
        ${_ascend_root}/include/experiment/runtime
    )

    target_compile_definitions(transfer_engine PRIVATE TRANSFER_ENGINE_ENABLE_P2P_THIRD_PARTY=1)
endif()

add_executable(transfer_engine_compile_smoke tests/smoke/compile_smoke.cpp)
target_link_libraries(transfer_engine_compile_smoke PRIVATE transfer_engine)
if (TRANSFER_ENGINE_ENABLE_P2P_THIRD_PARTY AND TRANSFER_ENGINE_BUILD_BUNDLED_P2P_SO)
    add_dependencies(transfer_engine_compile_smoke p2p-transfer_p2p-transfer)
endif()

add_executable(transfer_engine_cross_node_smoke
    tests/smoke/cross_node_transfer_smoke.cpp
    tests/acl_test_utils.cpp
)
target_include_directories(transfer_engine_cross_node_smoke PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/src
    ${CMAKE_CURRENT_SOURCE_DIR}/tests
)
target_link_libraries(transfer_engine_cross_node_smoke PRIVATE transfer_engine dl pthread)
if (TRANSFER_ENGINE_ENABLE_P2P_THIRD_PARTY AND TRANSFER_ENGINE_BUILD_BUNDLED_P2P_SO)
    add_dependencies(transfer_engine_cross_node_smoke p2p-transfer_p2p-transfer)
endif()

add_executable(transfer_engine_multi_node_multi_npu_smoke tests/smoke/multi_node_multi_npu_smoke.cpp)
target_include_directories(transfer_engine_multi_node_multi_npu_smoke PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src)
target_link_libraries(transfer_engine_multi_node_multi_npu_smoke PRIVATE transfer_engine)
if (TRANSFER_ENGINE_ENABLE_P2P_THIRD_PARTY AND TRANSFER_ENGINE_BUILD_BUNDLED_P2P_SO)
    add_dependencies(transfer_engine_multi_node_multi_npu_smoke p2p-transfer_p2p-transfer)
endif()

if (TRANSFER_ENGINE_BUILD_TESTS)
    find_package(GTest QUIET)
    if (NOT GTest_FOUND)
        include(FetchContent)
        FetchContent_Declare(
            googletest
            URL https://github.com/google/googletest/archive/refs/tags/v1.14.0.zip
        )
        set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
        FetchContent_MakeAvailable(googletest)
        set(GTest_FOUND ON)
    endif()

    if (GTest_FOUND)
        enable_testing()
        if (TRANSFER_ENGINE_ENABLE_P2P_THIRD_PARTY)
            set(_te_test_with_p2p_backend 1)
        else()
            set(_te_test_with_p2p_backend 0)
        endif()

        add_executable(transfer_engine_basic_ut
            tests/st/transfer_engine_basic_test.cpp
            tests/acl_test_utils.cpp
        )
        target_include_directories(transfer_engine_basic_ut PRIVATE
            ${CMAKE_CURRENT_SOURCE_DIR}/src
            ${CMAKE_CURRENT_SOURCE_DIR}/tests
        )
        target_link_libraries(transfer_engine_basic_ut PRIVATE transfer_engine GTest::gtest GTest::gtest_main dl)
        target_compile_definitions(transfer_engine_basic_ut PRIVATE
            TRANSFER_ENGINE_TEST_WITH_P2P_BACKEND=${_te_test_with_p2p_backend})
        if (TRANSFER_ENGINE_ENABLE_P2P_THIRD_PARTY AND TRANSFER_ENGINE_BUILD_BUNDLED_P2P_SO)
            add_dependencies(transfer_engine_basic_ut p2p-transfer_p2p-transfer)
        endif()
        add_test(NAME transfer_engine_basic_ut COMMAND transfer_engine_basic_ut)

        add_executable(transfer_engine_sync_read_ut tests/st/transfer_engine_sync_read_test.cpp)
        target_include_directories(transfer_engine_sync_read_ut PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src)
        target_link_libraries(transfer_engine_sync_read_ut PRIVATE transfer_engine GTest::gtest GTest::gtest_main)
        target_compile_definitions(transfer_engine_sync_read_ut PRIVATE
            TRANSFER_ENGINE_TEST_WITH_P2P_BACKEND=${_te_test_with_p2p_backend})
        if (TRANSFER_ENGINE_ENABLE_P2P_THIRD_PARTY AND TRANSFER_ENGINE_BUILD_BUNDLED_P2P_SO)
            add_dependencies(transfer_engine_sync_read_ut p2p-transfer_p2p-transfer)
        endif()
        add_test(NAME transfer_engine_sync_read_ut COMMAND transfer_engine_sync_read_ut)

        add_executable(transfer_engine_control_plane_e2e_llt tests/llt/control_plane_e2e_llt_test.cpp)
        target_include_directories(transfer_engine_control_plane_e2e_llt PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src)
        target_link_libraries(transfer_engine_control_plane_e2e_llt PRIVATE transfer_engine GTest::gtest GTest::gtest_main)
        if (TRANSFER_ENGINE_ENABLE_P2P_THIRD_PARTY AND TRANSFER_ENGINE_BUILD_BUNDLED_P2P_SO)
            add_dependencies(transfer_engine_control_plane_e2e_llt p2p-transfer_p2p-transfer)
        endif()
        add_test(NAME transfer_engine_control_plane_e2e_llt COMMAND transfer_engine_control_plane_e2e_llt)

        add_executable(transfer_engine_control_plane_components_llt tests/llt/control_plane_components_llt_test.cpp)
        target_include_directories(transfer_engine_control_plane_components_llt PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src)
        target_link_libraries(transfer_engine_control_plane_components_llt PRIVATE transfer_engine GTest::gtest GTest::gtest_main)
        if (TRANSFER_ENGINE_ENABLE_P2P_THIRD_PARTY AND TRANSFER_ENGINE_BUILD_BUNDLED_P2P_SO)
            add_dependencies(transfer_engine_control_plane_components_llt p2p-transfer_p2p-transfer)
        endif()
        add_test(NAME transfer_engine_control_plane_components_llt COMMAND transfer_engine_control_plane_components_llt)

        add_executable(transfer_engine_core_modules_llt tests/llt/core_modules_llt_test.cpp)
        target_include_directories(transfer_engine_core_modules_llt PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src)
        target_link_libraries(transfer_engine_core_modules_llt PRIVATE transfer_engine GTest::gtest GTest::gtest_main)
        if (TRANSFER_ENGINE_ENABLE_P2P_THIRD_PARTY AND TRANSFER_ENGINE_BUILD_BUNDLED_P2P_SO)
            add_dependencies(transfer_engine_core_modules_llt p2p-transfer_p2p-transfer)
        endif()
        add_test(NAME transfer_engine_core_modules_llt COMMAND transfer_engine_core_modules_llt)
    else()
        add_executable(transfer_engine_exec tests/smoke/transfer_engine_exec.cpp)
        target_include_directories(transfer_engine_exec PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src)
        target_link_libraries(transfer_engine_exec PRIVATE transfer_engine)
        if (TRANSFER_ENGINE_ENABLE_P2P_THIRD_PARTY AND TRANSFER_ENGINE_BUILD_BUNDLED_P2P_SO)
            add_dependencies(transfer_engine_exec p2p-transfer_p2p-transfer)
        endif()
    endif()
endif()

if (TRANSFER_ENGINE_BUILD_PYTHON)
    find_package(Python3 COMPONENTS Interpreter Development.Module REQUIRED)
    find_package(pybind11 CONFIG QUIET)
    if (NOT TARGET pybind11::module)
        include(FetchContent)
        FetchContent_Declare(
            pybind11
            URL https://github.com/pybind/pybind11/archive/refs/tags/v2.13.6.zip
        )
        FetchContent_MakeAvailable(pybind11)
    endif()

    pybind11_add_module(_transfer_engine MODULE src/python/py_transfer_engine.cpp)
    target_include_directories(_transfer_engine PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/src
    )
    target_link_libraries(_transfer_engine PRIVATE transfer_engine)
    if (TRANSFER_ENGINE_ENABLE_P2P_THIRD_PARTY AND TRANSFER_ENGINE_BUILD_BUNDLED_P2P_SO)
        add_dependencies(_transfer_engine p2p-transfer_p2p-transfer)
    endif()
    if (TRANSFER_ENGINE_PYTHON_OUTPUT_DIR)
        set_target_properties(_transfer_engine PROPERTIES
            LIBRARY_OUTPUT_DIRECTORY ${TRANSFER_ENGINE_PYTHON_OUTPUT_DIR}
        )
    else()
        set_target_properties(_transfer_engine PROPERTIES
            LIBRARY_OUTPUT_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/python/yr/datasystem
        )
    endif()

    if (TRANSFER_ENGINE_BUILD_TESTS)
        add_test(
            NAME transfer_engine_python_st
            COMMAND ${Python3_EXECUTABLE} -m unittest tests.python.st.test_python_api_st
        )
        set_tests_properties(transfer_engine_python_st PROPERTIES
            ENVIRONMENT "PYTHONPATH=${CMAKE_CURRENT_SOURCE_DIR}:${CMAKE_CURRENT_SOURCE_DIR}/python"
        )
    endif()
endif()
