cmake_minimum_required(VERSION 3.14)
project(transfer_engine LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

set(TRANSFER_ENGINE_STANDALONE OFF)
if (CMAKE_SOURCE_DIR STREQUAL CMAKE_CURRENT_SOURCE_DIR)
    set(TRANSFER_ENGINE_STANDALONE ON)
endif()

set(TRANSFER_ENGINE_CMAKE_ROOT_DIR "${CMAKE_CURRENT_SOURCE_DIR}/cmake" CACHE INTERNAL "")

include("${CMAKE_CURRENT_SOURCE_DIR}/cmake/options.cmake")
include("${CMAKE_CURRENT_SOURCE_DIR}/cmake/util.cmake")
include("${CMAKE_CURRENT_SOURCE_DIR}/cmake/dependency.cmake")

# Datasystem enables PIE globally, but the transfer_engine static/python outputs
# need plain PIC semantics inside this subproject.
foreach(_te_flag_var
    CMAKE_C_FLAGS
    CMAKE_C_FLAGS_RELEASE
    CMAKE_CXX_FLAGS
    CMAKE_CXX_FLAGS_RELEASE
    CMAKE_SHARED_LINKER_FLAGS
    CMAKE_SHARED_MODULE_LINKER_FLAGS
    CMAKE_EXE_LINKER_FLAGS)
    if (DEFINED ${_te_flag_var})
        string(REPLACE "-fPIE" "" ${_te_flag_var} "${${_te_flag_var}}")
        string(REPLACE "-pie" "" ${_te_flag_var} "${${_te_flag_var}}")
    endif()
endforeach()

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
    list(APPEND TRANSFER_ENGINE_SRCS src/internal/backend/ascend/p2p_transfer_backend.cpp)
endif()

add_library(transfer_engine_obj OBJECT ${TRANSFER_ENGINE_SRCS})
set_target_properties(transfer_engine_obj PROPERTIES POSITION_INDEPENDENT_CODE ON)
if (CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
    target_compile_options(transfer_engine_obj PRIVATE -fPIC)
endif()

target_include_directories(transfer_engine_obj
    PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}/include
    PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/src
)

target_link_libraries(transfer_engine_obj PUBLIC glog::glog PRIVATE dl)

if (TRANSFER_ENGINE_ENABLE_P2P_THIRD_PARTY)
    if (TRANSFER_ENGINE_BUILD_BUNDLED_P2P_SO)
        if (NOT TRANSFER_ENGINE_BUILD_BUNDLED_PROTOBUF)
            find_package(Protobuf QUIET)
            if (NOT Protobuf_FOUND)
                message(FATAL_ERROR
                    "Bundled P2P-Transfer requires Protobuf but it was not found. "
                    "Install protobuf dev package, configure -DProtobuf_ROOT_DIR=<path>, "
                    "or enable -DTRANSFER_ENGINE_BUILD_BUNDLED_PROTOBUF=ON."
                )
            endif()
        endif()

        set(_te_prev_build_shared_libs ${BUILD_SHARED_LIBS})
        set(BUILD_SHARED_LIBS ON CACHE BOOL "Build shared libs for bundled third party" FORCE)
        set(_te_prev_skip_install_rules ${CMAKE_SKIP_INSTALL_RULES})
        set(CMAKE_SKIP_INSTALL_RULES ON)
        add_subdirectory(
            ${CMAKE_CURRENT_SOURCE_DIR}/src/internal/backend/ascend/p2p_transfer
            ${CMAKE_CURRENT_BINARY_DIR}/ascend_p2p_transfer_build
            EXCLUDE_FROM_ALL
        )
        set_target_properties(p2p_transfer PROPERTIES
            BUILD_RPATH "$ORIGIN"
            INSTALL_RPATH "$ORIGIN"
        )
        set(CMAKE_SKIP_INSTALL_RULES ${_te_prev_skip_install_rules})
        set(BUILD_SHARED_LIBS ${_te_prev_build_shared_libs} CACHE BOOL "Restore shared library mode" FORCE)
        add_dependencies(transfer_engine_obj p2p_transfer)

        add_custom_target(copy_p2p_transfer_so ALL
            COMMAND ${CMAKE_COMMAND} -E make_directory ${CMAKE_CURRENT_BINARY_DIR}/lib
            COMMAND ${CMAKE_COMMAND} -E copy_if_different
                $<TARGET_FILE:p2p_transfer>
                ${CMAKE_CURRENT_BINARY_DIR}/lib/libp2p_transfer.so
            DEPENDS p2p_transfer
            VERBATIM
        )
        add_dependencies(transfer_engine_obj copy_p2p_transfer_so)
        message(STATUS "Bundled p2p_transfer shared library output: ${CMAKE_CURRENT_BINARY_DIR}/lib")
    endif()

    target_include_directories(transfer_engine_obj PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/src/internal/backend/ascend/p2p_transfer/include
    )

    if (DEFINED ENV{ASCEND_HOME_PATH})
        set(_ascend_root $ENV{ASCEND_HOME_PATH})
    elseif(DEFINED ENV{ASCEND_CUSTOM_PATH})
        set(_ascend_root $ENV{ASCEND_CUSTOM_PATH}/latest)
    else()
        set(_ascend_root /usr/local/Ascend/ascend-toolkit/latest)
    endif()

    target_include_directories(transfer_engine_obj PRIVATE
        ${_ascend_root}/include
        ${_ascend_root}/include/experiment
        ${_ascend_root}/include/experiment/runtime
    )

    target_compile_definitions(transfer_engine_obj PRIVATE TRANSFER_ENGINE_ENABLE_P2P_THIRD_PARTY=1)
endif()

add_library(transfer_engine STATIC $<TARGET_OBJECTS:transfer_engine_obj>)
set_target_properties(transfer_engine PROPERTIES POSITION_INDEPENDENT_CODE ON)
target_include_directories(transfer_engine
    PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}/include
    PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/src
)
target_link_libraries(transfer_engine PUBLIC glog::glog Threads::Threads PRIVATE dl)
if (TRANSFER_ENGINE_ENABLE_P2P_THIRD_PARTY)
    if (TRANSFER_ENGINE_BUILD_BUNDLED_P2P_SO)
        add_dependencies(transfer_engine p2p_transfer copy_p2p_transfer_so)
    endif()
    target_include_directories(transfer_engine PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/src/internal/backend/ascend/p2p_transfer/include
        ${_ascend_root}/include
        ${_ascend_root}/include/experiment
        ${_ascend_root}/include/experiment/runtime
    )
endif()

add_executable(transfer_engine_compile_smoke tests/smoke/compile_smoke.cpp)
target_link_libraries(transfer_engine_compile_smoke PRIVATE transfer_engine)
if (TRANSFER_ENGINE_ENABLE_P2P_THIRD_PARTY AND TRANSFER_ENGINE_BUILD_BUNDLED_P2P_SO)
    add_dependencies(transfer_engine_compile_smoke p2p_transfer)
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
    add_dependencies(transfer_engine_cross_node_smoke p2p_transfer)
endif()

add_executable(transfer_engine_multi_node_multi_npu_smoke tests/smoke/multi_node_multi_npu_smoke.cpp)
target_include_directories(transfer_engine_multi_node_multi_npu_smoke PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src)
target_link_libraries(transfer_engine_multi_node_multi_npu_smoke PRIVATE transfer_engine)
if (TRANSFER_ENGINE_ENABLE_P2P_THIRD_PARTY AND TRANSFER_ENGINE_BUILD_BUNDLED_P2P_SO)
    add_dependencies(transfer_engine_multi_node_multi_npu_smoke p2p_transfer)
endif()

if (TRANSFER_ENGINE_BUILD_TESTS)
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
        add_dependencies(transfer_engine_basic_ut p2p_transfer)
    endif()
    add_test(NAME transfer_engine_basic_ut COMMAND transfer_engine_basic_ut)

    add_executable(transfer_engine_sync_read_ut tests/st/transfer_engine_sync_read_test.cpp)
    target_include_directories(transfer_engine_sync_read_ut PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src)
    target_link_libraries(transfer_engine_sync_read_ut PRIVATE transfer_engine GTest::gtest GTest::gtest_main)
    target_compile_definitions(transfer_engine_sync_read_ut PRIVATE
        TRANSFER_ENGINE_TEST_WITH_P2P_BACKEND=${_te_test_with_p2p_backend})
    if (TRANSFER_ENGINE_ENABLE_P2P_THIRD_PARTY AND TRANSFER_ENGINE_BUILD_BUNDLED_P2P_SO)
        add_dependencies(transfer_engine_sync_read_ut p2p_transfer)
    endif()
    add_test(NAME transfer_engine_sync_read_ut COMMAND transfer_engine_sync_read_ut)

    add_executable(transfer_engine_control_plane_e2e_llt tests/llt/control_plane_e2e_llt_test.cpp)
    target_include_directories(transfer_engine_control_plane_e2e_llt PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src)
    target_link_libraries(transfer_engine_control_plane_e2e_llt PRIVATE transfer_engine GTest::gtest GTest::gtest_main)
    if (TRANSFER_ENGINE_ENABLE_P2P_THIRD_PARTY AND TRANSFER_ENGINE_BUILD_BUNDLED_P2P_SO)
        add_dependencies(transfer_engine_control_plane_e2e_llt p2p_transfer)
    endif()
    add_test(NAME transfer_engine_control_plane_e2e_llt COMMAND transfer_engine_control_plane_e2e_llt)

    add_executable(transfer_engine_control_plane_components_llt tests/llt/control_plane_components_llt_test.cpp)
    target_include_directories(transfer_engine_control_plane_components_llt PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src)
    target_link_libraries(transfer_engine_control_plane_components_llt PRIVATE transfer_engine GTest::gtest GTest::gtest_main)
    if (TRANSFER_ENGINE_ENABLE_P2P_THIRD_PARTY AND TRANSFER_ENGINE_BUILD_BUNDLED_P2P_SO)
        add_dependencies(transfer_engine_control_plane_components_llt p2p_transfer)
    endif()
    add_test(NAME transfer_engine_control_plane_components_llt COMMAND transfer_engine_control_plane_components_llt)

    add_executable(transfer_engine_core_modules_llt tests/llt/core_modules_llt_test.cpp)
    target_include_directories(transfer_engine_core_modules_llt PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src)
    target_link_libraries(transfer_engine_core_modules_llt PRIVATE transfer_engine GTest::gtest GTest::gtest_main)
    if (TRANSFER_ENGINE_ENABLE_P2P_THIRD_PARTY AND TRANSFER_ENGINE_BUILD_BUNDLED_P2P_SO)
        add_dependencies(transfer_engine_core_modules_llt p2p_transfer)
    endif()
    add_test(NAME transfer_engine_core_modules_llt COMMAND transfer_engine_core_modules_llt)
endif()

if (TRANSFER_ENGINE_BUILD_PYTHON)
    set(_te_saved_cxx_flags "${CMAKE_CXX_FLAGS}")
    set(_te_saved_cxx_flags_release "${CMAKE_CXX_FLAGS_RELEASE}")
    set(_te_saved_shared_module_linker_flags "${CMAKE_SHARED_MODULE_LINKER_FLAGS}")
    string(REPLACE "-fPIE" "" CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS}")
    string(REPLACE "-pie" "" CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS}")
    string(REPLACE "-fPIE" "" CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE}")
    string(REPLACE "-pie" "" CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE}")
    string(REPLACE "-pie" "" CMAKE_SHARED_MODULE_LINKER_FLAGS "${CMAKE_SHARED_MODULE_LINKER_FLAGS}")
    pybind11_add_module(_transfer_engine MODULE src/python/py_transfer_engine.cpp)
    set(CMAKE_CXX_FLAGS "${_te_saved_cxx_flags}")
    set(CMAKE_CXX_FLAGS_RELEASE "${_te_saved_cxx_flags_release}")
    set(CMAKE_SHARED_MODULE_LINKER_FLAGS "${_te_saved_shared_module_linker_flags}")
    set_target_properties(_transfer_engine PROPERTIES POSITION_INDEPENDENT_CODE ON)
    target_include_directories(_transfer_engine PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/src
        ${CMAKE_CURRENT_SOURCE_DIR}/include
    )
    target_sources(_transfer_engine PRIVATE $<TARGET_OBJECTS:transfer_engine_obj>)
    target_link_libraries(_transfer_engine PRIVATE glog::glog dl)
    if (Python3_LIBRARIES)
        target_link_libraries(_transfer_engine PRIVATE ${Python3_LIBRARIES})
    elseif(DEFINED Python3_LIBRARY_RELEASE AND EXISTS "${Python3_LIBRARY_RELEASE}")
        target_link_libraries(_transfer_engine PRIVATE "${Python3_LIBRARY_RELEASE}")
    elseif (TARGET Python3::Module)
        target_link_libraries(_transfer_engine PRIVATE Python3::Module)
    elseif(TARGET Python3::Python)
        target_link_libraries(_transfer_engine PRIVATE Python3::Python)
    endif()
    if (CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
        target_compile_options(_transfer_engine PRIVATE -fPIC)
    endif()
    if (TRANSFER_ENGINE_ENABLE_P2P_THIRD_PARTY AND TRANSFER_ENGINE_BUILD_BUNDLED_P2P_SO)
        add_dependencies(_transfer_engine p2p_transfer copy_p2p_transfer_so)
    endif()
    set_target_properties(_transfer_engine PROPERTIES
        BUILD_RPATH "$ORIGIN;$ORIGIN/lib"
        INSTALL_RPATH "$ORIGIN;$ORIGIN/lib"
    )
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

if (TRANSFER_ENGINE_STANDALONE)
    install(TARGETS transfer_engine
        ARCHIVE DESTINATION lib
        LIBRARY DESTINATION lib
        RUNTIME DESTINATION bin)
    install(DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/include/datasystem
        DESTINATION include
        FILES_MATCHING PATTERN "*.h")
    if (TARGET p2p_transfer)
        install(TARGETS p2p_transfer
            LIBRARY DESTINATION lib
            RUNTIME DESTINATION bin)
    endif()
    if (TARGET _transfer_engine)
        install(TARGETS _transfer_engine
            LIBRARY DESTINATION python/yr/datasystem)
    endif()
endif()
