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

if (TRANSFER_ENGINE_STANDALONE AND TARGET ds_spdlog::spdlog AND TRANSFER_ENGINE_SPDLOG_LIB_PATH)
    add_custom_target(copy_transfer_engine_spdlog_so ALL
        COMMAND ${CMAKE_COMMAND} -E make_directory ${CMAKE_CURRENT_BINARY_DIR}/lib
        COMMAND ${CMAKE_COMMAND} -E copy_if_different
            $<TARGET_FILE:ds_spdlog::spdlog>
            ${CMAKE_CURRENT_BINARY_DIR}/lib/$<TARGET_FILE_NAME:ds_spdlog::spdlog>
        COMMAND ${CMAKE_COMMAND} -E copy_if_different
            $<TARGET_SONAME_FILE:ds_spdlog::spdlog>
            ${CMAKE_CURRENT_BINARY_DIR}/lib/$<TARGET_SONAME_FILE_NAME:ds_spdlog::spdlog>
        COMMAND ${CMAKE_COMMAND} -E copy_if_different
            ${TRANSFER_ENGINE_SPDLOG_LIB_PATH}/libds-spdlog.so
            ${CMAKE_CURRENT_BINARY_DIR}/lib/libds-spdlog.so
        VERBATIM
    )
endif()

function(transfer_engine_add_no_needed_library_test test_name target_name needed_library)
    if (NOT CMAKE_READELF)
        return()
    endif()
    add_test(
        NAME ${test_name}
        COMMAND ${CMAKE_COMMAND}
            -DREADELF_EXECUTABLE=${CMAKE_READELF}
            -DLIBRARY_PATH=$<TARGET_FILE:${target_name}>
            -DNEEDED_LIBRARY=${needed_library}
            -P ${CMAKE_CURRENT_SOURCE_DIR}/tests/cmake/check_no_needed_library.cmake
    )
endfunction()

function(transfer_engine_add_no_gnu_unique_test test_name target_name)
    if (NOT CMAKE_READELF)
        return()
    endif()
    add_test(
        NAME ${test_name}
        COMMAND ${CMAKE_COMMAND}
            -DREADELF_EXECUTABLE=${CMAKE_READELF}
            -DLIBRARY_PATH=$<TARGET_FILE:${target_name}>
            -P ${CMAKE_CURRENT_SOURCE_DIR}/tests/cmake/check_no_gnu_unique_symbols.cmake
    )
endfunction()

function(transfer_engine_add_no_dynamic_symbol_test test_name target_name forbidden_symbol)
    if (NOT CMAKE_READELF)
        return()
    endif()
    add_test(
        NAME ${test_name}
        COMMAND ${CMAKE_COMMAND}
            -DREADELF_EXECUTABLE=${CMAKE_READELF}
            -DLIBRARY_PATH=$<TARGET_FILE:${target_name}>
            -DFORBIDDEN_SYMBOL=${forbidden_symbol}
            -P ${CMAKE_CURRENT_SOURCE_DIR}/tests/cmake/check_no_dynamic_symbol.cmake
    )
endfunction()

function(transfer_engine_add_no_defined_dynamic_symbol_test test_name target_name forbidden_symbol)
    if (NOT CMAKE_READELF)
        return()
    endif()
    add_test(
        NAME ${test_name}
        COMMAND ${CMAKE_COMMAND}
            -DREADELF_EXECUTABLE=${CMAKE_READELF}
            -DLIBRARY_PATH=$<TARGET_FILE:${target_name}>
            -DFORBIDDEN_SYMBOL=${forbidden_symbol}
            -DDEFINED_ONLY=ON
            -P ${CMAKE_CURRENT_SOURCE_DIR}/tests/cmake/check_no_dynamic_symbol.cmake
    )
endfunction()

function(transfer_engine_add_origin_rpath_test test_name target_name)
    if (NOT CMAKE_READELF)
        return()
    endif()
    add_test(
        NAME ${test_name}
        COMMAND ${CMAKE_COMMAND}
            -DREADELF_EXECUTABLE=${CMAKE_READELF}
            -DLIBRARY_PATH=$<TARGET_FILE:${target_name}>
            -P ${CMAKE_CURRENT_SOURCE_DIR}/tests/cmake/check_origin_rpath.cmake
    )
endfunction()

function(transfer_engine_add_forbidden_text_test test_name file_path forbidden_text)
    add_test(
        NAME ${test_name}
        COMMAND ${CMAKE_COMMAND}
            -DFILE_PATH=${file_path}
            -DFORBIDDEN_TEXT=${forbidden_text}
            -P ${CMAKE_CURRENT_SOURCE_DIR}/tests/cmake/check_forbidden_text.cmake
    )
endfunction()

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

if (TRANSFER_ENGINE_ENABLE_P2P_THIRD_PARTY OR TRANSFER_ENGINE_ENABLE_HIXL)
    if (NOT DEFINED Ascend_ROOT)
        if (DEFINED ENV{ASCEND_HOME_PATH})
            set(Ascend_ROOT $ENV{ASCEND_HOME_PATH})
        elseif(DEFINED ENV{ASCEND_CUSTOM_PATH})
            set(Ascend_ROOT $ENV{ASCEND_CUSTOM_PATH}/latest)
        else()
            set(Ascend_ROOT /usr/local/Ascend/ascend-toolkit/latest)
        endif()
    endif()
    set(_te_ascend_include_dir ${Ascend_ROOT}/include)
    set(_te_ascend_include_dirs
        ${_te_ascend_include_dir}
        ${_te_ascend_include_dir}/experiment
        ${_te_ascend_include_dir}/experiment/runtime)
    set(_te_ascend_lib_dirs ${Ascend_ROOT}/lib64 ${Ascend_ROOT}/lib)
endif()

if (TRANSFER_ENGINE_ENABLE_P2P_THIRD_PARTY)
    list(APPEND TRANSFER_ENGINE_SRCS src/internal/backend/ascend/p2p_transfer_backend.cpp)
endif()

if (TRANSFER_ENGINE_ENABLE_HIXL)
    find_path(TRANSFER_ENGINE_HIXL_INCLUDE_DIR
        NAMES hixl/hixl.h hixl/hixl_types.h
        PATHS ${_te_ascend_include_dir}
    )
    find_library(TRANSFER_ENGINE_HIXL_LIBRARY
        NAMES cann_hixl
        PATHS ${_te_ascend_lib_dirs}
    )
    find_library(TRANSFER_ENGINE_METADEF_LIBRARY
        NAMES metadef
        PATHS ${_te_ascend_lib_dirs}
    )
    find_library(TRANSFER_ENGINE_ASCENDCL_LIBRARY
        NAMES ascendcl
        PATHS ${_te_ascend_lib_dirs}
    )
    if (NOT TRANSFER_ENGINE_HIXL_INCLUDE_DIR OR NOT TRANSFER_ENGINE_HIXL_LIBRARY OR
        NOT TRANSFER_ENGINE_METADEF_LIBRARY OR NOT TRANSFER_ENGINE_ASCENDCL_LIBRARY)
        message(FATAL_ERROR
            "TRANSFER_ENGINE_ENABLE_HIXL=ON requires an Ascend CANN toolkit with HIXL. "
            "Check ASCEND_HOME_PATH or ASCEND_CUSTOM_PATH."
        )
    endif()
    set(_te_hixl_libraries
        ${TRANSFER_ENGINE_HIXL_LIBRARY}
        ${TRANSFER_ENGINE_METADEF_LIBRARY}
        ${TRANSFER_ENGINE_ASCENDCL_LIBRARY})
    list(APPEND TRANSFER_ENGINE_SRCS src/internal/backend/ascend/hixl_d2d_backend.cpp)
endif()

add_library(transfer_engine_obj OBJECT ${TRANSFER_ENGINE_SRCS})
if (TARGET copy_transfer_engine_spdlog_so)
    add_dependencies(transfer_engine_obj copy_transfer_engine_spdlog_so)
endif()
set_target_properties(transfer_engine_obj PROPERTIES
    POSITION_INDEPENDENT_CODE ON
    CXX_VISIBILITY_PRESET hidden
    VISIBILITY_INLINES_HIDDEN YES
)
if (CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
    target_compile_options(transfer_engine_obj PRIVATE -fPIC)
endif()
if (CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
    target_compile_options(transfer_engine_obj PRIVATE -fno-gnu-unique)
endif()

target_include_directories(transfer_engine_obj
    PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}/include
    PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/src
)

target_link_libraries(transfer_engine_obj PRIVATE ds_spdlog::spdlog dl)

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
            BUILD_WITH_INSTALL_RPATH ON
        )
        if (CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
            # p2p_transfer is dlopen-ed directly from the wheel. Its package-local protobuf dependency
            # must also resolve package-local Abseil without relying on process-global preloads.
            target_link_options(p2p_transfer PRIVATE "LINKER:--disable-new-dtags")
        endif()
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
        ${_te_ascend_include_dirs}
    )

    target_compile_definitions(transfer_engine_obj PRIVATE TRANSFER_ENGINE_ENABLE_P2P_THIRD_PARTY=1)
endif()

if (TRANSFER_ENGINE_ENABLE_HIXL)
    target_include_directories(transfer_engine_obj PRIVATE ${TRANSFER_ENGINE_HIXL_INCLUDE_DIR})
    target_compile_definitions(transfer_engine_obj PRIVATE TRANSFER_ENGINE_ENABLE_HIXL=1)
endif()

add_library(transfer_engine STATIC $<TARGET_OBJECTS:transfer_engine_obj>)
set_target_properties(transfer_engine PROPERTIES POSITION_INDEPENDENT_CODE ON)
target_include_directories(transfer_engine
    PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}/include
    PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/src
)
target_link_libraries(transfer_engine PUBLIC Threads::Threads PRIVATE ds_spdlog::spdlog dl)
if (TRANSFER_ENGINE_ENABLE_P2P_THIRD_PARTY)
    if (TRANSFER_ENGINE_BUILD_BUNDLED_P2P_SO)
        add_dependencies(transfer_engine p2p_transfer copy_p2p_transfer_so)
    endif()
endif()
if (TRANSFER_ENGINE_ENABLE_HIXL)
    target_link_libraries(transfer_engine PUBLIC ${_te_hixl_libraries})
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

    add_executable(transfer_engine_logging_llt tests/llt/logging_llt_test.cpp)
    target_include_directories(transfer_engine_logging_llt PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src)
    target_link_libraries(transfer_engine_logging_llt PRIVATE transfer_engine GTest::gtest GTest::gtest_main)
    add_test(
        NAME transfer_engine_logging_stderr_llt
        COMMAND transfer_engine_logging_llt --gtest_filter=TransferEngineLoggingStderrTest.*
    )
    set_tests_properties(transfer_engine_logging_stderr_llt PROPERTIES
        ENVIRONMENT
            "TRANSFER_ENGINE_LOG_LEVEL=WARNING;TRANSFER_ENGINE_VLOG_LEVEL=0;TRANSFER_ENGINE_LOG_TO_STDERR=1"
    )
    add_test(
        NAME transfer_engine_logging_file_llt
        COMMAND transfer_engine_logging_llt --gtest_filter=TransferEngineLoggingFileTest.*
    )
    set_tests_properties(transfer_engine_logging_file_llt PROPERTIES
        ENVIRONMENT
            "TRANSFER_ENGINE_LOG_LEVEL=INFO;TRANSFER_ENGINE_VLOG_LEVEL=0;TRANSFER_ENGINE_VMODULE=logging_llt_test=1;TRANSFER_ENGINE_LOG_DIR=${CMAKE_CURRENT_BINARY_DIR}/transfer_engine_log_test;TRANSFER_ENGINE_LOG_TO_STDERR=0;TRANSFER_ENGINE_ALSO_LOG_TO_STDERR=0;TRANSFER_ENGINE_LOG_TO_STDOUT=0;TRANSFER_ENGINE_STDERR_THRESHOLD=2;TRANSFER_ENGINE_LOG_BUFFER_LEVEL=0;TRANSFER_ENGINE_LOG_BUFFER_SECONDS=0;TRANSFER_ENGINE_MAX_LOG_SIZE_MB=1800;TRANSFER_ENGINE_LOG_FILE_MODE=0664;TRANSFER_ENGINE_TIMESTAMP_IN_LOG_FILE_NAME=1;TRANSFER_ENGINE_LOG_FILE_HEADER=1;TRANSFER_ENGINE_LOG_PREFIX=1;TRANSFER_ENGINE_LOG_YEAR_IN_PREFIX=1;TRANSFER_ENGINE_LOG_UTC_TIME=0"
    )

    if (TARGET p2p_transfer)
        transfer_engine_add_no_needed_library_test(
            transfer_engine_p2p_no_glog_needed p2p_transfer "libglog")
        transfer_engine_add_no_needed_library_test(
            transfer_engine_p2p_no_brpc_needed p2p_transfer "libbrpc")
        transfer_engine_add_no_needed_library_test(
            transfer_engine_p2p_no_upstream_spdlog_needed p2p_transfer "libspdlog")
        transfer_engine_add_no_needed_library_test(
            transfer_engine_p2p_no_ds_spdlog_needed p2p_transfer "libds-spdlog")
        transfer_engine_add_no_gnu_unique_test(
            transfer_engine_p2p_no_gnu_unique_symbols p2p_transfer)
        transfer_engine_add_no_dynamic_symbol_test(
            transfer_engine_p2p_no_glog_symbols p2p_transfer "google::Log")
        transfer_engine_add_no_dynamic_symbol_test(
            transfer_engine_p2p_no_upstream_spdlog_symbols p2p_transfer "(^|[^A-Za-z0-9_])spdlog::")
        transfer_engine_add_no_defined_dynamic_symbol_test(
            transfer_engine_p2p_no_fmt_symbols p2p_transfer "(^|[^A-Za-z0-9_])fmt::")
        transfer_engine_add_origin_rpath_test(
            transfer_engine_p2p_origin_rpath p2p_transfer)
    endif()

    if (EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/../python/yr/datasystem/__init__.py")
        transfer_engine_add_forbidden_text_test(
            transfer_engine_integrated_python_no_rtld_global
            "${CMAKE_CURRENT_SOURCE_DIR}/../python/yr/datasystem/__init__.py"
            "RTLD_GLOBAL")
        transfer_engine_add_forbidden_text_test(
            transfer_engine_integrated_python_no_ctypes_cdll
            "${CMAKE_CURRENT_SOURCE_DIR}/../python/yr/datasystem/__init__.py"
            "ctypes.CDLL")
        if (TRANSFER_ENGINE_BUILD_PYTHON)
            add_test(
                NAME transfer_engine_integrated_python_parent_import_lazy
                COMMAND ${Python3_EXECUTABLE} ${CMAKE_CURRENT_SOURCE_DIR}/tests/python/check_parent_import_lazy.py
            )
            set_tests_properties(transfer_engine_integrated_python_parent_import_lazy PROPERTIES
                ENVIRONMENT "PYTHONPATH=${CMAKE_CURRENT_SOURCE_DIR}/../python"
            )
        endif()
    endif()
    transfer_engine_add_forbidden_text_test(
        transfer_engine_standalone_python_no_rtld_global
        "${CMAKE_CURRENT_SOURCE_DIR}/python/yr/datasystem/__init__.py"
        "RTLD_GLOBAL")
    transfer_engine_add_forbidden_text_test(
        transfer_engine_standalone_python_no_ctypes_cdll
        "${CMAKE_CURRENT_SOURCE_DIR}/python/yr/datasystem/__init__.py"
        "ctypes.CDLL")
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
    pybind11_add_module(_transfer_engine MODULE NO_EXTRAS src/python/py_transfer_engine.cpp)
    set(CMAKE_CXX_FLAGS "${_te_saved_cxx_flags}")
    set(CMAKE_CXX_FLAGS_RELEASE "${_te_saved_cxx_flags_release}")
    set(CMAKE_SHARED_MODULE_LINKER_FLAGS "${_te_saved_shared_module_linker_flags}")
    set_target_properties(_transfer_engine PROPERTIES
        POSITION_INDEPENDENT_CODE ON
        CXX_VISIBILITY_PRESET hidden
        VISIBILITY_INLINES_HIDDEN YES
    )
    target_include_directories(_transfer_engine PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/src
        ${CMAKE_CURRENT_SOURCE_DIR}/include
    )
    target_sources(_transfer_engine PRIVATE $<TARGET_OBJECTS:transfer_engine_obj>)
    target_link_libraries(_transfer_engine PRIVATE ds_spdlog::spdlog Threads::Threads dl)
    if (TARGET Python3::Module)
        target_link_libraries(_transfer_engine PRIVATE Python3::Module)
    elseif(TARGET Python3::Python)
        target_link_libraries(_transfer_engine PRIVATE Python3::Python)
    elseif(Python3_LIBRARIES)
        target_link_libraries(_transfer_engine PRIVATE ${Python3_LIBRARIES})
    elseif(DEFINED Python3_LIBRARY_RELEASE AND EXISTS "${Python3_LIBRARY_RELEASE}")
        target_link_libraries(_transfer_engine PRIVATE "${Python3_LIBRARY_RELEASE}")
    endif()
    if (CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
        target_compile_options(_transfer_engine PRIVATE -fPIC)
        target_link_options(_transfer_engine PRIVATE "LINKER:--exclude-libs,ALL")
    endif()
    if (CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
        target_compile_options(_transfer_engine PRIVATE -fno-gnu-unique)
        target_link_options(_transfer_engine PRIVATE -Wno-error=maybe-uninitialized)
    endif()
    if (TRANSFER_ENGINE_ENABLE_P2P_THIRD_PARTY AND TRANSFER_ENGINE_BUILD_BUNDLED_P2P_SO)
        add_dependencies(_transfer_engine p2p_transfer copy_p2p_transfer_so)
    endif()
    if (TRANSFER_ENGINE_ENABLE_HIXL)
        target_link_libraries(_transfer_engine PRIVATE ${_te_hixl_libraries})
    endif()
    set_target_properties(_transfer_engine PROPERTIES
        BUILD_RPATH "$ORIGIN;$ORIGIN/lib"
        INSTALL_RPATH "$ORIGIN;$ORIGIN/lib"
        BUILD_WITH_INSTALL_RPATH ON
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
        set(_te_python_st_library_paths "${CMAKE_CURRENT_BINARY_DIR}/lib")
        if (SPDLOG_LIB_PATH)
            list(APPEND _te_python_st_library_paths "${SPDLOG_LIB_PATH}")
        endif()
        if (TRANSFER_ENGINE_SPDLOG_LIB_PATH AND NOT TRANSFER_ENGINE_SPDLOG_LIB_PATH STREQUAL SPDLOG_LIB_PATH)
            list(APPEND _te_python_st_library_paths "${TRANSFER_ENGINE_SPDLOG_LIB_PATH}")
        endif()
        list(JOIN _te_python_st_library_paths ":" _te_python_st_ld_library_path)
        if (DEFINED ENV{LD_LIBRARY_PATH} AND NOT "$ENV{LD_LIBRARY_PATH}" STREQUAL "")
            string(APPEND _te_python_st_ld_library_path ":$ENV{LD_LIBRARY_PATH}")
        endif()
        set(_te_python_st_environment
            "PYTHONPATH=${CMAKE_CURRENT_SOURCE_DIR}:${CMAKE_CURRENT_SOURCE_DIR}/python"
            "LD_LIBRARY_PATH=${_te_python_st_ld_library_path}"
        )
        if (TRANSFER_ENGINE_ENABLE_P2P_THIRD_PARTY AND TRANSFER_ENGINE_BUILD_BUNDLED_P2P_SO)
            list(APPEND _te_python_st_environment
                "TRANSFER_ENGINE_P2P_SO_PATH=${CMAKE_CURRENT_BINARY_DIR}/lib/libp2p_transfer.so")
        endif()
        add_test(
            NAME transfer_engine_python_st
            COMMAND ${Python3_EXECUTABLE} -m unittest tests.python.st.test_python_api_st
        )
        set_tests_properties(transfer_engine_python_st PROPERTIES
            ENVIRONMENT "${_te_python_st_environment}"
        )
        transfer_engine_add_no_needed_library_test(
            transfer_engine_python_no_glog_needed _transfer_engine "libglog")
        transfer_engine_add_no_needed_library_test(
            transfer_engine_python_no_brpc_needed _transfer_engine "libbrpc")
        transfer_engine_add_no_needed_library_test(
            transfer_engine_python_no_upstream_spdlog_needed _transfer_engine "libspdlog")
        transfer_engine_add_no_gnu_unique_test(
            transfer_engine_python_no_gnu_unique_symbols _transfer_engine)
        transfer_engine_add_no_dynamic_symbol_test(
            transfer_engine_python_no_glog_symbols _transfer_engine "google::Log")
        transfer_engine_add_no_dynamic_symbol_test(
            transfer_engine_python_no_upstream_spdlog_symbols _transfer_engine "(^|[^A-Za-z0-9_])spdlog::")
        transfer_engine_add_no_defined_dynamic_symbol_test(
            transfer_engine_python_no_fmt_symbols _transfer_engine "(^|[^A-Za-z0-9_])fmt::")
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
    if (TRANSFER_ENGINE_SPDLOG_LIB_PATH)
        install(DIRECTORY "${TRANSFER_ENGINE_SPDLOG_LIB_PATH}/"
            DESTINATION lib
            FILES_MATCHING PATTERN "libds-spdlog.so*")
    endif()
endif()
