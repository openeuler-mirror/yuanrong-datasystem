if (TARGET ds_spdlog::spdlog)
    return()
endif()

set(SPDLOG_VERSION 1.12.0)
if (NOT "$ENV{DS_LOCAL_LIBS_DIR}" STREQUAL "")
    set(SPDLOG_URL "$ENV{DS_LOCAL_LIBS_DIR}/opensource_third_party/v1.12.0.zip")
else()
    set(SPDLOG_URL "https://gitee.com/mirror-luyi/spdlog/repository/archive/v1.12.0.zip")
endif()
set(SPDLOG_SHA256 "a0a22ed8e4932cf5f7abc354fc96d20217a8b45471e732d5e672cc01979fe588")

get_filename_component(_TRANSFER_ENGINE_REPOSITORY_ROOT
    "${CMAKE_CURRENT_LIST_DIR}/../../.." ABSOLUTE)
set(_SPDLOG_NAMESPACE_PATCH_SOURCE
    "${_TRANSFER_ENGINE_REPOSITORY_ROOT}/third_party/patches/spdlog/change-namespace.patch")
if (NOT EXISTS "${_SPDLOG_NAMESPACE_PATCH_SOURCE}")
    message(FATAL_ERROR
        "TransferEngine standalone build requires the YuanRong spdlog patch: ${_SPDLOG_NAMESPACE_PATCH_SOURCE}")
endif()

# The upstream archive may carry README.md with different line endings. That
# documentation-only hunk must not make the runtime namespace patch fail.
file(READ "${_SPDLOG_NAMESPACE_PATCH_SOURCE}" _SPDLOG_NAMESPACE_PATCH_CONTENT)
string(FIND "${_SPDLOG_NAMESPACE_PATCH_CONTENT}"
    "diff --git a/README.md b/README.md" _SPDLOG_README_PATCH_BEGIN)
string(FIND "${_SPDLOG_NAMESPACE_PATCH_CONTENT}"
    "diff --git a/bench/CMakeLists.txt b/bench/CMakeLists.txt" _SPDLOG_README_PATCH_END)
set(_SPDLOG_NAMESPACE_PATCH
    "${CMAKE_CURRENT_BINARY_DIR}/transfer_engine_spdlog_change_namespace.patch")
if (_SPDLOG_README_PATCH_BEGIN GREATER_EQUAL 0
        AND _SPDLOG_README_PATCH_END GREATER _SPDLOG_README_PATCH_BEGIN)
    string(SUBSTRING "${_SPDLOG_NAMESPACE_PATCH_CONTENT}" 0
        ${_SPDLOG_README_PATCH_BEGIN} _SPDLOG_NAMESPACE_PATCH_PREFIX)
    string(SUBSTRING "${_SPDLOG_NAMESPACE_PATCH_CONTENT}"
        ${_SPDLOG_README_PATCH_END} -1 _SPDLOG_NAMESPACE_PATCH_SUFFIX)
    file(WRITE "${_SPDLOG_NAMESPACE_PATCH}"
        "${_SPDLOG_NAMESPACE_PATCH_PREFIX}${_SPDLOG_NAMESPACE_PATCH_SUFFIX}")
else()
    file(WRITE "${_SPDLOG_NAMESPACE_PATCH}" "${_SPDLOG_NAMESPACE_PATCH_CONTENT}")
endif()

set(SPDLOG_PATCHES
    "${_TRANSFER_ENGINE_REPOSITORY_ROOT}/third_party/patches/spdlog/change-filename.patch"
    "${_SPDLOG_NAMESPACE_PATCH}"
    "${_TRANSFER_ENGINE_REPOSITORY_ROOT}/third_party/patches/spdlog/change-rotating-file-sink.patch"
)
foreach(_SPDLOG_PATCH ${SPDLOG_PATCHES})
    if (NOT EXISTS "${_SPDLOG_PATCH}")
        message(FATAL_ERROR
            "TransferEngine standalone build requires the YuanRong spdlog patch: ${_SPDLOG_PATCH}")
    endif()
endforeach()

set(SPDLOG_CMAKE_OPTIONS
    -DCMAKE_BUILD_TYPE=Release
    -DSPDLOG_BUILD_SHARED=ON
    -DSPDLOG_BUILD_EXAMPLE=OFF
    -DSPDLOG_BUILD_TESTS=OFF
    -DSPDLOG_BUILD_BENCH=OFF
)

TE_ADD_THIRDPARTY_LIB(SPDLOG
    URL ${SPDLOG_URL}
    SHA256 ${SPDLOG_SHA256}
    VERSION ${SPDLOG_VERSION}
    CONF_OPTIONS ${SPDLOG_CMAKE_OPTIONS}
    PATCHES ${SPDLOG_PATCHES}
    CXX_FLAGS ${TRANSFER_ENGINE_THIRDPARTY_SAFE_FLAGS})

set(_TRANSFER_ENGINE_SPDLOG_INSTALL_ROOT "${SPDLOG_ROOT}")
find_library(TRANSFER_ENGINE_SPDLOG_LIBRARY
    NAMES ds-spdlog
    PATHS "${_TRANSFER_ENGINE_SPDLOG_INSTALL_ROOT}"
    PATH_SUFFIXES lib lib64
    REQUIRED
    NO_DEFAULT_PATH)

set(spdlog_DIR "${_TRANSFER_ENGINE_SPDLOG_INSTALL_ROOT}/lib/cmake/spdlog")
if (EXISTS "${_TRANSFER_ENGINE_SPDLOG_INSTALL_ROOT}/lib64/cmake/spdlog")
    set(spdlog_DIR "${_TRANSFER_ENGINE_SPDLOG_INSTALL_ROOT}/lib64/cmake/spdlog")
endif()
unset(SPDLOG_ROOT)
find_package(spdlog ${SPDLOG_VERSION} REQUIRED PATHS "${spdlog_DIR}" NO_DEFAULT_PATH)

set(TRANSFER_ENGINE_SPDLOG_ROOT "${_TRANSFER_ENGINE_SPDLOG_INSTALL_ROOT}" CACHE INTERNAL "")
set(TRANSFER_ENGINE_SPDLOG_LIB_PATH "${SPDLOG_LIB_PATH}" CACHE INTERNAL "")
