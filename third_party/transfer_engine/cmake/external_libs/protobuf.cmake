include(FetchContent)

function(te_prepare_protobuf)
    if (TARGET protobuf::libprotobuf)
        set(TRANSFER_ENGINE_PROTOBUF_READY ON PARENT_SCOPE)
        return()
    endif()

    set(_protobuf_version "3.25.5")
    set(_protobuf_sha256 "2ed51794f7a1f9da3e4d8ede931ff55206e33b5e49b876966c7b2af523913e54")
    if (NOT "$ENV{DS_LOCAL_LIBS_DIR}" STREQUAL "")
        set(_protobuf_url "$ENV{DS_LOCAL_LIBS_DIR}/opensource_third_party/v${_protobuf_version}.tar.gz")
    else()
        set(_protobuf_url "https://gitee.com/mirrors/protobuf_source/repository/archive/v${_protobuf_version}.tar.gz")
    endif()

    set(_protobuf_patch
        "${CMAKE_SOURCE_DIR}/third_party/patches/protobuf/3.25.5/protobuf_support_gcc_7_3.patch")
    set(_absl_patch
        "${CMAKE_SOURCE_DIR}/third_party/patches/absl/absl_failure_signal_handler.patch")

    set(_absl_commit "20240722")
    set(_absl_sha256 "cf9f05c6e3216aa49d2fb3e0935ce736b1440e7879e1fde626f57f6610aa8b95")
    if (NOT "$ENV{DS_LOCAL_LIBS_DIR}" STREQUAL "")
        set(_absl_url "$ENV{DS_LOCAL_LIBS_DIR}/opensource_third_party/lts_2024_07_22.zip")
    else()
        set(_absl_url "https://gitee.com/mirrors/abseil-cpp/repository/archive/lts_2024_07_22.zip")
    endif()

    find_program(_patch_exe patch)
    if (_patch_exe AND EXISTS "${_protobuf_patch}")
        set(_protobuf_patch_cmd PATCH_COMMAND ${_patch_exe} -p1 -i ${_protobuf_patch})
    else()
        set(_protobuf_patch_cmd "")
        message(WARNING "patch tool or protobuf patch not found, continue without patch: ${_protobuf_patch}")
    endif()
    if (_patch_exe AND EXISTS "${_absl_patch}")
        set(_absl_patch_cmd PATCH_COMMAND ${_patch_exe} -p1 -i ${_absl_patch})
    else()
        set(_absl_patch_cmd "")
        message(WARNING "patch tool or absl patch not found, continue without patch: ${_absl_patch}")
    endif()

    set(CMAKE_POSITION_INDEPENDENT_CODE ON)
    # Build absl/protobuf as static libs so p2p shared object does not expose
    # large absl *.so runtime dependencies.
    set(BUILD_SHARED_LIBS OFF CACHE BOOL "" FORCE)
    set(ABSL_PROPAGATE_CXX_STD ON CACHE BOOL "" FORCE)
    set(ABSL_BUILD_TESTING OFF CACHE BOOL "" FORCE)
    set(ABSL_ENABLE_INSTALL OFF CACHE BOOL "" FORCE)
    FetchContent_Declare(
        absl
        URL ${_absl_url}
        URL_HASH SHA256=${_absl_sha256}
        ${_absl_patch_cmd}
        DOWNLOAD_EXTRACT_TIMESTAMP TRUE
    )
    FetchContent_MakeAvailable(absl)

    # Match datasystem behavior: build protobuf from source instead of using host package.
    set(protobuf_BUILD_TESTS OFF CACHE BOOL "" FORCE)
    set(protobuf_INSTALL OFF CACHE BOOL "" FORCE)
    set(protobuf_BUILD_SHARED_LIBS OFF CACHE BOOL "" FORCE)
    set(protobuf_ABSL_PROVIDER module CACHE STRING "" FORCE)
    set(protobuf_JSONCPP_PROVIDER module CACHE STRING "" FORCE)
    set(protobuf_WITH_ZLIB OFF CACHE BOOL "" FORCE)
    set(utf8_range_ENABLE_INSTALL OFF CACHE BOOL "" FORCE)
    set(ABSL_ROOT_DIR "${absl_SOURCE_DIR}" CACHE PATH "" FORCE)

    FetchContent_Declare(
        protobuf
        URL ${_protobuf_url}
        URL_HASH SHA256=${_protobuf_sha256}
        ${_protobuf_patch_cmd}
        DOWNLOAD_EXTRACT_TIMESTAMP TRUE
    )
    FetchContent_MakeAvailable(protobuf)

    if (NOT COMMAND protobuf_generate AND EXISTS "${protobuf_SOURCE_DIR}/cmake/protobuf-generate.cmake")
        include("${protobuf_SOURCE_DIR}/cmake/protobuf-generate.cmake")
    endif()

    set(TRANSFER_ENGINE_PROTOBUF_READY ON PARENT_SCOPE)
endfunction()
