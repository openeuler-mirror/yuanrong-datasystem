function(ds_static_sdk_dependency_options name)
    string(TOLOWER "${name}" _name)
    set(_options ${ARG_CONF_OPTIONS})
    if (NOT ARG_TOOLCHAIN OR ARG_TOOLCHAIN STREQUAL "cmake")
        list(APPEND _options
            -DBUILD_SHARED_LIBS:BOOL=OFF -DCMAKE_POSITION_INDEPENDENT_CODE:BOOL=ON
            -DCMAKE_INSTALL_LIBDIR:STRING=lib
            "-DCMAKE_PROJECT_INCLUDE:FILEPATH=${CMAKE_SOURCE_DIR}/cmake/static-sdk/find-static-libs.cmake")
    endif()
    if (_name STREQUAL "absl")
        list(APPEND _options -DABSL_BUILD_MONOLITHIC_SHARED_LIBS:BOOL=OFF)
    elseif (_name STREQUAL "gflags")
        list(APPEND _options -DGFLAGS_BUILD_STATIC_LIBS:BOOL=ON -DGFLAGS_BUILD_SHARED_LIBS:BOOL=OFF
            -DGFLAGS_REGISTER_BUILD_DIR:BOOL=OFF -DGFLAGS_REGISTER_INSTALL_PREFIX:BOOL=OFF)
    elseif (_name STREQUAL "zlib")
        list(APPEND _options -DZLIB_BUILD_SHARED:BOOL=OFF -DZLIB_BUILD_STATIC:BOOL=ON)
    elseif (_name STREQUAL "protobuf")
        list(APPEND _options -Dprotobuf_BUILD_SHARED_LIBS:BOOL=OFF -Dprotobuf_BUILD_STATIC_LIBS:BOOL=ON
            -DCMAKE_SKIP_RPATH:BOOL=OFF)
    elseif (_name STREQUAL "grpc")
        list(APPEND _options -DgRPC_BUILD_CSHARP_EXT:BOOL=OFF -DgRPC_BUILD_GRPC_CSHARP_PLUGIN:BOOL=OFF
            -DOPENSSL_USE_STATIC_LIBS:BOOL=ON -DZLIB_USE_STATIC_LIBS:BOOL=ON)
    elseif (_name STREQUAL "brpc")
        list(APPEND _options -DWITH_BRPC_SHARED_LIB:BOOL=OFF -DDS_PROTOBUF_CONFIG:BOOL=ON
            -DOPENSSL_USE_STATIC_LIBS:BOOL=ON)
    elseif (_name STREQUAL "spdlog")
        list(APPEND _options -DSPDLOG_BUILD_SHARED:BOOL=OFF -DSPDLOG_FMT_EXTERNAL:BOOL=OFF)
    elseif (_name STREQUAL "openssl")
        list(REMOVE_ITEM _options shared)
        list(APPEND _options no-shared no-dso)
    elseif (_name STREQUAL "curl")
        list(APPEND _options -DBUILD_CURL_EXE:BOOL=OFF -DBUILD_TESTING:BOOL=OFF
            -DBUILD_STATIC_LIBS:BOOL=ON -DCURL_USE_OPENSSL:BOOL=ON -DOPENSSL_USE_STATIC_LIBS:BOOL=ON
            -DCURL_USE_LIBPSL:BOOL=OFF -DCURL_ZSTD:BOOL=OFF -DCURL_BROTLI:BOOL=OFF
            -DUSE_LIBIDN2:BOOL=OFF -DCURL_USE_LIBSSH2:BOOL=OFF -DCURL_USE_GSSAPI:BOOL=OFF
            -DCURL_DISABLE_LDAP:BOOL=ON -DCURL_DISABLE_LDAPS:BOOL=ON
            -DUSE_NGHTTP2:BOOL=OFF -DUSE_NGTCP2:BOOL=OFF -DUSE_QUICHE:BOOL=OFF
            -DCURL_USE_LIBSSH:BOOL=OFF -DCURL_USE_MBEDTLS:BOOL=OFF)
    elseif (_name STREQUAL "zeromq")
        list(APPEND _options -DBUILD_STATIC:BOOL=ON -DBUILD_SHARED:BOOL=OFF
            -DENABLE_DRAFTS:BOOL=OFF -DENABLE_WS:BOOL=OFF -DWITH_TLS:BOOL=OFF)
    elseif (_name STREQUAL "leveldb")
        list(APPEND _options -DCMAKE_DISABLE_FIND_PACKAGE_Snappy:BOOL=ON
            -DHAVE_SNAPPY:BOOL=OFF -DHAVE_CRC32C:BOOL=OFF)
    elseif (_name STREQUAL "tbb")
        set(ARG_LINK_FLAGS "" PARENT_SCOPE)
    endif()
    set(ARG_CONF_OPTIONS ${_options} PARENT_SCOPE)
    set(ARG_CXX_FLAGS "${ARG_CXX_FLAGS} -fPIC" PARENT_SCOPE)
    set(ARG_C_FLAGS "${ARG_C_FLAGS} -fPIC" PARENT_SCOPE)
    set(ARG_EXTRA_MSGS "${ARG_EXTRA_MSGS};static-sdk-v1" PARENT_SCOPE)
endfunction()

function(ds_static_sdk_prepare_source name root)
    string(TOLOWER "${name}" _name)
    if (_name STREQUAL "spdlog")
        file(READ "${root}/README.md" _readme)
        message(STATUS "Normalize spdlog README line endings for the DS namespace patch")
        string(REPLACE "\r\n" "\n" _readme "${_readme}")
        file(WRITE "${root}/README.md" "${_readme}")
    endif()
endfunction()
