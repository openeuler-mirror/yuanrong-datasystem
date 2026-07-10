# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Description: Build MLCacheDirect os_transport as a static CMake target for RH2D.

set(MLCacheDirect_VERSION 0.0.9)
set(MLCacheDirect_SHA256 3a0501077bdc81c6ffd84420abf41df8b45445b5b455ebc159ac28c61801c690)

if (MLCACHEDIRECT_URL)
    set(MLCacheDirect_URL "${MLCACHEDIRECT_URL}")
elseif (NOT "$ENV{DS_LOCAL_LIBS_DIR}" STREQUAL "")
    set(MLCacheDirect_URL "$ENV{DS_LOCAL_LIBS_DIR}/opensource_third_party/v${MLCacheDirect_VERSION}.tar.gz")
else()
    set(MLCacheDirect_URL "https://github.com/openeuler-mirror/MLCacheDirect/archive/refs/tags/v${MLCacheDirect_VERSION}.tar.gz")
endif()

message(STATUS "Build MLCacheDirect os_transport from ${MLCacheDirect_URL}")
download_lib_pkg(mlcachedirect ${MLCacheDirect_URL} ${MLCacheDirect_SHA256})

set(MLCacheDirect_SOURCE_DIR ${mlcachedirect_SOURCE_DIR})
set(MLCacheDirect_BINARY_DIR ${mlcachedirect_BINARY_DIR})

set(MLCacheDirect_OS_TRANSPORT_SRCS
    ${MLCacheDirect_SOURCE_DIR}/src/os_transport.c
    ${MLCacheDirect_SOURCE_DIR}/src/os_transport_thread_pool.c
    ${MLCacheDirect_SOURCE_DIR}/src/os_transport_urma.c
    ${MLCacheDirect_SOURCE_DIR}/src/os_transport_log.c
)

set(MLCacheDirect_PREFIXED_INCLUDE_DIR ${CMAKE_BINARY_DIR}/third_party/mlcachedirect_prefixed_include)
file(MAKE_DIRECTORY ${MLCacheDirect_PREFIXED_INCLUDE_DIR}/os-transport)
file(COPY ${MLCacheDirect_SOURCE_DIR}/include/
     DESTINATION ${MLCacheDirect_PREFIXED_INCLUDE_DIR}/os-transport
     FILES_MATCHING PATTERN "*.h")

add_library(mlcachedirect_os_transport_static STATIC ${MLCacheDirect_OS_TRANSPORT_SRCS})
set_target_properties(mlcachedirect_os_transport_static PROPERTIES POSITION_INDEPENDENT_CODE ON)
target_compile_options(mlcachedirect_os_transport_static PRIVATE
    -std=c99
    -Wall
    -Wextra
    -O2
    -fPIC
    -Wno-implicit-function-declaration
)
target_compile_definitions(mlcachedirect_os_transport_static PRIVATE _POSIX_C_SOURCE=200809L)
target_include_directories(mlcachedirect_os_transport_static
    PUBLIC
        ${MLCacheDirect_SOURCE_DIR}/include
        ${MLCacheDirect_PREFIXED_INCLUDE_DIR}
)
target_link_libraries(mlcachedirect_os_transport_static PUBLIC Threads::Threads ${URMA_LIBRARY})

set(OS_TRANSPORT_INCLUDE_DIR ${MLCacheDirect_PREFIXED_INCLUDE_DIR})
set(MLCacheDirect_INCLUDE_DIR ${MLCacheDirect_SOURCE_DIR}/include)
set(MLCacheDirect_LIBRARY mlcachedirect_os_transport_static)
