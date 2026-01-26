# cmake VERSION
cmake_minimum_required(VERSION 3.14.1)

# projcect
project(ds_example LANGUAGES CXX C)

set(CMAKE_CXX_STANDARD 17)

set(BASE_DIR "${CMAKE_CURRENT_SOURCE_DIR}/../..")
file(STRINGS "${BASE_DIR}/VERSION" DS_VERSION)

set(INSTALL_DIR ${BASE_DIR}/output)
set(BUILD_HETERO "on")

if(EXISTS "${BASE_DIR}/config.cmake")
    include(${BASE_DIR}/config.cmake)
endif()

if(NOT EXISTS "${INSTALL_DIR}/datasystem/sdk/cpp")
    file(ARCHIVE_EXTRACT
        INPUT ${INSTALL_DIR}/yr-datasystem-v${version}.tar.gz
        DESTINATION ${INSTALL_DIR}
    )
endif()

SET(CMAKE_PREFIX_PATH "${INSTALL_DIR}/datasystem/sdk/cpp")

find_package(Datasystem ${Datasystem_version} REQUIRED)

include_directories(${Datasystem_INCLUDE_DIR})

if (BUILD_HETERO)
    # The environment variable ASCEND_CUSTOM_PATH is used to locate the Ascend install path.
    # So cmake can find the header files and libraries in the compile stage.
    # If user don't set ASCEND_CUSTOM_PATH, find Ascend in `/usr/local/Ascend` in default.
    if (DEFINED ENV{ASCEND_HOME_PATH})
        set(Ascend_ROOT $ENV{ASCEND_HOME_PATH})
    elseif(DEFINED ENV{ASCEND_CUSTOM_PATH})
        set(Ascend_ROOT $ENV{ASCEND_CUSTOM_PATH}/latest)
    else()
        set(Ascend_ROOT /usr/local/Ascend/ascend-toolkit/latest)
    endif()
    include_directories(${Ascend_ROOT}/include)

    add_executable(hetero_client_example hetero_client_example.cpp)
    target_link_libraries(hetero_client_example datasystem ${Ascend_ROOT}/lib64/libascendcl.so)
endif()

add_executable(datasystem_example datasystem_example.cpp)
target_link_libraries(datasystem_example datasystem)

add_executable(object_client_example object_client_example.cpp)
target_link_libraries(object_client_example datasystem)

add_executable(kv_client_example kv_client_example.cpp)
target_link_libraries(kv_client_example datasystem)

add_executable(stream_client_example  stream_client_example.cpp)
target_link_libraries(stream_client_example datasystem)
