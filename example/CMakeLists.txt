# cmake version
cmake_minimum_required(VERSION 3.14.1)

# projcect
project(ds_example LANGUAGES CXX C)

set(CMAKE_CXX_STANDARD 17)

file(STRINGS "${CMAKE_CURRENT_SOURCE_DIR}/../VERSION" version)

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
endif()

add_executable(ds_example src/ds_example.cpp)
target_link_libraries(ds_example datasystem)

add_executable(object_example src/object_cache/object_example.cpp)
target_link_libraries(object_example datasystem)

add_executable(kv_example src/kv_cache/kv_example.cpp)
target_link_libraries(kv_example datasystem)
