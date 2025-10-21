# cmake version
cmake_minimum_required(VERSION 3.14.1)

# project
project(ds_example LANGUAGES CXX C)

set(CMAKE_CXX_STANDARD 17)

link_directories(${CMAKE_CURRENT_SOURCE_DIR}/lib)

include_directories(${CMAKE_CURRENT_SOURCE_DIR}/include)

add_executable(kv_example kv_cache_example.cpp)

target_link_libraries(kv_example PUBLIC datasystem)