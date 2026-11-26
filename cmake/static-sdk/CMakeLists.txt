cmake_minimum_required(VERSION 3.20)
if (TARGET Datasystem::client_static)
    message(FATAL_ERROR "A DataSystem static SDK is already present in this build")
endif()
if (CMAKE_CROSSCOMPILING OR NOT CMAKE_SYSTEM_NAME STREQUAL "Linux")
    message(FATAL_ERROR "The embedded static SDK requires a native Linux toolchain")
endif()

set(_jobs "${DS_STATIC_SDK_JOBS}")
if (_jobs STREQUAL "")
    set(_jobs "$ENV{CMAKE_BUILD_PARALLEL_LEVEL}")
endif()
if (_jobs STREQUAL "")
    set(_jobs 8)
endif()
if (NOT _jobs MATCHES "^[1-9][0-9]*$")
    message(FATAL_ERROR "Static client parallelism must be a positive integer")
endif()

get_filename_component(_source "${CMAKE_CURRENT_SOURCE_DIR}/../.." ABSOLUTE)
set(_cache "${DS_OPENSOURCE_DIR}")
if (NOT _cache)
    set(_cache "$ENV{DS_OPENSOURCE_DIR}")
endif()
if (_cache)
    get_filename_component(_cache "${_cache}" ABSOLUTE)
endif()
string(SHA256 _variant "${CMAKE_C_COMPILER}|${CMAKE_CXX_COMPILER}|${CMAKE_CXX_COMPILER_VERSION}|${DS_GLIBCXX_USE_CXX11_ABI}|${_cache}")
string(SUBSTRING "${_variant}" 0 16 _variant)
set(_build "${CMAKE_CURRENT_BINARY_DIR}/client/${_variant}")
set(_env "${CMAKE_COMMAND}" -E env --unset=CFLAGS --unset=CXXFLAGS --unset=CPPFLAGS --unset=LDFLAGS
    --unset=CPATH --unset=CPLUS_INCLUDE_PATH --unset=C_INCLUDE_PATH --unset=LIBRARY_PATH
    --unset=CMAKE_PREFIX_PATH --unset=CMAKE_TOOLCHAIN_FILE --unset=PKG_CONFIG_PATH
    "CMAKE_BUILD_PARALLEL_LEVEL=${_jobs}")
set(_options "-DBUILD_THREAD_NUM=${_jobs}"
    "-DCMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER}" "-DDS_GLIBCXX_USE_CXX11_ABI=${DS_GLIBCXX_USE_CXX11_ABI}")
if (CMAKE_C_COMPILER)
    list(APPEND _options "-DCMAKE_C_COMPILER=${CMAKE_C_COMPILER}")
endif()
if (_cache)
    list(APPEND _options "-DDS_OPENSOURCE_DIR=${_cache}")
endif()
file(MAKE_DIRECTORY "${_build}/.cmake/api/v1/query")
file(WRITE "${_build}/.cmake/api/v1/query/codemodel-v2" "")
execute_process(COMMAND ${_env} "${CMAKE_COMMAND}" -S "${_source}" -B "${_build}" -G "Unix Makefiles"
    -DDS_BUILD_STATIC_SDK=ON -DCMAKE_BUILD_TYPE=Release ${_options} COMMAND_ERROR_IS_FATAL ANY)

file(GLOB_RECURSE _recipes CONFIGURE_DEPENDS "${_source}/cmake/*.cmake" "${_source}/cmake/*.cmake.in"
    "${_source}/src/CMakeLists.txt" "${_source}/src/*.cmake" "${_source}/third_party/patches/*.patch")
set_property(DIRECTORY APPEND PROPERTY CMAKE_CONFIGURE_DEPENDS "${_source}/CMakeLists.txt"
    "${_source}/VERSION" ${_recipes})

include("${CMAKE_CURRENT_LIST_DIR}/link.cmake")
add_custom_target(datasystem_static_client_build
    COMMAND ${_env} "${CMAKE_COMMAND}" --build "${_build}" --parallel "${_jobs}" --target datasystem_static
    BYPRODUCTS ${_outputs} VERBATIM)
add_library(Datasystem::client_static STATIC IMPORTED GLOBAL)
set_target_properties(Datasystem::client_static PROPERTIES
    IMPORTED_LOCATION "${_client}"
    INTERFACE_INCLUDE_DIRECTORIES "${_source}/include"
    INTERFACE_COMPILE_FEATURES cxx_std_17
    INTERFACE_POSITION_INDEPENDENT_CODE TRUE
    INTERFACE_COMPILE_OPTIONS "$<$<COMPILE_LANGUAGE:CXX>:SHELL:-U_GLIBCXX_USE_CXX11_ABI -D_GLIBCXX_USE_CXX11_ABI=${_abi}>"
    INTERFACE_LINK_LIBRARIES "${_libraries}"
    INTERFACE_SOURCES "${_source}/src/datasystem/client/tls_bindings.cpp")
add_dependencies(Datasystem::client_static datasystem_static_client_build)
