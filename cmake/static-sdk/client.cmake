if (NOT CMAKE_SOURCE_DIR STREQUAL CMAKE_CURRENT_SOURCE_DIR)
    message(FATAL_ERROR "Use add_subdirectory(<ds>/cmake/static-sdk) to embed the static client")
endif()
if (NOT CMAKE_SYSTEM_NAME STREQUAL "Linux" OR CMAKE_CROSSCOMPILING)
    message(FATAL_ERROR "Static SDK currently supports native Linux builds only")
endif()
if (CMAKE_CONFIGURATION_TYPES)
    message(FATAL_ERROR "Static SDK requires a single-configuration generator")
endif()
if (NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Release CACHE STRING "Build type" FORCE)
endif()
set(BUILD_SHARED_LIBS OFF)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)
include("${CMAKE_SOURCE_DIR}/cmake/static-sdk/find-static-libs.cmake")
set(CMAKE_PREFIX_PATH "")
set_property(GLOBAL PROPERTY DS_STATIC_SDK_PREFIXES "")
include("${CMAKE_SOURCE_DIR}/cmake/static-sdk/dependencies.cmake")

if (NOT DEFINED DS_GLIBCXX_USE_CXX11_ABI OR DS_GLIBCXX_USE_CXX11_ABI STREQUAL "")
    include(CheckCXXSourceCompiles)
    foreach(_abi 1 0)
        check_cxx_source_compiles("#include <string>
#ifndef _GLIBCXX_USE_CXX11_ABI
#error libstdc++ required
#endif
static_assert(_GLIBCXX_USE_CXX11_ABI == ${_abi}, \"ABI mismatch\");
int main() {}" DS_HAS_LIBSTDCXX_ABI_${_abi})
        if (DS_HAS_LIBSTDCXX_ABI_${_abi})
            set(DS_GLIBCXX_USE_CXX11_ABI ${_abi})
            break()
        endif()
    endforeach()
endif()
foreach(_option WITH_TESTS BUILD_COVERAGE BUILD_PYTHON_API BUILD_JAVA_API BUILD_GO_API
        BUILD_TRANSFER_ENGINE BUILD_HETERO BUILD_HETERO_NPU BUILD_HETERO_GPU BUILD_PIPLN_H2D
        BUILD_WITH_RDMA BUILD_WITH_URMA BUILD_WITH_URMA_MOCK ENABLE_PERF)
    set(${_option} OFF)
endforeach()
set(ASCEND_HIXL_FOUND OFF)
set(ASCEND_HIXL_HCCS_SUPPORTED OFF)
include("${CMAKE_SOURCE_DIR}/cmake/util.cmake")
find_package(Threads REQUIRED)
if (NOT DS_GLIBCXX_USE_CXX11_ABI MATCHES "^[01]$")
    message(FATAL_ERROR "The static SDK requires libstdc++ with DS_GLIBCXX_USE_CXX11_ABI=0 or 1")
endif()
string(APPEND THIRDPARTY_SAFE_FLAGS " -D_GLIBCXX_USE_CXX11_ABI=${DS_GLIBCXX_USE_CXX11_ABI}")
set(CMAKE_CXX_STANDARD 17)
add_compile_definitions(_GLIBCXX_USE_CXX11_ABI=${DS_GLIBCXX_USE_CXX11_ABI})
add_compile_options(-fsigned-char)
include_directories("${CMAKE_SOURCE_DIR}/include" "${CMAKE_SOURCE_DIR}/src"
    "${CMAKE_BINARY_DIR}/src" "${CMAKE_BINARY_DIR}/third_party/protos")
get_git_hash(GIT_HASH)
get_git_branch(GIT_BRANCH)
include("${CMAKE_SOURCE_DIR}/cmake/dependency.cmake")

add_subdirectory(src/datasystem)

# Ask CMake for the resolved link line of a shared consumer, including static cycles.
add_library(ds_client_link_inputs MODULE EXCLUDE_FROM_ALL "${CMAKE_SOURCE_DIR}/src/datasystem/client/tls_bindings.cpp")
target_link_libraries(ds_client_link_inputs PRIVATE datasystem_static)
file(GENERATE OUTPUT "${CMAKE_BINARY_DIR}/client-abi.txt" CONTENT "${DS_GLIBCXX_USE_CXX11_ABI}\n")
