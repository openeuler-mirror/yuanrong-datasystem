if (TARGET pybind11::module)
    return()
endif()

set(pybind11_VERSION 2.13.6)
if (NOT "$ENV{DS_LOCAL_LIBS_DIR}" STREQUAL "")
    set(pybind11_URL "$ENV{DS_LOCAL_LIBS_DIR}/opensource_third_party/v2.13.6.zip")
else()
    set(pybind11_URL "https://github.com/pybind/pybind11/archive/refs/tags/v2.13.6.zip")
endif()
set(pybind11_SHA256 "d0a116e91f64a4a2d8fb7590c34242df92258a61ec644b79127951e821b47be6")

set(pybind11_CMAKE_OPTIONS
    -DCMAKE_BUILD_TYPE=Release
    -DPYBIND11_TEST=OFF
    -DPYBIND11_INSTALL=ON
    -DPYBIND11_NOPYTHON=OFF)

TE_ADD_THIRDPARTY_LIB(pybind11
    URL ${pybind11_URL}
    SHA256 ${pybind11_SHA256}
    VERSION ${pybind11_VERSION}
    CONF_OPTIONS ${pybind11_CMAKE_OPTIONS}
    CXX_FLAGS ${TRANSFER_ENGINE_THIRDPARTY_SAFE_FLAGS})

if (${CMAKE_VERSION} VERSION_GREATER_EQUAL "3.15")
    set(Python3_FIND_STRATEGY LOCATION)
endif()
find_package(Python3 COMPONENTS Interpreter Development.Module REQUIRED)

set(pybind11_DIR "${pybind11_ROOT}/share/cmake/pybind11")
if (EXISTS "${pybind11_ROOT}/lib/cmake/pybind11")
    set(pybind11_DIR "${pybind11_ROOT}/lib/cmake/pybind11")
elseif(EXISTS "${pybind11_ROOT}/lib64/cmake/pybind11")
    set(pybind11_DIR "${pybind11_ROOT}/lib64/cmake/pybind11")
endif()
find_package(pybind11 REQUIRED PATHS "${pybind11_DIR}" NO_DEFAULT_PATH)
set(TRANSFER_ENGINE_PYBIND11_ROOT "${pybind11_ROOT}" CACHE INTERNAL "")
