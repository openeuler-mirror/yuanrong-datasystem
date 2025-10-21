set(pybind11_VERSION 2.10.3)
if (NOT "$ENV{DS_LOCAL_LIBS_DIR}" STREQUAL "")
  set(pybind11_URL "$ENV{DS_LOCAL_LIBS_DIR}/opensource_third_party/v2.10.3.zip")
else()
  set(pybind11_URL "https://gitee.com/mirrors/pybind11/repository/archive/v2.10.3.zip")
endif()
set(pybind11_SHA256 "bea74700d4c841e5da499a1fccb680ad61716a4f0e94940ce372c92fbdb375c7")

set(pybind11_CMAKE_OPTIONS
    -DPYBIND11_TEST:BOOL=OFF 
    -DPYBIND11_LTO_CXX_FLAGS:BOOL=FALSE
    -DCMAKE_BUILD_TYPE:STRING=Release)

set(pybind11_CXX_FLAGS ${THIRDPARTY_SAFE_FLAGS})

add_thirdparty_lib(pybind11 
  URL ${pybind11_URL}
  SHA256 ${pybind11_SHA256}
  FAKE_SHA256 ${pybind11_FAKE_SHA256}
  VERSION ${pybind11_VERSION}
  CONF_OPTIONS ${pybind11_CMAKE_OPTIONS}
  CXX_FLAGS ${pybind11_CXX_FLAGS})

if (${CMAKE_VERSION} VERSION_GREATER_EQUAL "3.15")
  set(Python3_FIND_STRATEGY LOCATION)
endif()

find_package(Python3 COMPONENTS Interpreter Development)
if (NOT Python3_FOUND)
  message(FATAL_ERROR "Python interpreter or development not found!")
else()
  set(PYTHON_VERSION "${Python3_VERSION_MAJOR}.${Python3_VERSION_MINOR}")
  message(STATUS "Found python interpreter, version: ${Python3_VERSION_MAJOR}.${Python3_VERSION_MINOR}.${Python3_VERSION_PATCH}")
  message(STATUS "Found python interpreter, path: ${Python3_EXECUTABLE}")
endif()

set(pybind11_DIR ${pybind11_ROOT})
set(PYBIND11_PYTHON_VERSION ${PYTHON_VERSION} CACHE STRING "")
find_package(pybind11 ${pybind11_VERSION} REQUIRED)

include_directories(${pybind11_INCLUDE_DIRS})