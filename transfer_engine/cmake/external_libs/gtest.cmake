if (TARGET GTest::gtest)
    return()
endif()

set(gtest_VERSION 1.14.0)
if (NOT "$ENV{DS_LOCAL_LIBS_DIR}" STREQUAL "")
    set(gtest_URL "$ENV{DS_LOCAL_LIBS_DIR}/opensource_third_party/v1.14.0.zip")
else()
    set(gtest_URL "https://github.com/google/googletest/archive/refs/tags/v1.14.0.zip")
endif()
set(gtest_SHA256 "1f357c27ca988c3f7c6b4bf68a9395005ac6761f034046e9dde0896e3aba00e4")

set(gtest_CMAKE_OPTIONS
    -DCMAKE_BUILD_TYPE=Release
    -DBUILD_SHARED_LIBS=ON
    -DINSTALL_GTEST=ON
    -DBUILD_GMOCK=ON)

TE_ADD_THIRDPARTY_LIB(GTest
    URL ${gtest_URL}
    SHA256 ${gtest_SHA256}
    VERSION ${gtest_VERSION}
    CONF_OPTIONS ${gtest_CMAKE_OPTIONS}
    CXX_FLAGS ${TRANSFER_ENGINE_THIRDPARTY_SAFE_FLAGS})

set(GTest_DIR "${GTest_ROOT}/lib/cmake/GTest")
if (EXISTS "${GTest_ROOT}/lib64/cmake/GTest")
    set(GTest_DIR "${GTest_ROOT}/lib64/cmake/GTest")
endif()
find_package(GTest REQUIRED PATHS "${GTest_DIR}" NO_DEFAULT_PATH)
set(TRANSFER_ENGINE_GTEST_ROOT "${GTest_ROOT}" CACHE INTERNAL "")
set(TRANSFER_ENGINE_GTEST_LIB_PATH "${GTest_LIB_PATH}" CACHE INTERNAL "")
