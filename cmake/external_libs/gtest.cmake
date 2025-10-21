set(gtest_VERSION 1.12.1)
if (NOT "$ENV{DS_LOCAL_LIBS_DIR}" STREQUAL "")
  set(gtest_URL "$ENV{DS_LOCAL_LIBS_DIR}/opensource_third_party/release-1.12.1.zip")
else()
  set(gtest_URL "https://gitee.com/mirrors/googletest/repository/archive/release-1.12.1.zip")
endif()
set(gtest_SHA256 "4f1037f17462dbcc4d715f2fc8212c03431ad06e7a4b73bb49b3f534a13b21f1")

set(gtest_CMAKE_OPTIONS
    -DCMAKE_BUILD_TYPE:STRING=Release)

set(gtest_CXX_FLAGS ${THIRDPARTY_SAFE_FLAGS})

add_thirdparty_lib(GTest 
  URL ${gtest_URL}
  SHA256 ${gtest_SHA256}
  FAKE_SHA256 ${gtest_FAKE_SHA256}
  VERSION ${gtest_VERSION}
  CONF_OPTIONS ${gtest_CMAKE_OPTIONS}
  CXX_FLAGS ${gtest_CXX_FLAGS})

set(CMAKE_PREFIX_PATH ${GTest_ROOT})
find_package(GTest ${gtest_VERSION} CONFIG REQUIRED)

get_property(GTEST_INCLUDE_DIRS TARGET GTest::gtest PROPERTY INTERFACE_INCLUDE_DIRECTORIES)
include_directories(SYSTEM ${GTEST_INCLUDE_DIRS})
