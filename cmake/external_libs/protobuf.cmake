# protobuf
set(protobuf_VERSIONS 3.25.5)
if (NOT "$ENV{DS_LOCAL_LIBS_DIR}" STREQUAL "")
    set(protobuf_URLS "$ENV{DS_LOCAL_LIBS_DIR}/opensource_third_party/v3.25.5.tar.gz")
else()
    set(protobuf_URLS "https://gitee.com/mirrors/protobuf_source/repository/archive/v3.25.5.tar.gz")
endif()
set(protobuf_SHA256S "2ed51794f7a1f9da3e4d8ede931ff55206e33b5e49b876966c7b2af523913e54")

adjuice_thirdparty_version(protobuf)

set(protobuf_CMAKE_OPTIONS
    -Dprotobuf_BUILD_TESTS:BOOL=OFF 
    -Dprotobuf_BUILD_SHARED_LIBS:BOOL=OFF
    -DCMAKE_BUILD_TYPE:STRING=Release
    -Dprotobuf_ABSL_PROVIDER:STRING=package
    -Dabsl_DIR:PATH=${absl_PKG_PATH}
    -DCMAKE_CXX_STANDARD=17
    -DCMAKE_SKIP_RPATH:BOOL=TRUE)

set(protobuf_CXX_FLAGS "${THIRDPARTY_SAFE_FLAGS} -fPIE -pie -fPIC")

set(protobuf_PATCHES
        ${CMAKE_SOURCE_DIR}/third_party/patches/protobuf/3.25.5/protobuf_support_gcc_7_3.patch)

add_thirdparty_lib(Protobuf 
  URL ${protobuf_URL}
  SHA256 ${protobuf_SHA256}
  FAKE_SHA256 ${protobuf_FAKE_SHA256}
  VERSION ${protobuf_VERSION}
  CONF_OPTIONS ${protobuf_CMAKE_OPTIONS}
  CXX_FLAGS ${protobuf_CXX_FLAGS}
  PATCHES ${protobuf_PATCHES})

find_package(Protobuf 25.5.0 REQUIRED PATHS ${Protobuf_ROOT} CONFIG)

get_property(Protobuf_INCLUDE_DIR TARGET protobuf::libprotobuf PROPERTY INTERFACE_INCLUDE_DIRECTORIES)
include_directories(SYSTEM ${Protobuf_INCLUDE_DIR})
