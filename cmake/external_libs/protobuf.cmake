# protobuf
set(protobuf_VERSIONS 3.25.5 28.3)
if (NOT "$ENV{DS_LOCAL_LIBS_DIR}" STREQUAL "")
    set(protobuf_URLS
        "$ENV{DS_LOCAL_LIBS_DIR}/opensource_third_party/v3.25.5.tar.gz"
        "$ENV{DS_LOCAL_LIBS_DIR}/opensource_third_party/v28.3.tar.gz")
else()
    set(protobuf_URLS
        "https://gitee.com/mirrors/protobuf_source/repository/archive/v3.25.5.tar.gz"
        "https://gitee.com/mirrors/protobuf_source/repository/archive/v28.3.tar.gz")
endif()
set(protobuf_SHA256S
    "2ed51794f7a1f9da3e4d8ede931ff55206e33b5e49b876966c7b2af523913e54"
    "f1670b971d09f0d4fda656871bb17686f1fb5af21b1065538447603a03c57291")

adjuice_thirdparty_version(protobuf)

set(protobuf_CMAKE_OPTIONS
    -Dprotobuf_BUILD_TESTS:BOOL=OFF
    -Dprotobuf_BUILD_SHARED_LIBS:BOOL=ON
    -DCMAKE_BUILD_TYPE:STRING=Release
    -Dprotobuf_ABSL_PROVIDER:STRING=package
    -Dabsl_DIR:PATH=${absl_PKG_PATH}
    -DCMAKE_CXX_STANDARD=17
    -DCMAKE_SKIP_RPATH:BOOL=TRUE
    # zlib is required so protobuf compiles gzip_stream.cc (gated by HAVE_ZLIB); otherwise the
    # resulting libprotobuf.so is missing Gzip{Input,Output}Stream symbols that brpc links against.
    -Dprotobuf_WITH_ZLIB:BOOL=ON
    -DZLIB_ROOT:PATH=${ZLIB_ROOT}
    -DHAVE_ZLIB:BOOL=ON)

if (KVTEST_BUILD_STATIC)
    list(APPEND protobuf_CMAKE_OPTIONS
        -Dprotobuf_BUILD_SHARED_LIBS:BOOL=OFF
        -Dprotobuf_BUILD_STATIC_LIBS:BOOL=ON)
endif()

if (USE_SANITIZER)
    set(protobuf_CXX_FLAGS "${THIRDPARTY_SAFE_FLAGS} ${SANITIZER_FLAGS} -fPIE -pie -fPIC")
else ()
    set(protobuf_CXX_FLAGS "${THIRDPARTY_SAFE_FLAGS} -fPIE -pie -fPIC")
endif ()

# C_FLAGS is required for protobuf 28.x's upb (C library) to compile with -fPIC;
# without it, aarch64 linking fails with R_AARCH64_ADR_PREL_PG_HI21 relocation
# errors. Other .cmake files (brpc/gflags/leveldb) already pass C_FLAGS; protobuf
# was missing it (didn't matter for 3.25.5 which had no upb, but 28.3 does).
set(protobuf_C_FLAGS ${THIRDPARTY_SAFE_FLAGS})

add_thirdparty_lib(Protobuf
  URL ${protobuf_URL}
  SHA256 ${protobuf_SHA256}
  FAKE_SHA256 ${protobuf_FAKE_SHA256}
  VERSION ${protobuf_VERSION}
  CONF_OPTIONS ${protobuf_CMAKE_OPTIONS}
  CXX_FLAGS ${protobuf_CXX_FLAGS}
  C_FLAGS ${protobuf_C_FLAGS})

set(Protobuf_DIR ${Protobuf_ROOT})
if ("${protobuf_VERSION}" STREQUAL "28.3")
    # protobuf 28.3's cmake project version is "5.28.3"; don't pin to 25.5.0.
    find_package(Protobuf REQUIRED PATHS ${Protobuf_ROOT} CONFIG)
else()
    find_package(Protobuf 25.5.0 REQUIRED PATHS ${Protobuf_ROOT} CONFIG)
endif()

get_property(Protobuf_INCLUDE_DIR TARGET protobuf::libprotobuf PROPERTY INTERFACE_INCLUDE_DIRECTORIES)
include_directories(SYSTEM ${Protobuf_INCLUDE_DIR})
