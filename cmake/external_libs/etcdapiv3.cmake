# etcd-cpp-apiv3
set(etcd_cpp_apiv3_VERSION v0.15.4)
 
set(etcd_cpp_apiv3_URLS "https://github.com/etcd-cpp-apiv3/etcd-cpp-apiv3/archive/refs/tags/v0.15.4.tar.gz")
 
set(etcd_cpp_apiv3_SHA256S "4516ecfa420826088c187efd42dad249367ca94ea6cdfc24e3030c3cf47af7b4")  # 替换为实际的 SHA256 哈希
 
if (EXISTS ${Protobuf_ROOT}/lib64)
  set(Protobuf_PKG_PATH ${Protobuf_ROOT}/lib64/cmake/protobuf)
  set(utf8_range_PKG_PATH ${Protobuf_ROOT}/lib64/cmake/utf8_range)
else()
  set(Protobuf_PKG_PATH ${Protobuf_ROOT}/lib/cmake/protobuf)
  set(utf8_range_PKG_PATH ${Protobuf_ROOT}/lib/cmake/utf8_range)
endif()
 
if (EXISTS ${gRPC_ROOT}/lib64)
  set(gRPC_PKG_PATH ${gRPC_ROOT}/lib64/cmake/grpc)
else()
  set(gRPC_PKG_PATH ${gRPC_ROOT}/lib/cmake/grpc)
endif()
 
if (EXISTS ${cpprestsdk_ROOT}/lib64)
  set(CPPREST_PKG_PATH ${cpprestsdk_ROOT}/lib64/cmake/cpprestsdk)
else()
  set(CPPREST_PKG_PATH ${cpprestsdk_ROOT}/lib/cmake/cpprestsdk)
endif()
get_target_property(CPPREST_LIB cpprestsdk::cpprest IMPORTED_LOCATION_RELEASE)
 
set(GRPC_LIBDIR "${gRPC_ROOT}/lib")
set(GRPC_INCLUDE "${gRPC_ROOT}/include")
 
get_target_property(GPR_LIB gRPC::gpr IMPORTED_LOCATION_RELEASE)
get_target_property(GRPC_LIB gRPC::grpc IMPORTED_LOCATION_RELEASE)
get_target_property(GRPC_GRPC++_LIB gRPC::grpc++ IMPORTED_LOCATION_RELEASE)
get_target_property(GRPC_CPP_PLUGIN_LIB gRPC::grpc_cpp_plugin IMPORTED_LOCATION_RELEASE)
get_target_property(GRPC_GRPC++_REFLECTION_LIB gRPC::grpc++_reflection IMPORTED_LOCATION_RELEASE)
 
set(ENV{LD_LIBRARY_PATH} "${GRPC_LIBDIR}:$ENV{LD_LIBRARY_PATH}")
 
set(GRPC_GRPC++_LIB_AND_GRPC_PLUGIN_SUPPORT_LIB
    ${GRPC_GRPC++_LIB};${GRPC_PLUGIN_SUPPORT_LIB})
 
# -DCMAKE_BUILD_TYPE=Debug
# -DBUILD_SHARED_LIBS:BOOL=OFF
set(etcd_cpp_apiv3_CMAKE_OPTIONS
    -DCMAKE_BUILD_TYPE:STRING=IMPORTED_LOCATION_RELEASE
    -DCMAKE_CXX_STANDARD=17
    -DgRPC_VERSION=1.65
    -DgRPC_DIR:PATH=${gRPC_PKG_PATH}
    -DGPR_LIBRARY:PATH=${GPR_LIB}
    -DGRPC_LIBRARY:PATH=${GRPC_LIB}
    -DGRPC_GRPC++_LIBRARY:PATH=${GRPC_GRPC++_LIB}
    -DGRPC_CPP_PLUGIN:PATH=${GRPC_CPP_PLUGIN_LIB}
    -DGRPC_GRPC++_REFLECTION_LIBRARY:PATH=${GRPC_GRPC++_REFLECTION_LIB}
    -DGRPC_INCLUDE_DIR:PATH=${GRPC_INCLUDE}
    -DProtobuf_DIR:PATH=${Protobuf_PKG_PATH}
    -DCMAKE_PREFIX_PATH:PATH=${Protobuf_ROOT}
    -DCPPREST_INCLUDE_DIR:PATH=${cpprestsdk_INCLUDE_DIR}
    -DCPPREST_LIB=${CPPREST_LIB}
    -DCMAKE_SKIP_RPATH:BOOL=TRUE
    -DBUILD_SHARED_LIBS:BOOL=OFF
    -Dutf8_range_DIR:PATH=${utf8_range_PKG_PATH}
    -Dabsl_DIR:PATH=${absl_PKG_PATH}
    -Dc-ares_DIR:PATH=${c-ares_PKG_PATH}
    -Dre2_DIR:PATH=${re2_PKG_PATH}
    -DOPENSSL_ROOT_DIR=${OpenSSL_ROOT}
    -DZLIB_ROOT:PATH=${ZLIB_ROOT})
 
set(etcd_cpp_apiv3_CXX_FLAGS "${THIRDPARTY_SAFE_FLAGS} -fPIE -pie -fPIC")
 
set(etcd_cpp_apiv3_PATCHES 
    ${CMAKE_SOURCE_DIR}/third_party/patches/etcdapiv3/etcdapiv3.patch)

add_thirdparty_lib(etcd_cpp_apiv3
    URL ${etcd_cpp_apiv3_URLS}
    SHA256 ${etcd_cpp_apiv3_SHA256S}
    FAKE_SHA256 ${etcd_cpp_apiv3_FAKE_SHA256}
    VERSION ${etcd_cpp_apiv3_VERSION}
    CONF_OPTIONS ${etcd_cpp_apiv3_CMAKE_OPTIONS}
    CXX_FLAGS ${etcd_cpp_apiv3_CXX_FLAGS}
    PATCHES ${etcd_cpp_apiv3_PATCHES})
 
# 查找并包含 etcd-cpp-apiv3
find_package(etcd-cpp-api REQUIRED PATHS ${etcd_cpp_apiv3_ROOT} CONFIG)
 
get_property(etcd_cpp_apiv3_INCLUDE_DIR TARGET etcd-cpp-api PROPERTY INTERFACE_INCLUDE_DIRECTORIES)
include_directories(SYSTEM ${etcd_cpp_apiv3_INCLUDE_DIR})
