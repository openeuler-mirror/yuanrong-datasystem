set(p2p-transfer_VERSION 0.1.0)
# Don't need to use DS_PACKAGE environments because it's not opensource third-party library.
set(p2p-transfer_URL "file://${CMAKE_SOURCE_DIR}/third_party/P2P-Transfer")
set(p2p-transfer_SHA256 "8054c2972aa4313d34236e8f128e8bae8807e690f70730cc10d24c390172aeda")

set(p2p-transfer_CMAKE_OPTIONS
    -DWITH_CUSTOM_PREFIX:BOOL=ON
    -DSECUREC_ROOT_DIR:PATH=${SecureC_ROOT}
    -DZLIB_ROOT_DIR:PATH=${ZLIB_ROOT}
    -DProtobuf_ROOT_DIR:PATH=${Protobuf_ROOT}
    -Dabsl_ROOT_DIR:PATH=${absl_ROOT}
    -DCMAKE_CXX_STANDARD=17
    -DCMAKE_FIND_USE_CMAKE_SYSTEM_PATH:BOOL=FALSE
    -DDatasystem_CMAKE_ROOT_DIR:PATH=${CMAKE_SOURCE_DIR}/cmake/
    -DCMAKE_BUILD_TYPE:STRING=Release)

message(STATUS "Protobuf_ROOT: ${Protobuf_ROOT}")

set(p2p-transfer_CXX_FLAGS ${THIRDPARTY_SAFE_FLAGS})

add_thirdparty_lib(p2p-transfer 
  URL ${p2p-transfer_URL}
  SHA256 ${p2p-transfer_SHA256}
  FAKE_SHA256 ${p2p-transfer_FAKE_SHA256}
  VERSION ${p2p-transfer_VERSION}
  CONF_OPTIONS ${p2p-transfer_CMAKE_OPTIONS}
  CXX_FLAGS ${p2p-transfer_CXX_FLAGS}
  C_FLAGS ${p2p-transfer_C_FLAGS}
  PATCHES ${p2p-transfer_PATCHES})

set(p2p-transfer_DIR ${p2p-transfer_ROOT})
find_package(p2p-transfer ${p2p-transfer_VERSION} REQUIRED)

set(p2p-transfer_INCLUDE_DIR "${p2p-transfer_ROOT}/include")
message(STATUS "p2p-transfer include dir: ${p2p-transfer_INCLUDE_DIR}" )
include_directories(SYSTEM ${p2p-transfer_INCLUDE_DIR})