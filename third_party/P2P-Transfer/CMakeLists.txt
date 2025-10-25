cmake_minimum_required(VERSION 3.14)

include(cmake/prelude.cmake)

project(
    p2p-transfer
    VERSION 0.1.0
    DESCRIPTION "Fast NPU peer to peer transfer library"
    HOMEPAGE_URL "https://example.com/SIR/usched/P2P-Transfer"
    LANGUAGES CXX
)

set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_CURRENT_SOURCE_DIR}/cmake/module")
include(cmake/project-is-top-level.cmake)
include(cmake/variables.cmake)

# ---- Ascend ----

# The environment variable ASCEND_CUSTOM_PATH is used to locate the Ascend install path.
# So cmake can find the header files and libraries in the compile stage.
# If user don't set ASCEND_CUSTOM_PATH, find Ascend in `/usr/local/Ascend/ascend-toolkit/latest` in default.
if (DEFINED ENV{ASCEND_HOME_PATH})
    set(Ascend_ROOT $ENV{ASCEND_HOME_PATH})
elseif(DEFINED ENV{ASCEND_CUSTOM_PATH})
    set(Ascend_ROOT $ENV{ASCEND_CUSTOM_PATH}/latest)
else()
    set(Ascend_ROOT /usr/local/Ascend/ascend-toolkit/latest)
endif()

find_package(Ascend REQUIRED)
 
include_directories(SYSTEM ${ASCEND_INCLUDE_DIR})
# Starting from CANN 8, runtime and dependencies are available in include/experiment
include_directories(SYSTEM ${ASCEND_INCLUDE_DIR}/experiment/runtime)
include_directories(SYSTEM ${ASCEND_INCLUDE_DIR}/experiment/msprof)

# ---- Libraries ----
set(DEPENDS_LIBS pthread dl m ${ASCENDCL_LIBRARY} ${ASCEND_RUNTIME_LIBRARY} ${ASCEND_RA_LIBRARY} ${ASCEND_TSD_CLIENT_LIBRARY})

# ---- Proto ----
add_custom_target(p2p_proto_lib_depend)

if (DEFINED Protobuf_ROOT_DIR)
    message(STATUS "absl_ROOT_DIR in p2phccl.cmake: ${absl_ROOT_DIR}")
    message(STATUS "ZLIB_ROOT_DIR in p2phccl.cmake: ${ZLIB_ROOT_DIR}")
    message(STATUS "Protobuf_ROOT_DIR in p2phccl.cmake:: ${Protobuf_ROOT_DIR}")
    set(ZLIB_ROOT ${ZLIB_ROOT_DIR})
    find_package(ZLIB REQUIRED)
    set(absl_DIR ${absl_ROOT_DIR}/lib64/cmake/absl)
    find_package(utf8_range REQUIRED PATHS ${Protobuf_ROOT_DIR} CONFIG)
    find_package(Protobuf REQUIRED PATHS ${Protobuf_ROOT_DIR} CONFIG)
    get_property(Protobuf_INCLUDE_DIR TARGET protobuf::libprotobuf PROPERTY INTERFACE_INCLUDE_DIRECTORIES)
    get_property(absl_INCLUDE_DIR TARGET absl::base PROPERTY INTERFACE_INCLUDE_DIRECTORIES)
    include_directories(SYSTEM ${Protobuf_INCLUDE_DIR})
    include_directories(SYSTEM ${absl_INCLUDE_DIR})
    find_library(PROTOBUF_LIBRARY NAMES protobuf PATHS ${Protobuf_ROOT_DIR}/lib64)
    list(APPEND PROTOBUF_LIBRARY ${ZLIB_LIBRARIES})
    message(STATUS "PROTOBUF_LIBRARY: ${PROTOBUF_LIBRARY}")
    list(APPEND DEPENDS_LIBS ${PROTOBUF_LIBRARY})
    link_directories(${PROTOBUF_LIBRARY})
    set(CMAKE_INSTALL_RPATH_USE_LINK_PATH TRUE)

    # TODO: needs to be passed
    include(${Datasystem_CMAKE_ROOT_DIR}/util.cmake)
    set(Protobuf_LIB_PATH ${Protobuf_ROOT_DIR}/lib64)

    file(MAKE_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/generated)
    generate_proto_cpp(P2P_PROTO_SRCS_HCCS P2P_PROTO_HDRS_HCCS ${CMAKE_CURRENT_BINARY_DIR}/generated/include/communicator/hccs-ipc/proto/
                   PROTO_FILES  "${CMAKE_SOURCE_DIR}/include/communicator/hccs-ipc/proto/HccsIpcInitMsg.proto"
                   SOURCE_ROOT  "${CMAKE_SOURCE_DIR}/include/communicator/hccs-ipc/proto"
                   PROTO_DEPEND p2p_proto_lib_depend)

    generate_proto_cpp(P2P_PROTO_SRCS_ROCE P2P_PROTO_HDRS_ROCE ${CMAKE_CURRENT_BINARY_DIR}/generated/include/communicator/roce/proto/
        PROTO_FILES  "${CMAKE_SOURCE_DIR}/include/communicator/roce/proto/RoceInitMsg.proto"
        SOURCE_ROOT  "${CMAKE_SOURCE_DIR}/include/communicator/roce/proto"
        PROTO_DEPEND p2p_proto_lib_depend)
else()
    message(STATUS "Protobuf_ROOT_DIR is not defined")
    find_package(Protobuf REQUIRED)
    list(APPEND DEPENDS_LIBS protobuf)

    file(MAKE_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/generated)
    protobuf_generate(
     LANGUAGE cpp
     DEPENDENCIES p2p_proto_lib_depend
     PROTOS include/communicator/hccs-ipc/proto/HccsIpcInitMsg.proto
     PROTOC_OUT_DIR "${CMAKE_CURRENT_BINARY_DIR}/generated"
     OUT_VAR P2P_PROTO_SRCS_HCCS)

    protobuf_generate(
     LANGUAGE cpp
     DEPENDENCIES p2p_proto_lib_depend
     PROTOS include/communicator/roce/proto/RoceInitMsg.proto
     PROTOC_OUT_DIR "${CMAKE_CURRENT_BINARY_DIR}/generated"
     OUT_VAR P2P_PROTO_SRCS_ROCE)
endif ()

# ---- Declare library ----
add_library(
    p2p-transfer_p2p-transfer
    source/p2p.cpp
    source/tools/Status.cpp
    source/tools/env.cpp
    source/tools/hccl-convert.cpp
    source/tools/host-interface.cpp
    source/npu/RaWrapper.cpp
    source/npu/P2PMem.cpp
    source/npu/P2PNotify.cpp
    source/npu/LocalNotify.cpp
    source/npu/PeerManager.cpp
    source/npu/P2PStream.cpp
    source/npu/ffts/dispatcher_ffts.cpp
    source/npu/Hccp.cpp
    source/npu/RdmaAgent.cpp
    source/npu/RdmaNotify.cpp
    source/npu/RdmaQp.cpp
    source/npu/RdmaSocket.cpp
    source/npu/NotifyValueMem.cpp
    source/npu/NotifyValueMem.cpp
    source/communicator/hccs-ipc/HccsReceiver.cpp
    source/communicator/hccs-ipc/HccsSender.cpp
    source/communicator/roce/RoceReceiver.cpp
    source/communicator/roce/RoceSender.cpp
    source/communicator/P2PCommunicator.cpp
    source/communication/TcpServer.cpp
    source/communication/TcpClient.cpp
    ${P2P_PROTO_SRCS_HCCS}
    ${P2P_PROTO_SRCS_ROCE}
)

add_library(p2p-transfer::p2p-transfer ALIAS p2p-transfer_p2p-transfer)

add_dependencies(p2p-transfer_p2p-transfer p2p_proto_lib_depend)

include(GenerateExportHeader)
generate_export_header(
    p2p-transfer_p2p-transfer
    BASE_NAME p2p-transfer
    EXPORT_FILE_NAME export/p2p-transfer/p2p-transfer_export.h
    CUSTOM_CONTENT_FROM_VARIABLE pragma_suppress_c4251
)

if(NOT BUILD_SHARED_LIBS)
  target_compile_definitions(p2p-transfer_p2p-transfer PUBLIC P2P_TRANSFER_STATIC_DEFINE)
endif()

set_target_properties(
    p2p-transfer_p2p-transfer PROPERTIES
    CXX_VISIBILITY_PRESET hidden
    VISIBILITY_INLINES_HIDDEN YES
    VERSION "${PROJECT_VERSION}"
    SOVERSION "${PROJECT_VERSION_MAJOR}"
    EXPORT_NAME p2ptransfer
    OUTPUT_NAME p2ptransfer
)

target_include_directories(
    p2p-transfer_p2p-transfer ${warning_guard}
    PUBLIC
    "\$<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/include>"
)

target_include_directories(
    p2p-transfer_p2p-transfer SYSTEM
    PUBLIC
    "\$<BUILD_INTERFACE:${PROJECT_BINARY_DIR}/export>"
)

# Add generated proto headers
target_include_directories(
    p2p-transfer_p2p-transfer PRIVATE
    ${CMAKE_CURRENT_BINARY_DIR}/generated
)

#SecureC
if (DEFINED SECUREC_ROOT_DIR)
    set(SecureC_ROOT ${SECUREC_ROOT_DIR})
    find_package(SecureC REQUIRED)
    include_directories(${SECUREC_INCLUDE_DIR})
    list(APPEND DEPENDS_LIBS ${SECUREC_LIBRARY})
else()
    list(APPEND DEPENDS_LIBS securec)
endif ()

target_compile_features(p2p-transfer_p2p-transfer PUBLIC cxx_std_11)

target_link_libraries(p2p-transfer_p2p-transfer ${DEPENDS_LIBS})

# ---- Install rules ----

if(NOT CMAKE_SKIP_INSTALL_RULES)
  include(cmake/install-rules.cmake)
endif()

# ---- Developer mode ----

if(NOT p2p-transfer_DEVELOPER_MODE)
  return()
elseif(NOT PROJECT_IS_TOP_LEVEL)
  message(
      AUTHOR_WARNING
      "Developer mode is intended for developers of p2p-transfer"
  )
endif()

include(cmake/dev-mode.cmake)
