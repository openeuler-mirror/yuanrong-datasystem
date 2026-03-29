cmake_minimum_required(VERSION 3.14)

include(cmake/prelude.cmake)

project(
    p2p_transfer
    VERSION 0.1.0
    DESCRIPTION "Fast NPU peer to peer transfer library"
    LANGUAGES CXX
)

set(CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake/module" ${CMAKE_MODULE_PATH})
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
include_directories(SYSTEM ${ASCEND_INCLUDE_DIR}/experiment)
include_directories(SYSTEM ${ASCEND_INCLUDE_DIR}/experiment/runtime)
include_directories(SYSTEM ${ASCEND_INCLUDE_DIR}/experiment/msprof)
include_directories(SYSTEM ${ASCEND_INCLUDE_DIR}/experiment/slog/toolchain)
if(EXISTS ${ASCEND_INCLUDE_DIR}/../pkg_inc/runtime)
    include_directories(SYSTEM ${ASCEND_INCLUDE_DIR}/../pkg_inc/runtime)
endif()

# ---- Libraries ----
set(DEPENDS_LIBS pthread dl m ${ASCENDCL_LIBRARY} ${ASCEND_RUNTIME_LIBRARY} ${ASCEND_HCCL_TRANSFER_LIBRARY} ${ASCEND_RA_LIBRARY})

# ---- Proto ----
add_custom_target(p2p_proto_lib_depend)
include("${CMAKE_CURRENT_SOURCE_DIR}/../../../../../cmake/proto.cmake")

if (TARGET protobuf::libprotobuf)
    message(STATUS "Using protobuf targets preloaded by parent project")
    get_property(Protobuf_INCLUDE_DIR TARGET protobuf::libprotobuf PROPERTY INTERFACE_INCLUDE_DIRECTORIES)
    include_directories(SYSTEM ${Protobuf_INCLUDE_DIR})
    list(APPEND DEPENDS_LIBS protobuf::libprotobuf)
elseif (DEFINED TRANSFER_ENGINE_PROTOBUF_LIBRARY AND DEFINED TRANSFER_ENGINE_PROTOBUF_INCLUDE_DIR)
    message(STATUS "Using transfer_engine bundled protobuf from ${TRANSFER_ENGINE_PROTOBUF_ROOT}")
    include_directories(SYSTEM ${TRANSFER_ENGINE_PROTOBUF_INCLUDE_DIR})
    list(APPEND DEPENDS_LIBS ${TRANSFER_ENGINE_PROTOBUF_LIBRARY})
    if (DEFINED TRANSFER_ENGINE_PROTOBUF_EXTRA_LIBRARIES)
        list(APPEND DEPENDS_LIBS ${TRANSFER_ENGINE_PROTOBUF_EXTRA_LIBRARIES})
    endif()
else()
    if (DEFINED TRANSFER_ENGINE_PROTOBUF_ROOT AND NOT DEFINED Protobuf_ROOT_DIR)
        set(Protobuf_ROOT_DIR "${TRANSFER_ENGINE_PROTOBUF_ROOT}")
    endif()
    find_package(Protobuf REQUIRED)
    get_property(Protobuf_INCLUDE_DIR TARGET protobuf::libprotobuf PROPERTY INTERFACE_INCLUDE_DIRECTORIES)
    include_directories(SYSTEM ${Protobuf_INCLUDE_DIR})
    list(APPEND DEPENDS_LIBS protobuf::libprotobuf)
endif ()

file(MAKE_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/generated)
te_generate_proto_cpp(P2P_PROTO_SRCS_HCCS P2P_PROTO_HDRS_HCCS
    "${CMAKE_CURRENT_BINARY_DIR}/generated"
    PROTO_FILES "include/communicator/hccs-ipc/proto/HccsIpcInitMsg.proto"
    SOURCE_ROOT "${CMAKE_CURRENT_SOURCE_DIR}"
    PROTO_DEPEND p2p_proto_lib_depend)

te_generate_proto_cpp(P2P_PROTO_SRCS_ROCE P2P_PROTO_HDRS_ROCE
    "${CMAKE_CURRENT_BINARY_DIR}/generated"
    PROTO_FILES "include/communicator/roce/proto/RoceInitMsg.proto"
    SOURCE_ROOT "${CMAKE_CURRENT_SOURCE_DIR}"
    PROTO_DEPEND p2p_proto_lib_depend)

# ---- Declare library ----
add_library(
    p2p_transfer
    source/p2p.cpp
    source/tools/Status.cpp
    source/tools/logging.cpp
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
    source/npu/RdmaAgent.cpp
    source/npu/RdmaDev.cpp
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

add_library(p2p_transfer::p2p_transfer ALIAS p2p_transfer)

add_dependencies(p2p_transfer p2p_proto_lib_depend)

include(GenerateExportHeader)
generate_export_header(
    p2p_transfer
    BASE_NAME p2p_transfer
    EXPORT_FILE_NAME export/p2p_transfer/p2p_transfer_export.h
    CUSTOM_CONTENT_FROM_VARIABLE pragma_suppress_c4251
)

if(NOT BUILD_SHARED_LIBS)
    target_compile_definitions(p2p_transfer PUBLIC
        P2P_TRANSFER_STATIC_DEFINE
        OPEN_BUILD_PROJECT
    )
else()
    target_compile_definitions(p2p_transfer PUBLIC
        OPEN_BUILD_PROJECT
    )
endif()

set_target_properties(
    p2p_transfer PROPERTIES
    CXX_VISIBILITY_PRESET hidden
    VISIBILITY_INLINES_HIDDEN YES
    VERSION "${PROJECT_VERSION}"
    SOVERSION "${PROJECT_VERSION_MAJOR}"
    EXPORT_NAME p2p_transfer
    OUTPUT_NAME p2p_transfer
)

target_include_directories(
    p2p_transfer ${warning_guard}
    PUBLIC
    "\$<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/include>"
)

target_include_directories(
    p2p_transfer SYSTEM
    PUBLIC
    "\$<BUILD_INTERFACE:${PROJECT_BINARY_DIR}/export>"
)

# Add generated proto headers
target_include_directories(
    p2p_transfer PRIVATE
    ${CMAKE_CURRENT_BINARY_DIR}/generated
)

#SecureC
if (DEFINED SECUREC_ROOT_DIR)
    set(SecureC_ROOT ${SECUREC_ROOT_DIR})
    find_package(SecureC REQUIRED)
    include_directories(${SECUREC_INCLUDE_DIR})
    list(APPEND DEPENDS_LIBS ${SECUREC_LIBRARY})
else()
    find_library(SECUREC_LIBRARY NAMES securec)
    if (SECUREC_LIBRARY)
        list(APPEND DEPENDS_LIBS ${SECUREC_LIBRARY})
    else()
        message(WARNING "libsecurec not found, continue without explicit securec link")
    endif()
endif ()

target_compile_features(p2p_transfer PUBLIC cxx_std_11)

if (CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
    target_compile_options(p2p_transfer PRIVATE
        -Wno-error=reorder
        -Wno-error=unused-parameter
        -Wno-error=switch
        -Wno-error=sign-compare
        -Wno-error=missing-field-initializers
        -Wno-error=unused-function
        -Wno-error=unused-variable
    )
endif()

if (TARGET glog::glog)
    list(APPEND DEPENDS_LIBS glog::glog)
    target_compile_definitions(p2p_transfer PRIVATE P2P_TRANSFER_USE_GLOG=1)
endif()

target_link_libraries(p2p_transfer ${DEPENDS_LIBS})

# ---- Install rules ----

if(NOT CMAKE_SKIP_INSTALL_RULES)
  include(cmake/install-rules.cmake)
endif()

# ---- Developer mode ----

if(NOT p2p_transfer_DEVELOPER_MODE)
  return()
elseif(NOT PROJECT_IS_TOP_LEVEL)
  message(
      AUTHOR_WARNING
      "Developer mode is intended for developers of p2p_transfer"
  )
endif()

include(cmake/dev-mode.cmake)
