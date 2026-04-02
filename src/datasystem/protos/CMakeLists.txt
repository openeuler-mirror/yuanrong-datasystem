set(PROTO_BUILD_DIR ${CMAKE_BINARY_DIR}/src)
set(PROTO_SRC_DIR ${CMAKE_SOURCE_DIR}/src/datasystem/protos)

# proto_lib_depend is used for GENERATE_PROTO_CPP function
# zmq_proto_lib_depend is used for GENERATE_ZMQ_CPP function
# The target depend DAG example:
#   posix_protos -> zmq_proto_lib_depend -> zmq_proto_lib_depend_posix_protos -> protoc command execute success
add_custom_target(zmq_proto_lib_depend)
add_custom_target(proto_lib_depend)

function(GEN_GLOBAL_PROTO_LIB_DEPEND)
    set(options PUB)
    set(options CLIENT)
    set(oneValueArgs NAME)
    set(multiValueArgs SOURCE DEPEND LINK)
    cmake_parse_arguments(LIB "${options}" "${oneValueArgs}" "${multiValueArgs}" ${ARGN})
    add_library(${LIB_NAME} STATIC ${LIB_SOURCE})
    add_dependencies(${LIB_NAME} ${LIB_DEPEND})
    set_target_properties(${LIB_NAME} PROPERTIES COMPILE_FLAGS "-Wno-unused-parameter")

    target_include_directories(${LIB_NAME} PUBLIC ${PROTO_BUILD_DIR})

    if (${LIB_CLIENT})
        list(APPEND LIB_LINK common_rpc_zmq_client)
    else()
        list(APPEND LIB_LINK common_rpc_zmq)
    endif()

    if (${LIB_PUB})
        target_link_libraries(${LIB_NAME} PUBLIC ${LIB_LINK})
    else ()
        target_link_libraries(${LIB_NAME} PRIVATE ${LIB_LINK})
    endif ()
endfunction()

function(GEN_GLOBAL_PROTO_LIB_DEPEND_SHARED)
    set(options PUB CLIENT)
    set(oneValueArgs NAME)
    set(multiValueArgs SOURCE DEPEND LINK)
    cmake_parse_arguments(LIB "${options}" "${oneValueArgs}" "${multiValueArgs}" ${ARGN})
    add_library(${LIB_NAME} SHARED ${LIB_SOURCE})
    add_dependencies(${LIB_NAME} ${LIB_DEPEND})
    set_target_properties(${LIB_NAME} PROPERTIES COMPILE_FLAGS "-Wno-unused-parameter")

    target_include_directories(${LIB_NAME} PUBLIC ${PROTO_BUILD_DIR})

    if (${LIB_PUB})
        target_link_libraries(${LIB_NAME} PUBLIC ${LIB_LINK})
    else ()
        target_link_libraries(${LIB_NAME} PRIVATE ${LIB_LINK})
    endif ()
endfunction()

# Build the zero layer proto files, utils, zmq meta and so on
GENERATE_PROTO_CPP(UTILS_PROTO_SRCS COMMON_PROTO_HDRS ${PROTO_BUILD_DIR}
        PROTO_FILES ${PROTO_SRC_DIR}/utils.proto
        PROTO_DEPEND proto_lib_depend)
GEN_GLOBAL_PROTO_LIB_DEPEND(
        NAME utils_protos
        SOURCE ${UTILS_PROTO_SRCS}
        DEPEND proto_lib_depend
        LINK ${PROTOBUF_LIBRARIES})
GEN_GLOBAL_PROTO_LIB_DEPEND(CLIENT
        NAME utils_protos_client
        SOURCE ${UTILS_PROTO_SRCS}
        DEPEND proto_lib_depend
        LINK ${PROTOBUF_LIBRARIES})

GENERATE_PROTO_CPP(META_PROTO_SRCS META_PROTO_HDRS ${PROTO_BUILD_DIR}
        PROTO_FILES ${PROTO_SRC_DIR}/meta_zmq.proto
        PROTO_DEPEND proto_lib_depend)
GENERATE_PROTO_CPP(RPC_OPTION_PROTO_SRCS RPC_OPTION_PROTO_HDRS ${PROTO_BUILD_DIR}
        PROTO_FILES ${PROTO_SRC_DIR}/rpc_option.proto
        PROTO_DEPEND proto_lib_depend)

GEN_GLOBAL_PROTO_LIB_DEPEND_SHARED(
        NAME rpc_option_protos
        SOURCE ${RPC_OPTION_PROTO_SRCS}
        DEPEND proto_lib_depend
        LINK protobuf::libprotobuf)
GEN_GLOBAL_PROTO_LIB_DEPEND(
        NAME zmq_meta_protos
        SOURCE ${META_PROTO_SRCS}
        DEPEND proto_lib_depend
        LINK ${PROTOBUF_LIBRARIES})
GEN_GLOBAL_PROTO_LIB_DEPEND(CLIENT
        NAME zmq_meta_protos_client
        SOURCE ${META_PROTO_SRCS}
        DEPEND proto_lib_depend
        LINK ${PROTOBUF_LIBRARIES})

# Build share memory proto lib
GENERATE_ZMQ_CPP(zmq_proto_lib_depend SHARE_MEMORY_SRCS SHARE_MEMORY_ZMQ_HDRS ${PROTO_BUILD_DIR} ${PROTO_SRC_DIR} ${PROTO_SRC_DIR}/share_memory.proto)
GEN_GLOBAL_PROTO_LIB_DEPEND(
        NAME share_memory_protos
        SOURCE ${SHARE_MEMORY_SRCS}
        DEPEND zmq_proto_lib_depend
        LINK ${PROTOBUF_LIBRARIES} zmq_meta_protos rpc_option_protos)
GEN_GLOBAL_PROTO_LIB_DEPEND(CLIENT
        NAME share_memory_protos_client
        SOURCE ${SHARE_MEMORY_SRCS}
        DEPEND zmq_proto_lib_depend
        LINK ${PROTOBUF_LIBRARIES} zmq_meta_protos_client rpc_option_protos)

GENERATE_PROTO_CPP(HASHRING_PROTO_SRCS COMMON_PROTO_HDRS ${PROTO_BUILD_DIR}
        PROTO_FILES ${PROTO_SRC_DIR}/hash_ring.proto
        PROTO_DEPEND proto_lib_depend)
GEN_GLOBAL_PROTO_LIB_DEPEND(
        NAME hash_ring_protos
        SOURCE ${HASHRING_PROTO_SRCS}
        DEPEND proto_lib_depend
        LINK ${PROTOBUF_LIBRARIES})
GEN_GLOBAL_PROTO_LIB_DEPEND(CLIENT
        NAME hash_ring_protos_client
        SOURCE ${HASHRING_PROTO_SRCS}
        DEPEND proto_lib_depend
        LINK ${PROTOBUF_LIBRARIES})

GENERATE_PROTO_CPP(SLOT_RECOVERY_PROTO_SRCS COMMON_PROTO_HDRS ${PROTO_BUILD_DIR}
        PROTO_FILES ${PROTO_SRC_DIR}/slot_recovery.proto
        PROTO_DEPEND proto_lib_depend)
GEN_GLOBAL_PROTO_LIB_DEPEND(
        NAME slot_recovery_protos
        SOURCE ${SLOT_RECOVERY_PROTO_SRCS}
        DEPEND proto_lib_depend
        LINK ${PROTOBUF_LIBRARIES})

GENERATE_PROTO_CPP(TRANSPORT_PROTO_SRCS COMMON_PROTO_HDRS ${PROTO_BUILD_DIR}
        PROTO_FILES ${PROTO_SRC_DIR}/meta_transport.proto
        PROTO_DEPEND proto_lib_depend)
GEN_GLOBAL_PROTO_LIB_DEPEND(
        NAME meta_transport_protos
        SOURCE ${TRANSPORT_PROTO_SRCS}
        DEPEND proto_lib_depend
        LINK ${PROTOBUF_LIBRARIES})
GEN_GLOBAL_PROTO_LIB_DEPEND(CLIENT
        NAME meta_transport_protos_client
        SOURCE ${TRANSPORT_PROTO_SRCS}
        DEPEND proto_lib_depend
        LINK ${PROTOBUF_LIBRARIES})

GENERATE_PROTO_CPP(p2p_subscribe_PROTO_SRCS COMMON_PROTO_HDRS ${PROTO_BUILD_DIR}
        PROTO_FILES ${CMAKE_SOURCE_DIR}/src/datasystem/protos/p2p_subscribe.proto
        PROTO_DEPEND proto_lib_depend)
GEN_GLOBAL_PROTO_LIB_DEPEND(
        NAME p2p_subscribe_protos
        SOURCE ${p2p_subscribe_PROTO_SRCS}
        DEPEND proto_lib_depend
        LINK ${PROTOBUF_LIBRARIES})
GEN_GLOBAL_PROTO_LIB_DEPEND(CLIENT
        NAME p2p_subscribe_protos_client
        SOURCE ${p2p_subscribe_PROTO_SRCS}
        DEPEND proto_lib_depend
        LINK ${PROTOBUF_LIBRARIES})

# Compile the proto file at the first layer, for example, object_posix.proto.
GENERATE_ZMQ_CPP(zmq_proto_lib_depend OBJECT_POSIX_SRCS OBJECT_POSIX_HDRS ${PROTO_BUILD_DIR} ${PROTO_SRC_DIR} ${PROTO_SRC_DIR}/object_posix.proto)
GENERATE_ZMQ_CPP(zmq_proto_lib_depend STREAM_POSIX_SRCS STREAM_POSIX_ZMQ_HDRS ${PROTO_BUILD_DIR} ${PROTO_SRC_DIR} ${PROTO_SRC_DIR}/stream_posix.proto)

GEN_GLOBAL_PROTO_LIB_DEPEND(
        NAME posix_protos
        SOURCE ${OBJECT_POSIX_SRCS} ${STREAM_POSIX_SRCS}
        DEPEND zmq_proto_lib_depend
        LINK ${PROTOBUF_LIBRARIES} zmq_meta_protos utils_protos p2p_subscribe_protos rpc_option_protos)
GEN_GLOBAL_PROTO_LIB_DEPEND(CLIENT
        NAME posix_protos_client
        SOURCE ${OBJECT_POSIX_SRCS} ${STREAM_POSIX_SRCS}
        DEPEND zmq_proto_lib_depend
        LINK ${PROTOBUF_LIBRARIES} zmq_meta_protos_client utils_protos_client p2p_subscribe_protos_client rpc_option_protos)

# Compile the proto file at the second layer, for example, posix proto.
GENERATE_ZMQ_CPP(zmq_proto_lib_depend WORKER_OBJECT_SRCS WORKER_OBJECT_HDRS ${PROTO_BUILD_DIR} ${PROTO_SRC_DIR} ${PROTO_SRC_DIR}/worker_object.proto)
GENERATE_ZMQ_CPP(zmq_proto_lib_depend WORKER_STREAM_SRCS WORKER_STREAM_HDRS ${PROTO_BUILD_DIR} ${PROTO_SRC_DIR} ${PROTO_SRC_DIR}/worker_stream.proto)

GEN_GLOBAL_PROTO_LIB_DEPEND(
        NAME worker_object_protos
        SOURCE ${WORKER_OBJECT_SRCS}
        DEPEND zmq_proto_lib_depend
        LINK ${PROTOBUF_LIBRARIES} zmq_meta_protos hash_ring_protos rpc_option_protos meta_transport_protos)
GEN_GLOBAL_PROTO_LIB_DEPEND(CLIENT
        NAME worker_object_protos_client
        SOURCE ${WORKER_OBJECT_SRCS}
        DEPEND zmq_proto_lib_depend
        LINK ${PROTOBUF_LIBRARIES} zmq_meta_protos_client hash_ring_protos_client rpc_option_protos meta_transport_protos_client)

GEN_GLOBAL_PROTO_LIB_DEPEND(
        NAME worker_stream_protos
        SOURCE ${WORKER_STREAM_SRCS}
        DEPEND zmq_proto_lib_depend
        LINK ${PROTOBUF_LIBRARIES} zmq_meta_protos utils_protos posix_protos rpc_option_protos)
GEN_GLOBAL_PROTO_LIB_DEPEND(CLIENT
        NAME worker_stream_protos_client
        SOURCE ${WORKER_STREAM_SRCS}
        DEPEND zmq_proto_lib_depend
        LINK ${PROTOBUF_LIBRARIES} zmq_meta_protos_client utils_protos_client posix_protos_client rpc_option_protos)

# Compile the proto file at the third layer, for example, master_object.proto, master_stream.proto
GENERATE_ZMQ_CPP(zmq_proto_lib_depend MASTER_OBJECT_SRCS MASTER_OBJECT_HDRS ${PROTO_BUILD_DIR} ${PROTO_SRC_DIR} ${PROTO_SRC_DIR}/master_object.proto)
GENERATE_ZMQ_CPP(zmq_proto_lib_depend MASTER_STREAM_SRCS MASTER_STREAM_HDRS ${PROTO_BUILD_DIR} ${PROTO_SRC_DIR} ${PROTO_SRC_DIR}/master_stream.proto)
GENERATE_ZMQ_CPP(zmq_proto_lib_depend MASTER_HEARTBEAT_SRCS MASTER_HEARTBEAT_HDRS ${PROTO_BUILD_DIR} ${PROTO_SRC_DIR} ${PROTO_SRC_DIR}/master_heartbeat.proto)

GEN_GLOBAL_PROTO_LIB_DEPEND(
        NAME master_object_protos
        SOURCE ${MASTER_OBJECT_SRCS}
        DEPEND zmq_proto_lib_depend
        LINK ${PROTOBUF_LIBRARIES} posix_protos worker_object_protos)
GEN_GLOBAL_PROTO_LIB_DEPEND(CLIENT
        NAME master_object_protos_client
        SOURCE ${MASTER_OBJECT_SRCS}
        DEPEND zmq_proto_lib_depend
        LINK ${PROTOBUF_LIBRARIES} posix_protos_client worker_object_protos_client)

GEN_GLOBAL_PROTO_LIB_DEPEND(
        NAME master_stream_protos
        SOURCE ${MASTER_STREAM_SRCS}
        DEPEND zmq_proto_lib_depend
        LINK ${PROTOBUF_LIBRARIES} worker_stream_protos utils_protos posix_protos)
GEN_GLOBAL_PROTO_LIB_DEPEND(CLIENT
        NAME master_stream_protos_client
        SOURCE ${MASTER_STREAM_SRCS}
        DEPEND zmq_proto_lib_depend
        LINK ${PROTOBUF_LIBRARIES} worker_stream_protos_client utils_protos_client posix_protos_client)

GEN_GLOBAL_PROTO_LIB_DEPEND(
        NAME master_heartbeat_protos
        SOURCE ${MASTER_HEARTBEAT_SRCS}
        DEPEND zmq_proto_lib_depend
        LINK ${PROTOBUF_LIBRARIES} zmq_meta_protos rpc_option_protos)
GEN_GLOBAL_PROTO_LIB_DEPEND(CLIENT
        NAME master_heartbeat_protos_client
        SOURCE ${MASTER_HEARTBEAT_SRCS}
        DEPEND zmq_proto_lib_depend
        LINK ${PROTOBUF_LIBRARIES} zmq_meta_protos_client rpc_option_protos)

if (WITH_TESTS)
    # Build generic service proto
    GENERATE_ZMQ_CPP(zmq_proto_lib_depend GENERIC_SERVICE_SRCS GENERIC_SERVICE_HDRS ${PROTO_BUILD_DIR} ${PROTO_SRC_DIR} ${PROTO_SRC_DIR}/generic_service.proto)
    GEN_GLOBAL_PROTO_LIB_DEPEND(
            NAME generic_service_protos
            SOURCE ${GENERIC_SERVICE_SRCS}
            DEPEND zmq_proto_lib_depend)
    GEN_GLOBAL_PROTO_LIB_DEPEND(CLIENT
            NAME generic_service_protos_client
            SOURCE ${GENERIC_SERVICE_SRCS}
            DEPEND zmq_proto_lib_depend)

    GENERATE_ZMQ_CPP(zmq_proto_lib_depend ZMQ_TEST_ZMQ_SRCS ZMQ_TEST_ZMQ_HDRS ${PROTO_BUILD_DIR} ${PROTO_SRC_DIR} ${PROTO_SRC_DIR}/zmq_test.proto)
    add_library(common_rpc_zmq_demo STATIC ${ZMQ_TEST_ZMQ_SRCS})
    add_dependencies(common_rpc_zmq_demo zmq_proto_lib_depend)
    set_target_properties(common_rpc_zmq_demo PROPERTIES COMPILE_FLAGS "-Wno-unused-parameter")
    GENERATE_ZMQ_CPP(zmq_proto_lib_depend UNIT_TEST_SRCS UNIT_TEST_HDRS ${PROTO_BUILD_DIR} ${PROTO_SRC_DIR} ${PROTO_SRC_DIR}/ut_object.proto)
    add_library(ut_object_protos STATIC ${UNIT_TEST_SRCS})
    add_dependencies(ut_object_protos zmq_proto_lib_depend)
    set_target_properties(ut_object_protos PROPERTIES COMPILE_FLAGS "-Wno-unused-parameter")
endif()

if (ENABLE_PERF)
    GENERATE_ZMQ_CPP(zmq_proto_lib_depend PERF_SERVICE_SRCS PERF_SERVICE_HDRS ${PROTO_BUILD_DIR} ${PROTO_SRC_DIR} ${PROTO_SRC_DIR}/perf_posix.proto)
    GEN_GLOBAL_PROTO_LIB_DEPEND(
            NAME perf_posix_protos
            SOURCE ${PERF_SERVICE_SRCS}
            DEPEND zmq_proto_lib_depend
            LINK ${PROTOBUF_LIBRARIES} zmq_meta_protos rpc_option_protos)
    GEN_GLOBAL_PROTO_LIB_DEPEND(CLIENT
            NAME perf_posix_protos_client
            SOURCE ${PERF_SERVICE_SRCS}
            DEPEND zmq_proto_lib_depend
            LINK ${PROTOBUF_LIBRARIES} zmq_meta_protos_client rpc_option_protos)
endif()
