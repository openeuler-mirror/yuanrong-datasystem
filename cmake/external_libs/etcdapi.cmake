set(PROTOBUF_DIR "${CMAKE_SOURCE_DIR}/third_party/protos")

# Generate cpp files.
file(GLOB_RECURSE PROTO_SRCS "${PROTOBUF_DIR}/*.proto")

generate_proto_cpp(ETCD_PROTO_SRCS ETCD_PROTO_HDRS ${CMAKE_CURRENT_BINARY_DIR}/third_party/protos
                   PROTO_FILES  ${PROTO_SRCS}
                   SOURCE_ROOT  ${PROTOBUF_DIR})

set(GRPC_PROTO_SRCS
    "${PROTOBUF_DIR}/etcd/api/etcdserverpb/rpc.proto"
    "${PROTOBUF_DIR}/etcd/v3election.proto"
    "${PROTOBUF_DIR}/etcd/v3lock.proto")

generate_grpc_cpp(ETCD_GRPC_PROTO_SRCS ETCD_GRPC_PROTO_HDRS ${CMAKE_CURRENT_BINARY_DIR}/third_party/protos
                  PROTO_FILES  ${GRPC_PROTO_SRCS}
                  SOURCE_ROOT  ${PROTOBUF_DIR})

include_directories(${CMAKE_CURRENT_BINARY_DIR}/third_party/protos)

add_library(etcdapi_proto STATIC ${ETCD_PROTO_SRCS} ${ETCD_GRPC_PROTO_SRCS})
target_link_libraries(etcdapi_proto ${PROTOBUF_LIBRARIES} gRPC::grpc++)
target_include_directories(etcdapi_proto PUBLIC ${Protobuf_INCLUDE_DIRS})
