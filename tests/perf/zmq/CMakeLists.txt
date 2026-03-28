set(PROTO_SRC_DIR ${CMAKE_SOURCE_DIR}/src/datasystem/protos)
set(zmq_perf_files ${PROTO_SRC_DIR}/zmq_perf.proto)

GENERATE_ZMQ_CPP(zmq_proto_lib_depend PERF_ZMQ_SRCS PERF_ZMQ_HDRS ${CMAKE_BINARY_DIR}/src/ ${PROTO_SRC_DIR} ${zmq_perf_files})

add_library(zmq_perf_protos STATIC ${PERF_ZMQ_SRCS})
set_target_properties(zmq_perf_protos PROPERTIES COMPILE_FLAGS "-Wno-unused-parameter")

add_executable(zmq_perf_client zmq_perf_main.cpp zmq_perf_run.cpp ${PERF_ZMQ_HDRS})
target_link_libraries(zmq_perf_client common_rpc_zmq common_log utils_protos zmq_meta_protos zmq_perf_protos common_util rpc_option_protos)
if (BUILD_WITH_URMA)
    target_link_libraries(zmq_perf_client common_rdma)
endif()

add_executable(zmq_perf_server zmq_server_main.cpp zmq_perf_server.cpp ${PERF_ZMQ_HDRS})
target_link_libraries(zmq_perf_server common_rpc_zmq common_log zmq_perf_protos)
if (BUILD_WITH_URMA)
    target_link_libraries(zmq_perf_server common_rdma)
endif()
add_executable(zmq_perf_agent zmq_perf_agt_exe.cpp zmq_perf_agent.cpp ${PERF_ZMQ_HDRS})
target_link_libraries(zmq_perf_agent common_rpc_zmq common_log zmq_perf_protos)
if (BUILD_WITH_URMA)
    target_link_libraries(zmq_perf_agent common_rdma)
endif()
