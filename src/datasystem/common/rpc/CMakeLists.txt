# protoc plugin
set(ZMQ_PLUGIN_DEPEND_LIBS
        protobuf::libprotobuf
        protobuf::libprotoc
        common_util
        $<TARGET_FILE:zmq_meta_protos>
        $<TARGET_FILE:utils_protos>
        rpc_option_protos
        )

set(ZMQ_RPC_GENERATOR_SRC
        plugin_generator/zmq_rpc_generator.cpp
        plugin_generator/service_cpp_generator.cpp
        plugin_generator/service_header_generator.cpp
        plugin_generator/stub_cpp_generator.cpp
        plugin_generator/stub_header_generator.cpp
)

add_library(zmq_rpc_generator ${ZMQ_RPC_GENERATOR_SRC})
target_link_libraries(zmq_rpc_generator PRIVATE ${ZMQ_PLUGIN_DEPEND_LIBS})
add_dependencies(zmq_rpc_generator zmq_meta_protos rpc_option_protos)

add_executable(zmq_plugin plugin_generator/zmq_plugin.cpp)
target_link_libraries(zmq_plugin PRIVATE zmq_rpc_generator)

add_executable(rpc_plugin plugin_generator/rpc_plugin.cpp)
target_link_libraries(rpc_plugin PRIVATE zmq_rpc_generator)

# zmq server library
set(COMMON_RPC_ZMQ_SRCS
        rpc_auth_key_manager.cpp
        rpc_channel.cpp
        rpc_message.cpp
        rpc_credential.cpp
        rpc_options.cpp
        rpc_server.cpp
        rpc_service_cfg.cpp
        rpc_auth_keys.cpp
        mem_view.cpp
        unix_sock_fd.cpp
        zmq/zmq_auth.cpp
        zmq/zmq_common.cpp
        zmq/zmq_context.cpp
        zmq/zmq_epoll.cpp
        zmq/zmq_msg_decoder.cpp
        zmq/zmq_message.cpp
        zmq/zmq_monitor.cpp
        zmq/zmq_payload.cpp
        zmq/zmq_server_impl.cpp
        zmq/zmq_service.cpp
        zmq/zmq_socket_ref.cpp
        zmq/zmq_socket.cpp
        zmq/zmq_stub.cpp
        zmq/zmq_stub_impl.cpp
        zmq/zmq_stub_conn.cpp
        zmq/zmq_unary_client_impl.h
        zmq/zmq_server_stream_base.cpp
        zmq/zmq_client_stream_base.cpp
        zmq/zmq_stream_base.cpp
        zmq/exclusive_conn_mgr.cpp
        zmq/work_agent.cpp
        rpc_auth_key_manager_server.cpp)

set(COMMON_RPC_ZMQ_DEPENDS_LIBS
        protobuf::libprotobuf
        nlohmann_json::nlohmann_json
        ${ZeroMQ_LIBRARY}
        ${SECUREC_LIBRARY}
        common_log
        common_perf
        common_metrics
        common_encrypt
        utils_protos
        common_util
        common_event_loop
        zmq_meta_protos
        rpc_option_protos)

set(COMMON_RPC_ZMQ_CLIENT_DEPENDS_LIBS
        protobuf::libprotobuf
        nlohmann_json::nlohmann_json
        ${ZeroMQ_LIBRARY}
        ${SECUREC_LIBRARY}
        common_log
        common_perf
        common_util
        utils_protos_client
        zmq_meta_protos_client
        rpc_option_protos
        common_event_loop
        common_encrypt)

if (BUILD_WITH_URMA)
        list(APPEND COMMON_RPC_ZMQ_CLIENT_DEPENDS_LIBS
                common_rdma)
endif()

add_library(common_rpc_zmq STATIC ${COMMON_RPC_ZMQ_SRCS})
target_link_libraries(common_rpc_zmq PUBLIC ${COMMON_RPC_ZMQ_DEPENDS_LIBS})
add_dependencies(common_rpc_zmq zmq_meta_protos rpc_option_protos)

add_library(common_rpc_zmq_client STATIC ${COMMON_RPC_ZMQ_SRCS})
target_link_libraries(common_rpc_zmq_client PUBLIC ${COMMON_RPC_ZMQ_CLIENT_DEPENDS_LIBS})
add_dependencies(common_rpc_zmq_client zmq_meta_protos_client rpc_option_protos)

set(RPC_STUB_CACHE_MGR_SRCS
        rpc_stub_cache_mgr.cpp
)

set(RPC_STUB_CACHE_MGR_DEPENDS_LIBS
        master_object_protos
        worker_object_protos
        master_stream_protos
        worker_stream_protos
)

add_library(rpc_stub_cache_mgr STATIC ${RPC_STUB_CACHE_MGR_SRCS})
target_link_libraries(rpc_stub_cache_mgr PUBLIC ${RPC_STUB_CACHE_MGR_DEPENDS_LIBS})
