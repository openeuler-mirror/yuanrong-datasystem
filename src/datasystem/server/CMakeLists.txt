# Set source files
set(SERVER_SRCS
        common_server.cpp
        ${SERVER_PROTO_SRCS}
        ${SERVER_ZMQ_SRCS})

set(SERVER_DEPEND_LIBS
        common_event_loop
        common_log
        common_perf
        common_metrics
        common_rpc_zmq
        common_util)

if (WITH_TESTS)
    list(APPEND SERVER_SRCS "generic_service_impl.cpp")
    list(APPEND SERVER_DEPEND_LIBS generic_service_protos)
endif ()

# add server lib
add_library(ds_server STATIC ${SERVER_SRCS})
target_link_libraries(ds_server PRIVATE ${SERVER_DEPEND_LIBS})

if (WITH_TESTS)
    add_dependencies(ds_server generic_service_protos)
endif ()