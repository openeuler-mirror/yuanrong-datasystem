set(ETCD_SRCS
        etcd_elector.cpp
        etcd_store.cpp
        etcd_keep_alive.cpp
        etcd_watch.cpp
        etcd_health.cpp
        grpc_session.cpp)

set(ETCD_DEPEND_LIBS
        ${TBB_LIBRARY}
        gRPC::grpc++
        common_log
        common_util
        etcdapi_proto
        common_signal)

add_library(common_etcd STATIC ${ETCD_SRCS})
target_include_directories(common_etcd PUBLIC ${CMAKE_CURRENT_BINARY_DIR}/etcd/proto)
target_link_libraries(common_etcd PRIVATE ${ETCD_DEPEND_LIBS} common_encrypt)

add_library(common_etcd_client STATIC ${ETCD_SRCS})
target_include_directories(common_etcd_client PUBLIC ${CMAKE_CURRENT_BINARY_DIR}/etcd/proto)
target_link_libraries(common_etcd_client PRIVATE ${ETCD_DEPEND_LIBS} common_encrypt_client)