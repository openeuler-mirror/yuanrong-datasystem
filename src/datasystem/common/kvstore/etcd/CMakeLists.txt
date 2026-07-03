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

add_library(member_service_info STATIC member_service_info.cpp)
target_include_directories(member_service_info PUBLIC ${PROJECT_SOURCE_DIR}/src)
target_link_libraries(member_service_info PUBLIC common_util coordinator_protos)
add_dependencies(member_service_info coordinator_protos)

add_library(member_service_info_client STATIC member_service_info.cpp)
target_include_directories(member_service_info_client PUBLIC ${PROJECT_SOURCE_DIR}/src)
target_link_libraries(member_service_info_client PUBLIC common_util coordinator_protos_client)
add_dependencies(member_service_info_client coordinator_protos_client)

add_library(common_etcd STATIC ${ETCD_SRCS})
target_include_directories(common_etcd PUBLIC ${CMAKE_CURRENT_BINARY_DIR}/etcd/proto)
target_link_libraries(common_etcd PUBLIC member_service_info PRIVATE ${ETCD_DEPEND_LIBS} common_encrypt coordinator_protos)

add_library(common_etcd_client STATIC ${ETCD_SRCS})
target_include_directories(common_etcd_client PUBLIC ${CMAKE_CURRENT_BINARY_DIR}/etcd/proto)
target_link_libraries(common_etcd_client PUBLIC member_service_info_client PRIVATE ${ETCD_DEPEND_LIBS} common_encrypt_client coordinator_protos_client)
