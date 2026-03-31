# MetaStore service library - a lightweight etcd-compatible service

set(METASTORE_SERVICE_SRCS
    metastore_server.cpp
    manager/kv_manager.cpp
    manager/lease_manager.cpp
    manager/watch_manager.cpp
    service/kv_service_impl.cpp
    service/lease_service_impl.cpp
    service/watch_service_impl.cpp
    service/maintenance_service_impl.cpp
)

set(METASTORE_SERVICE_HDRS
    metastore_server.h
    manager/kv_manager.h
    manager/lease_manager.h
    manager/watch_manager.h
    service/kv_service_impl.h
    service/lease_service_impl.h
    service/watch_service_impl.h
    service/maintenance_service_impl.h
)

set(METASTORE_SERVICE_DEPEND_LIBS
    gRPC::grpc++
    etcdapi_proto
)

# MetaStore shared library
add_library(common_metastore_service SHARED ${METASTORE_SERVICE_SRCS})
target_sources(common_metastore_service PRIVATE ${METASTORE_SERVICE_HDRS})
target_include_directories(common_metastore_service PUBLIC ${CMAKE_SOURCE_DIR})
target_link_libraries(common_metastore_service PUBLIC ${METASTORE_SERVICE_DEPEND_LIBS})
