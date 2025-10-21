set(WORKER_OC_SRCS
        worker_master_oc_api.cpp
        worker_worker_oc_api.cpp
        worker_oc_service_impl.cpp
        worker_request_manager.cpp
        worker_worker_oc_service_impl.cpp
        master_worker_oc_service_impl.cpp
        migrate_data_handler.cpp
        worker_oc_eviction_manager.cpp
        worker_oc_spill.cpp
        obj_cache_hashmap.cpp
        obj_cache_shm_unit.cpp
        async_send_manager.cpp
        async_rollback_manager.cpp
        object_kv.cpp
        eviction_list.cpp
        device/device_obj_cache.cpp
        device/worker_device_oc_manager.cpp
        device/worker_dev_req_manager.cpp
        service/worker_oc_service_crud_common_api.cpp
        service/worker_oc_service_create_impl.cpp
        service/worker_oc_service_publish_impl.cpp
        service/worker_oc_service_multi_publish_impl.cpp
        service/worker_oc_service_get_impl.cpp
        service/worker_oc_service_delete_impl.cpp
        service/worker_oc_service_global_reference_impl.cpp
        service/worker_oc_service_migrate_impl.cpp
        service/worker_oc_service_batch_get_impl.cpp
        service/worker_oc_service_expire_impl.cpp)

set(WORKER_OBJECT_CACHE_DEPEND_LIBS
        ${TBB_LIBRARY}
        common_etcd
        common_log
        common_util
        common_buffer
        common_ref
        common_iam
        common_inject
        common_rpc_zmq
        common_event_loop
        common_shared_memory
        common_immutable_string
        common_persistence_api
        master_heartbeat_protos
        posix_protos
        worker_object_protos
        master_object_protos
        worker_client_manager
        cluster_manager
        worker_health_check
        )

list(APPEND WORKER_OBJECT_CACHE_DEPEND_LIBS common_rdma)

add_library(worker_object_cache STATIC ${WORKER_OC_SRCS})
target_link_libraries(worker_object_cache PRIVATE ${WORKER_OBJECT_CACHE_DEPEND_LIBS})
add_dependencies(worker_object_cache
        master_heartbeat_protos
        posix_protos
        worker_object_protos
        master_object_protos)
