set(WORKER_OC_SRCS
        worker_master_oc_api.cpp
        worker_worker_oc_api.cpp
        worker_oc_service_impl.cpp
        worker_request_manager.cpp
        worker_worker_oc_service_impl.cpp
        worker_worker_transport_service_impl.cpp
        master_worker_oc_service_impl.cpp
        worker_oc_eviction_manager.cpp
        worker_oc_spill.cpp
        obj_cache_hashmap.cpp
        obj_cache_shm_unit.cpp
        async_send_manager.cpp
        async_rollback_manager.cpp
        async_update_location_manager.cpp
        metadata_recovery_manager.cpp
        metadata_recovery_selector.cpp
        slot_recovery_orchestrator.cpp
        object_kv.cpp
        eviction_list.cpp
        cache_hit_info.cpp
        device/device_obj_cache.cpp
        device/worker_device_oc_manager.cpp
        device/worker_dev_req_manager.cpp
        data_migrator/basic/migrate_progress.cpp
        data_migrator/handler/async_resource_releaser.cpp
        data_migrator/handler/migrate_data_handler.cpp
        data_migrator/strategy/node_selector.cpp
        data_migrator/strategy/scale_down_node_selector.cpp
        data_migrator/strategy/spill_node_selector.cpp
        data_migrator/transport/fast_migrate_transport.cpp
        data_migrator/transport/tcp_migrate_transport.cpp
        data_migrator/data_migrator.cpp
        limiter/data_limiter.cpp
        service/worker_oc_service_crud_common_api.cpp
        service/worker_oc_service_create_impl.cpp
        service/worker_oc_service_publish_impl.cpp
        service/worker_oc_service_multi_publish_impl.cpp
        service/worker_oc_service_get_impl.cpp
        service/worker_oc_service_delete_impl.cpp
        service/worker_oc_service_clear_data_flow.cpp
        service/worker_oc_service_global_reference_impl.cpp
        service/worker_oc_service_migrate_impl.cpp
        service/worker_oc_service_batch_get_impl.cpp
        service/worker_oc_service_expire_impl.cpp
        slot_recovery/slot_recovery_store.cpp
        slot_recovery/slot_recovery_manager.cpp)

# Lightweight library for transport API
set(WORKER_TRANSPORT_API_SRCS
        worker_worker_transport_api.cpp)

set(WORKER_TRANSPORT_API_DEPEND_LIBS
        common_log
        common_util
        common_inject
        common_rpc_zmq
        common_rdma
        rpc_stub_cache_mgr
        worker_object_protos)

set(WORKER_OBJECT_CACHE_DEPEND_LIBS
        ${TBB_LIBRARY}
        common_etcd
        common_log
        common_util
        common_buffer
        common_ref
        common_iam
        common_rpc_zmq
        common_event_loop
        common_shared_memory
        common_immutable_string
        string_ref
        common_persistence_api
        common_slot_client
        common_parallel
        master_heartbeat_protos
        posix_protos
        worker_object_protos
        slot_recovery_protos
        master_object_protos
        worker_client_manager
        cluster_manager
        worker_health_check
        )

list(APPEND WORKER_OBJECT_CACHE_DEPEND_LIBS common_rdma)

add_library(worker_transport_api STATIC ${WORKER_TRANSPORT_API_SRCS})
target_link_libraries(worker_transport_api PRIVATE ${WORKER_TRANSPORT_API_DEPEND_LIBS})
add_dependencies(worker_transport_api worker_object_protos)

add_library(worker_object_cache STATIC ${WORKER_OC_SRCS} ${WORKER_TRANSPORT_API_SRCS})
target_link_libraries(worker_object_cache PRIVATE ${WORKER_OBJECT_CACHE_DEPEND_LIBS})
add_dependencies(worker_object_cache
        master_heartbeat_protos
        posix_protos
        worker_object_protos
        master_object_protos)
