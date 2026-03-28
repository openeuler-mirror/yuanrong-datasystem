add_subdirectory(store)
set(MASTER_OC_SRCS
        master_oc_service_impl.cpp
        oc_metadata_manager.cpp
        delete_object_mediator.cpp
        master_worker_oc_api.cpp
        master_worker_oc_local_api.cpp
        oc_nested_manager.cpp
        oc_notify_worker_manager.cpp
        oc_global_cache_delete_manager.cpp
        expired_object_manager.cpp
        oc_migrate_metadata_manager.cpp
        device/master_dev_oc_manager.cpp
        device/master_dev_hccl_rootinfo.cpp
        device/master_dev_oc_directory.cpp
        device/master_dev_npu_events.cpp
        device/master_dev_client_meta_manager.cpp
        device/master_dev_dead_lock_manager.cpp
        master_master_oc_api.cpp
        )

set(MASTER_OC_DEPEND_LIBS
        ${TBB_LIBRARY}
        common_etcd
        common_log
        common_rpc_zmq
        common_ref
        common_event_loop
        common_immutable_string
        string_ref
        common_persistence_api
        common_signal
        master_object_cache_store
        master_heartbeat_protos
        master_object_protos
        worker_object_protos
        cluster_manager
        )

add_library(master_object_cache STATIC ${MASTER_OC_SRCS})
target_link_libraries(master_object_cache PRIVATE ${MASTER_OC_DEPEND_LIBS})
add_dependencies(master_object_cache
        master_heartbeat_protos
        master_object_protos
        posix_protos
        worker_object_protos)
