set(WORKER_OC_SRCS
        object_endpoint_policy.cpp
        worker_master_oc_api.cpp
        worker_worker_oc_api.cpp
        worker_worker_peer_state_codec.cpp
        worker_oc_service_impl.cpp
        object_cache_worker_flags.cpp
        worker_request_manager.cpp
        worker_worker_oc_gather_layout.cpp
        worker_worker_oc_service_impl.cpp
        worker_worker_transport_service_impl.cpp
        master_worker_oc_service_impl.cpp
        central_metadata_address_resolver.cpp
        object_metadata_coordination_reader.cpp
        worker_topology_object_cache_actions.cpp
        worker_topology_metadata_actions.cpp
        worker_oc_eviction_manager.cpp
        worker_oc_spill.cpp
        obj_cache_hashmap.cpp
        obj_cache_shm_unit.cpp
        async_send_manager.cpp
        async_update_location_manager.cpp
        metadata_recovery_manager.cpp
        object_cache_recovery_state.cpp
        worker_recovery_evidence_adapter.cpp
        metadata_recovery_selector.cpp
        rebalance_candidate_provider.cpp
        slot_recovery_orchestrator.cpp
        object_table.cpp
        object_kv.cpp
        eviction_list.cpp
        cache_hit_info.cpp
        kv_event/kv_event_publisher.cpp
        kv_event/zmq_lite.cpp
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
        data_migrator/transport/fast_migrate_transport2.cpp
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
        common_metrics
        common_util
        common_request_context
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
        common_request_context
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
        cluster_topology_scope
        cluster_topology
        metadata_route_resolver
        worker_client_manager
        worker_health_check
        nlohmann_json::nlohmann_json
        ${BRPC_LIBRARIES}
        ${ZeroMQ_LIBRARY}
        worker_runtime_core
        )

list(APPEND WORKER_OBJECT_CACHE_DEPEND_LIBS common_rdma)

add_library(worker_transport_api STATIC ${WORKER_TRANSPORT_API_SRCS})
target_link_libraries(worker_transport_api PRIVATE ${WORKER_TRANSPORT_API_DEPEND_LIBS})
add_dependencies(worker_transport_api worker_object_protos)

add_library(worker_object_cache STATIC ${WORKER_OC_SRCS} ${WORKER_TRANSPORT_API_SRCS})
target_link_libraries(worker_object_cache PRIVATE ${WORKER_OBJECT_CACHE_DEPEND_LIBS})

# Reduce optimization for protobuf-heavy source files to prevent cc1plus OOM during parallel builds.
# Each of these files transitively includes multiple .pb.h headers; with -O2 each cc1plus process
# can consume 2-3 GB RSS.  Lowering to -O1 cuts per-process memory while preserving _FORTIFY_SOURCE compatibility.
set(WORKER_OC_HEAVY_FILES
    worker_oc_service_impl.cpp
    service/worker_oc_service_get_impl.cpp
    service/worker_oc_service_migrate_impl.cpp
    worker_oc_eviction_manager.cpp
)
set_source_files_properties(${WORKER_OC_HEAVY_FILES}
    PROPERTIES COMPILE_FLAGS "-O1")
add_dependencies(worker_object_cache
        master_heartbeat_protos
        posix_protos
        worker_object_protos
        master_object_protos)
