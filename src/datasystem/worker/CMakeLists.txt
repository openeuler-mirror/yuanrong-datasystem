
add_subdirectory(object_cache)
add_subdirectory(stream_cache)
add_subdirectory(hash_ring)
add_subdirectory(client_manager)
add_subdirectory(cluster_manager)

# Set source files
set(WORKER_SRCS
        authenticate.cpp
        worker_service_impl.cpp
        worker_oc_server.cpp
        worker_liveness_check.cpp
        worker_update_flag_check.cpp
        worker_cli.cpp
        worker.cpp
        worker_api_c_wrapper.cpp
        )

set(WORKER_DEPEND_LIBS
        ${TBB_LIBRARY}
        common_ak_sk
        common_encrypt
        common_etcd
        common_event_loop
        common_iam
        common_metastore_service
        common_perf
        common_rocksdb
        common_rpc_zmq
        common_shared_memory
        common_util
        common_metrics
        common_signal
        string_ref
        ds_master
        ds_server
        httpclient
        master_heartbeat_protos
        posix_protos
        share_memory_protos
        worker_object_cache
        worker_stream_cache
        rpc_stub_cache_mgr
        worker_hash_ring
        worker_client_manager
        cluster_manager
        )

if (WITH_TESTS)
    list(APPEND WORKER_SRCS "../../../tests/st/st_oc_service_impl.cpp")
    list(APPEND WORKER_DEPEND_LIBS ut_object_protos)
endif ()

if (BUILD_PIPLN_H2D)
    list(APPEND WORKER_DEPEND_LIBS os_transport_pipeline)
endif ()

if (ENABLE_PERF)
    list(APPEND WORKER_SRCS "perf_service/perf_service_impl.cpp")
    list(APPEND WORKER_DEPEND_LIBS perf_posix_protos)
endif ()

set_source_files_properties(worker.cpp PROPERTIES
        COMPILE_DEFINITIONS "GIT_HASH=\"${GIT_HASH}\";GIT_BRANCH=\"${GIT_BRANCH}\"")

add_library(datasystem_worker_static STATIC ${WORKER_SRCS})
target_link_libraries(datasystem_worker_static PRIVATE ${WORKER_DEPEND_LIBS})
add_dependencies(datasystem_worker_static
        master_heartbeat_protos
        share_memory_protos
        posix_protos)

add_library(datasystem_worker_shared SHARED ${WORKER_SRCS})
target_link_libraries(datasystem_worker_shared PUBLIC ${WORKER_DEPEND_LIBS})
add_dependencies(datasystem_worker_shared
        master_heartbeat_protos
        share_memory_protos
        posix_protos)

# generate datasystem_worker bin
add_executable(datasystem_worker_bin worker_main.cpp)
set_target_properties(datasystem_worker_bin PROPERTIES LINK_FLAGS "-fPIE -pie")
target_link_libraries(datasystem_worker_bin PRIVATE ${JEMALLOC_SHARED_LIBRARY} nlohmann_json::nlohmann_json datasystem_worker_shared)
target_link_options(datasystem_worker_bin PRIVATE -Wl,--no-as-needed ${JEMALLOC_SHARED_LIBRARY} -Wl,--as-needed)

set_target_properties(datasystem_worker_bin PROPERTIES
    OUTPUT_NAME datasystem_worker)
set_target_properties(datasystem_worker_shared PROPERTIES
    OUTPUT_NAME datasystem_worker)

# for ut test
if (WITH_TESTS)
    set(_WORKER_BIN_DIR ${CMAKE_CURRENT_BINARY_DIR} CACHE STRING "path of worker binary file")
endif ()
