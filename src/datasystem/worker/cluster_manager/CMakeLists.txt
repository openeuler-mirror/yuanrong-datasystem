
set(CLUSTER_MANAGER_SRCS
        etcd_cluster_manager.cpp
        )

set(CLUSTER_MANAGER_DEPEND_LIBS
        common_util
        common_ak_sk
        common_etcd
        ds_master
        worker_object_protos
        worker_hash_ring
        common_signal
        )

add_library(cluster_manager STATIC ${CLUSTER_MANAGER_SRCS})
target_link_libraries(cluster_manager PRIVATE ${CLUSTER_MANAGER_DEPEND_LIBS})

set(WORKER_HEALTH_CHECK_SRCS
        worker_health_check.cpp
        )

set(WORKER_HEALTH_CHECK_DEPEND_LIBS
        common_util
        )

add_library(worker_health_check STATIC ${WORKER_HEALTH_CHECK_SRCS})
target_link_libraries(worker_health_check PRIVATE ${WORKER_HEALTH_CHECK_DEPEND_LIBS})
