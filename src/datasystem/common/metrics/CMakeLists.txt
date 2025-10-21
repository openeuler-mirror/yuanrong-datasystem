add_library(metrics_exporter_base STATIC metrics_exporter.cpp)

set(METRICS_SRCS
        res_metric_collector.cpp
        )
set(METRICS_LIBS 
        common_log
        metrics_exporter_base) 

add_subdirectory(hard_disk_exporter)

add_library(common_metrics STATIC ${METRICS_SRCS})
target_link_libraries(common_metrics PRIVATE ${METRICS_LIBS}  common_log metrics_exporter_base)