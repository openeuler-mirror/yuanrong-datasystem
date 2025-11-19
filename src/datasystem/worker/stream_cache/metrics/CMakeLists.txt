set(SC_METRICS_SRCS
    sc_metrics.cpp
    sc_metrics_monitor.cpp)

set(SC_METRICS_DEPEND_LIBS
    common_log
    common_metrics
    common_util
    posix_protos
    )

add_library(sc_metrics STATIC ${SC_METRICS_SRCS})
target_link_libraries(sc_metrics PRIVATE ${SC_METRICS_DEPEND_LIBS})
add_dependencies(sc_metrics
    posix_protos
    )