# hard_disk_exporter is used by the AccessRecorder of the client.
set(HARD_DISK_EXPORTER_SRCS
        hard_disk_exporter.cpp)

set(HARD_DISK_EXPORTER_LIBS
        common_util
        dynamic_flag_config
        metrics_exporter_base)

add_library(hard_disk_exporter STATIC ${HARD_DISK_EXPORTER_SRCS})
target_link_libraries(hard_disk_exporter PRIVATE ${HARD_DISK_EXPORTER_LIBS})