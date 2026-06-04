add_subdirectory(spdlog)

set(LOG_SRCS
        log_manager.cpp
        logging.cpp
        access_recorder.cpp
        trace.cpp
        failure_handler.cpp
        log_sampler.cpp
        )

set(LOG_LIBS
        ds_flags
        Threads::Threads
        common_util
        hard_disk_exporter
        ds_spdlog
        absl::symbolize
        absl::failure_signal_handler
        common_flags
        )

add_library(common_log STATIC ${LOG_SRCS})
target_link_libraries(common_log PRIVATE ${LOG_LIBS})
target_include_directories(common_log PRIVATE ${CMAKE_BINARY_DIR}/src)

add_library(common_log_proto STATIC log_sampler_proto.cpp)
target_link_libraries(common_log_proto PUBLIC common_log share_memory_protos)
