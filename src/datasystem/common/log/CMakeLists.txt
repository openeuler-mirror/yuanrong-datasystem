add_subdirectory(spdlog)

set(LOG_SRCS
        log_manager.cpp
        logging.cpp
        operation_logger.cpp
        access_recorder.cpp
        trace.cpp
        latency_phase.cpp
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
# access_recorder.cpp calls GetRequestContext() from request_context.h.
# PUBLIC ensures all consumers of common_log resolve this symbol at link time.
target_link_libraries(common_log PUBLIC common_request_context PRIVATE ${LOG_LIBS})
target_include_directories(common_log PRIVATE ${CMAKE_BINARY_DIR}/src)

add_library(common_log_proto STATIC log_sampler_proto.cpp)
target_link_libraries(common_log_proto PUBLIC common_log share_memory_protos)
