set(SPDLOG_SRCS
        log_rate_limiter.cpp
        logger_context.cpp
        logger_provider.cpp
        log_message_impl.cpp
        provider.cpp
        log_message.cpp
        )

set(APPEND_LIB
        nlohmann_json::nlohmann_json
        ds_spdlog::spdlog
        ds_flags
        common_perf
        )

add_library(ds_spdlog STATIC ${SPDLOG_SRCS})
target_link_libraries(ds_spdlog PRIVATE ${APPEND_LIB})
