set(PERF_SRCS
        perf_manager.cpp)

add_library(common_perf STATIC ${PERF_SRCS})
target_link_libraries(common_perf PUBLIC nlohmann_json::nlohmann_json)
