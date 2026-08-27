add_library(jemalloc_stats_bvar STATIC jemalloc_stats_bvar.cpp)
target_include_directories(jemalloc_stats_bvar PUBLIC ${PROJECT_SOURCE_DIR}/src)
target_link_libraries(jemalloc_stats_bvar PRIVATE
        ${BRPC_LIBRARIES}
        ${CMAKE_DL_LIBS}
        common_log
        common_util)
