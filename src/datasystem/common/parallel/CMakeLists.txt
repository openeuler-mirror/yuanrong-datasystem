set(PARALLEL_SRCS
        detail/parallel_for_local.cpp)
 
add_library(common_parallel STATIC ${PARALLEL_SRCS})
target_link_libraries(common_parallel PRIVATE common_util)
