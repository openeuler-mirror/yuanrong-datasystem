set(GFLAG_SRCS
        common_gflags.cpp
        flags.cpp)

set(GFLAG_LIBS
        ${ZLIB_LIBRARIES}
        ${SECUREC_LIBRARY}
        common_log
        common_inject)

add_library(common_util_gflag STATIC ${GFLAG_SRCS})
target_link_libraries(common_util_gflag PRIVATE ${GFLAG_LIBS})