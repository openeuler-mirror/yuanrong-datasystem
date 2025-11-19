set(SC_COMMON_SRCS
    cursor.cpp
    stream_data_page.cpp
    stream_meta_shm.cpp)

set(SC_COMMON_LIBS
    common_log)

set(COMMON_SC_DEPEND_LIBS
    posix_protos_client
    )
add_library(common_sc STATIC ${SC_COMMON_SRCS})
target_link_libraries(common_sc PRIVATE ${SC_COMMON_LIBS} ${COMMON_SC_DEPEND_LIBS})
