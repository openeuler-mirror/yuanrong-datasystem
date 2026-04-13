set(SFS_CLIENT_SRC
        sfs_client.cpp)

add_library(common_sfs_client STATIC ${SFS_CLIENT_SRC})

set(SFS_CLIENT_DEPEND_LIBS
        common_util
        common_log
        l2cache
        httpclient
        )

target_link_libraries(common_sfs_client PRIVATE ${SFS_CLIENT_DEPEND_LIBS})