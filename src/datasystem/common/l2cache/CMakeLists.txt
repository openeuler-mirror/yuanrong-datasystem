set(L2_CACHE_SRC
        get_object_info_list_resp.cpp
        l2cache_client.cpp
        l2_storage.cpp)

add_library(l2cache STATIC ${L2_CACHE_SRC})

set(L2_CACHE_DEPEND_LIBS
        common_util
        CURL::libcurl
        nlohmann_json::nlohmann_json
        ${SECUREC_LIBRARY}
        )

target_link_libraries(l2cache PRIVATE ${L2_CACHE_DEPEND_LIBS})

add_subdirectory(obs_client)
add_subdirectory(sfs_client)

set(PERSIST_API_SRC persistence_api.cpp)
add_library(common_persistence_api STATIC ${PERSIST_API_SRC})
set(PERSIST_API_DEPEND_LIBS
        CURL::libcurl
        common_obs
        common_sfs_client)
target_link_libraries(common_persistence_api PRIVATE ${PERSIST_API_DEPEND_LIBS})