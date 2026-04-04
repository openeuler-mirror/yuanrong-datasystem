set(L2_CACHE_SRC
    get_object_info_list_resp.cpp
    l2cache_client.cpp
    l2_storage.cpp
)

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

set(SFS_SLOT_STORE_SRC
    slot_client/slot_compactor.cpp
    slot_client/slot_file_util.cpp
    slot_client/slot_index_codec.cpp
    slot_client/slot_manifest.cpp
    slot_client/slot.cpp
    slot_client/slot_snapshot.cpp
    slot_client/slot_takeover_planner.cpp
    slot_client/slot_client.cpp
    slot_client/slot_writer.cpp
)
add_library(common_slot_client STATIC ${SFS_SLOT_STORE_SRC})
target_link_libraries(common_slot_client PRIVATE
    common_inject
    common_util
    common_log
)

set(PERSIST_API_SRC
    aggregated_persistence_api.cpp
    object_persistence_api.cpp
    persistence_api.cpp
)
add_library(common_persistence_api STATIC ${PERSIST_API_SRC})
set(PERSIST_API_DEPEND_LIBS
    CURL::libcurl
    common_obs
    common_sfs_client
    common_slot_client
)
target_link_libraries(common_persistence_api PUBLIC ${PERSIST_API_DEPEND_LIBS})
