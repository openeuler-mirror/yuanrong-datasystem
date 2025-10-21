set(OBS_SRCS
        cloud_service_rotation.cpp
        obs_client.cpp)

set(OBS_LIBS
        l2cache
        httpclient
        nlohmann_json::nlohmann_json
        ${OBS_LIB_PATH}/libeSDKOBS.so
        ${PCRE}
        ${ICONV}
        ${XML2})

add_library(common_obs STATIC ${OBS_SRCS})
target_link_libraries(common_obs PRIVATE ${OBS_LIBS})