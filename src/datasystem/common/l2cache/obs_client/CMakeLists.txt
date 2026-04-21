set(OBS_SRCS
        aws_v4_signature.cpp
        cloud_service_rotation.cpp
        obs_client.cpp
        obs_service_detector.cpp
        obs_signature.cpp
        obs_xml_util.cpp)

set(OBS_LIBS
        l2cache
        httpclient
        common_encrypt_client
        nlohmann_json::nlohmann_json
        OpenSSL::SSL
        OpenSSL::Crypto
        ${PCRE}
        ${ICONV})

add_library(common_obs STATIC ${OBS_SRCS})
target_link_libraries(common_obs PUBLIC ${OBS_LIBS})
