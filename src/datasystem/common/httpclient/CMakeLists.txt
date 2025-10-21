set(HTTP_CLIENT_SRC
        curl_http_client.cpp
        http_message.cpp
        http_request.cpp
        http_response.cpp
        )

add_library(httpclient STATIC ${HTTP_CLIENT_SRC})

set(HTTP_CLIENT_DEPEND_LIBS
        ${OPENSSL_LIBRARIES}
        CURL::libcurl
        nlohmann_json::nlohmann_json
        common_log
        common_util
        common_perf
        )

target_link_libraries(httpclient PUBLIC ${HTTP_CLIENT_DEPEND_LIBS})
