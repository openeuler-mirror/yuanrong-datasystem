set(TOKEN_SRC
    client_access_token.cpp)

add_library(token STATIC ${TOKEN_SRC})

set(TOKEN_DEPEND_LIBS
        common_util
        ${SECUREC_LIBRARY}
        )

target_link_libraries(token PRIVATE ${TOKEN_DEPEND_LIBS})
