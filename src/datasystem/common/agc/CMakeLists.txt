set(AGC_SRC
    client_access_token.cpp)

add_library(agc STATIC ${AGC_SRC})

set(AGC_DEPEND_LIBS
        common_util
        ${SECUREC_LIBRARY}
        )

target_link_libraries(agc PRIVATE ${AGC_DEPEND_LIBS})
