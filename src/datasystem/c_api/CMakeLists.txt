file(GLOB CLIENT_C_API_SRC ./*.cpp)
set(CLIENT_C_DEPEND_LIBS
        datasystem_static
        ds_spdlog::spdlog
        CURL::libcurl)

add_library(datasystem_c SHARED ${CLIENT_C_API_SRC})
target_link_libraries(datasystem_c PRIVATE "${CLIENT_C_DEPEND_LIBS}")

# just for ut
add_library(datasystem_c_static STATIC ${CLIENT_C_API_SRC})
target_link_libraries(datasystem_c_static PRIVATE "${CLIENT_C_DEPEND_LIBS}")