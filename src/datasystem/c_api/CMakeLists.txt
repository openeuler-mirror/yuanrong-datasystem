set(CLIENT_C_API_SRC
        kv_client_c_wrapper.cpp
        object_client_c_wrapper.cpp
        util.cpp)

if (BUILD_STREAM_CACHE)
    list(APPEND CLIENT_C_API_SRC stream_client_c_wrapper.cpp)
endif()

set(CLIENT_C_DEPEND_LIBS
        datasystem_static
        ds_spdlog::spdlog
        CURL::libcurl)

add_library(datasystem_c SHARED ${CLIENT_C_API_SRC})
target_link_libraries(datasystem_c PRIVATE "${CLIENT_C_DEPEND_LIBS}")

# just for ut
add_library(datasystem_c_static STATIC ${CLIENT_C_API_SRC})
target_link_libraries(datasystem_c_static PRIVATE "${CLIENT_C_DEPEND_LIBS}")