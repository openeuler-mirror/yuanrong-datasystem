set(AES_SRC
        aes_impl.cpp)

set(AES_LIBS
        common_log
        common_util)

add_library(common_aes STATIC ${AES_SRC})
target_link_libraries(common_aes PRIVATE ${AES_LIBS})