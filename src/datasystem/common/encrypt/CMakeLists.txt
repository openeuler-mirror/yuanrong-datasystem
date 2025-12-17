set(ENCRYPT_SRCS
        encrypt_kit.cpp
        secret_manager.cpp
        phrase_pem_tls.cpp
        )

set(ENCRYPT_DEPEND_LIBS
        OpenSSL::SSL
        OpenSSL::Crypto
        common_log
        common_util
        )

# for router client, donot need the encrypt kit
add_library(common_encrypt_client STATIC ${ENCRYPT_SRCS})
target_link_libraries(common_encrypt_client PRIVATE ${ENCRYPT_DEPEND_LIBS})


add_library(common_encrypt STATIC ${ENCRYPT_SRCS})
target_link_libraries(common_encrypt PRIVATE ${ENCRYPT_DEPEND_LIBS})
