set(AK_SK_SIGNATURE
        hasher.cpp
        signature.cpp
        )
set(AK_SK_SIGNATURE_DEPEND_LIBS
        OpenSSL::SSL
        OpenSSL::Crypto
        common_log
        common_util
        )

set(AK_SK_SRCS
        hasher.cpp
        signature.cpp
        ak_sk_manager.cpp
        )

set(AK_SK_DEPEND_LIBS
        OpenSSL::SSL
        OpenSSL::Crypto
        common_log
        common_util
        common_encrypt
        )

add_library(ak_sk_signature STATIC ${AK_SK_SIGNATURE})
target_link_libraries(ak_sk_signature PRIVATE ${AK_SK_SIGNATURE_DEPEND_LIBS})

add_library(common_ak_sk STATIC ${AK_SK_SRCS})
target_link_libraries(common_ak_sk PRIVATE ${AK_SK_DEPEND_LIBS})