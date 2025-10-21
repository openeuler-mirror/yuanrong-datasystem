set(IAM_SRCS
        tenant_auth_manager.cpp
        )

set(IAM_DEPEND_LIBS
        httpclient
        common_log
        common_util
        nlohmann_json::nlohmann_json
        ${SECUREC_LIBRARY}
        )

add_library(common_iam STATIC ${IAM_SRCS})
target_link_libraries(common_iam PRIVATE ${IAM_DEPEND_LIBS})