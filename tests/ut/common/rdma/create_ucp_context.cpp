#include "common/rdma/create_ucp_context.h"

namespace datasystem {

CreateUcpContext::CreateUcpContext()
{
    InitContext();
}

CreateUcpContext::~CreateUcpContext()
{
    if (context_) {
        ucp_cleanup(context_);
    }
}

void CreateUcpContext::InitContext()
{
    ucp_params_t ucp_params;
    ucp_config_t *config;

    ucp_config_read(NULL, NULL, &config);
    std::memset(&ucp_params, 0, sizeof(ucp_params));
    ucp_params.field_mask = UCP_PARAM_FIELD_FEATURES;
    ucp_params.features = UCP_FEATURE_TAG | UCP_FEATURE_AM | UCP_FEATURE_RMA;
    ucp_init(&ucp_params, config, &context_);
}

}  // namespace datasystem