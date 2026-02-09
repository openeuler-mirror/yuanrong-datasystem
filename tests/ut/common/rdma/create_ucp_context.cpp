/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * Description: Create a global ucp context. Tool for Ucp tests.
 */

#include "common/rdma/create_ucp_context.h"
#include "datasystem/common/rdma/ucp_dlopen_util.h"

namespace datasystem {

CreateUcpContext::CreateUcpContext()
{
    InitContext();
}

CreateUcpContext::~CreateUcpContext()
{
    if (context_) {
        ds_ucp_cleanup(context_);
    }
}

void CreateUcpContext::InitContext()
{
    ucp_params_t ucp_params = {};
    ucp_config_t *config;

    ds_ucp_config_read(NULL, NULL, &config);
    ucp_params.field_mask = UCP_PARAM_FIELD_FEATURES;
    ucp_params.features = UCP_FEATURE_RMA | UCP_FEATURE_WAKEUP;
    ds_ucp_init(&ucp_params, config, &context_);
}

}  // namespace datasystem