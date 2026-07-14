/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
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

/** Description: Implements client-scoped authentication for worker RPCs. */

#include "datasystem/client/transport/rpc/client_request_auth.h"

#include <utility>

#include "datasystem/common/util/thread_local.h"
#include "datasystem/common/util/request_context.h"
#include "datasystem/common/util/uuid_generator.h"

namespace datasystem {
namespace client {

DefaultClientRequestAuth::DefaultClientRequestAuth(std::string clientId, std::string accessKey,
                                                   SensitiveValue secretKey,
                                                   std::string defaultTenantId)
    : clientId_(clientId.empty() ? GetStringUuid() : std::move(clientId)),
      defaultTenantId_(std::move(defaultTenantId)),
      signature_(accessKey, secretKey)
{
}

std::string DefaultClientRequestAuth::EffectiveTenantId() const
{
    auto *ctx = GetRequestContext();
    return ctx->tenantId.empty() ? defaultTenantId_ : ctx->tenantId;
}

Status DefaultClientRequestAuth::Authenticate(GetReqPb &request)
{
    request.set_client_id(clientId_);
    request.set_tenant_id(EffectiveTenantId());
    request.clear_token();
    return signature_.GenerateSignature(request);
}

Status DefaultClientRequestAuth::Authenticate(GetObjMetaInfoReqPb &request)
{
    request.set_tenantid(EffectiveTenantId());
    return signature_.GenerateSignature(request);
}

Status DefaultClientRequestAuth::UpdateAkSk(const std::string &accessKey, SensitiveValue secretKey)
{
    return signature_.SetClientAkSk(accessKey, std::move(secretKey));
}

}  // namespace client
}  // namespace datasystem
