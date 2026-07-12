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

/** Description: Defines client-scoped authentication for worker RPCs. */
#ifndef DATASYSTEM_CLIENT_TRANSPORT_CLIENT_REQUEST_AUTH_H
#define DATASYSTEM_CLIENT_TRANSPORT_CLIENT_REQUEST_AUTH_H

#include <string>

#include "datasystem/common/ak_sk/signature.h"
#include "datasystem/protos/object_posix.pb.h"
#include "datasystem/utils/sensitive_value.h"
#include "datasystem/utils/status.h"

namespace datasystem {
namespace client {

class ClientRequestAuth {
public:
    virtual ~ClientRequestAuth() = default;

    /** @brief Fill client identity and authenticate a Get request after all transport fields are set. */
    virtual Status Authenticate(GetReqPb &request) = 0;

    /** @brief Fill tenant identity and authenticate an object-size query. */
    virtual Status Authenticate(GetObjMetaInfoReqPb &request) = 0;

    /** @brief Replace the AK/SK pair used by subsequent requests. */
    virtual Status UpdateAkSk(const std::string &accessKey, SensitiveValue secretKey) = 0;
};

class DefaultClientRequestAuth final : public ClientRequestAuth {
public:
    DefaultClientRequestAuth(std::string clientId = "", std::string accessKey = "",
                             SensitiveValue secretKey = {},
                             std::string defaultTenantId = "");
    ~DefaultClientRequestAuth() override = default;

    Status Authenticate(GetReqPb &request) override;
    Status Authenticate(GetObjMetaInfoReqPb &request) override;
    Status UpdateAkSk(const std::string &accessKey, SensitiveValue secretKey) override;

    /** @return Stable client identity generated or supplied at construction. */
    const std::string &ClientId() const
    {
        return clientId_;
    }

private:
    std::string EffectiveTenantId() const;

    const std::string clientId_;
    const std::string defaultTenantId_;
    Signature signature_;
};

}  // namespace client
}  // namespace datasystem

#endif  // DATASYSTEM_CLIENT_TRANSPORT_CLIENT_REQUEST_AUTH_H
