/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2024. All rights reserved.
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
 * Description: This interface is used for IAM authentication.
 */
#ifndef DATASYSTEM_COMMON_IAM_IAM_H
#define DATASYSTEM_COMMON_IAM_IAM_H

#include "datasystem/utils/sensitive_value.h"
#include "datasystem/utils/status.h"

namespace datasystem {
class IAM {
public:
    IAM() = default;

    virtual ~IAM() = default;

    /**
     * @brief Init iam auth server.
     * @return Status of the call.
     */
    virtual Status Init(bool &authEnable) = 0;

    /**
     * @brief Authenticating the token using IAM service, and get a tenant id from IAM service.
     * @param[in] token Token to be authenticated.
     * @param[out] tenantId ID of the tenant corresponding to the token.
     * @param[out] expiredSec Expired second for token.
     * @return Status of the call.
     */
    virtual Status VerifyTenantToken(const SensitiveValue &token, std::string &tenantId, uint64_t &expireSec) = 0;

    /**
     * @brief Authenticating the AKSK using IAM service, and get a tenant id with SecertKey from IAM service.
     * @param[in] acccessKey Access key to be authenticated.
     * @param[out] seceretKey Secret key corresponding to the access key.
     * @param[out] tenantId ID of the tenant corresponding to the acceess key.
     * @param[out] expiredSec Expired second for access key.
     * @param[out] isSystemRole The tenant is system or not.
     * @return Status of the call.
     */
    virtual Status VerifyTenantAkSk(const std::string &accessKey, SensitiveValue &secretKey, std::string &tenantId,
                                    uint64_t &expireSec, bool &isSystemRole) = 0;
};
}  // namespace datasystem
#endif  // DATASYSTEM_COMMON_IAM_IAM_CLIENT_API_H