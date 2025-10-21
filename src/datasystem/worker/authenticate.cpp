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
 * Description: The authenticate implement.
 */

#include "datasystem/worker/authenticate.h"

namespace datasystem {
namespace worker {

Status AuthenticateMessageInternal(std::shared_ptr<AkSkManager> akSkManager, const std::string &reqTenantId,
                                          const std::string &token, std::string &tenantId)
{
    CHECK_FAIL_RETURN_STATUS(akSkManager != nullptr, StatusCode::K_INVALID, "AK/SK manager has not been initialized.");
    auto tenantAuthEnabled = TenantAuthManager::Instance()->AuthEnabled();
    auto signature = std::move(g_ReqSignature);
    auto accessKey = std::move(g_ReqAk);
    auto timestamp = g_ReqTimestamp;
    ZmqMessage serializedStr = std::move(g_SerializedMessage);
    if (tenantAuthEnabled) {
        if (!token.empty()) {
            RETURN_IF_NOT_OK_PRINT_ERROR_MSG(TenantAuthManager::Instance()->TenantTokenAuth(token, tenantId),
                                             "worker authenticate token failed");
            return Status::OK();
        } else if (!signature.empty() && akSkManager->IsTenantAk(accessKey)) {
            RETURN_IF_NOT_OK_PRINT_ERROR_MSG(
                TenantAuthManager::Instance()->TenantAkAuth(accessKey, reqTenantId, tenantId),
                "worker authenticate accesskey failed");
            CHECK_FAIL_RETURN_STATUS(tenantId == reqTenantId, StatusCode::K_NOT_AUTHORIZED, "Authentication failed.");
            RETURN_IF_NOT_OK_PRINT_ERROR_MSG(akSkManager->VerifySignatureAndTimestamp(
                                                 signature, timestamp, accessKey,
                                                 static_cast<const char *>(serializedStr.Data()), serializedStr.Size()),
                                             "tenant authenticate AK/SK signature failed");
            return Status::OK();
        }
    }
    auto SystemAuthEnabled = akSkManager->SystemAuthEnabled();
    if (SystemAuthEnabled && !signature.empty()) {
        RETURN_IF_NOT_OK_PRINT_ERROR_MSG(
            akSkManager->VerifySignatureAndTimestamp(
                signature, timestamp, accessKey, static_cast<const char *>(serializedStr.Data()), serializedStr.Size()),
            "AK/SK failed");
    } else if (tenantAuthEnabled || SystemAuthEnabled) {
        RETURN_STATUS(K_RUNTIME_ERROR, "AK/SK or token not provide");
    }
    tenantId = reqTenantId;
    return Status::OK();
}

}
}