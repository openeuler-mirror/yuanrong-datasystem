/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2023. All rights reserved.
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
 * [Experimental Feature / Work-in-Progress]
 * This module is an early implementation of Multi-Tenancy capabilities,
 * intended for future feature enablement.
 *
 * Warning: The implementation is incomplete; both APIs and data structures
 * are subject to incompatible changes in upcoming releases.
 * Do NOT use in production environments.
 *
 * Status: Under active development
 * Planned stable release: v2.0
 */
#ifndef DATASYSTEM_WORKER_AUTHENTICATE_H
#define DATASYSTEM_WORKER_AUTHENTICATE_H

#include "datasystem/common/ak_sk/ak_sk_manager.h"
#include "datasystem/common/iam/tenant_auth_manager.h"
#include "datasystem/common/util/gflag/common_gflags.h"
#include "datasystem/common/util/thread_local.h"
#include "datasystem/worker/client_manager/client_manager.h"

DS_DECLARE_bool(skip_authenticate);

namespace datasystem {
namespace worker {

Status AuthenticateMessageInternal(std::shared_ptr<AkSkManager> akSkManager, const std::string &reqTenantId,
                                   const std::string &token, std::string &tenantId);

inline Status CheckTenantId(const std::string &reqTenantId)
{
    CHECK_FAIL_RETURN_STATUS(reqTenantId.empty(), K_INVALID,
                             "Don't request worker with tenantId, when enable skip_authenticate ");
    return Status::OK();
}

template <typename ReqType>
Status AuthenticateRequest(std::shared_ptr<AkSkManager> akSkManager, const ReqType &req, const std::string &reqTenantId,
                           std::string &tenantId)
{
    if (FLAGS_skip_authenticate) {
        return CheckTenantId(reqTenantId);
    }
    if (!g_ReqAk.empty() && !g_ReqSignature.empty() && !g_SerializedMessage.Empty()) {
        return worker::AuthenticateMessageInternal(akSkManager, reqTenantId, req.token(), tenantId);
    }
    CHECK_FAIL_RETURN_STATUS(akSkManager != nullptr, StatusCode::K_INVALID, "AK/SK manager has not been initialized.");
    auto tenantAuthEnabled = TenantAuthManager::Instance()->AuthEnabled();
    if (tenantAuthEnabled) {
        if (!req.token().empty()) {
            RETURN_IF_NOT_OK_PRINT_ERROR_MSG(TenantAuthManager::Instance()->TenantTokenAuth(req.token(), tenantId),
                                             "worker authenticate token failed");
            return Status::OK();
        } else if (!req.signature().empty() && akSkManager->IsTenantAk(req.access_key())) {
            RETURN_IF_NOT_OK_PRINT_ERROR_MSG(
                TenantAuthManager::Instance()->TenantAkAuth(req.access_key(), reqTenantId, tenantId),
                "worker authenticate accesskey failed");
            CHECK_FAIL_RETURN_STATUS(tenantId == reqTenantId, StatusCode::K_NOT_AUTHORIZED, "Authentication failed.");
            RETURN_IF_NOT_OK_PRINT_ERROR_MSG(akSkManager->VerifySignatureAndTimestamp(req),
                                             "tenant authenticate AK/SK signature failed");
            return Status::OK();
        }
    }
    auto SystemAuthEnabled = akSkManager->SystemAuthEnabled();
    if (SystemAuthEnabled && !req.signature().empty()) {
        RETURN_IF_NOT_OK_PRINT_ERROR_MSG(akSkManager->VerifySignatureAndTimestamp(req), "AK/SK failed");
    } else if (tenantAuthEnabled || SystemAuthEnabled) {
        RETURN_STATUS(K_RUNTIME_ERROR, "AK/SK or token not provide");
    }
    tenantId = reqTenantId;
    return Status::OK();
}

template <typename ReqType>
Status Authenticate(std::shared_ptr<AkSkManager> akSkManager, const ReqType &req, std::string &tenantId,
                    const ClientKey &clientId)
{
    if (FLAGS_skip_authenticate) {
        return CheckTenantId(req.tenant_id());
    }
    Timer timer;
    std::string authTenantId = req.tenant_id();
    auto clientInfo = worker::ClientManager::Instance().GetClientInfo(clientId);
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(clientInfo != nullptr, StatusCode::K_CLIENT_WORKER_DISCONNECT,
                                         FormatString("The client id (%s) does not exist.", clientId));
    if (authTenantId.empty()) {
        authTenantId = clientInfo->GetTenantId();
    }
    RETURN_IF_NOT_OK(worker::AuthenticateRequest(akSkManager, req, authTenantId, tenantId));
    workerOperationTimeCost.Append("Authenticate", timer.ElapsedMilliSecond());
    return Status::OK();
}

template <typename ReqType>
Status AuthenticateMessage(std::shared_ptr<AkSkManager> akSkManager, const ReqType &req, const ClientKey &clientId,
                           std::string &tenantId)
{
    if (FLAGS_skip_authenticate) {
        return CheckTenantId(req.tenant_id());
    }
    Timer timer;
    std::string authTenantId = req.tenant_id();
    auto clientInfo = worker::ClientManager::Instance().GetClientInfo(clientId);
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(clientInfo != nullptr, StatusCode::K_CLIENT_WORKER_DISCONNECT,
                                         FormatString("The client id (%s) does not exist.", clientId));
    if (authTenantId.empty()) {
        authTenantId = clientInfo->GetTenantId();
    }
    RETURN_IF_NOT_OK(worker::AuthenticateMessageInternal(akSkManager, authTenantId, req.token(), tenantId));
    workerOperationTimeCost.Append("Authenticate message", static_cast<uint64_t>(timer.ElapsedMilliSecond()));
    return Status::OK();
}

template <typename ReqType>
Status Authenticate(std::shared_ptr<AkSkManager> akSkManager, const ReqType &req, std::string &tenantId)
{
    if (FLAGS_skip_authenticate) {
        return CheckTenantId(req.tenant_id());
    }
    return !g_ReqAk.empty() && !g_ReqSignature.empty() && !g_SerializedMessage.Empty()
               ? AuthenticateMessage(akSkManager, req, ClientKey::Intern(req.client_id()), tenantId)
               : Authenticate(akSkManager, req, tenantId, ClientKey::Intern(req.client_id()));
}
}  // namespace worker
}  // namespace datasystem
#endif