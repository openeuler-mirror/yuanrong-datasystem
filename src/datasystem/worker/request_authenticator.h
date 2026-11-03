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

#ifndef DATASYSTEM_WORKER_REQUEST_AUTHENTICATOR_H
#define DATASYSTEM_WORKER_REQUEST_AUTHENTICATOR_H

#include <memory>
#include <string>
#include <utility>

#include "datasystem/common/ak_sk/ak_sk_manager.h"
#include "datasystem/common/iam/tenant_auth_manager.h"
#include "datasystem/common/util/format.h"
#include "datasystem/common/util/request_context.h"
#include "datasystem/common/util/timer.h"
#include "datasystem/worker/client_manager/client_manager.h"

namespace datasystem {
namespace worker {

class RequestAuthenticator {
public:
    RequestAuthenticator(std::shared_ptr<AkSkManager> akSkManager, bool tenantAuthEnabled)
        : akSkManager_(std::move(akSkManager)),
          tenantAuthEnabled_(tenantAuthEnabled),
          systemAuthEnabled_(akSkManager_ != nullptr && akSkManager_->SystemAuthEnabled()),
          requiresAuthentication_(akSkManager_ == nullptr || systemAuthEnabled_ || tenantAuthEnabled_)
    {
    }

    ~RequestAuthenticator() = default;

    bool RequiresAuthentication() const noexcept
    {
        return requiresAuthentication_;
    }

    bool SystemAuthEnabled() const noexcept
    {
        return systemAuthEnabled_;
    }

    bool TenantAuthEnabled() const noexcept
    {
        return tenantAuthEnabled_;
    }

    template <typename ReqType>
    Status AuthenticateRequest(const ReqType &req, const std::string &reqTenantId, std::string &tenantId) const
    {
        if (!requiresAuthentication_) {
            tenantId = reqTenantId;
            return Status::OK();
        }
        CHECK_FAIL_RETURN_STATUS(akSkManager_ != nullptr, StatusCode::K_INVALID,
                                 "AK/SK manager has not been initialized.");
        if (tenantAuthEnabled_) {
            if (!req.token().empty()) {
                RETURN_IF_NOT_OK_PRINT_ERROR_MSG(TenantAuthManager::Instance()->TenantTokenAuth(req.token(), tenantId),
                                                 "worker authenticate token failed");
                return Status::OK();
            }
            if (!req.signature().empty()) {
                CHECK_FAIL_RETURN_STATUS(!req.access_key().empty(), K_NOT_AUTHORIZED, "Authentication failed.");
                if (akSkManager_->IsTenantAk(req.access_key())) {
                    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(
                        TenantAuthManager::Instance()->TenantAkAuth(req.access_key(), reqTenantId, tenantId),
                        "worker authenticate accesskey failed");
                    CHECK_FAIL_RETURN_STATUS(tenantId == reqTenantId, K_NOT_AUTHORIZED, "Authentication failed.");
                    return Status::OK();
                }
                if (systemAuthEnabled_) {
                    tenantId = reqTenantId;
                    return Status::OK();
                }
            }
            RETURN_STATUS(K_RUNTIME_ERROR, "AK/SK or token not provide");
        }
        if (systemAuthEnabled_ && !req.signature().empty()) {
            CHECK_FAIL_RETURN_STATUS(!req.access_key().empty(), K_NOT_AUTHORIZED, "Authentication failed.");
        }
        tenantId = reqTenantId;
        return Status::OK();
    }

    template <typename ReqType>
    Status Authenticate(const ReqType &req, std::string &tenantId, const ClientKey &clientId) const
    {
        if (!requiresAuthentication_) {
            tenantId = req.tenant_id();
            return Status::OK();
        }
        Timer timer;
        std::string authTenantId = req.tenant_id();
        auto clientInfo = worker::ClientManager::Instance().GetClientInfo(clientId);
        CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(clientInfo != nullptr, StatusCode::K_CLIENT_WORKER_DISCONNECT,
                                             FormatString("The client id (%s) does not exist.", clientId));
        if (authTenantId.empty()) {
            authTenantId = clientInfo->GetTenantId();
        }
        RETURN_IF_NOT_OK(AuthenticateRequest(req, authTenantId, tenantId));
        GetWorkerTimeCost().Append("Authenticate", timer.ElapsedMilliSecond());
        return Status::OK();
    }

    template <typename ReqType>
    Status Authenticate(const ReqType &req, std::string &tenantId) const
    {
        if (!requiresAuthentication_) {
            tenantId = req.tenant_id();
            return Status::OK();
        }
        return Authenticate(req, tenantId, ClientKey::Intern(req.client_id()));
    }

    template <typename ReqType>
    Status Authenticate(const ReqType &req, std::string &tenantId, const std::string &clientId) const
    {
        if (!requiresAuthentication_) {
            tenantId = req.tenant_id();
            return Status::OK();
        }
        return Authenticate(req, tenantId, ClientKey::Intern(clientId));
    }

private:
    std::shared_ptr<AkSkManager> akSkManager_;
    const bool tenantAuthEnabled_;
    const bool systemAuthEnabled_;
    const bool requiresAuthentication_;
};

}  // namespace worker
}  // namespace datasystem

#endif  // DATASYSTEM_WORKER_REQUEST_AUTHENTICATOR_H
