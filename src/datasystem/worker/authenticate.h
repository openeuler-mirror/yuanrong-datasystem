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
#include "datasystem/common/rpc/brpc_auth.h"
#include "datasystem/common/iam/tenant_auth_manager.h"
#include "datasystem/common/flags/common_flags.h"
#include "datasystem/common/util/request_context.h"
#include "datasystem/common/util/thread_local.h"
#include "datasystem/worker/client_manager/client_manager.h"
#include "datasystem/worker/request_authenticator.h"

namespace datasystem {
namespace worker {

Status CreateRpcAuthenticator(const google::protobuf::ServiceDescriptor &service,
                              const std::shared_ptr<AkSkManager> &manager, BrpcAuthService::Authenticate &authenticate);

template <typename ReqType>
Status AuthenticateRequest(const std::shared_ptr<const RequestAuthenticator> &authenticator, const ReqType &req,
                           const std::string &reqTenantId, std::string &tenantId)
{
    CHECK_FAIL_RETURN_STATUS(authenticator != nullptr, StatusCode::K_INVALID,
                             "Request authenticator has not been initialized.");
    return authenticator->AuthenticateRequest(req, reqTenantId, tenantId);
}

template <typename ReqType>
Status Authenticate(const std::shared_ptr<const RequestAuthenticator> &authenticator, const ReqType &req,
                    std::string &tenantId, const ClientKey &clientId)
{
    CHECK_FAIL_RETURN_STATUS(authenticator != nullptr, StatusCode::K_INVALID,
                             "Request authenticator has not been initialized.");
    return authenticator->Authenticate(req, tenantId, clientId);
}

template <typename ReqType>
Status Authenticate(const std::shared_ptr<const RequestAuthenticator> &authenticator, const ReqType &req,
                    std::string &tenantId, const std::string &clientId)
{
    CHECK_FAIL_RETURN_STATUS(authenticator != nullptr, StatusCode::K_INVALID,
                             "Request authenticator has not been initialized.");
    return authenticator->Authenticate(req, tenantId, clientId);
}

template <typename ReqType>
Status Authenticate(const std::shared_ptr<const RequestAuthenticator> &authenticator, const ReqType &req,
                    std::string &tenantId)
{
    CHECK_FAIL_RETURN_STATUS(authenticator != nullptr, StatusCode::K_INVALID,
                             "Request authenticator has not been initialized.");
    return authenticator->Authenticate(req, tenantId);
}
}  // namespace worker
}  // namespace datasystem
#endif
