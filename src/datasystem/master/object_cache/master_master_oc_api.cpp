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
 * Description: Module responsible for master master oc api.
 */

#include "datasystem/master/object_cache/master_master_oc_api.h"

#include "datasystem/common/rpc/rpc_auth_key_manager.h"
#include "datasystem/common/rpc/rpc_credential.h"
#include "datasystem/common/rpc/rpc_stub_cache_mgr.h"
#include "datasystem/common/flags/common_flags.h"
#include "datasystem/common/util/request_context.h"
#include "datasystem/common/util/rpc_util.h"
#include "datasystem/common/util/rpc_diagnostic.h"
#include "datasystem/common/util/status_helper.h"

namespace datasystem {
namespace master {
MasterMasterOCApi::MasterMasterOCApi(const HostPort &hostPort, const HostPort &localHostPort,
                                     std::shared_ptr<AkSkManager> akSkManager)
    : destHostPort_(hostPort), localHostPort_(localHostPort), akSkManager_(std::move(akSkManager))
{
}

Status MasterMasterOCApi::Init()
{
    std::shared_ptr<RpcStubBase> rpcStub;
    RETURN_IF_NOT_OK(
        RpcStubCacheMgr::Instance().GetStub(destHostPort_, StubType::MASTER_MASTER_OC_SVC, rpcStub));
    if (FLAGS_use_brpc) {
        brpcSession_ = std::dynamic_pointer_cast<master::MasterOCService_BrpcGenericStub>(rpcStub);
        RETURN_RUNTIME_ERROR_IF_NULL(brpcSession_);
    } else {
        rpcSession_ = std::dynamic_pointer_cast<master::MasterOCService_Stub>(rpcStub);
        RETURN_RUNTIME_ERROR_IF_NULL(rpcSession_);
    }
    return Status::OK();
}

Status MasterMasterOCApi::MigrateMetadata(MigrateMetadataReqPb &req, MigrateMetadataRspPb &rsp)
{
    INJECT_POINT("BatchMigrateMetadata.streamSendData", []() {
        return Status(K_RPC_UNAVAILABLE, "mock networker error");
    });
    auto rc = brpcSession_ ? brpcSession_->MigrateMetadata(req, rsp) : rpcSession_->MigrateMetadata(req, rsp);
    return WithRpcDiag(rc, "MigrateMetadata", localHostPort_, destHostPort_);
}

Status MasterMasterOCApi::GIncreaseMasterAppRef(const GIncreaseReqPb &req, GIncreaseRspPb &rsp)
{
    int64_t remainingTime = GetRequestContext()->reqTimeoutDuration.CalcRemainingTime();
    CHECK_FAIL_RETURN_STATUS(remainingTime > 0, K_RPC_DEADLINE_EXCEEDED,
                             FormatString("Request timeout (%lld ms).", -remainingTime));
    if (remainingTime > INT_MAX) {
        remainingTime = INT_MAX;
    }
    RpcOptions opts;
    opts.SetTimeout(remainingTime);
    auto rc = brpcSession_ ? brpcSession_->GIncreaseMasterAppRef(opts, req, rsp)
                           : rpcSession_->GIncreaseMasterAppRef(opts, req, rsp);
    return WithRpcDiag(rc, "GIncreaseMasterAppRef", localHostPort_, destHostPort_);
}

Status MasterMasterOCApi::ReleaseGRefsOfRemoteClientId(const ReleaseGRefsReqPb &req, ReleaseGRefsRspPb &rsp)
{
    int64_t remainingTime = GetRequestContext()->reqTimeoutDuration.CalcRemainingTime();
    CHECK_FAIL_RETURN_STATUS(remainingTime > 0, K_RPC_DEADLINE_EXCEEDED,
                             FormatString("Request timeout (%lld ms).", -remainingTime));
    if (remainingTime > INT_MAX) {
        remainingTime = INT_MAX;
    }
    RpcOptions opts;
    opts.SetTimeout(remainingTime);
    auto rc = brpcSession_ ? brpcSession_->ReleaseGRefsOfRemoteClientId(opts, req, rsp)
                           : rpcSession_->ReleaseGRefsOfRemoteClientId(opts, req, rsp);
    return WithRpcDiag(rc, "ReleaseGRefsOfRemoteClientId", localHostPort_, destHostPort_);
}

Status MasterMasterOCApi::RemoveMeta(const RemoveMetaReqPb &req, RemoveMetaRspPb &rsp)
{
    int64_t remainingTime = GetRequestContext()->reqTimeoutDuration.CalcRemainingTime();
    CHECK_FAIL_RETURN_STATUS(remainingTime > 0, K_RPC_DEADLINE_EXCEEDED,
                             FormatString("Request timeout (%lld ms).", -remainingTime));
    RpcOptions opts;
    opts.SetTimeout(remainingTime);
    auto rc = brpcSession_ ? brpcSession_->RemoveMeta(opts, req, rsp) : rpcSession_->RemoveMeta(opts, req, rsp);
    return WithRpcDiag(rc, "RemoveMeta", localHostPort_, destHostPort_);
}

Status MasterMasterOCApi::DeleteAllCopyMeta(DeleteAllCopyMetaReqPb &request, DeleteAllCopyMetaRspPb &response)
{
    RpcOptions opts;
    return RetryOnErrorRepent(
        GetRequestContext()->reqTimeoutDuration.CalcRealRemainingTime(),
        [this, &opts, &request, &response](int32_t) {
            int64_t remainingTime = GetRequestContext()->reqTimeoutDuration.CalcRemainingTime();
            CHECK_FAIL_RETURN_STATUS(remainingTime > 0, K_RPC_DEADLINE_EXCEEDED,
                                     FormatString("Request timeout (%lld ms).", -remainingTime));
            opts.SetTimeout(remainingTime);
            request.set_timeout(remainingTime);
            RETURN_IF_NOT_OK(akSkManager_->GenerateSignature(request));
            return brpcSession_ ? brpcSession_->DeleteAllCopyMeta(opts, request, response)
                                : rpcSession_->DeleteAllCopyMeta(opts, request, response);
        },
        []() { return Status::OK(); },
        { StatusCode::K_TRY_AGAIN, StatusCode::K_RPC_CANCELLED, StatusCode::K_RPC_DEADLINE_EXCEEDED,
          StatusCode::K_RPC_UNAVAILABLE });
}
}  // namespace master
}  // namespace datasystem