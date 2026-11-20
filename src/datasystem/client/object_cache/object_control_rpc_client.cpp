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

#include "datasystem/client/object_cache/object_control_rpc_client.h"

#include <utility>

#include "datasystem/client/object_cache/transport/rpc/exist_request_builder.h"
#include "datasystem/client/object_cache/transport/rpc/worker_rpc_client.h"
#include "datasystem/common/log/latency_phase.h"
#include "datasystem/common/perf/perf_manager.h"
#include "datasystem/common/rpc/api_deadline.h"
#include "datasystem/common/rpc/timeout_duration.h"
#include "datasystem/common/util/format.h"
#include "datasystem/common/util/rpc_util.h"
#include "datasystem/common/util/status_helper.h"

namespace datasystem {
namespace client {
namespace {

int64_t GetRemainingRpcTimeoutMs()
{
    return TimeoutDuration::CeilUsToMs(ApiDeadline::Instance().ApiRemainingUs());
}

void SaveExecutionState(const RpcAttempt &attempt, RpcExecutionState *executionState)
{
    if (executionState != nullptr) {
        *executionState = attempt.executionState;
    }
}

}  // namespace

ObjectControlRpcClient::ObjectControlRpcClient(std::shared_ptr<WorkerRpcDispatcher> dispatcher)
    : dispatcher_(std::move(dispatcher))
{
}

Status ObjectControlRpcClient::Exist(const HostPort &workerAddr, const TransportExistRequest &input,
                                     TransportExistResult &output)
{
    RETURN_RUNTIME_ERROR_IF_NULL(dispatcher_);
    ExistReqPb request;
    RETURN_IF_NOT_OK(BuildExistRequest(input, request));
    ExistRspPb response;
    PerfPoint perfPoint(PerfKey::RPC_CLIENT_EXIST);
    RpcAttempt attempt = dispatcher_->Invoke(workerAddr, "Exist", [&](WorkerRpcClient &endpoint) {
        return endpoint.InvokeObjectRpc(
            input.subTimeoutMs, "Exist", request, response,
            [&](WorkerOCService_BrpcGenericStub &stub, const RpcOptions &options) {
                return stub.Exist(options, request, response);
            });
    });
    perfPoint.Record();
    const bool traceEnabled = IsClientLatencyTraceActive();
    Trace::Instance().AddCommPhaseIfEnabled(LatencySummaryPhase::CLIENT_RPC_EXIST, traceEnabled);
    if (traceEnabled && response.latency_phase_us_size() > 0) {
        MergeDecodedPhasesToTrace(
            std::vector<uint32_t>(response.latency_phase_us().begin(), response.latency_phase_us().end()),
            response.latency_tick_dropped_count());
    }
    RETURN_IF_NOT_OK(attempt.status);
    if (!response.redirect_extra().empty()) {
        return Status(K_NOT_OWNER, "Exist keys redirected to new owners").WithExtra(response.redirect_extra());
    }
    CHECK_FAIL_RETURN_STATUS(static_cast<size_t>(response.exists_size()) == input.objectKeys.size(), K_RUNTIME_ERROR,
                             FormatString("Exist response size mismatch: expected %zu keys, got %d results",
                                          input.objectKeys.size(), response.exists_size()));
    output.exists.assign(response.exists().begin(), response.exists().end());
    return Status::OK();
}

Status ObjectControlRpcClient::Delete(const HostPort &workerAddr, const TransportRequestContext &context,
                                      const std::vector<std::string> &objectKeys, bool areDeviceObjects,
                                      bool isRouted, DeleteAllCopyRspPb &response,
                                      RpcExecutionState *executionState)
{
    if (executionState != nullptr) {
        *executionState = RpcExecutionState::NOT_SENT;
    }
    RETURN_RUNTIME_ERROR_IF_NULL(dispatcher_);
    DeleteAllCopyReqPb request;
    request.set_client_id(context.clientId);
    request.set_token(context.token);
    request.set_tenant_id(context.tenantId);
    request.set_are_device_objects(areDeviceObjects);
    request.set_is_routed(isRouted);
    *request.mutable_object_keys() = { objectKeys.begin(), objectKeys.end() };
    PerfPoint perfPoint(PerfKey::RPC_CLIENT_DEL_OBJECT);
    RpcAttempt attempt = dispatcher_->Invoke(workerAddr, "DeleteAllCopy", [&](WorkerRpcClient &endpoint) {
        response.Clear();
        return endpoint.InvokeObjectRpc(
            GetRemainingRpcTimeoutMs(), "DeleteAllCopy", request, response,
            [&](WorkerOCService_BrpcGenericStub &stub, const RpcOptions &options) {
                return stub.DeleteAllCopy(options, request, response);
            });
    });
    perfPoint.Record();
    SaveExecutionState(attempt, executionState);
    return attempt.status;
}

Status ObjectControlRpcClient::GetObjMetaInfo(const HostPort &workerAddr, const std::string &tenantId,
                                              const std::vector<std::string> &objectKeys,
                                              std::vector<ObjMetaInfo> &objMetas)
{
    RETURN_RUNTIME_ERROR_IF_NULL(dispatcher_);
    GetObjMetaInfoReqPb request;
    request.set_tenantid(tenantId);
    *request.mutable_object_keys() = { objectKeys.begin(), objectKeys.end() };
    GetObjMetaInfoRspPb response;
    RpcAttempt attempt = dispatcher_->Invoke(workerAddr, "GetObjMetaInfo", [&](WorkerRpcClient &endpoint) {
        return endpoint.InvokeObjectRpc(
            GetRemainingRpcTimeoutMs(), "GetObjMetaInfo", request, response,
            [&](WorkerOCService_BrpcGenericStub &stub, const RpcOptions &options) {
                return stub.GetObjMetaInfo(options, request, response);
            });
    });
    RETURN_IF_NOT_OK(attempt.status);
    objMetas.clear();
    objMetas.reserve(response.objs_meta_info_size());
    for (const auto &meta : response.objs_meta_info()) {
        objMetas.emplace_back(ObjMetaInfo{
            meta.obj_size(), std::vector<std::string>{ meta.location_ids().begin(), meta.location_ids().end() } });
    }
    return Status::OK();
}

Status ObjectControlRpcClient::QuerySize(const HostPort &workerAddr, const TransportRequestContext &context,
                                         const std::vector<std::string> &objectKeys, QuerySizeRspPb &response)
{
    RETURN_RUNTIME_ERROR_IF_NULL(dispatcher_);
    QuerySizeReqPb request;
    request.set_client_id(context.clientId);
    request.set_token(context.token);
    request.set_tenant_id(context.tenantId);
    request.set_is_routed(true);
    *request.mutable_object_keys() = { objectKeys.begin(), objectKeys.end() };
    RpcAttempt attempt = dispatcher_->Invoke(workerAddr, "QuerySize", [&](WorkerRpcClient &endpoint) {
        response.Clear();
        return endpoint.InvokeObjectRpc(
            GetRemainingRpcTimeoutMs(), "QuerySize", request, response,
            [&](WorkerOCService_BrpcGenericStub &stub, const RpcOptions &options) {
                return stub.QuerySize(options, request, response);
            });
    });
    RETURN_IF_NOT_OK(attempt.status);
    Status responseStatus(static_cast<StatusCode>(response.last_rc().error_code()), response.last_rc().error_msg());
    if (responseStatus.GetCode() == K_TRY_AGAIN || IsRetryableRpcError(responseStatus)
        || IsNonRetryableRpcError(responseStatus)) {
        return responseStatus;
    }
    return Status::OK();
}

Status ObjectControlRpcClient::Expire(const HostPort &workerAddr, const TransportRequestContext &context,
                                      const std::vector<std::string> &objectKeys, uint32_t ttlSeconds,
                                      ExpireRspPb &response, RpcExecutionState *executionState)
{
    if (executionState != nullptr) {
        *executionState = RpcExecutionState::NOT_SENT;
    }
    RETURN_RUNTIME_ERROR_IF_NULL(dispatcher_);
    ExpireReqPb request;
    request.set_client_id(context.clientId);
    request.set_token(context.token);
    request.set_tenant_id(context.tenantId);
    request.set_ttl_second(ttlSeconds);
    request.set_is_routed(true);
    *request.mutable_object_keys() = { objectKeys.begin(), objectKeys.end() };
    RpcAttempt attempt = dispatcher_->Invoke(workerAddr, "Expire", [&](WorkerRpcClient &endpoint) {
        response.Clear();
        return endpoint.InvokeObjectRpc(
            GetRemainingRpcTimeoutMs(), "Expire", request, response,
            [&](WorkerOCService_BrpcGenericStub &stub, const RpcOptions &options) {
                return stub.Expire(options, request, response);
            });
    });
    SaveExecutionState(attempt, executionState);
    return attempt.status;
}

Status ObjectControlRpcClient::GetMetaInfo(const HostPort &workerAddr, const TransportRequestContext &context,
                                           const std::vector<std::string> &objectKeys, bool isDeviceKey,
                                           bool isRouted, GetMetaInfoRspPb &response)
{
    RETURN_RUNTIME_ERROR_IF_NULL(dispatcher_);
    GetMetaInfoReqPb request;
    request.set_client_id(context.clientId);
    request.set_token(context.token);
    request.set_tenant_id(context.tenantId);
    request.set_is_dev_key(isDeviceKey);
    request.set_is_routed(isRouted);
    *request.mutable_object_keys() = { objectKeys.begin(), objectKeys.end() };
    RpcAttempt attempt = dispatcher_->Invoke(workerAddr, "GetMetaInfo", [&](WorkerRpcClient &endpoint) {
        response.Clear();
        return endpoint.InvokeObjectRpc(
            GetRemainingRpcTimeoutMs(), "GetMetaInfo", request, response,
            [&](WorkerOCService_BrpcGenericStub &stub, const RpcOptions &options) {
                return stub.GetMetaInfo(options, request, response);
            });
    });
    return attempt.status;
}

}  // namespace client
}  // namespace datasystem
