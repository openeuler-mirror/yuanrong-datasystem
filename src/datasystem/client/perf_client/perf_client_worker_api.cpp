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
 * Description: Implement api of admin generic service.
 */

#include "datasystem/client/perf_client/perf_client_worker_api.h"
#include "datasystem/common/rpc/rpc_auth_key_manager.h"

namespace datasystem {
PerfClientWorkerApi::PerfClientWorkerApi(const ConnectOptions &connectOptions)
{
    hostPort_ = HostPort(connectOptions.host, connectOptions.port);
    signature_ = std::make_unique<Signature>(connectOptions.accessKey, connectOptions.secretKey);
    (void)authKeys_.SetClientPublicKey(connectOptions.clientPublicKey);
    (void)authKeys_.SetClientPrivateKey(connectOptions.clientPrivateKey);
    LOG_IF_ERROR(authKeys_.SetServerKey(WORKER_SERVER_NAME, connectOptions.serverPublicKey),
                 "RpcAuthKeys SetServerKey failed");
}

Status PerfClientWorkerApi::Init()
{
    RpcCredential cred;
    RETURN_IF_NOT_OK(RpcAuthKeyManager::CreateClientCredentials(authKeys_, WORKER_SERVER_NAME, cred));
    std::shared_ptr<RpcChannel> channel = std::make_shared<RpcChannel>(hostPort_, cred);
    rpcSession_ = std::make_unique<PerfService_Stub>(std::move(channel));
    return Status::OK();
}

Status PerfClientWorkerApi::GetPerfLog(
    std::unordered_map<std::string, std::unordered_map<std::string, uint64_t>> &perfLog)
{
    GetPerfLogReqPb req;
    GetPerfLogRspPb rsp;
    RETURN_IF_NOT_OK(signature_->GenerateSignature(req));
    RETURN_IF_NOT_OK(rpcSession_->GetPerfLog(req, rsp));

    for (const GetPerfLogRspPb::PerfLogPb &detail : rsp.perf_logs()) {
        std::unordered_map<std::string, uint64_t> perfMap;
        perfMap["count"] = detail.count();
        perfMap["min_time"] = detail.min_time();
        perfMap["max_time"] = detail.max_time();
        perfMap["total_time"] = detail.total_time();
        perfMap["avg_time"] = detail.avg_time();
        perfMap["max_frequency"] = detail.max_frequency();
        perfLog.emplace(detail.key_name(), std::move(perfMap));
    }
    return Status::OK();
}

Status PerfClientWorkerApi::ResetPerfLog()
{
    ResetPerfLogReqPb req;
    ResetPerfLogRspPb rsp;
    RETURN_IF_NOT_OK(signature_->GenerateSignature(req));
    RETURN_IF_NOT_OK(rpcSession_->ResetPerfLog(req, rsp));
    return Status::OK();
}
}  // namespace datasystem