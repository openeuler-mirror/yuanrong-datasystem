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
 * Description: Replica rpc implement.
 */

#include "datasystem/master/replica_rpc_channel_impl.h"

#include <memory>
#include <utility>

#include "datasystem/common/log/log.h"
#include "datasystem/common/rpc/rpc_auth_key_manager.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/protos/worker_object.pb.h"
#include "datasystem/protos/worker_object.stub.rpc.pb.h"
#include "datasystem/utils/status.h"
#include "datasystem/worker/hash_ring/hash_ring.h"

namespace datasystem {
Status ReplicaRpcChannelImpl::TryPSync(const std::string &targetNodeId, const std::string &dbName,
                                       const std::string &backupNodeId, rocksdb::SequenceNumber seq,
                                       const std::string &replicaId)
{
    TryPSyncReqPb req;
    TryPSyncRspPb rsp;
    (void)replicaId;
    req.set_db_name(dbName);
    req.set_backup_node_id(backupNodeId);
    req.set_seq(seq);
    RETURN_IF_NOT_OK(akSkManager_->GenerateSignature(req));
    std::shared_ptr<ReplicationService_Stub> api;
    RETURN_IF_NOT_OK(GetReplicaApi(targetNodeId, api));
    RETURN_IF_NOT_OK(api->TryPSync(req, rsp));
    return Status::OK();
}

Status ReplicaRpcChannelImpl::PushNewLogs(const std::string &targetNodeId, const std::string &dbName,
                                          PushLogAction action, const std::vector<std::string> &logs)
{
    PushNewLogsReqPb req;
    PushNewLogsRspPb rsp;
    req.set_action_type(PushNewLogsReqPb::ActionType(action));
    req.set_db_name(dbName);
    RETURN_IF_NOT_OK(akSkManager_->GenerateSignature(req));
    std::shared_ptr<ReplicationService_Stub> api;
    RETURN_IF_NOT_OK(GetReplicaApi(targetNodeId, api));
    std::vector<MemView> payloads;
    for (auto &log : logs) {
        payloads.emplace_back(log.data(), log.size());
    }
    RETURN_IF_NOT_OK(api->PushNewLogs(req, rsp, payloads));
    return Status::OK();
}

Status ReplicaRpcChannelImpl::FetchMeta(const std::string &targetNodeId, const std::string &dbName,
                                        const std::string &backupNodeId, std::vector<std::string> &fileList)
{
    FetchMetaReqPb req;
    FetchMetaRspPb rsp;
    req.set_db_name(dbName);
    req.set_backup_node_id(backupNodeId);
    RETURN_IF_NOT_OK(akSkManager_->GenerateSignature(req));
    std::shared_ptr<ReplicationService_Stub> api;
    RETURN_IF_NOT_OK(GetReplicaApi(targetNodeId, api));
    RETURN_IF_NOT_OK(api->FetchMeta(req, rsp));
    for (auto &iter : (*rsp.mutable_file_list())) {
        fileList.emplace_back(iter);
    }
    return Status::OK();
}

Status ReplicaRpcChannelImpl::FetchFile(const std::string &targetNodeId, const std::string &dbName,
                                        const std::string &backupNodeId, const std::string &file, const uint64_t offset,
                                        bool &isFinish, std::string &data, uint32_t &crc32)
{
    FetchFileReqPb req;
    FetchFileRspPb rsp;
    req.set_db_name(dbName);
    req.set_backup_node_id(backupNodeId);
    req.set_file(file);
    req.set_offset(offset);
    req.set_crc32(crc32);
    RETURN_IF_NOT_OK(akSkManager_->GenerateSignature(req));
    std::shared_ptr<ReplicationService_Stub> api;
    RETURN_IF_NOT_OK(GetReplicaApi(targetNodeId, api));
    std::vector<RpcMessage> payloads;
    RETURN_IF_NOT_OK(api->FetchFile(req, rsp, payloads));
    for (auto &iter : payloads) {
        data += std::string((const char *)iter.Data(), iter.Size());
    }
    isFinish = rsp.is_finish();
    crc32 = rsp.crc32();
    return Status::OK();
}

Status ReplicaRpcChannelImpl::GetReplicaApi(const std::string &targetNodeId,
                                            std::shared_ptr<ReplicationService_Stub> &api)
{
    std::lock_guard<std::shared_timed_mutex> locker(mutex_);
    auto iter = apis_.find(targetNodeId);
    if (iter != apis_.end()) {
        VLOG(1) << "Find target node id: " << targetNodeId;
        api = iter->second;
    } else {
        LOG(INFO) << "Create target node id: " << targetNodeId;
        HostPort hostPort;
        RETURN_IF_NOT_OK_PRINT_ERROR_MSG(workerUuidToAddrFn_(targetNodeId, hostPort),
                                         "GetReplicaApi failed when get worker address.");
        RpcCredential cred;
        RETURN_IF_NOT_OK_PRINT_ERROR_MSG(RpcAuthKeyManager::CreateCredentials(WORKER_SERVER_NAME, cred),
                                         "GetReplicaApi failed.");
        auto channel = std::make_shared<RpcChannel>(hostPort, cred);
        auto rpcSession = std::make_shared<ReplicationService_Stub>(channel);
        apis_[targetNodeId] = rpcSession;
        api = rpcSession;
    }
    return Status::OK();
}
}  // namespace datasystem
