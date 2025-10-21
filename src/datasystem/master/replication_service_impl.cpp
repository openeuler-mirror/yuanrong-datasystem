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
 * Description: Defines the replica service processing main class.
 */

#include "datasystem/master/replication_service_impl.h"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <utility>
#include <vector>

#include "datasystem/common/log/log_helper.h"
#include "datasystem/common/log/log.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/utils/status.h"

namespace datasystem {

Status ReplicationServiceImpl::Init()
{
    return Status::OK();
}

Status ReplicationServiceImpl::TryPSync(const TryPSyncReqPb &req, TryPSyncRspPb &rsp)
{
    (void)rsp;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(akSkManager_->VerifySignatureAndTimestamp(req), "AK/SK failed.");
    LOG(INFO) << "Receive TryPSync " << LogHelper::IgnoreSensitive(req);
    INJECT_POINT("master.Replication.TryPSync");
    std::string targetNodeId = req.backup_node_id();
    std::string dbName = req.db_name();
    int seq = req.seq();
    std::string replicaId = req.replica_id();
    std::shared_ptr<Replica> replica;
    RETURN_IF_NOT_OK(replicaManager_->GetReplica(dbName, replica));
    auto type = replica->GetReplicaType();
    if (type == ReplicaType::Primary) {
        return replica->HandleTryPSync(targetNodeId, seq, replicaId);
    } else {
        LOG(ERROR) << "Dont fetch correct replica in TryPSync.";
        return { K_REPLICA_NOT_READY, "Failed to TryPSync. Dont fetch correct replica." };
    }
}

Status ReplicationServiceImpl::PushNewLogs(const PushNewLogsReqPb &req, PushNewLogsRspPb &rsp,
                                           std::vector<RpcMessage> payloads)
{
    (void)rsp;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(akSkManager_->VerifySignatureAndTimestamp(req), "AK/SK failed.");
    std::string dbName = req.db_name();
    std::string replicaId = req.replica_id();
    PushLogAction action = (PushLogAction)req.action_type();
    std::vector<std::string> logs;
    for (auto &msg : payloads) {
        logs.emplace_back(msg.ToString());
    }
    std::shared_ptr<Replica> replica;
    RETURN_IF_NOT_OK(replicaManager_->GetReplica(dbName, replica));
    auto type = replica->GetReplicaType();
    if (type == ReplicaType::Backup) {
        return replica->ApplyLogs(std::make_pair(action, logs));
    } else {
        LOG(ERROR) << "Dont fetch correct replica type in PushNewLogs.";
        return { K_REPLICA_NOT_READY, "Failed to PushNewLogs. Dont fetch correct replica." };
    }
}

Status ReplicationServiceImpl::FetchMeta(const FetchMetaReqPb &req, FetchMetaRspPb &rsp)
{
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(akSkManager_->VerifySignatureAndTimestamp(req), "AK/SK failed.");
    LOG(INFO) << "Receive FetchMetaReq " << LogHelper::IgnoreSensitive(req);
    std::string targetNodeId = req.backup_node_id();
    std::string dbName = req.db_name();
    std::vector<std::string> fileList;
    std::shared_ptr<Replica> replica;
    RETURN_IF_NOT_OK(replicaManager_->GetReplica(dbName, replica));
    auto type = replica->GetReplicaType();
    if (type == ReplicaType::Primary) {
        RETURN_IF_NOT_OK(replica->HandleFetchMeta(targetNodeId, fileList));
    } else {
        LOG(ERROR) << "Dont fetch correct replica type in FetchMeta.";
        return { K_REPLICA_NOT_READY, "Failed to FetchMeta. Dont fetch correct replica." };
    }
    for (const auto &file : fileList) {
        rsp.add_file_list(file);
    }
    return Status::OK();
}

Status ReplicationServiceImpl::FetchFile(const FetchFileReqPb &req, FetchFileRspPb &rsp,
                                         std::vector<RpcMessage> &payloads)
{
    (void)rsp;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(akSkManager_->VerifySignatureAndTimestamp(req), "AK/SK failed.");
    LOG(INFO) << "Receive FetchFileReq " << LogHelper::IgnoreSensitive(req);
    std::string targetNodeId = req.backup_node_id();
    std::string dbName = req.db_name();
    std::string file = req.file();
    size_t offset = req.offset();
    uint32_t crc32Calc = req.crc32();
    bool isFinish;
    std::string data;
    std::shared_ptr<Replica> replica;
    RETURN_IF_NOT_OK(replicaManager_->GetReplica(dbName, replica));
    auto type = replica->GetReplicaType();
    if (type == ReplicaType::Primary) {
        RETURN_IF_NOT_OK(replica->HandleFetchFile(targetNodeId, file, offset, isFinish, data, crc32Calc));
    } else {
        LOG(ERROR) << "Dont fetch correct replica type in FetchFile.";
        return { K_RUNTIME_ERROR, "Failed to FetchFile. Dont fetch correct replica." };
    }
    rsp.set_is_finish(isFinish);
    rsp.set_crc32(crc32Calc);
    RpcMessage msg;
    msg.CopyString(data);
    payloads.emplace_back(std::move(msg));
    return Status::OK();
}
}  // namespace datasystem
