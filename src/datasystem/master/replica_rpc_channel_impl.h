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
 * Description: Replica rpc define.
 */
#include <functional>
#include <memory>
#include <unordered_map>

#include "datasystem/common/ak_sk/ak_sk_manager.h"
#include "datasystem/common/kvstore/rocksdb/replica.h"
#include "datasystem/protos/worker_object.stub.rpc.pb.h"
#include "datasystem/worker/hash_ring/hash_ring.h"
namespace datasystem {
class ReplicaRpcChannelImpl : public ReplicaRpcChannel {
public:
    ReplicaRpcChannelImpl(std::shared_ptr<AkSkManager> akSkManager,
                          std::function<Status(const std::string &workerUuid, HostPort &workerAddr)> workerUuidToAddrFn)
        : akSkManager_(akSkManager), workerUuidToAddrFn_(std::move(workerUuidToAddrFn))
    {
    }

    ~ReplicaRpcChannelImpl() = default;

    /**
     * @brief Try incremental synchronization.
     * @param[in] targetNodeId The target node id.
     * @param[in] dbName The name of rocksdb.
     * @param[in] backupNodeId The backup node id.
     * @param[in] seq The next sequence number.
     * @param[in] replicaId The replica id.
     * @return Status of this call.
     */
    Status TryPSync(const std::string &targetNodeId, const std::string &dbName, const std::string &backupNodeId,
                    rocksdb::SequenceNumber seq, const std::string &replicaId);

    /**
     * @brief Push new logs.
     * @brief Try incremental synchronization.
     * @param[in] targetNodeId The target node id.
     * @param[in] dbName The name of rocksdb.
     * @param[in] action The action of push logs.
     * @param[in] logs The metadata needed to apply.
     * @return Status of this call.
     */
    Status PushNewLogs(const std::string &targetNodeId, const std::string &dbName, PushLogAction action,
                       const std::vector<std::string> &logs);

    /**
     * @brief Fetch log file meta.
     * @param[in] targetNodeId The target node id.
     * @param[in] dbName The name of rocksdb.
     * @param[in] backupNodeId The backup node id.
     * @param[out] fileList The list of log file.
     * @return Status of this call.
     */
    Status FetchMeta(const std::string &targetNodeId, const std::string &dbName, const std::string &backupNodeId,
                     std::vector<std::string> &fileList);

    /**
     * @brief Fetch log file.
     * @param[in] targetNodeId The target node id.
     * @param[in] dbName The name of rocksdb.
     * @param[in] backupNodeId The backup node id.
     * @param[in] file The log file.
     * @param[in] offset The offset of the file.
     * @param[out] isFinish Whether the file is encrypted using crc32.
     * @param[out] data The data of file.
     * @param[out] crc32 The crc32 of file.
     * @return Status of this call.
     */
    Status FetchFile(const std::string &targetNodeId, const std::string &dbName, const std::string &backupNodeId,
                     const std::string &file, const uint64_t offset, bool &isFinish, std::string &data,
                     uint32_t &crc32);

    Status GetReplicaApi(const std::string &targetNodeId, std::shared_ptr<ReplicationService_Stub> &api);

private:
    std::unordered_map<std::string, std::shared_ptr<ReplicationService_Stub>> apis_;
    std::shared_ptr<AkSkManager> akSkManager_;
    std::function<Status(const std::string &workerUuid, HostPort &workerAddr)> workerUuidToAddrFn_;
    std::shared_timed_mutex mutex_;
};
}  // namespace datasystem