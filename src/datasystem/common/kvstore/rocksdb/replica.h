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
 * Description: Replica define.
 */

#include <cstdint>
#include <shared_mutex>
#include <unordered_map>
#include <utility>
#include <vector>

#include "datasystem/common/kvstore/rocksdb/replica_task.h"
#include "datasystem/common/kvstore/rocksdb/rocks_store.h"
#include "datasystem/common/util/queue/queue.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/thread.h"
#include "datasystem/utils/status.h"

#ifndef DATASYSTEM_COMMON_KVSTORE_REPLICA_H
#define DATASYSTEM_COMMON_KVSTORE_REPLICA_H
namespace datasystem {
enum class ReplicaType { Primary, Backup };
enum class State { FullSync, PSync, Failed };

const std::string META_NAME = "replicas";

class ReplicaRpcChannel {
public:
    virtual ~ReplicaRpcChannel() = default;

    virtual Status TryPSync(const std::string &targetNodeId, const std::string &dbName, const std::string &backupNodeId,
                            rocksdb::SequenceNumber seq, const std::string &replicaId) = 0;

    virtual Status PushNewLogs(const std::string &targetNodeId, const std::string &dbName, PushLogAction action,
                               const std::vector<std::string> &logs) = 0;
    virtual Status FetchMeta(const std::string &targetNodeId, const std::string &dbName,
                             const std::string &backupNodeId, std::vector<std::string> &fileList) = 0;

    virtual Status FetchFile(const std::string &targetNodeId, const std::string &dbName,
                             const std::string &backupNodeId, const std::string &file, const uint64_t offset,
                             bool &isFinish, std::string &data, uint32_t &crc32) = 0;
};

class Replica {
public:
    Replica(const std::string &dbName, const std::string &dbRootPath, const std::string &currWorkerId,
            ReplicaRpcChannel *channel, bool multiReplicaEnabled);
    virtual ~Replica();

    Replica(const Replica &) = delete;
    Replica &operator=(const Replica &) = delete;

    /**
     * @brief Init replica.
     * @return Status of this call.
     */
    Status Init();

    /**
     * @brief Shutdown replica.
     */
    void Shutdown();

    /**
     * @brief Get the instance of rocks store.
     * @param[in] dbPath Directory to store the database.
     * @return The rocks store instance.
     */
    static Status CreateRocksStoreInstance(const std::string &dbPath, std::shared_ptr<RocksStore> &store);

    /**
     * @brief Get the instance of rocks store and create table.
     * @param[in] dbPath Directory to store the database.
     * @return The rocks store instance.
     */
    static Status CreateRocksStoreInstanceAndTable(const std::string &dbPath, std::shared_ptr<RocksStore> &store);

    /**
     * @brief Create object cache tables.
     * @param[in] store The rocks store pointer.
     * @return Status of this call.
     */
    static Status CreateOcTable(RocksStore *store);

    /**
     * @brief Create stream cache tables.
     * @return Status of this call.
     */
    static Status CreateScTable(RocksStore *store);

    /**
     * @brief Create table in rocksdb.
     * @param[in] table The Rocksdb table name to be created.
     * @param[in] store The rocks store pointer.
     * @param[in] isSc Create for stream cache.
     * @param[in] tables The tables currently in rocksdb.
     * @return Status of the call.
     */
    static Status CreateTable(const std::string &tableName, RocksStore *store, bool isSc,
                              std::vector<std::string> &tables);

    /**
     * @brief Get the object rocks store instance.
     * @return The rocks store instance.
     */
    RocksStore *GetObjectRocksStore()
    {
        return ocStore_.get();
    }

    /**
     * @brief Get the stream rocks store instance.
     * @return The rocks store instance.
     */
    RocksStore *GetStreamRocksStore()
    {
        return scStore_.get();
    }

    /**
     * @brief Get the rocks db path.
     * @return The rocks db path.
     */
    std::string GetDbPath()
    {
        return dbPath_;
    }

    /**
     * @brief Get the rocks checkpointPath.
     * @return The rocks store instance.
     */
    const std::string GetCheckpointPath()
    {
        return checkpointPath_;
    }

    /**
     * @brief Get the rocks syncPath.
     * @return The rocks store instance.
     */
    const std::string GetSyncPath()
    {
        return syncPath_;
    }

    /**
     * @brief Set the replica type.
     * @param[in] type The specified replica type
     */
    void SetReplicaType(ReplicaType type);

    /**
     * @brief Get the replica type.
     * @return ReplicaType
     */
    ReplicaType GetReplicaType()
    {
        return replicaType_;
    }

    /**
     * @brief Remove current replica.
     * @return Status of this call.
     */
    Status Remove();

    inline void SwapRocksStore(std::shared_ptr<RocksStore> &instance)
    {
        ocStore_ = instance;
        scStore_ = ocStore_;
    }

    /**
     * @brief Set primary node for backup replica and start incremental sync task.
     * @param[in] primaryNodeId The primary node uuid.
     * @return Status of this call.
     */
    Status AddPrimary(const std::string &primaryNodeId);

    /**
     * @brief Register rpc channel.
     * @param[in] channel The rpc channel.
     */
    void RegisterRpcChannel(ReplicaRpcChannel *channel)
    {
        channel_ = channel;
    }

    /**
     * @brief Fetch log file meta.
     * @param[in] targetNodeId The target node id.
     * @param[out] fileList The list of log file.
     * @return Status of this call.
     */
    Status FetchMeta(const std::string &targetNodeId, std::vector<std::string> &fileList)
    {
        return channel_->FetchMeta(targetNodeId, dbName_, currWorkerId_, fileList);
    }

    /**
     * @brief Fetch log file.
     * @param[in] targetNodeId The target node id.
     * @param[in] file The log file.
     * @param[in] offset The offset of the file.
     * @param[out] isFinish Whether the file is encrypted using crc32.
     * @param[out] data The data of file.
     * @param[out] crc32 The crc32 of file.
     * @return Status of this call.
     */
    Status FetchFile(const std::string &targetNodeId, const std::string &file, const uint64_t offset, bool &isFinish,
                     std::string &data, uint32_t &crc32)
    {
        return channel_->FetchFile(targetNodeId, dbName_, currWorkerId_, file, offset, isFinish, data, crc32);
    }

    /**
     * @brief Handle fetch meta method.
     * @param[in] backupNodeId The backup node id.
     * @param[out] fileList The list of log file.
     * @return Status of this call.
     */
    Status HandleFetchMeta(const std::string &backupNodeId, std::vector<std::string> &fileList);

    /**
     * @brief Handle fetch file method.
     * @param[in] backupNodeId The backup node id.
     * @param[in] file The log file.
     * @param[in] offset The offset of the file.
     * @param[out] isFinish Whether the file is encrypted using crc32.
     * @param[out] data The data of file.
     * @param[out] crc32Calc The crc32 of file.
     * @return Status of this call.
     */
    Status HandleFetchFile(const std::string &backupNodeId, const std::string &file, uint64_t offset, bool &isFinish,
                           std::string &data, uint32_t &crc32Calc);

    /**
     * @brief Get the parimary task information.
     * @param[in] backupNodeId The backup node id.
     * @param[out] latestSeqNo The the latest seq no in current rocksdb.
     * @param[out] latestSendSeqNo The latest seq no already send to backup replica.
     * @return Status of this call.
     */
    Status GetPrimaryTaskInfo(const std::string &backupNodeId, uint64_t &latestSeqNo, uint64_t &latestSendSeqNo);

    /**
     * @brief Try incremental synchronization.
     * @param[in] targetNodeId The target node id.
     * @param[in] nextSeq The next sequence number.
     * @param[in] replicaId The replica id.
     * @return Status of this call.
     */
    Status TryPSync(const std::string &targetNodeId, rocksdb::SequenceNumber nextSeq, const std::string &replicaId)
    {
        return channel_->TryPSync(targetNodeId, dbName_, currWorkerId_, nextSeq, replicaId);
    }

    /**
     * @brief Handle try incremental synchronization.
     * @param[in] backupNodeId The backup node uuid.
     * @param[in] nextSeq The next sequence number.
     * @param[in] replicaId The replica id.
     * @return Status of this call.
     */
    Status HandleTryPSync(const std::string &backupNodeId, rocksdb::SequenceNumber nextSeq,
                          const std::string &replicaId);

    /**
     * @brief * @brief Apply metadata information.
     * @param[in] logs The metadata needed to apply.
     * @return Status of this call.
     */
    Status ApplyLogs(const std::pair<PushLogAction, std::vector<std::string>> &logs);

    /**
     * @brief Push new logs.
     * @brief Try incremental synchronization.
     * @param[in] targetNodeId The target node id.
     * @param[in] action The action of push logs.
     * @param[in] logs The metadata needed to apply.
     * @return Status of this call.
     */
    Status PushNewLogs(const std::string &targetNodeId, PushLogAction action, const std::vector<std::string> &logs)
    {
        return channel_->PushNewLogs(targetNodeId, dbName_, action, logs);
    }

    rocksdb::Env *GetRocksStoreEnv()
    {
        return env_;
    }

    /**
     * @brief Remove rocks db from filesystem.
     * @param[in] dbRootPath The db root path.
     * @param[in] multiReplicaEnabled Whether enable multi-replica.
     * @return Status of this call.
     */
    static Status RemoveRocksFromFileSystem(const std::string &dbRootPath, bool multiReplicaEnabled);

    std::string GetDbName()
    {
        return dbName_;
    }

    static std::string ReplicaTypeToString(ReplicaType type)
    {
        switch (type) {
            case ReplicaType::Primary:
                return "Primary";
            case ReplicaType::Backup:
                return "Backup";
            default:
                return "Unknown";
        }
    }

private:
    /**
     * @brief Init RocksStore instance.
     *
     * @param[in] dbPath The rocks db path.
     * @param[out] store  The RocksStore instance.
     * @return Status of this call.
     */
    Status InitRocksStore(const std::string &dbPath, std::shared_ptr<RocksStore> &store);

    /**
     * @brief Check the nextSeq in WAL bondary.
     * @param[in] nextSeq The next sequence number.
     * @return Status of this call.
     */
    Status CheckWALBoundary(rocksdb::SequenceNumber nextSeq);

    /**
     * @brief Stop primary task.
     */
    void StopPrimaryTask();

    /**
     * @brief Stop backup task.
     */
    void StopBackupTask();

    std::string dbName_;
    std::string checkpointPath_;
    std::string syncPath_;
    std::string currWorkerId_;
    bool multiReplicaEnabled_;
    std::string dbPath_;
    std::shared_ptr<RocksStore> ocStore_;
    std::shared_ptr<RocksStore> scStore_;
    std::unique_ptr<BackupTask> backupTask_;
    std::unordered_map<std::string, std::unique_ptr<PrimaryTask>> primaryTasks_;
    ReplicaType replicaType_{ ReplicaType::Backup };
    ReplicaRpcChannel *channel_{ nullptr };
    rocksdb::Env *env_;
    std::shared_timed_mutex mutex_;
};

}  // namespace datasystem
#endif
