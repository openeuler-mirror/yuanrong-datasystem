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
 * Description: Replica implement.
 */
#include "datasystem/common/kvstore/rocksdb/replica.h"

#include <cstdint>
#include <fcntl.h>
#include <mutex>
#include <shared_mutex>
#include <vector>

#include "rocksdb/status.h"
#include "rocksdb/write_batch_base.h"
#include "rocksdb/utilities/checkpoint.h"
#include "zconf.h"
#include "zlib.h"

#include "datasystem/common/constants.h"
#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/kvstore/rocksdb/rocks_store.h"
#include "datasystem/common/util/file_util.h"
#include "datasystem/common/util/format.h"
#include "datasystem/common/util/raii.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/uri.h"
#include "datasystem/common/log/log.h"
#include "datasystem/utils/status.h"

namespace datasystem {
const size_t MAX_REPLICATION_BYTES = 32 * 1024 * 1024;
const std::string OBJECT_META_NAME = "object_metadata";

namespace {
std::unordered_map<std::string, rocksdb::ColumnFamilyOptions> GetTableOptions()
{
    rocksdb::ColumnFamilyOptions prefixOption = rocksdb::ColumnFamilyOptions();
    std::unordered_map<std::string, rocksdb::ColumnFamilyOptions> tableOptions;
    return tableOptions;
}
}  // namespace

Replica::Replica(const std::string &dbName, const std::string &dbRootPath, const std::string &currWorkerId,
                 ReplicaRpcChannel *channel, bool multiReplicaEnabled)
    : dbName_(dbName), currWorkerId_(currWorkerId), multiReplicaEnabled_(multiReplicaEnabled), channel_(channel)
{
    std::string replicaDBRootPath = dbRootPath + "/" + META_NAME;
    dbPath_ = multiReplicaEnabled ? (replicaDBRootPath + "/" + dbName_) : dbRootPath;
    checkpointPath_ = replicaDBRootPath + "/checkpoint_" + dbName;
    syncPath_ = replicaDBRootPath + "/sync_" + dbName;
    env_ = rocksdb::Env::Default();
}

Replica::~Replica()
{
    Shutdown();
}

Status Replica::Init()
{
    if (multiReplicaEnabled_) {
        RETURN_IF_NOT_OK_PRINT_ERROR_MSG(InitRocksStore(dbPath_, ocStore_), "InitRocksStore failed");
        scStore_ = ocStore_;
    } else {
        std::string objectPath = dbPath_ + "/" + OBJECT_META_NAME;
        RETURN_IF_NOT_OK_PRINT_ERROR_MSG(InitRocksStore(objectPath, ocStore_), "InitRocksStore for object failed");
    }
    return Status::OK();
}

void Replica::Shutdown()
{
    StopPrimaryTask();
    StopBackupTask();
}

Status Replica::CreateOcTable(RocksStore *store)
{
    std::vector<std::string> tables;
    RETURN_IF_NOT_OK(store->ListTables(tables));
    VLOG(1) << "Existing tables in rocksdb: " << VectorToString(tables);
    RETURN_IF_NOT_OK(CreateTable(META_TABLE, store, tables));
    RETURN_IF_NOT_OK(CreateTable(LOCATION_TABLE, store, tables));
    RETURN_IF_NOT_OK(CreateTable(NESTED_TABLE, store, tables));
    RETURN_IF_NOT_OK(CreateTable(NESTED_COUNT_TABLE, store, tables));
    RETURN_IF_NOT_OK(CreateTable(ASYNC_WORKER_OP_TABLE, store, tables));
    RETURN_IF_NOT_OK(CreateTable(GLOBAL_REF_TABLE, store, tables));
    RETURN_IF_NOT_OK(CreateTable(GLOBAL_CACHE_TABLE, store, tables));
    RETURN_IF_NOT_OK(CreateTable(REMOTE_CLIENT_OBJ_REF_TABLE, store, tables));
    RETURN_IF_NOT_OK(CreateTable(REMOTE_CLIENT_REF_TABLE, store, tables));
    RETURN_IF_NOT_OK(CreateTable(HEALTH_TABLE, store, tables));
    LOG(INFO) << FormatString("CreateOcTable success.");
    return Status::OK();
}

Status Replica::CreateTable(const std::string &tableName, RocksStore *store, std::vector<std::string> &tables)
{
    VLOG(1) << "tableName:" << tableName;
    bool exits = (std::find(tables.begin(), tables.end(), tableName) != tables.end());
    if (!exits) {
        tables.emplace_back(tableName);
        auto options = rocksdb::ColumnFamilyOptions();
        RETURN_IF_NOT_OK(store->CreateTable(tableName, options));
        LOG(INFO) << FormatString("Create table { %s } successfully.", tableName);
    }
    return Status::OK();
}

Status Replica::CreateRocksStoreInstance(const std::string &dbPath, std::shared_ptr<RocksStore> &store)
{
    auto options = GetTableOptions();
    store = RocksStore::GetInstance(dbPath, options);
    CHECK_FAIL_RETURN_STATUS(store != nullptr, K_KVSTORE_ERROR, "Cannot open the key/value store");
    return Status::OK();
}

Status Replica::CreateRocksStoreInstanceAndTable(const std::string &dbPath, std::shared_ptr<RocksStore> &store)
{
    RETURN_IF_NOT_OK(CreateRocksStoreInstance(dbPath, store));
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(CreateOcTable(store.get()), "Replica create oc table failed");
    return Status::OK();
}

Status Replica::InitRocksStore(const std::string &dbPath, std::shared_ptr<RocksStore> &store)
{
    if (!FileExist(dbPath)) {
        const int permission = 0700;
        RETURN_IF_NOT_OK(CreateDir(dbPath, true, permission));
    }
    Uri uri;
    // Change the permission of files in dbPath to 0600.
    const mode_t permission = 0600;
    RETURN_IF_NOT_OK(uri.ModifyFilesInInputDir(dbPath, permission));

    Status rc = CreateRocksStoreInstance(dbPath, store);
    if (rc.IsError() && !multiReplicaEnabled_) {
        LOG(WARNING) << "Open key/value store failed, clear rocksdb files and try to recover from etcd.";
        RETURN_IF_NOT_OK(RemoveAll(dbPath));
        const int permission = 0700;
        RETURN_IF_NOT_OK(CreateDir(dbPath, true, permission));
        rc = CreateRocksStoreInstance(dbPath, store);
    }
    return rc;
}

Status Replica::AddPrimary(const std::string &primaryNodeId)
{
    if (GetReplicaType() == ReplicaType::Primary) {
        LOG(INFO) << "Primary node can't do the operation of AddPrimary.";
        return Status::OK();
    }

    if (backupTask_ != nullptr) {
        auto existPrimaryNodeId = backupTask_->GetPrimaryNodeId();
        if (existPrimaryNodeId == primaryNodeId) {
            LOG(INFO) << "BackupTask is exist.";
            return Status::OK();
        } else {
            LOG(INFO) << "BackupTask is exist and the primary node id is different";
            backupTask_->Stop();
        }
    }
    LOG(INFO) << "Add primary to backup: " << primaryNodeId;
    backupTask_ = std::make_unique<BackupTask>(this, primaryNodeId);
    backupTask_->Start();
    return Status::OK();
}

Status Replica::CheckWALBoundary(rocksdb::SequenceNumber nextSeq)
{
    auto db = GetObjectRocksStore()->GetRawDB();
    auto currSeq = db->GetLatestSequenceNumber();
    LOG(INFO) << FormatString("Request nextSeq: %zu, currSeq: %zu", nextSeq, currSeq);
    if (nextSeq == currSeq + 1) {
        return Status::OK();
    }
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(nextSeq < currSeq + 1, K_RUNTIME_ERROR,
                                         FormatString("Invalid nextSeq %zu, currSeq %zu", nextSeq, currSeq));

    std::unique_ptr<rocksdb::TransactionLogIterator> iter;
    auto rc = db->GetUpdatesSince(nextSeq, &iter);
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(rc.ok(), K_RUNTIME_ERROR, rc.ToString());
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(iter->Valid(), K_RUNTIME_ERROR, "rocksdb interator is invalid");
    auto sequence = iter->GetBatch().sequence;
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(
        nextSeq == sequence, K_RUNTIME_ERROR,
        FormatString("The nextSeq %zu cannot find in WAL, sequcece in WAL is %zu", nextSeq, sequence));
    return Status::OK();
}

Status Replica::HandleTryPSync(const std::string &backupNodeId, rocksdb::SequenceNumber nextSeq,
                               const std::string &replicaId)
{
    if (GetReplicaType() == ReplicaType::Backup) {
        LOG(INFO) << "Backup node can't do the operation of HandleTryPSync.";
        return Status::OK();
    }
    RETURN_IF_NOT_OK(CheckWALBoundary(nextSeq));

    std::lock_guard<std::shared_timed_mutex> locker(mutex_);
    auto iter = primaryTasks_.find(backupNodeId);
    if (iter != primaryTasks_.end()) {
        iter->second->Stop();
        primaryTasks_.erase(iter);
    }

    (void)replicaId;
    auto primaryTask = std::make_unique<PrimaryTask>(this, backupNodeId, nextSeq);
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(primaryTask->Start().IsOk(), K_REPLICA_NOT_READY,
                                         "Primary task start failed.");
    primaryTasks_.emplace(backupNodeId, std::move(primaryTask));
    return Status::OK();
}

Status Replica::ApplyLogs(const std::pair<PushLogAction, std::vector<std::string>> &logs)
{
    if (GetReplicaType() == ReplicaType::Primary) {
        LOG(INFO) << "Primary node can't do the operation of ApplyLogs.";
        return Status::OK();
    }
    RETURN_RUNTIME_ERROR_IF_NULL(backupTask_);
    RETURN_IF_NOT_OK(backupTask_->ApplyLogs(logs));
    if (GetReplicaType() == ReplicaType::Primary) {
        LOG(INFO) << "Primary node can't do the operation of AddPrimary.";
        return Status::OK();
    }
    return Status::OK();
}

Status Replica::HandleFetchMeta(const std::string &backupNodeId, std::vector<std::string> &fileList)
{
    (void)backupNodeId;
    auto rocksStore = GetObjectRocksStore();
    RETURN_RUNTIME_ERROR_IF_NULL(rocksStore);
    auto rocksdb = rocksStore->GetRawDB();
    RETURN_RUNTIME_ERROR_IF_NULL(rocksdb);
    if (FileExist(checkpointPath_)) {
        RETURN_IF_NOT_OK(RemoveAll(checkpointPath_));
    }
    rocksdb::Checkpoint *checkpoint = nullptr;
    rocksdb::Status s = rocksdb::Checkpoint::Create(rocksdb, &checkpoint);
    if (!s.ok()) {
        RETURN_STATUS_LOG_ERROR(K_KVSTORE_ERROR,
                                FormatString("Failed to create checkpoint object. Error: %s", s.ToString()));
    }

    std::unique_ptr<rocksdb::Checkpoint> checkpointGuard(checkpoint);

    // Create checkpoint of rocksdb
    s = checkpoint->CreateCheckpoint(checkpointPath_);
    if (!s.ok()) {
        RETURN_STATUS_LOG_ERROR(
            K_KVSTORE_ERROR, FormatString("Rocksdb failed to create checkpoint (snapshot). Error:  %s", s.ToString()));
    }

    LOG(INFO) << "Rocksdb Create checkpoint successfully";

    // Get checkpoint file list
    std::vector<std::string> result;
    s = env_->GetChildren(checkpointPath_, &result);
    if (!s.ok()) {
        RETURN_STATUS_LOG_ERROR(K_KVSTORE_ERROR,
                                FormatString("Rocksdb failed to get checkpoint (snapshot). Error:  %s", s.ToString()));
    }
    for (const auto &f : result) {
        if (f == "." || f == "..") {
            continue;
        }
        fileList.emplace_back(f);
    }
    return Status::OK();
}

Status Replica::HandleFetchFile(const std::string &backupNodeId, const std::string &file, uint64_t pos, bool &isFinish,
                                std::string &data, uint32_t &crc32Calc)
{
    (void)backupNodeId;
    auto rocksStore = GetObjectRocksStore();
    RETURN_RUNTIME_ERROR_IF_NULL(rocksStore);
    Uri uri(checkpointPath_ + "/" + file);
    uri.NormalizePath();
    auto absPath = uri.Path();
    auto s = env_->FileExists(absPath);
    if (!s.ok()) {
        RETURN_STATUS_LOG_ERROR(K_RUNTIME_ERROR, FormatString("Rocksdb data file [%s] not found", absPath));
    }
    uint64_t fileSize = 0;
    env_->GetFileSize(absPath, &fileSize);
    int fd;
    RETURN_IF_NOT_OK(OpenFile(absPath, O_RDONLY, &fd));
    Raii raii([&fd] { RETRY_ON_EINTR(close(fd)); });
    size_t maxFetchSize = MAX_REPLICATION_BYTES;
    INJECT_POINT("worker.HandleFetchFile.fetchSize", [&maxFetchSize](size_t fetchSize) {
        maxFetchSize = fetchSize;
        return Status::OK();
    });
    CHECK_FAIL_RETURN_STATUS(fileSize > pos, K_INVALID,
                             "The param of pos is invalid. It exceeded the maximum file size.");
    auto readSize = std::min(fileSize - pos, maxFetchSize);
    data.resize(readSize);
    RETURN_IF_NOT_OK(ReadFile(fd, data.data(), readSize, pos));
    crc32Calc = crc32(crc32Calc, (const Bytef *)data.data(), data.length());
    isFinish = readSize + pos != fileSize ? false : true;
    LOG(INFO) << "Succeed handel file " << file << " from pos:  " << pos << " size: " << readSize
              << " crc32: " << crc32Calc << " is finish: " << isFinish;
    return Status::OK();
}

Status Replica::GetPrimaryTaskInfo(const std::string &backupNodeId, uint64_t &latestSeqNo, uint64_t &latestSendSeqNo)
{
    INJECT_POINT("replica.SyncGap", [&](uint64_t lsn, uint64_t sendLsn) {
        latestSeqNo = lsn;
        latestSendSeqNo = sendLsn;
        return Status::OK();
    });
    std::shared_lock<std::shared_timed_mutex> rlock(mutex_);
    auto iter = primaryTasks_.find(backupNodeId);
    if (iter == primaryTasks_.end()) {
        RETURN_STATUS(K_RUNTIME_ERROR, FormatString("Not found %s in primaryTasks", backupNodeId));
    }
    latestSeqNo = iter->second->GetLatestSeqNumber();
    latestSendSeqNo = iter->second->GetLatestSendSeqNumber();
    return Status::OK();
}

Status Replica::Remove()
{
    if (multiReplicaEnabled_) {
        Shutdown();
        ocStore_->Close();
        return RemoveAll(dbPath_);
    }
    return Status::OK();
}

Status Replica::RemoveRocksFromFileSystem(const std::string &dbRootPath, bool multiReplicaEnabled)
{
    if (multiReplicaEnabled) {
        std::string path = dbRootPath + "/" + META_NAME;
        LOG(INFO) << "try remove path " << path;
        RETURN_IF_NOT_OK(RemoveAll(path));
    } else {
        std::string objectPath = dbRootPath + "/" + OBJECT_META_NAME;
        RETURN_IF_NOT_OK(RemoveAll(objectPath));
    }
    return Status::OK();
}

void Replica::SetReplicaType(ReplicaType type)
{
    if (replicaType_ == type) {
        return;
    }
    if (type == ReplicaType::Backup) {
        StopPrimaryTask();
    } else if (type == ReplicaType::Primary) {
        StopBackupTask();
    } else {
        LOG(WARNING) << "Unknown replica type " << (int)type;
    }
    replicaType_ = type;
}

void Replica::StopPrimaryTask()
{
    std::lock_guard<std::shared_timed_mutex> locker(mutex_);
    for (auto &iter : primaryTasks_) {
        auto task = iter.second.get();
        task->Stop();
    }
    primaryTasks_.clear();
}

void Replica::StopBackupTask()
{
    std::lock_guard<std::shared_timed_mutex> locker(mutex_);
    if (backupTask_ != nullptr) {
        backupTask_->Stop();
        backupTask_ = nullptr;
    }
}
}  // namespace datasystem
