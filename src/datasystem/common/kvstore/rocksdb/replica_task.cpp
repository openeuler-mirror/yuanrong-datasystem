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

#include "datasystem/common/kvstore/rocksdb/replica_task.h"

#include <chrono>
#include <cstdint>
#include <thread>
#include <unistd.h>
#include <unistd.h>
#include <vector>

#include "zconf.h"
#include "zlib.h"

#include "datasystem/common/constants.h"
#include "datasystem/common/kvstore/rocksdb/replica.h"
#include "datasystem/common/kvstore/rocksdb/rocks_store.h"
#include "datasystem/common/log/trace.h"
#include "datasystem/common/log/log.h"
#include "datasystem/common/util/file_util.h"
#include "datasystem/common/util/format.h"
#include "datasystem/common/util/raii.h"
#include "datasystem/common/util/rpc_util.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/timer.h"
#include "datasystem/common/util/uuid_generator.h"
#include "datasystem/utils/status.h"

namespace datasystem {
const double PSYNC_RETRY_TIME_S = 25;          // 25s
const uint64_t NO_SPACE_RETRY_TIME_MS = 1000;  // 1s
const uint64_t RPC_RETRY_TIME_MS = 1000;       // 1s
const double PSYNC_HEARTBEAT_TIME_S = 20;      // 20s
const uint64_t QUEUE_TIMEOUT_MS = 1000;        // 1s
const uint64_t WAIT_READY_TIME_MS = 1000;      // 1s

TaskBase::TaskBase(Replica *replica) : replica_(replica), db_(replica->GetObjectRocksStore()->GetRawDB())
{
    waitPost_ = std::make_unique<WaitPost>();
}

BackupTask::~BackupTask()
{
    waitPost_->Set();
    Stop();
}

Status BackupTask::ApplyLogs(const std::pair<PushLogAction, std::vector<std::string>> &logs)
{
    return logsQueue_->Offer(logs, QUEUE_TIMEOUT_MS);
}

std::string BackupTask::GetPrimaryNodeId()
{
    return primaryNodeId_;
}

void BackupTask::LogReplay()
{
    std::string dbName = replica_->GetDbName();
    LOG(INFO) << "Start LogReplay for replica " << dbName << ", currSeq:" << GetLatestSeqNumber();
    Timer timer;
    Timer checkTimer;
    while (!stop_) {
        std::pair<PushLogAction, std::vector<std::string>> logs;
        Status rc = logsQueue_->Poll(&logs, 100);
        if (checkTimer.ElapsedSecond() > CHECK_FILE_EXIST_INTERVAL_S) {
            if (!FileExist(replica_->GetDbPath())) {
                LOG(INFO) << dbName << " db path " << replica_->GetDbPath() << " not exist, stop log replay";
                return;
            } else {
                checkTimer.Reset();
            }
        }
        if (rc.GetCode() == StatusCode::K_TRY_AGAIN) {
            if (timer.ElapsedSecond() > PSYNC_RETRY_TIME_S) {
                break;
            }
            continue;
        }
        auto action = logs.first;
        if (action == PushLogAction::HEARTBEAT) {
            timer.Reset();
            continue;
        }

        if (action == PushLogAction::SHUTDOWN) {
            stop_ = true;
            LOG(INFO) << "Receive shutdown from primary for replica " << dbName << ", the PSyncTask will stop.";
            break;
        }
        auto message = logs.second;
        for (auto &log : message) {
            timer.Reset();
            rocksdb::WriteBatch batch(std::move(log));
            rocksdb::WriteOptions opts;
            rocksdb::Status status;
            while (!stop_) {
                status = db_->Write(opts, &batch);
                if (!stop_ && status == rocksdb::Status::NoSpace()) {
                    waitPost_->WaitFor(NO_SPACE_RETRY_TIME_MS);
                    continue;
                }
                break;
            }
            if (status != rocksdb::Status::OK()) {
                LOG(ERROR) << "Apply write batch failed for replica " << dbName << ", currSeq:" << GetLatestSeqNumber()
                           << ", status:" << status.ToString();
                return;
            }
        }
    }
    LOG(INFO) << "Finish LogReplay for replica " << dbName << ", currSeq:" << GetLatestSeqNumber();
}

Status BackupTask::RestoreDb()
{
    std::string dbDir = replica_->GetDbPath();
    std::string tmpDir = dbDir + ".tmp";
    auto rocksStore = replica_->GetObjectRocksStore();
    RETURN_RUNTIME_ERROR_IF_NULL(rocksStore);
    rocksStore->Close();
    // Rename database directory to tmp, so we can restore if replica fails to load the checkpoint from master.
    // But only try best effort to make data safe
    LOG(INFO) << "rename " << dbDir << " to " << tmpDir;
    auto rc = replica_->GetRocksStoreEnv()->RenameFile(dbDir, tmpDir);
    if (!rc.ok()) {
        LOG(WARNING) << "rename failed, reopen rocksdb " << rc.ToString();
        auto instance = rocksStore->GetInstance(dbDir);
        CHECK_FAIL_RETURN_STATUS(instance != nullptr, K_KVSTORE_ERROR,
                                 FormatString("Failed to rename database directory %s to %s.", dbDir, tmpDir));
        db_ = instance->GetRawDB();
        replica_->SwapRocksStore(instance);
    }

    // Rename sync directory to database directory
    std::string syncDir = replica_->GetSyncPath();
    LOG(INFO) << "rename " << syncDir << " to " << dbDir;
    rc = replica_->GetRocksStoreEnv()->RenameFile(syncDir, dbDir);
    if (!rc.ok()) {
        LOG(WARNING) << "rename failed, reopen rocksdb " << rc.ToString();
        replica_->GetRocksStoreEnv()->RenameFile(tmpDir, dbDir);
        auto instance = rocksStore->GetInstance(dbDir);
        CHECK_FAIL_RETURN_STATUS(instance != nullptr, K_KVSTORE_ERROR,
                                 FormatString("Failed to rename sync directory %s to %s.", syncDir, dbDir));
        db_ = instance->GetRawDB();
        replica_->SwapRocksStore(instance);
    }

    // Open the new database, restore if replica fails to open
    LOG(INFO) << "open rocksdb from " << dbDir;
    std::shared_ptr<RocksStore> instance;
    auto status = Replica::CreateRocksStoreInstanceAndTable(dbDir, instance);
    if (status.IsError()) {
        LOG(WARNING) << "Failed to open rocksdb from " << dbDir;
        rocksdb::DestroyDB(dbDir, rocksdb::Options());
        replica_->GetRocksStoreEnv()->RenameFile(tmpDir, dbDir);
        RETURN_IF_NOT_OK(Replica::CreateRocksStoreInstanceAndTable(dbDir, instance));
        db_ = instance->GetRawDB();
        replica_->SwapRocksStore(instance);
        RETURN_STATUS_LOG_ERROR(K_KVSTORE_ERROR, "Failed to open master checkpoint.");
    }

    // Destroy the origin database
    db_ = instance->GetRawDB();
    replica_->SwapRocksStore(instance);
    rc = rocksdb::DestroyDB(tmpDir, rocksdb::Options());
    if (!rc.ok()) {
        LOG(WARNING) << "Failed to destroy the origin database at '" << tmpDir << "'. Error: " << rc.ToString();
    }
    LOG(INFO) << "restore rocksdb success.";
    return Status::OK();
}

Status BackupTask::FetchFiles(const std::string &dir, const std::vector<std::string> &fileList)
{
    for (const auto &file : fileList) {
        LOG(INFO) << "fetch file " << file;
        std::string tmpFile = dir + "/" + file + ".tmp";
        auto rocksStore = replica_->GetObjectRocksStore();
        RETURN_RUNTIME_ERROR_IF_NULL(rocksStore);
        std::unique_ptr<rocksdb::WritableFile> wf;
        auto s = replica_->GetRocksStoreEnv()->NewWritableFile(tmpFile, &wf, rocksdb::EnvOptions());
        CHECK_FAIL_RETURN_STATUS(s.ok(), K_KVSTORE_ERROR,
                                 FormatString("Failed to create data file %s. Error: %s", tmpFile, s.ToString()));
        uint32_t tmpCrc = crc32(0L, Z_NULL, 0);
        uint64_t offset = 0;
        uint32_t fileCrc = crc32(0L, Z_NULL, 0);
        bool isFinish = false;
        while (!stop_) {
            std::string data;
            Status rc = replica_->FetchFile(primaryNodeId_, file, offset, isFinish, data, fileCrc);
            if (IsRpcTimeoutOrTryAgain(rc)) {
                const int delayMs = 1000;  // retry after 1s.
                std::this_thread::sleep_for(std::chrono::milliseconds(delayMs));
                continue;
            }
            auto message = FormatString("FetchFile result file:%s, offset:%zu, len:%zu, fileCrc:%zu, isFinish:%d", file,
                                        offset, data.size(), fileCrc, isFinish);
            CHECK_FAIL_RETURN_STATUS(rc.IsOk(), K_KVSTORE_ERROR,
                                     FormatString("Fetch file failed, %s, status: %s", message, rc.ToString()));
            if (!isFinish) {
                wf->Append(rocksdb::Slice(data.data(), data.length()));
                tmpCrc = crc32(tmpCrc, (const Bytef *)data.data(), data.length());
                offset += data.length();
                LOG(INFO) << message << ", tmpCrc:" << tmpCrc;
                continue;
            }

            // Verify file crc checksum if crc is not 0
            wf->Append(rocksdb::Slice(data.data(), data.length()));
            tmpCrc = crc32(tmpCrc, (const Bytef *)data.data(), data.length());
            LOG(INFO) << message << ", tmpCrc:" << tmpCrc;
            if (fileCrc && fileCrc != tmpCrc) {
                RETURN_STATUS(K_KVSTORE_ERROR,
                              FormatString("CRC mismatched, %d was expected but got %d", fileCrc, tmpCrc));
            }
            std::string targetFile = dir + "/" + file;
            auto s = replica_->GetRocksStoreEnv()->RenameFile(tmpFile, targetFile);
            CHECK_FAIL_RETURN_STATUS(
                s.ok(), K_KVSTORE_ERROR,
                FormatString("Unable to rename '%s' to '%s'. Error: %s", tmpFile, targetFile, s.ToString()));
            break;
        }
    }
    return Status::OK();
}

void BackupTask::Run()
{
    Status rc;
    std::string dbName = replica_->GetDbName();
    LOG(INFO) << "BackupTask for replica " << dbName << " start";
    while (!stop_) {
        auto traceGuard = Trace::Instance().SetTraceNewID(GetStringUuid() + "-backup");
        logsQueue_->Reset(DEFAULT_MAX_LOG_QUEUE);
        auto currSeq = GetLatestSeqNumber();
        LOG(INFO) << "TryPSync for replica " << dbName << ", primaryNodeId:" << primaryNodeId_
                  << ", LSN:" << (currSeq + 1) << (rc.IsError() ? ", last error:" + rc.ToString() : "");
        rc = replica_->TryPSync(primaryNodeId_, currSeq + 1, replicaId_);
        if (IsRpcTimeoutOrTryAgain(rc)) {
            const int delayMs = 1000;
            std::this_thread::sleep_for(std::chrono::milliseconds(delayMs));
            continue;
        }
        if (rc.IsOk()) {
            LogReplay();
        } else {
            if (rc.GetCode() == K_REPLICA_NOT_READY) {
                waitPost_->WaitFor(WAIT_READY_TIME_MS);
                continue;
            }
            LOG(WARNING) << "TryFullSync due to TryPSync failed with " << rc.ToString();
            LOG_IF_ERROR(TryFullSync(), "TryFullSync failed");
        }

        if (!FileExist(replica_->GetDbPath())) {
            LOG(WARNING) << replica_->GetDbPath() << " db path not exist, replica change will failed";
            auto rocksStore = replica_->GetObjectRocksStore();
            if (rocksStore != nullptr) {
                rocksStore->Close();
            }
            LOG_IF_ERROR(replica_->Init(), "Replica init failed");
            LOG_IF_ERROR(TryFullSync(), "TryFullSync failed");
        }
        // TryPSync again after LogReplay/TryFullSync finish.
    }
    LOG(INFO) << "BackupTask for replica " << dbName << " finish, currSeq:" << GetLatestSeqNumber();
}

Status BackupTask::TryFullSync()
{
    auto syncDir = replica_->GetSyncPath();
    RETURN_IF_NOT_OK_APPEND_MSG(RemoveAll(syncDir), "clear sync dir failed");
    const int permission = 0700;
    LOG(INFO) << "Create sync path " << syncDir;
    RETURN_IF_NOT_OK_APPEND_MSG(CreateDir(syncDir, true, permission), "create sync dir failed");
    Raii clearSyncPath([&syncDir] { LOG_IF_ERROR(RemoveAll(syncDir), "clear sync dir failed"); });
    std::vector<std::string> fileList;
    LOG(INFO) << "Start fetch meta from " << primaryNodeId_;
    RETURN_IF_NOT_OK_APPEND_MSG(replica_->FetchMeta(primaryNodeId_, fileList), "FetchMeta failed");
    LOG(INFO) << "Start fetch file from " << primaryNodeId_ << " to path " << syncDir;
    RETURN_IF_NOT_OK_APPEND_MSG(FetchFiles(syncDir, fileList), "FetchFiles failed");
    RETURN_IF_NOT_OK_APPEND_MSG(RestoreDb(), "RestoreDb failed");
    return Status::OK();
}

Status PrimaryTask::CheckPSync(std::unique_ptr<rocksdb::TransactionLogIterator> &iter, Timer &timer)
{
    auto nextSeq = nextSeq_;
    Status rc;
    if (iter == nullptr || !iter->Valid()) {
        if (!HasNewData(nextSeq) || GetUpdatesSince(nextSeq, iter).IsError()) {
            if (timer.ElapsedSecond() >= PSYNC_HEARTBEAT_TIME_S) {
                Status rc = replica_->PushNewLogs(followerNodeId_, PushLogAction::HEARTBEAT, { "" });
                LOG(INFO) << "PSync Heartbeat: " << rc.ToString();
                timer.Reset();
            }
            const int delayMs = 5;
            std::this_thread::sleep_for(std::chrono::milliseconds(delayMs));
            RETURN_STATUS(K_TRY_AGAIN, "Waiting for new data.");
        }
    }
    return Status::OK();
}

void PrimaryTask::Run()
{
    std::unique_ptr<rocksdb::TransactionLogIterator> iter;
    Timer timer;
    Status rc;
    std::string dbName = replica_->GetDbName();
    LOG(INFO) << "PrimaryTask for replica " << dbName << " start at nextSeq:" << nextSeq_;
    while (!stop_) {
        auto traceGuard = Trace::Instance().SetTraceNewID(GetStringUuid() + "-primary");
        rc = CheckPSync(iter, timer);
        if (rc.GetCode() == K_TRY_AGAIN) {
            continue;
        }
        auto batch = iter->GetBatch();
        auto raw = batch.writeBatchPtr->Data();
        while (true) {
            rc = replica_->PushNewLogs(followerNodeId_, PushLogAction::WRITE_LOG, { raw });
            timer.Reset();
            if (!stop_ && rc.IsError()) {
                LOG(ERROR) << "Push new logs failed: " << rc.ToString() << " Retry, nextSeq:" << nextSeq_
                           << ", currSeq:" << GetLatestSeqNumber();
                waitPost_->WaitFor(RPC_RETRY_TIME_MS);
                continue;
            }
            break;
        }
        nextSeq_ = batch.sequence + batch.writeBatchPtr->Count();
        iter->Next();
    }
    LOG(INFO) << "PrimaryTask for replica " << dbName << " finish, nextSeq:" << nextSeq_
              << ", currSeq:" << GetLatestSeqNumber();
}

Status PrimaryTask::GetUpdatesSince(rocksdb::SequenceNumber nextSeq,
                                    std::unique_ptr<rocksdb::TransactionLogIterator> &iter)
{
    CHECK_FAIL_RETURN_STATUS(db_, StatusCode::K_NOT_FOUND, "Database does not exist");
    auto rc = db_->GetUpdatesSince(nextSeq, &iter);
    if (!rc.ok()) {
        RETURN_STATUS(K_RUNTIME_ERROR, rc.ToString());
    }
    if (!iter->Valid()) {
        RETURN_STATUS(K_RUNTIME_ERROR, "rocksdb interator is invalid");
    }
    return Status::OK();
}
}  // namespace datasystem
