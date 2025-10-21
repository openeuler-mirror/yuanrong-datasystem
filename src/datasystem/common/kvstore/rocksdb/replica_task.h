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

#include <cstddef>
#include <vector>

#include "datasystem/common/kvstore/rocksdb/rocks_store.h"
#include "datasystem/common/util/queue/queue.h"
#include "datasystem/common/util/thread.h"
#include "datasystem/common/util/timer.h"
#include "datasystem/common/util/wait_post.h"

#ifndef DATASYSTEM_COMMON_KVSTORE_REPLICA_TASK_H
#define DATASYSTEM_COMMON_KVSTORE_REPLICA_TASK_H
namespace datasystem {
enum class PushLogAction : int { WRITE_LOG = 0, HEARTBEAT = 1, SHUTDOWN = 2 };
constexpr uint32_t DEFAULT_MAX_LOG_QUEUE = 1024;

class Replica;
class TaskBase {
public:
    /**
     * @brief Construct PrimaryTask or BackupTask.
     * @param[in] replica The replica of task.
     */
    TaskBase(Replica *replica);

    virtual ~TaskBase()
    {
        Stop();
    }

    /**
     * @brief Start task.
     * @return Status of the call.
     */
    Status Start()
    {
        CHECK_FAIL_RETURN_STATUS(db_, StatusCode::K_NOT_FOUND, "Database does not exist");
        thread_ = std::make_unique<Thread>([this] { Run(); });
        thread_->set_name("ReplicaSyncTask");
        return Status::OK();
    }

    /**
     * @brief Stop task.
     */
    void Stop()
    {
        stop_ = true;
        if (thread_ != nullptr && thread_->joinable()) {
            thread_->join();
        }
    }

    /**
     * @brief Get latest sequence number of rocksdb logs.
     * @return Sequence number of the call.
     */
    rocksdb::SequenceNumber GetLatestSeqNumber()
    {
        if (db_ == nullptr) {
            return 0;
        }
        return db_->GetLatestSequenceNumber();
    }

protected:
    /**
     * @brief Run task.
     */
    virtual void Run() = 0;

    Replica *replica_;
    rocksdb::DB *db_;
    std::unique_ptr<Thread> thread_;
    std::atomic<bool> stop_{ false };
    std::unique_ptr<WaitPost> waitPost_{ nullptr };
};

class BackupTask : public TaskBase {
public:
    /**
     * @brief The task of backup.
     * @param[in] replica The replica of task.
     * @param[in] primaryNodeId The id  of primary node.
     */
    explicit BackupTask(Replica *replica, const std::string &primaryNodeId)
        : TaskBase(replica), primaryNodeId_(primaryNodeId)
    {
        logsQueue_ = std::make_unique<Queue<std::pair<PushLogAction, std::vector<std::string>>>>(DEFAULT_MAX_LOG_QUEUE);
    }

    ~BackupTask() override;

    /**
     * @brief Apply metadata information.
     * @param[in] logs The metadata needed to apply.
     * @return Status of the call.
     */
    Status ApplyLogs(const std::pair<PushLogAction, std::vector<std::string>> &logs);

    /**
     * @brief Apply metadata information.
     * @param[in] logs The metadata needed to apply.
     * @return PrimaryNodeId of the call.
     */
    std::string GetPrimaryNodeId();

    /**
     * @brief Fetch all files from primary replica.
     * @param[in] dir The path for save fetched files.
     * @param[in] fileList The file list.
     * @return Status of this call.
     */
    Status FetchFiles(const std::string &dir, const std::vector<std::string> &fileList);

    /**
     * @brief Restore rocksdb instacne from sync path.
     * @return Status of this call.
     */
    Status RestoreDb();

private:
    void Run() override;

    /**
     * @brief Replay log.
     */
    void LogReplay();

    /**
     * @brief Try full sync rocksdb instance.
     * @return Status of this call.
     */
    Status TryFullSync();

    std::unique_ptr<Queue<std::pair<PushLogAction, std::vector<std::string>>>> logsQueue_;
    std::string replicaId_;
    std::string primaryNodeId_;
};

class PrimaryTask : public TaskBase {
public:
    /**
     * @brief The task of primary.
     * @param[in] replica The replica of task.
     * @param[in] followerNodeId The id  of follower node.
     */
    explicit PrimaryTask(Replica *replica, const std::string &followerNodeId, rocksdb::SequenceNumber nextSeq)
        : TaskBase(replica), followerNodeId_(followerNodeId), nextSeq_(nextSeq)
    {
    }

    /**
     * @brief The task of primary.
     * @param[in] nextSeq The sequence number of next.
     * @return True if has new data.
     */
    bool HasNewData(rocksdb::SequenceNumber nextSeq)
    {
        return nextSeq <= GetLatestSeqNumber();
    }

    /**
     * @brief The task of primary.
     * @param[in] nextSeq The sequence number of next.
     * @param[in] iter Transaction log iterator.
     * @return Status of the call.
     */
    Status GetUpdatesSince(rocksdb::SequenceNumber nextSeq, std::unique_ptr<rocksdb::TransactionLogIterator> &iter);

    /**
     * @brief Get the last seq no already send to backup replica.
     * @return The last send seq no.
     */
    rocksdb::SequenceNumber GetLatestSendSeqNumber()
    {
        return nextSeq_ > 0 ? nextSeq_ - 1 : 0;
    }

private:
    void Run() override;

    /**
     * @brief The task of primary.
     * @param[in] iter Transaction log iterator.
     * @param[in] timer Timer of heartbeat.
     * @return Status of the call.
     */
    Status CheckPSync(std::unique_ptr<rocksdb::TransactionLogIterator> &iter, Timer &timer);

    const std::string followerNodeId_;
    rocksdb::SequenceNumber nextSeq_;
};
}  // namespace datasystem
#endif
