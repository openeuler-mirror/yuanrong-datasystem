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

/**
 * Description: Async resource releaser implementation.
 */
#include "datasystem/worker/object_cache/data_migrator/handler/async_resource_releaser.h"

#include <algorithm>
#include <chrono>
#include <vector>

#include "datasystem/common/log/log.h"
#include "datasystem/common/util/format.h"
#include "datasystem/common/util/raii.h"

namespace datasystem {
namespace object_cache {

AsyncResourceReleaser &AsyncResourceReleaser::Instance()
{
    static AsyncResourceReleaser instance;
    return instance;
}

AsyncResourceReleaser::AsyncResourceReleaser() : running_(false)
{
}

AsyncResourceReleaser::~AsyncResourceReleaser()
{
    Shutdown();
}

void AsyncResourceReleaser::Init(std::shared_ptr<ObjectTable> objectTable)
{
    if (running_.load()) {
        LOG(WARNING) << "AsyncResourceReleaser already initialized";
        return;
    }

    objectTable_ = std::move(objectTable);
    running_.store(true);

    workerThread_ = Thread(&AsyncResourceReleaser::WorkerThread, this);
    workerThread_.set_name("AsyncResReleaser");
    LOG(INFO) << "AsyncResourceReleaser initialized";
}

void AsyncResourceReleaser::Shutdown()
{
    if (!running_.load()) {
        return;
    }

    running_.store(false);
    taskCv_.notify_all();

    if (workerThread_.joinable()) {
        workerThread_.join();
    }

    objectTable_.reset();

    LOG(INFO) << "AsyncResourceReleaser shutdown";
}

void AsyncResourceReleaser::AddTask(const ImmutableString &objectKey, uint64_t version)
{
    if (!running_.load()) {
        LOG(WARNING) << FormatString("AsyncResourceReleaser not initialized, cannot add task for %s", objectKey);
        return;
    }

    std::lock_guard<std::mutex> lock(taskMutex_);
    taskQueue_.emplace(objectKey, version);
    taskCv_.notify_one();
    VLOG(1) << FormatString("Added async release task for object %s, version %ld, pending tasks: %ld", objectKey,
                            version, taskQueue_.size());
}

void AsyncResourceReleaser::WorkerThread()
{
    LOG(INFO) << "AsyncResourceReleaser worker thread started";

    const auto interval = std::chrono::milliseconds(WORKER_SLEEP_MS);
    while (running_) {
        std::vector<ReleaseTask> tasks;
        tasks.reserve(BATCH_SIZE);

        {
            std::unique_lock<std::mutex> lock(taskMutex_);
            (void)taskCv_.wait_for(lock, interval,
                                   [this]() { return !running_.load() || !taskQueue_.empty(); });
            if (!running_.load()) {
                break;
            }
            INJECT_POINT("AsyncResourceReleaser.WorkerThread.delay", [](int sleepMs) {
                std::this_thread::sleep_for(std::chrono::milliseconds(sleepMs));
            });

            const size_t count = std::min(BATCH_SIZE, taskQueue_.size());
            for (size_t i = 0; i < count; ++i) {
                tasks.push_back(taskQueue_.front());
                taskQueue_.pop();
            }
        }

        if (!tasks.empty()) {
            std::vector<ReleaseTask> retryTasks;
            for (auto &task : tasks) {
                if (Release(task.objectKey, task.version).IsError()) {
                    retryTasks.push_back(std::move(task));
                }
            }

            if (!retryTasks.empty()) {
                std::lock_guard<std::mutex> lock(taskMutex_);
                for (auto &task : retryTasks) {
                    taskQueue_.push(task);
                }
            }
            LOG(INFO) << FormatString("Async release batch processed %ld tasks, retry %ld", tasks.size(),
                                      retryTasks.size());
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(WORKER_INTERVAL_MS));
    }

    LOG(INFO) << "AsyncResourceReleaser worker thread exited";
}

Status AsyncResourceReleaser::Release(const ImmutableString &objectKey, uint64_t expectedVersion)
{
    std::shared_ptr<SafeObjType> entry;
    Status rc = objectTable_->Get(objectKey, entry);
    if (rc.IsError()) {
        LOG(INFO) << FormatString("Release skipped: object %s not found: %s", objectKey, rc.ToString());
        return Status::OK();
    }

    rc = entry->TryWLock();
    if (rc.IsError()) {
        LOG(WARNING) << FormatString("Release failed: lock object %s failed: %s", objectKey, rc.ToString());
        return Status(StatusCode::K_TRY_AGAIN, FormatString("Lock object %s failed: %s", objectKey, rc.ToString()));
    }

    Raii unlock([&entry]() { entry->WUnlock(); });
    const uint64_t currentVersion = (*entry)->GetCreateTime();
    if (expectedVersion < currentVersion) {
        LOG(INFO) << FormatString("Release skipped: object %s version advanced: %ld -> %ld", objectKey, expectedVersion,
                                  currentVersion);
        return Status::OK();
    }
    LOG_IF_ERROR(objectTable_->Erase(objectKey, *entry), FormatString("Erase object %s failed", objectKey));
    return Status::OK();
}

}  // namespace object_cache
}  // namespace datasystem
