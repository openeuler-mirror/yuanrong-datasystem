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
#include <exception>
#include <new>

#include "datasystem/common/log/log.h"
#include "datasystem/common/util/format.h"
#include "datasystem/common/util/raii.h"
#include "datasystem/worker/object_cache/worker_oc_eviction_manager.h"

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

void AsyncResourceReleaser::Init(std::shared_ptr<ObjectTable> objectTable, PostReleaseCleanup postReleaseCleanup)
{
    if (running_.load()) {
        LOG(WARNING) << "AsyncResourceReleaser already initialized";
        return;
    }

    objectTable_ = std::move(objectTable);
    postReleaseCleanup_ = std::move(postReleaseCleanup);
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
    postReleaseCleanup_ = nullptr;
    {
        std::lock_guard<std::mutex> lock(taskMutex_);
        taskQueue_.clear();
        pendingTasks_.clear();
    }

    LOG(INFO) << "AsyncResourceReleaser shutdown";
}

Status AsyncResourceReleaser::AddTask(const ImmutableString &objectKey, uint64_t version)
{
    PreparedTask task;
    RETURN_IF_NOT_OK(PrepareTask(objectKey, version, task));
    return AddTask(std::move(task));
}

Status AsyncResourceReleaser::PrepareTask(const ImmutableString &objectKey, uint64_t version,
                                          PreparedTask &task)
{
    try {
        task.tasks_.clear();
        task.tasks_.emplace_back(objectKey, version);
        return Status::OK();
    } catch (const std::exception &e) {
        return Status(K_OUT_OF_MEMORY, FormatString("Failed to prepare cleanup ownership for %s: %s", objectKey,
                                                    e.what()));
    } catch (...) {
        return Status(K_OUT_OF_MEMORY, FormatString("Failed to prepare cleanup ownership for %s", objectKey));
    }
}

Status AsyncResourceReleaser::AddTask(PreparedTask &&task)
{
    if (task.tasks_.empty()) {
        return Status(K_INVALID, "Prepared cleanup task is empty");
    }
    const auto &releaseTask = task.tasks_.front();
    AddResult result = AddResult::ADDED;
    size_t pendingTasks = 0;
    try {
        std::lock_guard<std::mutex> lock(taskMutex_);
        result = running_.load() ? EnqueueTaskLocked(task, pendingTasks) : AddResult::STOPPED;
    } catch (...) {
        return Status(K_RUNTIME_ERROR, "Failed to lock async cleanup queue");
    }
    if (result == AddResult::ADDED) {
        taskCv_.notify_one();
        // Ownership has transferred to the worker; it may already have erased the node, so do not read releaseTask.
        VLOG(1) << FormatString("Added async release task, pending tasks: %ld", pendingTasks);
        return Status::OK();
    }
    if (result == AddResult::DUPLICATE) {
        VLOG(1) << FormatString("Ignore duplicate async release task for object %s, version %ld",
                                releaseTask.objectKey, releaseTask.version);
        return Status::OK();
    }
    return Status(K_NOT_READY,
                  FormatString("AsyncResourceReleaser stopped before adding task for %s", releaseTask.objectKey));
}

AsyncResourceReleaser::AddResult AsyncResourceReleaser::EnqueueTaskLocked(PreparedTask &prepared,
                                                                          size_t &pendingTasks) noexcept
{
    auto &task = prepared.tasks_.front();
    try {
        INJECT_POINT_NO_RETURN("AsyncResourceReleaser.AddTask.beforeQueue", [] { throw std::bad_alloc(); });
        auto pendingResult = pendingTasks_.emplace(task.objectKey, task.version);
        if (!pendingResult.second) {
            return AddResult::DUPLICATE;
        }
        task.trackedPending = true;
    } catch (...) {
        // Deduplication is best effort. The prepared list node remains the authoritative cleanup owner and can be
        // spliced into the queue without allocation even if the hash table cannot grow under memory pressure.
    }
    taskQueue_.splice(taskQueue_.end(), prepared.tasks_);
    pendingTasks = pendingTasks_.size() + (task.trackedPending ? 0 : 1);
    return AddResult::ADDED;
}

void AsyncResourceReleaser::WorkerThread()
{
    LOG(INFO) << "AsyncResourceReleaser worker thread started";

    const auto interval = std::chrono::milliseconds(WORKER_SLEEP_MS);
    while (running_) {
        std::list<ReleaseTask> tasks;
        try {
            if (!TakeBatch(tasks, interval)) {
                break;
            }
            ProcessBatch(tasks);
        } catch (const std::exception &e) {
            LOG(ERROR) << "Unexpected exception processing async release batch: " << e.what();
            RequeueBatch(tasks);
        } catch (...) {
            LOG(ERROR) << "Unexpected non-standard exception processing async release batch";
            RequeueBatch(tasks);
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(WORKER_INTERVAL_MS));
    }

    LOG(INFO) << "AsyncResourceReleaser worker thread exited";
}

bool AsyncResourceReleaser::TakeBatch(std::list<ReleaseTask> &tasks, std::chrono::milliseconds interval)
{
    {
        std::unique_lock<std::mutex> lock(taskMutex_);
        (void)taskCv_.wait_for(lock, interval, [this]() { return !running_.load() || !taskQueue_.empty(); });
        if (!running_.load()) {
            return false;
        }
        auto end = taskQueue_.begin();
        std::advance(end, std::min(BATCH_SIZE, taskQueue_.size()));
        tasks.splice(tasks.end(), taskQueue_, taskQueue_.begin(), end);
    }
    ApplyInjectedDelay();
    return true;
}

void AsyncResourceReleaser::ApplyInjectedDelay()
{
    INJECT_POINT("AsyncResourceReleaser.WorkerThread.delay", [](int sleepMs) {
        std::this_thread::sleep_for(std::chrono::milliseconds(sleepMs));
    });
}

void AsyncResourceReleaser::ProcessBatch(std::list<ReleaseTask> &tasks)
{
    if (tasks.empty()) {
        return;
    }
    const size_t batchSize = tasks.size();
    size_t retryCount = 0;
    for (auto iter = tasks.begin(); iter != tasks.end();) {
        auto &task = *iter;
        bool retry = false;
        try {
            if (Release(task.objectKey, task.version).IsError()) {
                retry = true;
            }
        } catch (const std::exception &e) {
            LOG(ERROR) << FormatString("Unexpected exception releasing object %s: %s", task.objectKey, e.what());
            retry = true;
        } catch (...) {
            LOG(ERROR) << FormatString("Unexpected non-standard exception releasing object %s", task.objectKey);
            retry = true;
        }
        if (retry) {
            ++retryCount;
            ++iter;
        } else {
            std::lock_guard<std::mutex> lock(taskMutex_);
            if (task.trackedPending) {
                pendingTasks_.erase(task);
            }
            iter = tasks.erase(iter);
        }
    }
    RequeueBatch(tasks);
    LOG(INFO) << FormatString("Async release batch processed %ld tasks, retry %ld", batchSize, retryCount);
}

void AsyncResourceReleaser::RequeueBatch(std::list<ReleaseTask> &tasks)
{
    if (tasks.empty()) {
        return;
    }
    std::lock_guard<std::mutex> lock(taskMutex_);
    taskQueue_.splice(taskQueue_.end(), tasks);
}

Status AsyncResourceReleaser::Release(const ImmutableString &objectKey, uint64_t expectedVersion)
{
    INJECT_POINT_NO_RETURN("AsyncResourceReleaser.Release", []() {});
    std::shared_ptr<SafeObjType> entry;
    Status rc = objectTable_->Get(objectKey, entry);
    if (rc.GetCode() == StatusCode::K_NOT_FOUND) {
        LOG(INFO) << FormatString("Release skipped: object %s not found: %s", objectKey, rc.ToString());
        return Status::OK();
    }
    if (rc.IsError()) {
        LOG(ERROR) << FormatString("Release failed: cannot read object %s: %s", objectKey, rc.ToString());
        return rc;
    }

    rc = entry->TryWLock();
    if (rc.IsError()) {
        LOG(WARNING) << FormatString("Release failed: lock object %s failed: %s", objectKey, rc.ToString());
        return Status(StatusCode::K_TRY_AGAIN, FormatString("Lock object %s failed: %s", objectKey, rc.ToString()));
    }

    Raii unlock([&entry]() { entry->WUnlock(); });
    const uint64_t currentVersion = (*entry)->GetCreateTime();
    if (expectedVersion != currentVersion) {
        LOG(INFO) << FormatString("Release skipped: object %s version mismatch: expected %ld, current %ld", objectKey,
                                  expectedVersion, currentVersion);
        return Status::OK();
    }
    // ObjectTable is the ownership source of truth. Remove it before cleaning derived eviction state so every failure
    // direction is safe: a cleanup failure may leave a stale candidate, but can never leave a live object that is no
    // longer evictable. Candidate lookup already removes entries whose ObjectTable row is absent.
    Status eraseRc = objectTable_->Erase(objectKey, *entry);
    if (eraseRc.IsError()) {
        LOG(ERROR) << FormatString("Erase object %s failed: %s", objectKey, eraseRc.ToString());
        return eraseRc;
    }
    RunPostReleaseCleanup(objectKey);
    return Status::OK();
}

void AsyncResourceReleaser::RunPostReleaseCleanup(const ImmutableString &objectKey) noexcept
{
    if (!postReleaseCleanup_) {
        return;
    }
    try {
        postReleaseCleanup_(objectKey);
    } catch (const std::exception &e) {
        LOG(ERROR) << FormatString("Post-release cleanup for object %s failed: %s", objectKey, e.what());
    } catch (...) {
        LOG(ERROR) << FormatString("Post-release cleanup for object %s failed by non-standard exception", objectKey);
    }
}

}  // namespace object_cache
}  // namespace datasystem
