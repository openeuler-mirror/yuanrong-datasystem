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
 * Description: Async resource releaser for handling failed lock attempts during migration.
 */
#ifndef DATASYSTEM_ASYNC_RESOURCE_RELEASER_H
#define DATASYSTEM_ASYNC_RESOURCE_RELEASER_H

#include <atomic>
#include <condition_variable>
#include <functional>
#include <list>
#include <memory>
#include <mutex>
#include <unordered_set>

#include "datasystem/common/immutable_string/immutable_string.h"
#include "datasystem/common/util/thread.h"
#include "datasystem/worker/object_cache/object_kv.h"

namespace datasystem {
namespace object_cache {

class AsyncResourceReleaser {
public:
    // Runs only after the exact object version has been removed from ObjectTable. Implementations clean derived
    // side-table state and must not assume that the object is still reachable. A failure cannot create a live object
    // that is absent from a side table; stale derived state is tolerated and repaired by normal candidate lookup.
    using PostReleaseCleanup = std::function<void(const ImmutableString &)>;

    class PreparedTask {
    public:
        PreparedTask() = default;
        ~PreparedTask() = default;
        PreparedTask(PreparedTask &&) = default;
        PreparedTask &operator=(PreparedTask &&) = default;
        PreparedTask(const PreparedTask &) = delete;
        PreparedTask &operator=(const PreparedTask &) = delete;

    private:
        friend class AsyncResourceReleaser;
        struct ReleaseTask {
            ImmutableString objectKey;
            uint64_t version;
            bool trackedPending{ false };

            ReleaseTask(const ImmutableString &key, uint64_t ver) : objectKey(key), version(ver) {}

            bool operator==(const ReleaseTask &other) const
            {
                return objectKey == other.objectKey && version == other.version;
            }
        };

        std::list<ReleaseTask> tasks_;
    };

    static AsyncResourceReleaser &Instance();
    void Init(std::shared_ptr<ObjectTable> objectTable, PostReleaseCleanup postReleaseCleanup = nullptr);
    void Shutdown();

    /** Prepare queue-node ownership before an irreversible remote primary switch. */
    Status PrepareTask(const ImmutableString &objectKey, uint64_t version, PreparedTask &task);

    /** Transfer a prepared cleanup task without allocating. */
    Status AddTask(PreparedTask &&task);

    /** Convenience path for callers that do not cross an irreversible commit boundary. */
    Status AddTask(const ImmutableString &objectKey, uint64_t version);

    Status Release(const ImmutableString &objectKey, uint64_t expectedVersion);

private:
    enum class AddResult : uint8_t { ADDED, STOPPED, DUPLICATE };
    using ReleaseTask = PreparedTask::ReleaseTask;

    struct ReleaseTaskHash {
        size_t operator()(const ReleaseTask &task) const
        {
            const size_t keyHash = std::hash<ImmutableString>{}(task.objectKey);
            const size_t versionHash = std::hash<uint64_t>{}(task.version);
            return keyHash ^ (versionHash + 0x9e3779b9 + (keyHash << 6U) + (keyHash >> 2U));
        }
    };

    AsyncResourceReleaser();
    ~AsyncResourceReleaser();

    AsyncResourceReleaser(const AsyncResourceReleaser &) = delete;
    AsyncResourceReleaser(AsyncResourceReleaser &&) = delete;
    AsyncResourceReleaser &operator=(const AsyncResourceReleaser &) = delete;
    AsyncResourceReleaser &operator=(AsyncResourceReleaser &&) = delete;

    /**
     * @brief Background worker thread function.
     */
    void WorkerThread();
    bool TakeBatch(std::list<ReleaseTask> &tasks, std::chrono::milliseconds interval);
    void ProcessBatch(std::list<ReleaseTask> &tasks);
    void RequeueBatch(std::list<ReleaseTask> &tasks);
    void ApplyInjectedDelay();
    void RunPostReleaseCleanup(const ImmutableString &objectKey) noexcept;
    AddResult EnqueueTaskLocked(PreparedTask &task, size_t &pendingTasks) noexcept;

    std::shared_ptr<ObjectTable> objectTable_;
    PostReleaseCleanup postReleaseCleanup_;
    std::atomic<bool> running_;
    Thread workerThread_;

    mutable std::mutex taskMutex_;
    std::condition_variable taskCv_;
    // std::list lets the worker transfer task ownership with splice. Once a cleanup debt is accepted, batching and
    // retrying it never allocate and therefore cannot orphan it under memory pressure.
    std::list<ReleaseTask> taskQueue_;
    // Best-effort dedupe for queued and currently processed tasks. If this set cannot grow, the already-prepared list
    // node is still admitted, so correctness never depends on a fresh allocation after the remote commit.
    std::unordered_set<ReleaseTask, ReleaseTaskHash> pendingTasks_;

    static constexpr int WORKER_SLEEP_MS = 5000;    // 5 s
    static constexpr int WORKER_INTERVAL_MS = 100;  // 100 ms
    static constexpr size_t BATCH_SIZE = 100;
};

}  // namespace object_cache
}  // namespace datasystem

#endif  // DATASYSTEM_ASYNC_RESOURCE_RELEASER_H
