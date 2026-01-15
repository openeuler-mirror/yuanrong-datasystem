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
#include <memory>
#include <mutex>
#include <queue>

#include "datasystem/common/immutable_string/immutable_string.h"
#include "datasystem/common/util/thread.h"
#include "datasystem/worker/object_cache/object_kv.h"

namespace datasystem {
namespace object_cache {

class AsyncResourceReleaser {
public:
    /**
     * @brief Get the singleton instance.
     * @return The singleton instance.
     */
    static AsyncResourceReleaser &Instance();

    /**
     * @brief Initialize the releaser with object table.
     * @param[in] objectTable The object table.
     */
    void Init(std::shared_ptr<ObjectTable> objectTable);

    /**
     * @brief Shutdown the releaser and cleanup resources.
     */
    void Shutdown();

    /**
     * @brief Add a failed release task to the queue.
     * @param[in] objectKey The object key.
     * @param[in] version The object version (create time).
     */
    void AddTask(const ImmutableString &objectKey, uint64_t version);

    /**
     * @brief Release the resource for the given object key and expected version.
     * @param[in] objectKey The object key.
     * @param[in] expectedVersion The expected version (create time).
     * @return Status of the release operation.
     */
    Status Release(const ImmutableString &objectKey, uint64_t expectedVersion);

private:
    struct ReleaseTask {
        ImmutableString objectKey;
        uint64_t version;

        ReleaseTask(const ImmutableString &key, uint64_t ver) : objectKey(key), version(ver) {}
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

    std::shared_ptr<ObjectTable> objectTable_;
    std::atomic<bool> running_;
    Thread workerThread_;

    mutable std::mutex taskMutex_;
    std::condition_variable taskCv_;
    std::queue<ReleaseTask> taskQueue_;

    static constexpr int WORKER_SLEEP_MS = 5000;    // 5 s
    static constexpr int WORKER_INTERVAL_MS = 100;  // 100 ms
    static constexpr size_t BATCH_SIZE = 100;
};

}  // namespace object_cache
}  // namespace datasystem

#endif  // DATASYSTEM_ASYNC_RESOURCE_RELEASER_H
