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
 * Description: Watch dispatcher for coordinator store.
 */
#ifndef DATASYSTEM_COMMON_COORDINATOR_WATCH_DISPATCHER_H
#define DATASYSTEM_COMMON_COORDINATOR_WATCH_DISPATCHER_H

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include "datasystem/common/coordinator/watch_event.h"
#include "datasystem/common/coordinator/watch_registry.h"
#include "datasystem/utils/status.h"

namespace datasystem {
struct WatcherChannel {
    int64_t watchId = 0;
    std::string watcherAddr;
    std::deque<std::shared_ptr<WatchEvent>> queue;
    std::mutex mutex;
    size_t backpressureLimit = 1024;
    size_t assignedThread = 0;
    int64_t snapshotRevision = 0;
    bool needReWatch = false;
    bool dispatchQueued = false;
};

class WatchDispatcher {
public:
    explicit WatchDispatcher(WatchRegistry *watchRegistry);
    virtual ~WatchDispatcher();

    /**
     * @brief Enqueue a watch event into the pending queue.
     * Called by MemoryKvStore mutation callback. O(1).
     * @param[in] event The watch event.
     */
    void Enqueue(std::shared_ptr<WatchEvent> event);

    /**
     * @brief Get the current number of events waiting in the global pending queue.
     * @return Pending event count observed under the pending queue mutex.
     */
    size_t GetPendingEventCount();

    /**
     * @brief Create a channel for a watcher.
     * @param[in] watchId The watch ID.
     * @param[in] watcherAddr The watcher address.
     */
    void AddChannel(int64_t watchId, const std::string &watcherAddr);

    /**
     * @brief Remove a watcher's channel.
     * @param[in] watchId The watch ID.
     */
    void RemoveChannel(int64_t watchId);

    /**
     * @brief Set the snapshot revision for dedup after WatchRange snapshot.
     * @param[in] watchId The watch ID.
     * @param[in] revision The snapshot revision.
     */
    void SetSnapshotRevision(int64_t watchId, int64_t revision);

    /**
     * @brief Start the fan-out thread and dispatch thread pool.
     */
    void Start();

    /**
     * @brief Stop all threads gracefully.
     */
    void Stop();

protected:
    /**
     * @brief Abstract notification method. Override for RPC or mock.
     * @param[in] watchId The watch ID.
     * @param[in] watcherAddr The watcher address.
     * @param[in] events Batch of events to deliver.
     * @return Status of the notification.
     */
    virtual Status DoNotify(int64_t watchId, const std::string &watcherAddr,
                            std::vector<std::shared_ptr<WatchEvent>> &events) = 0;

private:
    struct RetryChannel {
        std::chrono::steady_clock::time_point readyTime;
        std::shared_ptr<WatcherChannel> channel;
    };

    struct DispatchThread {
        std::thread thread;
        std::deque<std::shared_ptr<WatcherChannel>> readyChannels;
        std::deque<RetryChannel> retryChannels;
        std::mutex mutex;
        std::condition_variable cv;
    };

    enum class HandleResult : uint8_t {
        NONE,
        READY_AGAIN,
        RETRY_LATER,
    };

    /**
     * @brief Fan-out thread main loop.
     */
    void FanOutLoop();

    /**
     * @brief Dispatch thread main loop.
     * @param[in] threadIndex Index of this thread in the pool.
     */
    void DispatchLoop(size_t threadIndex);

    /**
     * @brief Notify and commit queued events for one watcher channel.
     * @param[in] channel Watcher channel to process.
     */
    HandleResult HandleChannelEvents(const std::shared_ptr<WatcherChannel> &channel);

    /**
     * @brief Prepare a notification batch from one watcher channel.
     * @param[in] channel Watcher channel to process.
     * @param[out] needReWatch Whether the batch contains a rewatch event.
     * @param[out] maxEventRevision Maximum revision in the prepared batch.
     * @param[out] events Prepared events to notify.
     * @return True if no notification should be sent, otherwise false.
     */
    bool PrepareChannelEvents(const std::shared_ptr<WatcherChannel> &channel, bool &needReWatch,
                              int64_t &maxEventRevision, std::vector<std::shared_ptr<WatchEvent>> &events);

    /**
     * @brief Schedule a channel with pending work onto its assigned dispatch thread.
     * @param[in] channel Watcher channel to schedule.
     */
    void ScheduleChannelLocked(const std::shared_ptr<WatcherChannel> &channel);

    /**
     * @brief Cancel a watcher after notifying that the stream must be rebuilt.
     * @param[in] watchId The watch ID.
     */
    void RemoveRewatchRequiredWatcher(int64_t watchId);

    std::deque<std::shared_ptr<WatchEvent>> pendingQueue_;
    std::mutex pendingMutex_;
    std::condition_variable pendingEmptyCv_;
    std::condition_variable pendingFullCv_;
    std::thread fanOutThread_;

    // Worker failure handling must proactively remove all watch IDs associated with the workerAddr.
    std::unordered_map<int64_t, std::shared_ptr<WatcherChannel>> channels_;
    std::shared_mutex channelsMutex_;

    std::vector<std::unique_ptr<DispatchThread>> dispatchThreadPool_;

    WatchRegistry *watchRegistry_ = nullptr;
    std::atomic<bool> running_{ false };
};
}  // namespace datasystem
#endif  // DATASYSTEM_COMMON_COORDINATOR_WATCH_DISPATCHER_H
